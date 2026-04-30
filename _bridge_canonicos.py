"""Crea bridge canonicos.cdc_outbox -> canonicos.cdc_inbox (simula Kafka)
y prueba end-to-end."""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = conn("fcme_canonicos").cursor()

# 1) Checkpoint
print("== [1] Checkpoint ==")
c.execute("""
IF OBJECT_ID(N'dbo.cdc_outbox_bridge_state', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.cdc_outbox_bridge_state (
    channel VARCHAR(50) NOT NULL PRIMARY KEY,
    last_sent_id BIGINT NOT NULL DEFAULT 0,
    events_total BIGINT NOT NULL DEFAULT 0,
    last_run_at DATETIME2 NULL
  );
  INSERT INTO dbo.cdc_outbox_bridge_state (channel) VALUES ('OUTBOX_TO_INBOX');
END
""")
print("  ok")

# 2) Bridge SP: copia outbox -> inbox en la MISMA BD canonicos
print("\n== [2] SP usp_cdc_bridge_outbox_to_inbox ==")
c.execute("IF OBJECT_ID('dbo.usp_cdc_bridge_outbox_to_inbox','P') IS NOT NULL DROP PROCEDURE dbo.usp_cdc_bridge_outbox_to_inbox")
c.execute("""
CREATE PROCEDURE dbo.usp_cdc_bridge_outbox_to_inbox
    @max_rows INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @last_id BIGINT;
    SELECT @last_id = last_sent_id FROM dbo.cdc_outbox_bridge_state WHERE channel='OUTBOX_TO_INBOX';
    IF @last_id IS NULL SET @last_id = 0;

    DECLARE @copied INT = 0;
    DECLARE @new_max BIGINT = @last_id;

    ;WITH nuevos AS (
        SELECT TOP (@max_rows)
            o.id, o.aggregate_id, o.aggregate_type, o.event_type, o.payload, o.source_table, o.created_at
        FROM dbo.cdc_outbox o
        WHERE o.id > @last_id
          AND NOT EXISTS (SELECT 1 FROM dbo.cdc_inbox i WHERE i.id = o.id)
        ORDER BY o.id
    )
    INSERT INTO dbo.cdc_inbox
        (id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, processed, processed_at)
    SELECT id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, 0, NULL
    FROM nuevos;

    SET @copied = @@ROWCOUNT;
    SELECT @new_max = ISNULL(MAX(id), @last_id) FROM dbo.cdc_outbox WHERE id > @last_id;

    UPDATE dbo.cdc_outbox_bridge_state
    SET last_sent_id = @new_max,
        events_total = events_total + @copied,
        last_run_at = SYSUTCDATETIME()
    WHERE channel = 'OUTBOX_TO_INBOX';

    SELECT @copied AS copied, @last_id AS prev_last_id, @new_max AS new_last_id;
END
""")
print("  ok")

# 3) Test end-to-end
print("\n== [3] Test end-to-end ==")
cl = conn("dbFC").cursor()

c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); out_b = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox");  in_b = c.fetchone()[0]
print(f"  antes: canonicos.cdc_outbox={out_b}  canonicos.cdc_inbox={in_b}")

cl.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu ORDER BY ci_cedu")
ci = cl.fetchone()[0]
print(f"\n  UPDATE no-destructivo sobre dbFC.fctbafil_actu (ci_cedu={ci})")
cl.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)

c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); out_a = c.fetchone()[0]
print(f"  despues UPDATE: canonicos.cdc_outbox={out_a}  (delta={out_a-out_b})")

print("\n  EXEC dbo.usp_cdc_bridge_outbox_to_inbox")
c.execute("EXEC dbo.usp_cdc_bridge_outbox_to_inbox @max_rows=1000")
r = c.fetchone()
print(f"    copied={r.copied}  prev={r.prev_last_id}  new={r.new_last_id}")

c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); in_a = c.fetchone()[0]
print(f"  despues bridge: canonicos.cdc_inbox={in_a}  (delta={in_a-in_b})")

# 4) Muestra
print("\n== [4] Muestra eventos nuevos ==")
c.execute("""
  SELECT TOP 5 id, aggregate_type, aggregate_id, event_type, source_table, created_at
  FROM dbo.cdc_outbox ORDER BY id DESC
""")
print("  canonicos.cdc_outbox (ultimos 5):")
for r in c.fetchall():
    print(f"    id={r.id} type={r.aggregate_type} agg={r.aggregate_id} op={r.event_type} src={r.source_table}")

c.execute("""
  SELECT TOP 5 id, aggregate_type, aggregate_id, event_type, source_table, processed
  FROM dbo.cdc_inbox ORDER BY id DESC
""")
print("\n  canonicos.cdc_inbox (ultimos 5):")
for r in c.fetchall():
    print(f"    id={r.id} type={r.aggregate_type} agg={r.aggregate_id} op={r.event_type} src={r.source_table} processed={r.processed}")
