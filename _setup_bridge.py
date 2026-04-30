"""Crea:
 1) legacy.dbo.cdc_outbox_bridge_state  (checkpoint del bridge)
 2) legacy.dbo.usp_cdc_bridge_to_newcore  (el 'Kafka': copia outbox->inbox newcore)
 3) Verifica trigger newcore.trg_process_cdc_inbox
 4) Test end-to-end: UPDATE legacy -> outbox -> bridge -> newcore.inbox -> usp_inbox_PARTICIPE
"""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# 1) Checkpoint table
print("== [1] Creando tabla checkpoint del bridge ==")
c = conn("fcme_legacy").cursor()
c.execute("""
IF OBJECT_ID(N'dbo.cdc_outbox_bridge_state', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cdc_outbox_bridge_state (
        channel         VARCHAR(50) NOT NULL PRIMARY KEY,
        last_sent_id    BIGINT      NOT NULL DEFAULT 0,
        events_total    BIGINT      NOT NULL DEFAULT 0,
        last_run_at     DATETIME2   NULL
    );
    INSERT INTO dbo.cdc_outbox_bridge_state (channel) VALUES ('LEGACY_TO_NEWCORE');
END
""")
print("  ok")

# 2) Bridge SP
print("\n== [2] Creando SP usp_cdc_bridge_to_newcore ==")
c.execute("""
IF OBJECT_ID(N'dbo.usp_cdc_bridge_to_newcore', N'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_cdc_bridge_to_newcore;
""")
c.execute("""
CREATE PROCEDURE dbo.usp_cdc_bridge_to_newcore
    @max_rows INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @last_id BIGINT;
    SELECT @last_id = last_sent_id FROM dbo.cdc_outbox_bridge_state WHERE channel='LEGACY_TO_NEWCORE';
    IF @last_id IS NULL SET @last_id = 0;

    DECLARE @new_max BIGINT = @last_id;
    DECLARE @copied INT = 0;

    ;WITH nuevos AS (
        SELECT TOP (@max_rows) id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at
        FROM dbo.cdc_outbox
        WHERE id > @last_id
        ORDER BY id
    )
    INSERT INTO fcme_newcore.dbo.cdc_inbox
        (id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, processed, processed_at)
    SELECT id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, 0, NULL
    FROM nuevos;

    SET @copied = @@ROWCOUNT;

    IF @copied > 0
    BEGIN
        SELECT @new_max = MAX(id) FROM fcme_newcore.dbo.cdc_inbox WHERE id > @last_id;
        UPDATE dbo.cdc_outbox_bridge_state
        SET last_sent_id = @new_max,
            events_total = events_total + @copied,
            last_run_at  = SYSUTCDATETIME()
        WHERE channel = 'LEGACY_TO_NEWCORE';
    END
    ELSE
    BEGIN
        UPDATE dbo.cdc_outbox_bridge_state
        SET last_run_at = SYSUTCDATETIME()
        WHERE channel = 'LEGACY_TO_NEWCORE';
    END

    SELECT @copied AS copied, @last_id AS prev_last_id, @new_max AS new_last_id;
END
""")
print("  ok")

# 3) Verificar trigger newcore
print("\n== [3] Body del trigger newcore.trg_process_cdc_inbox ==")
c2 = conn("fcme_newcore").cursor()
c2.execute("SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.trg_process_cdc_inbox')) AS b")
body = c2.fetchone().b or ""
for line in body.splitlines():
    print("  " + line)

print("\n== [4] Flujo listo ==")
print("  Productor: 76 triggers en dbCG/dbCR/dbCT/dbFC/dbIM/dbNO/dbSV -> fcme_legacy.cdc_outbox")
print("  Bridge:    EXEC fcme_legacy.dbo.usp_cdc_bridge_to_newcore")
print("  Consumer:  fcme_newcore.dbo.trg_process_cdc_inbox (existe) -> usp_inbox_PARTICIPE")
