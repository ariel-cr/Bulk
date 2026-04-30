"""Crea trigger AFTER INSERT en canonicos.cdc_outbox que copia al inbox automaticamente."""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = conn("fcme_canonicos").cursor()

# Trigger AFTER INSERT en cdc_outbox -> escribe en cdc_inbox inmediatamente
print("== Creando trigger trg_cdc_outbox_to_inbox ==")
c.execute("""
IF OBJECT_ID(N'dbo.trg_cdc_outbox_to_inbox', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_cdc_outbox_to_inbox;
""")
c.execute("""
CREATE TRIGGER dbo.trg_cdc_outbox_to_inbox
ON dbo.cdc_outbox
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.cdc_inbox
        (id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, processed, processed_at)
    SELECT id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, 0, NULL
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM dbo.cdc_inbox x WHERE x.id = i.id);
END
""")
print("  ok")

# Test: disparar cambio en legacy y ver que llega a inbox AUTOMATICAMENTE
print("\n== Test ==")
c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); ob = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); ib = c.fetchone()[0]
print(f"  antes: outbox={ob}  inbox={ib}")

cl = conn("dbFC").cursor()
cl.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu ORDER BY ci_cedu")
ci = cl.fetchone()[0]
print(f"  UPDATE dbFC.fctbafil_actu (ci_cedu={ci})")
cl.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)

c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); oa = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); ia = c.fetchone()[0]
print(f"  despues: outbox={oa} (delta={oa-ob})  inbox={ia} (delta={ia-ib})")

print("\n  Ultimas filas:")
c.execute("""
  SELECT TOP 3 id, aggregate_type, aggregate_id, event_type, source_table
  FROM dbo.cdc_outbox ORDER BY id DESC
""")
print("  canonicos.cdc_outbox:")
for r in c.fetchall():
    print(f"    id={r.id} type={r.aggregate_type} agg={r.aggregate_id} op={r.event_type}")
c.execute("""
  SELECT TOP 3 id, aggregate_type, aggregate_id, event_type, source_table, processed
  FROM dbo.cdc_inbox ORDER BY id DESC
""")
print("  canonicos.cdc_inbox:")
for r in c.fetchall():
    print(f"    id={r.id} type={r.aggregate_type} agg={r.aggregate_id} op={r.event_type} processed={r.processed}")
