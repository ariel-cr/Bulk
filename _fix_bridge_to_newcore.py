"""Redirige el bridge: canonicos.cdc_outbox -> newcore.cdc_inbox (no canonicos.cdc_inbox).
Limpia los 5 eventos de prueba en canonicos.cdc_inbox."""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = conn("fcme_canonicos").cursor()

# 1) Recrear trigger para escribir a newcore.cdc_inbox
print("== [1] Recrear trigger trg_cdc_outbox_to_inbox -> NEWCORE ==")
c.execute("IF OBJECT_ID(N'dbo.trg_cdc_outbox_to_inbox',N'TR') IS NOT NULL DROP TRIGGER dbo.trg_cdc_outbox_to_inbox;")
c.execute("""
CREATE TRIGGER dbo.trg_cdc_outbox_to_inbox
ON dbo.cdc_outbox
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO fcme_newcore.dbo.cdc_inbox
        (id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, processed, processed_at)
    SELECT i.id, i.aggregate_id, i.aggregate_type, i.event_type, i.payload, i.source_table, i.created_at, 0, NULL
    FROM inserted i
    WHERE NOT EXISTS (SELECT 1 FROM fcme_newcore.dbo.cdc_inbox x WHERE x.id = i.id);
END
""")
print("  ok -> destino fcme_newcore.dbo.cdc_inbox")

# 2) Limpiar cdc_inbox de canonicos (estaba siendo usado por error)
print("\n== [2] Limpiando fcme_canonicos.dbo.cdc_inbox ==")
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); n = c.fetchone()[0]
c.execute("DELETE FROM dbo.cdc_inbox")
print(f"  borrados: {n}")

# 3) Test
print("\n== [3] Test end-to-end ==")
c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); ob = c.fetchone()[0]
cn = conn("fcme_newcore").cursor()
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id > 1557884"); ib_mios_before = cn.fetchone()[0]

cl = conn("dbFC").cursor()
cl.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu ORDER BY ci_cedu")
ci = cl.fetchone()[0]
print(f"  UPDATE dbFC.fctbafil_actu (ci_cedu={ci})")
cl.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)

c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); oa = c.fetchone()[0]
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id > 1557884"); ib_mios_after = cn.fetchone()[0]
print(f"  canonicos.cdc_outbox: {ob} -> {oa} (+{oa-ob})")
print(f"  newcore.cdc_inbox (eventos nuevos mios): {ib_mios_before} -> {ib_mios_after} (+{ib_mios_after-ib_mios_before})")

# obtener los nuevos ids de outbox
c.execute("""
  SELECT id, aggregate_type, aggregate_id, event_type, source_table
  FROM dbo.cdc_outbox WHERE id > ? ORDER BY id
""", ob)
new_ids = []
print("\n  Nuevos en canonicos.cdc_outbox:")
for r in c.fetchall():
    new_ids.append(r.id)
    print(f"    id={r.id} type={r.aggregate_type} agg={r.aggregate_id}")

if new_ids:
    placeholders = ",".join(["?"] * len(new_ids))
    cn.execute(f"""
      SELECT id, aggregate_type, aggregate_id, event_type, source_table, processed
      FROM dbo.cdc_inbox WHERE id IN ({placeholders}) ORDER BY id
    """, *new_ids)
    rows = cn.fetchall()
    print(f"\n  Mismos en newcore.cdc_inbox ({len(rows)} encontrados):")
    for r in rows:
        print(f"    id={r.id} type={r.aggregate_type} agg={r.aggregate_id} op={r.event_type} processed={r.processed}")
