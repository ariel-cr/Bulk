"""Test: UPDATE en dbFC.fctbafil_actu debe generar N eventos (1 por Type).
fctbafil_actu alimenta: actualizacionAfiliadoType, naturalTrabajoType, personaTelefonosType => 3 eventos."""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = conn("fcme_canonicos").cursor()
cl = conn("dbFC").cursor()

c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); ob = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox");  ib = c.fetchone()[0]
print(f"antes: outbox={ob}  inbox={ib}")

cl.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu ORDER BY ci_cedu")
ci = cl.fetchone()[0]
print(f"\nUPDATE dbFC.fctbafil_actu (ci_cedu={ci})")
cl.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)

c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); oa = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox");  ia = c.fetchone()[0]
print(f"despues: outbox={oa} (+{oa-ob})  inbox={ia} (+{ia-ib})")

print("\nNuevos eventos en outbox:")
c.execute("""
  SELECT id, aggregate_type, aggregate_id, event_type, source_table, created_at
  FROM dbo.cdc_outbox WHERE id > ? ORDER BY id
""", ob)
for r in c.fetchall():
    print(f"  id={r.id}  aggregate_type={r.aggregate_type}  agg_id={r.aggregate_id}  op={r.event_type}  src={r.source_table}")

print("\nNuevos eventos en inbox:")
c.execute("""
  SELECT id, aggregate_type, aggregate_id, event_type, source_table, processed
  FROM dbo.cdc_inbox WHERE id > ? ORDER BY id
""", ib)
for r in c.fetchall():
    print(f"  id={r.id}  aggregate_type={r.aggregate_type}  agg_id={r.aggregate_id}  op={r.event_type}  processed={r.processed}")
