"""Cleanup:
1) Drop trigger puente en canonicos (Kafka Connect es el puente real)
2) Limpiar canonicos.cdc_outbox y canonicos.cdc_inbox (no se usan)
3) Test: UPDATE legacy -> ver que aparece en legacy.cdc_outbox (Kafka lo recogera)
"""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = conn("fcme_canonicos").cursor()

print("== [1] Drop trigger puente en canonicos ==")
c.execute("IF OBJECT_ID(N'dbo.trg_cdc_outbox_to_inbox',N'TR') IS NOT NULL DROP TRIGGER dbo.trg_cdc_outbox_to_inbox;")
print("  ok (Kafka Connect hace el puente ahora)")

print("\n== [2] Limpiar canonicos.cdc_outbox / cdc_inbox ==")
c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); n1 = c.fetchone()[0]
c.execute("DELETE FROM dbo.cdc_outbox")
print(f"  canonicos.cdc_outbox: {n1} borradas")
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); n2 = c.fetchone()[0]
c.execute("DELETE FROM dbo.cdc_inbox")
print(f"  canonicos.cdc_inbox:  {n2} borradas")

# Tambien los eventos mios sin procesar que metio mi bridge anterior en newcore.cdc_inbox
cn = conn("fcme_newcore").cursor()
print("\n== [3] Limpiar eventos mios sin procesar en newcore.cdc_inbox ==")
# eventos con aggregate_type = nombre tabla legacy (los primeros que emiti)
cn.execute("""
  SELECT COUNT(*) FROM dbo.cdc_inbox
  WHERE processed = 0
    AND (aggregate_type LIKE 'fctbafil_%' OR aggregate_type LIKE 'sfct_%'
         OR aggregate_type LIKE 'fctb%' OR aggregate_type LIKE 'cgtb%'
         OR aggregate_type LIKE 'svtb%' OR aggregate_type LIKE 'crtb%'
         OR aggregate_type LIKE 'imtb%' OR aggregate_type LIKE 'notb%'
         OR aggregate_type LIKE 'cttb%'
         OR aggregate_type IN ('actualizacionAfiliadoType','naturalTrabajoType','personaTelefonosType'))
""")
n = cn.fetchone()[0]
print(f"  candidatos a limpiar: {n}")
if n > 0:
    cn.execute("""
      DELETE FROM dbo.cdc_inbox
      WHERE processed = 0
        AND (aggregate_type LIKE 'fctbafil_%' OR aggregate_type LIKE 'sfct_%'
             OR aggregate_type LIKE 'fctb%' OR aggregate_type LIKE 'cgtb%'
             OR aggregate_type LIKE 'svtb%' OR aggregate_type LIKE 'crtb%'
             OR aggregate_type LIKE 'imtb%' OR aggregate_type LIKE 'notb%'
             OR aggregate_type LIKE 'cttb%'
             OR aggregate_type IN ('actualizacionAfiliadoType','naturalTrabajoType','personaTelefonosType'))
    """)
    print(f"  borrados: {cn.rowcount}")

# 4) Test: UPDATE legacy -> ver legacy.cdc_outbox
print("\n== [4] Test: UPDATE -> fcme_legacy.cdc_outbox ==")
cl_fc = conn("dbFC").cursor()
cl = conn("fcme_legacy").cursor()

cl.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); ob = cl.fetchone()[0]
cl.execute("SELECT MAX(id) FROM dbo.cdc_outbox"); last_id = cl.fetchone()[0] or 0
print(f"  antes: legacy.cdc_outbox={ob}  last_id={last_id}")

cl_fc.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu ORDER BY ci_cedu")
ci = cl_fc.fetchone()[0]
print(f"  UPDATE dbFC.fctbafil_actu (ci_cedu={ci})")
cl_fc.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)

cl.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); oa = cl.fetchone()[0]
print(f"  despues: legacy.cdc_outbox={oa}  (delta={oa-ob})")

cl.execute("""
  SELECT id, aggregate_type, aggregate_id, event_type, source_table
  FROM dbo.cdc_outbox WHERE id > ? ORDER BY id
""", last_id)
print("\n  Nuevos en fcme_legacy.cdc_outbox:")
for r in cl.fetchall():
    print(f"    id={r.id}  aggregate_type={r.aggregate_type}  agg_id={r.aggregate_id}  op={r.event_type}")

print("\n  KAFKA CONNECT (si esta corriendo) los movera a convivencia.legacy.cdc.outbox")
print("  y el sink los escribira en fcme_newcore.cdc_inbox")
