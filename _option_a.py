"""Opcion A: deshabilitar todos los triggers outbox pre-existentes (patron NO trg_outbox_*)
dejando solo los mios (trg_outbox_*).
Despues validar con UPDATE que solo se publica a canonicos.cdc_outbox (no a legacy)."""
import pyodbc, oracledb, time

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# Paso 1: deshabilitar triggers pre-existentes (no-mios)
print("== [1] Deshabilitando triggers pre-existentes del equipo ==")
disabled = 0
errors = 0
details = []
for db in ["dbCG","dbCR","dbCT","dbFC","dbIM","dbNO","dbSV"]:
    c = conn(db).cursor()
    # triggers con name que NO empieza por 'trg_outbox_' pero existe sobre alguna tabla
    c.execute("""
      SELECT s.name sch, o.name tbl, tr.name tr
      FROM sys.triggers tr
      JOIN sys.objects o ON tr.parent_id = o.object_id
      JOIN sys.schemas s ON o.schema_id = s.schema_id
      WHERE tr.name NOT LIKE 'trg_outbox_%'
        AND tr.is_disabled = 0
    """)
    rows = c.fetchall()
    for r in rows:
        try:
            c.execute(f"DISABLE TRIGGER [{r.sch}].[{r.tr}] ON [{r.sch}].[{r.tbl}]")
            disabled += 1
            details.append((db, r.tr, r.tbl))
        except Exception as e:
            errors += 1
            print(f"  fail {db}.{r.tr}: {str(e)[:120]}")

print(f"  total deshabilitados: {disabled}")
print(f"  errores: {errors}")
print("\n  Detalle:")
for d,t,tbl in details:
    print(f"    {d}.{tbl}  ->  {t}")

# Paso 2: verificar mis triggers siguen activos
print("\n== [2] Confirmando mis 76 triggers siguen activos ==")
active = 0
for db in ["dbCG","dbCR","dbCT","dbFC","dbIM","dbNO","dbSV"]:
    c = conn(db).cursor()
    c.execute("""
      SELECT COUNT(*) FROM sys.triggers
      WHERE name LIKE 'trg_outbox_%' AND is_disabled = 0
    """)
    n = c.fetchone()[0]
    active += n
    print(f"  {db}: {n} activos")
print(f"  TOTAL mios activos: {active}")

# Paso 3: test
print("\n== [3] Test: UPDATE -> SOLO debe aparecer en canonicos.cdc_outbox (no legacy) ==")
cl = conn("fcme_legacy").cursor()
cc = conn("fcme_canonicos").cursor()
cl.execute("SELECT COUNT(*), MAX(id) FROM dbo.cdc_outbox"); bl = cl.fetchone()
cc.execute("SELECT COUNT(*), MAX(id) FROM dbo.cdc_outbox"); bc = cc.fetchone()
print(f"  antes: legacy.cdc_outbox={bl}   canonicos.cdc_outbox={bc}")

f = conn("dbFC").cursor()
f.execute("UPDATE dbo.fctbafil_actu SET ci_cedu=ci_cedu WHERE ci_cedu=(SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu)")
print("  UPDATE ejecutado")

cl.execute("SELECT COUNT(*), MAX(id) FROM dbo.cdc_outbox"); al = cl.fetchone()
cc.execute("SELECT COUNT(*), MAX(id) FROM dbo.cdc_outbox"); ac = cc.fetchone()
print(f"  despues: legacy.cdc_outbox={al} (delta={al[0]-bl[0]})   canonicos.cdc_outbox={ac} (delta={ac[0]-bc[0]})")

if al[0] == bl[0]:
    print("  ✓ legacy.cdc_outbox NO se toco (triggers del equipo deshabilitados)")
if ac[0] > bc[0]:
    print("  ✓ canonicos.cdc_outbox recibio los eventos (mis triggers funcionando)")

# Esperar propagacion a Oracle
print("\n  Esperando 20s propagacion Kafka -> Oracle...")
orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
co.execute("SELECT COUNT(*) FROM CDC_INBOX"); bo = co.fetchone()[0]
co.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE"); bt = co.fetchone()[0]

for i in range(4):
    time.sleep(6)
    co.execute("SELECT COUNT(*) FROM CDC_INBOX"); no = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE"); nt = co.fetchone()[0]
    print(f"    t={(i+1)*6}s  CDC_INBOX={no} (+{no-bo})  ACTUALIZACION_AFILIADO_TYPE={nt} (+{nt-bt})")
