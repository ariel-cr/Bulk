"""Deploy SP completo con 36 types + popular module_config + validar."""
import oracledb, pyodbc, time
from collections import defaultdict

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
c = orcl.cursor()

# 1) Drop trigger temporalmente
print("[1] Disable trigger")
try: c.execute("ALTER TRIGGER TRG_PROCESS_CDC_INBOX DISABLE")
except: pass

# 2) Deploy SP
print("[2] Deploy SP completo")
with open(r"C:\Users\Usuario\Downloads\Bulk\_usp_inbox_full.sql", encoding="utf-8") as f:
    sp = f.read()
try:
    c.execute(sp)
except Exception as e:
    print(f"  exec error: {str(e)[:300]}")
c.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='USP_INBOX_PARTICIPES'")
s = c.fetchone()[0]
print(f"  status: {s}")
if s == "INVALID":
    c.execute("SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPES' ORDER BY sequence")
    errs = c.fetchall()
    print(f"  errores: {len(errs)}")
    for r in errs[:8]: print(f"    line={r[0]} col={r[1]}: {r[2][:200]}")
    raise SystemExit(1)

# 3) Pueblar module_config con todas las 76 tablas legacy
print("[3] Poblar CDC_INBOX_MODULE_CONFIG con 76 tablas legacy")
c.execute("DELETE FROM CDC_INBOX_MODULE_CONFIG")
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql_(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)
cc = sql_("fcme_canonicos").cursor()
cc.execute("SELECT DISTINCT source_table FROM dbo.cdc_table_to_types WHERE aggregate_type_emit IS NOT NULL")
tables = [r.source_table for r in cc.fetchall()]
for t in tables:
    c.execute("INSERT INTO CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE) VALUES (:1, 'USP_INBOX_PARTICIPES', 1)", [t])
orcl.commit()
print(f"  {len(tables)} tablas legacy registradas en module_config")

# 4) Re-enable trigger
print("[4] Enable trigger")
c.execute("ALTER TRIGGER TRG_PROCESS_CDC_INBOX ENABLE")

# 5) Cleanup canonicos + Oracle
print("[5] Cleanup")
cc.execute("DELETE FROM dbo.cdc_outbox")
cc.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM CDC_INBOX")
c.execute("DELETE FROM CDC_INBOX_ERRORS")
# limpiar tablas destino para ver flujo limpio
for t in ["ACTUALIZACION_AFILIADO_TYPE","ACTUALIZACION_DOCUMENTOS_TYPE"]:
    try: c.execute(f"DELETE FROM {t}")
    except: pass
orcl.commit()
print("  ok")

# 6) Test masivo: UPDATE en varias tablas legacy diferentes
print("\n[6] Test masivo end-to-end")
tests = [
    ("dbFC", "fctbafil_actu"),
    ("dbFC", "sfct_afiliado"),
    ("dbFC", "fctbarea_lbrl"),
    ("dbFC", "sfct_referencias"),
]
for db, t in tests:
    try:
        f = sql_(db).cursor()
        # update no-destructivo: SET col = col en TOP 1
        f.execute(f"SELECT TOP 1 name FROM sys.columns WHERE object_id=OBJECT_ID('dbo.{t}') ORDER BY column_id")
        col = f.fetchone()[0]
        f.execute(f"UPDATE TOP (1) dbo.{t} SET {col} = {col}")
        print(f"  UPDATE {db}.{t} ok")
    except Exception as e:
        print(f"  UPDATE {db}.{t} fail: {str(e)[:120]}")

print("\n  Esperando propagacion...")
for i in range(8):
    time.sleep(7)
    cc.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); ob = cc.fetchone()[0]
    c.execute("SELECT COUNT(*), MAX(ID) FROM CDC_INBOX"); inb=c.fetchone()
    c.execute("SELECT COUNT(*) FROM CDC_INBOX WHERE PROCESSED=1"); pr=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM CDC_INBOX_ERRORS"); er=c.fetchone()[0]
    print(f"  t={(i+1)*7}s  outbox={ob}  inbox={inb}  processed={pr}  errors={er}")

# Final: ver donde llegaron datos
print("\n[7] Filas en tablas Oracle destino:")
for at_table in ["ACTUALIZACION_AFILIADO_TYPE","ACTUALIZACION_DOCUMENTOS_TYPE","AREALABORALPARTICIPE_TYPE",
                  "REFERENCIAPARTICIPE_TYPE","PERSONATYPE","NATURALTRABAJOTYPE","PERSONATELEFONOSTYPE"]:
    try:
        c.execute(f"SELECT COUNT(*) FROM {at_table}")
        n = c.fetchone()[0]
        print(f"  {at_table}: {n}")
    except: print(f"  {at_table}: ERR")

print("\n[8] Errores top 5:")
c.execute("SELECT INBOX_ID, AGGREGATE_TYPE, ERROR_MESSAGE FROM CDC_INBOX_ERRORS ORDER BY ID DESC FETCH FIRST 5 ROWS ONLY")
for r in c.fetchall(): print(f"  inbox={r[0]} type={r[1]}: {r[2][:200]}")
