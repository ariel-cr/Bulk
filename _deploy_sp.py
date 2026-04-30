"""Despliega el SP desde archivo .sql, valida compilacion, prueba flujo."""
import oracledb, pyodbc, time

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
c = orcl.cursor()

with open(r"C:\Users\Usuario\Downloads\Bulk\_usp_inbox_participes.sql", encoding="utf-8") as f:
    sp = f.read()

print("[1] Desplegando USP_INBOX_PARTICIPES")
c.execute(sp)

c.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='USP_INBOX_PARTICIPES'")
status = c.fetchone()[0]
print(f"  status: {status}")

c.execute("SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPES' ORDER BY sequence")
errs = c.fetchall()
if errs:
    print(f"  ERRORES: {len(errs)}")
    for r in errs[:5]: print(f"    line={r[0]} col={r[1]}: {r[2][:300]}")
    raise SystemExit(1)
print("  COMPILADO OK")

# Procesar pending del fctbafil_actu que ya esta en CDC_INBOX
print("\n[2] Procesar pending fctbafil_actu manualmente")
c.execute("""SELECT ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD FROM CDC_INBOX
             WHERE PROCESSED=0 AND AGGREGATE_TYPE='fctbafil_actu'""")
rows = c.fetchall()
for r in rows:
    p = r[3].read() if hasattr(r[3],'read') else r[3]
    c.execute("BEGIN USP_INBOX_PARTICIPES(:1, :2, :3, :4); END;", [r[0], r[1], r[2], p])
orcl.commit()
print(f"  procesados manualmente: {len(rows)}")

c.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE"); print(f"  ACTUALIZACION_AFILIADO_TYPE: {c.fetchone()[0]}")

# Trigger Oracle: habilitar
print("\n[3] Habilitar trigger Oracle")
c.execute("ALTER TRIGGER TRG_PROCESS_CDC_INBOX ENABLE")
print("  ok")

# UPDATE fresco -> ver flujo automatico
print("\n[4] UPDATE fresco para flujo automatico")
f = pyodbc.connect("DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=dbFC;UID=sa;PWD=YourPassword123", autocommit=True).cursor()
f.execute("UPDATE dbo.fctbafil_actu SET ci_cedu=ci_cedu WHERE ci_cedu=(SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu)")

for i in range(8):
    time.sleep(7)
    c.execute("SELECT COUNT(*), MAX(ID) FROM CDC_INBOX"); inb = c.fetchone()
    c.execute("SELECT COUNT(*) FROM CDC_INBOX WHERE PROCESSED=1"); pr = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE"); at = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM CDC_INBOX_ERRORS"); er = c.fetchone()[0]
    print(f"  t={(i+1)*7}s  inbox={inb}  processed={pr}  actu={at}  errors={er}")

print("\n[5] Resumen final")
c.execute("SELECT ID, AGGREGATE_TYPE, AGGREGATE_ID, PROCESSED, PROCESSED_AT FROM CDC_INBOX ORDER BY ID")
for r in c.fetchall(): print(f"  inbox id={r[0]} type={r[1]} agg={r[2]} processed={r[3]} at={r[4]}")
c.execute("SELECT CODIGO_CEDU, CODIGO_PROV, DESCRIPCION_CALL_PRIM FROM ACTUALIZACION_AFILIADO_TYPE")
for r in c.fetchall(): print(f"  ACTU_AFIL: cedu={r[0]} prov={r[1]} call_prim={r[2]}")
c.execute("SELECT INBOX_ID, AGGREGATE_TYPE, ERROR_MESSAGE FROM CDC_INBOX_ERRORS ORDER BY ID DESC FETCH FIRST 3 ROWS ONLY")
errs = c.fetchall()
if errs:
    print("\n  ULTIMOS ERRORES:")
    for r in errs: print(f"    inbox_id={r[0]} type={r[1]} : {r[2][:200]}")
