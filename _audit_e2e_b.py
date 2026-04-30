"""Test e2e del Paso B: 1 UPDATE -> 3 types -> 3 tablas Oracle distintas."""
import pyodbc, oracledb, time

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

def snap():
    s = {}
    cc = sql("fcme_canonicos").cursor()
    cc.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
    s["canon_outbox"] = cc.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM CDC_INBOX")
    s["ora_inbox"] = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM CDC_INBOX WHERE PROCESSED=1")
    s["ora_processed"] = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM CDC_INBOX_ERRORS")
    s["ora_errors"] = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE")
    s["t_actu"] = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM PERSONATELEFONOSTYPE")
    s["t_telf"] = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM NATURALTRABAJOTYPE")
    s["t_trab"] = co.fetchone()[0]
    return s

print("ESTADO INICIAL")
s0 = snap()
for k,v in s0.items(): print(f"  {k:<20} {v}")

print("\n>>> UPDATE en dbFC.fctbafil_actu")
c = sql("dbFC").cursor()
c.execute("UPDATE dbo.fctbafil_actu SET ci_cedu=ci_cedu WHERE ci_cedu=(SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu)")

for i in range(15):
    time.sleep(2)
    s = snap()
    print(f"  T+{(i+1)*2}s: outbox={s['canon_outbox']} inbox={s['ora_inbox']} proc={s['ora_processed']} err={s['ora_errors']} actu={s['t_actu']} telf={s['t_telf']} trab={s['t_trab']}")
    if s["t_actu"]>0 and s["t_telf"]>0 and s["t_trab"]>0:
        break

print("\n=== ESTADO FINAL ===")
sf = snap()
for k,v in sf.items(): print(f"  {k:<20} {v}  (delta {v-s0[k]})")

# Mostrar contenido de tablas destino
print("\n=== ACTUALIZACION_AFILIADO_TYPE ===")
co.execute("SELECT CODIGO_CEDU, DESCRIPCION_CALL_PRIM, NUMERO_MANZ FROM ACTUALIZACION_AFILIADO_TYPE")
for r in co.fetchall(): print(f"  cedu={r[0]} call_prim={r[1]} manz={r[2]}")

print("\n=== PERSONATELEFONOSTYPE (filas por persona) ===")
co.execute("SELECT IDENTIFICACION, SECUENCIATELEFONO, CODIGOTIPOTELEFONO, NUMEROTELEFONO FROM PERSONATELEFONOSTYPE ORDER BY IDENTIFICACION, SECUENCIATELEFONO")
for r in co.fetchall(): print(f"  id={r[0]} sec={r[1]} tipo={r[2]} num={r[3]}")

print("\n=== NATURALTRABAJOTYPE ===")
co.execute("SELECT IDENTIFICACION, SECUENCIATRABAJO, NOMBREEMPLEADOR, CODIGOCARGOPERSONA, TIPOCONTRATO FROM NATURALTRABAJOTYPE")
for r in co.fetchall(): print(f"  id={r[0]} sec={r[1]} empl={r[2]} cargo={r[3]} contr={r[4]}")

if sf["ora_errors"] > 0:
    print("\n=== ERRORES ===")
    co.execute("SELECT INBOX_ID, AGGREGATE_TYPE, ERROR_MESSAGE FROM CDC_INBOX_ERRORS")
    for r in co.fetchall(): print(f"  id={r[0]} type={r[1]} err={r[2][:200]}")

orcl.close()
