"""Test e2e de los 4 mapeos criticos validados en Paso D."""
import pyodbc, oracledb, time

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# Reset
print("[Reset]")
co.execute("DELETE FROM CDC_INBOX")
co.execute("DELETE FROM CDC_INBOX_ERRORS")
orcl.commit()
cs = sql("fcme_canonicos").cursor()
cs.execute("DELETE FROM dbo.cdc_outbox")

TARGETS = ["PERSONAVINCULACIONESTYPE","NATURALINFORMACIONBASICATYPE","REFERENCIAPARTICIPE_TYPE","PERSONATYPE"]
counts_b = {}
for t in TARGETS:
    co.execute(f"SELECT COUNT(*) FROM {t}")
    counts_b[t] = co.fetchone()[0]
print(f"  counts_before: {counts_b}")

# Triggers a disparar (especifico col segura para UPDATE no-op)
TESTS = [
    ("dbIM","imtbmiem_cony","no_apel"),
    ("dbFC","fctbafil_info_actu_docs","ds_ciud_naci"),
    ("dbFC","sfct_referencias","ds_tref"),
    ("dbFC","sfct_banco","no_banco"),
]
print("\n[Triggers]")
for db, tbl, col in TESTS:
    c = sql(db).cursor()
    c.execute(f"SELECT COUNT(*) FROM dbo.[{tbl}]")
    n = c.fetchone()[0]
    print(f"  {db}.{tbl}: filas existentes = {n}")
    if n > 0:
        try:
            c.execute(f"UPDATE TOP (1) dbo.[{tbl}] SET [{col}] = [{col}]")
            print(f"    UPDATE noop sobre col {col} disparado (rowcount={c.rowcount})")
        except Exception as e:
            print(f"    UPDATE fail: {str(e)[:150]}")
    else:
        # insertar fila dummy si vacia
        if tbl == "imtbmiem_cony":
            try:
                c.execute("INSERT INTO dbo.imtbmiem_cony (co_miem, co_cony, no_apel, no_nomb, ds_dire, co_prov, co_ciud) VALUES ('TEST001','TESTCONY01','APELL','NOMB','DIR',1,1)")
                print(f"    INSERT dummy disparado")
            except Exception as e:
                print(f"    INSERT fail: {str(e)[:120]}")
        elif tbl == "fctbafil_info_actu_docs":
            try:
                c.execute("""INSERT INTO dbo.fctbafil_info_actu_docs
                             (sc_actu_docs, co_empr, co_cedu, sc_actv_suje_cred, sc_orgn_ingr,
                              co_pers_poli_expu, ds_ciud_naci, in_comi_serv, ds_comi_serv, fx_ingr, co_usua_ingr)
                             VALUES (1, 1, '0915221477', 1, 1, 'N', 'GUAYAQUIL', 'N', 'N/A', GETDATE(), 'TEST')""")
                print(f"    INSERT dummy disparado")
            except Exception as e:
                print(f"    INSERT fail: {str(e)[:120]}")

# Esperar
print("\n[Esperando propagacion]")
for i in range(15):
    time.sleep(2)
    cs.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
    out = cs.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM CDC_INBOX")
    inb = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM CDC_INBOX WHERE PROCESSED=1")
    pr = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM CDC_INBOX_ERRORS")
    er = co.fetchone()[0]
    print(f"  T+{(i+1)*2}s: outbox={out} inbox={inb} processed={pr} errors={er}")
    if inb > 0 and pr == inb:
        time.sleep(2)
        break

# Resultado por target
print("\n[Resultado en tablas destino]")
for t in TARGETS:
    co.execute(f"SELECT COUNT(*) FROM {t}")
    n = co.fetchone()[0]
    delta = n - counts_b[t]
    print(f"  {t:<35}  delta={delta}  total={n}")

# Inspeccion detallada
print("\n[Detalle PERSONAVINCULACIONESTYPE (conyuges via imtbmiem_cony)]")
co.execute("""SELECT IDENTIFICACION, IDENTIFICACIONPERSONAVINCULADA, CODIGOTIPOVINCULACION, SECUENCIAPERSONAVINCULACION
              FROM PERSONAVINCULACIONESTYPE WHERE CODIGOTIPOVINCULACION='CONYUGE'""")
for r in co.fetchall():
    print(f"  miembro={r[0]} conyuge={r[1]} tipo={r[2]} sec={r[3]}")

print("\n[Detalle NATURALINFORMACIONBASICATYPE (via fctbafil_info_actu_docs)]")
co.execute("""SELECT IDENTIFICACION, HOMONIMIA FROM NATURALINFORMACIONBASICATYPE
              FETCH FIRST 5 ROWS ONLY""")
for r in co.fetchall():
    print(f"  id={r[0]} homonimia={r[1]}")

print("\n[Detalle REFERENCIAPARTICIPE_TYPE]")
co.execute("SELECT CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA FROM REFERENCIAPARTICIPE_TYPE FETCH FIRST 5 ROWS ONLY")
for r in co.fetchall():
    print(f"  cod={r[0]} desc={r[1]}")

print("\n[Detalle PERSONATYPE (todos los registros con CODIGOTIPOPERSONA='JUR')]")
co.execute("""SELECT IDENTIFICACION, CODIGOTIPOPERSONA, NOMBRELEGAL, FECHAINGRESO
              FROM PERSONATYPE WHERE CODIGOTIPOPERSONA='JUR' FETCH FIRST 10 ROWS ONLY""")
for r in co.fetchall():
    print(f"  id={r[0]} tipo={r[1]} nombre={r[2]} ingr={r[3]}")

# Errores
co.execute("SELECT COUNT(*) FROM CDC_INBOX_ERRORS")
n_err = co.fetchone()[0]
if n_err > 0:
    print(f"\n[Errores]: {n_err}")
    co.execute("""SELECT AGGREGATE_TYPE, ERROR_MESSAGE FROM CDC_INBOX_ERRORS FETCH FIRST 10 ROWS ONLY""")
    for r in co.fetchall():
        print(f"  {r[0]}: {r[1][:200]}")

orcl.close()
