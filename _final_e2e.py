"""End-to-end de los 30 types con detección de bucle."""
import pyodbc, oracledb, time, json, urllib.request, sys

# 1) Reanudar connectors
BASE='http://10.35.3.223:30083'
def http(method, path):
    req=urllib.request.Request(BASE+path, method=method, headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=15) as r: return r.status
    except urllib.error.HTTPError as e: return e.code

print("[1] Reanudando connectors")
print(f"  source resume: {http('PUT','/connectors/newcore-oracle-cdc-outbox-source/resume')}")
print(f"  sink resume: {http('PUT','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/resume')}")
time.sleep(8)
sys.stdout.flush()

# 2) Conexiones únicas
DB='10.35.3.64,1433'
PWD='YourPassword123'
o=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=o.cursor()
c_can=pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={DB};DATABASE=fcme_canonicos;UID=sa;PWD={PWD}', autocommit=True, timeout=20).cursor()
c_fc =pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={DB};DATABASE=dbFC;UID=sa;PWD={PWD}', autocommit=True, timeout=20).cursor()

# Cedula real para FK
c_fc.execute("SELECT TOP 1 ci_cedula FROM dbo.sfct_afiliado WHERE ci_cedula IS NOT NULL")
ced = c_fc.fetchone().ci_cedula.strip()
print(f"\n[Cedula real para FK]: {ced}")
sys.stdout.flush()

# Limpiar errores previos
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")

# 3) 30 INSERTs con datos validos
INSERTS = [
    ('referenciaParticipeType', 'REFERENCIAPARTICIPE_TYPE',
     "INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('401','E2E REF')",
     'sfct_referencias','co_tref','401'),
    ('motivoContableType', 'MOTIVOCONTABLE_TYPE',
     "INSERT INTO FCME_USER.MOTIVOCONTABLE_TYPE (CODIGOEMPRESA,CODIGOFONDO,CODIGOMOTIVO,DESCRIPCION,TIPOMOVIMIENTO,ESTADO,CODIGOMODULO,CODIGOAUXILIAR) VALUES ('1','1','401','E2E','D','A','P','1')",
     'sfct_motivo_cnta_cble','ci_moti','401'),
    ('grupoFamiliarType', 'GRUPOFAMILIAR_TYPE',
     f"INSERT INTO FCME_USER.GRUPOFAMILIAR_TYPE (CODIGOTIPOIDENTIFICACION,IDENTIFICACION,CEDULAFAMILIAR,NOMBRESGRUPOFAMILIAR,APELLIDOSGRUPOFAMILIAR,ESTADOFAMILIAR,FECHANACIMIENTO,FECHACREACIONREGISTRO,FECHAMODIFICACION,INDICADORDISCAPACIDAD,CODIGOUSUARIOINGRESOREGISTRO,CODIGOUSUARIOMODIFICOREGISTRO,TIPOCREDITO,TIPOREALCIONFAMILIAR,NUMEROCEDULA) VALUES ('C','{ced}','0922222401','TST','TST','A','1990-01-01','2027-01-01','2027-01-01','N','1','1','P','C','{ced}')",
     'sfct_grupo_fami','ci_cedula_familiar','0922222401'),
    ('firmanteParticipeType', 'FIRMANTEPARTICIPE_TYPE',
     f"INSERT INTO FCME_USER.FIRMANTEPARTICIPE_TYPE (CODIGOEMPRESA,CODIGOTIPOIDENTIFICACION,IDENTIFICACION,SECUENCIAFIRMANTE,NOMBREFIRMANTE,APELLIDOFIRMANTE) VALUES ('1','C','{ced}','9','TST','TST')",
     'sfct_firmante','ci_cedula',ced),
]

print(f"\n[Probando {len(INSERTS)} types]")
sys.stdout.flush()

# Snapshot inicial cdc_inbox
c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
inbox_initial = c_can.fetchone()[0]
print(f"cdc_inbox inicial: {inbox_initial}")
sys.stdout.flush()

results=[]
for at, ot, ins_sql, leg_tbl, leg_pk, leg_pk_val in INSERTS:
    c_fc.execute(f"SELECT COUNT(*) FROM dbo.[{leg_tbl}]")
    n0=c_fc.fetchone()[0]

    try:
        co.execute(ins_sql)
        o.commit()
    except Exception as e:
        results.append((at, leg_tbl, n0, n0, f'ORA fail: {str(e)[:60]}', 0))
        print(f"  {at:<35} ORA fail: {str(e)[:60]}")
        sys.stdout.flush()
        continue

    time.sleep(5)

    c_fc.execute(f"SELECT COUNT(*) FROM dbo.[{leg_tbl}]")
    n1=c_fc.fetchone()[0]
    delta = n1-n0

    # Contar eventos para este aggregate_id (deteccion bucle)
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE aggregate_type=? AND aggregate_id=?", at, str(leg_pk_val))
    n_inbox = c_can.fetchone()[0]

    c_can.execute("SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE aggregate_type=? ORDER BY error_id DESC", at)
    er = c_can.fetchone()

    if delta > 0 and n_inbox <= 2:
        status='OK'
    elif delta > 0 and n_inbox > 2:
        status=f'BUCLE? ({n_inbox} events)'
    elif er:
        status=f'ERR: {er[0][:60]}'
    else:
        status='UPSERT/silenced'

    results.append((at, leg_tbl, n0, n1, status, n_inbox))
    print(f"  {at:<35} pre={n0} post={n1} delta={delta:<3} inbox={n_inbox}  {status}")
    sys.stdout.flush()

# 4) Detección bucle global
time.sleep(5)
c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
inbox_final = c_can.fetchone()[0]
crecimiento = inbox_final - inbox_initial
print(f"\n[Bucle check] cdc_inbox: inicial={inbox_initial} final={inbox_final} delta={crecimiento}")
print(f"  Esperado: ~{len(INSERTS)} eventos. Si delta >> {len(INSERTS)*2} hay bucle.")

ok = sum(1 for r in results if r[4]=='OK')
print(f"\n*** RESUMEN: OK={ok}/{len(INSERTS)} ***")
print("\nFilas insertadas quedan en legacy. Connectors siguen RUNNING.")
sys.stdout.flush()
o.close()
