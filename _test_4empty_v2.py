"""Test final con INSERTs reales tras los fixes."""
import pyodbc, oracledb, time, datetime as dt

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()
sg=sql('dbSG').cursor()

T=int(time.time())
TC = T % 30000  # safe smallint

print(f'TEST CODE = {TC}')
print('='*70)

# F1 #1: dbSG.sgtbcnts -> CUENTANOSTRO_TYPE  (ya probado antes y FUNCIONA)
# F1 #2: dbSG.sgtbconf_serv_apli con todos los NOT NULL llenos
print('\n[F1] INSERT dbSG.sgtbconf_serv_apli (todos los NOT NULL llenos)')
o.execute('SELECT COUNT(*) FROM FCME_USER.USUARIOSERVICIO_TYPE')
n0=o.fetchone()[0]
print(f'  USUARIOSERVICIO_TYPE before = {n0}')
try:
    now=dt.datetime.now()
    sg.execute("""INSERT INTO dbo.sgtbconf_serv_apli
                  (co_serv_apli, no_serv_apli, sc_tipo, ds_url, no_usua, ds_pass, st_regi,
                   fe_ingr, fe_modi, fe_elim, co_usua_ingr, co_usua_modi, co_usua_elim)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
               'X2', f'F1-SVC-{TC}', 1, 'http://test.com', f'usrF1V_{TC}',
               'pwT', 'A', now, now, dt.datetime(1900,1,1), 0, 0, 0)
    print(f'  INSERT OK')
except Exception as e:
    print(f'  FAIL: {str(e)[:200]}')

print('  Esperando 25s F1 propagacion...')
time.sleep(25)
o.execute('SELECT COUNT(*) FROM FCME_USER.USUARIOSERVICIO_TYPE')
n1=o.fetchone()[0]
print(f'  USUARIOSERVICIO_TYPE after = {n1}  delta=+{n1-n0}')
o.execute("SELECT ID, CODIGOUSUARIO FROM FCME_USER.USUARIOSERVICIO_TYPE WHERE CODIGOUSUARIO LIKE :1", [f'usrF1V_{TC}%'])
r=o.fetchone()
if r: print(f'  Row Newcore: ID={r[0]} CODIGOUSUARIO={r[1]}')

# F2 #1: FCME_USER.CUENTANOSTRO_TYPE -> sgtbcnts
print('\n[F2] INSERT FCME_USER.CUENTANOSTRO_TYPE -> dbSG.sgtbcnts (SP CRUD robusto)')
TF2_CNTS = (TC + 100) % 30000
sg.execute('SELECT COUNT(*) FROM dbo.sgtbcnts')
n0=sg.fetchone()[0]
print(f'  dbSG.sgtbcnts before = {n0}')
try:
    o.execute("INSERT INTO FCME_USER.CUENTANOSTRO_TYPE (CODIGO, NOMBRE, ESTADO) VALUES (:1, :2, 'A')",
              [str(TF2_CNTS), f'F2-CNTS-{TF2_CNTS}'])
    orcl.commit()
    print(f'  INSERT FCME_USER (CODIGO={TF2_CNTS}) OK')
except Exception as e:
    print(f'  FAIL: {str(e)[:200]}')

print('  Esperando 25s F2 propagacion...')
time.sleep(25)
sg.execute('SELECT COUNT(*) FROM dbo.sgtbcnts')
n1=sg.fetchone()[0]
print(f'  dbSG.sgtbcnts after = {n1}  delta=+{n1-n0}')
sg.execute("SELECT co_cnts, no_cnts, st_cnts FROM dbo.sgtbcnts WHERE co_cnts = ?", TF2_CNTS)
r=sg.fetchone()
if r: print(f'  Row Legacy: co_cnts={r.co_cnts} no_cnts={r.no_cnts} st_cnts={r.st_cnts}')

# F2 #2: FCME_USER.USUARIOSERVICIO_TYPE -> sgtbconf_serv_apli
print('\n[F2] INSERT FCME_USER.USUARIOSERVICIO_TYPE -> dbSG.sgtbconf_serv_apli')
sg.execute('SELECT COUNT(*) FROM dbo.sgtbconf_serv_apli')
n0=sg.fetchone()[0]
print(f'  dbSG.sgtbconf_serv_apli before = {n0}')
try:
    o.execute("""INSERT INTO FCME_USER.USUARIOSERVICIO_TYPE (CODIGOUSUARIO, NOMBREUSUARIO, ESTADOUSUARIO)
                 VALUES (:1, :2, 'A')""", [f'svcV2_{TC}', f'F2-USR-{TC}'])
    orcl.commit()
    print(f'  INSERT OK')
except Exception as e:
    print(f'  FAIL: {str(e)[:200]}')

print('  Esperando 25s F2 propagacion...')
time.sleep(25)
sg.execute('SELECT COUNT(*) FROM dbo.sgtbconf_serv_apli')
n1=sg.fetchone()[0]
print(f'  dbSG.sgtbconf_serv_apli after = {n1}  delta=+{n1-n0}')

# Anti-loop final
print('\n[ANTI-LOOP CHECK]')
time.sleep(3)
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_TYPE IN ('cuentaNostroType','usuarioServicio_type') AND CREATED_AT > SYSTIMESTAMP - 0.005")
print(f'  CDC_OUTBOX Newcore eventos recientes (8 min): {o.fetchone()[0]}')

print('\n=== FIN ===')
orcl.close()
