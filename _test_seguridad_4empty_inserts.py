"""Prueba REAL con INSERTs (no UPDATE no-op) en las 4 tablas vacias.

F1: INSERT en dbSG legacy -> verificar llegada a FCME_USER
F2: INSERT en FCME_USER -> verificar llegada a dbSG legacy
"""
import pyodbc, oracledb, time, datetime as dt

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()
sg=sql('dbSG').cursor()
can=sql('fcme_canonicos').cursor()

RUN_TS=int(time.time())
TEST_CODE = RUN_TS % 30000  # smallint safe range, unique-ish

print(f'TEST RUN_TS={RUN_TS}  TEST_CODE={TEST_CODE}')
print('='*70)

# =====================================================
# FLUJO 1: INSERT en LEGACY -> verificar llegada NEWCORE
# =====================================================
print('\n[F1] INSERT en dbSG.sgtbcnts (cuentaNostroType)')
print('-'*70)
o.execute('SELECT COUNT(*) FROM FCME_USER.CUENTANOSTRO_TYPE')
n_before=o.fetchone()[0]
print(f'  FCME_USER.CUENTANOSTRO_TYPE before = {n_before}')

try:
    sg.execute("INSERT INTO dbo.sgtbcnts (co_cnts, nu_iden, no_cnts, st_cnts, ds_mail) VALUES (?, ?, ?, ?, ?)",
               TEST_CODE, '1234567890', f'TEST-CDC-{TEST_CODE}', 'A', f'test{TEST_CODE}@cdc.com')
    print(f'  INSERT dbSG.sgtbcnts (co_cnts={TEST_CODE}) OK')
except Exception as e:
    print(f'  INSERT FAIL: {str(e)[:200]}')

print(f'\n  Esperando 30s propagacion F1...')
deadline=time.time()+30
arrived=False
while time.time()<deadline:
    time.sleep(5)
    o.execute("SELECT COUNT(*) FROM FCME_USER.CUENTANOSTRO_TYPE WHERE CODIGO = :1", [str(TEST_CODE)])
    arr=o.fetchone()[0]
    if arr>=1:
        arrived=True
        break
o.execute('SELECT COUNT(*) FROM FCME_USER.CUENTANOSTRO_TYPE')
n_after=o.fetchone()[0]
print(f'  FCME_USER.CUENTANOSTRO_TYPE after = {n_after}  delta=+{n_after-n_before}  found_test={arrived}')
o.execute("SELECT ID, CODIGO, NOMBRE, ESTADO FROM FCME_USER.CUENTANOSTRO_TYPE WHERE CODIGO=:1", [str(TEST_CODE)])
r=o.fetchone()
if r: print(f'  Row insertado en Newcore: ID={r[0]} CODIGO={r[1]} NOMBRE={r[2]} ESTADO={r[3]}')

# =====================================================
# F1 segundo: sgtbconf_serv_apli (usuarioServicio_type)
# =====================================================
print('\n[F1] INSERT en dbSG.sgtbconf_serv_apli (usuarioServicio_type)')
print('-'*70)
o.execute('SELECT COUNT(*) FROM FCME_USER.USUARIOSERVICIO_TYPE')
n_before=o.fetchone()[0]
print(f'  FCME_USER.USUARIOSERVICIO_TYPE before = {n_before}')

try:
    now=dt.datetime.now()
    sg.execute("""INSERT INTO dbo.sgtbconf_serv_apli
                  (sc_serv, co_serv_apli, no_serv_apli, sc_tipo, ds_url, no_usua, ds_pass, st_regi,
                   fe_ingr, fe_modi, fe_elim, co_usua_ingr)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
               TEST_CODE, 'X1', f'TEST-SVC-{TEST_CODE}', 1, 'http://test.url', f'usrTEST{TEST_CODE}',
               'pwTEST', 'A', now, now, dt.datetime(1900,1,1), 0)
    print(f'  INSERT dbSG.sgtbconf_serv_apli (sc_serv={TEST_CODE}) OK')
except Exception as e:
    print(f'  INSERT FAIL: {str(e)[:200]}')

print(f'\n  Esperando 30s propagacion F1...')
deadline=time.time()+30
while time.time()<deadline:
    time.sleep(5)
    o.execute("SELECT COUNT(*) FROM FCME_USER.USUARIOSERVICIO_TYPE WHERE CODIGOUSUARIO LIKE :1", [f'usrTEST{TEST_CODE}%'])
    if o.fetchone()[0]>=1: break
o.execute('SELECT COUNT(*) FROM FCME_USER.USUARIOSERVICIO_TYPE')
n_after=o.fetchone()[0]
print(f'  FCME_USER.USUARIOSERVICIO_TYPE after = {n_after}  delta=+{n_after-n_before}')
o.execute("SELECT ID, CODIGOUSUARIO FROM FCME_USER.USUARIOSERVICIO_TYPE WHERE CODIGOUSUARIO LIKE :1", [f'usrTEST{TEST_CODE}%'])
r=o.fetchone()
if r: print(f'  Row insertado en Newcore: ID={r[0]} CODIGOUSUARIO={r[1]}')

# =====================================================
# FLUJO 2: INSERT en NEWCORE -> verificar llegada LEGACY
# =====================================================
TEST_CODE_F2 = (TEST_CODE + 1) % 30000

print('\n\n[F2] INSERT en FCME_USER.CUENTANOSTRO_TYPE (cuentaNostroType)')
print('-'*70)
sg.execute('SELECT COUNT(*) FROM dbo.sgtbcnts')
n_before=sg.fetchone()[0]
print(f'  dbSG.sgtbcnts before = {n_before}')

try:
    o.execute("INSERT INTO FCME_USER.CUENTANOSTRO_TYPE (CODIGO, NOMBRE, ESTADO) VALUES (:1, :2, :3)",
              [str(TEST_CODE_F2), f'F2-NEWCORE-{TEST_CODE_F2}', 'A'])
    orcl.commit()
    print(f'  INSERT FCME_USER.CUENTANOSTRO_TYPE (CODIGO={TEST_CODE_F2}) OK')
except Exception as e:
    print(f'  INSERT FAIL: {str(e)[:200]}')

print(f'\n  Esperando 30s propagacion F2...')
deadline=time.time()+30
while time.time()<deadline:
    time.sleep(5)
    sg.execute("SELECT COUNT(*) FROM dbo.sgtbcnts WHERE co_cnts = ?", TEST_CODE_F2)
    if sg.fetchone()[0]>=1: break
sg.execute('SELECT COUNT(*) FROM dbo.sgtbcnts')
n_after=sg.fetchone()[0]
print(f'  dbSG.sgtbcnts after = {n_after}  delta=+{n_after-n_before}')
sg.execute("SELECT co_cnts, no_cnts, st_cnts FROM dbo.sgtbcnts WHERE co_cnts=?", TEST_CODE_F2)
r=sg.fetchone()
if r: print(f'  Row insertado en Legacy: co_cnts={r.co_cnts} no_cnts={r.no_cnts} st_cnts={r.st_cnts}')

print('\n\n[F2] INSERT en FCME_USER.USUARIOSERVICIO_TYPE (usuarioServicio_type)')
print('-'*70)
sg.execute('SELECT COUNT(*) FROM dbo.sgtbconf_serv_apli')
n_before=sg.fetchone()[0]
print(f'  dbSG.sgtbconf_serv_apli before = {n_before}')

try:
    o.execute("""INSERT INTO FCME_USER.USUARIOSERVICIO_TYPE (CODIGOUSUARIO, CONTRASENIA, NOMBREUSUARIO, ESTADOUSUARIO)
                 VALUES (:1, :2, :3, :4)""",
              [f'svcF2{TEST_CODE_F2}', 'pwF2', f'NameF2-{TEST_CODE_F2}', 'A'])
    orcl.commit()
    print(f'  INSERT FCME_USER.USUARIOSERVICIO_TYPE (CODIGOUSUARIO=svcF2{TEST_CODE_F2}) OK')
except Exception as e:
    print(f'  INSERT FAIL: {str(e)[:300]}')

print(f'\n  Esperando 30s propagacion F2...')
deadline=time.time()+30
while time.time()<deadline:
    time.sleep(5)
    sg.execute("SELECT COUNT(*) FROM dbo.sgtbconf_serv_apli WHERE no_usua LIKE ?", f'svcF2{TEST_CODE_F2}%')
    if sg.fetchone()[0]>=1: break
sg.execute('SELECT COUNT(*) FROM dbo.sgtbconf_serv_apli')
n_after=sg.fetchone()[0]
print(f'  dbSG.sgtbconf_serv_apli after = {n_after}  delta=+{n_after-n_before}')

# =====================================================
# ANTI-LOOP CHECK
# =====================================================
print('\n\n[ANTI-LOOP CHECK]')
print('-'*70)
time.sleep(3)
# Buscar eventos en outbox de los IDs de test
can.execute(f"""SELECT aggregate_type, COUNT(*) FROM dbo.cdc_outbox
                WHERE aggregate_id IN ('{TEST_CODE}','{TEST_CODE_F2}','svcF2{TEST_CODE_F2}','usrTEST{TEST_CODE}')
                  OR payload LIKE '%TEST-CDC-{TEST_CODE}%'
                  OR payload LIKE '%F2-NEWCORE-{TEST_CODE_F2}%'
                GROUP BY aggregate_type""")
for r in can.fetchall():
    print(f'  cdc_outbox (canonicos) {r.aggregate_type:<25}: {r[1]} eventos')

o.execute(f"""SELECT AGGREGATE_TYPE, COUNT(*) FROM FCME_USER.CDC_OUTBOX
              WHERE AGGREGATE_ID IN ('{TEST_CODE}','{TEST_CODE_F2}')
              GROUP BY AGGREGATE_TYPE""")
for r in o.fetchall():
    print(f'  CDC_OUTBOX (Oracle)    {r[0]:<25}: {r[1]} eventos')

print('\n=== FIN ===')
orcl.close()
