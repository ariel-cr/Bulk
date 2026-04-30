"""Test Flujo 2 Seguridad - 11 types Newcore -> Legacy + anti-loop check."""
import pyodbc, oracledb, time
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

# (agg, oracle dest, safe col noop, legacy table dbSG)
TESTS=[
    ('aplicacionFuncion_type','APLICACIONFUNCION_TYPE','NOMBREAPLICACION','sgtbapli'),
    ('auditoriaFlujo_type','AUDITORIAFLUJO_TYPE','CODIGOPROCESO','sgtbtran'),
    ('cuentaNostroType','CUENTANOSTRO_TYPE','NOMBRE','sgtbcnts'),
    ('empresa_type','EMPRESA_TYPE','NOMBREEMPRESA','sgtbempr'),
    ('firmaSeguridad_type','FIRMASEGURIDAD_TYPE','NOMBREMAQUINAUSUARIO','sgtbfirm'),
    ('fondoSeguridad_type','FONDOSEGURIDAD_TYPE','NOMBREFONDO','sgtbfond'),
    ('localidad_type','LOCALIDAD_TYPE','CODIGOPROVINCIA','sgtbloca'),
    ('parametroSeguridad_type','PARAMETROSEGURIDAD_TYPE','NOMBREPARAMETRO','sgtbpara'),
    ('passwordSeguridad_type','PASSWORDSEGURIDAD_TYPE','CONTRASENIA','sgtbpass'),
    ('usuarioSeguridad_type','USUARIOSEGURIDAD_TYPE','CODIGOUSUARIO','sgtbusua'),
    ('usuarioServicio_type','USUARIOSERVICIO_TYPE','CODIGOUSUARIO','sgtbconf_serv_apli'),
]

orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()
can=sql('fcme_canonicos').cursor()
sg=sql('dbSG').cursor()

print(f'TYPES = {len(TESTS)}', flush=True)
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX')
out_max=o.fetchone()[0]
can.execute('SELECT ISNULL(MAX(id),0) FROM dbo.cdc_inbox')
inb_max=can.fetchone()[0]
can.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors')
err_b=can.fetchone()[0]
print(f'baseline outbox.max={out_max} inbox.max={inb_max} errors_total={err_b}', flush=True)

legacy_before={}
for agg,_,_,ltbl in TESTS:
    sg.execute(f'SELECT COUNT(*) FROM dbo.[{ltbl}]')
    legacy_before[ltbl]=sg.fetchone()[0]

print('\n[DISPAROS]', flush=True)
fired=0
for i,(agg,dest,col,ltbl) in enumerate(TESTS):
    try:
        o.execute(f"SELECT ID FROM FCME_USER.{dest} WHERE ROWNUM<=1")
        r=o.fetchone()
        if not r:
            print(f'  [{i+1:>2}] {agg:<28} {dest:<28} tabla vacia, skip', flush=True)
            continue
        rid=r[0]
        o.execute(f"UPDATE FCME_USER.{dest} SET {col}={col} WHERE ID=:1", [rid])
        orcl.commit()
        fired+=1
        print(f'  [{i+1:>2}] {agg:<28} {dest:<28} (ID={rid}) UPDATE noop', flush=True)
    except Exception as e:
        print(f'  [{i+1:>2}] {agg:<28} ERR {str(e)[:120]}', flush=True)

print(f'\nDisparados: {fired}/{len(TESTS)}', flush=True)
print('\n[PROPAGACION] 90s', flush=True)
deadline=time.time()+90
while time.time()<deadline:
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
    out_n=o.fetchone()[0]
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max}')
    inb_n=can.fetchone()[0]
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND processed=1')
    pr_n=can.fetchone()[0]
    can.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors')
    er_n=can.fetchone()[0]
    print(f'  out+={out_n} inb+={inb_n} proc+={pr_n} err_total={er_n} ({int(deadline-time.time())}s rest)', flush=True)
    if inb_n>=fired and pr_n>=inb_n: break
    time.sleep(8)

print('\n[RESULTADOS]', flush=True)
print(f"{'#':>3} {'aggregate_type':<28} {'inbox':<6} {'proc':<5} {'err':<5} {'leg_delta':<10} status")
print('-'*120)
ok=0
for i,(agg,dest,col,ltbl) in enumerate(TESTS):
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=?", agg)
    inb_n=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=? AND processed=1", agg)
    pr_n=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=?", agg)
    er_n=can.fetchone()[0]
    sg.execute(f'SELECT COUNT(*) FROM dbo.[{ltbl}]')
    leg_now=sg.fetchone()[0]
    leg_delta=leg_now-legacy_before[ltbl]
    em=None
    if er_n>0:
        can.execute(f"SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=? ORDER BY error_id DESC", agg)
        em=can.fetchone()[0][:100]
    if inb_n>=1 and pr_n>=inb_n and er_n==0:
        status='OK'; ok+=1
    elif er_n>0: status=f'ERR'
    else: status='NO INBOX'
    print(f'{i+1:>3} {agg:<28} {inb_n:<6} {pr_n:<5} {er_n:<5} {leg_delta:+}{"":<7} {status}{(": "+em) if em else ""}'[:120], flush=True)

print(f'\n[RESUMEN] OK={ok}/{len(TESTS)}', flush=True)
time.sleep(3)
o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
out_late=o.fetchone()[0]
print(f'\n[ANTI-LOOP] outbox 3s mas tarde = +{out_late} (initial fired={fired})', flush=True)
can.execute(f"SELECT COUNT(*) FROM dbo.cdc_outbox WHERE source_table LIKE 'dbSG%' AND created_at > DATEADD(MINUTE,-2,SYSDATETIME())")
flow1_echo=can.fetchone()[0]
print(f'  Flujo 1 echo dbSG ultimos 2 min: {flow1_echo}', flush=True)
print('=== FIN ===', flush=True)
orcl.close()
