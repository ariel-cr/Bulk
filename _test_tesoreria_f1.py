"""Test Flujo 1 Tesoreria - 7 types."""
import pyodbc, oracledb, time
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

TESTS=[
    ('bancoTesoreria_type','tstbbnco','no_cuenta','BANCOTESORERIA_TYPE'),
    ('cheque_type','tstbochq','co_orig','CHEQUE_TYPE'),
    ('estadoRegistroTesoreria_type','tstbesta_regi','ds_pago','ESTADOREGISTROTESORERIA_TYPE'),
    ('facturaTesoreria_type','tstbfact_teso','sc_cmpb_regi','FACTURATESORERIA_TYPE'),
    ('ordenPago_type','tstborde','fe_gene','ORDENPAGO_TYPE'),
    ('reversaDesembolso_type','tstbreve_dsmb','st_regi','REVERSADESEMBOLSO_TYPE'),
    ('transferenciaOrden_type','tstbtord','no_tord','TRANSFERENCIAORDEN_TYPE'),
]

c=sql('dbTS').cursor()
can=sql('fcme_canonicos').cursor()
o=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1').cursor()

print(f'TYPES = {len(TESTS)}', flush=True)
can.execute('SELECT ISNULL(MAX(id),0) FROM dbo.cdc_outbox')
out_max=can.fetchone()[0]
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_INBOX')
inb_max=o.fetchone()[0]

print('\n[DISPAROS]', flush=True)
fired=0
for i,(agg,tbl,col,dst) in enumerate(TESTS):
    try:
        c.execute(f"UPDATE TOP (1) dbo.[{tbl}] SET [{col}]=[{col}]")
        rc=c.rowcount
        if rc>0:
            fired+=1
            print(f'  [{i+1}/{len(TESTS)}] {agg:<32} {tbl:<18} UPDATE noop rows={rc}', flush=True)
    except Exception as e:
        print(f'  [{i+1}/{len(TESTS)}] {agg:<32} ERR {str(e)[:120]}', flush=True)

print(f'\nDisparados: {fired}/{len(TESTS)}', flush=True)
print('\n[PROPAGACION] 60s', flush=True)
deadline=time.time()+60
while time.time()<deadline:
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_outbox WHERE id>{out_max}')
    out=can.fetchone()[0]
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max}')
    inb=o.fetchone()[0]
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max} AND PROCESSED=1')
    pr=o.fetchone()[0]
    print(f'  out+={out} inb+={inb} proc+={pr} ({int(deadline-time.time())}s rest)', flush=True)
    if inb>=fired and pr>=inb: break
    time.sleep(6)

print('\n[RESULTADOS]', flush=True)
print(f"{'#':>3} {'aggregate_type':<32} {'inbox':<6} {'proc':<5} {'err':<5} status")
print('-'*90)
ok=0
for i,(agg,_,_,_) in enumerate(TESTS):
    o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max} AND AGGREGATE_TYPE = :1", [agg])
    inb=o.fetchone()[0]
    o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max} AND AGGREGATE_TYPE = :1 AND PROCESSED=1", [agg])
    pr=o.fetchone()[0]
    o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS WHERE INBOX_ID>{inb_max} AND AGGREGATE_TYPE = :1", [agg])
    er=o.fetchone()[0]
    status='OK' if (inb>=1 and pr>=inb and er==0) else ('ERR' if er>0 else 'NO INBOX')
    if status=='OK': ok+=1
    print(f'{i+1:>3} {agg:<32} {inb:<6} {pr:<5} {er:<5} {status}', flush=True)

print(f'\n[RESUMEN] OK={ok}/{len(TESTS)}', flush=True)
time.sleep(3)
can.execute(f'SELECT COUNT(*) FROM dbo.cdc_outbox WHERE id>{out_max}')
print(f'\n[ANTI-LOOP] outbox 3s mas tarde = +{can.fetchone()[0]} (fired={fired})', flush=True)
o.connection.close()
