"""Test Flujo 2 Tesoreria - 7 types Newcore -> Legacy + anti-loop."""
import pyodbc, oracledb, time
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

TESTS=[
    ('bancoTesoreria_type','BANCOTESORERIA_TYPE','NOMBRECUENTA','tstbbnco'),
    ('cheque_type','CHEQUE_TYPE','MONTOPAGO','tstbochq'),
    ('estadoRegistroTesoreria_type','ESTADOREGISTROTESORERIA_TYPE','DESCRIPCIONPAGO','tstbesta_regi'),
    ('facturaTesoreria_type','FACTURATESORERIA_TYPE','SECUENCIACMPBREGI','tstbfact_teso'),
    ('ordenPago_type','ORDENPAGO_TYPE','MONTOORDE','tstborde'),
    ('reversaDesembolso_type','REVERSADESEMBOLSO_TYPE','ESTADOREGISTRO','tstbreve_dsmb'),
    ('transferenciaOrden_type','TRANSFERENCIAORDEN_TYPE','NOMBRETORD','tstbtord'),
]

orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()
can=sql('fcme_canonicos').cursor()
ts=sql('dbTS').cursor()

print(f'TYPES = {len(TESTS)}', flush=True)
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX')
out_max=o.fetchone()[0]
can.execute('SELECT ISNULL(MAX(id),0) FROM dbo.cdc_inbox')
inb_max=can.fetchone()[0]

print('\n[DISPAROS]', flush=True)
fired=0
for i,(agg,dest,col,_) in enumerate(TESTS):
    try:
        o.execute(f"SELECT ID FROM FCME_USER.{dest} WHERE ROWNUM<=1")
        r=o.fetchone()
        if not r:
            print(f'  [{i+1}/{len(TESTS)}] {agg:<32} {dest:<32} tabla vacia, skip', flush=True)
            continue
        rid=r[0]
        o.execute(f"UPDATE FCME_USER.{dest} SET {col}={col} WHERE ID=:1", [rid])
        orcl.commit()
        fired+=1
        print(f'  [{i+1}/{len(TESTS)}] {agg:<32} {dest:<32} (ID={rid}) UPDATE noop', flush=True)
    except Exception as e:
        print(f'  [{i+1}/{len(TESTS)}] {agg:<32} ERR {str(e)[:120]}', flush=True)

print(f'\nDisparados: {fired}/{len(TESTS)}', flush=True)
print('\n[PROPAGACION] 90s', flush=True)
deadline=time.time()+90
while time.time()<deadline:
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
    out=o.fetchone()[0]
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max}')
    inb=can.fetchone()[0]
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND processed=1')
    pr=can.fetchone()[0]
    print(f'  out+={out} inb+={inb} proc+={pr} ({int(deadline-time.time())}s rest)', flush=True)
    if inb>=fired and pr>=inb: break
    time.sleep(8)

print('\n[RESULTADOS]', flush=True)
print(f"{'#':>3} {'aggregate_type':<32} {'inbox':<6} {'proc':<5} {'err':<5} status")
print('-'*90)
ok=0
for i,(agg,_,_,_) in enumerate(TESTS):
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=?", agg)
    inb=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=? AND processed=1", agg)
    pr=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=?", agg)
    er=can.fetchone()[0]
    em=None
    if er>0:
        can.execute(f"SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=? ORDER BY error_id DESC", agg)
        em=can.fetchone()[0][:80]
    if inb>=1 and pr>=inb and er==0: status='OK'; ok+=1
    elif er>0: status=f'ERR: {em}'
    else: status='NO INBOX'
    print(f'{i+1:>3} {agg:<32} {inb:<6} {pr:<5} {er:<5} {status[:60]}', flush=True)

print(f'\n[RESUMEN] OK={ok}/{len(TESTS)}', flush=True)
time.sleep(3)
o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
print(f'\n[ANTI-LOOP] outbox 3s mas tarde = +{o.fetchone()[0]} (fired={fired})', flush=True)
can.execute(f"SELECT COUNT(*) FROM dbo.cdc_outbox WHERE source_table LIKE 'dbTS%' AND created_at > DATEADD(MINUTE,-2,SYSDATETIME())")
print(f'  Flujo 1 echo dbTS ultimos 2 min: {can.fetchone()[0]}', flush=True)
orcl.close()
