"""Test Flujo 1 Recaudaciones - 11 types."""
import pyodbc, oracledb, time
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

# (agg, dbRC tabla, safe col para UPDATE no-op, oracle dest)
TESTS=[
    ('aplicacionRecaudacion_type','rctbapli_reca','mo_reca','APLICACIONRECAUDACION_TYPE'),
    ('caucionRecaudacion_type','rctbcaut','co_cnta_auto_noid','CAUCIONRECAUDACION_TYPE'),
    ('devolucionRendicion_type','rctbdevo_rind','mo_devo','DEVOLUCIONRENDICION_TYPE'),
    ('estadoCuenta_type','rctbesta_cnta','mo_depo','ESTADOCUENTA_TYPE'),
    ('estadoRecaudacion_type','rctbesta_reca','ds_estado','ESTADORECAUDACION_TYPE'),
    ('recaudacion_type','rctbreca','mo_reca','RECAUDACION_TYPE'),
    ('rendicion_type','rctbrind','nu_cpbt','RENDICION_TYPE'),
    ('saldoRecaudacion_type','rctbcsal_reca','mo_gene','SALDORECAUDACION_TYPE'),
    ('sciRecaudacion_type','rctbcsci_bce','nu_envi','SCIRECAUDACION_TYPE'),
    ('tipoDescuento_type','rctbtipo_desc','no_tipo_desc','TIPODESCUENTO_TYPE'),
    ('transaccionRecaudacion_type','rctbtrec','no_reca','TRANSACCIONRECAUDACION_TYPE'),
]

c_rc=sql('dbRC').cursor()
can=sql('fcme_canonicos').cursor()
o=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1').cursor()

print(f'TYPES = {len(TESTS)}', flush=True)
can.execute('SELECT ISNULL(MAX(id),0) FROM dbo.cdc_outbox')
out_max=can.fetchone()[0]
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_INBOX')
inb_max=o.fetchone()[0]
o.execute('SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS')
err_b=o.fetchone()[0]
print(f'baseline outbox.max={out_max} inbox.max={inb_max} errors={err_b}', flush=True)

# disparos
print('\n[DISPAROS]', flush=True)
fired=0
for i,(agg,tbl,col,dst) in enumerate(TESTS):
    try:
        c_rc.execute(f"UPDATE TOP (1) dbo.[{tbl}] SET [{col}]=[{col}]")
        rc=c_rc.rowcount
        if rc>0:
            fired+=1
            print(f'  [{i+1:>2}/{len(TESTS)}] {agg:<32} {tbl:<22} UPDATE noop rows={rc}', flush=True)
        else:
            print(f'  [{i+1:>2}/{len(TESTS)}] {agg:<32} {tbl:<22} no rows', flush=True)
    except Exception as e:
        print(f'  [{i+1:>2}/{len(TESTS)}] {agg:<32} {tbl:<22} ERR {str(e)[:150]}', flush=True)

print(f'\nDisparados: {fired}/{len(TESTS)}', flush=True)
print('\n[PROPAGACION] 60s', flush=True)
deadline=time.time()+60
while time.time()<deadline:
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_outbox WHERE id>{out_max}')
    out_n=can.fetchone()[0]
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max}')
    inb_n=o.fetchone()[0]
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max} AND PROCESSED=1')
    pr_n=o.fetchone()[0]
    o.execute('SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS')
    er_n=o.fetchone()[0]
    print(f'  out+={out_n} inb+={inb_n} proc+={pr_n} err_total={er_n} ({int(deadline-time.time())}s rest)', flush=True)
    if inb_n>=fired and pr_n>=inb_n: break
    time.sleep(6)

print('\n[RESULTADOS]', flush=True)
print(f"{'#':>3} {'aggregate_type':<32} {'inbox':<6} {'proc':<5} {'err':<5} status")
print('-'*100)
ok=0
for i,(agg,tbl,col,dst) in enumerate(TESTS):
    o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max} AND AGGREGATE_TYPE = :1", [agg])
    inb_n=o.fetchone()[0]
    o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max} AND AGGREGATE_TYPE = :1 AND PROCESSED=1", [agg])
    pr_n=o.fetchone()[0]
    o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS WHERE INBOX_ID>{inb_max} AND AGGREGATE_TYPE = :1", [agg])
    er_n=o.fetchone()[0]
    em=None
    if er_n>0:
        o.execute(f"SELECT * FROM (SELECT ERROR_MESSAGE FROM FCME_USER.CDC_INBOX_ERRORS WHERE INBOX_ID>{inb_max} AND AGGREGATE_TYPE = :1 ORDER BY ERROR_DATE DESC) WHERE ROWNUM=1", [agg])
        em=o.fetchone()
        em=em[0][:120] if em else None
    status='OK' if (inb_n>=1 and pr_n==inb_n and er_n==0) else (f'ERR: {em}' if em else ('NO INBOX' if inb_n==0 else 'PARTIAL'))
    if inb_n>=1 and pr_n==inb_n and er_n==0: ok+=1
    print(f'{i+1:>3} {agg:<32} {inb_n:<6} {pr_n:<5} {er_n:<5} {status[:60]}', flush=True)

print(f'\n[RESUMEN] OK={ok}/{len(TESTS)}', flush=True)
time.sleep(3)
can.execute(f'SELECT COUNT(*) FROM dbo.cdc_outbox WHERE id>{out_max}')
out_late=can.fetchone()[0]
print(f'\n[ANTI-LOOP] outbox 3s mas tarde = +{out_late} (initial fired={fired})', flush=True)
print('=== FIN ===', flush=True)
o.connection.close()
