"""Test Flujo 1 Nomina - 12 types.
UPDATE no-op en cada tabla legacy dbNO. Watch propagacion a FCME_USER.
"""
import pyodbc, oracledb, time

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# (agg, dbNO_table, safe_col_for_noop, oracle_dest)
TESTS = [
    ('anticipoNominaType','notbcant','mo_soli','ANTICIPONOMINA_TYPE'),
    ('anticipoPagoType','notbpant','mo_dese','ANTICIPOPAGO_TYPE'),
    ('catalogoNominaType','notbcnom','no_nomi','CATALOGONOMINA_TYPE'),
    ('configuracionNominaType','notbpara','qs_cnta_bnco','CONFIGURACIONNOMINA_TYPE'),
    ('nominaCabeceraType','notbcrol','co_nomi','NOMINACABECERA_TYPE'),
    ('obligacionRolType','notbcgrl','mo_cgrl','OBLIGACIONROL_TYPE'),
    ('pagoNominaType','notbpago_nomi','mo_pago','PAGONOMINA_TYPE'),
    ('parametroNominaType','notbpara_gene','ds_par1','PARAMETRONOMINA_TYPE'),
    ('patronalNominaType','notbpatr','no_patr','PATRONALNOMINA_TYPE'),
    ('rolPagoType','notbdrol','mo_rol_pago','ROLPAGO_TYPE'),
    ('rubroNominaType','notbrubr','no_rubr','RUBRONOMINA_TYPE'),
    ('viaticoNominaType','notbcvia','mo_viat','VIATICONOMINA_TYPE'),
]

c_no = sql('dbNO').cursor()
can = sql('fcme_canonicos').cursor()
o = oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1').cursor()

print(f'TYPES = {len(TESTS)}', flush=True)

# baseline
can.execute('SELECT ISNULL(MAX(id),0) FROM dbo.cdc_outbox')
out_max = can.fetchone()[0]
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_INBOX')
inb_max = o.fetchone()[0]
o.execute('SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS')
err_b = o.fetchone()[0]
print(f'baseline outbox.max={out_max} inbox.max={inb_max} errors={err_b}', flush=True)

# disparos
print('\n[DISPAROS]', flush=True)
fired=0
for i,(agg,tbl,col,dst) in enumerate(TESTS):
    try:
        c_no.execute(f"UPDATE TOP (1) dbo.[{tbl}] SET [{col}] = [{col}]")
        rc=c_no.rowcount
        if rc>0:
            fired+=1
            print(f'  [{i+1:>2}/12] {agg:<28} {tbl:<18} UPDATE noop rows={rc}', flush=True)
        else:
            print(f'  [{i+1:>2}/12] {agg:<28} {tbl:<18} no rows to update', flush=True)
    except Exception as e:
        print(f'  [{i+1:>2}/12] {agg:<28} {tbl:<18} ERR: {str(e)[:200]}', flush=True)

print(f'\nDisparados: {fired}/12', flush=True)

# propagacion 60s
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
    if inb_n>=fired and pr_n>=inb_n:
        break
    time.sleep(6)

# resultados
print('\n[RESULTADOS]', flush=True)
print(f"{'#':>3} {'aggregate_type':<28} {'inbox':<6} {'proc':<5} {'err':<5} status")
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
    print(f'{i+1:>3} {agg:<28} {inb_n:<6} {pr_n:<5} {er_n:<5} {status[:60]}', flush=True)

print(f'\n[RESUMEN] OK={ok}/12', flush=True)

# anti-loop check
time.sleep(3)
can.execute(f'SELECT COUNT(*) FROM dbo.cdc_outbox WHERE id>{out_max}')
out_late=can.fetchone()[0]
print(f'\n[ANTI-LOOP] outbox 3s mas tarde = +{out_late} (initial fired={fired})', flush=True)
print('=== FIN ===', flush=True)
o.connection.close()
