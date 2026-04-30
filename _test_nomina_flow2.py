"""Test Flujo 2 Nomina (Newcore -> Legacy) - 21 types."""
import pyodbc, oracledb, time

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# (agg, dest, safe_col_for_noop)
TESTS = [
    ('anticipoNominaType','ANTICIPONOMINA_TYPE','ANIO'),
    ('cargaFamiliarType','CARGAFAMILIAR_TYPE','ESTADOREGISTRO'),
    ('cargoGeneralType','CARGOGENERAL_TYPE','ANIOCREDITO'),
    ('cargoLaboralType','CARGOLABORAL_TYPE','CODIGOCARGADMINISTRADOR'),
    ('catalogoNominaType','CATALOGONOMINA_TYPE','DESCRIPCIONADICIONAL'),
    ('configuracionNominaType','CONFIGURACIONNOMINA_TYPE','TIPOINSTITUCION'),
    ('empleadoAuditoriaType','EMPLEADOAUDITORIA_TYPE','CODIGOTRANSACCIONUTILIZADA'),
    ('empleadoDetalleType','EMPLEADODETALLE_TYPE','TIPOCONT'),
    ('empleadoType','EMPLEADO_TYPE','CODIGOCARGO'),
    ('firmaHorarioType','FIRMAHORARIO_TYPE','MAQUINAENLAQUEFIRMO'),
    ('fondoReservaType','FONDORESERVA_TYPE','TIPOACREDITACIONFONDORESERVA'),
    ('historialIngresoType','HISTORIALINGRESO_TYPE','ANIO'),
    ('nivelAcademicoType','NIVELACADEMICO_TYPE','CODIGOINSTITUCION'),
    ('nominaCabeceraType','NOMINACABECERA_TYPE','CODIGONOMINA'),
    ('pagoNominaType','PAGONOMINA_TYPE','CEDULABENEFICIARIO'),
    ('parametroNominaType','PARAMETRONOMINA_TYPE','CODIGOFRECUENCIAPAGOROL'),
    ('patronalNominaType','PATRONALNOMINA_TYPE','CODIGOCIUDAD'),
    ('rolPagoType','ROLPAGO_TYPE','CODIGONOMINA'),
    ('rubroNominaType','RUBRONOMINA_TYPE','CODIGORUBRO'),
    ('sectorIessType','SECTORIESS_TYPE','CODIGOGESTIONIESS'),
    ('viaticoNominaType','VIATICONOMINA_TYPE','CODIGOEMPLEADO'),
]

orcl = oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o = orcl.cursor()
can = sql('fcme_canonicos').cursor()

print(f'TYPES = {len(TESTS)}', flush=True)

o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX')
out_max = o.fetchone()[0]
can.execute('SELECT ISNULL(MAX(id),0) FROM dbo.cdc_inbox')
inb_max = can.fetchone()[0]
can.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors')
err_b = can.fetchone()[0]
print(f'baseline outbox.max={out_max} inbox.max={inb_max} errors={err_b}', flush=True)

print('\n[DISPAROS]', flush=True)
fired = 0
for i,(agg,dest,col) in enumerate(TESTS):
    try:
        # tomar 1 fila existente
        o.execute(f"SELECT ID FROM FCME_USER.{dest} WHERE ROWNUM<=1")
        r = o.fetchone()
        if not r:
            print(f'  [{i+1:>2}/{len(TESTS)}] {agg:<28} {dest:<28} tabla vacia, skip', flush=True)
            continue
        rid = r[0]
        o.execute(f"UPDATE FCME_USER.{dest} SET {col}={col} WHERE ID=:1", [rid])
        orcl.commit()
        fired += 1
        print(f'  [{i+1:>2}/{len(TESTS)}] {agg:<28} {dest:<28} (ID={rid}) UPDATE noop', flush=True)
    except Exception as e:
        print(f'  [{i+1:>2}/{len(TESTS)}] {agg:<28} {dest:<28} ERR {str(e)[:150]}', flush=True)

print(f'\nDisparados: {fired}/{len(TESTS)}', flush=True)
print('\n[PROPAGACION] 90s', flush=True)
deadline = time.time()+90
while time.time()<deadline:
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
    out_n = o.fetchone()[0]
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max}')
    inb_n = can.fetchone()[0]
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND processed=1')
    pr_n = can.fetchone()[0]
    can.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors')
    er_n = can.fetchone()[0]
    print(f'  out+={out_n} inb+={inb_n} proc+={pr_n} err_total={er_n} ({int(deadline-time.time())}s rest)', flush=True)
    if inb_n>=fired and pr_n>=inb_n:
        break
    time.sleep(8)

print('\n[RESULTADOS]', flush=True)
print(f"{'#':>3} {'aggregate_type':<28} {'inbox':<6} {'proc':<5} {'err':<5} status")
print('-'*100)
ok=0
for i,(agg,dest,col) in enumerate(TESTS):
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=?", agg)
    inb_n=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=? AND processed=1", agg)
    pr_n=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=?", agg)
    er_n=can.fetchone()[0]
    em=None
    if er_n>0:
        can.execute(f"SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=? ORDER BY error_id DESC", agg)
        em=can.fetchone()[0][:120]
    status='OK' if (inb_n>=1 and pr_n==inb_n and er_n==0) else (f'ERR: {em}' if em else ('NO INBOX' if inb_n==0 else 'PARTIAL'))
    if inb_n>=1 and pr_n==inb_n and er_n==0: ok+=1
    print(f'{i+1:>3} {agg:<28} {inb_n:<6} {pr_n:<5} {er_n:<5} {status[:60]}', flush=True)

print(f'\n[RESUMEN] OK={ok}/{len(TESTS)}', flush=True)
time.sleep(3)
o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
out_late = o.fetchone()[0]
print(f'\n[ANTI-LOOP] outbox 3s mas tarde = +{out_late} (initial fired={fired})', flush=True)
print('=== FIN ===', flush=True)
orcl.close()
