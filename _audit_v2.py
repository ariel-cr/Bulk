"""Auditoria v2: una sola conexion SQL, un mapping explicito, retry hasta conectar."""
import os, re, json, time, sys
import pytds
import oracledb

ROOT = '/Users/dennise/Desktop/CAPA/cdc/REFACTOR_PARTICIPE'

# Mapeo explicito folder -> (sql_table, oracle_table, aggregate_type)
MAPPING = {
    'actualizacionAfiliado_type':       ('participes.actualizacionAfiliado_type',       'ACTUALIZACION_AFILIADO_TYPE',         'actualizacionAfiliadoType'),
    'actualizacionDocumentos_type':     ('participes.actualizacionDocumentos_type',     'ACTUALIZACION_DOCUMENTOS_TYPE',       'actualizacionDocumentosType'),
    'agendaMailAfiliado_type':          ('participes.agendaMailAfiliado_type',          'AGENDAMAILAFILIADO_TYPE',             'agendaMailAfiliadoType'),
    'auditoriaAfiliado_type':           ('participes.auditoriaAfiliado_type',           'AUDITORIAAFILIADO_TYPE',              'auditoriaAfiliadoType'),
    'beneficiario_type':                ('participes.beneficiario_type',                'BENEFICIARIOPARTICIPE_TYPE',          'beneficiarioParticipeType'),
    'correoElectronico_type':           ('participes.correoElectronico_type',           None,                                  'correoElectronicoType'),
    'cuentaBancariaAfiliado_type':      ('participes.cuentaBancariaAfiliado_type',      'CUENTABANCARIAAFILIADO_TYPE',         'cuentaBancariaAfiliadoType'),
    'documentacionAfiliado_type':       ('participes.documentacionAfiliado_type',       'DOCUMENTACIONAFILIADO_TYPE',          'documentacionAfiliadoType'),
    'firmanteParticipe_type':           ('participes.firmanteParticipe_type',           'FIRMANTEPARTICIPE_TYPE',              'firmanteParticipeType'),
    'grupoFamiliar_type':               ('participes.grupoFamiliar_type',               'GRUPOFAMILIAR_TYPE',                  'grupoFamiliarType'),
    'imagenesType':                     ('participes.imagenesType',                     'IMAGENESTYPE',                        'imagenesType'),
    'informacionAdicionalAfiliado_type':('participes.informacionAdicionalAfiliado_type','INFORMACIONADICIONALAFILIADO_TYPE',   'informacionAdicionalAfiliadoType'),
    'otrosIngresosAfiliado_type':       ('participes.otrosIngresosAfiliado_type',       'OTROSINGRESOSAFILIADO_TYPE',          'otrosIngresosAfiliadoType'),
}

def connect_sql(retries=8):
    for i in range(retries):
        try:
            return pytds.connect('10.35.3.64', database='fcme_canonicos', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
        except Exception as e:
            print(f'  SQL retry {i+1}/{retries}: {str(e)[:80]}', file=sys.stderr)
            time.sleep(5 + i*2)
    raise RuntimeError('No SQL connection')

print('Connecting...', file=sys.stderr)
sql_conn = connect_sql()
sql = sql_conn.cursor()
print('SQL OK', file=sys.stderr)
o_conn = oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
ora = o_conn.cursor()
print('Oracle OK', file=sys.stderr)

# Pre-check: aggregate_types con errores
print('\n[ERRORES POR aggregate_type en cdc_inbox_errors]')
sql.execute("SELECT aggregate_type, COUNT(*) FROM dbo.cdc_inbox_errors GROUP BY aggregate_type ORDER BY 2 DESC")
err_counts = {r[0]: r[1] for r in sql.fetchall()}
for at, n in err_counts.items():
    print(f'  {at:<45} {n}')

# Recientes errores
print('\n[ULTIMOS 30 ERRORES]')
sql.execute("SELECT TOP 30 error_id, aggregate_type, event_type, LEFT(error_message, 200), created_at FROM dbo.cdc_inbox_errors ORDER BY error_id DESC")
recent_errors = sql.fetchall()
for r in recent_errors:
    print(f'  [{r[0]}] {str(r[4])[:19]}  {r[1]}/{r[2]}: {r[3]}')

# Por type
print('\n' + '=' * 110)
print('AUDITORIA POR TYPE  (SQL canonicos -> Oracle migration)')
print('=' * 110)
print(f'{"folder":<38} {"SQL":>8} {"Oracle":>8} {"delta":>10} {"errs":>6}  notas')
print('-' * 110)
results = []
for folder, (sql_table, ora_table, agg_type) in MAPPING.items():
    notes = []
    sql_count = ora_count = None
    try:
        sql.execute(f'SELECT COUNT(*) FROM {sql_table}')
        sql_count = sql.fetchone()[0]
    except Exception as e:
        notes.append(f'SQL: {str(e)[:60]}')

    if ora_table:
        try:
            ora.execute(f'SELECT COUNT(*) FROM FCME_USER.{ora_table}')
            ora_count = ora.fetchone()[0]
        except Exception as e:
            notes.append(f'Oracle: {str(e)[:60]}')
    else:
        notes.append('Oracle: tabla no mapeada')

    errs = err_counts.get(agg_type, 0)
    delta = (sql_count - ora_count) if (sql_count is not None and ora_count is not None) else None
    if delta and delta > 0: notes.append(f'falta migrar {delta}')
    if delta and delta < 0: notes.append(f'sobran en Oracle {-delta}')

    print(f'{folder:<38} {str(sql_count):>8} {str(ora_count):>8} {str(delta):>10} {errs:>6}  {" | ".join(notes)}')
    results.append((folder, sql_count, ora_count, delta, errs, notes))

# Detalle errores por type (ultimos 3)
print('\n' + '=' * 110)
print('DETALLE: ULTIMOS ERRORES POR TYPE')
print('=' * 110)
for folder, (sql_t, ora_t, agg_type) in MAPPING.items():
    if err_counts.get(agg_type, 0) == 0: continue
    sql.execute("SELECT TOP 3 error_id, event_type, LEFT(error_message, 250), created_at FROM dbo.cdc_inbox_errors WHERE aggregate_type=%s ORDER BY error_id DESC", (agg_type,))
    rows = sql.fetchall()
    print(f'\n[{folder}]  ({err_counts.get(agg_type)} errores total)')
    for r in rows:
        print(f'  [{r[0]}] {str(r[3])[:19]} {r[1]}: {r[2]}')

# Sumario
print('\n' + '=' * 110)
print('RESUMEN')
print('=' * 110)
ok = [r for r in results if r[3]==0 and r[4]==0]
mismatch = [r for r in results if r[3] is not None and r[3] != 0]
errs_only = [r for r in results if r[4] > 0]
sql_fail = [r for r in results if r[1] is None]
ora_fail = [r for r in results if r[2] is None]
print(f'  Types totales auditados: {len(results)}')
print(f'  OK (counts iguales, sin errores): {len(ok)}')
print(f'  Counts no coinciden SQL vs Oracle: {len(mismatch)}')
print(f'  Con errores en cdc_inbox_errors: {len(errs_only)}')
print(f'  SQL canonicos: tabla no existe / fallo: {len(sql_fail)}')
print(f'  Oracle: tabla no existe / fallo: {len(ora_fail)}')

sql_conn.close()
o_conn.close()
