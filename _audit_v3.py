"""Auditoria v3: SQL canonicos -> SQL fcme_migration (NO Oracle).
Una sola conexion reutilizada, mapping explicito.
"""
import os, time, sys
import pytds

MAPPING = {
    # folder -> (sql_canonicos_table, sql_migration_table, aggregate_type)
    'actualizacionAfiliado_type':       ('participes.actualizacionAfiliado_type',       'FCME_USER.ACTUALIZACION_AFILIADO_TYPE',         'actualizacionAfiliadoType'),
    'actualizacionDocumentos_type':     ('participes.actualizacionDocumentos_type',     'FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE',       'actualizacionDocumentosType'),
    'agendaMailAfiliado_type':          ('participes.agendaMailAfiliado_type',          'FCME_USER.AGENDAMAILAFILIADO_TYPE',             'agendaMailAfiliadoType'),
    'auditoriaAfiliado_type':           ('participes.auditoriaAfiliado_type',           'FCME_USER.AUDITORIAAFILIADO_TYPE',              'auditoriaAfiliadoType'),
    'beneficiario_type':                ('participes.beneficiario_type',                'FCME_USER.BENEFICIARIO_TYPE',                   'beneficiarioParticipeType'),
    'correoElectronico_type':           ('participes.correoElectronico_type',           'FCME_USER.CORREOELECTRONICO_TYPE',              'correoElectronicoType'),
    'cuentaBancariaAfiliado_type':      ('participes.cuentaBancariaAfiliado_type',      'FCME_USER.CUENTABANCARIAAFILIADO_TYPE',         'cuentaBancariaAfiliadoType'),
    'documentacionAfiliado_type':       ('participes.documentacionAfiliado_type',       'FCME_USER.DOCUMENTACIONAFILIADO_TYPE',          'documentacionAfiliadoType'),
    'firmanteParticipe_type':           ('participes.firmanteParticipe_type',           'FCME_USER.FIRMANTEPARTICIPE_TYPE',              'firmanteParticipeType'),
    'grupoFamiliar_type':               ('participes.grupoFamiliar_type',               'FCME_USER.GRUPOFAMILIAR_TYPE',                  'grupoFamiliarType'),
    'imagenesType':                     ('participes.imagenesType',                     'FCME_USER.IMAGENESTYPE',                        'imagenesType'),
    'informacionAdicionalAfiliado_type':('participes.informacionAdicionalAfiliado_type','FCME_USER.INFORMACIONADICIONALAFILIADO_TYPE',   'informacionAdicionalAfiliadoType'),
    'otrosIngresosAfiliado_type':       ('participes.otrosIngresosAfiliado_type',       'FCME_USER.OTROSINGRESOSAFILIADO_TYPE',          'otrosIngresosAfiliadoType'),
}

def conn(db, retries=15, delay=20):
    for i in range(retries):
        try:
            return pytds.connect('10.35.3.64', database=db, user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
        except Exception as e:
            print(f'  retry {db} {i+1}/{retries}: {str(e)[:60]}', file=sys.stderr)
            time.sleep(delay)
    raise RuntimeError(f'No connection to {db}')

print('Connecting fcme_canonicos...', file=sys.stderr, flush=True)
can = conn('fcme_canonicos')
print('Connecting fcme_migration...', file=sys.stderr, flush=True)
mig = conn('fcme_migration')
print('Both OK', file=sys.stderr, flush=True)

c_can = can.cursor()
c_mig = mig.cursor()

# Errores aggregated
print('\n[ERRORES POR aggregate_type en cdc_inbox_errors (top 30)]')
c_can.execute("SELECT TOP 30 aggregate_type, COUNT(*) c FROM dbo.cdc_inbox_errors GROUP BY aggregate_type ORDER BY 2 DESC")
err_counts = {}
for r in c_can.fetchall():
    err_counts[r[0]] = r[1]
    print(f'  {r[0]:<48} {r[1]}')

# Por type
print('\n' + '=' * 110)
print('AUDITORIA: SQL fcme_canonicos -> SQL fcme_migration')
print('=' * 110)
print(f'{"folder":<38} {"canonicos":>10} {"migration":>10} {"delta":>10} {"errs":>6}  notas')
print('-' * 110)
results = []
for folder, (sql_t, mig_t, agg) in MAPPING.items():
    notes = []
    sql_count = mig_count = None
    try:
        c_can.execute(f'SELECT COUNT(*) FROM {sql_t}')
        sql_count = c_can.fetchone()[0]
    except Exception as e:
        notes.append(f'canonicos: {str(e)[:60]}')
    try:
        c_mig.execute(f'SELECT COUNT(*) FROM {mig_t}')
        mig_count = c_mig.fetchone()[0]
    except Exception as e:
        notes.append(f'migration: {str(e)[:60]}')
    errs = err_counts.get(agg, 0)
    delta = (sql_count - mig_count) if (sql_count is not None and mig_count is not None) else None
    if delta is not None:
        if delta > 0: notes.append(f'falta migrar {delta}')
        elif delta < 0: notes.append(f'sobran en migration {-delta}')
    print(f'{folder:<38} {str(sql_count):>10} {str(mig_count):>10} {str(delta):>10} {errs:>6}  {" | ".join(notes)}')
    results.append((folder, sql_count, mig_count, delta, errs, notes))

# Detalle errores por type
print('\n' + '=' * 110)
print('ULTIMOS 3 ERRORES POR TYPE (con texto del error)')
print('=' * 110)
for folder, (sql_t, mig_t, agg) in MAPPING.items():
    if err_counts.get(agg, 0) == 0:
        continue
    c_can.execute("SELECT TOP 3 error_id, event_type, LEFT(error_message, 300), created_at FROM dbo.cdc_inbox_errors WHERE aggregate_type=%s ORDER BY error_id DESC", (agg,))
    rows = c_can.fetchall()
    print(f'\n[{folder}]  ({err_counts.get(agg)} errores total)')
    for r in rows:
        print(f'  [{r[0]}] {str(r[3])[:19]}  {r[1]}: {r[2]}')

# Errores recientes en general
print('\n' + '=' * 110)
print('TOP 15 ERRORES RECIENTES (cualquier type)')
print('=' * 110)
c_can.execute("SELECT TOP 15 error_id, aggregate_type, event_type, LEFT(error_message, 280), created_at FROM dbo.cdc_inbox_errors ORDER BY error_id DESC")
for r in c_can.fetchall():
    print(f'  [{r[0]}] {str(r[4])[:19]} {r[1]}/{r[2]}: {r[3]}')

# Sumario
print('\n' + '=' * 110)
print('RESUMEN')
print('=' * 110)
ok = [r for r in results if r[3]==0 and r[4]==0]
mismatch = [r for r in results if r[3] is not None and r[3] != 0]
errs_only = [r for r in results if r[4] > 0]
sql_fail = [r for r in results if r[1] is None]
mig_fail = [r for r in results if r[2] is None]
print(f'  Types totales auditados: {len(results)}')
print(f'  OK (counts iguales, sin errores): {len(ok)}')
print(f'  Counts no coinciden canonicos vs migration: {len(mismatch)}')
print(f'  Con errores en cdc_inbox_errors: {len(errs_only)}')
print(f'  SQL canonicos: tabla no existe / fallo: {len(sql_fail)}')
print(f'  SQL migration: tabla no existe / fallo: {len(mig_fail)}')

can.close()
mig.close()
