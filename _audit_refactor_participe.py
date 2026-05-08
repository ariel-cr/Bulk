"""Auditoria de los types en REFACTOR_PARTICIPE.

Para cada folder *_type / imagenesType:
  - Lee el JSON canonicos-source para obtener tabla y aggregate_type
  - Cuenta filas en fcme_canonicos.participes.<table>
  - Cuenta filas en Oracle FCME_USER.<TYPE>
  - Lee cdc_inbox_errors / cdc_inbox_parsed filtrados por aggregate_type
  - Reporta delta y errores

NO modifica nada. Solo SELECT.
"""
import os, re, json, sys
import pytds
import oracledb

ROOT = '/Users/dennise/Desktop/CAPA/cdc/REFACTOR_PARTICIPE'

def folder_to_table(folder):
    p = os.path.join(ROOT, folder)
    candidates = [f for f in os.listdir(p) if 'source' in f and 'jdbc' in f and f.endswith('.json')]
    pref_order = ['normalizada-', 'canonicos-']
    candidates.sort(key=lambda f: next((i for i,p in enumerate(pref_order) if f.startswith(p)), 99))
    for f in candidates:
        cfg = json.load(open(os.path.join(p, f)))['config']
        q = cfg.get('query', '')
        m = re.search(r'FROM\s+([\w.]+)', q, re.I)
        if m:
            return m.group(1), cfg.get('topic.prefix'), f
    return None, None, None

def folder_to_oracle_table(folder):
    base = folder.replace('_type', '')
    if folder == 'imagenesType':
        return 'IMAGENES_TYPE'
    return base.upper() + '_TYPE'

def folder_to_aggregate_type(folder):
    if folder == 'imagenesType':
        return 'imagenesType'
    base = folder.replace('_type', '')
    suffix_map = {
        'beneficiario': 'beneficiarioParticipeType',
        'firmanteParticipe': 'firmanteParticipeType',
    }
    if base in suffix_map:
        return suffix_map[base]
    return base + 'Type'

print('=' * 90)
print('AUDITORIA REFACTOR_PARTICIPE - normalizada y migracion')
print('=' * 90)

folders = sorted([f for f in os.listdir(ROOT) if f.endswith('_type') or f == 'imagenesType'])
print(f'\n{len(folders)} types detectados\n')

c_can = pytds.connect('10.35.3.64', database='fcme_canonicos', user='sa', password='YourPassword123', port=1433, autocommit=True).cursor()
o_conn = oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co = o_conn.cursor()

results = []

for folder in folders:
    sql_table, topic, src_file = folder_to_table(folder)
    ora_table = folder_to_oracle_table(folder)
    agg_type = folder_to_aggregate_type(folder)

    row = {'folder': folder, 'sql_table': sql_table, 'ora_table': ora_table,
           'agg_type': agg_type, 'topic': topic,
           'sql_count': None, 'ora_count': None, 'errors': 0, 'parsed': 0,
           'last_error': None, 'sql_err': None, 'ora_err': None}

    try:
        c_can.execute(f'SELECT COUNT(*) FROM {sql_table}')
        row['sql_count'] = c_can.fetchone()[0]
    except Exception as e:
        row['sql_err'] = str(e)[:150]

    try:
        co.execute(f'SELECT COUNT(*) FROM FCME_USER.{ora_table}')
        row['ora_count'] = co.fetchone()[0]
    except Exception as e:
        row['ora_err'] = str(e)[:150]

    try:
        c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE aggregate_type=%s", (agg_type,))
        row['errors'] = c_can.fetchone()[0]
        if row['errors'] > 0:
            c_can.execute("SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE aggregate_type=%s ORDER BY id DESC", (agg_type,))
            r = c_can.fetchone()
            if r:
                row['last_error'] = r[0][:200]
    except Exception as e:
        row['errors_err'] = str(e)[:150]

    try:
        c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_parsed WHERE aggregate_type=%s", (agg_type,))
        row['parsed'] = c_can.fetchone()[0]
    except: pass

    results.append(row)

# Print table
print(f'{"folder":<38} {"SQL canonicos":>15} {"Oracle migration":>17} {"delta":>10} {"parsed":>8} {"errores":>8}  status')
print('-' * 130)
for r in results:
    sql_c = r['sql_count'] if r['sql_count'] is not None else 'ERR'
    ora_c = r['ora_count'] if r['ora_count'] is not None else 'ERR'
    delta = (r['sql_count'] - r['ora_count']) if (r['sql_count'] is not None and r['ora_count'] is not None) else '?'
    status = []
    if r['sql_err']: status.append(f'sql:{r["sql_err"][:50]}')
    if r['ora_err']: status.append(f'ora:{r["ora_err"][:50]}')
    if r['errors']: status.append(f'{r["errors"]} errores')
    if isinstance(delta, int) and delta > 0: status.append(f'falta migrar {delta}')
    if isinstance(delta, int) and delta < 0: status.append(f'sobran en oracle {-delta}')
    print(f'{r["folder"]:<38} {str(sql_c):>15} {str(ora_c):>17} {str(delta):>10} {str(r["parsed"]):>8} {str(r["errors"]):>8}  {" | ".join(status)}')

# Errors detail
print('\n' + '=' * 90)
print('ULTIMOS ERRORES POR TYPE (cdc_inbox_errors)')
print('=' * 90)
for r in results:
    if r['errors']:
        print(f'\n[{r["folder"]}]  total errores: {r["errors"]}')
        print(f'  ultimo: {r["last_error"]}')

# Errors sin agg_type matching
print('\n' + '=' * 90)
print('TOP errores recientes (cualquier aggregate_type) - ultimos 10')
print('=' * 90)
c_can.execute("""SELECT TOP 10 aggregate_type, event_type, LEFT(error_message, 250), id
                 FROM dbo.cdc_inbox_errors ORDER BY id DESC""")
for r in c_can.fetchall():
    print(f'  [{r[3]}] {r[0]} {r[1]}: {r[2]}')

# Sumario
print('\n' + '=' * 90)
print('RESUMEN')
print('=' * 90)
ok = [r for r in results if r['sql_count'] is not None and r['ora_count'] is not None and r['errors']==0 and r['sql_count']==r['ora_count']]
mismatch = [r for r in results if r['sql_count'] is not None and r['ora_count'] is not None and r['sql_count']!=r['ora_count']]
errs = [r for r in results if r['errors']>0]
sql_missing = [r for r in results if r['sql_err']]
ora_missing = [r for r in results if r['ora_err']]

print(f'  OK (counts iguales, sin errores): {len(ok)}')
print(f'  Mismatch counts SQL vs Oracle: {len(mismatch)}')
print(f'  Con errores en cdc_inbox_errors: {len(errs)}')
print(f'  Tabla SQL no existe / inaccesible: {len(sql_missing)}')
print(f'  Tabla Oracle no existe / inaccesible: {len(ora_missing)}')
