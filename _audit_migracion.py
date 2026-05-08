"""Audita migracion: fcme_canonicos_normalizada.participes.* -> fcme_migration.FCME_USER.*

Usa el table.name.format de cada migration-sink para saber a que tabla apunta.
"""
import json, urllib.request
import pytds

KC = 'http://10.35.3.223:30083'

# 1. Listar todos los migration-sink y leer sus targets
all_conns = json.loads(urllib.request.urlopen(f'{KC}/connectors').read().decode())
migration_sinks = sorted([c for c in all_conns if 'sink' in c and 'migration' in c.lower() and 'participe' in c.lower() and 'configuracion' not in c.lower()])

table_map = {}  # nor_table -> (mig_table, sink_name, status)
for sink in migration_sinks:
    try:
        cfg = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/config', timeout=10).read().decode(), strict=False)
        topic = cfg.get('topics', '')
        target = cfg.get('table.name.format', '')
        # target ej: fcme_migration.FCME_USER.AGENDAMAILAFILIADO_TYPE
        parts = target.split('.')
        mig_table = parts[-1] if len(parts) >= 3 else None

        # Status
        s = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/status', timeout=10).read().decode(), strict=False)
        states = [t.get('state', '?') for t in s.get('tasks', [])]
        st = ','.join(states) if states else 'no-task'

        table_map[sink] = {'topic': topic, 'mig_table': mig_table, 'status': st}
    except Exception as e:
        table_map[sink] = {'error': str(e)[:100]}

print('=' * 90)
print(f'MIGRATION SINKS: {len(migration_sinks)}')
print('=' * 90)
for sink, info in table_map.items():
    print(f'  {sink}')
    print(f'    -> {info.get("mig_table","?")}    status: {info.get("status","?")}')

# 2. Mapear topics a tablas en normalizada (a partir del topic name)
# topic ej: migration.canonicos.participe.agendamailafiliado -> normalizada table = agendaMailAfiliado_type
nor = pytds.connect('capa.federada', database='fcme_canonicos_normalizada', user='sa', password='SqlServer2025!', port=31433, autocommit=True, timeout=20)
c_nor = nor.cursor()
c_nor.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('participes') ORDER BY name")
nor_tables = [r[0] for r in c_nor.fetchall()]

def short_to_nor(short):
    """Find normalizada table whose lowercase concatenated name matches short."""
    for t in nor_tables:
        if t.lower().replace('_', '').replace('type', '') == short.lower().replace('type', ''):
            return t
    return None

# Conectar a migration
mig = pytds.connect('10.35.3.64', database='fcme_migration', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
c_mig = mig.cursor()
c_mig.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('FCME_USER')")
mig_tables = {r[0] for r in c_mig.fetchall()}

# 3. Audit por cada sink: nor table count vs mig table count
print('\n' + '=' * 100)
print('AUDIT MIGRACION: normalizada -> migration')
print('=' * 100)
print(f'{"#":>3}  {"sink":<55} {"nor":>8} {"mig":>8} {"delta":>8}  {"status":<20}')
print('-' * 100)
results = []
for i, (sink, info) in enumerate(table_map.items(), 1):
    if 'error' in info:
        print(f'{i:>3}  {sink:<55} ERROR {info["error"][:40]}')
        continue
    mig_t = info.get('mig_table')
    topic = info.get('topic', '')
    # Derive nor_table from topic (e.g. migration.canonicos.participe.agendamailafiliado)
    short = topic.split('.')[-1] if topic else ''
    nor_t = short_to_nor(short)

    a = b = None
    if nor_t:
        try:
            c_nor.execute(f'SELECT COUNT(*) FROM participes.[{nor_t}]')
            a = c_nor.fetchone()[0]
        except Exception as e: a = f'ERR'
    if mig_t and mig_t in mig_tables:
        try:
            c_mig.execute(f'SELECT COUNT(*) FROM FCME_USER.[{mig_t}]')
            b = c_mig.fetchone()[0]
        except: b = 'ERR'
    elif mig_t:
        b = 'NOT EXIST'

    if isinstance(a,int) and isinstance(b,int):
        d = a - b
        if d == 0: status = 'OK' if a > 0 else 'vacios'
        elif d > 0: status = f'falta {d}'
        else: status = f'sobran {-d}'
    else:
        d = '?'
        status = 'err'

    label = sink.replace('migration-', '').replace('-jdbc-sink', '')
    print(f'{i:>3}  {label:<55} {str(a):>8} {str(b):>8} {str(d):>8}  {status:<20}  task={info["status"]}')
    results.append((sink, nor_t, mig_t, a, b, d, status, info['status']))

# Resumen
print('\n' + '=' * 100)
ok = [r for r in results if r[6] == 'OK']
empty = [r for r in results if r[6] == 'vacios']
falta = [r for r in results if str(r[6]).startswith('falta')]
sobran = [r for r in results if str(r[6]).startswith('sobran')]
errs = [r for r in results if r[6] == 'err']
failed_tasks = [r for r in results if 'FAILED' in r[7]]
running_tasks = [r for r in results if 'RUNNING' in r[7] and 'FAILED' not in r[7]]
print(f'  Total sinks de migration:   {len(results)}')
print(f'  OK iguales (con datos):     {len(ok)}')
print(f'  Vacios:                     {len(empty)}')
print(f'  Faltan datos:               {len(falta)}')
print(f'  Sobran (duplicados):        {len(sobran)}')
print(f'  Errores de query:           {len(errs)}')
print(f'  Tasks RUNNING:              {len(running_tasks)}')
print(f'  Tasks FAILED:               {len(failed_tasks)}')

print('\n  Faltan datos en migration:')
for r in falta: print(f'    {r[0].replace("migration-",""):<55} nor={r[3]} mig={r[4]} delta={r[5]}')
print('\n  Sobran en migration:')
for r in sobran: print(f'    {r[0].replace("migration-",""):<55} nor={r[3]} mig={r[4]} delta={-r[5]}')
print('\n  Tasks FAILED:')
for r in failed_tasks: print(f'    {r[0]}')
