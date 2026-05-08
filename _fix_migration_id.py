"""Fix migration sinks que incluyen 'id' en fields.whitelist.
- Quita 'id' del whitelist
- STOP + reset offsets + truncate destino + resume

NO toca los sinks que ya estan OK.
"""
import json, urllib.request, urllib.parse, time
import pytds

KC = 'http://10.35.3.223:30083'
KU = 'http://10.35.3.223:30180/api/clusters/fcme-kafka'

def req(url, method='GET', body=None):
    data = json.dumps(body).encode() if body else None
    headers = {'Content-Type': 'application/json'} if body else {}
    r = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(r, timeout=20) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

# Tablas que estan OK (no tocar)
OK_LABELS = {
    'cuentabancariaafiliado','firmanteparticipe','grupofamiliar','movimientocuenta',
    'naturalingresosegresos','naturaltrabajo','personareferenciaspersonales','personavinculaciones',
}

all_conns = json.loads(urllib.request.urlopen(f'{KC}/connectors').read().decode())
migration_sinks = sorted([c for c in all_conns if 'sink' in c and 'migration' in c.lower() and 'participe' in c.lower() and 'configuracion' not in c.lower()])

# 1. Identificar sinks con 'id' en whitelist (excluyendo OK)
to_fix = []
for sink in migration_sinks:
    label = sink.replace('migration-', '').replace('-jdbc-sink', '').replace('canonicos-participe-','').replace('participe-','').lower()
    is_ok = any(ok in label for ok in OK_LABELS)
    if is_ok: continue
    cfg = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/config', timeout=10).read().decode(), strict=False)
    fields = cfg.get('fields.whitelist','').split(',')
    has_id = 'id' in fields
    table_target = cfg.get('table.name.format','').split('.')[-1]
    topic = cfg.get('topics','')
    if has_id:
        to_fix.append((sink, fields, table_target, topic, cfg))

print(f'\nSinks a arreglar (con id en whitelist): {len(to_fix)}')
for s, _, t, _, _ in to_fix: print(f'  {s} -> {t}')

# 2. Para cada uno: quitar 'id', PUT, stop, reset offsets, truncate, resume
mig = pytds.connect('10.35.3.64', database='fcme_migration', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
c_mig = mig.cursor()

print('\n=== Aplicando fix a cada sink ===')
for sink, fields, target, topic, cfg in to_fix:
    new_fields = [f for f in fields if f != 'id']
    cfg.pop('name', None)
    cfg['fields.whitelist'] = ','.join(new_fields)

    code, _ = req(f'{KC}/connectors/{sink}/config', method='PUT', body=cfg)
    print(f'  {sink}')
    print(f'    PUT config: {code} (fields {len(fields)}->{len(new_fields)})')

# 3. STOP y reset offsets en lote
print('\n=== STOP sinks ===')
for sink, _, _, _, _ in to_fix:
    req(f'{KC}/connectors/{sink}/stop', method='PUT')
time.sleep(15)

print('\n=== DELETE offsets ===')
for sink, _, _, _, _ in to_fix:
    req(f'{KC}/connectors/{sink}/offsets', method='DELETE')

# 4. TRUNCATE tablas destino
print('\n=== TRUNCATE tablas migration destino ===')
ok_t = 0
for sink, _, target, _, _ in to_fix:
    try:
        c_mig.execute(f'TRUNCATE TABLE FCME_USER.[{target}]')
        ok_t += 1
    except Exception as e:
        try:
            c_mig.execute(f'DELETE FROM FCME_USER.[{target}]')
            ok_t += 1
        except Exception as e2:
            print(f'  fail {target}: {str(e2)[:80]}')
print(f'  truncated: {ok_t}/{len(to_fix)}')

# 5. RESUME en grupos de 5
print('\n=== RESUME escalonado ===')
ok_r = 0
for i in range(0, len(to_fix), 5):
    chunk = to_fix[i:i+5]
    for sink, _, _, _, _ in chunk:
        code, _ = req(f'{KC}/connectors/{sink}/resume', method='PUT')
        if 200 <= code < 300: ok_r += 1
    if i+5 < len(to_fix): time.sleep(8)
print(f'  resumed: {ok_r}/{len(to_fix)}')

print('\n  esperando 90s para procesamiento...')
time.sleep(90)

# 6. Verificar
print('\n=== Counts despues del fix ===')
nor = pytds.connect('capa.federada', database='fcme_canonicos_normalizada', user='sa', password='SqlServer2025!', port=31433, autocommit=True, timeout=20)
c_nor = nor.cursor()
c_nor.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('participes') ORDER BY name")
nor_tables = [r[0] for r in c_nor.fetchall()]

def short_to_nor(short):
    s_norm = short.lower().replace('_','').replace('type','')
    for t in nor_tables:
        if t.lower().replace('_','').replace('type','') == s_norm: return t
    return None

print(f'\n{"sink":<55} {"nor":>8} {"mig":>8} {"delta":>8}  status')
print('-'*85)
for sink, _, target, topic, _ in to_fix:
    short = topic.split('.')[-1] if topic else ''
    nor_t = short_to_nor(short)
    a=b=None
    if nor_t:
        try: c_nor.execute(f'SELECT COUNT(*) FROM participes.[{nor_t}]'); a=c_nor.fetchone()[0]
        except: a='ERR'
    try: c_mig.execute(f'SELECT COUNT(*) FROM FCME_USER.[{target}]'); b=c_mig.fetchone()[0]
    except: b='ERR'
    if isinstance(a,int) and isinstance(b,int):
        d=a-b
        status='OK' if d==0 and a>0 else ('vacios' if d==0 else (f'falta {d}' if d>0 else f'sobran {-d}'))
    else: d='?'; status='err'
    label = sink.replace('migration-','').replace('-jdbc-sink','')
    print(f'{label:<55} {str(a):>8} {str(b):>8} {str(d):>8}  {status}')
