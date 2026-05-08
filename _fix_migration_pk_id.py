"""Fix sinks de migration con pk.fields=id (causa identity column error).
Cambia a insert.mode=insert + pk.mode=none + saca id de whitelist.
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

# 1. Identificar todos los sinks con pk.fields=id
all_conns = json.loads(urllib.request.urlopen(f'{KC}/connectors').read().decode())
migration_sinks = sorted([c for c in all_conns if 'sink' in c and 'migration' in c.lower() and 'participe' in c.lower() and 'configuracion' not in c.lower()])

to_fix = []
for sink in migration_sinks:
    cfg = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/config', timeout=10).read().decode(), strict=False)
    if cfg.get('pk.fields','') == 'id':
        target = cfg.get('table.name.format','').split('.')[-1]
        to_fix.append((sink, target, cfg))

print(f'Sinks con pk.fields=id: {len(to_fix)}')
for s, t, _ in to_fix:
    print(f'  {s} -> {t}')

# 2. Aplicar fix
print('\n=== Aplicando fix ===')
for sink, target, cfg in to_fix:
    cfg.pop('name', None)
    cfg['insert.mode'] = 'insert'
    cfg['pk.mode'] = 'none'
    # Quitar id del whitelist por las dudas
    fields = [f for f in cfg.get('fields.whitelist','').split(',') if f != 'id']
    cfg['fields.whitelist'] = ','.join(fields)
    # Quitar pk.fields tambien (no aplica con pk.mode=none)
    cfg.pop('pk.fields', None)
    code, _ = req(f'{KC}/connectors/{sink}/config', method='PUT', body=cfg)
    print(f'  {sink}: PUT {code}')

# 3. STOP + reset offsets
print('\n=== STOP + reset offsets ===')
for sink, _, _ in to_fix:
    req(f'{KC}/connectors/{sink}/stop', method='PUT')
time.sleep(15)
for sink, _, _ in to_fix:
    req(f'{KC}/connectors/{sink}/offsets', method='DELETE')

# 4. TRUNCATE destinos
print('\n=== TRUNCATE destinos ===')
mig = pytds.connect('10.35.3.64', database='fcme_migration', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
c_mig = mig.cursor()
ok_t = 0
for sink, target, _ in to_fix:
    try:
        c_mig.execute(f'TRUNCATE TABLE FCME_USER.[{target}]')
        ok_t += 1
    except Exception:
        try:
            c_mig.execute(f'DELETE FROM FCME_USER.[{target}]')
            ok_t += 1
        except Exception as e:
            print(f'  fail {target}: {str(e)[:80]}')
print(f'  truncated: {ok_t}/{len(to_fix)}')

# 5. RESUME escalonado
print('\n=== RESUME ===')
ok_r = 0
for i in range(0, len(to_fix), 5):
    chunk = to_fix[i:i+5]
    for sink, _, _ in chunk:
        code, _ = req(f'{KC}/connectors/{sink}/resume', method='PUT')
        if 200 <= code < 300: ok_r += 1
    if i+5 < len(to_fix): time.sleep(8)
print(f'  resumed: {ok_r}/{len(to_fix)}')

print('\n  esperando 90s...')
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

for sink, target, _ in to_fix:
    cfg = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/config', timeout=10).read().decode(), strict=False)
    topic = cfg.get('topics','')
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
    print(f'  {label:<55} nor={a} mig={b} delta={d}  {status}')
