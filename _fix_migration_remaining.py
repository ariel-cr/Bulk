"""Para los migration sinks/sources con delta != 0:
limpiar topic + truncar destino + reset offsets + resume.
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

# Lista de sinks problematicos a limpiar (basado en audit reciente)
TARGETS = [
    'migration-canonicos-participe-naturalinformacionadicional-jdbc-sink',
    'migration-canonicos-participe-personareferenciasbancarias-jdbc-sink',
    'migration-participe-agendamailafiliado-jdbc-sink',
    'migration-participe-auditoriaafiliado-jdbc-sink',
    'migration-participe-beneficiarioParticipe-jdbc-sink',
    'migration-participe-documentacionafiliado-jdbc-sink',
    'migration-participe-imagenes-jdbc-sink',
    'migration-participe-informacionadicionalafiliado-jdbc-sink',
    'migration-participe-naturalinformacionbasica-jdbc-sink',
    'migration-participe-otrosingresosafiliado-jdbc-sink',
    'migration-participe-personadirecciones-jdbc-sink',
    'migration-participe-reportesibs-jdbc-sink',
    'migration-participe-retiroliquidacion-jdbc-sink',
    'migration-participe-rolnomina-jdbc-sink',
    'migration-participe-saldodiariorubro-jdbc-sink',
    'migration-participe-servicioadicional-jdbc-sink',
]

# Para cada sink, encontrar source asociado, topic y tabla destino
items = []
for sink in TARGETS:
    cfg = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/config', timeout=10).read().decode(), strict=False)
    topic = cfg.get('topics', '')
    target = cfg.get('table.name.format','').split('.')[-1]
    src = sink.replace('-jdbc-sink', '-jdbc-source')
    items.append({'sink': sink, 'src': src, 'topic': topic, 'target': target})

print(f'Total a procesar: {len(items)}')

# 1. STOP todos
print('\n=== STOP ===')
for it in items:
    req(f'{KC}/connectors/{it["src"]}/stop', method='PUT')
    req(f'{KC}/connectors/{it["sink"]}/stop', method='PUT')
time.sleep(15)

# 2. DELETE offsets
print('\n=== DELETE offsets ===')
for it in items:
    req(f'{KC}/connectors/{it["src"]}/offsets', method='DELETE')
    req(f'{KC}/connectors/{it["sink"]}/offsets', method='DELETE')

# 3. Borrar topics
print('\n=== Borrar topics ===')
ok_t = 0
for it in items:
    for suf in ['', '.sink.dlq', '.dlq']:
        t = f'{it["topic"]}{suf}'
        code, _ = req(f'{KU}/topics/{urllib.parse.quote(t, safe="")}', method='DELETE')
        if 200 <= code < 300: ok_t += 1
print(f'  borrados: {ok_t}')

# 4. TRUNCATE tablas destino
print('\n=== TRUNCATE migration ===')
mig = pytds.connect('10.35.3.64', database='fcme_migration', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
c_mig = mig.cursor()
ok_tr = 0
for it in items:
    if it['target']:
        try:
            c_mig.execute(f'TRUNCATE TABLE FCME_USER.[{it["target"]}]')
            ok_tr += 1
        except Exception:
            try: c_mig.execute(f'DELETE FROM FCME_USER.[{it["target"]}]'); ok_tr += 1
            except Exception as e: print(f'  fail {it["target"]}: {str(e)[:80]}')
print(f'  truncated: {ok_tr}/{len(items)}')

# 5. Resume sources en grupos de 4
print('\n=== Resume sources (escalonado) ===')
for i in range(0, len(items), 4):
    chunk = items[i:i+4]
    for it in chunk:
        req(f'{KC}/connectors/{it["src"]}/resume', method='PUT')
    if i + 4 < len(items): time.sleep(10)
print('  esperando 60s para que publiquen...')
time.sleep(60)

# 6. Resume sinks en grupos de 4
print('\n=== Resume sinks (escalonado) ===')
for i in range(0, len(items), 4):
    chunk = items[i:i+4]
    for it in chunk:
        req(f'{KC}/connectors/{it["sink"]}/resume', method='PUT')
    if i + 4 < len(items): time.sleep(10)
print('  esperando 90s...')
time.sleep(90)

# 7. Restart sources con bulk que necesitan poll inicial
print('\n=== Restart connector-level de sources ===')
for it in items:
    req(f'{KC}/connectors/{it["src"]}/restart?includeTasks=true&onlyFailed=false', method='POST')
print('  esperando 90s...')
time.sleep(90)

# 8. Verificar
print('\n=== Counts ===')
nor = pytds.connect('capa.federada', database='fcme_canonicos_normalizada', user='sa', password='SqlServer2025!', port=31433, autocommit=True, timeout=20)
c_nor = nor.cursor()
c_nor.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('participes') ORDER BY name")
nor_tables = [r[0] for r in c_nor.fetchall()]
def short_to_nor(short):
    s_norm = short.lower().replace('_','').replace('type','')
    for t in nor_tables:
        if t.lower().replace('_','').replace('type','') == s_norm: return t
    return None

for it in items:
    short = it['topic'].split('.')[-1]
    nor_t = short_to_nor(short)
    a=b=None
    if nor_t:
        try: c_nor.execute(f'SELECT COUNT(*) FROM participes.[{nor_t}]'); a=c_nor.fetchone()[0]
        except: a='ERR'
    try: c_mig.execute(f'SELECT COUNT(*) FROM FCME_USER.[{it["target"]}]'); b=c_mig.fetchone()[0]
    except: b='ERR'
    if isinstance(a,int) and isinstance(b,int):
        d=a-b
        status='OK' if d==0 and a>0 else ('vacios' if d==0 else (f'falta {d}' if d>0 else f'sobran {-d}'))
    else: d='?'; status='err'
    label = it['sink'].replace('migration-','').replace('-jdbc-sink','')
    print(f'  {label:<55} nor={str(a):>6} mig={str(b):>6} delta={str(d):>6}  {status}')
