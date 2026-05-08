"""Arregla duplicados en normalizada SIN tocar lo que ya funciona.

Plan:
1) Detectar pares de sources que comparten topic.prefix (causa duplicados).
2) Para cada par, PAUSE el normalizada-source -> dejar solo el canonicos-source publicando.
3) Para las tablas duplicadas (delta != 0 con sobran):
   - STOP normalizada-sink y canonicos-source (para reset offsets)
   - DELETE offsets de ambos
   - TRUNCATE tabla destino normalizada
   - Borrar topic data + sink.dlq
   - RESUME canonicos-source (publica desde 0)
   - Esperar
   - RESUME normalizada-sink (consume desde 0)

NO toca: tablas OK ni conectores OK.
"""
import json, time, urllib.request, urllib.error
import pytds

KC='http://10.35.3.223:30083'
KU='http://10.35.3.223:30180/api/clusters/fcme-kafka'

def req(url, method='GET', body=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {'Content-Type':'application/json'} if body else {}
    r = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return -1, str(e)

# Tablas con duplicados (de la auditoria reciente)
DUP_TABLES = [
    'agendaMailAfiliado_type',
    'areaLaboralParticipe_type',
    'auditoriaAfiliado_type',
    'cuentaBancariaAfiliado_type',
    'distribucionAfiliado_type',
    'documentacionAfiliado_type',
    'firmanteParticipe_type',
    'grupoFamiliar_type',
    'imagenesType',
    'informacionAdicionalAfiliado_type',
    'motivoContable_type',
    'movimientoTemporal_type',
    'naturalInformacionAdicionalType',
    'naturalInformacionBasicaType',
    'naturalReferenciasComercialesType',
    'otrosIngresosAfiliado_type',
    'personaDireccionesType',
    'personaReferenciasBancariasType',
    'personaTelefonosType',
    'personaType',
    'retiroLiquidacion_type',
    'retiroVoluntarioEstado_type',
    'saldoDiarioRubro_type',
]

def normalize_short(t):
    """tabla -> short (lowercase, sin _ ni Type)."""
    return t.lower().replace('_type','').replace('_','').replace('type','')

# === Listar conectores ===
print('=== Listando conectores ===')
_, txt = req(f'{KC}/connectors')
all_conns = sorted([c for c in json.loads(txt) if 'participe' in c.lower() and 'configuracion' not in c.lower()])

# Para cada DUP_TABLE: identificar conectores asociados
print('\n=== Mapeo tabla -> conectores ===')
def find_conns_for(table):
    short = normalize_short(table)
    matches = {'canonicos_src':[], 'normalizada_src':[], 'normalizada_sink':[], 'topic':None}
    for c in all_conns:
        cn = c.lower()
        if short not in cn: continue
        if c.startswith('canonicos-') and 'source' in c: matches['canonicos_src'].append(c)
        elif c.startswith('normalizada-') and 'source' in c: matches['normalizada_src'].append(c)
        elif c.startswith('normalizada-') and 'sink' in c: matches['normalizada_sink'].append(c)
    return matches

table_conns = {}
for t in DUP_TABLES:
    m = find_conns_for(t)
    table_conns[t] = m
    print(f'  {t}:')
    for k,v in m.items():
        if isinstance(v,list) and v: print(f'    {k}: {v}')

# === Paso 1: PAUSE los normalizada-source duplicados ===
print('\n=== Paso 1: PAUSE normalizada-source duplicados ===')
to_pause = set()
for t, m in table_conns.items():
    if m['canonicos_src'] and m['normalizada_src']:
        for ns in m['normalizada_src']:
            to_pause.add(ns)
print(f'  normalizada-source a pausar: {len(to_pause)}')
for c in sorted(to_pause):
    code,_ = req(f'{KC}/connectors/{c}/pause', method='PUT')
    print(f'    pause {c}: {code}')

print('  esperando 10s...')
time.sleep(10)

# === Paso 2: Para cada tabla duplicada, STOP los conectores afectados ===
print('\n=== Paso 2: STOP canonicos-source + normalizada-sink afectados ===')
to_stop = set()
for t, m in table_conns.items():
    for c in m['canonicos_src']: to_stop.add(c)
    for c in m['normalizada_sink']: to_stop.add(c)
print(f'  conectores a stop: {len(to_stop)}')
for c in sorted(to_stop):
    code,_ = req(f'{KC}/connectors/{c}/stop', method='PUT')
print('  esperando 15s para que el stop concrete...')
time.sleep(15)

# === Paso 3: DELETE offsets ===
print('\n=== Paso 3: DELETE offsets ===')
for c in sorted(to_stop):
    code,_ = req(f'{KC}/connectors/{c}/offsets', method='DELETE')
    if not (200 <= code < 300):
        print(f'  fail offsets {c}: {code}')
print(f'  done: {len(to_stop)}')

# === Paso 4: TRUNCATE tablas afectadas ===
print('\n=== Paso 4: TRUNCATE 23 tablas en normalizada ===')
nor = pytds.connect('capa.federada', database='fcme_canonicos_normalizada', user='sa', password='SqlServer2025!', port=31433, autocommit=True, timeout=20)
c_nor = nor.cursor()
ok_t = 0
for t in DUP_TABLES:
    try:
        c_nor.execute(f'TRUNCATE TABLE participes.[{t}]')
        ok_t += 1
    except Exception as e:
        try:
            c_nor.execute(f'DELETE FROM participes.[{t}]')
            ok_t += 1
        except Exception as e2:
            print(f'  fail truncate {t}: {str(e2)[:80]}')
print(f'  truncated: {ok_t}/{len(DUP_TABLES)}')

# === Paso 5: Borrar topics afectados ===
print('\n=== Paso 5: Borrar topics asociados ===')
import urllib.parse
topics_to_delete = set()
for t in DUP_TABLES:
    short = normalize_short(t)
    topics_to_delete.add(f'normalizada.canonicos.participe.{short}')
    topics_to_delete.add(f'normalizada.canonicos.participe.{short}.sink.dlq')
    topics_to_delete.add(f'normalizada.canonicos.participe.{short}.dlq')
# verificar cuales existen
_, txt = req(f'{KU}/topics?search=normalizada.canonicos.participe&perPage=300')
existing = set(t['name'] for t in json.loads(txt).get('topics',[]))
to_del = sorted(topics_to_delete & existing)
print(f'  topics a borrar: {len(to_del)}')
ok_d = 0
for t in to_del:
    enc = urllib.parse.quote(t, safe='')
    code, _ = req(f'{KU}/topics/{enc}', method='DELETE')
    if 200 <= code < 300: ok_d += 1
    else: print(f'  fail {t}: {code}')
print(f'  borrados: {ok_d}/{len(to_del)}')

# === Paso 6: RESUME escalonado ===
print('\n=== Paso 6: RESUME escalonado ===')
canonicos_to_resume = sorted([c for c in to_stop if c.startswith('canonicos-')])
sinks_to_resume = sorted([c for c in to_stop if 'sink' in c])

print(f'  canonicos-source ({len(canonicos_to_resume)}) en grupos de 5...')
ok_r = 0
for i in range(0, len(canonicos_to_resume), 5):
    chunk = canonicos_to_resume[i:i+5]
    for c in chunk:
        code,_ = req(f'{KC}/connectors/{c}/resume', method='PUT')
        if 200 <= code < 300: ok_r += 1
    if i+5 < len(canonicos_to_resume): time.sleep(15)
print(f'    canonicos resumed: {ok_r}/{len(canonicos_to_resume)}')

print('  esperando 60s para que sources publiquen...')
time.sleep(60)

print(f'  normalizada-sink ({len(sinks_to_resume)}) en grupos de 8...')
ok_s = 0
for i in range(0, len(sinks_to_resume), 8):
    chunk = sinks_to_resume[i:i+8]
    for c in chunk:
        code,_ = req(f'{KC}/connectors/{c}/resume', method='PUT')
        if 200 <= code < 300: ok_s += 1
    if i+8 < len(sinks_to_resume): time.sleep(10)
print(f'    sinks resumed: {ok_s}/{len(sinks_to_resume)}')

print('\n  esperando 90s para procesamiento...')
time.sleep(90)

# Restart sources FAILED (paso C en miniatura)
def get_status(c):
    try: return json.loads(urllib.request.urlopen(f'{KC}/connectors/{c}/status', timeout=10).read().decode(), strict=False)
    except: return None

failed = []
for c in canonicos_to_resume:
    s = get_status(c)
    if not s: continue
    for tt in s.get('tasks',[]):
        if tt.get('state')=='FAILED': failed.append((c, tt.get('id',0))); break
print(f'\n  sources FAILED tras resume: {len(failed)}')
for c, tid in failed:
    req(f'{KC}/connectors/{c}/tasks/{tid}/restart', method='POST')
if failed:
    time.sleep(45)

# === Paso 7: Comparar counts ===
print('\n=== Paso 7: Comparar counts solo de tablas afectadas ===')
can = pytds.connect('10.35.3.64', database='fcme_canonicos', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
c_can = can.cursor()

print(f'\n{"tabla":<46} {"canonicos":>10} {"normalizada":>12} {"delta":>10}  status')
print('-'*92)
results=[]
for t in DUP_TABLES:
    a=b=None
    try: c_can.execute(f'SELECT COUNT(*) FROM participes.[{t}]'); a=c_can.fetchone()[0]
    except: a='ERR'
    try: c_nor.execute(f'SELECT COUNT(*) FROM participes.[{t}]'); b=c_nor.fetchone()[0]
    except: b='ERR'
    if isinstance(a,int) and isinstance(b,int):
        delta = a-b
        if delta == 0: status = 'OK' if a > 0 else 'vacios'
        elif delta > 0: status = f'falta {delta}'
        else: status = f'sobran {-delta}'
    else: delta='?'; status='err'
    print(f'{t:<46} {str(a):>10} {str(b):>12} {str(delta):>10}  {status}')
    results.append((t,a,b,delta,status))

ok = [r for r in results if r[4]=='OK']
falta = [r for r in results if str(r[4]).startswith('falta')]
sobran = [r for r in results if str(r[4]).startswith('sobran')]
print(f'\n  OK:     {len(ok)}/{len(DUP_TABLES)}')
print(f'  faltan: {len(falta)}')
print(f'  sobran: {len(sobran)}')
