"""Limpia y vuelve a procesar todos los flujos de participe.

Pasos:
1) STOP todos los conectores de participe (cualquier nombre con 'participe' excluyendo configuracion)
2) DELETE offsets de cada conector (requiere stopped)
3) Borrar topics normalizada.canonicos.participe.* y migration.canonicos.participe.*
4) TRUNCATE tablas destino en fcme_canonicos_normalizada.participes.*
5) TRUNCATE tablas destino en fcme_migration.FCME_USER.*
6) RESUME conectores en orden: canonicos-source -> normalizada-sink -> migration-source -> migration-sink

Endpoints:
  Kafka Connect: http://10.35.3.223:30083
  Kafka UI:      http://10.35.3.223:30180/api/clusters/fcme-kafka
"""
import json, time, urllib.request, urllib.error, sys
import pytds

KC = 'http://10.35.3.223:30083'
KU = 'http://10.35.3.223:30180/api/clusters/fcme-kafka'

def req(url, method='GET', body=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {'Content-Type':'application/json'} if body else {}
    r = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            txt = resp.read().decode()
            return resp.status, txt
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

# === 0. Listar conectores y topics ===
print('=== Listando conectores de participe ===')
_, txt = req(f'{KC}/connectors')
all_conns = json.loads(txt)
conns = sorted([c for c in all_conns if 'participe' in c.lower() and 'configuracion' not in c.lower()])
print(f'  {len(conns)} conectores')

# Categorizar por tipo
canonicos_sources = [c for c in conns if c.startswith('canonicos-')]
normalizada_sources = [c for c in conns if c.startswith('normalizada-') and 'source' in c]
normalizada_sinks   = [c for c in conns if c.startswith('normalizada-') and 'sink' in c]
migration_sources   = [c for c in conns if c.startswith('migration-') and 'source' in c]
migration_sinks     = [c for c in conns if c.startswith('migration-') and 'sink' in c]

print(f'  canonicos-source:    {len(canonicos_sources)}')
print(f'  normalizada-source:  {len(normalizada_sources)}')
print(f'  normalizada-sink:    {len(normalizada_sinks)}')
print(f'  migration-source:    {len(migration_sources)}')
print(f'  migration-sink:      {len(migration_sinks)}')

# === 1. STOP todos ===
print('\n=== Paso 1: STOP de todos los conectores ===')
stopped = []
failed_stop = []
for c in conns:
    code, txt = req(f'{KC}/connectors/{c}/stop', method='PUT')
    if 200 <= code < 300:
        stopped.append(c)
    else:
        failed_stop.append((c, code, txt[:120]))
        print(f'  FAIL stop {c}: {code} {txt[:80]}')
print(f'  stopped OK: {len(stopped)}/{len(conns)}')
if failed_stop: print(f'  failed: {len(failed_stop)}')

# Esperar que el stop se concrete
print('  esperando 15s a que el stop se concrete...')
time.sleep(15)

# === 2. DELETE offsets ===
print('\n=== Paso 2: DELETE offsets de todos los conectores ===')
ok_offset = 0
fail_offset = []
for c in conns:
    code, txt = req(f'{KC}/connectors/{c}/offsets', method='DELETE')
    if 200 <= code < 300:
        ok_offset += 1
    else:
        fail_offset.append((c, code, txt[:120]))
print(f'  offsets borrados: {ok_offset}/{len(conns)}')
if fail_offset:
    print(f'  fallaron {len(fail_offset)}:')
    for c,code,t in fail_offset[:5]: print(f'    {c}: {code} {t[:80]}')

# === 3. Borrar topics ===
print('\n=== Paso 3: Borrar topics ===')
# Listar topics de participe
_, txt = req(f'{KU}/topics?search=participe&perPage=500')
all_topics = [t['name'] for t in json.loads(txt).get('topics',[])]
target_topics = [t for t in all_topics if (t.startswith('normalizada.canonicos.participe.') or t.startswith('migration.canonicos.participe.'))]
print(f'  topics target: {len(target_topics)}')

ok_topic = 0
fail_topic = []
for t in target_topics:
    # URL encode topic name
    enc = urllib.parse.quote(t, safe='')
    code, body = req(f'{KU}/topics/{enc}', method='DELETE')
    if 200 <= code < 300:
        ok_topic += 1
    else:
        fail_topic.append((t, code, body[:120]))
print(f'  topics borrados: {ok_topic}/{len(target_topics)}')
if fail_topic:
    print(f'  fallaron {len(fail_topic)}:')
    for t,code,b in fail_topic[:5]: print(f'    {t}: {code} {b[:80]}')

# === 4. TRUNCATE tablas destino ===
print('\n=== Paso 4: TRUNCATE tablas destino ===')
print('  Conectando a fcme_canonicos_normalizada (capa.federada:31433)...')
nor = pytds.connect('capa.federada', database='fcme_canonicos_normalizada', user='sa', password='SqlServer2025!', port=31433, autocommit=True, timeout=20)
c_nor = nor.cursor()
c_nor.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('participes') ORDER BY name")
nor_tables = [r[0] for r in c_nor.fetchall()]
print(f'  tablas en normalizada.participes: {len(nor_tables)}')

ok_truncate_nor = 0
fail_truncate_nor = []
for t in nor_tables:
    try:
        c_nor.execute(f'TRUNCATE TABLE participes.[{t}]')
        ok_truncate_nor += 1
    except Exception as e:
        msg = str(e)[:100]
        try:
            c_nor.execute(f'DELETE FROM participes.[{t}]')
            ok_truncate_nor += 1
        except Exception as e2:
            fail_truncate_nor.append((t, str(e2)[:100]))
print(f'  truncated OK normalizada: {ok_truncate_nor}/{len(nor_tables)}')
if fail_truncate_nor:
    for t,m in fail_truncate_nor[:5]: print(f'    {t}: {m}')

print('  Conectando a fcme_migration (10.35.3.64:1433)...')
mig = pytds.connect('10.35.3.64', database='fcme_migration', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
c_mig = mig.cursor()
c_mig.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('FCME_USER') ORDER BY name")
mig_tables = [r[0] for r in c_mig.fetchall()]
print(f'  tablas en FCME_USER: {len(mig_tables)}')

# Solo truncar las que correspondan a tablas en normalizada (mismo nombre upper)
target_mig = []
for nt in nor_tables:
    upper = nt.upper().replace('_', '')  # normalizada usa snake, FCME_USER usa concat upper a veces
    for mt in mig_tables:
        if mt.upper() == nt.upper() or mt.upper().replace('_','') == upper:
            target_mig.append(mt); break

ok_truncate_mig = 0
fail_truncate_mig = []
for t in set(target_mig):
    try:
        c_mig.execute(f'TRUNCATE TABLE FCME_USER.[{t}]')
        ok_truncate_mig += 1
    except Exception as e:
        try:
            c_mig.execute(f'DELETE FROM FCME_USER.[{t}]')
            ok_truncate_mig += 1
        except Exception as e2:
            fail_truncate_mig.append((t, str(e2)[:100]))
print(f'  truncated OK migration: {ok_truncate_mig}/{len(set(target_mig))}')
if fail_truncate_mig:
    for t,m in fail_truncate_mig[:5]: print(f'    {t}: {m}')

# === 5. RESUME conectores en orden ===
print('\n=== Paso 5: RESUME conectores en orden ===')

def resume_batch(name, batch):
    ok = 0; fail = []
    for c in batch:
        code, txt = req(f'{KC}/connectors/{c}/resume', method='PUT')
        if 200 <= code < 300:
            ok += 1
        else:
            fail.append((c, code, txt[:80]))
    print(f'  {name}: {ok}/{len(batch)} resumed')
    if fail:
        for c,code,t in fail[:3]: print(f'    FAIL {c}: {code} {t}')

resume_batch('canonicos-source', canonicos_sources)
print('  esperando 30s para que sources publiquen al topic...')
time.sleep(30)

resume_batch('normalizada-source', normalizada_sources)
resume_batch('normalizada-sink', normalizada_sinks)
print('  esperando 30s para que sinks escriban a normalizada DB...')
time.sleep(30)

resume_batch('migration-source', migration_sources)
resume_batch('migration-sink', migration_sinks)

print('\n=== Esperando 60s antes de detectar tasks FAILED...')
time.sleep(60)

# === 6. Detectar tasks FAILED y restartear (paso C) ===
print('\n=== Paso 6: Detectar sources FAILED y restartear (paso C) ===')
def get_status(c):
    try:
        return json.loads(urllib.request.urlopen(f'{KC}/connectors/{c}/status', timeout=10).read().decode(), strict=False)
    except: return None

failed_sources = []
for c in conns:
    if 'sink' in c: continue
    s = get_status(c)
    if not s: continue
    for t in s.get('tasks', []):
        if t.get('state') == 'FAILED':
            failed_sources.append((c, t.get('id', 0))); break
print(f'  sources FAILED: {len(failed_sources)}')
for c, tid in failed_sources:
    code, txt = req(f'{KC}/connectors/{c}/tasks/{tid}/restart', method='POST')
    if 200 <= code < 300:
        print(f'  restart OK: {c}')
    else:
        print(f'  restart FAIL {c}: {code} {txt[:80]}')

print('  esperando 45s a que los sources reprocesen...')
time.sleep(45)

# Segunda pasada de restart (algunos sources tardan un poco más)
failed_sources_2 = []
for c in conns:
    if 'sink' in c: continue
    s = get_status(c)
    if not s: continue
    for t in s.get('tasks', []):
        if t.get('state') == 'FAILED':
            failed_sources_2.append((c, t.get('id', 0))); break
if failed_sources_2:
    print(f'  segunda pasada de restart: {len(failed_sources_2)}')
    for c, tid in failed_sources_2:
        req(f'{KC}/connectors/{c}/tasks/{tid}/restart', method='POST')
    print('  esperando 45s...')
    time.sleep(45)

# === 7. Comparar counts ===
print('\n=== Paso 7: Comparando counts canonicos vs normalizada ===')
can = pytds.connect('10.35.3.64', database='fcme_canonicos', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
c_can = can.cursor()
c_can.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('participes') ORDER BY name")
can_tables = sorted(set(r[0] for r in c_can.fetchall()) & set(nor_tables))

print(f'\n{"tabla":<46} {"canonicos":>12} {"normalizada":>12} {"delta":>10}  status')
print('-' * 100)
results = []
for t in can_tables:
    a = b = None
    try:
        c_can.execute(f'SELECT COUNT(*) FROM participes.[{t}]')
        a = c_can.fetchone()[0]
    except: a='ERR'
    try:
        c_nor.execute(f'SELECT COUNT(*) FROM participes.[{t}]')
        b = c_nor.fetchone()[0]
    except: b='ERR'
    if isinstance(a,int) and isinstance(b,int):
        delta = a - b
        if delta == 0: status = 'OK' if a > 0 else 'AMBAS VACIAS'
        elif delta > 0: status = f'falta {delta}'
        else: status = f'sobran {-delta}'
    else:
        delta = '?'; status = 'error'
    print(f'{t:<46} {str(a):>12} {str(b):>12} {str(delta):>10}  {status}')
    results.append((t,a,b,delta,status))

ok = [r for r in results if r[4]=='OK']
empty = [r for r in results if r[4]=='AMBAS VACIAS']
falta = [r for r in results if r[4].startswith('falta')]
sobran = [r for r in results if r[4].startswith('sobran')]
print()
print(f'  OK:                        {len(ok)}')
print(f'  Ambas vacias:              {len(empty)}')
print(f'  Faltan en normalizada:     {len(falta)}')
print(f'  Sobran en normalizada:     {len(sobran)}')

can.close(); nor.close(); mig.close()
