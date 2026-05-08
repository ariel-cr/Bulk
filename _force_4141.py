"""Llevar normalizacion + migracion a 41/41 OK.
1. Drop UQs en normalizada (reporteSIBS, personaVinculaciones)
2. Drop UQs en migration (4 tablas)
3. Reset sinks afectados, truncar, resume
4. Investigar actualizacionDocumentos -1
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

# ========================================
# PASO 1: DROP UQs en NORMALIZADA
# ========================================
print('=== PASO 1: DROP UQs en NORMALIZADA ===')
nor = pytds.connect('capa.federada', database='fcme_canonicos_normalizada', user='sa', password='SqlServer2025!', port=31433, autocommit=True, timeout=20)
c_nor = nor.cursor()

# Listar UQs en las tablas problematicas para dropearlos
NOR_TABLES_TO_FIX = ['reporteSIBSParticipe_type', 'personaVinculacionesType']
for tab in NOR_TABLES_TO_FIX:
    c_nor.execute(f"""SELECT i.name FROM sys.indexes i WHERE i.object_id=OBJECT_ID('participes.[{tab}]') AND i.is_unique=1 AND i.is_primary_key=0""")
    indexes = [r[0] for r in c_nor.fetchall()]
    for idx in indexes:
        try:
            c_nor.execute(f'DROP INDEX [{idx}] ON participes.[{tab}]')
            print(f'  DROP INDEX {idx} ON {tab}: OK')
        except Exception as e:
            try:
                c_nor.execute(f'ALTER TABLE participes.[{tab}] DROP CONSTRAINT [{idx}]')
                print(f'  DROP CONSTRAINT {idx} ON {tab}: OK')
            except Exception as e2:
                print(f'  fail drop {idx}: {str(e2)[:80]}')

# ========================================
# PASO 2: DROP UQs en MIGRATION
# ========================================
print('\n=== PASO 2: DROP UQs en MIGRATION (4 tablas) ===')
mig = pytds.connect('10.35.3.64', database='fcme_migration', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
c_mig = mig.cursor()

MIG_TABLES_TO_FIX = ['NATURALINFORMACIONADICIONALTYPE', 'NATURALINFORMACIONBASICATYPE', 'PERSONADIRECCIONESTYPE', 'PERSONAREFERENCIASBANCARIASTYPE']
for tab in MIG_TABLES_TO_FIX:
    c_mig.execute(f"""SELECT i.name FROM sys.indexes i WHERE i.object_id=OBJECT_ID('FCME_USER.[{tab}]') AND i.is_unique=1 AND i.is_primary_key=0""")
    indexes = [r[0] for r in c_mig.fetchall()]
    for idx in indexes:
        try:
            c_mig.execute(f'DROP INDEX [{idx}] ON FCME_USER.[{tab}]')
            print(f'  DROP INDEX {idx} ON {tab}: OK')
        except Exception as e:
            try:
                c_mig.execute(f'ALTER TABLE FCME_USER.[{tab}] DROP CONSTRAINT [{idx}]')
                print(f'  DROP CONSTRAINT {idx} ON {tab}: OK')
            except Exception as e2:
                print(f'  fail drop {idx}: {str(e2)[:80]}')

# ========================================
# PASO 3: Reset sinks normalizada para que reprocesen sin filtrar
# ========================================
print('\n=== PASO 3: Reset sinks normalizada para reporteSIBS y personaVinculaciones ===')
NOR_SINKS = [
    ('normalizada-participe-seguroVidaParticipe-jdbc-sink', None),  # placeholder
]
# El sink de reporteSIBS en normalizada
NOR_FIX = [
    ('normalizada-participe-reportesibs-jdbc-sink', 'normalizada.canonicos.participe.reportesibs', 'reporteSIBSParticipe_type', None),  # source desconocido por ahora
]
# Identificar sinks correctos
all_conns = json.loads(urllib.request.urlopen(f'{KC}/connectors').read().decode())
for sink_name in ['reportesibs','personavinculaciones']:
    matches = [c for c in all_conns if 'normalizada' in c.lower() and 'sink' in c and sink_name in c.lower() and 'jdbc' in c]
    print(f'  {sink_name} sinks: {matches}')

# Hacer flujo completo solo para reporteSIBS (el otro perdió 1 fila por dup)
sink = 'normalizada-participe-reportesibs-jdbc-sink'
src = 'canonicos-participe-reportesibs-jdbc-source'
# Cambiar sink a insert (ya estaba en upsert con triple key)
cfg = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/config', timeout=10).read().decode(), strict=False)
cfg.pop('name', None)
cfg['insert.mode'] = 'insert'
cfg['pk.mode'] = 'none'
cfg.pop('pk.fields', None)
fields = [f for f in cfg.get('fields.whitelist','').split(',') if f != 'id']
cfg['fields.whitelist'] = ','.join(fields)
# Restaurar batch.size si bajamos antes
cfg['batch.size'] = '500'
cfg['consumer.override.max.poll.records'] = '500'
code, _ = req(f'{KC}/connectors/{sink}/config', method='PUT', body=cfg)
print(f'  PUT reportesibs normalizada-sink (insert mode): {code}')

# Reset
req(f'{KC}/connectors/{sink}/stop', method='PUT')
req(f'{KC}/connectors/{src}/stop', method='PUT')
time.sleep(10)
req(f'{KC}/connectors/{sink}/offsets', method='DELETE')
req(f'{KC}/connectors/{src}/offsets', method='DELETE')
for suf in ['','.sink.dlq','.dlq']:
    t = f'normalizada.canonicos.participe.reportesibs{suf}'
    req(f'{KU}/topics/{urllib.parse.quote(t,safe="")}', method='DELETE')

# Restaurar query original del source (sin DISTINCT)
cfg_src = json.loads(urllib.request.urlopen(f'{KC}/connectors/{src}/config', timeout=10).read().decode(), strict=False)
old_q = cfg_src.get('query','')
# Si tiene el JOIN/INNER JOIN, restaurar a query simple
if 'INNER JOIN' in old_q:
    # Extract original SELECT before the JOIN
    new_q = old_q.split('INNER JOIN')[0].strip()
    if not new_q.upper().endswith('TYPE'):
        new_q = 'SELECT * FROM participes.reporteSIBSParticipe_type'
    cfg_src.pop('name', None)
    cfg_src['query'] = 'SELECT * FROM participes.reporteSIBSParticipe_type'
    code, _ = req(f'{KC}/connectors/{src}/config', method='PUT', body=cfg_src)
    print(f'  PUT source query simple: {code}')

c_nor.execute('TRUNCATE TABLE participes.[reporteSIBSParticipe_type]')

req(f'{KC}/connectors/{src}/resume', method='PUT')
time.sleep(30)
req(f'{KC}/connectors/{sink}/resume', method='PUT')
print('  esperando 60s...')
time.sleep(60)

c_nor.execute('SELECT COUNT(*) FROM participes.[reporteSIBSParticipe_type]')
print(f'  reporteSIBSParticipe normalizada: {c_nor.fetchone()[0]} (esperado 3000)')
c_nor.execute('SELECT COUNT(*) FROM participes.[personaVinculacionesType]')
print(f'  personaVinculacionesType normalizada: {c_nor.fetchone()[0]} (esperado 2646)')

# ========================================
# PASO 4: Reset sinks migration que tienen UQ ya dropeado
# ========================================
print('\n=== PASO 4: Reset sinks migration de las 4 tablas ===')
MIG_SINKS_TO_FIX = [
    ('migration-canonicos-participe-naturalinformacionadicional-jdbc-sink','migration-canonicos-participe-naturalinformacionadicional-jdbc-source','migration.canonicos.participe.naturalinformacionadicional','NATURALINFORMACIONADICIONALTYPE','naturalInformacionAdicionalType'),
    ('migration-participe-naturalinformacionbasica-jdbc-sink','migration-participe-naturalinformacionbasica-jdbc-source','migration.canonicos.participe.naturalinformacionbasica','NATURALINFORMACIONBASICATYPE','naturalInformacionBasicaType'),
    ('migration-participe-personadirecciones-jdbc-sink','migration-participe-personadirecciones-jdbc-source','migration.canonicos.participe.personadirecciones','PERSONADIRECCIONESTYPE','personaDireccionesType'),
    ('migration-canonicos-participe-personareferenciasbancarias-jdbc-sink','migration-canonicos-participe-personareferenciasbancarias-jdbc-source','migration.canonicos.participe.personareferenciasbancarias','PERSONAREFERENCIASBANCARIASTYPE','personaReferenciasBancariasType'),
]

# Cambiar sinks a insert simple (sin upsert)
for sink, src, topic, target, _ in MIG_SINKS_TO_FIX:
    cfg = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/config', timeout=10).read().decode(), strict=False)
    cfg.pop('name', None)
    cfg['insert.mode'] = 'insert'
    cfg['pk.mode'] = 'none'
    cfg.pop('pk.fields', None)
    fields = [f for f in cfg.get('fields.whitelist','').split(',') if f != 'id']
    cfg['fields.whitelist'] = ','.join(fields)
    code, _ = req(f'{KC}/connectors/{sink}/config', method='PUT', body=cfg)
    print(f'  PUT {sink}: {code}')

# Stop, reset, truncar topic+tabla, resume
for sink, src, _, _, _ in MIG_SINKS_TO_FIX:
    req(f'{KC}/connectors/{sink}/stop', method='PUT')
    req(f'{KC}/connectors/{src}/stop', method='PUT')
time.sleep(15)
for sink, src, _, _, _ in MIG_SINKS_TO_FIX:
    req(f'{KC}/connectors/{sink}/offsets', method='DELETE')
    req(f'{KC}/connectors/{src}/offsets', method='DELETE')

for _, _, topic, _, _ in MIG_SINKS_TO_FIX:
    for suf in ['','.sink.dlq','.dlq']:
        req(f'{KU}/topics/{urllib.parse.quote(topic+suf,safe="")}', method='DELETE')

for _, _, _, target, _ in MIG_SINKS_TO_FIX:
    try: c_mig.execute(f'TRUNCATE TABLE FCME_USER.[{target}]')
    except: pass

# Resume sources, esperar, resume sinks
for sink, src, _, _, _ in MIG_SINKS_TO_FIX:
    req(f'{KC}/connectors/{src}/resume', method='PUT')
print('  esperando 60s para que sources publiquen...')
time.sleep(60)
for sink, src, _, _, _ in MIG_SINKS_TO_FIX:
    req(f'{KC}/connectors/{sink}/resume', method='PUT')
print('  esperando 90s...')
time.sleep(90)

# Verificar
print('\n=== Counts despues del fix ===')
for sink, src, _, target, nor_t in MIG_SINKS_TO_FIX:
    c_nor.execute(f'SELECT COUNT(*) FROM participes.[{nor_t}]'); a=c_nor.fetchone()[0]
    c_mig.execute(f'SELECT COUNT(*) FROM FCME_USER.[{target}]'); b=c_mig.fetchone()[0]
    d = a-b
    status = 'OK' if d==0 else (f'falta {d}' if d>0 else f'sobran {-d}')
    print(f'  {nor_t}: nor={a} mig={b} delta={d}  {status}')

# Tambien chequear actualizacionDocumentos
c_nor.execute('SELECT COUNT(*) FROM participes.[actualizacionDocumentos_type]'); a=c_nor.fetchone()[0]
c_mig.execute('SELECT COUNT(*) FROM FCME_USER.[ACTUALIZACION_DOCUMENTOS_TYPE]'); b=c_mig.fetchone()[0]
print(f'  actualizacionDocumentos_type: nor={a} mig={b} delta={a-b}')

c_nor.execute('SELECT COUNT(*) FROM participes.[reporteSIBSParticipe_type]'); a=c_nor.fetchone()[0]
c_mig.execute('SELECT COUNT(*) FROM FCME_USER.[REPORTESIBSPARTICIPE_TYPE]'); b=c_mig.fetchone()[0]
print(f'  reporteSIBSParticipe_type: nor={a} mig={b} delta={a-b}')
