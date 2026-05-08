"""Fix migration sinks con pk.fields incompleto (causa upsert que reduce filas).
- Grupo A (sin UQ en destino): insert.mode=insert + pk.mode=none
- Grupo B (con UQ): pk.fields ajustado al UQ
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

# Grupo A: insert.mode=insert (sin UQ en destino)
GROUP_A = [
    ('migration-participe-rolnomina-jdbc-sink','ROLNOMINA_TYPE','migration.canonicos.participe.rolnomina','migration-participe-rolnomina-jdbc-source'),
    ('migration-participe-saldodiariorubro-jdbc-sink','SALDODIARIORUBRO_TYPE','migration.canonicos.participe.saldodiariorubro','migration-participe-saldodiariorubro-jdbc-source'),
    ('migration-participe-servicioadicional-jdbc-sink','SERVICIOADICIONAL_TYPE','migration.canonicos.participe.servicioadicional','migration-participe-servicioadicional-jdbc-source'),
    ('migration-participe-retiroliquidacion-jdbc-sink','RETIROLIQUIDACION_TYPE','migration.canonicos.participe.retiroliquidacion','migration-participe-retiroliquidacion-jdbc-source'),
    ('migration-participe-agendamailafiliado-jdbc-sink','AGENDAMAILAFILIADO_TYPE','migration.canonicos.participe.agendamailafiliado','migration-participe-agendamailafiliado-jdbc-source'),
    ('migration-participe-otrosingresosafiliado-jdbc-sink','OTROSINGRESOSAFILIADO_TYPE','migration.canonicos.participe.otrosingresosafiliado','migration-participe-otrosingresosafiliado-jdbc-source'),
    ('migration-participe-imagenes-jdbc-sink','IMAGENESTYPE','migration.canonicos.participe.imagenes','migration-participe-imagenes-jdbc-source'),
    ('migration-participe-documentacionafiliado-jdbc-sink','DOCUMENTACIONAFILIADO_TYPE','migration.canonicos.participe.documentacionafiliado','migration-participe-documentacionafiliado-jdbc-source'),
    ('migration-participe-informacionadicionalafiliado-jdbc-sink','INFORMACIONADICIONALAFILIADO_TYPE','migration.canonicos.participe.informacionadicionalafiliado','migration-participe-informacionadicionalafiliado-jdbc-source'),
]

# Grupo B: pk.fields al UQ correcto (record_value para que use el UQ en lugar del key)
GROUP_B = [
    # (sink, target, topic, source, new_pk_fields)
    ('migration-participe-personadirecciones-jdbc-sink','PERSONADIRECCIONESTYPE','migration.canonicos.participe.personadirecciones','migration-participe-personadirecciones-jdbc-source','codigoTipoIdentificacion,identificacion,numeroDireccion'),
    ('migration-canonicos-participe-naturalinformacionadicional-jdbc-sink','NATURALINFORMACIONADICIONALTYPE','migration.canonicos.participe.naturalinformacionadicional','migration-canonicos-participe-naturalinformacionadicional-jdbc-source','codigoTipoIdentificacion,identificacion'),
    ('migration-participe-naturalinformacionbasica-jdbc-sink','NATURALINFORMACIONBASICATYPE','migration.canonicos.participe.naturalinformacionbasica','migration-participe-naturalinformacionbasica-jdbc-source','codigoTipoIdentificacion,identificacion'),
    ('migration-canonicos-participe-personareferenciasbancarias-jdbc-sink','PERSONAREFERENCIASBANCARIASTYPE','migration.canonicos.participe.personareferenciasbancarias','migration-canonicos-participe-personareferenciasbancarias-jdbc-source','codigoTipoIdentificacion,identificacion,secuenciaReferenciaBancaria'),
]

# Aplicar config Grupo A
print('=== Grupo A: insert.mode=insert + pk.mode=none ===')
for sink, target, topic, src in GROUP_A:
    cfg = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/config', timeout=10).read().decode(), strict=False)
    cfg.pop('name', None)
    cfg['insert.mode'] = 'insert'
    cfg['pk.mode'] = 'none'
    cfg.pop('pk.fields', None)
    fields = [f for f in cfg.get('fields.whitelist','').split(',') if f != 'id']
    cfg['fields.whitelist'] = ','.join(fields)
    code, _ = req(f'{KC}/connectors/{sink}/config', method='PUT', body=cfg)
    print(f'  {sink}: PUT {code}')

# Aplicar config Grupo B
print('\n=== Grupo B: pk.mode=record_value + pk.fields=UQ ===')
for sink, target, topic, src, pk_fields in GROUP_B:
    cfg = json.loads(urllib.request.urlopen(f'{KC}/connectors/{sink}/config', timeout=10).read().decode(), strict=False)
    cfg.pop('name', None)
    cfg['insert.mode'] = 'upsert'
    cfg['pk.mode'] = 'record_value'
    cfg['pk.fields'] = pk_fields
    fields = [f for f in cfg.get('fields.whitelist','').split(',') if f != 'id']
    cfg['fields.whitelist'] = ','.join(fields)
    code, _ = req(f'{KC}/connectors/{sink}/config', method='PUT', body=cfg)
    print(f'  {sink}: PUT {code} (pk={pk_fields})')

# Stop, reset, truncate, resume
ALL = GROUP_A + [(s,t,topic,src) for s,t,topic,src,_ in GROUP_B]
print('\n=== STOP all ===')
for sink, _, _, src in ALL:
    req(f'{KC}/connectors/{src}/stop', method='PUT')
    req(f'{KC}/connectors/{sink}/stop', method='PUT')
time.sleep(15)

print('=== DELETE offsets ===')
for sink, _, _, src in ALL:
    req(f'{KC}/connectors/{src}/offsets', method='DELETE')
    req(f'{KC}/connectors/{sink}/offsets', method='DELETE')

print('=== Borrar topics ===')
for _, _, topic, _ in ALL:
    for suf in ['','.sink.dlq','.dlq']:
        req(f'{KU}/topics/{urllib.parse.quote(topic+suf,safe="")}', method='DELETE')

print('=== TRUNCATE destinos ===')
mig = pytds.connect('10.35.3.64', database='fcme_migration', user='sa', password='YourPassword123', port=1433, autocommit=True, timeout=20)
c_mig = mig.cursor()
for _, target, _, _ in ALL:
    try: c_mig.execute(f'TRUNCATE TABLE FCME_USER.[{target}]')
    except:
        try: c_mig.execute(f'DELETE FROM FCME_USER.[{target}]')
        except: pass

print('=== RESUME sources (escalonado) ===')
for i in range(0, len(ALL), 4):
    chunk = ALL[i:i+4]
    for sink, _, _, src in chunk:
        req(f'{KC}/connectors/{src}/resume', method='PUT')
    if i+4 < len(ALL): time.sleep(10)

print('  esperando 60s para que publiquen...')
time.sleep(60)

print('=== RESUME sinks ===')
for i in range(0, len(ALL), 4):
    chunk = ALL[i:i+4]
    for sink, _, _, _ in chunk:
        req(f'{KC}/connectors/{sink}/resume', method='PUT')
    if i+4 < len(ALL): time.sleep(8)

print('  esperando 90s...')
time.sleep(90)

# Verificar
nor = pytds.connect('capa.federada', database='fcme_canonicos_normalizada', user='sa', password='SqlServer2025!', port=31433, autocommit=True, timeout=20)
c_nor = nor.cursor()
print('\n=== Counts despues del fix ===')
TARGETS_NOR = {
    'ROLNOMINA_TYPE':'rolNomina_type',
    'SALDODIARIORUBRO_TYPE':'saldoDiarioRubro_type',
    'SERVICIOADICIONAL_TYPE':'servicioAdicional_type',
    'RETIROLIQUIDACION_TYPE':'retiroLiquidacion_type',
    'AGENDAMAILAFILIADO_TYPE':'agendaMailAfiliado_type',
    'OTROSINGRESOSAFILIADO_TYPE':'otrosIngresosAfiliado_type',
    'IMAGENESTYPE':'imagenesType',
    'DOCUMENTACIONAFILIADO_TYPE':'documentacionAfiliado_type',
    'INFORMACIONADICIONALAFILIADO_TYPE':'informacionAdicionalAfiliado_type',
    'PERSONADIRECCIONESTYPE':'personaDireccionesType',
    'NATURALINFORMACIONADICIONALTYPE':'naturalInformacionAdicionalType',
    'NATURALINFORMACIONBASICATYPE':'naturalInformacionBasicaType',
    'PERSONAREFERENCIASBANCARIASTYPE':'personaReferenciasBancariasType',
}
for sink, target, _, _ in ALL:
    nor_t = TARGETS_NOR.get(target)
    a=b=None
    if nor_t:
        try: c_nor.execute(f'SELECT COUNT(*) FROM participes.[{nor_t}]'); a=c_nor.fetchone()[0]
        except: a='ERR'
    try: c_mig.execute(f'SELECT COUNT(*) FROM FCME_USER.[{target}]'); b=c_mig.fetchone()[0]
    except: b='ERR'
    if isinstance(a,int) and isinstance(b,int):
        d=a-b
        st='OK' if d==0 and a>0 else (f'falta {d}' if d>0 else f'sobran {-d}')
    else: d='?'; st='err'
    print(f'  {target:<40} nor={str(a):>6} mig={str(b):>6}  {st}')
