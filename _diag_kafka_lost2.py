import pyodbc, oracledb, urllib.request, json, time

BASE='http://10.35.3.223:30083'
def http(method, path, body=None):
    url=BASE+path
    data=json.dumps(body).encode() if body else None
    req=urllib.request.Request(url, data=data, method=method, headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode() or '{}')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

o=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=o.cursor()
c=pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123', autocommit=True).cursor()

# Reset clean
co.execute('DELETE FROM FCME_USER.CDC_OUTBOX'); o.commit()
c.execute('DELETE FROM dbo.cdc_inbox')
c.execute('DELETE FROM dbo.cdc_inbox_errors')

# Forzar tolerance=none (sin DLQ) para que cualquier fallo sea visible
st, cfg = http('GET','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/config')
cfg['errors.tolerance']='none'
for k in list(cfg.keys()):
    if 'deadletterqueue' in k:
        cfg.pop(k, None)
http('PUT','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/config', cfg)
http('POST','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/tasks/0/restart')
time.sleep(5)

def status_with_trace():
    st, s = http('GET','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/status')
    state = s.get('tasks',[{}])[0].get('state','?')
    trace = s.get('tasks',[{}])[0].get('trace','')
    return state, trace

def offsets():
    st, src = http('GET','/connectors/newcore-oracle-cdc-outbox-source/offsets')
    st, snk = http('GET','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/offsets')
    src_id = src['offsets'][0]['offset']['incrementing'] if src.get('offsets') else None
    snk_off = snk['offsets'][0]['offset']['kafka_offset'] if snk.get('offsets') else None
    return src_id, snk_off

src0, snk0 = offsets()
print('Inicio: src_last_id=%s sink_offset=%s' % (src0, snk0))

print('\n--- Test 1: REFERENCIAPARTICIPE_TYPE (simple) ---')
co.execute("INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('Z1','simple')")
o.commit()

for i in range(12):
    time.sleep(2)
    src, snk = offsets()
    c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox WHERE aggregate_type=?', 'referenciaParticipeType')
    n = c.fetchone()[0]
    state, trace = status_with_trace()
    print('  T+%ds: src=%s snk=%s inbox_simple=%d state=%s' % ((i+1)*2, src, snk, n, state))
    if trace:
        print('    TRACE: ' + trace[:2000])
        break
    if n > 0:
        print('    OK llego al inbox')
        break

print('\n--- Test 2: ACTUALIZACION_AFILIADO_TYPE (49 cols complejo) ---')
co.execute("SELECT column_name FROM all_tab_columns WHERE owner='FCME_USER' AND table_name='ACTUALIZACION_AFILIADO_TYPE' ORDER BY column_id")
cols = [r[0] for r in co.fetchall() if r[0] != 'ID']
col_names = ', '.join(cols)
vals = ', '.join(["'X'"] * len(cols))
co.execute("INSERT INTO FCME_USER.ACTUALIZACION_AFILIADO_TYPE (" + col_names + ") VALUES (" + vals + ")")
o.commit()
print('  Oracle INSERT con %d cols' % len(cols))

for i in range(15):
    time.sleep(2)
    src, snk = offsets()
    c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox WHERE aggregate_type=?', 'actualizacionAfiliadoType')
    n = c.fetchone()[0]
    state, trace = status_with_trace()
    print('  T+%ds: src=%s snk=%s inbox_complejo=%d state=%s' % ((i+1)*2, src, snk, n, state))
    if trace:
        print('    TRACE:')
        print(trace[:3500])
        break
    if n > 0:
        c.execute("SELECT TOP 1 payload FROM dbo.cdc_inbox WHERE aggregate_type='actualizacionAfiliadoType'")
        p = c.fetchone()
        pl = p.payload or ''
        print('    OK llego. payload[:300]=' + pl[:300])
        break

# Cleanup
print('\n--- Cleanup ---')
try: co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='Z1'")
except: pass
try: co.execute("DELETE FROM FCME_USER.ACTUALIZACION_AFILIADO_TYPE WHERE CODIGO_CEDU='X'")
except: pass
co.execute('DELETE FROM FCME_USER.CDC_OUTBOX')
o.commit()
o.close()

cfg['errors.tolerance']='all'
cfg['errors.deadletterqueue.topic.name']='newcore.canonicos.CDC_OUTBOX.dlq'
cfg['errors.deadletterqueue.topic.replication.factor']='1'
cfg['errors.deadletterqueue.context.headers.enable']='true'
cfg['errors.log.enable']='true'
cfg['errors.log.include.messages']='true'
http('PUT','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/config', cfg)
http('POST','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/tasks/0/restart')
print('  tolerance=all + DLQ restaurado')
