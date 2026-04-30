"""Diagnostico: por que se pierden mensajes en el sink Kafka."""
import json, urllib.request, time, oracledb, pyodbc

BASE='http://10.35.3.223:30083'
def http(method, path, body=None):
    url=BASE+path; data=json.dumps(body).encode() if body else None
    req=urllib.request.Request(url, data=data, method=method, headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return r.status, json.loads(r.read().decode() or '{}')
    except urllib.error.HTTPError as e: return e.code, e.read().decode()

# 1) Source offset (cuantos mensajes ha publicado el source al topic)
print("="*70)
print("[1] Cuanto consumio cada lado")
print("="*70)
st, src_off = http('GET','/connectors/newcore-oracle-cdc-outbox-source/offsets')
print(f"  source last_id_publicado: {src_off}")
st, snk_off = http('GET','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/offsets')
print(f"  sink kafka_offset:        {snk_off}")
# Si sink_offset >= mensajes_totales, los mensajes ya fueron CONSUMIDOS pero no escritos => DLQ

# 2) Cambiar tolerance=none para forzar error visible
print("\n" + "="*70)
print("[2] Cambiar tolerance=none + forzar 1 INSERT problematico")
print("="*70)
st, cfg = http('GET','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/config')
cfg['errors.tolerance']='none'
for k in list(cfg.keys()):
    if 'deadletterqueue' in k: cfg.pop(k, None)
http('PUT','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/config', cfg)
http('POST','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/tasks/0/restart')
time.sleep(5)

# 3) Insertar UN evento de un type que sospechamos que falla
o=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=o.cursor()
c=pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123', autocommit=True).cursor()

# Reset
co.execute('DELETE FROM FCME_USER.CDC_OUTBOX'); o.commit()
c.execute('DELETE FROM dbo.cdc_inbox')

# Test 1: type SIMPLE que sabemos funciona
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='D1'")
co.execute("INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('D1','diag1')")
o.commit()
print("  [test simple] INSERT REFERENCIAPARTICIPE_TYPE")
time.sleep(8)
c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox')
print(f"    inbox count: {c.fetchone()[0]}")
st, s = http('GET','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/status')
print(f"    sink_state: {s.get('tasks',[{}])[0].get('state','?')}")

# Test 2: type COMPLEJO con muchas columnas
co.execute("""SELECT column_name, data_type FROM all_tab_columns
              WHERE owner='FCME_USER' AND table_name='ACTUALIZACION_AFILIADO_TYPE' ORDER BY column_id""")
cols = [(r[0], r[1]) for r in co.fetchall() if r[0] != 'ID']
n_cols = len(cols)
print(f"\n  [test complejo] ACTUALIZACION_AFILIADO_TYPE tiene {n_cols} cols")
print(f"    types unicos: {set(c[1] for c in cols)}")
col_names = ", ".join(c[0] for c in cols)
vals = ["'9'" if 'VARCHAR' in c[1] or 'CHAR' in c[1] else ('99' if 'NUMBER' in c[1] else 'NULL') for c in cols]
ins = f"INSERT INTO FCME_USER.ACTUALIZACION_AFILIADO_TYPE ({col_names}) VALUES ({', '.join(vals)})"
try:
    co.execute(ins); o.commit()
    print("    Oracle INSERT OK")
except Exception as e:
    print(f"    Oracle INSERT fail: {str(e)[:200]}")

time.sleep(10)
c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox')
inb_count = c.fetchone()[0]
print(f"    inbox count: {inb_count}")
st, s = http('GET','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/status')
state = s.get('tasks',[{}])[0].get('state','?')
trace = s.get('tasks',[{}])[0].get('trace','')
print(f"    sink_state: {state}")
if trace:
    print(f"    TRACE:")
    print(trace[:3000])

# Cleanup test rows en Oracle
print("\n" + "="*70)
print("[3] Cleanup")
print("="*70)
try: co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='D1'")
except: pass
try: co.execute("DELETE FROM FCME_USER.ACTUALIZACION_AFILIADO_TYPE WHERE CODIGO_CEDU='99'")
except: pass
co.execute('DELETE FROM FCME_USER.CDC_OUTBOX')
o.commit()

# Restaurar tolerance=all + DLQ
cfg['errors.tolerance']='all'
cfg['errors.log.enable']='true'
cfg['errors.log.include.messages']='true'
cfg['errors.deadletterqueue.topic.name']='newcore.canonicos.CDC_OUTBOX.dlq'
cfg['errors.deadletterqueue.topic.replication.factor']='1'
cfg['errors.deadletterqueue.context.headers.enable']='true'
http('PUT','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/config', cfg)
http('POST','/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/tasks/0/restart')
print("  tolerance=all + DLQ restaurado")
o.close()
