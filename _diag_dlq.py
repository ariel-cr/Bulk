"""Consume el DLQ y el topic CDC_OUTBOX para ver el error exacto."""
from confluent_kafka import Consumer
import json, time, oracledb, pyodbc

BROKER = "10.35.3.223:31092"

def consume_topic(topic, max_msgs=10, timeout_s=8):
    c = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': f'diag-{topic}-{int(time.time())}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 6000
    })
    c.subscribe([topic])
    msgs = []
    end = time.time() + timeout_s
    while time.time() < end and len(msgs) < max_msgs:
        m = c.poll(1.0)
        if m is None: continue
        if m.error(): print(f'  err: {m.error()}'); continue
        msgs.append(m)
    c.close()
    return msgs

# Verificar que podemos conectarnos al broker
print("="*70)
print("[1] Listar topics relevantes")
print("="*70)
c = Consumer({'bootstrap.servers': BROKER, 'group.id': f'diag-list-{int(time.time())}'})
md = c.list_topics(timeout=10)
relevant = [t for t in md.topics.keys() if 'CDC_OUTBOX' in t.upper() or 'newcore' in t.lower()]
print(f"  topics relevantes: {len(relevant)}")
for t in sorted(relevant):
    print(f"    {t}")
c.close()

# Generar un evento COMPLEJO para forzar fallo y verlo en DLQ
print("\n" + "="*70)
print("[2] Generar evento ACTUALIZACION_AFILIADO_TYPE (complejo)")
print("="*70)
o=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=o.cursor()
sql_can=pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123', autocommit=True).cursor()

co.execute("DELETE FROM FCME_USER.ACTUALIZACION_AFILIADO_TYPE WHERE CODIGO_CEDU='99'")
o.commit()

# Insert con cols mas validas
co.execute("""SELECT column_name FROM all_tab_columns WHERE owner='FCME_USER' AND table_name='ACTUALIZACION_AFILIADO_TYPE' ORDER BY column_id""")
cols = [r[0] for r in co.fetchall() if r[0] != 'ID']
col_names = ', '.join(cols)
vals = ', '.join(["'9'"] * len(cols))
co.execute(f"INSERT INTO FCME_USER.ACTUALIZACION_AFILIADO_TYPE ({col_names}) VALUES ({vals})")
o.commit()
print(f"  Oracle INSERT con {len(cols)} cols")

# Esperar
time.sleep(8)

# Consumir CDC_OUTBOX para ver formato del mensaje publicado por source
print("\n" + "="*70)
print("[3] Consumir topic newcore.canonicos.CDC_OUTBOX (lo que publica el source)")
print("="*70)
msgs = consume_topic('newcore.canonicos.CDC_OUTBOX', max_msgs=5, timeout_s=10)
print(f"  consumidos: {len(msgs)}")
for m in msgs[-3:]:
    val = m.value()
    print(f"  offset={m.offset()}")
    if val:
        try:
            v = json.loads(val.decode('utf-8'))
            # Si tiene schema/payload (con schemas.enable=true)
            if 'schema' in v and 'payload' in v:
                print(f"    payload.AGGREGATE_TYPE: {v['payload'].get('AGGREGATE_TYPE')}")
                print(f"    payload.AGGREGATE_ID: {v['payload'].get('AGGREGATE_ID')}")
                print(f"    payload.PAYLOAD[:120]: {(v['payload'].get('PAYLOAD') or '')[:120]}")
                # Mostrar tipos del schema
                if 'fields' in v['schema']:
                    print(f"    schema fields:")
                    for f in v['schema']['fields'][:8]:
                        print(f"      {f.get('field')}: {f.get('type')} optional={f.get('optional')}")
            else:
                print(f"    raw: {str(v)[:300]}")
        except Exception as e:
            print(f"    decode err: {e}")
            print(f"    raw bytes[:300]: {val[:300]}")

# Consumir DLQ para ver errores
print("\n" + "="*70)
print("[4] Consumir DLQ newcore.canonicos.CDC_OUTBOX.dlq")
print("="*70)
msgs = consume_topic('newcore.canonicos.CDC_OUTBOX.dlq', max_msgs=10, timeout_s=10)
print(f"  consumidos: {len(msgs)}")
for m in msgs[-5:]:
    print(f"\n  DLQ msg offset={m.offset()}")
    if m.headers():
        for k, v in m.headers():
            if v:
                vstr = v.decode('utf-8', errors='replace')[:300]
                if 'error' in k.lower() or 'exception' in k.lower():
                    print(f"    [{k}] {vstr}")

# Cleanup
print("\n[Cleanup]")
co.execute("DELETE FROM FCME_USER.ACTUALIZACION_AFILIADO_TYPE WHERE CODIGO_CEDU='99'")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
o.commit()
o.close()
