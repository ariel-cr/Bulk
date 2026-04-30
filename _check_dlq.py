"""Consume DLQ del sink y del source para ver por que los ids 7-9 no llegaron."""
from confluent_kafka import Consumer, TopicPartition
import json, sys

conf = {
    'bootstrap.servers': '10.35.3.223:31092',
    'group.id': f'debug-dlq-reader',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

# DLQ topics a revisar
for topic in ["convivencia.canonicos.cdc.outbox.sink.dlq",
              "convivencia.canonicos.cdc.outbox.dlq",
              "convivencia.canonicos.cdc.outbox"]:
    print(f"\n=== {topic} ===")
    cons = Consumer(conf)
    try:
        cons.subscribe([topic])
        count = 0
        while count < 30:
            msg = cons.poll(timeout=5.0)
            if msg is None: break
            if msg.error():
                if "PARTITION_EOF" in str(msg.error()): break
                print(f"  err: {msg.error()}"); break
            count += 1
            hdrs = dict(msg.headers() or [])
            err = hdrs.get("__connect.errors.exception.message", b"")
            if isinstance(err, bytes): err = err.decode(errors="replace")
            try:
                v = json.loads(msg.value().decode())
                payload_id = v.get("payload", {}).get("id") if isinstance(v.get("payload"), dict) else v.get("id")
            except:
                payload_id = "?"
            print(f"  [{msg.partition()}:{msg.offset()}]  id={payload_id}  err={err[:200] if err else '(sin header error)'}")
        print(f"  total leidos: {count}")
    finally:
        cons.close()
