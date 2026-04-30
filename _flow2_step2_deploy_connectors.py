"""Crea source + sink connectors para Newcore -> Legacy."""
import json, urllib.request, urllib.error, time

BASE = "http://10.35.3.223:30083"
def http(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type":"application/json","Accept":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode() or "{}"
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try: b = json.loads(raw)
        except: b = raw
        return e.code, b
    except Exception as e:
        return None, str(e)

SOURCE = {
  "name": "newcore-oracle-cdc-outbox-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:oracle:thin:@10.35.3.223:31521/XEPDB1",
    "connection.user": "fcme_user",
    "connection.password": "FcmeUser2025!",
    "mode": "incrementing",
    "incrementing.column.name": "ID",
    "table.whitelist": "FCME_USER.CDC_OUTBOX",
    "table.types": "TABLE",
    "poll.interval.ms": "1000",
    "topic.prefix": "newcore.canonicos.",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}

SINK = {
  "name": "newcore-canonicos-cdc-inbox-jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:sqlserver://10.35.3.64:1433;databaseName=fcme_canonicos;encrypt=false;trustServerCertificate=true",
    "connection.user": "sa",
    "connection.password": "YourPassword123",
    "topics": "newcore.canonicos.CDC_OUTBOX",
    "table.name.format": "dbo.cdc_inbox",
    "auto.create": "false",
    "auto.evolve": "false",
    "insert.mode": "insert",
    "pk.mode": "none",
    "fields.whitelist": "aggregate_id,aggregate_type,event_type,payload,source_table,created_at",
    "quote.sql.identifiers": "NEVER",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}

def deploy(spec):
    name = spec["name"]
    print(f"\n--- {name} ---")
    st, lst = http("GET", "/connectors")
    if isinstance(lst, list) and name in lst:
        print(f"  ya existe -> PUT /connectors/{name}/config")
        st, body = http("PUT", f"/connectors/{name}/config", spec["config"])
    else:
        print(f"  no existe -> POST /connectors")
        st, body = http("POST", "/connectors", spec)
    print(f"  HTTP {st}")
    if isinstance(body, dict):
        # mostrar campos clave del response
        keys = ("error_code","message","name","tasks")
        for k in keys:
            if k in body: print(f"  {k}: {str(body[k])[:300]}")
    else:
        print(f"  body: {str(body)[:400]}")

print("="*70)
print("[1] Deploy SOURCE: Oracle FCME_USER.CDC_OUTBOX -> topic")
print("="*70)
deploy(SOURCE)

print("\n" + "="*70)
print("[2] Deploy SINK: topic -> SQL canonicos.cdc_inbox")
print("="*70)
deploy(SINK)

# Wait + status
print("\n" + "="*70)
print("[3] Status check (esperando 6s a que arranquen)")
print("="*70)
time.sleep(6)
for spec in (SOURCE, SINK):
    n = spec["name"]
    st, s = http("GET", f"/connectors/{n}/status")
    if isinstance(s, dict):
        cstate = s.get("connector",{}).get("state","?")
        tasks = s.get("tasks",[])
        tstates = [(t.get("state","?"), (t.get("trace") or "")[:200]) for t in tasks]
        print(f"\n  {n}")
        print(f"    connector_state: {cstate}")
        for i,(ts,tr) in enumerate(tstates):
            print(f"    task[{i}]: {ts}  trace={tr}")
    else:
        print(f"\n  {n}  status err: {s}")
