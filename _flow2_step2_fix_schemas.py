"""Fix: habilitar value.converter.schemas.enable=true en source y sink."""
import json, urllib.request, urllib.error, time

BASE = "http://10.35.3.223:30083"
def http(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode() or "{}"
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return None, str(e)

# Source: agregar schemas.enable=true
SOURCE_CFG = {
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
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
}

SINK_CFG = {
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
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
}

print("[1] PUT update source config con schemas=true")
st, body = http("PUT", "/connectors/newcore-oracle-cdc-outbox-source/config", SOURCE_CFG)
print(f"  HTTP {st}")

print("\n[2] PUT update sink config con schemas=true")
st, body = http("PUT", "/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/config", SINK_CFG)
print(f"  HTTP {st}")

# Restart sink task
print("\n[3] Restart sink task")
st, body = http("POST", "/connectors/newcore-canonicos-cdc-inbox-jdbc-sink/tasks/0/restart")
print(f"  HTTP {st}")

# Restart source task (para que re-emita desde el inicio si el offset esta consumido)
print("\n[4] Restart source connector")
st, body = http("POST", "/connectors/newcore-oracle-cdc-outbox-source/restart?includeTasks=true&onlyFailed=false")
print(f"  HTTP {st}")

# Wait + status
print("\n[5] Status check (8s)")
time.sleep(8)
for n in ["newcore-oracle-cdc-outbox-source","newcore-canonicos-cdc-inbox-jdbc-sink"]:
    st, s = http("GET", f"/connectors/{n}/status")
    if isinstance(s, dict):
        cstate = s.get("connector",{}).get("state","?")
        tasks = s.get("tasks",[])
        for t in tasks:
            ts = t.get("state","?")
            tr = (t.get("trace") or "")[:300]
            print(f"  {n}: connector={cstate} task={ts}  trace={tr}")
