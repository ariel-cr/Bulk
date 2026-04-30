"""Obtener trace del source FAILED y reiniciarlo."""
import json, urllib.request, urllib.error, time
BASE = "http://10.35.3.223:30083"
NAME = "canonicos-convivencia-cdc-outbox-jdbc-source"

def http(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(BASE+path, data=data, method=method,
                                 headers={"Accept":"application/json","Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode() or "{}"
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try: return e.code, json.loads(raw)
        except: return e.code, raw

print("== Status completo del source ==")
st, s = http("GET", f"/connectors/{NAME}/status")
print(json.dumps(s, indent=2)[:2500] if isinstance(s,dict) else s)

print("\n== Intentando restart connector ==")
st, s = http("POST", f"/connectors/{NAME}/restart")
print(f"  HTTP {st}")
time.sleep(8)

print("\n== Status post-restart ==")
st, s = http("GET", f"/connectors/{NAME}/status")
if isinstance(s, dict):
    print(f"  connector.state: {s.get('connector',{}).get('state','?')}")
    for t in s.get("tasks",[]):
        print(f"  task[{t.get('id')}].state: {t.get('state')}")
        if t.get("trace"):
            print(f"    trace: {t['trace'][:1500]}")
