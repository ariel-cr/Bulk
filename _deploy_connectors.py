"""Despliega connectors en Kafka Connect http://10.35.3.223:30083
1) Ver connectors actuales
2) Crear canonicos-convivencia-cdc-outbox-jdbc-source
3) Actualizar config del newcore-convivencia-cdc-outbox-jdbc-sink (topic al nuevo)
4) NO desplegar oracle-sink (tiene placeholders)
"""
import json, urllib.request, urllib.error

BASE = "http://10.35.3.223:30083"

def http(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json",
                                          "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode("utf-8") or "{}"
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try: body = json.loads(raw)
        except: body = raw
        return e.code, body
    except Exception as e:
        return None, str(e)

def load(path):
    with open(path, encoding="utf-8") as f: return json.load(f)

# 1) lista actual
print("== [1] Connectors registrados ==")
st, cur_list = http("GET", "/connectors")
print(f"  HTTP {st}  connectors: {cur_list}")

# 2) Registrar/actualizar canonicos source
print("\n== [2] canonicos-convivencia-cdc-outbox-jdbc-source ==")
src = load(r"C:\Users\Usuario\Downloads\newBranch\cdc\CONVIVENCIA\canonicos-convivencia-cdc-outbox-jdbc-source.json")
name = src["name"]
cfg = src["config"]

if name in (cur_list if isinstance(cur_list, list) else []):
    print(f"  ya existe -> PUT /connectors/{name}/config")
    st, body = http("PUT", f"/connectors/{name}/config", cfg)
else:
    print(f"  no existe -> POST /connectors")
    st, body = http("POST", "/connectors", src)
print(f"  HTTP {st}")
print(f"  {json.dumps(body, indent=2)[:600] if isinstance(body,dict) else body}")

# 3) Status
print("\n  /status:")
st2, s = http("GET", f"/connectors/{name}/status")
print(f"  HTTP {st2}  {json.dumps(s, indent=2)[:500] if isinstance(s,dict) else s}")

# 4) Actualizar newcore sink con nuevo topic
print("\n== [3] newcore-convivencia-cdc-outbox-jdbc-sink (update config) ==")
snk = load(r"C:\Users\Usuario\Downloads\newBranch\cdc\CONVIVENCIA\newcore-convivencia-cdc-outbox-jdbc-sink.json")
name2 = snk["name"]; cfg2 = snk["config"]

st, cur_list = http("GET", "/connectors")
if name2 in (cur_list if isinstance(cur_list, list) else []):
    print(f"  existe -> PUT /connectors/{name2}/config")
    st, body = http("PUT", f"/connectors/{name2}/config", cfg2)
else:
    print(f"  no existe -> POST /connectors")
    st, body = http("POST", "/connectors", snk)
print(f"  HTTP {st}")
print(f"  {json.dumps(body, indent=2)[:600] if isinstance(body,dict) else body}")

st2, s = http("GET", f"/connectors/{name2}/status")
print(f"  status: HTTP {st2}  {json.dumps(s, indent=2)[:500] if isinstance(s,dict) else s}")

# 5) Nota oracle sink (NO desplegar, tiene placeholders)
print("\n== [4] newcore-oracle-cdc-inbox-jdbc-sink ==")
print("  NO DESPLEGADO: contiene placeholders ORACLE_HOST y REPLACE_ME")
print("  Completar y desplegar manualmente con:")
print(f"   curl -X POST -H 'Content-Type: application/json' "
      f"--data @newcore-oracle-cdc-inbox-jdbc-sink.json {BASE}/connectors")

# 6) Estado global
print("\n== [5] Lista final de connectors ==")
st, lst = http("GET", "/connectors")
if isinstance(lst, list):
    for n in lst:
        st2, s = http("GET", f"/connectors/{n}/status")
        if isinstance(s, dict):
            state = s.get("connector", {}).get("state", "?")
            tasks = s.get("tasks", [])
            tstates = [t.get("state","?") for t in tasks]
            print(f"  {n:<60} connector={state}  tasks={tstates}")
