"""Busca sinks Oracle funcionando en Kafka Connect para copiar config exacta."""
import json, urllib.request, urllib.error
BASE = "http://10.35.3.223:30083"

def http(method, path):
    req = urllib.request.Request(BASE+path, method=method, headers={"Accept":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:400]

# Lista connectors
st, lst = http("GET", "/connectors")
# Solo sinks o que tengan 'jdbc-sink' en nombre
sink_names = [n for n in lst if 'sink' in n.lower()]
print(f"Total sinks en cluster: {len(sink_names)}")

# buscar los que tienen Oracle en config
print("\nBuscando sinks con Oracle...")
oracle_sinks = []
for n in sink_names:
    st, cfg = http("GET", f"/connectors/{n}/config")
    if isinstance(cfg, dict):
        url = cfg.get("connection.url", "")
        if "oracle" in url.lower():
            st2, status = http("GET", f"/connectors/{n}/status")
            cs = status.get("connector",{}).get("state","?") if isinstance(status, dict) else "?"
            ts = [t.get("state","?") for t in status.get("tasks",[])] if isinstance(status, dict) else []
            oracle_sinks.append((n, cfg, cs, ts))

print(f"Oracle sinks encontrados: {len(oracle_sinks)}")
for n, cfg, cs, ts in oracle_sinks:
    ok = cs == "RUNNING" and all(t == "RUNNING" for t in ts) and len(ts) > 0
    marker = "OK" if ok else "--"
    if ok:
        print(f"  [{marker}] {n}  state={cs} tasks={ts}")

# Mostrar config completa del primer Oracle sink RUNNING
running = [s for s in oracle_sinks if s[2]=="RUNNING" and s[3] and all(t=="RUNNING" for t in s[3])]
if running:
    n, cfg, cs, ts = running[0]
    print(f"\n=== CONFIG DE REFERENCIA: {n} ===")
    for k in sorted(cfg):
        val = cfg[k]
        if "password" in k.lower(): val = "***"
        print(f"  {k}: {val}")
else:
    print("\nNo hay sinks Oracle RUNNING para copiar config")
