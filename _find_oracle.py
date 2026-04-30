"""Busca connectors existentes que apunten a Oracle para copiar URL/credenciales."""
import json, urllib.request, urllib.error
BASE = "http://10.35.3.223:30083"

def http(method, path):
    req = urllib.request.Request(BASE+path, method=method, headers={"Accept":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:400]

# lista
st, lst = http("GET", "/connectors")
if not isinstance(lst, list):
    print(lst); raise SystemExit

print(f"Total connectors: {len(lst)}")
# buscar oracle
oracle_connectors = []
for n in lst:
    st, cfg = http("GET", f"/connectors/{n}/config")
    if isinstance(cfg, dict):
        url = cfg.get("connection.url","")
        if "oracle" in url.lower():
            oracle_connectors.append((n, url, cfg.get("connection.user","?"), cfg.get("table.name.format","?")))

print(f"\nConnectors Oracle encontrados: {len(oracle_connectors)}")
for n, url, user, tbl in oracle_connectors[:5]:
    print(f"\n  {n}")
    print(f"    url: {url}")
    print(f"    user: {user}")
    print(f"    table: {tbl}")
