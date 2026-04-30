"""Diagnostico Kafka: estado real de los connectors y task errors."""
import json, urllib.request, urllib.error

BASE = "http://10.35.3.223:30083"
def http(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return None, str(e)

NAMES = ["newcore-oracle-cdc-outbox-source","newcore-canonicos-cdc-inbox-jdbc-sink"]

for n in NAMES:
    print("="*70)
    print(f"=== {n} ===")
    print("="*70)
    st, status = http("GET", f"/connectors/{n}/status")
    print(json.dumps(status, indent=2)[:3000])

    st, cfg = http("GET", f"/connectors/{n}/config")
    if isinstance(cfg, dict):
        print("\nConfig:")
        for k, v in cfg.items():
            if "password" not in k.lower():
                print(f"  {k}: {v}")
    print()
