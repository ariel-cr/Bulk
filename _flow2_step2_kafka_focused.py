"""Audit enfocado: solo busca el camino especifico FCME_USER.CDC_OUTBOX -> cdc_inbox."""
import json, urllib.request, urllib.error

BASE = "http://10.35.3.223:30083"
def http(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode() or "{}"
            return r.status, (json.loads(raw) if raw.strip() else {})
    except Exception as e:
        return None, str(e)

st, lst = http("GET", "/connectors")
print(f"total connectors: {len(lst)}")

# Filtrar SOLO los que necesitamos
print("\n[A] Connectors que tocan Oracle FCME_USER.CDC_OUTBOX (source)")
print("="*70)
src_outbox = []
for n in lst:
    st, cfg = http("GET", f"/connectors/{n}")
    if not isinstance(cfg, dict): continue
    config = cfg.get("config", {})
    url = config.get("connection.url","").lower()
    table_w = (config.get("table.whitelist","") + " " + config.get("query","") + " " + config.get("table.name.format","")).upper()
    klass = config.get("connector.class","")
    is_source = "Source" in klass
    has_oracle = "oracle" in url
    has_outbox = "CDC_OUTBOX" in table_w
    has_fcme_user = "FCME_USER" in table_w
    if is_source and has_oracle and (has_outbox or has_fcme_user):
        src_outbox.append((n, config))

print(f"  {len(src_outbox)} matches")
for n, cfg in src_outbox:
    print(f"\n  >> {n}")
    print(f"     class={cfg.get('connector.class')}")
    print(f"     url={cfg.get('connection.url')}")
    print(f"     table.whitelist={cfg.get('table.whitelist','')}")
    print(f"     query={(cfg.get('query','') or '')[:200]}")
    print(f"     topic.prefix={cfg.get('topic.prefix','')}")
    print(f"     mode={cfg.get('mode','')}")
    print(f"     incrementing.column.name={cfg.get('incrementing.column.name','')}")

print("\n[B] Connectors que escriben a fcme_canonicos.cdc_inbox (sink)")
print("="*70)
snk_inbox = []
for n in lst:
    st, cfg = http("GET", f"/connectors/{n}")
    if not isinstance(cfg, dict): continue
    config = cfg.get("config", {})
    klass = config.get("connector.class","")
    if "Sink" not in klass: continue
    url = config.get("connection.url","").lower()
    table = (config.get("table.name.format","") + " " + config.get("topics","")).lower()
    if "fcme_canonicos" in url and "cdc_inbox" in table:
        snk_inbox.append((n, config))
    elif "cdc_inbox" in table and "sqlserver" in url:
        snk_inbox.append((n, config))

print(f"  {len(snk_inbox)} matches")
for n, cfg in snk_inbox:
    print(f"\n  >> {n}")
    print(f"     class={cfg.get('connector.class')}")
    print(f"     url={cfg.get('connection.url')}")
    print(f"     table.name.format={cfg.get('table.name.format')}")
    print(f"     topics={cfg.get('topics')}")

print("\n[C] Resumen")
print("="*70)
print(f"  Source FCME_USER.CDC_OUTBOX -> Kafka: {'EXISTE' if src_outbox else 'NO EXISTE - hay que crear'}")
print(f"  Sink Kafka -> SQL canonicos.cdc_inbox: {'EXISTE' if snk_inbox else 'NO EXISTE - hay que crear'}")
