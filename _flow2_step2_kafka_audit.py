"""Paso 2 audit: Kafka. Verificar que existe (o no) el flujo:
   Oracle CDC_OUTBOX -> Kafka -> SQL canonicos.cdc_inbox
"""
import json, urllib.request, urllib.error

BASE = "http://10.35.3.223:30083"

def http(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type":"application/json","Accept":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode("utf-8") or "{}"
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try: b = json.loads(raw)
        except: b = raw
        return e.code, b
    except Exception as e:
        return None, str(e)

print("="*70)
print("[2.1] Conectividad al Kafka Connect REST API")
print("="*70)
st, info = http("GET", "/")
print(f"  GET / -> HTTP {st}")
if isinstance(info, dict):
    print(f"  version={info.get('version')}  cluster={info.get('kafka_cluster_id')}")

print("\n" + "="*70)
print("[2.2] Listado total de connectors")
print("="*70)
st, lst = http("GET", "/connectors")
if not isinstance(lst, list):
    print(f"  ERROR: {lst}")
    raise SystemExit(1)
print(f"  total: {len(lst)}")

print("\n" + "="*70)
print("[2.3] Buscar SOURCE connectors leyendo Oracle CDC_OUTBOX")
print("="*70)
matches_source = []
for n in lst:
    if not any(k in n.lower() for k in ("source","outbox","cdc")):
        continue
    st, cfg = http("GET", f"/connectors/{n}")
    if not isinstance(cfg, dict): continue
    config = cfg.get("config", {})
    klass = config.get("connector.class","")
    url = config.get("connection.url","")
    query = config.get("query","")
    table = config.get("table.whitelist","") or config.get("table.name.format","")
    if "oracle" in url.lower() or "FCME_USER" in (query+table).upper() or "CDC_OUTBOX" in (query+table).upper():
        matches_source.append((n, klass, url, table[:60], query[:80]))

print(f"  candidatos source Oracle/CDC: {len(matches_source)}")
for n, k, u, t, q in matches_source:
    print(f"    {n}")
    print(f"      class={k}")
    print(f"      url={u[:80]}")
    if t: print(f"      table={t}")
    if q: print(f"      query={q}")

print("\n" + "="*70)
print("[2.4] Buscar SINK connectors escribiendo a SQL canonicos.cdc_inbox")
print("="*70)
matches_sink = []
for n in lst:
    if "sink" not in n.lower():
        continue
    st, cfg = http("GET", f"/connectors/{n}")
    if not isinstance(cfg, dict): continue
    config = cfg.get("config", {})
    klass = config.get("connector.class","")
    url = config.get("connection.url","")
    table = config.get("table.name.format","")
    topics = config.get("topics","")
    if "sqlserver" in url.lower() or "fcme_canonicos" in url.lower() or "cdc_inbox" in (table+topics).lower():
        matches_sink.append((n, klass, url[:80], table, topics[:80]))

print(f"  candidatos sink SQL Server: {len(matches_sink)}")
for n, k, u, t, tp in matches_sink:
    print(f"    {n}")
    print(f"      class={k}  url={u}")
    if t: print(f"      table={t}")
    if tp: print(f"      topics={tp}")

print("\n" + "="*70)
print("[2.5] Buscar topics conocidos del CDC outbox saliente")
print("="*70)
# Listar todos los topics que coincidan con el patron esperado
topics_known = set()
for n in lst:
    st, cfg = http("GET", f"/connectors/{n}")
    if isinstance(cfg, dict):
        config = cfg.get("config", {})
        for k, v in config.items():
            if k in ("topics","topic","topic.prefix") and v:
                topics_known.update(v.split(","))

cdc_topics = sorted(t for t in topics_known if "cdc" in t.lower() or "outbox" in t.lower() or "newcore" in t.lower())
print(f"  topics relevantes detectados:")
for t in cdc_topics[:20]:
    print(f"    {t}")
print(f"  total {len(cdc_topics)} topics relevantes (de {len(topics_known)} totales)")

print("\n" + "="*70)
print("[2.6] Existe ya el camino Newcore (Oracle CDC_OUTBOX) -> SQL cdc_inbox?")
print("="*70)

# Heuristica: source Oracle con table=CDC_OUTBOX + sink SQL escribiendo a cdc_inbox
have_source_outbox = any("CDC_OUTBOX" in (m[3]+m[4]).upper() for m in matches_source)
have_sink_inbox = any("cdc_inbox" in (m[3]+m[4]).lower() for m in matches_sink)
print(f"  Source Oracle CDC_OUTBOX existente: {'SI' if have_source_outbox else 'NO'}")
print(f"  Sink SQL Server cdc_inbox existente: {'SI' if have_sink_inbox else 'NO'}")

if have_source_outbox and have_sink_inbox:
    print("\n  → flujo Kafka YA aprovisionado")
else:
    print("\n  → falta aprovisionar al menos un connector")
    if not have_source_outbox:
        print("    [PENDIENTE] Source: Oracle FCME_USER.CDC_OUTBOX -> topic")
    if not have_sink_inbox:
        print("    [PENDIENTE] Sink: topic -> SQL fcme_canonicos.dbo.cdc_inbox")
