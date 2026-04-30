"""Verifica eventos con ids bajos (los que vienen de canonicos)."""
import json, urllib.request, urllib.error, time
BASE = "http://10.35.3.223:30083"

def http(method, path):
    req = urllib.request.Request(BASE+path, method=method, headers={"Accept":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:400]

# status del source despues de esperar
print("== Esperando 10s al source ==")
time.sleep(10)
st, s = http("GET", "/connectors/canonicos-convivencia-cdc-outbox-jdbc-source/status")
print(f"  HTTP {st}")
if isinstance(s, dict):
    print(f"  connector.state: {s.get('connector',{}).get('state','?')}")
    for t in s.get("tasks",[]):
        print(f"  task[{t.get('id')}].state: {t.get('state')}")
        if t.get("trace"): print(f"    trace: {t['trace'][:500]}")

import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# Todo el contenido de canonicos.cdc_outbox
c = conn("fcme_canonicos").cursor()
c.execute("SELECT id, aggregate_type, aggregate_id, event_type, source_table, created_at FROM dbo.cdc_outbox ORDER BY id")
print("\n== Todo canonicos.cdc_outbox ==")
for r in c.fetchall():
    print(f"  id={r.id} type={r.aggregate_type} agg={r.aggregate_id} op={r.event_type} src={r.source_table}")

# Ids bajos en newcore.cdc_inbox (vinieron del source canonicos)
cn = conn("fcme_newcore").cursor()
cn.execute("SELECT id, aggregate_type, aggregate_id, event_type, source_table, processed, processed_at FROM dbo.cdc_inbox WHERE id < 1000 ORDER BY id")
print("\n== newcore.cdc_inbox con id bajos (vienen del source canonicos) ==")
for r in cn.fetchall():
    print(f"  id={r.id} type={r.aggregate_type} agg={r.aggregate_id} op={r.event_type} processed={r.processed} processed_at={r.processed_at}")

cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id < 1000")
print(f"\ntotal con id<1000 en newcore.cdc_inbox: {cn.fetchone()[0]}")
