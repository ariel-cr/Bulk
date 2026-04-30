"""Verifica estado de los 2 connectors desplegados + flujo end-to-end real."""
import json, urllib.request, urllib.error, time
BASE = "http://10.35.3.223:30083"

def http(method, path):
    req = urllib.request.Request(BASE+path, method=method,
                                 headers={"Accept":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:400]

for name in ["canonicos-convivencia-cdc-outbox-jdbc-source",
             "newcore-convivencia-cdc-outbox-jdbc-sink"]:
    print(f"\n== {name} ==")
    st, s = http("GET", f"/connectors/{name}/status")
    if isinstance(s, dict):
        c_state = s.get("connector",{}).get("state","?")
        tasks = s.get("tasks",[])
        print(f"  connector.state: {c_state}")
        for t in tasks:
            print(f"  task[{t.get('id')}].state: {t.get('state')}")
            if t.get("trace"):
                print(f"    trace: {t['trace'][:400]}")
    else:
        print(f"  {st}  {s}")

# Ahora test real: UPDATE legacy, ver que fluye por todo
print("\n\n== TEST DE FLUJO REAL ==")
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c_can = conn("fcme_canonicos").cursor()
c_nc  = conn("fcme_newcore").cursor()
c_fc  = conn("dbFC").cursor()

c_can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); ob = c_can.fetchone()[0]
c_nc.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); ib = c_nc.fetchone()[0]
print(f"antes: canonicos.cdc_outbox={ob}  newcore.cdc_inbox={ib}")

c_fc.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu ORDER BY ci_cedu")
ci = c_fc.fetchone()[0]
print(f"\nUPDATE dbFC.fctbafil_actu ci_cedu={ci}")
c_fc.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)

c_can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); oa = c_can.fetchone()[0]
print(f"  canonicos.cdc_outbox: {ob} -> {oa}  (+{oa-ob})")

print("\nesperando 15s a que Kafka Connect propague...")
time.sleep(15)

c_nc.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); ia = c_nc.fetchone()[0]
print(f"  newcore.cdc_inbox: {ib} -> {ia}  (+{ia-ib})")

if ia > ib:
    print("\nUltimas filas llegadas a newcore.cdc_inbox:")
    c_nc.execute("""
      SELECT TOP 5 id, aggregate_type, aggregate_id, event_type, processed, processed_at
      FROM dbo.cdc_inbox ORDER BY id DESC
    """)
    for r in c_nc.fetchall():
        print(f"  id={r.id}  type={r.aggregate_type}  agg={r.aggregate_id}  op={r.event_type}  processed={r.processed}")
else:
    print("\nNo llegaron aun. Ver tasks del sink:")
    st, s = http("GET", "/connectors/newcore-convivencia-cdc-outbox-jdbc-sink/status")
    print(json.dumps(s, indent=2)[:1500] if isinstance(s,dict) else s)
