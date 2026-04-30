"""Diagnostica por que el sink Oracle dejo de escribir."""
import pyodbc, oracledb, time, json, urllib.request, urllib.error

BASE = "http://10.35.3.223:30083"
NAME = "newcore-oracle-convivencia-cdc-inbox-sink"

def http(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(BASE+path, data=data, method=method,
        headers={"Accept":"application/json","Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode() or "{}"
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:2000]

print("== Status sink Oracle ==")
st, s = http("GET", f"/connectors/{NAME}/status")
if isinstance(s, dict):
    print(f"  connector.state: {s['connector'].get('state')}")
    for t in s.get("tasks",[]):
        state = t.get("state")
        print(f"  task[{t['id']}].state: {state}")
        if t.get("trace"):
            print(f"    trace:\n{t['trace'][:2500]}")

print("\n== Conteos actuales ==")
orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX"); oc = co.fetchone()[0]
co.execute("SELECT MIN(ID), MAX(ID) FROM FCME_USER.CDC_INBOX"); mm = co.fetchone()
print(f"  Oracle FCME_USER.CDC_INBOX: {oc}  id range=[{mm[0]}..{mm[1]}]")

c = pyodbc.connect("DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123", autocommit=True).cursor()
c.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM dbo.cdc_outbox"); oo = c.fetchone()
print(f"  canonicos.cdc_outbox: {oo[0]}  id range=[{oo[1]}..{oo[2]}]")

# si canonicos tiene mas filas que oracle, hay un gap
missing = oo[0] - oc
print(f"\n  FALTAN EN ORACLE: {missing} filas")

# ver cuales ids no estan en oracle
if missing > 0:
    c.execute("SELECT id FROM dbo.cdc_outbox ORDER BY id")
    all_ids = [r[0] for r in c.fetchall()]
    co.execute("SELECT ID FROM FCME_USER.CDC_INBOX ORDER BY ID")
    or_ids = {r[0] for r in co.fetchall()}
    miss_ids = [i for i in all_ids if i not in or_ids]
    print(f"  IDs faltantes en oracle: {miss_ids[:30]}")

# Restart sink si failed
if isinstance(s, dict):
    cstate = s.get("connector",{}).get("state")
    failed = any(t.get("state")=="FAILED" for t in s.get("tasks",[]))
    if cstate == "FAILED" or failed:
        print("\n>> Restart connector + tasks")
        st, r = http("POST", f"/connectors/{NAME}/restart?includeTasks=true&onlyFailed=true")
        print(f"  HTTP {st}")
        time.sleep(10)
        st, s2 = http("GET", f"/connectors/{NAME}/status")
        print(json.dumps(s2, indent=2)[:1500] if isinstance(s2,dict) else s2)
