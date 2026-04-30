"""Reset offsets del sink (Solucion 4): saltar backlog 'podrido'.

NO toca:
  - Source connector (sigue publicando)
  - Triggers Oracle, CDC_OUTBOX
  - Tablas SQL (cdc_inbox, module_config, errors, parsed)
  - Wrappers, CRUDs, dispatcher

SI toca:
  - Sink: STOP -> DELETE offsets -> agregar auto.offset.reset=latest -> RESUME
  - Resultado: el sink consumira solo mensajes nuevos publicados despues del reset.
"""
import json, urllib.request, time, oracledb, pyodbc

BASE = "http://10.35.3.223:30083"
SINK = "newcore-canonicos-cdc-inbox-jdbc-sink"
SOURCE = "newcore-oracle-cdc-outbox-source"

def http(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode() or "{}"
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read().decode() or "{}")
        except: return e.code, e.read().decode() if e.fp else ""
    except Exception as e:
        return None, str(e)

def snapshot(label):
    print(f"\n[Snapshot {label}]")
    st, src = http("GET", f"/connectors/{SOURCE}/offsets")
    print(f"  source.last_id: {src}")
    st, snk = http("GET", f"/connectors/{SINK}/offsets")
    print(f"  sink.kafka_offset: {snk}")
    st, sst = http("GET", f"/connectors/{SINK}/status")
    state = sst.get('connector',{}).get('state','?') if isinstance(sst,dict) else '?'
    tstate = sst.get('tasks',[{}])[0].get('state','?') if isinstance(sst,dict) and sst.get('tasks') else '?'
    print(f"  sink.state: connector={state} task={tstate}")

    c = pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123', autocommit=True).cursor()
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
    print(f"  cdc_inbox: {c.fetchone()[0]} filas")
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
    print(f"  cdc_inbox_errors: {c.fetchone()[0]} filas")

snapshot("INICIAL")

# === [1] STOP sink ===
print("\n" + "="*70)
print("[1] STOP sink")
print("="*70)
st, body = http("PUT", f"/connectors/{SINK}/stop")
print(f"  PUT /stop -> HTTP {st}")
# esperar a STOPPED
for i in range(20):
    time.sleep(1)
    st, s = http("GET", f"/connectors/{SINK}/status")
    state = s.get('connector',{}).get('state','?') if isinstance(s,dict) else '?'
    if state == 'STOPPED':
        print(f"  STOPPED tras {i+1}s")
        break
    print(f"  T+{i+1}s: state={state}")

# === [2] DELETE offsets ===
print("\n" + "="*70)
print("[2] DELETE offsets del sink (limpia consumer-group)")
print("="*70)
st, body = http("DELETE", f"/connectors/{SINK}/offsets")
print(f"  DELETE /offsets -> HTTP {st}  body={str(body)[:200]}")

# === [3] Agregar auto.offset.reset=latest ===
print("\n" + "="*70)
print("[3] Agregar consumer.override.auto.offset.reset=latest a la config")
print("="*70)
st, cfg = http("GET", f"/connectors/{SINK}/config")
cfg['consumer.override.auto.offset.reset'] = 'latest'
st, body = http("PUT", f"/connectors/{SINK}/config", cfg)
print(f"  PUT /config -> HTTP {st}")

# === [4] RESUME sink ===
print("\n" + "="*70)
print("[4] RESUME sink")
print("="*70)
st, body = http("PUT", f"/connectors/{SINK}/resume")
print(f"  PUT /resume -> HTTP {st}")
for i in range(20):
    time.sleep(2)
    st, s = http("GET", f"/connectors/{SINK}/status")
    state = s.get('connector',{}).get('state','?') if isinstance(s,dict) else '?'
    tstate = s.get('tasks',[{}])[0].get('state','?') if isinstance(s,dict) and s.get('tasks') else '?'
    if state == 'RUNNING' and tstate == 'RUNNING':
        print(f"  RUNNING tras {(i+1)*2}s")
        break
    print(f"  T+{(i+1)*2}s: connector={state} task={tstate}")

snapshot("DESPUES DEL RESET")

# === [5] Test smoke: insertar un evento simple y verificar que llega ===
print("\n" + "="*70)
print("[5] Test smoke: REFERENCIAPARTICIPE_TYPE")
print("="*70)
o = oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co = o.cursor()
c = pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123', autocommit=True).cursor()

# Solo limpieza de las filas test del piloto previo (no toca data real)
c.execute("DELETE FROM dbo.cdc_inbox WHERE aggregate_id IN ('Z1','D1','K7','K3','K4','K5','K6','99','88')")

# Disparar evento
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='RST'")
co.execute("INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('RST','reset test')")
o.commit()
print("  Oracle INSERT")

for i in range(15):
    time.sleep(2)
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE aggregate_id='RST'")
    n = c.fetchone()[0]
    if n > 0:
        c.execute("SELECT id, aggregate_id, aggregate_type, processed FROM dbo.cdc_inbox WHERE aggregate_id='RST'")
        for r in c.fetchall(): print(f"  inbox: {r}")
        c_fc = pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=dbFC;UID=sa;PWD=YourPassword123', autocommit=True).cursor()
        c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref=999 OR ds_tref='reset test'")
        rows = c_fc.fetchall()
        print(f"  legacy hits: {len(rows)}")
        for r in rows: print(f"    {r}")
        break
    print(f"  T+{(i+1)*2}s: inbox_RST={n}")

# Cleanup
print("\n[Cleanup test]")
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='RST'")
o.commit()
c.execute("DELETE FROM dbo.cdc_inbox WHERE aggregate_id='RST'")
c_fc = pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=dbFC;UID=sa;PWD=YourPassword123', autocommit=True).cursor()
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 1")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE ds_tref='reset test'")
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 0")
o.close()

snapshot("FINAL")

print("\n=== Reset completado ===")
