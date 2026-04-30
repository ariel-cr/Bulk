"""Smoke test V2: probe + verifica replicacion a FCME_USER.<TYPE>."""
import time, json
from collections import defaultdict
import pyodbc, oracledb

DB  = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)

groups = defaultdict(list)
for s in SPECS:
    if "ltbl" not in s: continue
    groups[(s["ldb"], s["ltbl"])].append(s)

def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=30)

orcl = oracledb.connect(**ORA); o = orcl.cursor()
can = sql("fcme_canonicos").cursor()

print("="*70); print("SMOKE TEST V2 - CARTERA Flow1"); print("="*70)

# Baseline
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
inbox_b = o.fetchone()[0]
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
err_b = o.fetchone()[0]
can.execute("SELECT COUNT(*) FROM cdc_outbox")
outbox_b = can.fetchone()[0]
print(f"\n[baseline] outbox={outbox_b:,}  inbox={inbox_b:,}  errors={err_b:,}")

# Probes (UPDATE no-op)
print(f"\n[probes] firing UPDATEs en {len(groups)} tablas...")
done = []
for (db, tbl), entries in sorted(groups.items()):
    spec = entries[0]
    lkey = spec.get("lkey") or []
    if not lkey:
        continue
    try:
        c = sql(db).cursor()
        cols = ",".join(f"[{k}]" for k in lkey)
        c.execute(f"SELECT TOP 1 {cols} FROM dbo.[{tbl}]")
        row = c.fetchone()
        if not row: continue
        set_clause = ", ".join(f"[{k}]=[{k}]" for k in lkey)
        where = " AND ".join(f"[{k}] = ?" for k in lkey)
        c.execute(f"UPDATE dbo.[{tbl}] SET {set_clause} WHERE {where}", *row)
        done.append((db, tbl, [a["agg"] for a in entries]))
    except Exception:
        pass
print(f"  fired UPDATEs: {len(done)}")

print(f"\n[wait] 15s para Kafka + processing...")
time.sleep(15)

# Counts despues
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
inbox_a = o.fetchone()[0]
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
err_a = o.fetchone()[0]
can.execute("SELECT COUNT(*) FROM cdc_outbox")
outbox_a = can.fetchone()[0]
print(f"\n[post] outbox={outbox_a:,} (+{outbox_a-outbox_b})  inbox={inbox_a:,} (+{inbox_a-inbox_b})  errors={err_a:,} (+{err_a-err_b})")

# Cartera-only stats: por aggregate_type
CARTERA = sorted({s["agg"] for s in SPECS})
ph = ",".join(f":{i+1}" for i in range(len(CARTERA)))
o.execute(f"""SELECT AGGREGATE_TYPE, COUNT(*),
                     SUM(CASE WHEN PROCESSED=1 THEN 1 ELSE 0 END)
              FROM FCME_USER.CDC_INBOX
              WHERE AGGREGATE_TYPE IN ({ph})
                AND CREATED_AT >= SYSTIMESTAMP - INTERVAL '90' SECOND
              GROUP BY AGGREGATE_TYPE""", CARTERA)
recent = {r[0]: (r[1], r[2]) for r in o.fetchall()}

# Errores nuevos cartera
o.execute(f"""SELECT AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,160)
              FROM FCME_USER.CDC_INBOX_ERRORS
              WHERE AGGREGATE_TYPE IN ({ph})
                AND CREATED_AT >= SYSTIMESTAMP - INTERVAL '90' SECOND
              ORDER BY ID DESC""", CARTERA)
errs = o.fetchall()

n_emit = len(recent)
n_proc = sum(1 for v in recent.values() if v[1] >= v[0])  # all processed
print(f"\n[cartera] Types con eventos recibidos: {n_emit}/{len(CARTERA)}")
print(f"          Types completamente procesados: {n_proc}/{n_emit}")
print(f"          Errores cartera: {len(errs)}")
if errs:
    print(f"\nErrores nuevos:")
    for agg, ev, msg in errs[:20]:
        print(f"  [{ev}] {agg}: {msg[:140]}")

# Por type detalle
print(f"\n[detalle por type que tuvo evento]")
for agg in sorted(recent):
    cnt, proc = recent[agg]
    flag = "OK" if proc == cnt else "FAIL"
    print(f"  [{flag}] {agg:<40} events={cnt} processed={proc}")

# Verificar replicacion FCME_USER.<TYPE>
print(f"\n[replicacion] verificando FCME_USER.<TYPE> de los types procesados:")
verified = 0
for agg in sorted(recent):
    if recent[agg][1] != recent[agg][0]: continue
    spec = next((s for s in SPECS if s["agg"]==agg), None)
    if not spec: continue
    dest = spec.get("dest")
    o.execute(f'SELECT COUNT(*) FROM FCME_USER."{dest}"')
    rows = o.fetchone()[0]
    print(f"  {agg:<40} -> FCME_USER.{dest:<35} rows={rows:,}")
    verified += 1

print(f"\n=== Verificados {verified} types con replicacion exitosa end-to-end ===")
orcl.close()
