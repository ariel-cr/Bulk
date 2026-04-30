"""Smoke test E2E del Flujo 1 cartera.

Estrategia segura:
  - Para cada una de las 60 tablas legacy, busca 1 fila existente (SELECT TOP 1)
  - Hace UPDATE no-op (col_pk = col_pk) -> fires trg_outbox_<tabla>
  - Si no hay filas, intenta INSERT minimal solo con PK (puede fallar por NOT NULL,
    se reporta y continua)
  - Espera ~10s para Kafka
  - Cuenta deltas en cdc_outbox / CDC_INBOX / CDC_INBOX_ERRORS / FCME_USER destinos
  - Reporta success/fail por type
"""
import time, json, traceback
import pyodbc, oracledb

DB  = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)

# Agrupar por (ldb, ltbl) -> primer spec representativo (uso lkey de el)
from collections import defaultdict
groups = defaultdict(list)
for s in SPECS:
    if "ltbl" not in s: continue
    groups[(s["ldb"], s["ltbl"])].append(s)

def sql(db):
    s = f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}"
    return pyodbc.connect(s, autocommit=True, timeout=30)

# 1) BASELINE
print("="*70); print("SMOKE TEST FLUJO 1 CARTERA"); print("="*70)
print("\n[1] Baseline counts...")

orcl = oracledb.connect(**ORA); o = orcl.cursor()
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
inbox_before = o.fetchone()[0]
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
errors_before = o.fetchone()[0]

can = sql("fcme_canonicos").cursor()
can.execute("SELECT COUNT(*) FROM cdc_outbox")
outbox_before = can.fetchone()[0]
print(f"  cdc_outbox       : {outbox_before:,}")
print(f"  CDC_INBOX        : {inbox_before:,}")
print(f"  CDC_INBOX_ERRORS : {errors_before:,}")

# 2) Trigger probes (UPDATE no-op por tabla)
print(f"\n[2] Probing {len(groups)} legacy tables...")

probes = []  # (db, tbl, status, agg_types_emitted)
for (db, tbl), entries in sorted(groups.items()):
    spec = entries[0]
    lkey = spec.get("lkey") or []
    agg_list = [e["agg"] for e in entries]
    if not lkey:
        probes.append((db, tbl, "NO_PK", agg_list, ""))
        continue
    try:
        c = sql(db).cursor()
        # Buscar 1 fila existente
        cols = ",".join(f"[{k}]" for k in lkey)
        c.execute(f"SELECT TOP 1 {cols} FROM dbo.[{tbl}]")
        row = c.fetchone()
        if row:
            # UPDATE no-op - usamos col_pk = col_pk (no-op real)
            set_clause = ", ".join(f"[{k}]=[{k}]" for k in lkey)
            where = " AND ".join(f"[{k}] = ?" for k in lkey)
            params = list(row)
            c.execute(f"UPDATE dbo.[{tbl}] SET {set_clause} WHERE {where}", *params)
            probes.append((db, tbl, "UPDATE_OK", agg_list, "|".join(str(v) for v in row)))
        else:
            probes.append((db, tbl, "EMPTY_TBL", agg_list, ""))
    except Exception as e:
        probes.append((db, tbl, f"ERR:{str(e)[:80]}", agg_list, ""))

ok_n = sum(1 for p in probes if p[2]=="UPDATE_OK")
empty_n = sum(1 for p in probes if p[2]=="EMPTY_TBL")
err_n = sum(1 for p in probes if p[2].startswith("ERR"))
print(f"  UPDATE_OK : {ok_n}")
print(f"  EMPTY     : {empty_n}")
print(f"  ERR       : {err_n}")

if ok_n == 0:
    print("\nNingun probe exitoso. Abortando.")
    raise SystemExit

# 3) Wait Kafka
print(f"\n[3] Waiting 12s for Kafka propagation...")
time.sleep(12)

# 4) Counts despues
print(f"\n[4] Post counts:")
can.execute("SELECT COUNT(*) FROM cdc_outbox")
outbox_after = can.fetchone()[0]
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
inbox_after = o.fetchone()[0]
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
errors_after = o.fetchone()[0]
print(f"  cdc_outbox       : {outbox_after:,}  (delta +{outbox_after-outbox_before})")
print(f"  CDC_INBOX        : {inbox_after:,}  (delta +{inbox_after-inbox_before})")
print(f"  CDC_INBOX_ERRORS : {errors_after:,}  (delta +{errors_after-errors_before})")

# 5) Por aggregate_type, ver eventos emitidos
print(f"\n[5] Eventos en CDC_INBOX por aggregate_type (ultimos {inbox_after-inbox_before}):")
o.execute(f"""SELECT AGGREGATE_TYPE, COUNT(*), SUM(CASE WHEN PROCESSED=1 THEN 1 ELSE 0 END) AS P
              FROM FCME_USER.CDC_INBOX
              WHERE ID > (SELECT MAX(ID)-{inbox_after-inbox_before+5} FROM FCME_USER.CDC_INBOX)
              GROUP BY AGGREGATE_TYPE ORDER BY 2 DESC""")
inbox_by_type = {r[0]: (r[1], r[2]) for r in o.fetchall()}
for agg, (cnt, proc) in sorted(inbox_by_type.items()):
    print(f"  {agg:<40}  events={cnt}  processed={proc}")

# 6) Errores nuevos
if errors_after > errors_before:
    print(f"\n[6] Nuevos errores ({errors_after-errors_before}):")
    o.execute(f"""SELECT AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,160)
                  FROM FCME_USER.CDC_INBOX_ERRORS
                  WHERE ID > (SELECT MAX(ID)-{errors_after-errors_before+5} FROM FCME_USER.CDC_INBOX_ERRORS)
                  ORDER BY ID DESC""")
    for agg, ev, msg in o.fetchall():
        print(f"  [{ev}] {agg}: {msg}")
else:
    print(f"\n[6] 0 errores nuevos")

# 7) Resumen por type
print(f"\n[7] Resumen por agg_type:")
all_aggs = sorted({a for _,_,_,aggs,_ in probes for a in aggs})
n_in_inbox = sum(1 for a in all_aggs if a in inbox_by_type)
print(f"  Types con evento en CDC_INBOX: {n_in_inbox}/{len(all_aggs)}")
print(f"  Types sin evento (no probe o probe fallo o evento perdido):")
for a in all_aggs:
    if a not in inbox_by_type:
        print(f"    - {a}")

orcl.close()
print(f"\n=== SMOKE TEST COMPLETO ===")
