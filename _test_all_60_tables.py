"""Smoke test sistematico sobre las 60 tablas legacy de cartera (v2 con tee a archivo)."""
import sys, time, json, pyodbc
class Tee:
    def __init__(self,*s):self.s=s
    def write(self,t):
        for x in self.s: x.write(t); x.flush()
    def flush(self):
        for x in self.s: x.flush()
sys.stdout = Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_test_60_output.txt","w",encoding="utf-8"))
from collections import defaultdict
DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=30)

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)

# Agrupar por (ldb, ltbl) -> [agg_types]
groups = defaultdict(list)
for s in SPECS:
    if "ltbl" not in s: continue
    groups[(s["ldb"], s["ltbl"])].append(s["agg"])

can = sql("fcme_canonicos").cursor()

results = []  # (db, tbl, status, n_rows, n_types, n_events, payload_ok)

for (db, tbl), agg_types in sorted(groups.items()):
    spec = next(s for s in SPECS if s["ldb"]==db and s["ltbl"]==tbl)
    lkey = spec.get("lkey", [])
    if not lkey:
        results.append((db, tbl, "NO_LKEY", 0, len(agg_types), 0, None))
        continue

    n_types = len(agg_types)
    src = f"{db}.dbo.{tbl}"

    # Filtrar SOLO por nuestros aggregate_types (evita contar eventos de triggers del equipo)
    placeholders = ",".join("?" * len(agg_types))
    can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({placeholders})", src, *agg_types)
    before = can.fetchone()[0]

    try:
        c = sql(db).cursor()
        cols_q = ",".join(f"[{k}]" for k in lkey)
        c.execute(f"SELECT TOP 1 {cols_q} FROM dbo.[{tbl}]")
        row = c.fetchone()
        if not row:
            results.append((db, tbl, "EMPTY", 0, n_types, 0, None))
            continue

        where = " AND ".join(f"[{k}] = ?" for k in lkey)
        c.execute(f"UPDATE dbo.[{tbl}] SET [{lkey[0]}]=[{lkey[0]}] WHERE {where}", *row)
        n_rows = c.rowcount
    except Exception as e:
        results.append((db, tbl, f"ERR:{str(e)[:80]}", 0, n_types, 0, None))
        continue

    time.sleep(1.5)
    can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({placeholders})", src, *agg_types)
    after = can.fetchone()[0]
    n_events = after - before

    expected = n_rows * n_types

    # check payload (1 obj per event) - solo nuestros types
    payload_ok = None
    if n_events > 0:
        can.execute(f"SELECT TOP 1 LEFT(payload, 200) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({placeholders}) ORDER BY id DESC", src, *agg_types)
        head = can.fetchone()[0] or ""
        obj_count = head.count('{"')
        payload_ok = obj_count == 1

    status = "OK" if (n_events == expected and (payload_ok or n_events == 0)) else "FAIL"
    results.append((db, tbl, status, n_rows, n_types, n_events, payload_ok))

# Reporte
print(f"{'#':>3}  {'BD':<6} {'tabla':<35} {'status':<8} rows  types  events  expected payload")
print("-"*100)
n_ok=0; n_empty=0; n_fail=0; n_err=0
for i,(db, tbl, status, rows, types, events, pl) in enumerate(results, 1):
    expected = rows * types if rows else "-"
    pl_str = "OK" if pl else ("--" if pl is None else "FAIL")
    flag = status if len(status) < 9 else "ERR"
    print(f"{i:3}  {db:<6} {tbl:<35} [{flag:<6}] {rows:>4}  {types:>5}  {events:>6}  {str(expected):>8}  {pl_str}")
    if status == "OK": n_ok += 1
    elif status == "EMPTY": n_empty += 1
    elif status == "FAIL": n_fail += 1
    else: n_err += 1

print(f"\n=== RESUMEN ===")
print(f"  OK    : {n_ok}/60")
print(f"  EMPTY : {n_empty}/60 (tablas sin filas)")
print(f"  FAIL  : {n_fail}/60")
print(f"  ERR   : {n_err}/60")
