"""Diagnostico de los 7 ERR_UPDATE - probe usando columna NO-FK.

Para cada tabla:
  1) Lista FK constraints (incoming = otras tablas referencian esta col)
  2) Lista cols no-FK
  3) Hace UPDATE no-op de la primera col no-FK
  4) Verifica eventos en cdc_outbox
"""
import sys, time, json, signal, atexit, pyodbc
class Tee:
    def __init__(self,*s):self.s=s
    def write(self,t):
        for x in self.s:
            try: x.write(t); x.flush()
            except: pass
    def flush(self):
        for x in self.s:
            try: x.flush()
            except: pass
sys.stdout = Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_diag_7_errs_out.txt","w",encoding="utf-8"))

DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
_conns = {}
def conn(db):
    if db in _conns: return _conns[db]
    s = f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}"
    cn = pyodbc.connect(s, autocommit=True, timeout=10)
    cn.timeout = 30
    c = cn.cursor()
    c.execute("SET LOCK_TIMEOUT 5000")
    _conns[db] = cn
    return cn

def cleanup():
    for db, cn in list(_conns.items()):
        try: cn.close()
        except: pass
    _conns.clear()

atexit.register(cleanup)
for s in (signal.SIGINT, signal.SIGTERM):
    signal.signal(s, lambda *a: (cleanup(), sys.exit(130)))

ERR_TABLES = [
    ("dbCR","crtbabno_extr"),("dbCR","crtbccbr_cred_judi"),("dbCR","crtbconv_pago"),
    ("dbCR","crtbcred_autr_deta"),("dbCR","crtbcred_liqd_diar"),
    ("dbCR","crtbdevo_masi_deta"),("dbCR","crtbsegi_autr_ofic"),
]

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)

can_cn = pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=fcme_canonicos;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
can = can_cn.cursor(); can.execute("SET LOCK_TIMEOUT 5000")
_conns['fcme_canonicos'] = can_cn

print("="*80)
print("DIAGNOSTICO 7 ERR_UPDATE - probe con col no-FK")
print("="*80)

for db, tbl in ERR_TABLES:
    src = f"{db}.dbo.{tbl}"
    aggs = [s["agg"] for s in SPECS if s.get("ldb")==db and s.get("ltbl")==tbl]
    spec = next(s for s in SPECS if s.get("ldb")==db and s.get("ltbl")==tbl)
    lkey = spec.get("lkey") or []

    print(f"\n--- {db}.{tbl} (types={len(aggs)}) ---")
    print(f"  lkey: {lkey}")

    cn = conn(db); c = cn.cursor()

    # 1) Cols con FK (saliente + entrante) usando INFORMATION_SCHEMA (mas amplio)
    c.execute("""SELECT DISTINCT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                 WHERE TABLE_NAME = ?""", tbl)
    fk_cols = {r[0] for r in c.fetchall()}

    # 2) Cols identity y computed
    c.execute("""SELECT name FROM sys.columns
                 WHERE object_id=OBJECT_ID(?) AND (is_identity=1 OR is_computed=1)""",
              f"dbo.{tbl}")
    auto_cols = {r[0] for r in c.fetchall()}

    # 3) Todas las cols
    c.execute("SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) ORDER BY column_id", f"dbo.{tbl}")
    all_cols = [r[0] for r in c.fetchall()]

    # Probe candidatos: NO lkey + NO FK (any) + NO identity/computed
    excluded = fk_cols | set(lkey) | auto_cols
    non_fk = [col for col in all_cols if col not in excluded]
    print(f"  total cols: {len(all_cols)}  excluidas (FK/lkey/identity): {len(excluded)}  candidatos: {len(non_fk)}")

    if not non_fk:
        print(f"  [SKIP] todas las cols son FK, no se puede probar")
        continue

    # 3) Probe: tomar 1 fila, hacer UPDATE no-op de la 1ra col no-FK
    probe_col = non_fk[0]
    print(f"  probe_col: {probe_col}")

    if not lkey:
        print(f"  [SKIP] sin lkey")
        continue

    placeholders = ",".join("?" * len(aggs))
    can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({placeholders})", src, *aggs)
    before = can.fetchone()[0]

    try:
        cols_q = ",".join(f"[{k}]" for k in lkey)
        c.execute(f"SELECT TOP 1 {cols_q} FROM dbo.[{tbl}]")
        row = c.fetchone()
        if not row:
            print(f"  [EMPTY]")
            continue
        where = " AND ".join(f"[{k}] = ?" for k in lkey)
        c.execute(f"UPDATE dbo.[{tbl}] SET [{probe_col}]=[{probe_col}] WHERE {where}", *row)
        n_rows = c.rowcount
        time.sleep(1.5)
        can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({placeholders})", src, *aggs)
        after = can.fetchone()[0]
        events = after - before
        expected = n_rows * len(aggs)
        flag = "OK" if events == expected else "FAIL"
        print(f"  rows={n_rows} types={len(aggs)} events={events} expected={expected}  [{flag}]")
        if events > 0:
            can.execute(f"SELECT TOP 1 LEFT(payload, 200) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({placeholders}) ORDER BY id DESC", src, *aggs)
            head = (can.fetchone() or [""])[0] or ""
            obj_count = head.count('{"')
            print(f"  payload obj_count: {obj_count}  {'OK' if obj_count==1 else 'FAIL'}")
    except Exception as e:
        print(f"  [ERR_UPDATE] {str(e)[:200]}")

print("\n[cleanup]")
cleanup()
print("done")
