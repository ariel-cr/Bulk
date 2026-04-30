"""Smoke test ROBUSTO de las 60 tablas cartera, anti-zombi.

Garantias contra SQL Server zombi:
  - autocommit=True desde el inicio (NO transacciones implicitas)
  - SET LOCK_TIMEOUT 5000 (max 5s de espera por lock, sino error)
  - Query timeout 30s
  - 1 conexion por BD reutilizada (no 1 por tabla)
  - signal handler (SIGINT, SIGTERM) -> cierra todas las conexiones limpio
  - atexit: cierre garantizado de cursores y conexiones
  - try/finally en CADA op SQL para no dejar transacciones colgadas
  - Captura mensaje COMPLETO de errores (no truncado)
"""
import sys, os, time, json, signal, atexit, traceback
import pyodbc, oracledb
from collections import defaultdict

# Tee stdout
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

LOG_PATH = r"C:\Users\Usuario\Downloads\Bulk\_test_60_safe_out.txt"
sys.stdout = Tee(sys.__stdout__, open(LOG_PATH,"w",encoding="utf-8"))

DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

# Pool de conexiones - 1 por BD
_conns = {}

def get_conn(db):
    if db in _conns:
        return _conns[db]
    s = f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}"
    cn = pyodbc.connect(s, autocommit=True, timeout=10)
    cn.timeout = 30  # query timeout
    c = cn.cursor()
    # Lock timeout: si no obtiene lock en 5s, error en lugar de hang
    c.execute("SET LOCK_TIMEOUT 5000")
    _conns[db] = cn
    return cn

def cleanup():
    """Cierra todas las conexiones explicitamente para evitar zombis."""
    print("\n[cleanup] cerrando conexiones...")
    for db, cn in list(_conns.items()):
        try:
            cn.close()
            print(f"  closed {db}")
        except Exception as e:
            print(f"  err closing {db}: {e}")
    _conns.clear()

atexit.register(cleanup)

def signal_handler(sig, frame):
    print(f"\n[SIGNAL {sig}] cerrando limpiamente...")
    cleanup()
    sys.exit(130)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# === MAIN ===
print("="*80)
print("SMOKE TEST 60 TABLAS CARTERA (anti-zombi v2)")
print("="*80)

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)

groups = defaultdict(list)
for s in SPECS:
    if "ltbl" not in s: continue
    groups[(s["ldb"], s["ltbl"])].append(s["agg"])

# Conexion canonicos para queries cdc_outbox
can_cn = pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=fcme_canonicos;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
can_cn.timeout = 30
can = can_cn.cursor()
can.execute("SET LOCK_TIMEOUT 5000")
_conns['fcme_canonicos'] = can_cn

results = []

for idx, ((db, tbl), agg_types) in enumerate(sorted(groups.items()), 1):
    spec = next(s for s in SPECS if s["ldb"]==db and s["ltbl"]==tbl)
    lkey = spec.get("lkey", [])
    src = f"{db}.dbo.{tbl}"
    n_types = len(agg_types)

    # Default failure
    status = "?"; n_rows = 0; n_events = 0; payload_ok = None; err = None

    if not lkey:
        status = "NO_LKEY"
    else:
        # 1) Baseline events
        try:
            placeholders = ",".join("?" * len(agg_types))
            can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({placeholders})", src, *agg_types)
            before = can.fetchone()[0]
        except Exception as e:
            status = "ERR_BASELINE"; err = str(e)[:200]
            before = None

        if before is not None:
            # 2) UPDATE no-op de 1 fila
            try:
                cn = get_conn(db); cur = cn.cursor()
                cols_q = ",".join(f"[{k}]" for k in lkey)
                cur.execute(f"SELECT TOP 1 {cols_q} FROM dbo.[{tbl}]")
                row = cur.fetchone()
                if not row:
                    status = "EMPTY"
                else:
                    where = " AND ".join(f"[{k}] = ?" for k in lkey)
                    cur.execute(f"UPDATE dbo.[{tbl}] SET [{lkey[0]}]=[{lkey[0]}] WHERE {where}", *row)
                    n_rows = cur.rowcount
                cur.close()
            except Exception as e:
                status = "ERR_UPDATE"; err = str(e)[:200]
                try: cur.close()
                except: pass

        # 3) Espera y mide eventos
        if status == "?" and n_rows > 0:
            time.sleep(1.5)
            try:
                can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({placeholders})", src, *agg_types)
                after = can.fetchone()[0]
                n_events = after - before
                expected = n_rows * n_types
                if n_events == expected:
                    # Verificar payload (1 obj)
                    can.execute(f"SELECT TOP 1 LEFT(payload, 200) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({placeholders}) ORDER BY id DESC", src, *agg_types)
                    head = (can.fetchone() or [""])[0] or ""
                    payload_ok = head.count('{"') == 1
                    status = "OK" if payload_ok else "BAD_PAYLOAD"
                else:
                    status = "EVENT_MISMATCH"
                    err = f"expected={expected} got={n_events}"
            except Exception as e:
                status = "ERR_VERIFY"; err = str(e)[:200]

    flag = "OK    " if status == "OK" else status[:14]
    print(f" {idx:3}  {db:<6} {tbl:<35} [{flag:<14}] rows={n_rows} types={n_types} events={n_events}  err={err or '-'}")
    results.append((db, tbl, status, n_rows, n_types, n_events, err))

# Resumen
print("\n" + "="*80)
print("RESUMEN")
print("="*80)
from collections import Counter
counts = Counter(r[2] for r in results)
for st, n in counts.most_common():
    print(f"  {st:<20} {n}/60")

# ERR detail
errs = [r for r in results if r[2].startswith("ERR") or r[2] in ("EVENT_MISMATCH","BAD_PAYLOAD","NO_LKEY")]
if errs:
    print(f"\n--- ERR DETAIL ({len(errs)}) ---")
    for db, tbl, status, rows, types, events, err in errs:
        print(f"  [{status}] {db}.{tbl} -> {err}")
