"""Smoke test 60 tablas cartera con INSERT sintetico.

Por cada tabla:
  1) Introspectar schema (cols, nullable, identity, computed, defaults)
  2) Build INSERT minimal: solo cols NOT NULL no-identity no-computed
     - Valor sintetico por tipo (int=99999, char='TEST', date='2099-12-31', etc.)
  3) Try INSERT
     - FK violation -> reportar
     - Otro error -> reportar
  4) Si OK: esperar 2s, contar eventos en cdc_outbox para nuestros agg_types
  5) DELETE la fila insertada (por lkey)
  6) Esperar 2s, verificar evento DELETE

Anti-zombi:
  - autocommit=True
  - SET LOCK_TIMEOUT 5000
  - 1 conexion por BD
  - signal handler + atexit cleanup
"""
import sys, time, json, signal, atexit
import pyodbc
from collections import defaultdict, Counter

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

LOG = r"C:\Users\Usuario\Downloads\Bulk\_test_60_insert_out.txt"
sys.stdout = Tee(sys.__stdout__, open(LOG,"w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
_conns={}
def conn(db):
    if db in _conns: return _conns[db]
    cn = pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
    cn.timeout = 30
    c = cn.cursor(); c.execute("SET LOCK_TIMEOUT 5000")
    _conns[db]=cn
    return cn

def cleanup():
    print("\n[cleanup] cerrando conexiones...")
    for db, cn in list(_conns.items()):
        try: cn.close(); print(f"  closed {db}")
        except: pass
    _conns.clear()

atexit.register(cleanup)
for s in (signal.SIGINT, signal.SIGTERM):
    signal.signal(s, lambda *a:(cleanup(),sys.exit(130)))

def synth_value(dt, ml):
    """Valor sintetico por tipo de dato SQL Server."""
    dt = dt.lower()
    if 'int' in dt or 'numeric' in dt or 'decimal' in dt or 'money' in dt or 'float' in dt or 'real' in dt or 'bit' in dt:
        return 99
    if 'date' in dt or 'time' in dt:
        return '2099-12-31'
    # text
    s = 'TEST'
    if ml and 0 < ml < 4: s = s[:ml]
    return s

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)

groups = defaultdict(list)
for s in SPECS:
    if "ltbl" not in s: continue
    groups[(s["ldb"], s["ltbl"])].append(s["agg"])

# Conexion canonicos
can_cn = pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=fcme_canonicos;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
can_cn.timeout = 30
can = can_cn.cursor(); can.execute("SET LOCK_TIMEOUT 5000")
_conns['fcme_canonicos'] = can_cn

print("="*90)
print(f"SMOKE TEST INSERT - 60 tablas cartera")
print("="*90)

results = []

for idx, ((db, tbl), aggs) in enumerate(sorted(groups.items()), 1):
    spec = next(s for s in SPECS if s.get("ldb")==db and s.get("ltbl")==tbl)
    lkey = spec.get("lkey", [])
    src = f"{db}.dbo.{tbl}"
    n_types = len(aggs)

    cn = conn(db); c = cn.cursor()

    # Introspeccion
    c.execute("""SELECT c.name, t.name AS dt, c.max_length, c.is_nullable, c.is_identity, c.is_computed,
                        CASE WHEN dc.definition IS NOT NULL THEN 1 ELSE 0 END AS has_default
                 FROM sys.columns c
                 JOIN sys.types t ON t.user_type_id = c.user_type_id
                 LEFT JOIN sys.default_constraints dc ON dc.parent_object_id=c.object_id AND dc.parent_column_id=c.column_id
                 WHERE c.object_id = OBJECT_ID(?)
                 ORDER BY c.column_id""", f"dbo.{tbl}")
    cols = c.fetchall()

    # Cols a INSERT: not identity + not computed + (NOT NULL sin default OR forzar valor sintetico)
    insert_cols = []
    insert_vals = []
    for nm, dt, ml, nullable, ident, comp, has_def in cols:
        if ident or comp:
            continue
        # Si nullable y no default -> podemos saltar (queda NULL)
        if nullable and not has_def:
            continue
        # Si NOT NULL sin default O nullable con default -> proveer valor sintetico
        if not nullable or has_def:
            insert_cols.append(nm)
            insert_vals.append(synth_value(dt, ml))

    if not insert_cols:
        results.append((db, tbl, "NO_INSERT_PLAN", n_types, 0, 0, "no required cols"))
        print(f" {idx:3} {db:<6} {tbl:<35} [NO_INSERT_PLAN]")
        continue

    cols_q = ",".join(f"[{c}]" for c in insert_cols)
    placeholders = ",".join("?" * len(insert_cols))

    # Baseline cdc_outbox por aggregate_type
    ph = ",".join("?" * len(aggs))
    can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({ph})", src, *aggs)
    before = can.fetchone()[0]

    # INSERT
    inserted = False
    err = None
    try:
        c.execute(f"INSERT INTO dbo.[{tbl}] ({cols_q}) VALUES ({placeholders})", *insert_vals)
        inserted = c.rowcount > 0
    except Exception as e:
        err = str(e)[:160]

    if not inserted:
        results.append((db, tbl, "ERR_INSERT", n_types, 0, 0, err))
        print(f" {idx:3} {db:<6} {tbl:<35} [ERR_INSERT] {err}")
        continue

    # Wait y verificar
    time.sleep(2)
    can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({ph})", src, *aggs)
    after_ins = can.fetchone()[0]
    n_ins_events = after_ins - before

    # Verificar payload
    payload_ok = None
    if n_ins_events > 0:
        can.execute(f"SELECT TOP 1 LEFT(payload, 200) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({ph}) ORDER BY id DESC", src, *aggs)
        head = (can.fetchone() or [""])[0] or ""
        payload_ok = head.count('{"') == 1

    # Cleanup: DELETE la fila
    delete_err = None
    try:
        # Use lkey-based WHERE if all lkey cols are in our inserted vals
        if lkey and all(k in insert_cols for k in lkey):
            where_clause = " AND ".join(f"[{k}] = ?" for k in lkey)
            params = [insert_vals[insert_cols.index(k)] for k in lkey]
            c.execute(f"DELETE FROM dbo.[{tbl}] WHERE {where_clause}", *params)
        else:
            # Fallback: DELETE TOP 1 (riesgo: borra otra fila si alguien insert despues)
            # No hacer, mejor reportar
            delete_err = "lkey not in inserted cols"
    except Exception as e:
        delete_err = str(e)[:120]

    # Wait y contar eventos DELETE
    time.sleep(2)
    can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({ph})", src, *aggs)
    after_del = can.fetchone()[0]
    n_del_events = after_del - after_ins

    expected_ins = n_types  # 1 row inserted * n_types
    expected_del = n_types if not delete_err else 0

    if n_ins_events == expected_ins and n_del_events == expected_del and (payload_ok is None or payload_ok):
        st = "OK"
    elif n_ins_events != expected_ins:
        st = "INS_MISMATCH"
    elif n_del_events != expected_del:
        st = "DEL_MISMATCH"
    else:
        st = "BAD_PAYLOAD"

    note = (f"ins={n_ins_events}/{expected_ins} del={n_del_events}/{expected_del}"
            + (f" del_err={delete_err}" if delete_err else ""))
    results.append((db, tbl, st, n_types, n_ins_events, n_del_events, note))
    print(f" {idx:3} {db:<6} {tbl:<35} [{st:<13}] {note}")

# Resumen
print("\n" + "="*90)
print("RESUMEN")
print("="*90)
counts = Counter(r[2] for r in results)
for st, n in counts.most_common():
    print(f"  {st:<20} {n}/60")
