"""Prueba REAL de inserts_garantia.txt: parsea y ejecuta cada INSERT en su BD,
luego hace ROLLBACK global para no persistir datos.

Anti-zombi:
  - autocommit=False (necesario para BEGIN TRAN/ROLLBACK)
  - Cada conexion abre TRAN, al final ROLLBACK forzado
  - try/finally garantiza ROLLBACK aunque falle
  - signal handler + atexit cierran limpio
"""
import sys, re, signal, atexit
import pyodbc
from collections import defaultdict, OrderedDict

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
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_test_inserts_garantia_out.txt","w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
_conns={}
def cn(db):
    if db in _conns: return _conns[db]
    c=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=False, timeout=10)
    c.timeout=30
    cur=c.cursor()
    cur.execute("SET LOCK_TIMEOUT 5000")
    _conns[db]=c
    return c

def cleanup_rollback():
    """ROLLBACK forzado en TODAS las conexiones, despues close."""
    print("\n[cleanup] ROLLBACK + close en todas las conexiones...")
    for db, c in list(_conns.items()):
        try:
            c.rollback()
            print(f"  ROLLBACK {db}")
        except Exception as e:
            print(f"  ROLLBACK {db} ERR: {e}")
    for db, c in list(_conns.items()):
        try:
            c.close()
        except: pass
    _conns.clear()

atexit.register(cleanup_rollback)
for s in (signal.SIGINT, signal.SIGTERM):
    signal.signal(s, lambda *a:(cleanup_rollback(),sys.exit(130)))

# Parsear inserts_garantia.txt
print("="*80)
print("TEST inserts_garantia.txt - ROLLBACK al final, no persiste")
print("="*80)

txt=open(r"C:\Users\Usuario\Downloads\Bulk\inserts_garantia.txt","r",encoding="utf-8").read()

# Split por BD: USE [dbX]; GO marcadores
sections=re.split(r"(USE \[(\w+)\];\s*GO)", txt)
# sections es: [pre, 'USE [dbX]; GO', 'dbX', 'content', 'USE [dbY]; GO', 'dbY', 'content', ...]

current_db=None
inserts_by_db=defaultdict(list)  # db -> [(tbl, insert_sql, line_no)]

# Parser simplificado: escanear linea por linea
current_db=None
current_tbl=None
line_no=0
for raw in txt.split("\n"):
    line_no+=1
    line=raw.strip()
    m=re.match(r"USE\s+\[(\w+)\]", line)
    if m:
        current_db=m.group(1)
        continue
    m=re.match(r"-- TABLA\s*:\s*\[\w+\]\.\[\w+\]\.\[(\w+)\]", line)
    if m:
        current_tbl=m.group(1)
        continue
    if line.startswith("INSERT INTO"):
        if current_db and current_tbl:
            inserts_by_db[current_db].append((current_tbl, line, line_no))

print(f"\nParseados:")
total_inserts=0
for db, items in inserts_by_db.items():
    print(f"  {db}: {len(items)} inserts")
    total_inserts+=len(items)
print(f"  TOTAL: {total_inserts}")

# Ejecutar
print(f"\nEjecutando (con ROLLBACK garantizado al final)...")
results_by_table=defaultdict(lambda: {"ok":0,"fail":0,"errors":[]})

for db, items in inserts_by_db.items():
    print(f"\n--- BD: {db} ---")
    try:
        c=cn(db).cursor()
    except Exception as e:
        print(f"  No se pudo conectar a {db}: {e}")
        for tbl, _, ln in items:
            results_by_table[f"{db}.{tbl}"]["fail"]+=1
            results_by_table[f"{db}.{tbl}"]["errors"].append(f"conn err: {e}")
        continue

    for tbl, sql, ln in items:
        key=f"{db}.{tbl}"
        try:
            c.execute(sql)
            results_by_table[key]["ok"]+=1
        except Exception as e:
            results_by_table[key]["fail"]+=1
            err=str(e).split('\n')[0][:200]
            results_by_table[key]["errors"].append(f"L{ln}: {err}")

# Reporte
print(f"\n"+"="*80)
print("RESUMEN POR TABLA")
print("="*80)
total_ok=0; total_fail=0
for key in sorted(results_by_table.keys()):
    r=results_by_table[key]
    total_ok+=r["ok"]; total_fail+=r["fail"]
    flag="OK    " if r["fail"]==0 else "FAIL  "
    print(f"  [{flag}] {key:<45} ok={r['ok']:>2} fail={r['fail']:>2}")
    for err in r["errors"][:3]:
        print(f"          - {err}")
    if len(r["errors"])>3:
        print(f"          ... y {len(r['errors'])-3} mas")

print(f"\n--- TOTAL ---")
print(f"  OK   : {total_ok}/{total_ok+total_fail}")
print(f"  FAIL : {total_fail}")

# Cleanup hara ROLLBACK en TODAS las conexiones
