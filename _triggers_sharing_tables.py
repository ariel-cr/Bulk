"""Identifica:
  A) Tablas con MULTIPLES triggers (los que comparten tabla)
  B) Triggers que emiten MULTIPLES aggregate_types (el patron @types CROSS JOIN)
Reporta para cada BD legacy.
"""
import sys
import pyodbc
from collections import defaultdict

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
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\triggers_sharing.txt","w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def cn(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)

DBS=["dbCR","dbFC","dbCG","dbCT","dbIM","dbNO","dbSV","dbIN"]

print("="*100)
print(" TRIGGERS QUE COMPARTEN TABLAS (varios triggers sobre la misma tabla)")
print("="*100)

import re
for db in DBS:
    try:
        c=cn(db).cursor()
    except Exception as e:
        print(f"\n  [!] {db} no accesible: {str(e)[:80]}")
        continue

    # A) Tablas con multiples triggers
    c.execute("""SELECT tb.name AS tabla, COUNT(t.object_id) AS n_triggers,
                        STUFF((SELECT ', '+t2.name+(CASE WHEN t2.is_disabled=1 THEN '*DIS*' ELSE '' END)
                               FROM sys.triggers t2
                               WHERE t2.parent_id=tb.object_id
                               FOR XML PATH('')), 1, 2, '') AS lista
                 FROM sys.tables tb
                 JOIN sys.triggers t ON t.parent_id=tb.object_id
                 GROUP BY tb.name, tb.object_id
                 HAVING COUNT(t.object_id) > 1
                 ORDER BY n_triggers DESC, tb.name""")
    rows=c.fetchall()
    if not rows:
        continue

    print(f"\n--- BD: {db} ({len(rows)} tablas con >1 trigger) ---")
    print(f"  {'tabla':<35} {'#triggers':>10}  triggers (DIS=disabled)")
    print(f"  {'-'*35} {'-'*10}  {'-'*60}")
    for tabla, n, lista in rows:
        print(f"  {tabla:<35} {n:>10}  {lista}")

# B) Triggers que emiten MULTIPLES aggregate_types
print(f"\n\n"+"="*100)
print(" TRIGGERS F1 QUE EMITEN MULTIPLES aggregate_types (CROSS JOIN @types)")
print("="*100)

import re
for db in DBS:
    try:
        c=cn(db).cursor()
    except Exception:
        continue
    c.execute("""SELECT t.name, tb.name, OBJECT_DEFINITION(t.object_id)
                 FROM sys.triggers t
                 JOIN sys.tables tb ON tb.object_id=t.parent_id
                 WHERE t.name LIKE 'trg_outbox%'""")
    multi=[]
    for trg, tbl, defn in c.fetchall():
        if not defn: continue
        # Buscar VALUES (N'<agg>'),(N'<agg>'),...
        m=re.search(r"INSERT\s+INTO\s+@types[^V]*VALUES\s*([^;]+);", defn, re.IGNORECASE | re.DOTALL)
        if not m: continue
        types=re.findall(r"N'([^']+)'", m.group(1))
        if len(types)>1:
            multi.append((trg, tbl, types))
    if not multi:
        continue
    print(f"\n--- BD: {db} ({len(multi)} triggers con N>1 types) ---")
    for trg, tbl, types in multi:
        print(f"\n  {db}.{tbl}  ->  {trg}  ({len(types)} types)")
        for t in types:
            print(f"      - {t}")

# C) Resumen cartera-specifico (cargado de _cartera_specs.json)
print(f"\n\n"+"="*100)
print(" RESUMEN CARTERA: tablas legacy con N>1 aggregate_type emitido por NUESTRO trigger")
print("="*100)
import json
with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS=json.load(f)
groups=defaultdict(list)
for s in SPECS:
    if "ltbl" in s:
        groups[(s["ldb"], s["ltbl"])].append(s["agg"])
shared=[(k,v) for k,v in groups.items() if len(v)>1]
shared.sort(key=lambda x: -len(x[1]))
print(f"\n  {'tabla legacy':<45} {'#types':>7}  types")
print(f"  {'-'*45} {'-'*7}  {'-'*60}")
for (db, tbl), aggs in shared:
    print(f"  {db}.dbo.{tbl:<35} {len(aggs):>7}  {', '.join(aggs[:4])}{'...' if len(aggs)>4 else ''}")
    if len(aggs)>4:
        for a in aggs[4:]:
            print(f"  {' '*53}    {a}")

print(f"\n  Total tablas cartera con >1 type: {len(shared)}")

print("\n=== FIN ===")
print("Archivo: triggers_sharing.txt")
