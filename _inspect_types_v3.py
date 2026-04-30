"""Analisis preciso usando sys.sql_expression_dependencies + sys.synonyms.
Resuelve sinonimos y descubre BDs originales reales por cada sp_*Type."""
import pyodbc
from collections import defaultdict

DB = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123",
    "database": "fcme_canonicos",
}

SYS_DBS = {"fcme_canonicos","fcme_legacy","fcme_newcore","master","tempdb","msdb","model"}

def conn():
    s = (f"DRIVER={DB['driver']};SERVER={DB['server']};"
         f"DATABASE={DB['database']};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s)

cur = conn().cursor()

# 1) Lista SPs _type
cur.execute("""
 SELECT s.name sch, o.name nm
 FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id
 WHERE o.type='P' AND s.name='participes'
   AND (o.name LIKE '%Type' OR o.name LIKE '%_type')
   AND o.name NOT LIKE '%_crud' AND o.name NOT LIKE '%_dep'
 ORDER BY o.name
""")
sps = [(r.sch, r.nm) for r in cur.fetchall()]

# 2) Cache de sinonimos en fcme_canonicos: name -> base_object_name ([db].[sch].[obj])
cur.execute("""
 SELECT s.name AS sch, sy.name AS nm, sy.base_object_name AS tgt
 FROM sys.synonyms sy JOIN sys.schemas s ON sy.schema_id=s.schema_id
""")
syn = {f"{r.sch}.{r.nm}": r.tgt for r in cur.fetchall()}

def parse_three_part(s):
    # "[db].[sch].[obj]" o "db.sch.obj"
    parts = []
    cur_s = s
    for _ in range(3):
        cur_s = cur_s.lstrip(". ")
        if cur_s.startswith("["):
            end = cur_s.index("]")
            parts.append(cur_s[1:end]); cur_s = cur_s[end+1:]
        else:
            # hasta siguiente punto
            dot = cur_s.find(".")
            if dot == -1:
                parts.append(cur_s); break
            parts.append(cur_s[:dot]); cur_s = cur_s[dot:]
    while len(parts) < 3: parts.insert(0, "")
    return parts  # [db, sch, obj]

detail = {}  # sp -> {db: set(tabla)}
for sch, nm in sps:
    cur.execute("""
      SELECT referenced_database_name AS db,
             referenced_schema_name AS sch,
             referenced_entity_name AS obj,
             referenced_class_desc AS cls
      FROM sys.sql_expression_dependencies
      WHERE referencing_id = OBJECT_ID(?)
    """, f"[{sch}].[{nm}]")
    rows = cur.fetchall()
    dbs = defaultdict(set)
    for r in rows:
        db, s_, obj = r.db, r.sch, r.obj
        # caso 1: referencia ya calificada con BD
        if db and db not in SYS_DBS:
            dbs[db].add(f"{s_ or ''}.{obj}".lstrip("."))
            continue
        # caso 2: referencia local a fcme_canonicos -> puede ser sinonimo
        syn_key = f"{s_ or 'dbo'}.{obj}"
        if syn_key in syn:
            db2, s2, obj2 = parse_three_part(syn[syn_key])
            if db2 and db2 not in SYS_DBS:
                dbs[db2].add(f"{s2 or ''}.{obj2}".lstrip("."))
            else:
                # sinonimo que apunta dentro de fcme_canonicos
                dbs["fcme_canonicos"].add(f"{s2 or ''}.{obj2}".lstrip("."))
        else:
            # referencia local real en fcme_canonicos
            if obj:
                dbs["fcme_canonicos"].add(f"{s_ or 'dbo'}.{obj}")
    detail[nm] = dbs

# salida
print("="*140)
print(f"{'TYPE SP':<52} {'BDs (sin sistema)':<30} TABLAS ORIGINALES")
print("="*140)
for sch, nm in sps:
    dbs = detail[nm]
    ext = {d:v for d,v in dbs.items() if d != "fcme_canonicos"}
    dbs_list = ", ".join(sorted(ext)) or "(ninguna externa)"
    tabs = [f"{d}.{t}" for d in sorted(ext) for t in sorted(ext[d])]
    if not tabs:
        can = sorted(dbs.get("fcme_canonicos", []))
        extra = f"  [canonicos: {', '.join(can[:3])}{'...' if len(can)>3 else ''}]" if can else ""
        print(f"{nm:<52} {dbs_list:<30} -{extra}"); continue
    print(f"{nm:<52} {dbs_list:<30} {tabs[0]}")
    for t in tabs[1:]:
        print(f"{'':<52} {'':<30} {t}")

# Resumen inverso
print("\n" + "="*80)
print("RESUMEN: BD ORIGINAL -> TYPES que la usan")
print("="*80)
by_db = defaultdict(set)
for sp, dbs in detail.items():
    for d in dbs:
        if d != "fcme_canonicos": by_db[d].add(sp)
for d in sorted(by_db):
    print(f"\n[{d}]  ({len(by_db[d])} types)")
    for s in sorted(by_db[d]): print(f"   - {s}")

print("\n" + "="*80)
print("RESUMEN: TABLA ORIGINAL -> TYPES")
print("="*80)
by_t = defaultdict(set)
for sp, dbs in detail.items():
    for d, tabs in dbs.items():
        if d == "fcme_canonicos": continue
        for t in tabs: by_t[f"{d}.{t}"].add(sp)
for t in sorted(by_t):
    print(f"\n{t}")
    for s in sorted(by_t[t]): print(f"   - {s}")
