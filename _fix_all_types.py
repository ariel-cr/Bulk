"""Arregla duplicación + inspecciona qué tablas Oracle _TYPE tienen mapeo 1:1 directo
con el payload legacy (snake_case), vs cuáles requieren mapeo semántico (camelCase).
"""
import pyodbc, oracledb, re

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql_conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# 1) Listar TODAS las tablas Oracle _TYPE en FCME_USER
print("== [1] Tablas Oracle _TYPE ==")
co.execute("""
  SELECT table_name FROM all_tables
  WHERE owner='FCME_USER' AND table_name LIKE '%TYPE%'
  ORDER BY table_name
""")
all_oracle_types = [r[0] for r in co.fetchall()]
print(f"  total: {len(all_oracle_types)}")

# 2) Clasificar por estilo de naming
snake_case = [t for t in all_oracle_types if "_" in t]
camel_case = [t for t in all_oracle_types if "_" not in t]
print(f"  snake_case (con underscore): {len(snake_case)}")
print(f"  camelCase (sin underscore):  {len(camel_case)}")

# 3) Revisar columnas del primer snake_case y primer camelCase para ver estilo
for style, tables in [("SNAKE", snake_case[:3]), ("CAMEL", camel_case[:3])]:
    print(f"\n-- muestra estilo {style} --")
    for tbl in tables:
        co.execute("""
          SELECT column_name FROM all_tab_columns
          WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id FETCH FIRST 6 ROWS ONLY
        """, t=tbl)
        cols = [r[0] for r in co.fetchall()]
        print(f"  {tbl}: {cols}")

# 4) Los 36 aggregate_types que yo emito desde el trigger (según cdc_table_to_types)
print("\n== [2] Mis 36 aggregate_types canonicos ==")
c = sql_conn("fcme_canonicos").cursor()
c.execute("SELECT DISTINCT aggregate_type_emit FROM dbo.cdc_table_to_types WHERE aggregate_type_emit IS NOT NULL ORDER BY aggregate_type_emit")
my_types = [r[0] for r in c.fetchall()]
for t in my_types: print(f"  {t}")

# 5) Matchear cada aggregate_type con una tabla Oracle (por nombre similar)
print("\n== [3] Match aggregate_type -> Oracle table ==")
def norm(s): return re.sub(r'[_\s]','', s).upper()

match_results = []
for at in my_types:
    at_norm = norm(at)
    matches = [t for t in all_oracle_types if norm(t) == at_norm]
    if not matches:
        # intento parcial
        matches = [t for t in all_oracle_types if at_norm.replace("TYPE","") in norm(t)]
    match_results.append((at, matches))

for at, ms in match_results:
    print(f"  {at:<40} -> {ms if ms else '(NO MATCH)'}")

# 6) Para cada aggregate_type, ver las tablas legacy que lo alimentan (del cdc_table_to_types)
print("\n== [4] Tabla legacy -> aggregate_type (para mapping) ==")
c.execute("""
  SELECT source_table, aggregate_type_emit FROM dbo.cdc_table_to_types
  WHERE aggregate_type_emit IS NOT NULL ORDER BY source_table, aggregate_type_emit
""")
from collections import defaultdict
by_src = defaultdict(list)
for r in c.fetchall():
    by_src[r.source_table].append(r.aggregate_type_emit)
for src in sorted(by_src):
    print(f"  {src:<30} -> {by_src[src]}")

# 7) Totales
print("\n== [5] Resumen ==")
mapped_count = sum(1 for _, ms in match_results if ms)
print(f"  aggregate_types con tabla Oracle identificable: {mapped_count}/{len(my_types)}")
print(f"  tablas legacy afectadas: {len(by_src)}")
