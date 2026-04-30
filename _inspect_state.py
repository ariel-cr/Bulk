"""Inspecciona estado real CDC: cdc_outbox/inbox en cada BD, tablas PARTICIPE en newcore,
SPs existentes en newcore y canonicos."""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

for db in ["fcme_legacy","fcme_canonicos","fcme_newcore"]:
    c = conn(db).cursor()
    print(f"\n========== {db} ==========")
    # tablas cdc_*
    c.execute("""
      SELECT s.name sch, t.name tbl
      FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
      WHERE t.name LIKE 'cdc_%' ORDER BY t.name
    """)
    rows = c.fetchall()
    print(f"Tablas cdc_*: {len(rows)}")
    for r in rows: print(f"  {r.sch}.{r.tbl}")

# Estructuras cdc_inbox
print("\n========== cdc_inbox (fcme_canonicos) ==========")
c = conn("fcme_canonicos").cursor()
c.execute("""
 SELECT c.name, t.name tp, c.max_length, c.is_nullable
 FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
 WHERE c.object_id=OBJECT_ID('dbo.cdc_inbox') ORDER BY c.column_id
""")
for r in c.fetchall():
    print(f"  {r.name:<20} {r.tp:<15} max={r.max_length}  null={r.is_nullable}")

# Triggers existentes en cdc_inbox
print("\n========== Triggers sobre cdc_inbox en canonicos ==========")
c.execute("""
 SELECT tr.name, tr.is_disabled, OBJECT_NAME(tr.parent_id) parent
 FROM sys.triggers tr WHERE OBJECT_NAME(tr.parent_id) IN ('cdc_inbox','cdc_outbox')
""")
for r in c.fetchall(): print(f"  {r.parent} -> {r.name}  disabled={r.is_disabled}")

# SPs en newcore (PARTICIPE)
print("\n========== fcme_newcore: schemas y tablas ==========")
c = conn("fcme_newcore").cursor()
c.execute("""
 SELECT s.name sch, COUNT(*) n_tables
 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
 WHERE s.name NOT LIKE 'db_%' AND s.name NOT IN ('sys','INFORMATION_SCHEMA','guest')
 GROUP BY s.name ORDER BY s.name
""")
for r in c.fetchall(): print(f"  {r.sch}: {r.n_tables} tablas")

print("\n========== fcme_newcore.PARTICIPE: tablas ==========")
c.execute("""
 SELECT t.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
 WHERE s.name='PARTICIPE' ORDER BY t.name
""")
tables = [r.name for r in c.fetchall()]
print(f"  total: {len(tables)}")
for t in tables: print(f"    {t}")

# SPs en newcore
print("\n========== fcme_newcore: SPs usp_inbox_* o sp_inbox_* ==========")
c.execute("""
 SELECT s.name sch, o.name nm FROM sys.objects o
 JOIN sys.schemas s ON o.schema_id=s.schema_id
 WHERE o.type='P' AND (o.name LIKE 'usp_inbox_%' OR o.name LIKE 'sp_inbox_%' OR o.name LIKE '%_cdc_%')
 ORDER BY o.name
""")
sps = c.fetchall()
print(f"  total: {len(sps)}")
for r in sps: print(f"    {r.sch}.{r.nm}")

# module_config en algun sitio
print("\n========== module_config / cdc_* config tables en canonicos ==========")
c = conn("fcme_canonicos").cursor()
c.execute("""
 SELECT s.name sch, t.name tbl FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
 WHERE t.name LIKE '%module_config%' OR t.name LIKE 'cdc_%'
 ORDER BY t.name
""")
for r in c.fetchall(): print(f"  {r.sch}.{r.tbl}")
