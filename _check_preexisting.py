"""Verifica triggers preexistentes en tablas legacy que YA estaban publicando en camelCase."""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

for db in ["dbFC","dbNO","dbCG","dbCT","dbCR","dbIM","dbSV"]:
    c = conn(db).cursor()
    c.execute("""
      SELECT s.name sch, o.name tbl, tr.name tr, tr.is_disabled
      FROM sys.triggers tr
      JOIN sys.objects o ON tr.parent_id=o.object_id
      JOIN sys.schemas s ON o.schema_id=s.schema_id
      WHERE tr.name NOT LIKE 'trg_outbox_%'  -- excluir los mios
      ORDER BY o.name, tr.name
    """)
    rows = c.fetchall()
    if not rows: continue
    print(f"\n== {db} (triggers NO mios) ==")
    print(f"  total: {len(rows)}")
    for r in rows[:30]:
        print(f"    {r.sch}.{r.tbl}  ->  {r.tr}  disabled={r.is_disabled}")
    if len(rows) > 30:
        print(f"    ... ({len(rows)-30} mas)")
