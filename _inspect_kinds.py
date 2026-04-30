"""Identifica qué tipo de objeto son los 'sp_*_type' / 'sp_*Type' en participes."""
import pyodbc
from collections import defaultdict

DB = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123",
    "database": "fcme_legacy",
}

def conn():
    s = (f"DRIVER={DB['driver']};SERVER={DB['server']};"
         f"DATABASE={DB['database']};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s)

c = conn().cursor()

print("== sys.objects en participes (type='P' procedimientos) ==")
c.execute("""
  SELECT o.name, o.type_desc
  FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id
  WHERE s.name='participes' AND o.type='P'
  ORDER BY o.name
""")
rows = c.fetchall()
print(f"total SPs: {len(rows)}")
for r in rows[:10]:
    print(f"  {r.name}  ({r.type_desc})")
if len(rows) > 10: print("  ...")

print("\n== sys.types user-defined (table types) en esquema participes ==")
c.execute("""
  SELECT t.name, t.is_table_type
  FROM sys.types t JOIN sys.schemas s ON t.schema_id=s.schema_id
  WHERE s.name='participes'
  ORDER BY t.name
""")
rows = c.fetchall()
print(f"total user types: {len(rows)}")
for r in rows:
    kind = "TABLE TYPE" if r.is_table_type else "SCALAR TYPE"
    print(f"  {r.name}  [{kind}]")
