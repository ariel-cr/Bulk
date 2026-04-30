"""Verifica la estructura real de fcme_canonicos.dbo.cdc_outbox."""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123","database":"fcme_canonicos"}

s = (f"DRIVER={DB['driver']};SERVER={DB['server']};"
     f"DATABASE={DB['database']};UID={DB['username']};PWD={DB['password']}")
c = pyodbc.connect(s).cursor()

for tbl in ["cdc_outbox", "cdc_inbox"]:
    print(f"\n== fcme_canonicos.dbo.{tbl} ==")
    c.execute("""
      SELECT c.name, t.name AS data_type, c.max_length, c.is_nullable, c.is_identity
      FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
      WHERE c.object_id = OBJECT_ID(?)
      ORDER BY c.column_id
    """, f"dbo.{tbl}")
    rows = c.fetchall()
    if not rows:
        print("  (no existe)"); continue
    for r in rows:
        print(f"  {r.name:<30} {r.data_type:<15} max={r.max_length:<6} null={r.is_nullable}  id={r.is_identity}")
