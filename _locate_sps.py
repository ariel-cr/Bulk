"""Busca en todas las BDs del servidor donde viven los SPs 'sp_*Type' / 'sp_*_type'
en esquema 'participes', excluyendo _crud y _dep."""
import pyodbc

DB = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123",
}

def conn(db):
    s = (f"DRIVER={DB['driver']};SERVER={DB['server']};"
         f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# Lista de BDs según el arbol que pasaste
DBS = ["fcme_legacy","fcme_newcore","fcme_canonicos"]

for db in DBS:
    try:
        c = conn(db).cursor()
    except Exception as e:
        print(f"\n=== {db}: NO ACCESIBLE ({e}) ==="); continue
    print(f"\n=== {db} ===")
    c.execute("""
      SELECT s.name AS sch, o.name AS nm, o.type_desc
      FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id
      WHERE s.name='participes'
        AND o.type IN ('P','FN','IF','TF')
        AND (o.name LIKE '%Type' OR o.name LIKE '%_type')
        AND o.name NOT LIKE '%_crud'
        AND o.name NOT LIKE '%_dep'
      ORDER BY o.name
    """)
    rows = c.fetchall()
    print(f"SPs/FNs 'Type' en participes (sin _crud/_dep): {len(rows)}")
    for r in rows[:80]:
        print(f"  {r.sch}.{r.nm}  [{r.type_desc}]")
