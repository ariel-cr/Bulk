"""Lee el SP usp_inbox_PARTICIPE de newcore y estructuras de config/mapping
para entender el contrato esperado."""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# 1) estructuras de cdc_inbox y cdc_outbox en legacy + newcore
for db in ["fcme_legacy","fcme_newcore"]:
    c = conn(db).cursor()
    for tbl in ["cdc_outbox","cdc_inbox","cdc_inbox_errors","cdc_inbox_module_config","cdc_inbox_param_mapping"]:
        c.execute("""
          SELECT c.name, t.name tp, c.max_length, c.is_nullable
          FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
          WHERE c.object_id = OBJECT_ID(?)
          ORDER BY c.column_id
        """, f"dbo.{tbl}")
        rows = c.fetchall()
        if not rows: continue
        print(f"\n== {db}.dbo.{tbl} ==")
        for r in rows:
            print(f"  {r.name:<30} {r.tp:<15} max={r.max_length}  null={r.is_nullable}")

# 2) contenido actual de module_config en newcore
print("\n\n== fcme_newcore.dbo.cdc_inbox_module_config (contenido) ==")
c = conn("fcme_newcore").cursor()
try:
    c.execute("SELECT * FROM dbo.cdc_inbox_module_config")
    rows = c.fetchall()
    cols = [d[0] for d in c.description]
    print("  cols:", cols)
    for r in rows:
        print(f"   {dict(zip(cols, r))}")
except Exception as e:
    print(f"  ERROR: {e}")

# 3) contenido param_mapping en legacy
print("\n== fcme_legacy.dbo.cdc_inbox_param_mapping (contenido, sample 20) ==")
c = conn("fcme_legacy").cursor()
try:
    c.execute("SELECT TOP 20 * FROM dbo.cdc_inbox_param_mapping")
    rows = c.fetchall()
    cols = [d[0] for d in c.description]
    print("  cols:", cols)
    for r in rows: print(f"   {dict(zip(cols, r))}")
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_param_mapping")
    print(f"  total rows: {c.fetchone()[0]}")
except Exception as e:
    print(f"  ERROR: {e}")

# 4) contenido module_config en legacy
print("\n== fcme_legacy.dbo.cdc_inbox_module_config (contenido) ==")
c = conn("fcme_legacy").cursor()
try:
    c.execute("SELECT * FROM dbo.cdc_inbox_module_config")
    rows = c.fetchall()
    cols = [d[0] for d in c.description]
    print("  cols:", cols)
    for r in rows: print(f"   {dict(zip(cols, r))}")
except Exception as e:
    print(f"  ERROR: {e}")

# 5) Body de usp_inbox_PARTICIPE en newcore (primeros 80 lineas)
print("\n== fcme_newcore.dbo.usp_inbox_PARTICIPE body ==")
c = conn("fcme_newcore").cursor()
c.execute("SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.usp_inbox_PARTICIPE')) AS body")
body = c.fetchone().body or ""
for line in body.splitlines()[:120]:
    print("  " + line)
print(f"  ... (total lineas: {len(body.splitlines())})")

# 6) Triggers existentes en cdc_inbox de legacy y newcore
for db in ["fcme_legacy","fcme_newcore"]:
    c = conn(db).cursor()
    print(f"\n== Triggers en cdc_inbox ({db}) ==")
    c.execute("""
      SELECT tr.name, tr.is_disabled FROM sys.triggers tr
      WHERE OBJECT_NAME(tr.parent_id)='cdc_inbox'
    """)
    rows = c.fetchall()
    if not rows: print("  (ninguno)")
    for r in rows: print(f"  {r.name}  disabled={r.is_disabled}")
