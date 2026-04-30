"""Lee los parametros y primeras lineas de varios sp_*Type canonicos
para saber que esperan y como invocarlos desde el proyector."""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123","database":"fcme_canonicos"}
def conn():
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={DB['database']};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = conn().cursor()

# 1) Parametros de los 36 SPs _type
c.execute("""
SELECT s.name sch, o.name nm, p.name pname, t.name pt, p.max_length, p.parameter_id
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id=s.schema_id
LEFT JOIN sys.parameters p ON p.object_id=o.object_id
LEFT JOIN sys.types t ON p.user_type_id=t.user_type_id
WHERE s.name='participes'
  AND o.type='P'
  AND (o.name LIKE '%Type' OR o.name LIKE '%_type')
  AND o.name NOT LIKE '%_crud' AND o.name NOT LIKE '%_dep'
ORDER BY o.name, p.parameter_id
""")
from collections import defaultdict
params = defaultdict(list)
for r in c.fetchall():
    if r.pname:
        params[r.nm].append((r.pname, r.pt, r.max_length))
print("=== Parametros de cada sp_*Type ===")
for sp, plist in sorted(params.items()):
    if plist:
        sig = ", ".join(f"{n} {t}({m})" if t in ('varchar','nvarchar') else f"{n} {t}" for n,t,m in plist)
    else:
        sig = "(sin parametros)"
    print(f"  {sp}({sig})")

# 2) Body de sp_personaType (primeras 60 lineas) para ver SELECT/FOR JSON que devuelve
print("\n=== sp_personaType body (head) ===")
c.execute("SELECT OBJECT_DEFINITION(OBJECT_ID('participes.sp_personaType')) AS body")
body = c.fetchone().body or ""
for line in body.splitlines()[:60]:
    print("  " + line)
print(f"  ... (total lineas: {len(body.splitlines())})")

# 3) Body de sp_personaType_crud para ver si es reverso (newcore->legacy)
print("\n=== sp_personaType_crud body (head, para ver si aplica INSERT/UPDATE) ===")
c.execute("SELECT OBJECT_DEFINITION(OBJECT_ID('participes.sp_personaType_crud')) AS body")
body = c.fetchone().body or ""
for line in body.splitlines()[:40]:
    print("  " + line)
print(f"  ... (total lineas: {len(body.splitlines())})")

# 4) Firma completa (params) de usp_inbox_PARTICIPE y que aggregate_types soporta
c2 = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER=10.35.3.64,1433;DATABASE=fcme_newcore;UID=sa;PWD=YourPassword123", autocommit=True).cursor()
c2.execute("SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.usp_inbox_PARTICIPE')) AS b")
pbody = c2.fetchone().b or ""
import re
types_sup = sorted(set(re.findall(r"@aggregate_type\s*=\s*'([a-zA-Z]+Type)'", pbody) +
                       re.findall(r"IF @aggregate_type = '([a-zA-Z]+Type)'", pbody)))
print(f"\n=== usp_inbox_PARTICIPE soporta aggregate_types: {types_sup}")

# 5) Listar todos aggregate_type del cdc_inbox_module_config (newcore) que van a PARTICIPE
c2.execute("""
SELECT aggregate_type FROM dbo.cdc_inbox_module_config
WHERE sp_name LIKE '%usp_inbox_PARTICIPE%' AND active=1
ORDER BY aggregate_type
""")
print("\n=== aggregate_types mapeados a usp_inbox_PARTICIPE en cdc_inbox_module_config ===")
for r in c2.fetchall(): print(f"  {r[0]}")
