"""Reporte de bases/tablas/types usados por el modulo GARANTIA.

Fuentes:
  1) cdc_inbox_module_config (canonicos) WHERE module_name LIKE 'GARANT%'
  2) Triggers existentes con prefijo trg_outbox_* en cada BD legacy
  3) cdc_outbox events recientes (analisis empirico)
  4) Oracle FCME_USER.<TYPE> tables match para garantia*

Formato de salida similar a inserts_cartera.txt.
"""
import sys, signal, atexit
import pyodbc, oracledb
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
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\garantia_modulo_bases.txt","w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
ORA={'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}
_conns={}
def cn(db):
    if db in _conns: return _conns[db]
    c=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
    _conns[db]=c
    return c
def cleanup():
    for db,c in list(_conns.items()):
        try: c.close()
        except: pass
atexit.register(cleanup)

print("="*100)
print("MODULO: GARANTIAS - bases, tablas y aggregate_types")
print(f"Servidor: {DB['server']}     Fecha: 2026-04-30")
print("="*100)

# 1) Buscar entries en cdc_inbox_module_config con module_name relacionado a GARANT
ccn=cn("fcme_canonicos"); cc=ccn.cursor()
cc.execute("""SELECT DISTINCT aggregate_type, sp_name, target_db, module_name, active
              FROM dbo.cdc_inbox_module_config
              WHERE module_name LIKE '%GARANT%' OR aggregate_type LIKE '%garant%' OR aggregate_type LIKE '%Garant%'
              ORDER BY aggregate_type""")
mod_entries=cc.fetchall()
print(f"\n[1] cdc_inbox_module_config entries garantia: {len(mod_entries)}")
for r in mod_entries:
    print(f"    agg={r[0]:<40} sp={r[1]:<32} target_db={r[2]:<6} module={r[3]:<12} active={r[4]}")

# 2) Buscar tablas Oracle FCME_USER.* con nombre GARANT*
print(f"\n[2] FCME_USER.<*GARANT*>_TYPE tables:")
orcl=oracledb.connect(**ORA); o=orcl.cursor()
o.execute("""SELECT table_name FROM all_tables
             WHERE owner='FCME_USER' AND (table_name LIKE '%GARANT%' OR table_name LIKE '%CAUC%')
             ORDER BY table_name""")
ora_tables=[r[0] for r in o.fetchall()]
for t in ora_tables:
    print(f"    {t}")

# 3) Tablas legacy con nombres relacionados a garantia
LEGACY_DBS=["dbCR","dbFC","dbCG","dbCT","dbIM","dbNO","dbSV"]
print(f"\n[3] Tablas legacy que contienen 'gara' o 'caut' o 'caucion' en su nombre:")
legacy_tbls=defaultdict(list)
for db in LEGACY_DBS:
    try:
        c=cn(db).cursor()
        c.execute("""SELECT name FROM sys.tables WHERE name LIKE '%gara%' OR name LIKE '%caut%' OR name LIKE '%cauc%' ORDER BY name""")
        for r in c.fetchall():
            legacy_tbls[db].append(r[0])
    except Exception as e:
        print(f"  [!] {db}: {str(e)[:80]}")

for db, tbls in sorted(legacy_tbls.items()):
    print(f"\n  --- {db} ({len(tbls)}) ---")
    for t in tbls:
        # Cuantos triggers cdc tiene
        c=cn(db).cursor()
        c.execute("""SELECT name FROM sys.triggers
                     WHERE parent_id=OBJECT_ID(?) AND name LIKE 'trg_outbox%'""", f"dbo.{t}")
        trgs=[r[0] for r in c.fetchall()]
        marker="(con trg)" if trgs else ""
        print(f"    {db}.dbo.{t}  {marker}")

# 4) Detectar source_table garantia en cdc_outbox events
print(f"\n[4] source_table en cdc_outbox que han disparado eventos garantia (ultimos 7 dias):")
cc.execute("""SELECT DISTINCT source_table, aggregate_type
              FROM cdc_outbox
              WHERE (aggregate_type LIKE '%garant%' OR aggregate_type LIKE '%Garant%' OR aggregate_type LIKE '%cauc%')
                AND created_at >= DATEADD(DAY, -7, SYSDATETIME())
              ORDER BY source_table""")
emp=cc.fetchall()
if not emp:
    print("    (sin eventos recientes)")
else:
    for r in emp:
        print(f"    {r[0]:<45} -> {r[1]}")

# 5) FORMATO ESTILO inserts_cartera.txt
print("\n\n"+"="*100)
print("FORMATO base.tabla -> aggregate_type")
print("="*100)

# Compilar la matriz: aggregate_type -> dest Oracle + source legacy
print("\n# AGGREGATE_TYPES (de modulo GARANTIAS)")
for r in mod_entries:
    agg=r[0]; sp=r[1]; tdb=r[2]
    # buscar a que tabla legacy corresponde via sp_name pattern (sp_<X>_CRUD existe)
    # buscar tabla Oracle TYPE
    print(f"\n  TYPE        : fcme_newcore.GARANTIAS.{agg}")
    print(f"  Oracle dest : FCME_USER.??_TYPE")
    print(f"  SP wrapper  : {sp}")
    print(f"  Legacy DB   : {tdb}")
    print(f"  Active F2   : {r[4]}")

print("\n\n=== FIN REPORTE ===")
orcl.close()
