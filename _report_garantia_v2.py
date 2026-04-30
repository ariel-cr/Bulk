"""Reporte detallado del modulo GARANTIAS:
   por cada aggregate_type, encontrar:
     - Oracle FCME_USER.<TYPE> destino (con cols + PK)
     - Legacy table fuente (via trigger o SP CRUD)
     - DB legacy
   Formato: similar a inserts_cartera.txt header style.
"""
import sys, json
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

def cn(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)

print("="*100)
print(" MODULO: GARANTIAS - Bases legacy, tablas y aggregate_types")
print(f" Servidor: {DB['server']}     Fecha: 2026-04-30")
print("="*100)

# 1) Aggregate types del modulo GARANTIAS
ccn=cn("fcme_canonicos"); cc=ccn.cursor()
cc.execute("""SELECT aggregate_type, sp_name, target_db, active
              FROM dbo.cdc_inbox_module_config
              WHERE module_name='GARANTIAS'
              ORDER BY aggregate_type""")
entries=cc.fetchall()
print(f"\n Total aggregate_types modulo GARANTIAS: {len(entries)}")
print(" "+"-"*97)

# 2) Para cada agg, encontrar Oracle dest y legacy source
orcl=oracledb.connect(**ORA); o=orcl.cursor()

# Cache: tablas Oracle por aggregate_type (vienen de TRG_OUTBOX que tiene VALUES('agg'))
# Mas simple: derivar nombre del agg
def derive_dest(agg):
    """Convierte agg_type al nombre tabla Oracle convencional."""
    s=agg
    if s.endswith("_type"): return s[:-5].upper()+"_TYPE"
    if s.endswith("Type"): return s[:-4].upper()+"TYPE"
    return s.upper()

# Verificar existencia y candidatos en Oracle
tables_info={}
for r in entries:
    agg=r[0]
    dest_guess1=derive_dest(agg)  # snake -> _TYPE | camel -> TYPE
    # Tambien probar snake con underscore para camelCaseType:
    dest_guess2=None
    if agg.endswith("Type"):
        dest_guess2=agg[:-4].upper()+"_TYPE"
    candidates=[dest_guess1] + ([dest_guess2] if dest_guess2 else [])

    found=None
    for cand in candidates:
        o.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name=:t", [cand])
        if o.fetchone()[0]>0:
            found=cand; break

    # PK Oracle
    pk_cols=[]
    if found:
        o.execute("""SELECT acc.column_name
                     FROM all_constraints ac
                     JOIN all_cons_columns acc ON ac.owner=acc.owner AND ac.constraint_name=acc.constraint_name
                     WHERE ac.owner='FCME_USER' AND ac.table_name=:t AND ac.constraint_type='P'
                     ORDER BY acc.position""", [found])
        pk_cols=[r[0] for r in o.fetchall()]

    tables_info[agg]={"dest":found,"dest_pk":pk_cols,"sp":r[1],"target_db":r[2],"active":r[3]}

# 3) Buscar tabla legacy fuente. La SP dbo.sp_<Type>_CRUD apunta a una tabla.
# Podemos extraerlo de la definicion del SP en el target_db.
def find_legacy_table(agg, target_db, sp_name):
    if not target_db or not sp_name: return None, None
    base = sp_name.replace('dbo.usp_inbox_','').strip()  # garantiaPagareType
    # nombre del sp_CRUD esperado en target_db
    if base.endswith('_type'):
        sp_base = base[0].upper()+base[1:-5]+'Type'
    else:
        sp_base = base[0].upper()+base[1:]
    sp_crud_name = f"sp_{sp_base}_CRUD"
    try:
        c = cn(target_db).cursor()
        c.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", f"dbo.{sp_crud_name}")
        defn = c.fetchone()
        if defn and defn[0]:
            # Buscar "FROM dbo.[<tbl>]" o "INSERT INTO dbo.[<tbl>]"
            import re
            m = re.search(r"dbo\.\[([a-zA-Z0-9_]+)\]", defn[0])
            if m:
                return target_db, m.group(1)
    except Exception:
        pass
    return target_db, None

for r in entries:
    agg=r[0]
    target_db=tables_info[agg]["target_db"]
    sp=tables_info[agg]["sp"]
    ldb, ltbl = find_legacy_table(agg, target_db, sp)
    tables_info[agg]["ldb"]=ldb
    tables_info[agg]["ltbl"]=ltbl

# 4) Tambien buscar source_table en cdc_outbox por agg (eventos historicos)
for r in entries:
    agg=r[0]
    cc.execute("""SELECT TOP 1 source_table FROM cdc_outbox
                  WHERE aggregate_type=? ORDER BY id DESC""", agg)
    rr=cc.fetchone()
    if rr:
        tables_info[agg]["seen_source"]=rr[0]
    else:
        tables_info[agg]["seen_source"]=None

# 5) Reporte por target_db
by_db=defaultdict(list)
for r in entries:
    agg=r[0]
    info=tables_info[agg]
    db=info["target_db"] or "(unknown)"
    by_db[db].append((agg, info))

print(f"\n Distribucion por BD legacy:")
for db, items in sorted(by_db.items()):
    print(f"   {db:<8}: {len(items)} types")

# 6) Reporte detallado por BD
for db in sorted(by_db.keys()):
    print(f"\n\n{'#'*100}")
    print(f"# BD: {db}     ({len(by_db[db])} tipos)")
    print(f"{'#'*100}")
    print(f"USE [{db}];")
    print("GO")

    for agg, info in sorted(by_db[db], key=lambda x: x[0]):
        ltbl=info.get("ltbl") or "(no_detect)"
        dest=info.get("dest") or "(no_existe)"
        pk=info.get("dest_pk") or []
        seen=info.get("seen_source") or "(sin eventos)"
        active=info.get("active")

        print()
        print("-"*100)
        print(f"-- AGGREGATE TYPE : {agg}")
        print(f"-- LEGACY         : {db}.dbo.{ltbl}")
        print(f"-- ORACLE DEST    : FCME_USER.{dest}")
        print(f"-- PK Oracle      : {','.join(pk) if pk else '(no PK)'}")
        print(f"-- SP wrapper     : {info['sp']}")
        print(f"-- F2 active      : {active}")
        print(f"-- Seen source    : {seen}")
        print("-"*100)

print("\n\n=== FIN REPORTE ===")
print(f"Archivo: garantia_modulo_bases.txt")
orcl.close(); ccn.close()
