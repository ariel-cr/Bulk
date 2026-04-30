"""V3: extrae tabla legacy desde el cuerpo del wrapper canonicos usp_inbox_<agg>.
El wrapper canonicos llama a <ldb>.dbo.<sp_legacy> @cols=...; del cual podemos
extraer la tabla target buscando en su body en el ldb.
"""
import sys, re
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
print("====================================================================================================")
print(" MODULO: GARANTIAS - bases legacy, tablas y aggregate_types (formato base.tabla -> type)")
print(f" Servidor: {DB['server']}     Fecha: 2026-04-30")
print("="*100)
print()

ccn=cn("fcme_canonicos"); cc=ccn.cursor()
cc.execute("""SELECT aggregate_type, sp_name, target_db, active
              FROM dbo.cdc_inbox_module_config
              WHERE module_name='GARANTIAS'
              ORDER BY aggregate_type""")
entries=cc.fetchall()

orcl=oracledb.connect(**ORA); o=orcl.cursor()

def derive_dest(agg):
    s=agg
    candidates=[]
    if s.endswith("_type"):
        candidates.append(s[:-5].upper()+"_TYPE")
    elif s.endswith("Type"):
        candidates.append(s[:-4].upper()+"TYPE")
        candidates.append(s[:-4].upper()+"_TYPE")
    return candidates

# Para cada agg buscar:
# - Oracle dest table
# - Definition del wrapper canonicos -> EXEC <ldb>.dbo.<sp_xxx_CRUD>
# - Definition del sp legacy -> tabla referenciada
results=[]
for r in entries:
    agg=r[0]; sp_name=r[1]; target_db=r[2]; active=r[3]
    info={"agg":agg,"sp_name":sp_name,"target_db":target_db,"active":active,
          "dest":None,"dest_pk":[],"legacy_sp":None,"legacy_table":None}

    # Oracle dest
    for cand in derive_dest(agg):
        o.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name=:t", [cand])
        if o.fetchone()[0]>0:
            info["dest"]=cand
            break
    if info["dest"]:
        o.execute("""SELECT acc.column_name FROM all_constraints ac
                     JOIN all_cons_columns acc ON ac.owner=acc.owner AND ac.constraint_name=acc.constraint_name
                     WHERE ac.owner='FCME_USER' AND ac.table_name=:t AND ac.constraint_type='P'
                     ORDER BY acc.position""", [info["dest"]])
        info["dest_pk"]=[r[0] for r in o.fetchall()]

    # Buscar triggers F1 en TODAS las BDs legacy que emitan ESTE agg
    # (los triggers tienen N'<agg_type>' literal en su body)
    LEGACY_DBS=["dbCR","dbFC","dbCG","dbCT","dbIM","dbNO","dbSV","dbIN"]
    for ldb in LEGACY_DBS:
        try:
            lc=cn(ldb).cursor()
            lc.execute("""SELECT t.name AS trigger_name, tb.name AS table_name
                          FROM sys.triggers t
                          JOIN sys.tables tb ON tb.object_id = t.parent_id
                          WHERE OBJECT_DEFINITION(t.object_id) LIKE ?""",
                       f"%N'{agg}'%")
            for trg, tbl in lc.fetchall():
                info["legacy_sp"]=f"{ldb}.dbo.{trg}"
                info["legacy_table"]=tbl
                if not info.get("ldb_detected"):
                    info["ldb_detected"]=ldb
                break
            if info.get("legacy_table"):
                break
        except Exception as e:
            pass

    results.append(info)

# Distribucion por LEGACY DB (donde vive el trigger F1, no target_db)
by_db=defaultdict(list)
for r in results:
    actual_ldb = r["legacy_sp"].split(".")[0] if r["legacy_sp"] else (r["target_db"] or "?")
    by_db[actual_ldb].append(r)

print(f" Total aggregate_types modulo GARANTIAS: {len(results)}")
print(f" Distribucion por BD legacy:")
for db, items in sorted(by_db.items()):
    print(f"   {db:<8}: {len(items)} types")
print()

# Detalle por BD legacy
for db in sorted(by_db.keys()):
    items=by_db[db]
    print()
    print("#"*100)
    print(f"# BD LEGACY: {db}     ({len(items)} aggregate_types - tablas que disparan triggers F1)")
    print("#"*100)
    print(f"USE [{db}];")
    print("GO")
    print()

    for info in sorted(items, key=lambda x: x["agg"]):
        # ldb real donde vive el trigger F1 (extraido del legacy_sp)
        actual_ldb = info["legacy_sp"].split(".")[0] if info["legacy_sp"] else "?"
        print("-"*100)
        print(f"-- AGGREGATE TYPE : {info['agg']}")
        if info["legacy_table"]:
            print(f"-- LEGACY TABLE   : [{actual_ldb}].[dbo].[{info['legacy_table']}]")
        else:
            print(f"-- LEGACY TABLE   : (no detectado)")
        print(f"-- TRIGGER F1     : {info['legacy_sp'] or '(no detectado)'}")
        print(f"-- ORACLE DEST    : FCME_USER.{info['dest'] or '(no_existe)'}")
        if info["dest_pk"]:
            print(f"-- ORACLE PK      : {','.join(info['dest_pk'])}")
        print(f"-- WRAPPER CANON  : {info['sp_name']}")
        print(f"-- TARGET DB (F2) : {info['target_db']}")
        print(f"-- F2 ACTIVO      : {info['active']}")
        print("-"*100)
        print()

print("="*100)
print(" RESUMEN base.tabla -> aggregate_type")
print("="*100)
for db in sorted(by_db.keys()):
    print(f"\n {db}:")
    for info in sorted(by_db[db], key=lambda x: x["agg"]):
        ltbl=info["legacy_table"] or "?"
        actual_ldb = info["legacy_sp"].split(".")[0] if info["legacy_sp"] else db
        print(f"   {actual_ldb}.dbo.{ltbl:<35} -> {info['agg']}  ({info['dest'] or '?'})")

print("\n=== FIN REPORTE ===")
print(f"Archivo: C:\\Users\\Usuario\\Downloads\\Bulk\\garantia_modulo_bases.txt")
orcl.close(); ccn.close()
