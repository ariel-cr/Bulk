"""Reporte multi-modulo: bases, tablas, types
Modulos: CARTERA, NOMINA, PARTICIPE, RECAUDACIONES, SEGURIDAD, TESORERIA"""
import sys, re
import pyodbc, oracledb
from collections import defaultdict, OrderedDict

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
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\modulos_bases_tablas_types.txt","w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
ORA={'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

_conns={}
def cn(db):
    if db in _conns: return _conns[db]
    c=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
    _conns[db]=c
    return c

LEGACY_DBS=["dbCR","dbFC","dbCG","dbCT","dbIM","dbNO","dbSV","dbIN","dbRC","dbSG","dbGN"]
MODULES=["CARTERA","NOMINA","PARTICIPE","RECAUDACIONES","SEGURIDAD","TESORERIA","CATALOGOS"]

print("="*100)
print(" REPORTE MULTI-MODULO: bases, tablas y aggregate_types")
print(f" Servidor: {DB['server']}     Fecha: 2026-04-30")
print(" Modulos: CARTERA, NOMINA, PARTICIPE, RECAUDACIONES, SEGURIDAD, TESORERIA")
print("="*100)

# 1) Cargar entries de cdc_inbox_module_config para los modulos
ccn=cn("fcme_canonicos"); cc=ccn.cursor()
ph=",".join("?"*len(MODULES))
cc.execute(f"""SELECT module_name, aggregate_type, sp_name, target_db, active
              FROM dbo.cdc_inbox_module_config
              WHERE module_name IN ({ph})
              ORDER BY module_name, aggregate_type""", *MODULES)
entries=cc.fetchall()
print(f"\nTotal entries en cdc_inbox_module_config: {len(entries)}")

# Distribucion
from collections import Counter
mod_count=Counter(r[0] for r in entries)
print(f"\nDistribucion por modulo:")
for m, n in mod_count.most_common():
    print(f"  {m:<20}: {n} types")

# 2) Para cada agg buscar trigger F1 en BDs legacy (escanear definicion del trigger)
# Construir indice global trigger -> (db, tbl, agg_types_emitidos)
print(f"\n[Indexando triggers F1 en {len(LEGACY_DBS)} BDs legacy...]")
trg_index={}  # agg -> (ldb, ltbl, trigger_name)
for ldb in LEGACY_DBS:
    try:
        c=cn(ldb).cursor()
        c.execute("""SELECT t.name AS trigger_name, tb.name AS table_name, OBJECT_DEFINITION(t.object_id)
                     FROM sys.triggers t
                     JOIN sys.tables tb ON tb.object_id = t.parent_id
                     WHERE OBJECT_DEFINITION(t.object_id) LIKE '%cdc_outbox%'""")
        for trg, tbl, defn in c.fetchall():
            if not defn: continue
            # Buscar todos los N'<agg>' en VALUES de @types
            aggs_in_trigger=re.findall(r"N'([a-zA-Z][a-zA-Z0-9_]+(?:_type|Type))'", defn)
            for a in set(aggs_in_trigger):
                if a not in trg_index:
                    trg_index[a]=(ldb, tbl, trg)
    except Exception as e:
        print(f"  [!] {ldb} no accesible: {str(e)[:80]}")

print(f"  Indexados {len(trg_index)} aggregate_types con trigger F1")

# 3) Para cada agg, buscar tabla Oracle FCME_USER.<TYPE>
print(f"\n[Buscando tablas Oracle FCME_USER...]")
orcl=oracledb.connect(**ORA); o=orcl.cursor()
def derive_dest(agg):
    candidates=[]
    if agg.endswith("_type"):
        candidates.append(agg[:-5].upper()+"_TYPE")
    elif agg.endswith("Type"):
        candidates.append(agg[:-4].upper()+"TYPE")
        candidates.append(agg[:-4].upper()+"_TYPE")
    candidates.append(agg.upper())
    return candidates

oracle_dest={}
for r in entries:
    agg=r[1]
    for cand in derive_dest(agg):
        o.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name=:t", [cand])
        if o.fetchone()[0]>0:
            oracle_dest[agg]=cand
            break

# 4) Reporte por modulo
by_mod=defaultdict(list)
for r in entries:
    by_mod[r[0]].append({
        "agg":r[1], "sp":r[2], "target_db":r[3], "active":r[4],
        "trg_info": trg_index.get(r[1]),
        "dest": oracle_dest.get(r[1])
    })

for mod in MODULES:
    if mod not in by_mod: continue
    items=by_mod[mod]
    print(f"\n\n{'#'*100}")
    print(f"# MODULO: {mod}     ({len(items)} aggregate_types)")
    print(f"{'#'*100}")

    # Distribucion BD legacy en este modulo
    by_ldb=defaultdict(list)
    for it in items:
        if it["trg_info"]:
            by_ldb[it["trg_info"][0]].append(it)
        else:
            by_ldb["?"].append(it)
    print(f"\nDistribucion por BD legacy:")
    for ldb, x in sorted(by_ldb.items()):
        print(f"   {ldb:<8}: {len(x)} types")

    # Detalle por BD
    for ldb in sorted(by_ldb.keys()):
        print(f"\n  ----- BD: {ldb} ({len(by_ldb[ldb])} aggregate_types) -----")
        for it in sorted(by_ldb[ldb], key=lambda x: x["agg"]):
            agg=it["agg"]
            trg_info=it["trg_info"]
            ltbl=trg_info[1] if trg_info else "(no detectado)"
            trg_name=trg_info[2] if trg_info else "(no detectado)"
            dest=it["dest"] or "(no_existe)"
            active="ON " if it["active"] else "off"
            print(f"    [{active}]  {agg:<40} -> [{ldb}].dbo.[{ltbl}]  ->  FCME_USER.{dest}")

# 5) Resumen ejecutivo
print(f"\n\n{'='*100}")
print(f" RESUMEN EJECUTIVO")
print(f"{'='*100}")
total_types=sum(len(by_mod[m]) for m in MODULES if m in by_mod)
total_active=sum(1 for m in MODULES for it in by_mod.get(m,[]) if it["active"])
total_with_trigger=sum(1 for m in MODULES for it in by_mod.get(m,[]) if it["trg_info"])
total_with_dest=sum(1 for m in MODULES for it in by_mod.get(m,[]) if it["dest"])

print(f"\n  Total aggregate_types     : {total_types}")
print(f"  Active (F2 routing)       : {total_active}/{total_types}")
print(f"  Con trigger F1 detectado  : {total_with_trigger}/{total_types}")
print(f"  Con tabla Oracle destino  : {total_with_dest}/{total_types}")

# BDs unicas usadas
all_ldbs=set()
for m in MODULES:
    for it in by_mod.get(m,[]):
        if it["trg_info"]:
            all_ldbs.add(it["trg_info"][0])
print(f"\n  BDs legacy en uso:")
for ldb in sorted(all_ldbs):
    n=sum(1 for m in MODULES for it in by_mod.get(m,[]) if it["trg_info"] and it["trg_info"][0]==ldb)
    print(f"    {ldb:<8}: {n} types")

print(f"\n=== FIN ===")
print(f"Archivo: C:\\Users\\Usuario\\Downloads\\Bulk\\modulos_bases_tablas_types.txt")
orcl.close(); ccn.close()
