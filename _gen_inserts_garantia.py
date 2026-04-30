"""Genera 10 INSERTs por tabla del modulo GARANTIAS, con valores reales de FK
en formato similar a inserts_cartera.txt.

Garantias contra errores:
  - Inspecciona sys.foreign_keys para cada tabla
  - Cols FK toman valores existentes de la tabla padre (TOP 1 del padre, alternando)
  - FK compuesto: cols del mismo constraint quedan correlacionadas
  - Si padre vacio: skip y documentar
  - Cols no-FK: PK numerica usa seed_base + idx; otras sinteticas por tipo

Anti-zombi: autocommit + LOCK_TIMEOUT + cleanup.
"""
import sys, signal, atexit
from datetime import datetime
import pyodbc
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

OUT=r"C:\Users\Usuario\Downloads\Bulk\inserts_garantia.txt"
sys.stdout=Tee(sys.__stdout__, open(OUT,"w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
_conns={}
def conn(db):
    if db in _conns: return _conns[db]
    cn=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
    cn.timeout=30
    cn.cursor().execute("SET LOCK_TIMEOUT 5000")
    _conns[db]=cn
    return cn

def cleanup():
    for db,cn in list(_conns.items()):
        try: cn.close()
        except: pass

atexit.register(cleanup)
for s in (signal.SIGINT, signal.SIGTERM):
    signal.signal(s, lambda *a:(cleanup(),sys.exit(130)))

# Tablas del modulo GARANTIAS (extraido de garantia_modulo_bases.txt)
TABLES=[
    ("dbCG","cgtbaval_hipo_cdio","avaluoGarantiaType","fcme_newcore.GARANTIAS.AVALUOGARANTIA_TYPE"),
    ("dbCG","cgtbgara_hipo_cdio","datosInmueblesType","fcme_newcore.GARANTIAS.DATOSINMUEBLESTYPE"),
    ("dbCG","cgtbgara_vehi_cdio","datosVehiculosType","fcme_newcore.GARANTIAS.DATOSVEHICULOSTYPE"),
    ("dbCR","crtbcaut_gara","contabilizacionGarantiaType / garantiaPorPrestamoType","fcme_newcore.GARANTIAS.CONTABILIZACIONGARANTIA_TYPE / GARANTIAPORPRESTAMOTYPE"),
    ("dbCR","crtgrtes","garanteCreditoType","fcme_newcore.GARANTIAS.GARANTECREDITO_TYPE"),
    ("dbCR","crtbgara_real","garantiaGeneralType","fcme_newcore.GARANTIAS.GARANTIAGENERALTYPE"),
    ("dbCR","crtbgara_pgre","garantiaPagareType","fcme_newcore.GARANTIAS.GARANTIAPAGARE_TYPE"),
    ("dbCR","crtbcgar_code_sibs","reporteSBSGarantiaType","fcme_newcore.GARANTIAS.REPORTESBSGARANTIA_TYPE"),
    ("dbFC","sfct_afiliado_bienes","datosOtrosActivosType","fcme_newcore.GARANTIAS.DATOSOTROSACTIVOSTYPE"),
    ("dbFC","fctbdeta_liqd_grnt","liquidacionGarantiaType","fcme_newcore.GARANTIAS.LIQUIDACIONGARANTIA_TYPE"),
    ("dbFC","sfct_bienes","tipoBienType","fcme_newcore.GARANTIAS.TIPOBIEN_TYPE"),
    ("dbIN","intbcinv","datosRentaVariableType","fcme_newcore.GARANTIAS.DATOSRENTAVARIABLETYPE"),
    ("dbIN","intbgara","garantiaInversionType","fcme_newcore.GARANTIAS.GARANTIAINVERSION_TYPE"),
]

def col_metadata(c, tbl):
    c.execute("""SELECT c.name, t.name AS dt, c.max_length, c.is_nullable, c.is_identity, c.is_computed,
                        CASE WHEN dc.definition IS NOT NULL THEN 1 ELSE 0 END AS has_default
                 FROM sys.columns c
                 JOIN sys.types t ON t.user_type_id=c.user_type_id
                 LEFT JOIN sys.default_constraints dc ON dc.parent_object_id=c.object_id AND dc.parent_column_id=c.column_id
                 WHERE c.object_id=OBJECT_ID(?)
                 ORDER BY c.column_id""", f"dbo.{tbl}")
    return c.fetchall()

def pk_cols(c, tbl):
    c.execute("""SELECT col.name
                 FROM sys.indexes i
                 JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
                 JOIN sys.columns col ON col.object_id=ic.object_id AND col.column_id=ic.column_id
                 WHERE i.object_id=OBJECT_ID(?) AND i.is_primary_key=1
                 ORDER BY ic.key_ordinal""", f"dbo.{tbl}")
    return [r[0] for r in c.fetchall()]

def get_fks(c, tbl):
    """Devuelve dict {fk_name: {'parent_table':..., 'pairs':[(child_col, parent_col)]}}"""
    c.execute("""SELECT fk.name, OBJECT_NAME(fkc.referenced_object_id) AS parent_tbl,
                        col_p.name AS parent_col, col_c.name AS child_col,
                        fkc.constraint_column_id
                 FROM sys.foreign_keys fk
                 JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id=fk.object_id
                 JOIN sys.columns col_p ON col_p.object_id=fkc.referenced_object_id AND col_p.column_id=fkc.referenced_column_id
                 JOIN sys.columns col_c ON col_c.object_id=fkc.parent_object_id AND col_c.column_id=fkc.parent_column_id
                 WHERE fk.parent_object_id=OBJECT_ID(?)
                 ORDER BY fk.name, fkc.constraint_column_id""", f"dbo.{tbl}")
    fks=OrderedDict()
    for fk_name, ptbl, pcol, ccol, _ in c.fetchall():
        if fk_name not in fks:
            fks[fk_name]={'parent_table':ptbl, 'pairs':[]}
        fks[fk_name]['pairs'].append((ccol, pcol))
    return fks

def sample_parent(c, parent_tbl, parent_cols, n=10):
    """Devuelve hasta N filas de cols del padre, en orden."""
    cols_q=",".join(f"[{c}]" for c in parent_cols)
    try:
        c.execute(f"SELECT TOP {n} {cols_q} FROM dbo.[{parent_tbl}]")
        return [tuple(r) for r in c.fetchall()]
    except Exception:
        return []

TYPE_MAX={'tinyint':255,'smallint':32767,'int':2_147_483_000,'bigint':9_000_000_000_000_000}
def _clamp(v, dt):
    """Clamp v al rango del tipo numerico."""
    for t,m in TYPE_MAX.items():
        if t in dt: return min(v, m)
    return v

def synth_value(dt, ml, idx, col_name=None, is_pk=False, seed=200):
    """Sintetico por tipo. Auto-clamp al rango del tipo.
    Para PK: seed+idx; para no-PK: valor pequeño fijo + idx."""
    dt=dt.lower()
    base = seed if is_pk else 90
    if 'tinyint' in dt:
        return _clamp(base+idx, 'tinyint')
    if 'smallint' in dt:
        # smallint: si seed+idx supera 32767, usar valor seguro
        v=base+idx
        if v > 32767:
            v = 32700 - idx  # de-escalar y mantener uniqueness
        return _clamp(v, 'smallint')
    if 'bigint' in dt:
        return _clamp(base*1000+idx if is_pk else 90+idx, 'bigint')
    if 'int' in dt:
        return _clamp(base+idx, 'int')
    if any(t in dt for t in ['numeric','decimal','money','smallmoney']):
        return f"{(base+idx) % 999999}.0000"
    if 'float' in dt or 'real' in dt:
        return f"{base+idx}.0"
    if 'bit' in dt: return idx % 2
    if 'date' in dt or 'time' in dt:
        return f"2024-01-{(idx%28)+1:02d} 01:00:00"
    if 'uniqueidentifier' in dt:
        return f"00000000-0000-0000-0000-{idx:012d}"
    # text - poner idx PRIMERO para que sobreviva truncamiento
    s=f"{idx:02d}T{seed}"
    if ml and ml > 0 and ml < len(s): s=s[:ml]
    return s

def quote_value(v, dt):
    if v is None: return "NULL"
    dt=dt.lower()
    if any(t in dt for t in ['int','tinyint','smallint','bit','numeric','decimal','money','float','real','bigint']):
        return str(v)
    # string-like
    s=str(v).replace("'","''")
    return f"'{s}'"

def gen_inserts_for_table(ldb, tbl, agg, type_full, n_inserts=10, seed=200):
    """Devuelve string con header y N inserts.
    Auto-ajusta seed para evitar colisiones de PK numerica."""
    out=[]
    cn=conn(ldb); c=cn.cursor()
    cols=col_metadata(c, tbl)
    if not cols:
        return f"-- TABLA {ldb}.dbo.{tbl} no encontrada\n"
    pk=pk_cols(c, tbl)
    fks=get_fks(c, tbl)

    # AUTO-SEED: query MAX de cada col PK numerica y usar max + 100
    col_dt={r[0]:r[1].lower() for r in cols}
    safe_seed=seed
    for pk_col in pk:
        dt=col_dt.get(pk_col,"")
        if any(t in dt for t in ['int','smallint','bigint','tinyint','numeric','decimal']):
            try:
                c.execute(f"SELECT ISNULL(MAX([{pk_col}]),0) FROM dbo.[{tbl}]")
                m=c.fetchone()[0] or 0
                if m+100 > safe_seed:
                    safe_seed=int(m)+100
            except Exception:
                pass
    seed=safe_seed

    # Header
    out.append("-"*100)
    out.append(f"-- TABLA : [{ldb}].[dbo].[{tbl}]")
    out.append(f"-- TYPE  : {type_full}")
    out.append(f"-- PK    : {', '.join(pk) if pk else '(sin PK declarada)'}")
    if fks:
        out.append(f"-- FKs   :")
        for fk_name, info in fks.items():
            child_cols=[p[0] for p in info['pairs']]
            ptbl=info['parent_table']
            pcols=[p[1] for p in info['pairs']]
            samples=sample_parent(c, ptbl, pcols, 10)
            status=f"[OK, {len(samples)} rows]" if samples else "[VACIO!]"
            out.append(f"--   ({', '.join(child_cols):<40}) -> [dbo].[{ptbl}]    {status}")
    else:
        out.append(f"-- FKs   : (sin FKs)")
    out.append("-"*100)

    # Build FK value pool por FK
    fk_pool={}  # fk_name -> list[tuple] de filas padre
    fk_failed=False
    for fk_name, info in fks.items():
        ptbl=info['parent_table']
        pcols=[p[1] for p in info['pairs']]
        rows=sample_parent(c, ptbl, pcols, n_inserts)
        if not rows:
            fk_failed=True
            out.append(f"-- INSERTS OMITIDOS: padre {ptbl} esta vacio")
            return "\n".join(out)+"\n"
        fk_pool[fk_name]=rows

    # Mapping child_col -> (fk_name, position_in_pair)
    child_col_to_fk={}
    for fk_name, info in fks.items():
        for pos, (ccol, _) in enumerate(info['pairs']):
            child_col_to_fk[ccol]=(fk_name, pos)

    # Generar N inserts
    for i in range(n_inserts):
        insert_cols=[]
        insert_vals=[]
        for nm, dt, ml, nullable, ident, comp, has_def in cols:
            if ident or comp:
                continue
            # Si nullable y no en FK ni PK, podemos saltarlo
            if nm in child_col_to_fk:
                fk_name, pos=child_col_to_fk[nm]
                # tomar fila padre: rotar
                row=fk_pool[fk_name][i % len(fk_pool[fk_name])]
                v=row[pos]
            elif nm in pk:
                v=synth_value(dt, ml, i, col_name=nm, is_pk=True, seed=seed)
            elif nullable and not has_def:
                # opcional
                v=synth_value(dt, ml, i, col_name=nm, seed=seed)
            else:
                # NOT NULL no-FK no-PK
                v=synth_value(dt, ml, i, col_name=nm, seed=seed)
            insert_cols.append(nm)
            insert_vals.append((v, dt))
        cols_q=", ".join(f"[{c}]" for c in insert_cols)
        vals_q=", ".join(quote_value(v, dt) for v, dt in insert_vals)
        out.append(f"INSERT INTO [dbo].[{tbl}] ({cols_q}) VALUES ({vals_q});")
    out.append("")
    return "\n".join(out)+"\n"

# Header global
print("="*100)
print("GARANTIAS - 10 INSERT por TABLA con valores REALES de FK")
print(f"Servidor: {DB['server']}     Fecha: 2026-04-30")
print("="*100)
print()
print("CAMBIOS vs version anterior:")
print("  - Se inspecciona sys.foreign_keys para cada tabla.")
print("  - Las columnas FK toman valores existentes en la tabla padre.")
print("  - FK compuesto: cols del mismo constraint quedan correlacionadas.")
print("  - Si la tabla padre esta vacia, se omiten los inserts y se documenta.")
print("  - Columnas no-FK: PK numerica usa seed+idx; otras usan datos sinteticos.")
print()

# Agrupar por BD
by_db=defaultdict(list)
for ldb, tbl, agg, type_full in TABLES:
    by_db[ldb].append((tbl, agg, type_full))

for ldb in sorted(by_db.keys()):
    tbls=by_db[ldb]
    print()
    print("#"*100)
    print(f"# BD: {ldb}     ({len(tbls)} tablas)")
    print("#"*100)
    print(f"USE [{ldb}];")
    print(f"GO")
    print()
    for tbl, agg, type_full in tbls:
        try:
            body=gen_inserts_for_table(ldb, tbl, agg, type_full, n_inserts=10, seed=200)
            print(body)
        except Exception as e:
            import traceback
            print(f"-- ERROR procesando {ldb}.dbo.{tbl}: {str(e)[:300]}")
            print(f"-- TRACE: {traceback.format_exc()}")
            print()

print()
print("="*100)
print("FIN DEL ARCHIVO")
print("="*100)
