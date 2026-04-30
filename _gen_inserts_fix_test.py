"""Genera INSERTs de prueba SOLO para las tablas legacy que producen los 32 TYPES
del fix de wrappers, con valores FK reales y seed nuevo (no choca con inserts previos)."""
import json
from collections import defaultdict
from datetime import datetime, timedelta
from config import DB_CONFIG
import pyodbc

TYPES_NOK = [
    "FLUJOTRABAJOCREDITO_TYPE","PERSONACREDITOTYPE","LIQUIDACIONDIARIACREDITO_TYPE",
    "MOVIMIENTOCONTABLECREDITO_TYPE","COSTOFINANCIEROCREDITO_TYPE","CREDITOTYPE",
    "DESEMBOLSOCREDITO_TYPE","PAGOSCREDITOTYPE","REFINANCIAMIENTOCREDITOTYPE",
    "REFERENCIADEUDOR_TYPE","DEVOLUCIONMASIVADETALLE_TYPE","DEVOLUCIONMASIVA_TYPE",
    "PERSONACXPCXCTYPE","REPORTESBSOPERACIONCANCELADA_TYPE",
    "REPORTESBSOPERACIONCONCEDIDA_TYPE","REPORTESBSSALDOOPERACION_TYPE",
    "OPERACIONCONYUGAL_TYPE","DETALLERECUPERACION_TYPE","RECUPERACIONCONVENIO_TYPE",
    "RECUPERACIONCREDITO_TYPE","TRANSACCIONRECUPERACION_TYPE",
    "REPORTESBSSUJETORIESGO_TYPE","CUOTACREDITOTYPE","SALDOCARTERADETALLE_TYPE",
    "PAGOCREDITO_TYPE","SOLIDARIOCREDITO_TYPE","ESTADOLEGALTYPE",
    "REPORTESBSGARANTECODEUDOR_TYPE","AUXDATOSCOBROSADICIONALESTYPE",
    "GRUPOCREDITODETALLE_TYPE","CUENTASENLEGALTYPE","FECHASPROCESOTYPE",
]

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    specs = json.load(f)

# Sacar tablas legacy unicas que generan esos TYPES
src_to_types = defaultdict(list)
for s in specs:
    if s.get("dest") in TYPES_NOK and s.get("ldb") and s.get("ltbl"):
        src_to_types[(s["ldb"], s["ltbl"])].append((s["dest"], s["agg"]))


def open_conn(db):
    cs = (f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};"
          f"DATABASE={db};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}")
    return pyodbc.connect(cs)


def get_cols(cur, schema, table):
    cur.execute("""
        SELECT
            c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE,
            COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA+'.'+c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity'),
            COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA+'.'+c.TABLE_NAME), c.COLUMN_NAME, 'IsComputed')
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA=? AND c.TABLE_NAME=?
        ORDER BY c.ORDINAL_POSITION
    """, schema, table)
    return [{'name': r[0], 'type': r[1].lower(), 'len': r[2],
             'prec': r[3], 'scale': r[4], 'null': r[5]=='YES',
             'identity': r[6]==1, 'computed': r[7]==1} for r in cur.fetchall()]


def get_pk(cur, schema, table):
    cur.execute("""
        SELECT col.name
        FROM sys.indexes ix
        JOIN sys.index_columns ic ON ix.object_id=ic.object_id AND ix.index_id=ic.index_id
        JOIN sys.columns col ON ic.object_id=col.object_id AND ic.column_id=col.column_id
        WHERE ix.is_primary_key=1 AND ix.object_id=OBJECT_ID(?+'.'+?)
        ORDER BY ic.key_ordinal
    """, schema, table)
    return [r[0] for r in cur.fetchall()]


def get_fks(cur, schema, table):
    cur.execute("""
        SELECT fk.object_id,
               OBJECT_SCHEMA_NAME(fkc.referenced_object_id), OBJECT_NAME(fkc.referenced_object_id),
               cp.name, cr.name, fkc.constraint_column_id
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id
        JOIN sys.columns cp ON fkc.parent_object_id=cp.object_id AND fkc.parent_column_id=cp.column_id
        JOIN sys.columns cr ON fkc.referenced_object_id=cr.object_id AND fkc.referenced_column_id=cr.column_id
        WHERE fkc.parent_object_id=OBJECT_ID(?+'.'+?)
        ORDER BY fk.object_id, fkc.constraint_column_id
    """, schema, table)
    groups = {}
    for r in cur.fetchall():
        if r[0] not in groups: groups[r[0]] = {'ref_schema': r[1], 'ref_table': r[2], 'cols': []}
        groups[r[0]]['cols'].append((r[3], r[4]))
    return list(groups.values())


def fetch_parent_rows(cur, schema, table, cols, limit=200):
    cols_q = ",".join(f"[{c}]" for c in cols)
    try:
        cur.execute(f"SELECT DISTINCT TOP {limit} {cols_q} FROM [{schema}].[{table}]")
        return [tuple(r) for r in cur.fetchall()]
    except Exception:
        return []


def sql_lit(val, col):
    if val is None: return "NULL"
    t = col['type']
    if t in ('int','bigint','smallint','tinyint','numeric','decimal','money','smallmoney','float','real','bit'):
        return str(val)
    if isinstance(val, datetime):
        return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
    return "'" + str(val).replace("'","''") + "'"


def gen_value(col, idx, seed):
    t = col['type']
    # SMALLINT max 32767; TINYINT max 255 -> clamp para evitar arith overflow
    if t == 'tinyint':
        return str(((seed % 200) + idx) % 250)
    if t == 'smallint':
        return str(((seed % 25000) + idx) % 32000)
    if t in ('int', 'bigint'):
        return str(seed + idx)
    if t in ('numeric','decimal','money','smallmoney','float','real'):
        scale = col.get('scale') or 2
        prec = col.get('prec') or 18
        # Si la precision es chica, no inflar el numero
        if prec and prec <= 6:
            base_int = ((seed % 9000) + idx) % (10 ** max(1, prec - (scale or 0) - 1))
        else:
            base_int = seed + idx
        return f"{base_int + 0.99:.{min(scale,4)}f}"
    if t == 'bit': return str(idx % 2)
    if t == 'date':
        d = datetime(2024,1,1)+timedelta(days=idx); return f"'{d.strftime('%Y-%m-%d')}'"
    if t in ('datetime','datetime2','smalldatetime','datetimeoffset'):
        d = datetime(2024,1,1)+timedelta(days=idx, hours=idx); return f"'{d.strftime('%Y-%m-%d %H:%M:%S')}'"
    if t == 'time': return f"'{(8+idx%12):02d}:{(idx*7)%60:02d}:00'"
    if t == 'uniqueidentifier': return "NEWID()"
    if t in ('varbinary','binary','image'): return "0x00"
    maxlen = col.get('len') or 50
    if maxlen == -1: maxlen = 100
    base = f"X{seed%1000:03d}_{idx:02d}"
    return "'" + base[:max(1,maxlen)].replace("'","''") + "'"


# OFFSET para que el seed sea distinto al del archivo previo (evita conflictos de PK)
SEED_OFFSET = 50000

lines = []
P = lines.append
P("="*100)
P("INSERTS DE PRUEBA - 32 TYPES de CARTERA con wrappers parchados")
P("Servidor: 10.35.3.64,1433        Fecha: 2026-04-29")
P("="*100)
P("")
P("USO:")
P("  1) Aplica primero los wrappers parchados:  python _gen_cartera_wrappers.py --apply")
P("  2) Ejecuta este script (10 inserts por tabla legacy fuente).")
P("  3) Verifica filas en FCME_USER.<TYPE> y errores en FCME_USER.CDC_INBOX_ERRORS.")
P("")
P("Cada bloque marca el TYPE de Oracle al que llega via Kafka -> CDC_INBOX -> wrapper.")
P("Seed con offset 50000: no choca con inserts anteriores (que usaron seed < 10000).")
P("FKs resueltos contra valores reales de las tablas padre.")
P("")
P(f"Tablas legacy fuente para los 32 TYPES: {len(src_to_types)}")
for (db, tbl), types in sorted(src_to_types.items()):
    P(f"  {db}.{tbl}  ->  {', '.join(t for t,_ in types)}")
P("")

total = 0
skipped = []
by_db = defaultdict(list)
for (db, tbl), types in src_to_types.items():
    by_db[db].append((tbl, types))

for db in sorted(by_db):
    P("")
    P("#"*100)
    P(f"# BD: {db}")
    P("#"*100)
    P(f"USE [{db}];")
    P("GO")
    cn = open_conn(db); cur = cn.cursor()
    for tbl, types in sorted(by_db[db]):
        cur.execute("""SELECT s.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
                       WHERE t.name=?""", tbl)
        row = cur.fetchone()
        P("")
        P("-"*100)
        P(f"-- TABLA : [{db}].[{row[0] if row else '?'}].[{tbl}]")
        for typ, agg in types:
            P(f"--   ->  fcme_newcore.CARTERA.{typ:<35}  (agg='{agg}')")
        if not row:
            P("-- NO EXISTE; se omite")
            continue
        schema = row[0]
        cols = get_cols(cur, schema, tbl)
        pk = get_pk(cur, schema, tbl)
        fks = get_fks(cur, schema, tbl)
        insertable = [c for c in cols if not c['identity'] and not c['computed']]
        fk_data = []; any_empty = False
        for fk in fks:
            local = [lc for lc,_ in fk['cols']]
            ref = [rc for _,rc in fk['cols']]
            pr = fetch_parent_rows(cur, fk['ref_schema'], fk['ref_table'], ref)
            fk_data.append({'local': local, 'ref': f"[{fk['ref_schema']}].[{fk['ref_table']}]", 'rows': pr})
            if not pr: any_empty = True
        P(f"-- PK   : {', '.join(pk) if pk else '(sin PK)'}")
        if fk_data:
            P("-- FKs  :")
            for fk in fk_data:
                m = "OK" if fk['rows'] else "VACIA"
                P(f"--   ({', '.join(fk['local']):<40}) -> {fk['ref']:<35} [{m}, {len(fk['rows'])} rows]")
        P("-"*100)
        if any_empty:
            P("-- *** OMITIDA: tabla padre vacia (poblar primero) ***")
            skipped.append(f"{db}.{tbl}")
            continue
        seed = SEED_OFFSET + (abs(hash(tbl)) % 9000)
        col_list = ", ".join(f"[{c['name']}]" for c in insertable)
        col_to_fk = {}
        for fi, fk in enumerate(fk_data):
            for pos, lc in enumerate(fk['local']):
                if lc not in col_to_fk:
                    col_to_fk[lc] = (fi, pos)
        for i in range(1, 11):  # 10 inserts por tabla (= >=10 events por TYPE)
            fk_pick = [fk['rows'][(seed+i) % len(fk['rows'])] for fk in fk_data]
            vals = []
            for c in insertable:
                if c['name'] in col_to_fk:
                    fi, pos = col_to_fk[c['name']]
                    vals.append(sql_lit(fk_pick[fi][pos], c))
                elif c['null'] and c['name'].lower() in ('co_usua_elim','fe_elim','co_usua_veri','fe_veri','co_usua_mvto','fe_mvto'):
                    vals.append("NULL")
                else:
                    vals.append(gen_value(c, i, seed))
            P(f"INSERT INTO [{schema}].[{tbl}] ({col_list}) VALUES ({', '.join(vals)});")
            total += 1
    P("GO")
    cn.close()

P("")
P("="*100)
P(f"TOTAL INSERTS: {total}")
P(f"Tablas omitidas (parent vacio): {len(skipped)}")
for s in skipped: P(f"  - {s}")
P("="*100)
P("")
P("VERIFICACION POST-INSERT (en Oracle FCME_USER):")
P("  -- Conteo por TYPE")
for typ in TYPES_NOK:
    P(f"  SELECT '{typ}' AS t, COUNT(*) FROM FCME_USER.{typ}")
    P("  UNION ALL")
P("")
P("  -- Errores recientes (deberian ser 0)")
P("  SELECT AGGREGATE_TYPE, COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
P("  WHERE CREATED_AT > SYSDATE - 1/24")
P("  GROUP BY AGGREGATE_TYPE ORDER BY 2 DESC;")
P("="*100)
P("FIN")
P("="*100)

with open(r"C:\Users\Usuario\Downloads\Bulk\INSERTFALLOSCORCART.TXT","w",encoding="utf-8") as f:
    f.write("\n".join(lines))
print(f"Wrote {len(lines)} lines, {total} INSERTs, skipped={len(skipped)}")
