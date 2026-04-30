"""Genera 10 INSERT por tabla CARTERA usando valores reales para FKs."""
import json
from collections import defaultdict
from datetime import datetime, timedelta
from config import DB_CONFIG
import pyodbc

with open('_cartera_specs.json') as f:
    specs = json.load(f)

mapping = defaultdict(list)
for s in specs:
    db = s.get('ldb'); tbl = s.get('ltbl'); typ = s.get('dest'); agg = s.get('agg')
    if db and tbl and typ:
        mapping[(db, tbl)].append((typ, agg))

by_db = defaultdict(dict)
for (db, tbl), types in mapping.items():
    by_db[db][tbl] = sorted(types)


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
    out = []
    for r in cur.fetchall():
        out.append({'name': r[0], 'type': r[1].lower(), 'len': r[2],
                    'prec': r[3], 'scale': r[4], 'null': r[5] == 'YES',
                    'identity': r[6] == 1, 'computed': r[7] == 1})
    return out


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
               OBJECT_SCHEMA_NAME(fkc.referenced_object_id) AS ref_sch,
               OBJECT_NAME(fkc.referenced_object_id) AS ref_tbl,
               cp.name AS local_col, cr.name AS ref_col,
               fkc.constraint_column_id
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        WHERE fkc.parent_object_id = OBJECT_ID(?+'.'+?)
        ORDER BY fk.object_id, fkc.constraint_column_id
    """, schema, table)
    rows = cur.fetchall()
    groups = {}
    for r in rows:
        if r[0] not in groups:
            groups[r[0]] = {'ref_schema': r[1], 'ref_table': r[2], 'cols': []}
        groups[r[0]]['cols'].append((r[3], r[4]))
    return list(groups.values())


def fetch_parent_rows(cur, ref_schema, ref_table, ref_cols, limit=200):
    cols_q = ",".join(f"[{c}]" for c in ref_cols)
    try:
        cur.execute(f"SELECT DISTINCT TOP {limit} {cols_q} FROM [{ref_schema}].[{ref_table}]")
        return [tuple(row) for row in cur.fetchall()]
    except Exception:
        return []


def sql_literal(val, col):
    if val is None:
        return "NULL"
    t = col['type']
    if t in ('int', 'bigint', 'smallint', 'tinyint', 'numeric', 'decimal',
             'money', 'smallmoney', 'float', 'real', 'bit'):
        return str(val)
    if isinstance(val, datetime):
        return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
    s = str(val)
    return "'" + s.replace("'", "''") + "'"


def gen_value(col, idx, seed):
    t = col['type']
    if t in ('int', 'bigint', 'smallint', 'tinyint'):
        return str(seed + idx)
    if t in ('numeric', 'decimal', 'money', 'smallmoney', 'float', 'real'):
        scale = col.get('scale') or 2
        return f"{(seed+idx)+0.99:.{min(scale,4)}f}"
    if t == 'bit':
        return str(idx % 2)
    if t == 'date':
        d = datetime(2024, 1, 1) + timedelta(days=idx)
        return f"'{d.strftime('%Y-%m-%d')}'"
    if t in ('datetime', 'datetime2', 'smalldatetime', 'datetimeoffset'):
        d = datetime(2024, 1, 1) + timedelta(days=idx, hours=idx)
        return f"'{d.strftime('%Y-%m-%d %H:%M:%S')}'"
    if t == 'time':
        return f"'{(8+idx%12):02d}:{(idx*7)%60:02d}:00'"
    if t == 'uniqueidentifier':
        return "NEWID()"
    if t in ('varbinary', 'binary', 'image'):
        return "0x00"
    maxlen = col.get('len') or 50
    if maxlen == -1:
        maxlen = 100
    base = f"T{seed%1000:03d}_{idx:02d}"
    return "'" + base[:max(1, maxlen)].replace("'", "''") + "'"


lines = []
P = lines.append
P("=" * 100)
P("CARTERA - 10 INSERT por TABLA con valores REALES de FK")
P("Servidor: 10.35.3.64,1433     Fecha: 2026-04-29")
P("=" * 100)
P("")
P("CAMBIOS vs version anterior:")
P("  - Se inspecciona sys.foreign_keys para cada tabla.")
P("  - Las columnas FK toman valores existentes en la tabla padre.")
P("  - FK compuesto: cols del mismo constraint quedan correlacionadas.")
P("  - Si la tabla padre esta vacia, se omiten los inserts y se documenta.")
P("  - Columnas no-FK: PK numerica usa seed+idx; otras usan datos sinteticos.")
P("")

total = 0
skipped_tables = []
for db in sorted(by_db):
    P("")
    P("#" * 100)
    P(f"# BD: {db}     ({len(by_db[db])} tablas)")
    P("#" * 100)
    P(f"USE [{db}];")
    P("GO")
    try:
        cn = open_conn(db)
        cur = cn.cursor()
    except Exception as e:
        P(f"-- ERROR conexion: {e}")
        continue

    for tbl in sorted(by_db[db]):
        types = by_db[db][tbl]
        cur.execute("""SELECT s.name FROM sys.tables t
                       JOIN sys.schemas s ON t.schema_id=s.schema_id
                       WHERE t.name=?""", tbl)
        row = cur.fetchone()
        P("")
        P("-" * 100)
        P(f"-- TABLA : [{db}].[{row[0] if row else '?'}].[{tbl}]")
        P(f"-- TYPE  : fcme_newcore.CARTERA.{', '.join(t for t,_ in types)}")
        if not row:
            P("-- (NO EXISTE EN ESTA BD)")
            continue
        schema = row[0]
        cols = get_cols(cur, schema, tbl)
        pk = get_pk(cur, schema, tbl)
        fks = get_fks(cur, schema, tbl)
        insertable = [c for c in cols if not c['identity'] and not c['computed']]
        fk_data = []
        any_empty = False
        for fk in fks:
            local_cols = [lc for lc, _ in fk['cols']]
            ref_cols = [rc for _, rc in fk['cols']]
            parent_rows = fetch_parent_rows(cur, fk['ref_schema'], fk['ref_table'], ref_cols)
            fk_data.append({'local': local_cols,
                            'ref_table': f"[{fk['ref_schema']}].[{fk['ref_table']}]",
                            'parent_rows': parent_rows})
            if not parent_rows:
                any_empty = True
        P(f"-- PK    : {', '.join(pk) if pk else '(sin PK)'}")
        if fk_data:
            P("-- FKs   :")
            for fk in fk_data:
                marker = "OK" if fk['parent_rows'] else "VACIA"
                P(f"--   ({', '.join(fk['local']):<40}) -> {fk['ref_table']:<35} [{marker}, {len(fk['parent_rows'])} rows]")
        P("-" * 100)
        if any_empty:
            P("-- *** OMITIDA: alguna tabla padre esta vacia. Poblar el padre primero. ***")
            skipped_tables.append(f"{db}.{tbl}")
            continue
        seed = (abs(hash(tbl)) % 9000) + 1000
        col_list = ", ".join(f"[{c['name']}]" for c in insertable)
        col_to_fk = {}
        for fi, fk in enumerate(fk_data):
            for pos, lc in enumerate(fk['local']):
                if lc not in col_to_fk:
                    col_to_fk[lc] = (fi, pos)

        for i in range(1, 11):
            fk_pick = []
            for fk in fk_data:
                pr = fk['parent_rows']
                fk_pick.append(pr[(seed + i) % len(pr)])
            vals = []
            for c in insertable:
                cn_name = c['name']
                if cn_name in col_to_fk:
                    fi, pos = col_to_fk[cn_name]
                    vals.append(sql_literal(fk_pick[fi][pos], c))
                elif c['null'] and cn_name.lower() in ('co_usua_elim', 'fe_elim',
                                                       'co_usua_veri', 'fe_veri',
                                                       'co_usua_mvto', 'fe_mvto'):
                    vals.append("NULL")
                else:
                    vals.append(gen_value(c, i, seed))
            P(f"INSERT INTO [{schema}].[{tbl}] ({col_list}) VALUES ({', '.join(vals)});")
            total += 1
    P("GO")
    cn.close()

P("")
P("=" * 100)
P(f"TOTAL INSERTS: {total}")
P(f"Tablas omitidas (parent vacio): {len(skipped_tables)}")
for t in skipped_tables:
    P(f"  - {t}")
P("=" * 100)
P("FIN")
P("=" * 100)

with open(r'C:\Users\Usuario\Downloads\Bulk\inserts_cartera.txt', 'w', encoding='utf-8') as f:
    f.write("\n".join(lines))
print(f"Wrote {len(lines)} lines, {total} INSERTs, {len(skipped_tables)} tables skipped")
