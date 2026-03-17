"""Fix the 9 tables that failed in the initial bulk insert."""
import pyodbc
import random
import time
from datetime import datetime, timedelta

conn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_newcore;UID=sa;PWD=YourPassword123'
)
cursor = conn.cursor()
QUANTITY = 1000


def get_max(schema, table, col):
    try:
        cursor.execute(f"SELECT ISNULL(MAX(TRY_CAST([{col}] AS BIGINT)), 0) FROM [{schema}].[{table}]")
        r = cursor.fetchone()[0]
        return int(r) if r else 0
    except:
        return 0


def get_columns(schema, table):
    cursor.execute("""
        SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
               c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE,
               COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS is_identity
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """, schema, table)
    cols = []
    for r in cursor.fetchall():
        if r.is_identity == 1:
            continue
        cols.append({
            'name': r.COLUMN_NAME,
            'type': r.DATA_TYPE,
            'max_len': r.CHARACTER_MAXIMUM_LENGTH,
            'precision': r.NUMERIC_PRECISION,
            'scale': r.NUMERIC_SCALE,
        })
    return cols


def gen_value(c, uid, unique_col=None):
    name = c['name'].lower()
    dtype = c['type'].lower()

    # If this is the unique column, always use sequential uid
    if c['name'] == unique_col:
        if dtype in ('int', 'bigint', 'smallint'):
            return uid
        ml = c['max_len'] or 30
        return f"BLK{uid}"[:ml]

    if name.endswith('id') and dtype in ('int', 'bigint'):
        return uid
    if name.endswith('id') and dtype in ('smallint', 'tinyint'):
        return uid % 30000
    if name.endswith('id') and dtype in ('varchar', 'nvarchar', 'char', 'nchar'):
        ml = c['max_len'] or 20
        return f"BLK{uid}"[:ml]

    if dtype in ('decimal', 'numeric'):
        prec = c['precision'] or 18
        scale = c['scale'] or 2
        max_int_digits = prec - scale
        max_val = min(10 ** max_int_digits - 1, 999)
        return round(random.uniform(1, max_val), scale)
    if dtype in ('int', 'bigint'):
        return random.randint(1, 10000)
    if dtype in ('smallint', 'tinyint'):
        return random.randint(1, 100)
    if dtype == 'bit':
        return random.randint(0, 1)
    if dtype == 'money':
        return round(random.uniform(100, 50000), 2)
    if dtype in ('datetime', 'datetime2', 'date'):
        return datetime.now() - timedelta(days=random.randint(0, 365))
    if dtype == 'time':
        return f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:00"
    if dtype in ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext'):
        ml = c['max_len'] or 30
        return f"BULK_{c['name']}_{uid}"[:ml]

    return None


def insert_table(schema, table, unique_col=None):
    cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
    before = cursor.fetchone()[0]

    cols = get_columns(schema, table)
    if not cols:
        print(f"  {schema}.{table}: No insertable columns")
        return

    # Get offset
    ref_col = unique_col or next((c['name'] for c in cols if c['name'].lower().endswith('id')), None)
    offset = get_max(schema, table, ref_col) + 1 if ref_col else int(time.time())

    col_names = ", ".join([f"[{c['name']}]" for c in cols])
    placeholders = ", ".join(["?" for _ in cols])
    sql = f"INSERT INTO [{schema}].[{table}] ({col_names}) VALUES ({placeholders})"

    rows = []
    for i in range(QUANTITY):
        uid = offset + i + 1
        row = tuple(gen_value(c, uid, unique_col) for c in cols)
        rows.append(row)

    try:
        cursor.execute(f"DISABLE TRIGGER ALL ON [{schema}].[{table}]")
        conn.commit()
    except:
        pass

    start = time.time()
    try:
        cursor.fast_executemany = True
        cursor.executemany(sql, rows)
        conn.commit()
        elapsed = time.time() - start

        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        after = cursor.fetchone()[0]
        rps = round(QUANTITY / elapsed, 1)
        print(f"  {schema}.{table:45s} +{after - before:,} rows  {elapsed:.2f}s  {rps:,.1f} r/s")
    except Exception as e:
        conn.rollback()
        print(f"  {schema}.{table}: ERROR - {str(e)[:150]}")

    try:
        cursor.execute(f"ENABLE TRIGGER ALL ON [{schema}].[{table}]")
        conn.commit()
    except:
        pass


print("=== Fixing PAGOS_PROVEEDORES (decimal overflow) ===")
insert_table('PAGOS_PROVEEDORES', 'CASTIGOINVERSIONDETALLETYPE')
insert_table('PAGOS_PROVEEDORES', 'TIPORETENCIONPAGOTYPE')

print("\n=== Fixing SEGURIDAD (UNIQUE constraints) ===")
seg_uniques = {
    'DETALLEUSUARIOSEGURIDADTYPE': 'usuario_codigoUsuario',
    'OPCIONMENUTYPE': 'codigoOpcionMenu',
    'PERFILACCESOTYPE': 'codigoPerfilAcceso',
    'PERFILOPCIONMENUTYPE': 'codigoPerfilOpcionMenu',
    'ROLTYPE': 'codigoRol',
    'USUARIOPERFILACCESOTYPE': 'codigoUsuarioPerfilAcceso',
    'USUARIOTYPE': 'codigoUsuario',
}

for table, unique_col in seg_uniques.items():
    insert_table('SEGURIDAD', table, unique_col)

conn.close()
print("\nDone!")
