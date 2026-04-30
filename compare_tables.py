"""Compara columnas de las 5 tablas participes entre fcme_canonicos y fcme_canonicos_normalizada"""
import pyodbc

DB_CONFIG = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123"
}

TABLES = [
    "naturalInformacionAdicionalType",
    "naturalInformacionBasicaType",
    "naturalTrabajoType",
    "personaDireccionesType",
    "personaFirmasType",
]

def get_conn(db_name):
    return pyodbc.connect(
        f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};"
        f"DATABASE={db_name};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}"
    )

def get_columns(db_name, schema, table):
    conn = get_conn(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS is_identity
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """, schema, table)
    cols = []
    for row in cursor.fetchall():
        cols.append({
            "name": row.COLUMN_NAME,
            "type": row.DATA_TYPE,
            "max_length": row.CHARACTER_MAXIMUM_LENGTH,
            "precision": row.NUMERIC_PRECISION,
            "scale": row.NUMERIC_SCALE,
            "nullable": row.IS_NULLABLE,
            "is_identity": row.is_identity
        })
    conn.close()
    return cols

def format_type(col):
    if col["type"] == "varchar":
        return f"varchar({col['max_length']})"
    elif col["type"] == "numeric":
        return f"numeric({col['precision']},{col['scale']})"
    elif col["type"] == "int":
        return "int"
    elif col["type"] == "date":
        return "date"
    else:
        return col["type"]

for table in TABLES:
    print(f"\n{'='*80}")
    print(f"  {table}")
    print(f"{'='*80}")

    try:
        canonicos = get_columns("fcme_canonicos", "participes", table)
    except Exception as e:
        print(f"  ERROR fcme_canonicos: {e}")
        continue

    try:
        normalizada = get_columns("fcme_canonicos_normalizada", "participes", table)
    except Exception as e:
        print(f"  ERROR fcme_canonicos_normalizada: {e}")
        continue

    canon_dict = {c["name"]: c for c in canonicos}
    norm_dict = {c["name"]: c for c in normalizada}

    all_cols = []
    for c in canonicos:
        all_cols.append(c["name"])
    for c in normalizada:
        if c["name"] not in all_cols:
            all_cols.append(c["name"])

    print(f"  {'COLUMNA':<40} {'CANONICOS':<25} {'NORMALIZADA':<25} {'COINCIDE'}")
    print(f"  {'-'*40} {'-'*25} {'-'*25} {'-'*10}")

    for col_name in all_cols:
        c = canon_dict.get(col_name)
        n = norm_dict.get(col_name)

        c_type = format_type(c) if c else "-- NO EXISTE --"
        n_type = format_type(n) if n else "-- NO EXISTE --"

        c_null = c["nullable"] if c else ""
        n_null = n["nullable"] if n else ""

        c_ident = " IDENTITY" if c and c["is_identity"] == 1 else ""
        n_ident = " IDENTITY" if n and n["is_identity"] == 1 else ""

        c_full = f"{c_type}{c_ident} {'NULL' if c_null == 'YES' else 'NOT NULL' if c_null else ''}"
        n_full = f"{n_type}{n_ident} {'NULL' if n_null == 'YES' else 'NOT NULL' if n_null else ''}"

        match = "OK" if c and n and c_type == n_type else "DIFF" if c and n else "MISSING"

        print(f"  {col_name:<40} {c_full:<25} {n_full:<25} {match}")

print(f"\n{'='*80}")
print("  Verificando si fcme_canonicos tiene columna 'id' en cada tabla...")
print(f"{'='*80}")
for table in TABLES:
    cols = get_columns("fcme_canonicos", "participes", table)
    has_id = any(c["name"] == "id" for c in cols)
    print(f"  {table}: {'SI tiene id' if has_id else 'NO tiene id'}")
