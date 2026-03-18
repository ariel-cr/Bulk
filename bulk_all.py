"""
Bulk insert into ALL tables of all modules in fcme_newcore.
Skips CDC infrastructure tables (dbo.cdc_*).
"""
import pyodbc
import time
import random
import string
import sys
from datetime import datetime, timedelta

DB_CONFIG = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123"
}

DATABASE = "fcme_newcore"
QUANTITY = 1000  # registros por tabla
BATCH_SIZE = 1000

SCHEMAS = [
    'COBRANZAS', 'CONTABILIDAD', 'CREDITOS',
    'CUENTAS_INDIVIDUALES', 'dbo', 'GARANTIAS', 'INMUEBLES',
    'INVERSIONES', 'PAGOS_PROVEEDORES', 'PARTICIPE',
    'RECAUDACIONES', 'SEGURIDAD'
]

# Tablas CDC de infraestructura a excluir
SKIP_TABLES = {
    'dbo.cdc_inbox', 'dbo.cdc_outbox', 'dbo.cdc_inbox_errors',
    'dbo.cdc_inbox_module_config'
}


def get_connection():
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DATABASE};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)


def get_all_tables(conn):
    cursor = conn.cursor()
    schema_list = ",".join([f"'{s}'" for s in SCHEMAS])
    cursor.execute(f"""
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name IN ({schema_list})
        ORDER BY s.name, t.name
    """)
    tables = []
    for row in cursor.fetchall():
        full = f"{row.schema_name}.{row.table_name}"
        if full not in SKIP_TABLES:
            tables.append((row.schema_name, row.table_name))
    return tables


def get_columns(conn, schema, table):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS is_identity
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """, schema, table)
    columns = []
    for row in cursor.fetchall():
        columns.append({
            "name": row.COLUMN_NAME,
            "type": row.DATA_TYPE,
            "max_length": row.CHARACTER_MAXIMUM_LENGTH,
            "nullable": row.IS_NULLABLE == "YES",
            "has_default": row.COLUMN_DEFAULT is not None,
            "is_identity": row.is_identity == 1
        })
    return columns


def get_max_id(conn, schema, table, column):
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT ISNULL(MAX(TRY_CAST([{column}] AS BIGINT)), 0) FROM [{schema}].[{table}]")
        result = cursor.fetchone()[0]
        if result and int(result) > 0:
            return int(result)
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        count = cursor.fetchone()[0]
        return int(time.time()) + count
    except:
        return int(time.time())


def generate_value(col, index, offset):
    name = col["name"].lower()
    dtype = col["type"].lower()
    max_len = col["max_length"] or 50
    unique_id = offset + index

    # IDs string
    if name.endswith("id") and dtype in ("varchar", "nvarchar", "char", "nchar"):
        return f"BLK{unique_id}"[:min(max_len, 20)]

    # IDs numerico
    if name.endswith("id") and dtype in ("int", "bigint"):
        return unique_id

    if name.endswith("id") and dtype in ("smallint", "tinyint"):
        return unique_id % 30000

    # Uniqueidentifier
    if dtype == "uniqueidentifier":
        import uuid
        return str(uuid.uuid4())

    # Numericos
    if dtype in ("int", "bigint"):
        return random.randint(1, 10000)
    if dtype in ("smallint", "tinyint"):
        return random.randint(1, 100)
    if dtype in ("decimal", "numeric", "float", "real"):
        return round(random.uniform(100, 99999), 2)
    if dtype == "money":
        return round(random.uniform(100, 50000), 2)
    if dtype == "bit":
        return random.randint(0, 1)

    # Fechas
    if dtype in ("datetime", "datetime2", "date"):
        days_ago = random.randint(0, 365)
        return datetime.now() - timedelta(days=days_ago)
    if dtype == "time":
        return f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:00"

    # Strings
    if dtype in ("varchar", "nvarchar", "char", "nchar", "text", "ntext"):
        if "nombre" in name or "name" in name:
            return f"Test {col['name']} {unique_id}"[:max_len]
        if "descripcion" in name or "desc" in name or "observacion" in name:
            return f"Descripcion bulk {unique_id}"[:max_len]
        if "direccion" in name or "address" in name:
            return f"Av. Test {unique_id}, Sector {index}"[:max_len]
        if "telefono" in name or "phone" in name:
            return f"09{random.randint(10000000,99999999)}"[:max_len]
        if "correo" in name or "email" in name:
            return f"test{unique_id}@bulk.com"[:max_len]
        if "estado" in name or "status" in name:
            if max_len <= 2:
                return random.choice(["A", "I", "E"])[:max_len]
            return random.choice(["A", "ACTIVO", "I"])[:max_len]
        if "codigo" in name or "code" in name:
            return f"{random.randint(1,999):05d}"[:max_len]
        if "usuario" in name or "user" in name:
            return f"user{random.randint(1,50)}"[:max_len]
        if "moneda" in name:
            return "USD"[:max_len]
        if "pais" in name:
            return "ECUADOR"[:max_len]
        if "provincia" in name:
            return "GUAYAS"[:max_len]
        if "ciudad" in name or "canton" in name:
            return "GUAYAQUIL"[:max_len]
        if "sector" in name:
            return f"Sector {index}"[:max_len]
        if "referencia" in name:
            return f"Ref bulk {unique_id}"[:max_len]
        if "calle" in name:
            return f"Calle {index} y Av {index+1}"[:max_len]
        # Generico
        text_len = min(max_len, 30)
        return f"BULK_{col['name']}_{unique_id}"[:text_len]

    # Binarios
    if dtype in ("varbinary", "binary", "image"):
        return None

    return None


def insert_table(conn, schema, table, quantity):
    """Insert bulk data into a single table. Returns (inserted, error_msg)"""
    cursor = conn.cursor()

    columns = get_columns(conn, schema, table)
    insertable = [c for c in columns if not c["is_identity"]]

    if not insertable:
        return 0, "No insertable columns"

    col_names = ", ".join([f"[{c['name']}]" for c in insertable])
    placeholders = ", ".join(["?" for _ in insertable])
    sql = f"INSERT INTO [{schema}].[{table}] ({col_names}) VALUES ({placeholders})"

    # Get offset for unique IDs
    first_id_col = next((c for c in insertable if c["name"].lower().endswith("id")), None)
    offset = 0
    if first_id_col:
        offset = get_max_id(conn, schema, table, first_id_col["name"]) + 1

    # Disable triggers
    try:
        cursor.execute(f"DISABLE TRIGGER ALL ON [{schema}].[{table}]")
        conn.commit()
    except:
        pass

    inserted = 0
    error_msg = None

    while inserted < quantity:
        current_batch = min(BATCH_SIZE, quantity - inserted)
        rows = []
        for i in range(current_batch):
            row = []
            for col in insertable:
                row.append(generate_value(col, inserted + i + 1, offset))
            rows.append(tuple(row))

        try:
            cursor.fast_executemany = True
            cursor.executemany(sql, rows)
            conn.commit()
            inserted += current_batch
        except Exception as e:
            conn.rollback()
            error_msg = str(e)[:300]
            # Try single row to get better error
            try:
                cursor.execute(sql, rows[0])
                conn.commit()
                inserted += 1
            except Exception as e2:
                conn.rollback()
                error_msg = str(e2)[:300]
            break

    # Re-enable triggers
    try:
        cursor.execute(f"ENABLE TRIGGER ALL ON [{schema}].[{table}]")
        conn.commit()
    except:
        pass

    return inserted, error_msg


def main():
    quantity = int(sys.argv[1]) if len(sys.argv) > 1 else QUANTITY

    print(f"={'='*70}")
    print(f"  BULK INSERT - fcme_newcore - {quantity} registros por tabla")
    print(f"={'='*70}")
    print()

    conn = get_connection()
    tables = get_all_tables(conn)
    print(f"Tablas a procesar: {len(tables)}")
    print(f"Registros por tabla: {quantity:,}")
    print(f"Total estimado: {len(tables) * quantity:,} registros")
    print()

    results = {"ok": [], "partial": [], "failed": []}
    total_inserted = 0
    total_start = time.time()

    current_schema = None
    for i, (schema, table) in enumerate(tables, 1):
        if schema != current_schema:
            current_schema = schema
            print(f"\n--- {schema} ---")

        start = time.time()
        inserted, error = insert_table(conn, schema, table, quantity)
        elapsed = time.time() - start
        total_inserted += inserted

        status = "OK" if inserted == quantity else ("PARTIAL" if inserted > 0 else "FAIL")
        rps = round(inserted / elapsed, 1) if elapsed > 0 else 0

        if status == "OK":
            results["ok"].append(f"{schema}.{table}")
            print(f"  [{i:3d}/{len(tables)}] {table:45s} {inserted:>6,} rows  {elapsed:>6.2f}s  {rps:>8,.1f} r/s")
        elif status == "PARTIAL":
            results["partial"].append((f"{schema}.{table}", inserted, error))
            print(f"  [{i:3d}/{len(tables)}] {table:45s} {inserted:>6,}/{quantity:,}  PARTIAL  {error[:80]}")
        else:
            results["failed"].append((f"{schema}.{table}", error))
            print(f"  [{i:3d}/{len(tables)}] {table:45s}   FAIL  {error[:80]}")

    total_time = time.time() - total_start
    conn.close()

    # Summary
    print(f"\n{'='*70}")
    print(f"  RESUMEN")
    print(f"{'='*70}")
    print(f"  Total insertado:  {total_inserted:>10,} registros")
    print(f"  Tiempo total:     {total_time:>10.1f} segundos")
    print(f"  Velocidad:        {total_inserted/total_time:>10,.1f} rows/seg" if total_time > 0 else "")
    print(f"  Exitosas:         {len(results['ok']):>10d} tablas")
    print(f"  Parciales:        {len(results['partial']):>10d} tablas")
    print(f"  Fallidas:         {len(results['failed']):>10d} tablas")
    print()

    if results["partial"]:
        print("  PARCIALES:")
        for t, count, err in results["partial"]:
            print(f"    {t}: {count} rows - {err[:100]}")
        print()

    if results["failed"]:
        print("  FALLIDAS:")
        for t, err in results["failed"]:
            print(f"    {t}: {err[:120]}")
        print()


if __name__ == "__main__":
    main()
