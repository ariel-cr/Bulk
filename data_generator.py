"""Generador de datos fake para inserts masivos."""
import random
import time
from datetime import datetime, timedelta


def generate_fake_value(col, index, offset, is_pk=False, fk_values=None):
    """Genera un valor fake segun el tipo de dato.
    fk_values: dict {col_name: [valores validos]} para cols con FK constraint."""
    name = col["name"].lower()
    dtype = col["type"].lower()
    max_len = col["max_length"] or 50
    unique_id = offset + index

    # Si la columna tiene FK, usar valores validos de la tabla referenciada
    if fk_values and col["name"] in fk_values:
        vals = fk_values[col["name"]]
        return vals[index % len(vals)]

    # Columnas PK o que terminan en "id" - usar offset para unicidad
    if (is_pk or name.endswith("id")) and dtype in ("varchar", "nvarchar", "char", "nchar"):
        return f"BLK{unique_id}"[:min(max_len, 20)]

    if (is_pk or name.endswith("id")) and dtype in ("int", "bigint"):
        return unique_id

    if (is_pk or name.endswith("id")) and dtype in ("smallint", "tinyint"):
        return unique_id % 30000

    # Tipos numericos
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
        if "estado" in name or "status" in name or name.startswith("st_"):
            if max_len <= 2:
                return random.choice(["A", "E", "E"])[:max_len]
            return random.choice(["A", "ACTIVO", "E"])[:max_len]
        if name.startswith("in_"):
            if max_len <= 2:
                return random.choice(["S", "N"])[:max_len]
            return random.choice(["SI", "NO"])[:max_len]
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
        text_len = min(max_len, 30)
        return f"BULK_{col['name']}_{unique_id}"[:text_len]

    return None


def get_max_numeric_id(conn, schema, table, column):
    """Obtiene el maximo ID numerico de una columna, incluyendo IDs con prefijo BLK/BULK_"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT ISNULL(MAX(val), 0) FROM (
                SELECT TRY_CAST([{column}] AS BIGINT) AS val FROM [{schema}].[{table}]
                UNION ALL
                SELECT TRY_CAST(
                    REPLACE(REPLACE(REPLACE([{column}], 'BLK', ''), 'BULK_', ''), ' ', '')
                AS BIGINT) FROM [{schema}].[{table}]
            ) t WHERE val IS NOT NULL
        """)
        result = cursor.fetchone()[0]
        if result and int(result) > 0:
            return int(result)
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        count = cursor.fetchone()[0]
        return int(time.time()) + count
    except:
        return int(time.time())
