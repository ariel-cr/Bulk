"""Generador de datos fake para inserts masivos."""
import random
import time
from datetime import datetime, timedelta

# Limites por tipo numerico de SQL Server
INT_MAX      = 2_147_483_647
SMALLINT_MAX = 32_767
TINYINT_MAX  = 255

def _numeric_pk(unique_id, dtype):
    """ID numerico determinista pero acotado al rango del tipo."""
    if dtype == "tinyint":
        return unique_id % TINYINT_MAX
    if dtype == "smallint":
        return unique_id % SMALLINT_MAX
    if dtype in ("int",):
        return unique_id % INT_MAX
    return unique_id


def generate_fake_value(col, index, offset, is_pk=False, fk_values=None):
    """Genera un valor fake segun el tipo de dato.
    fk_values: dict {col_name: [valores validos]} para cols con FK constraint."""
    name = col["name"].lower()
    dtype = col["type"].lower()
    max_len = col["max_length"] or 50
    unique_id = offset + index

    # 1) FK: usar valor real de la tabla referenciada
    #    Pick rota por (offset+index) para que distintas corridas elijan filas
    #    distintas y no colisionen en PK (cuando la PK incluye FKs).
    if fk_values and col["name"] in fk_values:
        vals = fk_values[col["name"]]
        if vals:
            return vals[unique_id % len(vals)]

    # 2) Defaults por convencion: co_empr siempre 1 (sistema mono-tenant)
    #    Solo si NO es PK — si es PK, dejamos que el bloque de PK genere unico
    if not is_pk and name in ("co_empr", "ci_empresa", "codigoempresa"):
        return 1

    # 3) PKs/IDs: clamp al rango del tipo numerico para evitar overflow
    if is_pk or name.endswith("id") or name.endswith("_id"):
        if dtype in ("varchar", "nvarchar", "char", "nchar"):
            prefix = "BLK"
            max_id_digits = max(1, min(max_len, 20) - len(prefix))
            short_id = unique_id % (10 ** max_id_digits)
            return f"{prefix}{short_id}"[:min(max_len, 20)]
        if dtype == "tinyint":
            return _numeric_pk(unique_id, "tinyint")
        if dtype == "smallint":
            return _numeric_pk(unique_id, "smallint")
        if dtype == "int":
            return _numeric_pk(unique_id, "int")
        if dtype == "bigint":
            return unique_id

    # 4) Tipos numericos no-PK (siempre dentro del rango)
    if dtype == "tinyint":
        return random.randint(1, min(100, TINYINT_MAX))
    if dtype == "smallint":
        return random.randint(1, min(100, SMALLINT_MAX))
    if dtype == "int":
        return random.randint(1, 10000)
    if dtype == "bigint":
        return random.randint(1, 10000)
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
