from flask import Flask, render_template, request, jsonify
import pyodbc
import time
import random
import string
from datetime import datetime, timedelta

app = Flask(__name__)

DB_CONFIG = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123"
}

DATABASES = {
    "newcore": "fcme_newcore",
    "legacy": "fcme_legacy",
    "dbIM": "dbIM",
}

# Módulos que en legacy_to_newcore usan bases de datos originales
# Mapeo: módulo → { db: nombre_bd, tables: [tablas que usa el pipeline CDC] }
LEGACY_ORIGINAL_DBS = {
    "INMUEBLES": {
        "db": "dbIM",
        "tables": [
            "imtbbene_entr",
            "imtbbene_firm",
            "imtbcben",
            "imtbcpro",
            "imtbdivi",
            "imtbmanz",
            "imtbmejo_tram",
        ]
    },
}

# Schemas por dirección
DIRECTION_CONFIG = {
    "newcore_to_legacy": {
        "label": "Newcore → Legacy",
        "description": "Insertar en tablas de Newcore (pasan por outbox → Kafka → inbox Legacy)",
        "database": "newcore",
        "modules": {}  # se llena dinámicamente
    },
    "legacy_to_newcore": {
        "label": "Legacy → Newcore",
        "description": "Insertar en tablas originales Legacy (pasan por outbox → Kafka → inbox Newcore)",
        "database": "legacy",
        "modules": {}
    }
}


def get_connection(db_key="newcore"):
    db_name = DATABASES[db_key]
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={db_name};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)


def get_schemas_and_tables(db_key):
    """Lee schemas y tablas dinámicamente de la BD"""
    conn = get_connection(db_key)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name NOT IN ('dbo', 'sys', 'INFORMATION_SCHEMA')
        ORDER BY s.name, t.name
    """)
    result = {}
    for row in cursor.fetchall():
        schema = row.schema_name
        table = row.table_name
        if schema not in result:
            result[schema] = []
        result[schema].append(table)
    conn.close()
    return result


def get_table_columns(db_key, schema, table):
    """Lee columnas y tipos de una tabla"""
    conn = get_connection(db_key)
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
    conn.close()
    return columns


def generate_fake_value(col, index, offset):
    """Genera un valor fake según el tipo de dato"""
    name = col["name"].lower()
    dtype = col["type"].lower()
    max_len = col["max_length"] or 50
    unique_id = offset + index

    # Campos de ID con nombre que sugiere PK
    if name.endswith("id") and dtype in ("varchar", "nvarchar", "char", "nchar"):
        return f"BLK{unique_id}"[:min(max_len, 20)]

    if name.endswith("id") and dtype in ("int", "bigint"):
        return unique_id

    if name.endswith("id") and dtype in ("smallint", "tinyint"):
        return unique_id % 30000

    # Tipos numéricos
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
        # Nombres descriptivos según el campo
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
        # Genérico
        text_len = min(max_len, 30)
        return f"BULK_{col['name']}_{unique_id}"[:text_len]

    return None


def get_max_numeric_id(conn, schema, table, column):
    """Obtiene el máximo ID numérico de una columna, o el COUNT si no es numérico"""
    try:
        cursor = conn.cursor()
        # Intentar MAX numérico
        cursor.execute(f"SELECT ISNULL(MAX(TRY_CAST([{column}] AS BIGINT)), 0) FROM [{schema}].[{table}]")
        result = cursor.fetchone()[0]
        if result and int(result) > 0:
            return int(result)
        # Fallback: usar timestamp para unicidad
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        count = cursor.fetchone()[0]
        import time
        return int(time.time()) + count
    except:
        import time
        return int(time.time())


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/modules/<direction>")
def get_modules(direction):
    """Retorna módulos (schemas) y tablas para una dirección"""
    if direction == "newcore_to_legacy":
        db_key = "newcore"
        try:
            schemas = get_schemas_and_tables(db_key)
            return jsonify(schemas)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        # legacy_to_newcore: combinar tablas de fcme_legacy + bases originales
        try:
            schemas = get_schemas_and_tables("legacy")
        except:
            schemas = {}

        # Agregar módulos de bases originales (dbIM, etc.)
        for module_name, config in LEGACY_ORIGINAL_DBS.items():
            schemas[module_name] = sorted(config["tables"])

        return jsonify(schemas)


def resolve_db_key(direction, schema):
    """Determina qué base de datos usar según dirección y módulo"""
    if direction == "newcore_to_legacy":
        return "newcore"
    if schema in LEGACY_ORIGINAL_DBS:
        return LEGACY_ORIGINAL_DBS[schema]["db"]
    return "legacy"


def resolve_table_schema(direction, schema):
    """En bases originales (dbIM, etc.) las tablas están en dbo"""
    if direction == "legacy_to_newcore" and schema in LEGACY_ORIGINAL_DBS:
        return "dbo"
    return schema


@app.route("/api/columns", methods=["POST"])
def get_columns():
    """Retorna columnas de una tabla"""
    data = request.json
    direction = data.get("direction", "newcore_to_legacy")
    schema = data.get("schema")
    table = data.get("table")
    db_key = resolve_db_key(direction, schema)
    actual_schema = resolve_table_schema(direction, schema)

    try:
        columns = get_table_columns(db_key, actual_schema, table)
        return jsonify(columns)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/test", methods=["POST"])
def run_test():
    data = request.json
    direction = data.get("direction", "newcore_to_legacy")
    schema = data.get("schema")
    table = data.get("table")
    quantity = int(data.get("quantity", 100))
    disable_triggers = data.get("disable_triggers", False)

    db_key = resolve_db_key(direction, schema)
    actual_schema = resolve_table_schema(direction, schema)
    batch_size = min(1000, quantity)

    try:
        # Leer columnas
        columns = get_table_columns(db_key, actual_schema, table)
        # Filtrar identity y columnas con default que no necesitan valor
        insertable_cols = [c for c in columns if not c["is_identity"]]

        col_names = ", ".join([f"[{c['name']}]" for c in insertable_cols])
        placeholders = ", ".join(["?" for _ in insertable_cols])
        sql = f"INSERT INTO [{actual_schema}].[{table}] ({col_names}) VALUES ({placeholders})"

        conn = get_connection(db_key)
        cursor = conn.cursor()

        # Offset para IDs únicos
        first_id_col = next((c for c in insertable_cols if c["name"].lower().endswith("id")), None)
        offset = 0
        if first_id_col:
            offset = get_max_numeric_id(conn, actual_schema, table, first_id_col["name"]) + 1

        # Conteos antes
        cursor.execute(f"SELECT COUNT(*) FROM [{actual_schema}].[{table}]")
        count_before = cursor.fetchone()[0]

        outbox_before = 0
        inbox_other_before = 0
        other_db = "legacy" if direction == "newcore_to_legacy" else "newcore"
        other_db_name = DATABASES.get(other_db, other_db)
        try:
            cursor.execute(f"SELECT COUNT(*) FROM dbo.cdc_outbox")
            outbox_before = cursor.fetchone()[0]
        except:
            pass
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {other_db_name}.dbo.cdc_inbox")
            inbox_other_before = cursor.fetchone()[0]
        except:
            pass

        # Desactivar triggers
        if disable_triggers:
            try:
                cursor.execute(f"DISABLE TRIGGER ALL ON [{actual_schema}].[{table}]")
                conn.commit()
            except:
                pass

        # Insertar
        metrics = {
            "direction": direction,
            "schema": schema,
            "table": table,
            "full_table": f"{actual_schema}.{table}" if actual_schema != schema else f"{schema}.{table}",
            "quantity": quantity,
            "triggers_disabled": disable_triggers,
            "columns_count": len(insertable_cols),
            "batches": [],
            "errors": []
        }

        total_start = time.time()
        inserted = 0
        batch_num = 0

        while inserted < quantity:
            batch_num += 1
            current_batch = min(batch_size, quantity - inserted)
            batch_start = time.time()

            rows = []
            for i in range(current_batch):
                row = []
                for col in insertable_cols:
                    row.append(generate_fake_value(col, inserted + i + 1, offset))
                rows.append(tuple(row))

            try:
                cursor.fast_executemany = True
                cursor.executemany(sql, rows)
                conn.commit()
                batch_time = time.time() - batch_start

                metrics["batches"].append({
                    "batch": batch_num,
                    "rows": current_batch,
                    "time_sec": round(batch_time, 3),
                    "rows_per_sec": round(current_batch / batch_time, 1) if batch_time > 0 else 0
                })
                inserted += current_batch

            except Exception as e:
                conn.rollback()
                metrics["errors"].append(f"Batch {batch_num}: {str(e)[:200]}")
                # Intentar uno por uno para identificar el error
                individual_ok = 0
                for row in rows[:10]:  # solo los primeros 10 para diagnosticar
                    try:
                        cursor.execute(sql, row)
                        conn.commit()
                        individual_ok += 1
                        inserted += 1
                    except Exception as e2:
                        conn.rollback()
                        metrics["errors"].append(f"Row detail: {str(e2)[:150]}")
                        break
                if individual_ok == 0:
                    break  # todos fallan, no seguir
                break

        total_time = time.time() - total_start

        # Reactivar triggers
        if disable_triggers:
            try:
                cursor.execute(f"ENABLE TRIGGER ALL ON [{actual_schema}].[{table}]")
                conn.commit()
            except:
                pass

        # Conteos después
        cursor.execute(f"SELECT COUNT(*) FROM [{actual_schema}].[{table}]")
        count_after = cursor.fetchone()[0]

        outbox_after = 0
        inbox_other_after = 0
        try:
            cursor.execute(f"SELECT COUNT(*) FROM dbo.cdc_outbox")
            outbox_after = cursor.fetchone()[0]
        except:
            pass
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {other_db_name}.dbo.cdc_inbox")
            inbox_other_after = cursor.fetchone()[0]
        except:
            pass

        conn.close()

        metrics.update({
            "total_time_sec": round(total_time, 3),
            "total_inserted": inserted,
            "avg_rows_per_sec": round(inserted / total_time, 1) if total_time > 0 else 0,
            "count_before": count_before,
            "count_after": count_after,
            "net_inserted": count_after - count_before,
            "outbox_generated": outbox_after - outbox_before,
            "inbox_other_received": inbox_other_after - inbox_other_before,
            "total_errors": len(metrics["errors"])
        })

        return jsonify(metrics)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/status")
def get_status():
    """Estado de las colas CDC"""
    status = {}
    try:
        for db_key, db_name in DATABASES.items():
            conn = get_connection(db_key)
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
                status[f"{db_key}_outbox"] = cursor.fetchone()[0]
            except:
                status[f"{db_key}_outbox"] = "N/A"
            try:
                cursor.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed = 0")
                status[f"{db_key}_inbox_pending"] = cursor.fetchone()[0]
            except:
                status[f"{db_key}_inbox_pending"] = "N/A"
            try:
                cursor.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed = 1")
                status[f"{db_key}_inbox_processed"] = cursor.fetchone()[0]
            except:
                status[f"{db_key}_inbox_processed"] = "N/A"
            try:
                cursor.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
                status[f"{db_key}_errors"] = cursor.fetchone()[0]
            except:
                status[f"{db_key}_errors"] = "N/A"
            conn.close()
    except Exception as e:
        status["error"] = str(e)

    return jsonify(status)


@app.route("/api/cleanup", methods=["POST"])
def cleanup():
    data = request.json
    direction = data.get("direction", "newcore_to_legacy")
    schema = data.get("schema")
    table = data.get("table")
    db_key = resolve_db_key(direction, schema)
    actual_schema = resolve_table_schema(direction, schema)

    try:
        conn = get_connection(db_key)
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM [{actual_schema}].[{table}]")
        before = cursor.fetchone()[0]

        try:
            cursor.execute(f"DISABLE TRIGGER ALL ON [{actual_schema}].[{table}]")
            conn.commit()
        except:
            pass

        cursor.execute(f"DELETE FROM [{actual_schema}].[{table}]")
        deleted = cursor.rowcount
        conn.commit()

        try:
            cursor.execute(f"ENABLE TRIGGER ALL ON [{actual_schema}].[{table}]")
            conn.commit()
        except:
            pass

        conn.close()
        return jsonify({"table": f"{actual_schema}.{table}", "deleted": deleted, "before": before})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/truncate", methods=["POST"])
def truncate_table():
    data = request.json
    direction = data.get("direction", "newcore_to_legacy")
    schema = data.get("schema")
    table = data.get("table")
    db_key = resolve_db_key(direction, schema)
    actual_schema = resolve_table_schema(direction, schema)

    try:
        conn = get_connection(db_key)
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM [{actual_schema}].[{table}]")
        before = cursor.fetchone()[0]

        try:
            cursor.execute(f"DISABLE TRIGGER ALL ON [{actual_schema}].[{table}]")
            conn.commit()
        except:
            pass

        try:
            cursor.execute(f"TRUNCATE TABLE [{actual_schema}].[{table}]")
            conn.commit()
            method = "TRUNCATE"
        except Exception:
            # TRUNCATE falla si hay FK, intentar DELETE
            conn.rollback()
            cursor.execute(f"DELETE FROM [{actual_schema}].[{table}]")
            conn.commit()
            method = "DELETE (TRUNCATE no permitido por FK)"

        try:
            cursor.execute(f"ENABLE TRIGGER ALL ON [{actual_schema}].[{table}]")
            conn.commit()
        except:
            pass

        conn.close()
        return jsonify({"table": f"{actual_schema}.{table}", "deleted": before, "method": method})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cdc-pending-summary/<db_key>")
def get_pending_summary(db_key):
    """Diagnóstico de registros pendientes en cdc_inbox"""
    if db_key not in DATABASES:
        return jsonify({"error": "DB inválida"}), 400

    try:
        conn = get_connection(db_key)
        cursor = conn.cursor()

        # Total pendientes
        cursor.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed = 0")
        total = cursor.fetchone()[0]

        if total == 0:
            conn.close()
            return jsonify({"total": 0, "groups": [], "oldest": None, "newest": None})

        # Agrupado por aggregate_type + event_type
        cursor.execute("""
            SELECT aggregate_type, event_type, source_table,
                   COUNT(*) as cnt,
                   MIN(created_at) as oldest,
                   MAX(created_at) as newest
            FROM dbo.cdc_inbox WHERE processed = 0
            GROUP BY aggregate_type, event_type, source_table
            ORDER BY cnt DESC
        """)
        groups = []
        for r in cursor.fetchall():
            groups.append({
                "aggregate_type": r.aggregate_type,
                "event_type": r.event_type,
                "source_table": r.source_table or "N/A",
                "count": r.cnt,
                "oldest": str(r.oldest),
                "newest": str(r.newest)
            })

        # Rango de fechas global
        cursor.execute("""
            SELECT MIN(created_at), MAX(created_at),
                   DATEDIFF(MINUTE, MAX(created_at), GETDATE()) as minutes_ago
            FROM dbo.cdc_inbox WHERE processed = 0
        """)
        row = cursor.fetchone()
        oldest = str(row[0]) if row[0] else None
        newest = str(row[1]) if row[1] else None
        minutes_stuck = row[2] if row[2] else 0

        conn.close()
        return jsonify({
            "total": total,
            "groups": groups,
            "oldest": oldest,
            "newest": newest,
            "minutes_stuck": minutes_stuck
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cdc-data/<db_key>/<table_name>")
def get_cdc_data(db_key, table_name):
    """Retorna datos de cualquier tabla CDC (inbox, outbox, inbox_errors)"""
    allowed = ['cdc_inbox', 'cdc_outbox', 'cdc_inbox_errors']
    if db_key not in DATABASES:
        return jsonify({"error": "DB inválida"}), 400
    if table_name not in allowed:
        return jsonify({"error": "Tabla no permitida"}), 400

    filter_type = request.args.get("filter")  # 'pending', 'processed'
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 100))
    offset = (page - 1) * per_page

    try:
        conn = get_connection(db_key)
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        col_names = [row.COLUMN_NAME for row in cursor.fetchall()]

        if not col_names:
            conn.close()
            return jsonify({"columns": [], "rows": [], "total": 0, "page": page, "pages": 0})

        cols_sql = ", ".join([f"[{c}]" for c in col_names])

        where = ""
        if table_name == 'cdc_inbox' and filter_type == 'pending':
            where = "WHERE processed = 0"
        elif table_name == 'cdc_inbox' and filter_type == 'processed':
            where = "WHERE processed = 1"

        cursor.execute(f"SELECT COUNT(*) FROM dbo.[{table_name}] {where}")
        total = cursor.fetchone()[0]
        total_pages = (total + per_page - 1) // per_page if total > 0 else 1

        cursor.execute(f"""
            SELECT {cols_sql} FROM dbo.[{table_name}] {where}
            ORDER BY 1 DESC
            OFFSET {offset} ROWS FETCH NEXT {per_page} ROWS ONLY
        """)
        rows = []
        for row in cursor.fetchall():
            rows.append([str(v) if v is not None else "" for v in row])

        conn.close()
        return jsonify({
            "columns": col_names, "rows": rows, "total": total,
            "page": page, "pages": total_pages, "per_page": per_page
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/truncate-cdc", methods=["POST"])
def truncate_cdc():
    """Truncate a CDC table (inbox, outbox, inbox_errors)"""
    data = request.json
    db_key = data.get("db_key")
    table = data.get("table")  # 'cdc_inbox', 'cdc_outbox', 'cdc_inbox_errors'

    allowed_tables = ['cdc_inbox', 'cdc_outbox', 'cdc_inbox_errors']
    if db_key not in DATABASES:
        return jsonify({"error": "DB inválida"}), 400
    if table not in allowed_tables:
        return jsonify({"error": "Tabla no permitida"}), 400

    try:
        conn = get_connection(db_key)
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM dbo.[{table}]")
        before = cursor.fetchone()[0]

        try:
            cursor.execute(f"TRUNCATE TABLE dbo.[{table}]")
            conn.commit()
            method = "TRUNCATE"
        except Exception:
            conn.rollback()
            cursor.execute(f"DELETE FROM dbo.[{table}]")
            conn.commit()
            method = "DELETE"

        conn.close()
        return jsonify({"table": f"{db_key}.dbo.{table}", "deleted": before, "method": method})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def find_staging_table(dest_db_key, source_schema, source_table):
    """Busca la tabla staging destino correspondiente a la tabla fuente.
    Ej: INMUEBLES.CIUDADELATYPE en newcore → INMUEBLES.Ciudadela_Staging en legacy"""
    try:
        conn = get_connection(dest_db_key)
        cursor = conn.cursor()

        # Quitar sufijo TYPE del nombre
        base_name = source_table
        if base_name.upper().endswith("TYPE"):
            base_name = base_name[:-4]

        # Buscar tabla _Staging en el mismo schema (case-insensitive)
        cursor.execute("""
            SELECT s.name AS sn, t.name AS tn
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name LIKE '%_Staging'
              AND LOWER(s.name) = LOWER(?)
              AND LOWER(REPLACE(t.name, '_Staging', '')) = LOWER(?)
        """, source_schema, base_name)
        row = cursor.fetchone()
        conn.close()

        if row:
            return row.sn, row.tn
        return None, None
    except:
        return None, None


# Estado global del monitor (por simplicidad, un dict en memoria)
pipeline_monitors = {}


@app.route("/api/pipeline-start", methods=["POST"])
def pipeline_start():
    """Inicia el monitoreo de pipeline después de un insert"""
    data = request.json
    direction = data.get("direction", "newcore_to_legacy")
    schema = data.get("schema")
    table = data.get("table")
    quantity = int(data.get("quantity", 0))

    source_db = "newcore" if direction == "newcore_to_legacy" else "legacy"
    dest_db = "legacy" if direction == "newcore_to_legacy" else "newcore"

    # Buscar tabla staging destino
    dest_schema, dest_table = find_staging_table(dest_db, schema, table)

    # Contar estado actual en destino
    dest_count_before = 0
    if dest_schema and dest_table:
        try:
            conn = get_connection(dest_db)
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM [{dest_schema}].[{dest_table}]")
            dest_count_before = cursor.fetchone()[0]
            conn.close()
        except:
            pass

    # Contar outbox actual en source
    source_outbox_before = 0
    try:
        conn = get_connection(source_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
        source_outbox_before = cursor.fetchone()[0]
        conn.close()
    except:
        pass

    # Contar inbox actual en dest
    dest_inbox_before = 0
    dest_inbox_processed_before = 0
    dest_errors_before = 0
    try:
        conn = get_connection(dest_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
        dest_inbox_before = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed = 1")
        dest_inbox_processed_before = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
        dest_errors_before = cursor.fetchone()[0]
        conn.close()
    except:
        pass

    monitor_id = f"{direction}_{schema}_{table}_{int(time.time())}"
    pipeline_monitors[monitor_id] = {
        "direction": direction,
        "source_db": source_db,
        "dest_db": dest_db,
        "source_table": f"{schema}.{table}",
        "dest_table": f"{dest_schema}.{dest_table}" if dest_schema else "No encontrada",
        "quantity": quantity,
        "start_time": time.time(),
        "source_outbox_before": source_outbox_before,
        "dest_inbox_before": dest_inbox_before,
        "dest_inbox_processed_before": dest_inbox_processed_before,
        "dest_errors_before": dest_errors_before,
        "dest_count_before": dest_count_before,
        "dest_schema": dest_schema,
        "dest_table_name": dest_table,
        "completed": False,
        "completed_at": None
    }

    return jsonify({"monitor_id": monitor_id, "dest_table": f"{dest_schema}.{dest_table}" if dest_schema else None})


@app.route("/api/pipeline-poll/<monitor_id>")
def pipeline_poll(monitor_id):
    """Consulta el estado actual del pipeline"""
    mon = pipeline_monitors.get(monitor_id)
    if not mon:
        return jsonify({"error": "Monitor no encontrado"}), 404

    elapsed = round(time.time() - mon["start_time"], 1)
    result = {
        "elapsed_sec": elapsed,
        "source_table": mon["source_table"],
        "dest_table": mon["dest_table"],
        "quantity": mon["quantity"],
    }

    try:
        # Source outbox
        conn_src = get_connection(mon["source_db"])
        cur_src = conn_src.cursor()
        cur_src.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
        outbox_total = cur_src.fetchone()[0]
        result["outbox_new"] = outbox_total - mon["source_outbox_before"]
        conn_src.close()
    except:
        result["outbox_new"] = "N/A"

    try:
        # Dest inbox
        conn_dst = get_connection(mon["dest_db"])
        cur_dst = conn_dst.cursor()

        cur_dst.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
        inbox_total = cur_dst.fetchone()[0]
        result["inbox_received"] = inbox_total - mon["dest_inbox_before"]

        cur_dst.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed = 1")
        inbox_proc = cur_dst.fetchone()[0]
        result["inbox_processed"] = inbox_proc - mon["dest_inbox_processed_before"]

        cur_dst.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed = 0")
        result["inbox_pending"] = cur_dst.fetchone()[0]

        cur_dst.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
        errors_total = cur_dst.fetchone()[0]
        result["inbox_errors"] = errors_total - mon["dest_errors_before"]

        # Dest staging table count
        if mon["dest_schema"] and mon["dest_table_name"]:
            cur_dst.execute(f"SELECT COUNT(*) FROM [{mon['dest_schema']}].[{mon['dest_table_name']}]")
            dest_count = cur_dst.fetchone()[0]
            result["dest_count"] = dest_count
            result["dest_new"] = dest_count - mon["dest_count_before"]

            # Check if completed
            if result["dest_new"] >= mon["quantity"] and not mon["completed"]:
                mon["completed"] = True
                mon["completed_at"] = elapsed
                result["completed_at"] = elapsed
            elif mon["completed"]:
                result["completed_at"] = mon["completed_at"]
        else:
            result["dest_count"] = "N/A"
            result["dest_new"] = "N/A"

        conn_dst.close()
    except Exception as e:
        result["dest_error"] = str(e)[:200]

    # Progress percentage
    try:
        if isinstance(result.get("dest_new"), int) and mon["quantity"] > 0:
            result["progress_pct"] = min(100, round(result["dest_new"] / mon["quantity"] * 100, 1))
        else:
            result["progress_pct"] = 0
    except:
        result["progress_pct"] = 0

    result["completed"] = mon["completed"]

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, port=5050)
