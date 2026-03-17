"""
Rutas para la direccion Newcore -> Legacy.
Responsable: [TU NOMBRE]

Inserta en tablas TYPE de fcme_newcore.
Los triggers generan outbox -> Kafka -> inbox legacy -> SP -> tabla final dbXX.
"""
import time
from flask import Blueprint, request, jsonify
from config import DATABASES, get_connection
from db import get_schemas_and_tables, get_table_columns
from data_generator import generate_fake_value, get_max_numeric_id

newcore_bp = Blueprint('newcore_to_legacy', __name__)


# ── Modulos y tablas ──

@newcore_bp.route("/api/newcore/modules")
def get_modules():
    """Retorna modulos (schemas) y tablas de fcme_newcore"""
    try:
        schemas = get_schemas_and_tables("newcore")
        return jsonify(schemas)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Columnas ──

@newcore_bp.route("/api/newcore/columns", methods=["POST"])
def get_columns():
    """Retorna columnas de una tabla de newcore"""
    data = request.json
    schema = data.get("schema")
    table = data.get("table")

    try:
        columns = get_table_columns("newcore", schema, table)
        return jsonify(columns)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Test de insert ──

@newcore_bp.route("/api/newcore/test", methods=["POST"])
def run_test():
    """Ejecuta insert masivo en una tabla de fcme_newcore"""
    data = request.json
    schema = data.get("schema")
    table = data.get("table")
    quantity = int(data.get("quantity", 100))
    disable_triggers = data.get("disable_triggers", False)

    batch_size = min(1000, quantity)

    try:
        columns = get_table_columns("newcore", schema, table)
        insertable_cols = [c for c in columns if not c["is_identity"]]

        col_names = ", ".join([f"[{c['name']}]" for c in insertable_cols])
        placeholders = ", ".join(["?" for _ in insertable_cols])
        sql = f"INSERT INTO [{schema}].[{table}] ({col_names}) VALUES ({placeholders})"

        conn = get_connection("newcore")
        cursor = conn.cursor()

        first_id_col = next((c for c in insertable_cols if c["name"].lower().endswith("id")), None)
        offset = 0
        if first_id_col:
            offset = get_max_numeric_id(conn, schema, table, first_id_col["name"]) + 1

        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        count_before = cursor.fetchone()[0]

        outbox_before = 0
        try:
            cursor.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
            outbox_before = cursor.fetchone()[0]
        except:
            pass

        if disable_triggers:
            try:
                cursor.execute(f"DISABLE TRIGGER ALL ON [{schema}].[{table}]")
                conn.commit()
            except:
                pass

        metrics = {
            "direction": "newcore_to_legacy",
            "schema": schema, "table": table,
            "full_table": f"{schema}.{table}",
            "quantity": quantity,
            "triggers_disabled": disable_triggers,
            "columns_count": len(insertable_cols),
            "batches": [], "errors": []
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
                row = [generate_fake_value(col, inserted + i + 1, offset) for col in insertable_cols]
                rows.append(tuple(row))

            try:
                cursor.fast_executemany = True
                cursor.executemany(sql, rows)
                conn.commit()
                batch_time = time.time() - batch_start
                metrics["batches"].append({
                    "batch": batch_num, "rows": current_batch,
                    "time_sec": round(batch_time, 3),
                    "rows_per_sec": round(current_batch / batch_time, 1) if batch_time > 0 else 0
                })
                inserted += current_batch
            except Exception as e:
                conn.rollback()
                metrics["errors"].append(f"Batch {batch_num}: {str(e)[:200]}")
                individual_ok = 0
                for row in rows[:10]:
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
                    break
                break

        total_time = time.time() - total_start

        if disable_triggers:
            try:
                cursor.execute(f"ENABLE TRIGGER ALL ON [{schema}].[{table}]")
                conn.commit()
            except:
                pass

        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        count_after = cursor.fetchone()[0]

        outbox_after = 0
        try:
            cursor.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
            outbox_after = cursor.fetchone()[0]
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
            "total_errors": len(metrics["errors"])
        })

        return jsonify(metrics)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Cleanup ──

@newcore_bp.route("/api/newcore/cleanup", methods=["POST"])
def cleanup():
    data = request.json
    schema = data.get("schema")
    table = data.get("table")

    try:
        conn = get_connection("newcore")
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        before = cursor.fetchone()[0]

        try:
            cursor.execute(f"DISABLE TRIGGER ALL ON [{schema}].[{table}]")
            conn.commit()
        except:
            pass

        cursor.execute(f"DELETE FROM [{schema}].[{table}]")
        deleted = cursor.rowcount
        conn.commit()

        try:
            cursor.execute(f"ENABLE TRIGGER ALL ON [{schema}].[{table}]")
            conn.commit()
        except:
            pass

        conn.close()
        return jsonify({"table": f"{schema}.{table}", "deleted": deleted, "before": before})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@newcore_bp.route("/api/newcore/truncate", methods=["POST"])
def truncate_table():
    data = request.json
    schema = data.get("schema")
    table = data.get("table")

    try:
        conn = get_connection("newcore")
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        before = cursor.fetchone()[0]

        try:
            cursor.execute(f"DISABLE TRIGGER ALL ON [{schema}].[{table}]")
            conn.commit()
        except:
            pass

        try:
            cursor.execute(f"TRUNCATE TABLE [{schema}].[{table}]")
            conn.commit()
            method = "TRUNCATE"
        except Exception:
            conn.rollback()
            cursor.execute(f"DELETE FROM [{schema}].[{table}]")
            conn.commit()
            method = "DELETE (TRUNCATE no permitido por FK)"

        try:
            cursor.execute(f"ENABLE TRIGGER ALL ON [{schema}].[{table}]")
            conn.commit()
        except:
            pass

        conn.close()
        return jsonify({"table": f"{schema}.{table}", "deleted": before, "method": method})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Pipeline monitor ──

pipeline_monitors = {}


@newcore_bp.route("/api/newcore/pipeline-start", methods=["POST"])
def pipeline_start():
    data = request.json
    schema = data.get("schema")
    table = data.get("table")
    quantity = int(data.get("quantity", 0))

    # Buscar tabla _Staging destino en legacy
    dest_schema, dest_table = _find_staging_table(schema, table)

    dest_count_before = 0
    if dest_schema and dest_table:
        try:
            conn = get_connection("legacy")
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM [{dest_schema}].[{dest_table}]")
            dest_count_before = cursor.fetchone()[0]
            conn.close()
        except:
            pass

    source_outbox_before = 0
    try:
        conn = get_connection("newcore")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
        source_outbox_before = cursor.fetchone()[0]
        conn.close()
    except:
        pass

    dest_inbox_before = dest_inbox_processed_before = dest_errors_before = 0
    try:
        conn = get_connection("legacy")
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

    monitor_id = f"n2l_{schema}_{table}_{int(time.time())}"
    pipeline_monitors[monitor_id] = {
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
        "completed": False, "completed_at": None
    }

    return jsonify({
        "monitor_id": monitor_id,
        "dest_table": f"{dest_schema}.{dest_table}" if dest_schema else None
    })


@newcore_bp.route("/api/newcore/pipeline-poll/<monitor_id>")
def pipeline_poll(monitor_id):
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
        conn_src = get_connection("newcore")
        cur_src = conn_src.cursor()
        cur_src.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
        result["outbox_new"] = cur_src.fetchone()[0] - mon["source_outbox_before"]
        conn_src.close()
    except:
        result["outbox_new"] = "N/A"

    try:
        conn_dst = get_connection("legacy")
        cur_dst = conn_dst.cursor()

        cur_dst.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
        result["inbox_received"] = cur_dst.fetchone()[0] - mon["dest_inbox_before"]

        cur_dst.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed = 1")
        result["inbox_processed"] = cur_dst.fetchone()[0] - mon["dest_inbox_processed_before"]

        cur_dst.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed = 0")
        result["inbox_pending"] = cur_dst.fetchone()[0]

        cur_dst.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
        result["inbox_errors"] = cur_dst.fetchone()[0] - mon["dest_errors_before"]

        if mon["dest_schema"] and mon["dest_table_name"]:
            cur_dst.execute(f"SELECT COUNT(*) FROM [{mon['dest_schema']}].[{mon['dest_table_name']}]")
            dest_count = cur_dst.fetchone()[0]
            result["dest_count"] = dest_count
            result["dest_new"] = dest_count - mon["dest_count_before"]

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

    try:
        if isinstance(result.get("dest_new"), int) and mon["quantity"] > 0:
            result["progress_pct"] = min(100, round(result["dest_new"] / mon["quantity"] * 100, 1))
        else:
            result["progress_pct"] = 0
    except:
        result["progress_pct"] = 0

    result["completed"] = mon["completed"]
    return jsonify(result)


def _find_staging_table(source_schema, source_table):
    """Busca tabla _Staging en legacy para una tabla TYPE de newcore"""
    try:
        conn = get_connection("legacy")
        cursor = conn.cursor()
        base_name = source_table
        if base_name.upper().endswith("TYPE"):
            base_name = base_name[:-4]
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
