"""
Rutas para la direccion Legacy -> Newcore.
Responsable: [NOMBRE OTRA PERSONA]

Inserta en tablas originales de las BDs legacy (dbIM, dbCR, dbFC, etc.).
Los triggers generan outbox en fcme_legacy -> Kafka -> inbox newcore -> SP -> tabla TYPE.
"""
import time
from flask import Blueprint, request, jsonify
from config import get_connection
from db import get_table_columns, get_pk_columns, sync_outbox_identity, get_max_id_across_destinations
from data_generator import generate_fake_value, get_max_numeric_id

legacy_bp = Blueprint('legacy_to_newcore', __name__)


# ── Configuracion de bases originales y sus tablas CDC ──
# Cada base de datos original con las tablas que tienen triggers outbox.
# Para agregar mas tablas, solo agrega a la lista correspondiente.

ORIGINAL_DBS = {
    "dbIM": [
        "imtbbene_entr",
        "imtbbene_firm",
        "imtbcben",
        "imtbcpro",
        "imtbdivi",
        "imtbmanz",
        "imtbmejo_tram",
        "imtbtviv",
    ],
    "dbFC": [
        "fctbafil_actu",
        "fctbafil_info_adic",
        "fctbagen_telf",
        "fctbmvto_pend_cbro",
        "fctbotro_ingr_afil",
        "fctbsald_diar_afil_rubr",
        "sfct_afiliado",
        "sfct_afiliado_auditor",
        "sfct_beneficiario",
        "sfct_cabecera_rol",
        "sfct_estados_afiliado",
        "sfct_movimiento",
    ],
    "dbCG": [
        "cgtbcasi",
        "cgtbcaut_cpla",
        "cgtbcaut_dpla",
        "cgtbcfac",
        "cgtbcier_proc",
        "cgtbcncl",
        "cgtbcncl_deta",
        "cgtbconc",
        "cgtbdasi",
        "cgtbdcto",
        "cgtbdfac_nota_cred",
        "cgtbdfac_prod",
        "cgtbdfac_rete",
        "cgtbfact_dbso",
        "cgtbgara_hipo_cdio",
        "cgtbgara_vehi_cdio",
        "cgtbplcn",
        "cgtbprod",
        "cgtbprvd",
        "cgtbrete",
    ],
    "dbCR": [
        "crtbcobr_judi_dist",
        "crtbgara_real",
        "crtbgest_cart_asig",
        "crtbgest_deta_cbnz",
        "crtbmedi_cobr",
        "crtboper_segu",
        "crtoblig",
        "crtplpag",
        "crtrecup",
        "crtsolid",
    ],
    "dbIN": [
        "intbcabe_dbso_inve",
        "intbcinv",
        "intbdcto_cble",
        "intbdinv",
        "intbdpag",
        "intbdpre_inve",
        "intbemis",
        "intbgara",
        "intbprec_diar",
        "intbvinv",
        "intbvinv_audi",
    ],
}


# ── Modulos y tablas ──

@legacy_bp.route("/api/legacy/modules")
def get_modules():
    """Retorna bases originales (dbIM, dbCR, etc.) con sus tablas CDC"""
    schemas = {}
    for db_name, tables in ORIGINAL_DBS.items():
        schemas[db_name] = sorted(tables)
    return jsonify(schemas)


# ── Columnas ──

@legacy_bp.route("/api/legacy/columns", methods=["POST"])
def get_columns():
    """Retorna columnas de una tabla de BD original (dbIM, dbCR, etc.)"""
    data = request.json
    db_key = data.get("schema")  # ej: "dbIM"
    table = data.get("table")

    try:
        columns = get_table_columns(db_key, "dbo", table)
        return jsonify(columns)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Test de insert ──

@legacy_bp.route("/api/legacy/test", methods=["POST"])
def run_test():
    """Ejecuta insert masivo en una tabla original legacy"""
    data = request.json
    db_key = data.get("schema")  # ej: "dbIM"
    table = data.get("table")
    quantity = int(data.get("quantity", 100))
    disable_triggers = data.get("disable_triggers", False)

    batch_size = min(1000, quantity)

    try:
        columns = get_table_columns(db_key, "dbo", table)
        insertable_cols = [c for c in columns if not c["is_identity"]]

        col_names = ", ".join([f"[{c['name']}]" for c in insertable_cols])
        placeholders = ", ".join(["?" for _ in insertable_cols])
        sql = f"INSERT INTO [dbo].[{table}] ({col_names}) VALUES ({placeholders})"

        conn = get_connection(db_key)
        cursor = conn.cursor()

        # Detectar PK real, fallback a primera col numerica
        pk_cols = get_pk_columns(conn, "dbo", table)
        first_id_col = None
        if pk_cols:
            first_id_col = next((c for c in insertable_cols if c["name"] == pk_cols[0]), None)
        if not first_id_col:
            first_id_col = next((c for c in insertable_cols if c["name"].lower().endswith("id")), None)
        if not first_id_col:
            first_id_col = next((c for c in insertable_cols if c["type"].lower() in ("int", "bigint", "smallint")), None)
            if first_id_col:
                pk_cols = [first_id_col["name"]]

        offset = 0
        if first_id_col:
            offset = get_max_numeric_id(conn, "dbo", table, first_id_col["name"])
            # Verificar max en tablas destino de newcore para evitar colisiones
            dest_max = get_max_id_across_destinations(db_key, table)
            offset = max(offset, dest_max) + 1

        # Sincronizar identity del outbox con Kafka e insertar warmup
        sync_outbox_identity("legacy")
        _insert_outbox_warmup()

        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{table}]")
        count_before = cursor.fetchone()[0]

        # Outbox de legacy (donde los triggers de dbXX escriben)
        outbox_before = 0
        try:
            conn_lg = get_connection("legacy")
            cur_lg = conn_lg.cursor()
            cur_lg.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
            outbox_before = cur_lg.fetchone()[0]
            conn_lg.close()
        except:
            pass

        if disable_triggers:
            try:
                cursor.execute(f"DISABLE TRIGGER ALL ON [dbo].[{table}]")
                conn.commit()
            except:
                pass

        metrics = {
            "direction": "legacy_to_newcore",
            "schema": db_key, "table": table,
            "full_table": f"{db_key}.dbo.{table}",
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

            pk_names = set(pk_cols) if pk_cols else set()
            rows = []
            for i in range(current_batch):
                row = [generate_fake_value(col, inserted + i + 1, offset, is_pk=col["name"] in pk_names) for col in insertable_cols]
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
                cursor.execute(f"ENABLE TRIGGER ALL ON [dbo].[{table}]")
                conn.commit()
            except:
                pass

        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{table}]")
        count_after = cursor.fetchone()[0]

        outbox_after = 0
        try:
            conn_lg = get_connection("legacy")
            cur_lg = conn_lg.cursor()
            cur_lg.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
            outbox_after = cur_lg.fetchone()[0]
            conn_lg.close()
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

@legacy_bp.route("/api/legacy/cleanup", methods=["POST"])
def cleanup():
    data = request.json
    db_key = data.get("schema")
    table = data.get("table")

    try:
        conn = get_connection(db_key)
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{table}]")
        before = cursor.fetchone()[0]

        try:
            cursor.execute(f"DISABLE TRIGGER ALL ON [dbo].[{table}]")
            conn.commit()
        except:
            pass

        cursor.execute(f"DELETE FROM [dbo].[{table}]")
        deleted = cursor.rowcount
        conn.commit()

        try:
            cursor.execute(f"ENABLE TRIGGER ALL ON [dbo].[{table}]")
            conn.commit()
        except:
            pass

        conn.close()
        return jsonify({"table": f"dbo.{table}", "deleted": deleted, "before": before})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@legacy_bp.route("/api/legacy/truncate", methods=["POST"])
def truncate_table():
    data = request.json
    db_key = data.get("schema")
    table = data.get("table")

    try:
        conn = get_connection(db_key)
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[{table}]")
        before = cursor.fetchone()[0]

        try:
            cursor.execute(f"DISABLE TRIGGER ALL ON [dbo].[{table}]")
            conn.commit()
        except:
            pass

        try:
            cursor.execute(f"TRUNCATE TABLE [dbo].[{table}]")
            conn.commit()
            method = "TRUNCATE"
        except Exception:
            conn.rollback()
            cursor.execute(f"DELETE FROM [dbo].[{table}]")
            conn.commit()
            method = "DELETE (TRUNCATE no permitido por FK)"

        try:
            cursor.execute(f"ENABLE TRIGGER ALL ON [dbo].[{table}]")
            conn.commit()
        except:
            pass

        conn.close()
        return jsonify({"table": f"dbo.{table}", "deleted": before, "method": method})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Pipeline monitor ──

pipeline_monitors = {}


@legacy_bp.route("/api/legacy/pipeline-start", methods=["POST"])
def pipeline_start():
    data = request.json
    db_key = data.get("schema")  # ej: "dbIM"
    table = data.get("table")
    quantity = int(data.get("quantity", 0))

    # Buscar tabla TYPE destino en newcore via aggregate_type en outbox
    dest_schema, dest_table = _find_newcore_dest(table)

    dest_count_before = 0
    if dest_schema and dest_table:
        try:
            conn = get_connection("newcore")
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM [{dest_schema}].[{dest_table}]")
            dest_count_before = cursor.fetchone()[0]
            conn.close()
        except:
            pass

    # Outbox en fcme_legacy (donde los triggers de dbXX escriben)
    source_outbox_before = 0
    try:
        conn = get_connection("legacy")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
        source_outbox_before = cursor.fetchone()[0]
        conn.close()
    except:
        pass

    dest_inbox_before = dest_inbox_processed_before = dest_errors_before = 0
    try:
        conn = get_connection("newcore")
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

    monitor_id = f"l2n_{db_key}_{table}_{int(time.time())}"
    pipeline_monitors[monitor_id] = {
        "source_table": f"{db_key}.{table}",
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


@legacy_bp.route("/api/legacy/pipeline-poll/<monitor_id>")
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
        conn_src = get_connection("legacy")
        cur_src = conn_src.cursor()
        cur_src.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
        result["outbox_new"] = cur_src.fetchone()[0] - mon["source_outbox_before"]
        conn_src.close()
    except:
        result["outbox_new"] = "N/A"

    try:
        conn_dst = get_connection("newcore")
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


def _find_newcore_dest(source_table):
    """Busca tabla TYPE en newcore via aggregate_type en outbox o trigger"""
    try:
        import re
        agg_type = None

        # Primero buscar en outbox
        try:
            conn_outbox = get_connection("legacy")
            cur = conn_outbox.cursor()
            cur.execute("""
                SELECT TOP 1 aggregate_type FROM dbo.cdc_outbox
                WHERE source_table = ? AND aggregate_type != '_warmup'
            """, source_table)
            row = cur.fetchone()
            conn_outbox.close()
            if row:
                agg_type = row.aggregate_type
        except:
            pass

        # Si outbox vacío, parsear el trigger para encontrar aggregate_types
        if not agg_type:
            try:
                # Detectar la BD original del source_table por prefijo
                prefix = source_table[:2]
                db_map = {db: db for db in ORIGINAL_DBS}
                source_db = None
                for db_name in ORIGINAL_DBS:
                    for tbl in ORIGINAL_DBS[db_name]:
                        if tbl == source_table:
                            source_db = db_name
                            break
                    if source_db:
                        break

                if source_db:
                    conn_src = get_connection(source_db)
                    cur_src = conn_src.cursor()
                    cur_src.execute("""
                        SELECT OBJECT_DEFINITION(t.object_id)
                        FROM sys.triggers t
                        WHERE t.parent_id = OBJECT_ID('dbo.' + ?)
                          AND t.name LIKE '%outbox%'
                    """, source_table)
                    row = cur_src.fetchone()
                    conn_src.close()
                    if row and row[0]:
                        matches = re.findall(r"'(\w+Type)'", row[0], re.IGNORECASE)
                        # Tomar el primer aggregate_type que no sea genérico
                        if matches:
                            agg_type = matches[0]
            except:
                pass

        if not agg_type:
            return None, None

        conn_nc = get_connection("newcore")
        cur_nc = conn_nc.cursor()
        cur_nc.execute("""
            SELECT s.name AS sn, t.name AS tn
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE LOWER(t.name) = LOWER(?)
        """, agg_type)
        row_nc = cur_nc.fetchone()
        conn_nc.close()
        if row_nc:
            return row_nc.sn, row_nc.tn
        return None, None
    except:
        return None, None


def _insert_outbox_warmup():
    """Inserta un registro dummy en el outbox para que el sink de Kafka lo consuma primero.
    El JDBC sink siempre pierde el primer mensaje de cada batch al DLQ.
    Este dummy absorbe esa perdida, protegiendo los datos reales."""
    try:
        conn = get_connection("legacy")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table)
            VALUES ('_warmup', '_warmup', 'WARMUP', '{}', '_warmup')
        """)
        conn.commit()
        conn.close()
    except:
        pass
