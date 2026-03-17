"""Rutas compartidas: index, status CDC, visor de datos CDC, truncate CDC."""
from flask import Blueprint, render_template, request, jsonify
import time as _time
import urllib.request as _urlreq
from config import DATABASES, get_connection
from db import get_kafka_outbox_offset

common_bp = Blueprint('common', __name__)


@common_bp.route("/")
def index():
    return render_template("index.html")


@common_bp.route("/api/status")
def get_status():
    """Estado de las colas CDC - optimizado con NOLOCK y query unica"""
    status = {}
    try:
        for db_key, db_name in DATABASES.items():
            conn = get_connection(db_key)
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT
                        (SELECT COUNT_BIG(*) FROM dbo.cdc_outbox WITH (NOLOCK) WHERE aggregate_type != '_warmup') AS outbox,
                        (SELECT COUNT_BIG(*) FROM dbo.cdc_inbox WITH (NOLOCK) WHERE processed = 0) AS inbox_pending,
                        (SELECT COUNT_BIG(*) FROM dbo.cdc_inbox WITH (NOLOCK) WHERE processed = 1) AS inbox_processed,
                        (SELECT COUNT_BIG(*) FROM dbo.cdc_inbox_errors WITH (NOLOCK)) AS errors
                """)
                row = cursor.fetchone()
                status[f"{db_key}_outbox"] = row[0]
                status[f"{db_key}_inbox_pending"] = row[1]
                status[f"{db_key}_inbox_processed"] = row[2]
                status[f"{db_key}_errors"] = row[3]
            except:
                status[f"{db_key}_outbox"] = "N/A"
                status[f"{db_key}_inbox_pending"] = "N/A"
                status[f"{db_key}_inbox_processed"] = "N/A"
                status[f"{db_key}_errors"] = "N/A"
            conn.close()
    except Exception as e:
        status["error"] = str(e)

    return jsonify(status)


@common_bp.route("/api/cdc-pending-summary/<db_key>")
def get_pending_summary(db_key):
    """Diagnostico de registros pendientes en cdc_inbox"""
    if db_key not in DATABASES:
        return jsonify({"error": "DB invalida"}), 400

    try:
        conn = get_connection(db_key)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed = 0")
        total = cursor.fetchone()[0]

        if total == 0:
            conn.close()
            return jsonify({"total": 0, "groups": [], "oldest": None, "newest": None})

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
            "total": total, "groups": groups,
            "oldest": oldest, "newest": newest,
            "minutes_stuck": minutes_stuck
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@common_bp.route("/api/cdc-data/<db_key>/<table_name>")
def get_cdc_data(db_key, table_name):
    """Retorna datos de cualquier tabla CDC con paginacion"""
    allowed = ['cdc_inbox', 'cdc_outbox', 'cdc_inbox_errors']
    if db_key not in DATABASES:
        return jsonify({"error": "DB invalida"}), 400
    if table_name not in allowed:
        return jsonify({"error": "Tabla no permitida"}), 400

    filter_type = request.args.get("filter")
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


@common_bp.route("/api/truncate-cdc", methods=["POST"])
def truncate_cdc():
    """Truncate a CDC table (inbox, outbox, inbox_errors)"""
    data = request.json
    db_key = data.get("db_key")
    table = data.get("table")

    allowed_tables = ['cdc_inbox', 'cdc_outbox', 'cdc_inbox_errors']
    if db_key not in DATABASES:
        return jsonify({"error": "DB invalida"}), 400
    if table not in allowed_tables:
        return jsonify({"error": "Tabla no permitida"}), 400

    try:
        conn = get_connection(db_key)
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM dbo.[{table}]")
        before = cursor.fetchone()[0]

        # Si es outbox, obtener offset de Kafka ANTES de truncar
        kafka_offset = None
        if table == "cdc_outbox":
            kafka_offset = get_kafka_outbox_offset(db_key)

        try:
            cursor.execute(f"TRUNCATE TABLE dbo.[{table}]")
            conn.commit()
            method = "TRUNCATE"
        except Exception:
            conn.rollback()
            cursor.execute(f"DELETE FROM dbo.[{table}]")
            conn.commit()
            method = "DELETE"

        # Reseedear identity del outbox para que Kafka siga leyendo
        if table == "cdc_outbox" and kafka_offset:
            cursor.execute(f"DBCC CHECKIDENT ('dbo.cdc_outbox', RESEED, {int(kafka_offset)})")
            conn.commit()
            method += f" + RESEED identity a {kafka_offset}"

        conn.close()
        return jsonify({"table": f"{db_key}.dbo.{table}", "deleted": before, "method": method})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
