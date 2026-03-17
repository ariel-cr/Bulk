"""Utilidades compartidas para consultas a la base de datos."""
import re
import json
import urllib.request
from config import get_connection


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


def get_pk_columns(conn, schema, table):
    """Obtiene las columnas de la PK de una tabla"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT col.name
        FROM sys.indexes ix
        JOIN sys.index_columns ic ON ix.object_id = ic.object_id AND ix.index_id = ic.index_id
        JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
        WHERE ix.is_primary_key = 1
          AND ix.object_id = OBJECT_ID(? + '.' + ?)
        ORDER BY ic.key_ordinal
    """, schema, table)
    return [row.name for row in cursor.fetchall()]


def get_kafka_outbox_offset(db_key):
    """Consulta el offset de Kafka para el conector SOURCE de un outbox"""
    connector_map = {
        "legacy": "legacy-convivencia-cdc-outbox-jdbc-source",
        "newcore": "newcore-convivencia-cdc-outbox-jdbc-source",
    }
    connector = connector_map.get(db_key)
    if not connector:
        return None
    try:
        url = f"http://10.35.3.64:8083/connectors/{connector}/offsets"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
            return data["offsets"][0]["offset"]["incrementing"]
    except:
        return None


def sync_outbox_identity(outbox_db):
    """Limpia outbox huerfanos y reseedea identity para sincronizar con Kafka"""
    try:
        kafka_offset = get_kafka_outbox_offset(outbox_db)
        if not kafka_offset:
            return
        conn = get_connection(outbox_db)
        cursor = conn.cursor()
        # Limpiar registros que Kafka ya proceso
        cursor.execute(f"DELETE FROM dbo.cdc_outbox WHERE id <= {int(kafka_offset)}")
        conn.commit()
        # Reseedear para que el proximo registro sea offset + 1
        cursor.execute("SELECT IDENT_CURRENT('dbo.cdc_outbox')")
        current_ident = int(cursor.fetchone()[0])
        if current_ident <= kafka_offset:
            cursor.execute(f"DBCC CHECKIDENT ('dbo.cdc_outbox', RESEED, {int(kafka_offset)})")
            conn.commit()
        conn.close()
    except:
        pass


def get_max_id_across_destinations(source_db_key, source_table):
    """Busca el max ID numerico en las tablas destino de newcore
    para evitar colisiones de PK cuando el trigger replica datos"""
    max_id = 0
    try:
        conn = get_connection("newcore")
        cursor = conn.cursor()

        # Buscar aggregate_types: primero en outbox, luego en trigger
        agg_types = []
        try:
            conn_outbox = get_connection("legacy")
            cur_outbox = conn_outbox.cursor()
            cur_outbox.execute(
                "SELECT DISTINCT aggregate_type FROM dbo.cdc_outbox WHERE source_table = ?",
                source_table)
            agg_types = [r.aggregate_type for r in cur_outbox.fetchall()]
            conn_outbox.close()
        except:
            pass

        # Si outbox vacio, parsear el trigger del source
        if not agg_types:
            try:
                conn_src = get_connection(source_db_key)
                cur_src = conn_src.cursor()
                cur_src.execute("""
                    SELECT OBJECT_DEFINITION(t.object_id)
                    FROM sys.triggers t
                    WHERE t.parent_id = OBJECT_ID('dbo.' + ?)
                      AND t.name LIKE '%outbox%'
                """, source_table)
                row = cur_src.fetchone()
                if row and row[0]:
                    matches = re.findall(r"'(\w+Type)'", row[0], re.IGNORECASE)
                    agg_types = list(set(matches))
                conn_src.close()
            except:
                pass

        # Para cada aggregate_type, buscar tabla TYPE en newcore y su max PK
        for agg_type in agg_types:
            cursor.execute("""
                SELECT s.name AS sn, t.name AS tn
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE LOWER(t.name) = LOWER(?)
            """, agg_type)
            row = cursor.fetchone()
            if row:
                dest_pks = get_pk_columns(cursor.connection, row.sn, row.tn)
                for pk_col in dest_pks:
                    try:
                        cursor.execute(f"SELECT ISNULL(MAX(TRY_CAST([{pk_col}] AS BIGINT)), 0) FROM [{row.sn}].[{row.tn}]")
                        val = cursor.fetchone()[0]
                        if val and int(val) > max_id:
                            max_id = int(val)
                    except:
                        pass
        conn.close()
    except:
        pass
    return max_id
