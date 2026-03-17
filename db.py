"""Utilidades compartidas para consultas a la base de datos."""
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
