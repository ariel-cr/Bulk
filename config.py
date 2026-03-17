"""Configuración compartida de bases de datos y conexiones."""
import pyodbc

DB_CONFIG = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123"
}

DATABASES = {
    "newcore": "fcme_newcore",
    "legacy": "fcme_legacy",
}


def get_connection(db_key="newcore"):
    db_name = DATABASES.get(db_key, db_key)
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={db_name};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)
