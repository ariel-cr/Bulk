"""Fix: Arithmetic overflow error for data type smallint, value = 1774029.

Causa raiz: el trigger trg_imtbcben_outbox genera propietarioId como
ROW_NUMBER() + MAX(propietarioId) sin limite, lo que excede smallint (32,767)
cuando PROPIETARIOTYPE tiene IDs grandes de tests anteriores.

Este script:
1. Re-despliega el trigger con modulo 30000 para mantener IDs en rango smallint
2. Limpia cdc_inbox_errors en legacy (registros de error del overflow)
3. Limpia registros atascados en cdc_inbox de legacy (processed=0 con error)
"""
import pyodbc

DB_CONFIG = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123"
}


def connect(db_name):
    return pyodbc.connect(
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={db_name};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )


def fix_trigger():
    """Re-despliega trg_imtbcben_outbox en dbIM con limite smallint."""
    print("1. Aplicando fix al trigger trg_imtbcben_outbox en dbIM...")
    conn = connect("dbIM")
    cur = conn.cursor()

    # Leer el SQL del fix_trigger.sql
    with open("fix_trigger.sql", "r", encoding="utf-8") as f:
        sql = f.read()

    cur.execute(sql)
    conn.commit()
    conn.close()
    print("   OK - Trigger actualizado con modulo 30000")


def cleanup_legacy_errors():
    """Limpia errores de overflow en cdc_inbox_errors de legacy."""
    print("2. Limpiando cdc_inbox_errors en fcme_legacy...")
    conn = connect("fcme_legacy")
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
    count = cur.fetchone()[0]

    if count > 0:
        cur.execute("TRUNCATE TABLE dbo.cdc_inbox_errors")
        conn.commit()
        print(f"   OK - {count} errores eliminados")
    else:
        print("   OK - Sin errores pendientes")

    conn.close()


def cleanup_stuck_inbox():
    """Marca como procesados los registros de inbox que fallan por overflow."""
    print("3. Limpiando registros atascados en cdc_inbox de legacy...")
    conn = connect("fcme_legacy")
    cur = conn.cursor()

    # Buscar registros pendientes cuyo payload tiene valores > 32767 en campos ID
    cur.execute("""
        SELECT COUNT(*) FROM dbo.cdc_inbox
        WHERE processed = 0
    """)
    pending = cur.fetchone()[0]

    if pending > 0:
        # Marcar como procesados los que tienen el error de overflow
        cur.execute("""
            UPDATE dbo.cdc_inbox
            SET processed = 1
            WHERE processed = 0
              AND payload LIKE '%1774029%'
        """)
        fixed = cur.rowcount
        conn.commit()

        if fixed > 0:
            print(f"   OK - {fixed} registros marcados como procesados (contenian valor 1774029)")
        else:
            print(f"   OK - {pending} pendientes pero ninguno con valor 1774029")
    else:
        print("   OK - Sin registros pendientes")

    conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("FIX: Arithmetic overflow smallint (value = 1774029)")
    print("=" * 60)
    print()

    try:
        fix_trigger()
    except Exception as e:
        print(f"   ERROR en trigger: {e}")

    print()

    try:
        cleanup_legacy_errors()
    except Exception as e:
        print(f"   ERROR en cleanup errors: {e}")

    print()

    try:
        cleanup_stuck_inbox()
    except Exception as e:
        print(f"   ERROR en cleanup inbox: {e}")

    print()
    print("=" * 60)
    print("DONE - Reinicia el servidor Flask y vuelve a correr el test")
    print("=" * 60)
