"""Repuebla CDC_INBOX_MODULE_CONFIG con los aggregate_type canonicos
(de cdc_table_to_types.aggregate_type_emit) que ahora emiten los triggers,
y reprocesa los eventos pendientes en CDC_INBOX.
"""
import pyodbc, oracledb, time

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# 1) Sacar todos los aggregate_type canonicos activos
c = sql("fcme_canonicos").cursor()
c.execute("""
  SELECT DISTINCT aggregate_type_emit
  FROM dbo.cdc_table_to_types
  WHERE is_active=1 AND aggregate_type_emit IS NOT NULL
""")
canon_types = sorted({r.aggregate_type_emit for r in c.fetchall()})
print(f"Types canonicos activos: {len(canon_types)}")
for t in canon_types: print(f"  {t}")

# 2) Repoblar CDC_INBOX_MODULE_CONFIG
print("\n[1] Repoblando CDC_INBOX_MODULE_CONFIG")
co.execute("DELETE FROM CDC_INBOX_MODULE_CONFIG")
print(f"  borrados: {co.rowcount}")
for t in canon_types:
    co.execute("""INSERT INTO CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE)
                  VALUES (:1, 'USP_INBOX_PARTICIPES', 1)""", [t])
orcl.commit()
co.execute("SELECT COUNT(*) FROM CDC_INBOX_MODULE_CONFIG")
print(f"  filas insertadas: {co.fetchone()[0]}")

# 3) Reprocesar eventos pendientes en CDC_INBOX (PROCESSED=0)
print("\n[2] Reprocesando eventos pendientes")
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=0")
pending = co.fetchone()[0]
print(f"  pendientes antes: {pending}")

# Re-disparar el compound trigger via UPDATE no-op (CREATED_AT)
co.execute("""
  UPDATE FCME_USER.CDC_INBOX
  SET CREATED_AT = CREATED_AT
  WHERE PROCESSED = 0
""")
orcl.commit()
print(f"  filas re-tocadas: {co.rowcount}")

time.sleep(2)
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=0")
print(f"  pendientes despues: {co.fetchone()[0]}")
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=1")
print(f"  procesados: {co.fetchone()[0]}")
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
print(f"  errores: {co.fetchone()[0]}")

orcl.close()
