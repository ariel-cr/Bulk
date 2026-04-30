"""Inspecciona Oracle: triggers, SPs y configuracion del inbox."""
import oracledb
orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!",
                        dsn="10.35.3.223:31521/XEPDB1")
c = orcl.cursor()

print("== Triggers sobre CDC_INBOX ==")
c.execute("""
  SELECT trigger_name, trigger_type, triggering_event, status
  FROM all_triggers
  WHERE owner='FCME_USER' AND table_name='CDC_INBOX'
""")
for r in c.fetchall(): print(f"  {r[0]}  type={r[1]} event={r[2]} status={r[3]}")

print("\n== Tablas de configuracion ==")
c.execute("""
  SELECT table_name FROM all_tables
  WHERE owner='FCME_USER' AND (table_name LIKE 'CDC_%' OR table_name LIKE '%MODULE_CONFIG%')
  ORDER BY table_name
""")
for r in c.fetchall(): print(f"  {r[0]}")

print("\n== cdc_inbox_module_config (contenido) ==")
try:
    c.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG")
    print(f"  total filas: {c.fetchone()[0]}")
    c.execute("SELECT * FROM FCME_USER.CDC_INBOX_MODULE_CONFIG FETCH FIRST 20 ROWS ONLY")
    cols = [d[0] for d in c.description]
    print("  " + " | ".join(cols))
    for r in c.fetchall(): print("  " + " | ".join(str(v)[:40] for v in r))
except Exception as e:
    print(f"  err: {e}")

print("\n== SPs usp_inbox_* ==")
c.execute("""
  SELECT object_name, object_type, status FROM all_objects
  WHERE owner='FCME_USER' AND (object_name LIKE 'USP_INBOX%' OR object_name LIKE 'SP_INBOX%' OR object_name LIKE '%TRG%INBOX%' OR object_name LIKE '%PROCESS_CDC%')
  ORDER BY object_name
""")
for r in c.fetchall(): print(f"  {r[0]:<50} {r[1]:<15} {r[2]}")

print("\n== Eventos no procesados en CDC_INBOX (ultimos 5) ==")
c.execute("""
  SELECT * FROM (
    SELECT ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PROCESSED, CREATED_AT
    FROM FCME_USER.CDC_INBOX WHERE PROCESSED = 0 ORDER BY ID DESC
  ) WHERE ROWNUM <= 5
""")
for r in c.fetchall():
    print(f"  id={r[0]} type={r[1]} agg={r[2]} op={r[3]} processed={r[4]} created={r[5]}")
