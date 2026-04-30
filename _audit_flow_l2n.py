"""Audita el flujo Legacy -> Newcore (Oracle):
    Tabla Legacy -> Trigger outbox -> cdc_outbox (canonicos) -> Kafka
    -> CDC_INBOX (Oracle XEPDB1) -> TRG_PROCESS_CDC_INBOX -> USP_INBOX_PARTICIPES
    -> Tablas destino (FCME_USER)
"""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

def banner(t):
    print("\n" + "="*70)
    print(t)
    print("="*70)

# ===========================================================
# PASO 1: Triggers de outbox en BDs legacy
# ===========================================================
banner("[1] TRIGGERS de OUTBOX en BDs legacy")
LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
total_trig = 0
for db in LEG_DBS:
    try:
        c = sql(db).cursor()
        c.execute("""
          SELECT COUNT(*) FROM sys.triggers t
          WHERE t.name LIKE '%outbox%' OR t.name LIKE 'trg_%'
        """)
        n = c.fetchone()[0]
        c.execute("""
          SELECT TOP 5 t.name, OBJECT_NAME(t.parent_id) parent, t.is_disabled
          FROM sys.triggers t
          WHERE t.name LIKE '%outbox%' OR t.name LIKE 'trg_%'
          ORDER BY t.name
        """)
        sample = c.fetchall()
        print(f"  {db}: {n} triggers (sample: {[(r.name, r.parent, r.is_disabled) for r in sample[:3]]})")
        total_trig += n
    except Exception as e:
        print(f"  {db}: ERROR {str(e)[:80]}")
print(f"  TOTAL triggers outbox/trg en legacy DBs: {total_trig}")

# ===========================================================
# PASO 2: cdc_outbox en canonicos
# ===========================================================
banner("[2] cdc_outbox en fcme_canonicos")
try:
    c = sql("fcme_canonicos").cursor()
    c.execute("""
      SELECT s.name sch, t.name tbl FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id=s.schema_id
      WHERE t.name='cdc_outbox'
    """)
    rows = c.fetchall()
    print(f"  Tabla cdc_outbox existe: {len(rows)>0}  ({[(r.sch,r.tbl) for r in rows]})")
    if rows:
        c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
        print(f"  Total registros cdc_outbox: {c.fetchone()[0]}")
        c.execute("""
          SELECT TOP 5 id, aggregate_type, source_table, event_type, created_at
          FROM dbo.cdc_outbox ORDER BY id DESC
        """)
        for r in c.fetchall():
            print(f"    id={r.id} type={r.aggregate_type} src={r.source_table} ev={r.event_type} at={r.created_at}")
except Exception as e:
    print(f"  ERROR: {e}")

# ===========================================================
# PASO 2b: cdc_outbox en fcme_legacy (donde realmente escriben los triggers segun el code)
# ===========================================================
banner("[2b] cdc_outbox en fcme_legacy (ubicacion alternativa)")
try:
    c = sql("fcme_legacy").cursor()
    c.execute("""
      SELECT s.name sch, t.name tbl FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id=s.schema_id
      WHERE t.name='cdc_outbox'
    """)
    rows = c.fetchall()
    print(f"  Tabla cdc_outbox existe en fcme_legacy: {len(rows)>0}")
    if rows:
        c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
        print(f"  Total registros: {c.fetchone()[0]}")
        c.execute("""
          SELECT TOP 5 id, aggregate_type, source_table, event_type, created_at
          FROM dbo.cdc_outbox ORDER BY id DESC
        """)
        for r in c.fetchall():
            print(f"    id={r.id} type={r.aggregate_type} src={r.source_table} ev={r.event_type} at={r.created_at}")
except Exception as e:
    print(f"  ERROR: {e}")

# ===========================================================
# PASO 3: Conector Kafka (revision indirecta via state)
# ===========================================================
banner("[3] Kafka (verificacion indirecta - source/sink connectors)")
print("  Kafka connectors no son consultables directamente desde DB.")
print("  Indicador indirecto: registros llegando del outbox al inbox Oracle.")

# ===========================================================
# PASO 4: CDC_INBOX en Oracle
# ===========================================================
banner("[4] CDC_INBOX en Oracle XEPDB1 (FCME_USER)")
try:
    orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
    co = orcl.cursor()
    co.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name='CDC_INBOX'")
    print(f"  CDC_INBOX existe: {co.fetchone()[0]>0}")
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
    print(f"  Total registros: {co.fetchone()[0]}")
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=1")
    pr = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=0")
    pe = co.fetchone()[0]
    print(f"  Procesados: {pr}  Pendientes: {pe}")
    co.execute("""
      SELECT * FROM (
        SELECT ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PROCESSED, CREATED_AT
        FROM FCME_USER.CDC_INBOX ORDER BY ID DESC
      ) WHERE ROWNUM <= 5
    """)
    for r in co.fetchall():
        print(f"    id={r[0]} type={r[1]} agg={r[2]} ev={r[3]} processed={r[4]} created={r[5]}")
except Exception as e:
    print(f"  ERROR Oracle: {e}")
    raise SystemExit(1)

# ===========================================================
# PASO 5: TRG_PROCESS_CDC_INBOX (trigger Oracle)
# ===========================================================
banner("[5] TRG_PROCESS_CDC_INBOX en Oracle")
co.execute("""
  SELECT trigger_name, trigger_type, triggering_event, status
  FROM all_triggers
  WHERE owner='FCME_USER' AND table_name='CDC_INBOX'
""")
trigs = co.fetchall()
print(f"  Triggers en CDC_INBOX: {len(trigs)}")
for r in trigs:
    print(f"    {r[0]:<30} type={r[1]:<20} ev={r[2]:<25} status={r[3]}")

# ===========================================================
# PASO 6: USP_INBOX_PARTICIPES (SP)
# ===========================================================
banner("[6] USP_INBOX_PARTICIPES en Oracle")
co.execute("""
  SELECT object_name, object_type, status
  FROM all_objects
  WHERE owner='FCME_USER'
    AND object_name IN ('USP_INBOX_PARTICIPES','USP_INBOX_PARTICIPE')
""")
for r in co.fetchall():
    print(f"  {r[0]:<30} {r[1]:<10} status={r[2]}")

# ===========================================================
# PASO 7: CDC_INBOX_MODULE_CONFIG y rutas activas
# ===========================================================
banner("[7] CDC_INBOX_MODULE_CONFIG")
try:
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG")
    print(f"  total filas: {co.fetchone()[0]}")
    co.execute("""
      SELECT AGGREGATE_TYPE, SP_NAME, ACTIVE
      FROM FCME_USER.CDC_INBOX_MODULE_CONFIG ORDER BY AGGREGATE_TYPE
      FETCH FIRST 15 ROWS ONLY
    """)
    for r in co.fetchall():
        print(f"    {r[0]:<35} -> {r[1]:<30} active={r[2]}")
except Exception as e:
    print(f"  ERROR: {e}")

# ===========================================================
# PASO 8: Tablas destino en FCME_USER (counts)
# ===========================================================
banner("[8] Tablas destino FCME_USER (con datos)")
co.execute("""
  SELECT table_name FROM all_tables
  WHERE owner='FCME_USER' AND table_name LIKE '%TYPE%'
  ORDER BY table_name
""")
type_tables = [r[0] for r in co.fetchall()]
print(f"  Total tablas _TYPE: {len(type_tables)}")
non_empty = []
for t in type_tables:
    try:
        co.execute(f"SELECT COUNT(*) FROM FCME_USER.{t}")
        n = co.fetchone()[0]
        if n > 0:
            non_empty.append((t, n))
    except:
        pass
print(f"  Con datos: {len(non_empty)}")
for t, n in non_empty[:20]:
    print(f"    {t:<45} {n}")

# ===========================================================
# PASO 9: Errores y registros sin procesar
# ===========================================================
banner("[9] CDC_INBOX_ERRORS")
try:
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
    print(f"  total errores: {co.fetchone()[0]}")
    co.execute("""
      SELECT * FROM (
        SELECT INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE
        FROM FCME_USER.CDC_INBOX_ERRORS ORDER BY INBOX_ID DESC
      ) WHERE ROWNUM <= 5
    """)
    for r in co.fetchall():
        print(f"    inbox_id={r[0]} type={r[1]} ev={r[2]} err={(r[3] or '')[:120]}")
except Exception as e:
    print(f"  ERROR: {e}")

orcl.close()
print("\n=== FIN AUDITORIA ===")
