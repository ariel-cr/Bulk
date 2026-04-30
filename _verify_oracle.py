"""Verifica el flujo completo Legacy -> canonicos.cdc_outbox -> Kafka -> Oracle FCME_USER.CDC_INBOX"""
import pyodbc, oracledb, time, json, urllib.request

# 1) conexion Oracle
try:
    orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!",
                            dsn="10.35.3.223:31521/XEPDB1")
    print("Oracle conectado OK")
except Exception as e:
    print(f"Oracle err: {e}")
    raise SystemExit

co = orcl.cursor()

# estructura de la tabla
print("\n== Columnas de FCME_USER.CDC_INBOX ==")
co.execute("""
  SELECT column_name, data_type, nullable FROM all_tab_columns
  WHERE owner='FCME_USER' AND table_name='CDC_INBOX' ORDER BY column_id
""")
for r in co.fetchall(): print(f"  {r[0]:<20} {r[1]:<20} nullable={r[2]}")

# conteo actual
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
before_oracle = co.fetchone()[0]
print(f"\n== Antes ==\n  Oracle FCME_USER.CDC_INBOX: {before_oracle} filas")

# conteo canonicos
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn_sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)
c_can = conn_sql("fcme_canonicos").cursor()
c_can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
before_can = c_can.fetchone()[0]
print(f"  canonicos.cdc_outbox: {before_can}")

# UPDATE legacy
c_fc = conn_sql("dbFC").cursor()
c_fc.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu ORDER BY ci_cedu")
ci = c_fc.fetchone()[0]
print(f"\nUPDATE dbFC.fctbafil_actu ci_cedu={ci}")
c_fc.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)

c_can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
after_can = c_can.fetchone()[0]
print(f"  canonicos.cdc_outbox: {before_can} -> {after_can}  (+{after_can-before_can})")

print("\nesperando 20s a Kafka propagacion...")
time.sleep(20)

co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
after_oracle = co.fetchone()[0]
print(f"\n== Despues ==\n  Oracle FCME_USER.CDC_INBOX: {before_oracle} -> {after_oracle}  (+{after_oracle-before_oracle})")

if after_oracle > before_oracle:
    print("\nUltimos 5 en Oracle:")
    co.execute("""
      SELECT * FROM (
        SELECT ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, SOURCE_TABLE, PROCESSED
        FROM FCME_USER.CDC_INBOX ORDER BY ID DESC
      ) WHERE ROWNUM <= 5
    """)
    for r in co.fetchall():
        print(f"  id={r[0]} type={r[1]} agg={r[2]} op={r[3]} src={r[4]} processed={r[5]}")
else:
    print("\nNo llego. Status del sink:")
    req = urllib.request.Request("http://10.35.3.223:30083/connectors/newcore-convivencia-cdc-outbox-jdbc-sink/status")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            s = json.loads(r.read())
        print(f"  connector: {s.get('connector',{}).get('state')}")
        for t in s.get("tasks",[]):
            print(f"  task[{t.get('id')}]: {t.get('state')}")
            if t.get("trace"):
                print(f"    trace: {t['trace'][:800]}")
    except Exception as e:
        print(f"  status err: {e}")
