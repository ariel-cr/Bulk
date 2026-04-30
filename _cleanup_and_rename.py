"""Limpieza total + rename SP a USP_INBOX_PARTICIPES (con S segun convencion usuario).
+ Test end-to-end del flujo confirmado:
  Tabla Legacy -> trg outbox -> canonicos.cdc_outbox -> Kafka -> Oracle CDC_INBOX
  -> TRG_PROCESS_CDC_INBOX -> USP_INBOX_PARTICIPES -> FCME_USER.*_TYPE
"""
import pyodbc, oracledb, time
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

print("== [1] Limpieza total ==")
c_can = sql("fcme_canonicos").cursor()
c_can.execute("DELETE FROM dbo.cdc_outbox"); print(f"  canonicos.cdc_outbox: {c_can.rowcount} borrados")
c_can.execute("DELETE FROM dbo.cdc_inbox"); print(f"  canonicos.cdc_inbox: {c_can.rowcount} borrados")
c_can.execute("DBCC CHECKIDENT ('dbo.cdc_outbox', RESEED, 10000)")
print("  IDENTITY canonicos.cdc_outbox reset a 10000")

co.execute("DELETE FROM CDC_INBOX"); print(f"  Oracle CDC_INBOX: {co.rowcount} borrados")
co.execute("DELETE FROM CDC_INBOX_ERRORS"); print(f"  Oracle ERRORS: {co.rowcount} borrados")
co.execute("DELETE FROM ACTUALIZACION_AFILIADO_TYPE"); print(f"  Oracle ACTUALIZACION_AFILIADO_TYPE: {co.rowcount} borrados")
co.execute("DELETE FROM ACTUALIZACION_DOCUMENTOS_TYPE"); print(f"  Oracle ACTUALIZACION_DOCUMENTOS_TYPE: {co.rowcount} borrados")
orcl.commit()

print("\n== [2] Rename SP a USP_INBOX_PARTICIPES (con S) ==")
# leer el body del SP actual y re-crearlo con nombre nuevo
co.execute("SELECT text FROM all_source WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPE' ORDER BY line")
lines = [r[0] for r in co.fetchall()]
body = "".join(lines)
# reemplazar header para PARTICIPES
new_body = body.replace("USP_INBOX_PARTICIPE", "USP_INBOX_PARTICIPES")
# Crear SP nuevo
co.execute("CREATE OR REPLACE PROCEDURE " + new_body.split("PROCEDURE",1)[1])
print("  USP_INBOX_PARTICIPES creado")

# drop el anterior
try:
    co.execute("DROP PROCEDURE USP_INBOX_PARTICIPE")
    print("  USP_INBOX_PARTICIPE (viejo) eliminado")
except Exception as e:
    print(f"  drop old: {str(e)[:100]}")

# Actualizar module_config para apuntar al nombre nuevo
co.execute("UPDATE CDC_INBOX_MODULE_CONFIG SET SP_NAME='USP_INBOX_PARTICIPES'")
orcl.commit()
print(f"  module_config actualizado: {co.rowcount} filas")

print("\n== [3] Rehabilitar trigger TRG_PROCESS_CDC_INBOX ==")
# El trigger actual llama al SP con nombre viejo via sp_executesql dinamico, asi que lee module_config
# que ya fue actualizado. No hace falta cambiar el trigger.
co.execute("ALTER TRIGGER TRG_PROCESS_CDC_INBOX ENABLE")
print("  ok - el trigger lee SP_NAME desde module_config")

print("\n== [4] Recrear sink Oracle para consumer group fresh con 'latest' ==")
# delete cualquier sink previo
import urllib.request, json
def kc(method, path, body=None):
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request("http://10.35.3.223:30083"+path, data=data, method=method,
        headers={"Accept":"application/json","Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode() or "{}"
            return r.status, json.loads(raw) if raw.strip() else {}
    except Exception as e: return None, str(e)

for name in ["newcore-oracle-sink-v3","newcore-oracle-sink-v4","newcore-oracle-sink-v5"]:
    st, _ = kc("DELETE", f"/connectors/{name}")
    if st: print(f"  DELETE {name}: {st}")
time.sleep(3)

sink_config = {
  "name": "newcore-oracle-convivencia-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:oracle:thin:@10.35.3.223:31521/XEPDB1",
    "connection.user": "fcme_user",
    "connection.password": "FcmeUser2025!",
    "topics": "convivencia.canonicos.cdc.outbox",
    "insert.mode": "insert",
    "table.name.format": "FCME_USER.CDC_INBOX",
    "auto.create": "false",
    "auto.evolve": "false",
    "quote.sql.identifiers": "never",
    "consumer.override.auto.offset.reset": "latest",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
st, _ = kc("POST", "/connectors", sink_config)
print(f"  CREATE sink: {st}")
time.sleep(15)

print("\n== [5] Test end-to-end ==")
f = sql("dbFC").cursor()
f.execute("UPDATE dbo.fctbafil_actu SET ci_cedu=ci_cedu WHERE ci_cedu=(SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu)")
print("  UPDATE ejecutado")

c_can.execute("SELECT id, aggregate_type FROM dbo.cdc_outbox")
for r in c_can.fetchall(): print(f"  canonicos.cdc_outbox: id={r[0]} type={r[1]}")

for i in range(5):
    time.sleep(8)
    co.execute("SELECT ID, AGGREGATE_TYPE, PROCESSED FROM CDC_INBOX")
    rows = co.fetchall()
    co.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE"); at = co.fetchone()[0]
    print(f"t={(i+1)*8}s  Oracle CDC_INBOX={len(rows)}  ACTUALIZACION_AFILIADO_TYPE={at}")
    for r in rows: print(f"    inbox id={r[0]} type={r[1]} processed={r[2]}")
    if rows: break
