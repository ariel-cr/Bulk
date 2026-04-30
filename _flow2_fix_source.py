"""Solucion 1: cambiar source a modo query con CAST/SUBSTR explicito.
Backup de la config previa para rollback rapido si algo falla.

NO toca:
  - Sink connector (sigue con la misma config + auto.offset.reset=latest)
  - Triggers Oracle, tablas, wrappers, CRUDs
"""
import json, urllib.request, time, oracledb, pyodbc

BASE = "http://10.35.3.223:30083"
SOURCE = "newcore-oracle-cdc-outbox-source"
SINK = "newcore-canonicos-cdc-inbox-jdbc-sink"

def http(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode() or "{}"
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read().decode() or "{}")
        except: return e.code, ""

# ===== [1] Backup config actual =====
print("="*70)
print("[1] Backup config actual del source")
print("="*70)
st, cfg_old = http("GET", f"/connectors/{SOURCE}/config")
backup_path = r"C:\Users\Usuario\Downloads\Bulk\_source_config_backup.json"
with open(backup_path, "w", encoding="utf-8") as f:
    json.dump(cfg_old, f, indent=2)
print(f"  backup -> {backup_path}")
print(f"  modo previo: {cfg_old.get('mode')} table={cfg_old.get('table.whitelist','')}")

# ===== [2] Construir nueva config con query + CAST =====
print("\n" + "="*70)
print("[2] Nueva config: modo query con CAST/SUBSTR")
print("="*70)
QUERY = (
  "SELECT "
  "CAST(ID AS NUMBER(19)) AS id, "
  "CAST(AGGREGATE_ID AS VARCHAR2(200)) AS aggregate_id, "
  "CAST(AGGREGATE_TYPE AS VARCHAR2(200)) AS aggregate_type, "
  "CAST(EVENT_TYPE AS VARCHAR2(50)) AS event_type, "
  "DBMS_LOB.SUBSTR(PAYLOAD, 4000, 1) AS payload, "
  "CAST(SOURCE_TABLE AS VARCHAR2(200)) AS source_table, "
  "CAST(CREATED_AT AS TIMESTAMP) AS created_at "
  "FROM FCME_USER.CDC_OUTBOX"
)

cfg_new = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:oracle:thin:@10.35.3.223:31521/XEPDB1",
    "connection.user": "fcme_user",
    "connection.password": "FcmeUser2025!",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "query": QUERY,
    "topic.prefix": "newcore.canonicos.CDC_OUTBOX_v2",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "numeric.mapping": "best_fit",
    "poll.interval.ms": "1000",
    "validate.non.null": "false"
}
print(f"  query truncado a 4000 chars en payload")
print(f"  topic destino: newcore.canonicos.CDC_OUTBOX_v2 (NUEVO topic)")
print(f"  numeric.mapping: best_fit")

# Aplicar
st, body = http("PUT", f"/connectors/{SOURCE}/config", cfg_new)
print(f"  PUT /config -> HTTP {st}")
if st >= 400:
    print(f"  ERROR: {body}")
    print("  ROLLBACK: restaurando config previa")
    http("PUT", f"/connectors/{SOURCE}/config", cfg_old)
    raise SystemExit(1)

# Source en modo query con incrementing necesita un offset reset porque la partition cambia
# (de table=FCME_USER.CDC_OUTBOX a query="..."). Lo manejamos:
print("\n[3] Reset offsets del source (la partition cambio de table -> query)")
http("PUT", f"/connectors/{SOURCE}/stop")
time.sleep(2)
st, body = http("DELETE", f"/connectors/{SOURCE}/offsets")
print(f"  DELETE /offsets -> HTTP {st}")
http("PUT", f"/connectors/{SOURCE}/resume")
time.sleep(5)

st, s = http("GET", f"/connectors/{SOURCE}/status")
print(f"  source status: {json.dumps(s, indent=2)[:600]}")

# ===== [4] Apuntar el SINK al nuevo topic =====
print("\n" + "="*70)
print("[4] Apuntar el SINK al nuevo topic")
print("="*70)
st, sink_cfg = http("GET", f"/connectors/{SINK}/config")
sink_cfg['topics'] = 'newcore.canonicos.CDC_OUTBOX_v2'
# Tambien resetear consumer offset al latest del nuevo topic
http("PUT", f"/connectors/{SINK}/stop")
time.sleep(2)
st, body = http("DELETE", f"/connectors/{SINK}/offsets")
print(f"  DELETE sink offsets -> HTTP {st}")
st, body = http("PUT", f"/connectors/{SINK}/config", sink_cfg)
print(f"  PUT sink config -> HTTP {st}")
http("PUT", f"/connectors/{SINK}/resume")
time.sleep(5)
http("POST", f"/connectors/{SINK}/tasks/0/restart")
time.sleep(3)
st, s = http("GET", f"/connectors/{SINK}/status")
print(f"  sink status: {json.dumps(s, indent=2)[:600]}")

# ===== [5] Test masivo: insertar evento por type =====
print("\n" + "="*70)
print("[5] Test masivo: 30 INSERTs Oracle, observar propagacion")
print("="*70)
o = oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co = o.cursor()
c = pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123', autocommit=True).cursor()
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c.execute("DELETE FROM dbo.cdc_inbox_parsed")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX"); o.commit()

AT_TO_TABLE = {
    "actualizacionAfiliadoType":"ACTUALIZACION_AFILIADO_TYPE",
    "actualizacionDocumentosType":"ACTUALIZACION_DOCUMENTOS_TYPE",
    "agendaMailAfiliadoType":"AGENDAMAILAFILIADO_TYPE",
    "auditoriaAfiliadoType":"AUDITORIAAFILIADO_TYPE",
    "beneficiarioParticipeType":"BENEFICIARIOPARTICIPE_TYPE",
    "cuentaBancariaAfiliadoType":"CUENTABANCARIAAFILIADO_TYPE",
    "distribucionAfiliadoType":"DISTRIBUCIONAFILIADO_TYPE",
    "documentacionAfiliadoType":"DOCUMENTACIONAFILIADO_TYPE",
    "firmanteParticipeType":"FIRMANTEPARTICIPE_TYPE",
    "grupoFamiliarType":"GRUPOFAMILIAR_TYPE",
    "informacionAdicionalAfiliadoType":"INFORMACIONADICIONALAFILIADO_TYPE",
    "institucionType":"INSTITUCION_TYPE",
    "motivoContableType":"MOTIVOCONTABLE_TYPE",
    "movimientoCuentaType":"MOVIMIENTOCUENTA_TYPE",
    "movimientoTemporalType":"MOVIMIENTOTEMPORAL_TYPE",
    "naturalInformacionAdicionalType":"NATURALINFORMACIONADICIONALTYPE",
    "naturalIngresosEgresosType":"NATURALINGRESOSEGRESOSTYPE",
    "naturalTrabajoType":"NATURALTRABAJOTYPE",
    "personaReferenciasBancariasType":"PERSONAREFERENCIASBANCARIASTYPE",
    "personaReferenciasPersonalesType":"PERSONAREFERENCIASPERSONALESTYPE",
    "personaTelefonosType":"PERSONATELEFONOSTYPE",
    "personaVinculacionesType":"PERSONAVINCULACIONESTYPE",
    "referenciaParticipeType":"REFERENCIAPARTICIPE_TYPE",
    "reporteSIBSParticipeType":"REPORTESIBSPARTICIPE_TYPE",
    "retiroLiquidacionType":"RETIROLIQUIDACION_TYPE",
    "retiroVoluntarioEstadoType":"RETIROVOLUNTARIOESTADO_TYPE",
    "saldoDiarioRubroType":"SALDODIARIORUBRO_TYPE",
    "saldoDiarioType":"SALDODIARIO_TYPE",
    "seguroVidaParticipeType":"SEGUROVIDAPARTICIPE_TYPE",
    "servicioAdicionalType":"SERVICIOADICIONAL_TYPE",
}

inserted = []
for at, ot in AT_TO_TABLE.items():
    co.execute("""SELECT column_name, data_type, data_length FROM all_tab_columns
                  WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id""", t=ot)
    cols = [(r[0], r[1], r[2]) for r in co.fetchall() if r[0] != "ID"]
    if not cols: continue
    col_names = ", ".join(c[0] for c in cols)
    vals = []
    for cn, ct, dl in cols:
        if "VARCHAR" in ct or "CHAR" in ct: vals.append("'9'" if (dl or 99) <= 2 else "'99'")
        elif "NUMBER" in ct: vals.append("9")
        elif "DATE" in ct or "TIMESTAMP" in ct: vals.append("SYSTIMESTAMP")
        elif "CLOB" in ct: vals.append("'{}'")
        else: vals.append("NULL")
    try:
        co.execute(f"INSERT INTO FCME_USER.{ot} ({col_names}) VALUES ({', '.join(vals)})")
        orcl_commit_ok = True
        inserted.append((at, ot))
    except: pass
o.commit()
print(f"  Oracle INSERTs: {len(inserted)}")

# Esperar
print("\n[Propagacion]")
prev = 0; stable = 0
for i in range(40):
    time.sleep(2)
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX"); ob=co.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); ib=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_parsed"); ps=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors"); er=c.fetchone()[0]
    print(f"  T+{(i+1)*2}s: outbox={ob} inbox={ib} parsed={ps} errors={er}")
    if ib == prev:
        stable += 1
        if stable >= 5 and ib >= len(inserted): break
    else: stable = 0
    prev = ib

# Reporte
print("\n[Resultado por type]")
ok = 0; err_n = 0; missing = 0
for at, ot in inserted:
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE aggregate_type=?", at)
    n = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE aggregate_type=?", at)
    e = c.fetchone()[0]
    if n == 0: missing += 1
    elif e > 0: err_n += 1
    else: ok += 1

print(f"  OK={ok}  ERR={err_n}  MISSING={missing}  total={len(inserted)}")

# Sink status
st, s = http("GET", f"/connectors/{SINK}/status")
state = s.get('tasks',[{}])[0].get('state','?') if isinstance(s,dict) else '?'
trace = s.get('tasks',[{}])[0].get('trace','') if isinstance(s,dict) else ''
print(f"  sink task: {state}")
if trace: print(f"    trace: {trace[:1500]}")

# Cleanup test rows
for at, ot in inserted:
    try: co.execute(f"DELETE FROM FCME_USER.{ot} WHERE ROWID IN (SELECT ROWID FROM FCME_USER.{ot} ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY)")
    except: pass
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX"); o.commit()
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c.execute("DELETE FROM dbo.cdc_inbox_parsed")
o.close()
print("\n=== Fin ===")
