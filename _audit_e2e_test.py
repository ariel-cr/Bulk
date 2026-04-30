"""Test end-to-end: dispara un UPDATE en una tabla con trigger activo
y observa la propagacion canonicos -> Kafka -> Oracle inbox -> tabla destino.
"""
import pyodbc, oracledb, time

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# Estado base
def snap():
    s = {}
    c = sql("fcme_canonicos").cursor()
    c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox WHERE aggregate_type != '_warmup'")
    s["canon_outbox"] = c.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
    s["ora_inbox"] = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=1")
    s["ora_processed"] = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
    s["ora_errors"] = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM FCME_USER.ACTUALIZACION_AFILIADO_TYPE")
    s["ora_target"] = co.fetchone()[0]
    return s

print("ESTADO INICIAL")
s0 = snap()
for k,v in s0.items(): print(f"  {k:<20} {v}")

# Disparar evento: UPDATE en fctbafil_actu (trigger activo)
print("\n>>> Disparando UPDATE en dbFC.dbo.fctbafil_actu")
c = sql("dbFC").cursor()
c.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu")
row = c.fetchone()
if not row:
    print("  ERROR: no hay filas en fctbafil_actu")
    raise SystemExit(1)
ced = row[0]
c.execute("UPDATE dbo.fctbafil_actu SET ci_cedu=ci_cedu WHERE ci_cedu=?", ced)
print(f"  UPDATE ejecutado sobre ci_cedu={ced} (rowcount={c.rowcount})")

# Inmediato: deberia haber subido canon_outbox
time.sleep(1)
s1 = snap()
print(f"\nT+1s: canon_outbox={s1['canon_outbox']} (delta {s1['canon_outbox']-s0['canon_outbox']})")
print(f"      ora_inbox={s1['ora_inbox']}  ora_processed={s1['ora_processed']}  target={s1['ora_target']}")

# Esperar Kafka -> Oracle
print("\nEsperando propagacion Kafka -> Oracle...")
for i in range(15):
    time.sleep(3)
    s = snap()
    print(f"  T+{(i+1)*3+1}s: canon_outbox={s['canon_outbox']}  ora_inbox={s['ora_inbox']}  processed={s['ora_processed']}  errors={s['ora_errors']}  target={s['ora_target']}")
    if s["ora_inbox"] > s0["ora_inbox"] and s["ora_processed"] > s0["ora_processed"]:
        break

print("\nESTADO FINAL")
sf = snap()
for k,v in sf.items(): print(f"  {k:<20} {v}  (delta {v-s0[k]})")

# Mostrar ultimos eventos
print("\nUltimos eventos en cdc_outbox (canonicos):")
c = sql("fcme_canonicos").cursor()
c.execute("SELECT TOP 3 id, aggregate_type, source_table, event_type, created_at FROM dbo.cdc_outbox ORDER BY id DESC")
for r in c.fetchall():
    print(f"  id={r.id} type={r.aggregate_type} src={r.source_table} ev={r.event_type} at={r.created_at}")

print("\nUltimos eventos en CDC_INBOX (Oracle):")
co.execute("SELECT * FROM (SELECT ID,AGGREGATE_TYPE,AGGREGATE_ID,EVENT_TYPE,PROCESSED,CREATED_AT FROM FCME_USER.CDC_INBOX ORDER BY ID DESC) WHERE ROWNUM<=3")
for r in co.fetchall():
    print(f"  id={r[0]} type={r[1]} agg={r[2]} ev={r[3]} processed={r[4]} created={r[5]}")

print("\n=== DIAGNOSTICO ===")
delta_outbox = sf["canon_outbox"] - s0["canon_outbox"]
delta_inbox = sf["ora_inbox"] - s0["ora_inbox"]
delta_proc = sf["ora_processed"] - s0["ora_processed"]
delta_targ = sf["ora_target"] - s0["ora_target"]

print(f"  Trigger -> canon_outbox: {'OK' if delta_outbox>0 else 'FAIL'}  (delta={delta_outbox})")
print(f"  Kafka  -> Oracle inbox:  {'OK' if delta_inbox>0 else 'FAIL'}  (delta={delta_inbox})")
print(f"  TRG_PROCESS -> SP -> tabla: {'OK' if delta_proc>0 else 'FAIL'}  (delta={delta_proc})")

orcl.close()
