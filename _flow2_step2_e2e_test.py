"""Test e2e Newcore -> Kafka -> cdc_inbox SQL.
1) INSERT en FCME_USER.REFERENCIAPARTICIPE_TYPE
2) Trigger Oracle emite a CDC_OUTBOX
3) Source connector publica al topic newcore.canonicos.CDC_OUTBOX
4) Sink connector lee y escribe a fcme_canonicos.cdc_inbox
5) Verificar la fila final en cdc_inbox SQL Server
"""
import oracledb, pyodbc, time

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
c = sql("fcme_canonicos").cursor()

# Reset
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
orcl.commit()
c.execute("DELETE FROM dbo.cdc_inbox")
print("[Reset] outbox + inbox vacios")

# Estado base
def snap():
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX")
    o = co.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
    i = c.fetchone()[0]
    return o, i

o0, i0 = snap()
print(f"  inicial: outbox={o0} inbox={i0}")

# Disparar evento
print("\n[Disparar] INSERT en FCME_USER.REFERENCIAPARTICIPE_TYPE")
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='K2K'")
co.execute("INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('K2K','KAFKA E2E')")
orcl.commit()
print("  Oracle INSERT ok")

# Esperar propagacion
print("\n[Esperando propagacion Kafka]")
for i in range(20):
    time.sleep(2)
    o, ic = snap()
    print(f"  T+{(i+1)*2}s: outbox={o} inbox={ic}")
    if ic > i0:
        break

# Verificar fila final en cdc_inbox
print("\n[Verificacion final]")
c.execute("""SELECT TOP 5 id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, processed
             FROM dbo.cdc_inbox ORDER BY id DESC""")
rows = c.fetchall()
for r in rows:
    print(f"  inbox: id={r.id} agg={r.aggregate_id} type={r.aggregate_type} ev={r.event_type}")
    print(f"    src={r.source_table}")
    print(f"    payload={r.payload[:200]}")
    print(f"    created={r.created_at}  processed={r.processed}")

# Cleanup
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='K2K'")
orcl.commit()

if rows:
    print("\n=== Paso 2 OK: Newcore -> Kafka -> cdc_inbox SQL funciona ===")
else:
    print("\n=== Paso 2 FAIL: no llego al inbox SQL ===")
orcl.close()
