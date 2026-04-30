"""Bypass Kafka: copia los 4 eventos atorados directo de Oracle.CDC_OUTBOX -> SQL Server.cdc_inbox.
Trigger trg_process_cdc_inbox dispara los wrappers automaticamente."""
import oracledb, pyodbc, time

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
conn=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=fcme_canonicos;UID={DB['username']};PWD={DB['password']}", autocommit=True)
c=conn.cursor()
o=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1').cursor()

STUCK = ['anticipoNominaType','firmaHorarioType','nivelAcademicoType','viaticoNominaType']

# Baseline
c.execute("SELECT ISNULL(MAX(id),0) FROM dbo.cdc_inbox")
inb_max = c.fetchone()[0]
print(f"baseline cdc_inbox.max_id = {inb_max}")

print()
print("[INYECTANDO] eventos de Oracle.CDC_OUTBOX -> SQL Server.cdc_inbox")
copied = 0
for agg in STUCK:
    o.execute("""SELECT ID, AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT
                 FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_TYPE = :1 ORDER BY ID""", [agg])
    rows = o.fetchall()
    print(f"\n  {agg}: {len(rows)} eventos en outbox")
    for r in rows:
        oid, agg_id, agg_t, ev, payload_clob, src, created = r
        payload_str = payload_clob.read() if hasattr(payload_clob, 'read') else (payload_clob or '')
        try:
            c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at, processed)
                         VALUES (?, ?, ?, ?, ?, ?, 0)""",
                      str(agg_id), agg_t, ev, payload_str, src, created)
            copied += 1
            print(f"    + outbox.id={oid} -> cdc_inbox  (ev={ev})")
        except Exception as e:
            print(f"    FAIL outbox.id={oid}: {str(e)[:200]}")

print(f"\nTotal inyectados: {copied}")

# Esperar 10s para que trigger procese
print("\n[ESPERANDO TRIGGER PROCESSING] 10s...")
time.sleep(10)

# Verificar resultados
print("\n[RESULTADOS]")
for agg in STUCK:
    c.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=?", agg)
    inb_n=c.fetchone()[0]
    c.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=? AND processed=1", agg)
    pr_n=c.fetchone()[0]
    c.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=?", agg)
    er_n=c.fetchone()[0]
    em=None
    if er_n>0:
        c.execute(f"SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=? ORDER BY error_id DESC", agg)
        em=c.fetchone()[0][:150]
    status='OK' if (inb_n>=1 and pr_n>=inb_n and er_n==0) else f'ERR: {em}' if em else 'PARTIAL'
    print(f"  {agg:<28}  inbox={inb_n} processed={pr_n} errors={er_n}  {status}")

o.connection.close()
print("\n=== FIN ===")
