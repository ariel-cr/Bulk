"""Re-disparar los 4 stuck + capturar evento + inyectar a cdc_inbox bypass Kafka."""
import oracledb, pyodbc, time, json

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
conn=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=fcme_canonicos;UID={DB['username']};PWD={DB['password']}", autocommit=True)
c=conn.cursor()
orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()

TESTS = [
    ('anticipoNominaType','ANTICIPONOMINA_TYPE','ANIO'),
    ('firmaHorarioType','FIRMAHORARIO_TYPE','MAQUINAENLAQUEFIRMO'),
    ('nivelAcademicoType','NIVELACADEMICO_TYPE','CODIGOINSTITUCION'),
    ('viaticoNominaType','VIATICONOMINA_TYPE','CODIGOEMPLEADO'),
]

c.execute("SELECT ISNULL(MAX(id),0) FROM dbo.cdc_inbox")
inb_max = c.fetchone()[0]

print("[1] Disparando UPDATE no-op + captura inmediata + inyeccion directa")
print("-"*70)

for agg, dest, col in TESTS:
    print(f'\n--- {agg} ---')
    o.execute(f"SELECT ID FROM FCME_USER.{dest} WHERE ROWNUM<=1")
    r = o.fetchone()
    if not r:
        print(f"  tabla vacia, skip")
        continue
    rid = r[0]
    # capturar offset CDC_OUTBOX antes
    o.execute("SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX")
    out_before = o.fetchone()[0]

    # disparar
    o.execute(f"UPDATE FCME_USER.{dest} SET {col}={col} WHERE ID=:1", [rid])
    orcl.commit()
    print(f"  UPDATE FCME_USER.{dest}.ID={rid}")

    # capturar evento INMEDIATAMENTE
    time.sleep(0.5)
    o.execute(f"""SELECT ID, AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT
                 FROM FCME_USER.CDC_OUTBOX WHERE ID > {out_before} AND AGGREGATE_TYPE = :1
                 ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY""", [agg])
    evt = o.fetchone()
    if not evt:
        print(f"  WARN: no se encontro evento en CDC_OUTBOX")
        continue
    oid, agg_id, agg_t, ev, payload_clob, src, created = evt
    payload_str = payload_clob.read() if hasattr(payload_clob, 'read') else (payload_clob or '')
    print(f"  capturado: outbox.id={oid}  payload_len={len(payload_str)}")

    # inyectar directo en cdc_inbox SQL Server
    try:
        c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at, processed)
                     VALUES (?, ?, ?, ?, ?, ?, 0)""",
                  str(agg_id), agg_t, ev, payload_str, src, created)
        print(f"  INYECTADO en cdc_inbox  (trigger procesara)")
    except Exception as e:
        print(f"  FAIL inject: {str(e)[:200]}")

# Esperar trigger processing
print("\n[2] Esperando trigger processing 10s...")
time.sleep(10)

# Verificar resultados
print("\n[3] RESULTADOS")
print("-"*70)
ok = 0
for agg, dest, col in TESTS:
    c.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=?", agg)
    inb_n=c.fetchone()[0]
    c.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=? AND processed=1", agg)
    pr_n=c.fetchone()[0]
    c.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=?", agg)
    er_n=c.fetchone()[0]
    em=None
    if er_n>0:
        c.execute(f"SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=? ORDER BY error_id DESC", agg)
        em=c.fetchone()[0][:200]
    status='OK' if (inb_n>=1 and pr_n>=inb_n and er_n==0) else f'ERR: {em}' if em else 'PARTIAL'
    if inb_n>=1 and pr_n>=inb_n and er_n==0: ok+=1
    print(f"  {agg:<28}  inbox+={inb_n} proc={pr_n} err={er_n}  {status[:80]}")

print(f"\n[RESUMEN] OK={ok}/4")
orcl.close()
