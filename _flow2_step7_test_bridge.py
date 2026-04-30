"""Test e2e con bridge manual Kafka:
1) INSERT en tabla Oracle TYPE
2) Trigger Oracle escribe a CDC_OUTBOX (JSON con todas las cols)
3) Lee outbox y reinsertarlo manualmente en SQL canonicos.cdc_inbox (simula el sink)
4) Trigger SQL dispara dispatcher -> wrapper -> CRUD -> tabla legacy
5) Verificar la fila en la tabla legacy
"""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
c_can = sql("fcme_canonicos").cursor()

# Limpieza
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
c_can.execute("DELETE FROM dbo.cdc_inbox")
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")
orcl.commit()

TESTS = [
    {"ora_table":"REFERENCIAPARTICIPE_TYPE", "leg_db":"dbFC", "leg_table":"sfct_referencias", "leg_pk":"co_tref", "test_id":77,
     "ora_insert":"INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('77','BRIDGE TEST')"},
    {"ora_table":"GRUPOFAMILIAR_TYPE", "leg_db":"dbFC", "leg_table":"sfct_grupo_fami", "leg_pk":"co_fami", "test_id":None,
     "ora_insert":None},  # se decidira tras inspeccion
    {"ora_table":"AGENDAMAILAFILIADO_TYPE", "leg_db":"dbFC", "leg_table":"fctbagen_mail", "leg_pk":None, "test_id":None,
     "ora_insert":None},
]

# Inspeccionamos columnas oracle de cada tabla destino para construir INSERT generico
def get_ora_cols(t):
    co.execute("SELECT column_name, data_type, nullable FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id", t=t)
    return co.fetchall()

# Construir INSERTs para los que no tenemos
for t in TESTS:
    if t["ora_insert"]:
        continue
    cols = get_ora_cols(t["ora_table"])
    insertable = [(c[0], c[1]) for c in cols if c[0]!="ID"]
    col_names = ", ".join(c[0] for c in insertable)
    vals = []
    for cn, ct in insertable:
        if "VARCHAR" in ct or "CHAR" in ct or "CLOB" in ct:
            vals.append(f"'TST_{cn[:10]}'")
        elif "NUMBER" in ct:
            vals.append("99")
        elif "DATE" in ct or "TIMESTAMP" in ct:
            vals.append("SYSTIMESTAMP")
        else:
            vals.append("NULL")
    t["ora_insert"] = f"INSERT INTO FCME_USER.{t['ora_table']} ({col_names}) VALUES ({', '.join(vals)})"

print("="*70)
print("BRIDGE TEST")
print("="*70)

ok = 0; fail = 0
for t in TESTS:
    print(f"\n--- {t['ora_table']} -> {t['leg_db']}.{t['leg_table']} ---")

    # 1) INSERT en Oracle TYPE
    try:
        co.execute(t["ora_insert"])
        orcl.commit()
        print(f"  Oracle INSERT OK")
    except Exception as e:
        print(f"  Oracle INSERT fail: {str(e)[:250]}")
        fail += 1
        continue

    # 2) Verificar outbox
    co.execute("SELECT ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE FROM FCME_USER.CDC_OUTBOX ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY")
    row = co.fetchone()
    if not row:
        print(f"  outbox vacio - trigger no se disparo")
        fail += 1
        continue
    ora_id, agg_type, agg_id, ev, payload, src = row
    payload_str = payload.read() if hasattr(payload,'read') else payload
    print(f"  outbox: agg_type={agg_type} agg_id={agg_id} ev={ev}")
    print(f"  payload[:200]={payload_str[:200]}")

    # 3) Insertar en cdc_inbox (simula el Kafka sink)
    try:
        c_can.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
                         VALUES (?, ?, ?, ?, ?)""",
                      agg_id, agg_type, ev, payload_str, src)
    except Exception as e:
        print(f"  cdc_inbox INSERT fail: {str(e)[:300]}")
        fail += 1
        continue

    # 4) Verificar processed y errors
    c_can.execute("SELECT TOP 1 id, processed FROM dbo.cdc_inbox ORDER BY id DESC")
    inbox_row = c_can.fetchone()
    c_can.execute("SELECT error_message FROM dbo.cdc_inbox_errors WHERE inbox_id=?", inbox_row.id)
    errs = c_can.fetchall()
    if errs:
        print(f"  processed={inbox_row.processed} ERROR: {errs[0].error_message[:300]}")
        fail += 1
    else:
        # 5) Verificar fila en legacy
        leg_pk = t.get("leg_pk")
        leg_id = t.get("test_id") or agg_id
        if leg_pk:
            c_leg = sql(t["leg_db"]).cursor()
            try:
                c_leg.execute(f"SELECT TOP 1 * FROM dbo.[{t['leg_table']}] WHERE [{leg_pk}]=?", leg_id)
                row_leg = c_leg.fetchone()
                if row_leg:
                    cols = [d[0] for d in c_leg.description]
                    pairs = dict(zip(cols, row_leg))
                    print(f"  OK legacy row: { {k:v for k,v in list(pairs.items())[:5]} }...")
                    ok += 1
                else:
                    print(f"  WARN: processed sin error pero legacy row no encontrada (PK={leg_pk}={leg_id})")
                    fail += 1
            except Exception as e:
                print(f"  legacy lookup fail: {str(e)[:200]}")
                fail += 1
        else:
            print(f"  processed={inbox_row.processed} (sin verificacion legacy)")
            ok += 1

# Cleanup
print("\n[Cleanup]")
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='77'")
co.execute("DELETE FROM FCME_USER.GRUPOFAMILIAR_TYPE WHERE CODIGOTIPOIDENTIFICACION LIKE 'TST_%' OR IDENTIFICACION LIKE 'TST_%'")
co.execute("DELETE FROM FCME_USER.AGENDAMAILAFILIADO_TYPE WHERE CODIGOTIPOIDENTIFICACION LIKE 'TST_%' OR IDENTIFICACION LIKE 'TST_%'")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
orcl.commit()
c_can.execute("DELETE FROM dbo.cdc_inbox")
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")
# limpiar test rows en legacy
c_fc = sql("dbFC").cursor()
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 1")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref=77")
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 0")

print(f"\n=== Resultado: ok={ok} fail={fail} ===")
orcl.close()
