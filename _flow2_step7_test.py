"""Test e2e de varios types: simula el evento desde cdc_inbox y verifica
que actualiza la tabla legacy correspondiente."""
import pyodbc, oracledb
import json as jsonmod

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# ==== Resumen module_config ====
c = sql("fcme_canonicos").cursor()
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE active=1")
print(f"module_config types activos: {c.fetchone()[0]}")

# ==== Verificar triggers Oracle desplegados ====
co.execute("""SELECT trigger_name, table_name, status FROM all_triggers
              WHERE owner='FCME_USER' AND trigger_name LIKE 'TRG_OUTBOX_%'
              ORDER BY trigger_name""")
trgs = co.fetchall()
print(f"\nTriggers Oracle outbox: {len(trgs)}")
for r in trgs[:10]:
    print(f"  {r[0]:<35} on {r[1]:<35} {r[2]}")
if len(trgs)>10: print(f"  ...y {len(trgs)-10} mas")

# ==== Test e2e con 3 types ====
TESTS = [
    {
        "type": "referenciaParticipeType",
        "src_db": "dbFC", "src_table": "sfct_referencias", "pk_col": "co_tref",
        "pk_value": 88,
        "payload": {"CODIGOTIPOREFERENCIA":"88","DESCRIPCIONTIPOREFERENCIA":"REF E2E"},
    },
    {
        "type": "firmanteParticipeType",
        "src_db": "dbFC", "src_table": "sfct_firmante", "pk_col": "ci_cedula",
        "pk_value": "9999999999",
        "payload": {"IDENTIFICACION":"9999999999","CODIGOTIPOIDENTIFICACION":"C","SECUENCIA":"1","NOMBRE":"FIRMANTE TEST","CARGO":"GERENTE"},
    },
    {
        "type": "motivoContableType",
        "src_db": "dbFC", "src_table": "sfct_motivo_cnta_cble", "pk_col": None,
        "pk_value": None,
        "payload": {"CODIGOTIPOIDENTIFICACION":"C","IDENTIFICACION":"9999999998","SECUENCIA":"1","CODIGOMOTIVOCONTABLE":"01","DESCRIPCIONMOTIVOCONTABLE":"Test"},
    },
]

print("\n" + "="*70)
print("Test e2e (simulamos sink Kafka -> cdc_inbox)")
print("="*70)
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")

ok = 0
fail = 0
for t in TESTS:
    print(f"\n--- {t['type']} ({t['src_db']}.{t['src_table']}) ---")
    payload_json = jsonmod.dumps(t["payload"])
    try:
        c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
                     VALUES (?, ?, 'INSERT', ?, ?)""",
                  str(t["pk_value"] or "X"), t["type"], payload_json, f"FCME_USER.{t['type']}")
        # check processed
        c.execute("SELECT TOP 1 id, processed FROM dbo.cdc_inbox ORDER BY id DESC")
        r = c.fetchone()
        c.execute("SELECT inbox_id, error_message FROM dbo.cdc_inbox_errors WHERE inbox_id=?", r.id)
        errs = c.fetchall()
        if errs:
            print(f"  processed={r.processed}  ERROR: {errs[0].error_message[:200]}")
            fail += 1
        else:
            # check legacy table
            if t["pk_col"] and t["pk_value"] is not None:
                c2 = sql(t["src_db"]).cursor()
                c2.execute(f"SELECT TOP 1 * FROM dbo.[{t['src_table']}] WHERE [{t['pk_col']}]=?", t["pk_value"])
                row = c2.fetchone()
                if row:
                    cols = [d[0] for d in c2.description]
                    print(f"  OK processed={r.processed}, legacy row found:")
                    print(f"    {dict(zip(cols, row))}")
                    ok += 1
                else:
                    print(f"  WARN processed={r.processed}, legacy row NOT found")
                    fail += 1
            else:
                print(f"  processed={r.processed} (no PK check porque pk_col=None)")
                ok += 1
    except Exception as e:
        print(f"  EXCEPCION: {str(e)[:300]}")
        fail += 1

print(f"\n=== Resultado: ok={ok} fail={fail} ===")

# Limpieza de filas test
print("\n[Cleanup]")
for t in TESTS:
    if t["pk_col"] and t["pk_value"] is not None:
        try:
            c2 = sql(t["src_db"]).cursor()
            c2.execute("EXEC sp_set_session_context N'is_replicating', 1")
            c2.execute(f"DELETE FROM dbo.[{t['src_table']}] WHERE [{t['pk_col']}]=?", t["pk_value"])
            c2.execute("EXEC sp_set_session_context N'is_replicating', 0")
        except: pass
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")

orcl.close()
