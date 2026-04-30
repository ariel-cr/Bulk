"""Diagnostico paso-a-paso del piloto: invocar cada capa manualmente."""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = sql("fcme_canonicos").cursor()
c_fc = sql("dbFC").cursor()

# Cleanup
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref='TST01'")
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")

# === Layer 1: el CRUD aislado ===
print("[1] Llamando sp_referenciaParticipeType_CRUD directamente en dbFC")
try:
    c_fc.execute("EXEC dbo.sp_referenciaParticipeType_CRUD @Accion='I', @CodigoTipoReferencia='TST01', @DescripcionTipoReferencia='Test 1'")
    c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref='TST01'")
    for r in c_fc.fetchall():
        print(f"  OK: cod={r.co_tref} desc={r.ds_tref}")
except Exception as e:
    print(f"  FAIL: {e}")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref='TST01'")

# === Layer 2: el wrapper aislado (cross-DB EXEC) ===
print("\n[2] Llamando wrapper usp_inbox_referenciaParticipeType directamente")
try:
    c.execute("""EXEC dbo.usp_inbox_referenciaParticipeType
                 @inbox_id=999, @aggregate_id='TST01', @aggregate_type='referenciaParticipeType',
                 @source_table='X', @event_type='INSERT',
                 @payload='{"CODIGOTIPOREFERENCIA":"TST01","DESCRIPCIONTIPOREFERENCIA":"Test 1 from wrapper"}'""")
    c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref='TST01'")
    for r in c_fc.fetchall():
        print(f"  OK: cod={r.co_tref} desc={r.ds_tref}")
    c.execute("SELECT inbox_id, error_message FROM dbo.cdc_inbox_errors WHERE inbox_id=999")
    for r in c.fetchall():
        print(f"  WRAPPER ERROR LOGGED: {r.error_message[:200]}")
except Exception as e:
    print(f"  FAIL: {e}")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref='TST01'")
c.execute("DELETE FROM dbo.cdc_inbox_errors WHERE inbox_id=999")

# === Layer 3: el dispatcher con un evento manual ===
print("\n[3] Insertar evento en cdc_inbox CON trigger DESHABILITADO, luego llamar dispatcher manual")
c.execute("DISABLE TRIGGER dbo.trg_process_cdc_inbox ON dbo.cdc_inbox")
try:
    c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
                 VALUES ('TST02','referenciaParticipeType','INSERT',
                         '{"CODIGOTIPOREFERENCIA":"TST02","DESCRIPCIONTIPOREFERENCIA":"Test 2"}',
                         'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")
    c.execute("SELECT id FROM dbo.cdc_inbox WHERE aggregate_id='TST02'")
    inbox_id = c.fetchone()[0]
    print(f"  inbox_id={inbox_id}")

    # Llamada manual al dispatcher
    c.execute("EXEC dbo.usp_process_cdc_inbox @inbox_id=?", inbox_id)
    while c.nextset(): pass

    c.execute("SELECT id, processed FROM dbo.cdc_inbox WHERE id=?", inbox_id)
    r = c.fetchone()
    print(f"  cdc_inbox: processed={r.processed}")
    c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref='TST02'")
    for r in c_fc.fetchall():
        print(f"  sfct_referencias: cod={r.co_tref} desc={r.ds_tref}")
    c.execute("SELECT inbox_id, error_message FROM dbo.cdc_inbox_errors")
    for r in c.fetchall():
        print(f"  ERROR: {r.inbox_id} {r.error_message[:200]}")
except Exception as e:
    print(f"  FAIL: {e}")
c.execute("ENABLE TRIGGER dbo.trg_process_cdc_inbox ON dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref='TST02'")

# === Layer 4: trigger CON event ===
print("\n[4] Trigger AFTER INSERT con event")
try:
    c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
                 VALUES ('TST03','referenciaParticipeType','INSERT',
                         '{"CODIGOTIPOREFERENCIA":"TST03","DESCRIPCIONTIPOREFERENCIA":"Test 3"}',
                         'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")
    c.execute("SELECT id, processed FROM dbo.cdc_inbox WHERE aggregate_id='TST03'")
    r = c.fetchone()
    print(f"  cdc_inbox: id={r.id} processed={r.processed}")
    c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref='TST03'")
    for r in c_fc.fetchall():
        print(f"  sfct_referencias: cod={r.co_tref} desc={r.ds_tref}")
except Exception as e:
    print(f"  FAIL: {e}")

# Cleanup
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref LIKE 'TST%'")
