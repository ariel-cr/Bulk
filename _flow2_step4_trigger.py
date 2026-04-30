"""Paso 4 (Flujo 2): trg_process_cdc_inbox AFTER INSERT en fcme_canonicos.cdc_inbox.

Para cada fila insertada (vienen del Kafka sink, posiblemente en batch),
invoca usp_process_cdc_inbox(@inbox_id) que despacha al wrapper segun module_config.
"""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = sql("fcme_canonicos").cursor()

print("="*70)
print("[4.1] Crear trigger trg_process_cdc_inbox AFTER INSERT")
print("="*70)
trg_sql = """
CREATE OR ALTER TRIGGER dbo.trg_process_cdc_inbox
ON dbo.cdc_inbox
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @id BIGINT;
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT id FROM inserted ORDER BY id;

    OPEN cur;
    FETCH NEXT FROM cur INTO @id;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            EXEC dbo.usp_process_cdc_inbox @inbox_id = @id;
        END TRY
        BEGIN CATCH
            -- Salvaguardia: el dispatcher ya maneja su propio TRY/CATCH,
            -- pero si el EXEC mismo fallase (objeto no existe, etc.),
            -- registramos para no romper el INSERT del sink.
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            SELECT @id, aggregate_type, event_type,
                   N'trigger CATCH: ' + ERROR_MESSAGE()
            FROM dbo.cdc_inbox WHERE id = @id;
        END CATCH

        FETCH NEXT FROM cur INTO @id;
    END
    CLOSE cur;
    DEALLOCATE cur;
END
"""
c.execute(trg_sql)
c.execute("""SELECT t.name, t.is_disabled FROM sys.triggers t
             WHERE t.parent_id=OBJECT_ID('dbo.cdc_inbox')""")
for r in c.fetchall():
    print(f"  trigger: {r.name}  disabled={r.is_disabled}")

print("\n" + "="*70)
print("[4.2] Wrapper dummy + module_config")
print("="*70)
c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_inbox_TEST_PRUEBA
    @inbox_id BIGINT,
    @aggregate_id NVARCHAR(200),
    @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200),
    @event_type NVARCHAR(50),
    @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    -- simulamos insertar en una "tabla legacy" — usamos tempdb para no tocar produccion
    IF OBJECT_ID('tempdb..##test_legacy_target') IS NULL
        CREATE TABLE ##test_legacy_target (
            id BIGINT, agg_id NVARCHAR(200), payload NVARCHAR(MAX), at DATETIME2 DEFAULT SYSUTCDATETIME()
        );
    INSERT INTO ##test_legacy_target (id, agg_id, payload)
    VALUES (@inbox_id, @aggregate_id, @payload);
END
""")
c.execute("""DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type='_TEST_TYPE'""")
c.execute("""INSERT INTO dbo.cdc_inbox_module_config
             (aggregate_type, sp_name, target_db, module_name, active)
             VALUES ('_TEST_TYPE', 'dbo.usp_inbox_TEST_PRUEBA', 'tempdb', 'TEST', 1)""")
print("  wrapper y config listos")

print("\n" + "="*70)
print("[4.3] Test single-row: INSERT en cdc_inbox dispara dispatcher")
print("="*70)
c.execute("""IF OBJECT_ID('tempdb..##test_legacy_target') IS NOT NULL DROP TABLE ##test_legacy_target""")
c.execute("""CREATE TABLE ##test_legacy_target (id BIGINT, agg_id NVARCHAR(200), payload NVARCHAR(MAX), at DATETIME2 DEFAULT SYSUTCDATETIME())""")

c.execute("""INSERT INTO dbo.cdc_inbox
             (aggregate_id, aggregate_type, event_type, payload)
             VALUES ('AGG001', '_TEST_TYPE', 'INSERT', '{"k":"v1"}')""")
c.execute("SELECT TOP 1 id, processed, processed_at FROM dbo.cdc_inbox ORDER BY id DESC")
r = c.fetchone()
print(f"  cdc_inbox last: id={r.id} processed={r.processed} processed_at={r.processed_at}")
c.execute("SELECT id, agg_id, payload FROM ##test_legacy_target")
for r in c.fetchall():
    print(f"  legacy target: inbox_id={r.id} agg={r.agg_id} payload={r.payload}")

print("\n" + "="*70)
print("[4.4] Test multi-row: INSERT batch (Kafka sink en lote)")
print("="*70)
c.execute("""INSERT INTO dbo.cdc_inbox
             (aggregate_id, aggregate_type, event_type, payload) VALUES
             ('AGG002', '_TEST_TYPE', 'INSERT', '{"k":"v2"}'),
             ('AGG003', '_TEST_TYPE', 'UPDATE', '{"k":"v3"}'),
             ('AGG004', '_TEST_TYPE', 'INSERT', '{"k":"v4"}')""")
c.execute("SELECT id, aggregate_id, processed FROM dbo.cdc_inbox WHERE aggregate_id IN ('AGG002','AGG003','AGG004') ORDER BY id")
for r in c.fetchall():
    print(f"  cdc_inbox: id={r.id} agg={r.aggregate_id} processed={r.processed}")
c.execute("SELECT id, agg_id, payload FROM ##test_legacy_target ORDER BY id")
print("  legacy target rows:")
for r in c.fetchall():
    print(f"    inbox_id={r.id} agg={r.agg_id} payload={r.payload}")

print("\n" + "="*70)
print("[4.5] Test error: wrapper que lanza excepcion -> registrado en errors")
print("="*70)
c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_inbox_FAIL_TEST
    @inbox_id BIGINT,
    @aggregate_id NVARCHAR(200),
    @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200),
    @event_type NVARCHAR(50),
    @payload NVARCHAR(MAX)
AS
BEGIN
    THROW 50000, 'simulacion de error de wrapper', 1;
END
""")
c.execute("""DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type='_FAIL_TYPE'""")
c.execute("""INSERT INTO dbo.cdc_inbox_module_config
             (aggregate_type, sp_name, target_db, module_name, active)
             VALUES ('_FAIL_TYPE', 'dbo.usp_inbox_FAIL_TEST', 'tempdb', 'TEST', 1)""")

err_before = 0
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
err_before = c.fetchone()[0]

c.execute("""INSERT INTO dbo.cdc_inbox
             (aggregate_id, aggregate_type, event_type, payload)
             VALUES ('FAIL001', '_FAIL_TYPE', 'INSERT', '{}')""")
c.execute("SELECT TOP 1 id, processed FROM dbo.cdc_inbox WHERE aggregate_id='FAIL001'")
r = c.fetchone()
print(f"  FAIL row: id={r.id} processed={r.processed}")
c.execute("SELECT inbox_id, error_message FROM dbo.cdc_inbox_errors WHERE inbox_id=?", r.id)
for er in c.fetchall():
    print(f"  error logged: inbox_id={er.inbox_id} msg={er.error_message[:200]}")

c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
err_after = c.fetchone()[0]
print(f"  errores total: antes={err_before} despues={err_after} delta={err_after-err_before}")

print("\n" + "="*70)
print("[4.6] Cleanup tests")
print("="*70)
c.execute("DELETE FROM dbo.cdc_inbox WHERE aggregate_id LIKE 'AGG%' OR aggregate_id='FAIL001'")
c.execute("DELETE FROM dbo.cdc_inbox_errors WHERE inbox_id NOT IN (SELECT id FROM dbo.cdc_inbox)")
c.execute("DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type IN ('_TEST_TYPE','_FAIL_TYPE')")
c.execute("DROP PROCEDURE dbo.usp_inbox_TEST_PRUEBA")
c.execute("DROP PROCEDURE dbo.usp_inbox_FAIL_TEST")
c.execute("IF OBJECT_ID('tempdb..##test_legacy_target') IS NOT NULL DROP TABLE ##test_legacy_target")
print("  ok")

c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
print(f"  cdc_inbox final: {c.fetchone()[0]} filas")
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
print(f"  cdc_inbox_errors final: {c.fetchone()[0]} filas")

print("\n=== PASO 4 OK ===")
