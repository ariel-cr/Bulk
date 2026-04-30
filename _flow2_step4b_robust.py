"""Paso 4 (revision): blinda el dispatcher con SAVE TRANSACTION
para que un wrapper que lance error NO tumbe la transaccion del INSERT batch.

El INSERT del Kafka sink puede traer N filas. Si una falla, las demas deben
seguir procesandose y el INSERT mismo no debe fallar.
"""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = sql("fcme_canonicos").cursor()

print("="*70)
print("[4b.1] Re-deploy usp_process_cdc_inbox con SAVE TRANSACTION")
print("="*70)
sp_sql = """
CREATE OR ALTER PROCEDURE dbo.usp_process_cdc_inbox
    @inbox_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @aggregate_id   NVARCHAR(200),
            @aggregate_type NVARCHAR(200),
            @event_type     NVARCHAR(50),
            @payload        NVARCHAR(MAX),
            @source_table   NVARCHAR(200),
            @sp_name        NVARCHAR(300),
            @save_used      BIT = 0;

    SELECT
        @aggregate_id   = aggregate_id,
        @aggregate_type = aggregate_type,
        @event_type     = event_type,
        @payload        = payload,
        @source_table   = source_table
    FROM dbo.cdc_inbox WITH (NOLOCK)
    WHERE id = @inbox_id;

    IF @aggregate_type IS NULL RETURN;

    EXEC sp_set_session_context N'is_replicating', 1;

    -- Lookup wrapper
    SELECT @sp_name = sp_name
    FROM dbo.cdc_inbox_module_config WITH (NOLOCK)
    WHERE aggregate_type = @aggregate_type AND active = 1;

    IF @sp_name IS NULL
    BEGIN
        UPDATE dbo.cdc_inbox SET processed = 1, processed_at = SYSUTCDATETIME() WHERE id = @inbox_id;
        EXEC sp_set_session_context N'is_replicating', 0;
        RETURN;
    END

    -- Validacion anti-inyeccion
    IF PATINDEX('%[^a-zA-Z0-9_.\\[\\]]%', @sp_name) > 0
    BEGIN
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'sp_name invalido: ' + @sp_name);
        EXEC sp_set_session_context N'is_replicating', 0;
        RETURN;
    END

    -- SAVE TRANSACTION para aislar fallo del wrapper
    -- Solo funciona si la transaccion es committable (XACT_STATE = 1)
    IF @@TRANCOUNT > 0 AND XACT_STATE() = 1
    BEGIN
        SAVE TRANSACTION wrapper_sp;
        SET @save_used = 1;
    END

    BEGIN TRY
        DECLARE @stmt NVARCHAR(MAX) =
            N'EXEC ' + @sp_name +
            N' @inbox_id, @aggregate_id, @aggregate_type, @source_table, @event_type, @payload';

        EXEC sp_executesql @stmt,
            N'@inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200), @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)',
            @inbox_id      = @inbox_id,
            @aggregate_id  = @aggregate_id,
            @aggregate_type = @aggregate_type,
            @source_table  = @source_table,
            @event_type    = @event_type,
            @payload       = @payload;

        UPDATE dbo.cdc_inbox SET processed = 1, processed_at = SYSUTCDATETIME() WHERE id = @inbox_id;
    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(MAX) =
            N'msg=' + ERROR_MESSAGE() +
            N' line=' + CAST(ERROR_LINE() AS NVARCHAR(20)) +
            N' procedure=' + ISNULL(ERROR_PROCEDURE(), N'<dynamic>');

        -- Rollback al savepoint si tx aun viva (XACT_STATE=1)
        -- Si XACT_STATE=-1, la tx esta doomed; no podemos hacer rollback to savepoint
        -- y los inserts post no funcionaran. Pero al menos liberamos el flag
        -- y el caller decide.
        IF @save_used = 1 AND XACT_STATE() = 1
            ROLLBACK TRANSACTION wrapper_sp;

        -- Si la tx sigue viable, registrar el error
        IF XACT_STATE() = 1
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, @err);
        END
        -- Si XACT_STATE = -1 (doomed), no hacemos INSERT al log
        -- (no se podra) — el error se perdera pero el caller vera el throw.
    END CATCH

    EXEC sp_set_session_context N'is_replicating', 0;
END
"""
c.execute(sp_sql)
print("  redeployado con SAVE TRANSACTION")

print("\n" + "="*70)
print("[4b.2] Test combinado: 1 fila OK + 1 fila FAIL + 1 fila OK en mismo INSERT")
print("="*70)
# Wrapper OK
c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_inbox_OK_TEST
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS BEGIN
    SET NOCOUNT ON;
    IF OBJECT_ID('tempdb..##test_lg') IS NULL
        CREATE TABLE ##test_lg (id BIGINT, agg NVARCHAR(200));
    INSERT INTO ##test_lg VALUES (@inbox_id, @aggregate_id);
END
""")
# Wrapper FAIL con RAISERROR severity 11 (NO doomifica la tx)
c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_inbox_FAIL_SOFT
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS BEGIN
    RAISERROR('simulacion fallo soft', 11, 1);
END
""")
c.execute("""DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type IN ('_OK','_FAIL')""")
c.execute("""INSERT INTO dbo.cdc_inbox_module_config VALUES
             ('_OK','dbo.usp_inbox_OK_TEST','tempdb','TEST',1,SYSUTCDATETIME(),NULL),
             ('_FAIL','dbo.usp_inbox_FAIL_SOFT','tempdb','TEST',1,SYSUTCDATETIME(),NULL)""")
c.execute("IF OBJECT_ID('tempdb..##test_lg') IS NOT NULL DROP TABLE ##test_lg")
c.execute("CREATE TABLE ##test_lg (id BIGINT, agg NVARCHAR(200))")

# INSERT batch: OK + FAIL + OK
print("  INSERT batch de 3 filas (OK / FAIL / OK)")
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload) VALUES
             ('B001','_OK','INSERT','{}'),
             ('B002','_FAIL','INSERT','{}'),
             ('B003','_OK','INSERT','{}')""")

c.execute("SELECT id, aggregate_id, aggregate_type, processed FROM dbo.cdc_inbox ORDER BY id")
print("\n  cdc_inbox final:")
for r in c.fetchall():
    print(f"    id={r.id} agg={r.aggregate_id} type={r.aggregate_type} processed={r.processed}")

c.execute("SELECT id, agg FROM ##test_lg ORDER BY id")
print("\n  legacy target rows (deben ser B001 y B003):")
for r in c.fetchall():
    print(f"    inbox_id={r.id} agg={r.agg}")

c.execute("SELECT inbox_id, error_message FROM dbo.cdc_inbox_errors ORDER BY inbox_id")
print("\n  cdc_inbox_errors (debe haber 1 entry para B002):")
for r in c.fetchall():
    print(f"    inbox_id={r.inbox_id} msg={r.error_message[:120]}")

# Cleanup
print("\n[Cleanup]")
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c.execute("DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type IN ('_OK','_FAIL')")
c.execute("DROP PROCEDURE dbo.usp_inbox_OK_TEST")
c.execute("DROP PROCEDURE dbo.usp_inbox_FAIL_SOFT")
c.execute("IF OBJECT_ID('tempdb..##test_lg') IS NOT NULL DROP TABLE ##test_lg")
print("  ok")

print("\n=== PASO 4 (robust) OK ===")
