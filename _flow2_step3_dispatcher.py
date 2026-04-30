"""Paso 3 (Flujo 2): SP generico usp_process_cdc_inbox en fcme_canonicos.

Dado un id de cdc_inbox:
1) Anti-loop: SESSION_CONTEXT('is_replicating', 1) para no re-emitir desde triggers legacy
2) Lee el evento (aggregate_type, source_table, event_type, payload)
3) Busca sp_name en cdc_inbox_module_config WHERE aggregate_type=@aggregate_type AND active=1
4) Valida formato de sp_name (anti-inyeccion)
5) EXEC sp_executesql con la firma estandar
6) Marca processed=1
7) Si error, registra en cdc_inbox_errors
"""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = sql("fcme_canonicos").cursor()

print("="*70)
print("[3.1] Asegurar cdc_inbox_errors")
print("="*70)
c.execute("""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='cdc_inbox_errors')
BEGIN
    CREATE TABLE dbo.cdc_inbox_errors (
        error_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
        inbox_id       BIGINT       NOT NULL,
        aggregate_type NVARCHAR(200) NULL,
        event_type     NVARCHAR(50)  NULL,
        error_message  NVARCHAR(MAX) NULL,
        created_at     DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_cdc_inbox_errors_inbox ON dbo.cdc_inbox_errors(inbox_id);
    PRINT 'tabla cdc_inbox_errors creada';
END
""")

c.execute("""SELECT COUNT(*) FROM sys.tables WHERE name='cdc_inbox_errors'""")
print(f"  cdc_inbox_errors existe: {c.fetchone()[0]>0}")

print("\n" + "="*70)
print("[3.2] Crear SP usp_process_cdc_inbox")
print("="*70)
sp_sql = """
CREATE OR ALTER PROCEDURE dbo.usp_process_cdc_inbox
    @inbox_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @aggregate_id  NVARCHAR(200),
            @aggregate_type NVARCHAR(200),
            @event_type    NVARCHAR(50),
            @payload       NVARCHAR(MAX),
            @source_table  NVARCHAR(200),
            @sp_name       NVARCHAR(300);

    -- 1) Leer evento
    SELECT
        @aggregate_id   = aggregate_id,
        @aggregate_type = aggregate_type,
        @event_type     = event_type,
        @payload        = payload,
        @source_table   = source_table
    FROM dbo.cdc_inbox WITH (NOLOCK)
    WHERE id = @inbox_id;

    IF @aggregate_type IS NULL
    BEGIN
        -- evento no encontrado - nada que hacer
        RETURN;
    END

    -- 2) Anti-loop: marcar la sesion como replicacion para que los triggers
    --    de outbox legacy (trg_outbox_*) hagan RETURN y no re-emitan
    EXEC sp_set_session_context N'is_replicating', 1;

    BEGIN TRY
        -- 3) Lookup wrapper SP
        SELECT @sp_name = sp_name
        FROM dbo.cdc_inbox_module_config WITH (NOLOCK)
        WHERE aggregate_type = @aggregate_type AND active = 1;

        IF @sp_name IS NULL
        BEGIN
            -- type no configurado: marcar processed igual y registrar nota
            UPDATE dbo.cdc_inbox
            SET processed = 1, processed_at = SYSUTCDATETIME()
            WHERE id = @inbox_id;
            RETURN;
        END

        -- 4) Validacion anti-inyeccion: sp_name solo permite [a-zA-Z0-9_.[\\]]
        IF PATINDEX('%[^a-zA-Z0-9_.\\[\\]]%', @sp_name) > 0
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type,
                    N'sp_name invalido: ' + @sp_name);
            RETURN;
        END

        -- 5) EXEC dinamico con parametros tipados
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

        -- 6) Marcar procesado
        UPDATE dbo.cdc_inbox
        SET processed = 1, processed_at = SYSUTCDATETIME()
        WHERE id = @inbox_id;

    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(MAX) =
            N'msg=' + ERROR_MESSAGE() +
            N' line=' + CAST(ERROR_LINE() AS NVARCHAR(20)) +
            N' procedure=' + ISNULL(ERROR_PROCEDURE(), N'<dynamic>');

        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, @err);
    END CATCH

    -- 7) Liberar el flag de replicacion
    EXEC sp_set_session_context N'is_replicating', 0;
END
"""
c.execute(sp_sql)
print("  usp_process_cdc_inbox creado/actualizado")

# Verificar
c.execute("""SELECT o.name, o.type_desc, CASE WHEN m.definition IS NULL THEN 0 ELSE 1 END AS has_def
             FROM sys.objects o LEFT JOIN sys.sql_modules m ON o.object_id=m.object_id
             WHERE o.name='usp_process_cdc_inbox' AND o.type='P'""")
for r in c.fetchall():
    print(f"  {r.name} {r.type_desc}  has_definition={r.has_def}")

print("\n" + "="*70)
print("[3.3] Test del SP con un wrapper dummy")
print("="*70)

# Crear wrapper de prueba
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
    PRINT 'wrapper TEST llamado';
    PRINT '  inbox_id=' + CAST(@inbox_id AS NVARCHAR(20));
    PRINT '  aggregate_type=' + ISNULL(@aggregate_type, '<null>');
    PRINT '  payload=' + ISNULL(@payload, '<null>');
END
""")

# Registrarlo en module_config
c.execute("""DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type='_TEST_TYPE'""")
c.execute("""INSERT INTO dbo.cdc_inbox_module_config
             (aggregate_type, sp_name, target_db, module_name, active)
             VALUES ('_TEST_TYPE', 'dbo.usp_inbox_TEST_PRUEBA', 'dbFC', 'PARTICIPE', 1)""")

# Insertar evento de prueba en cdc_inbox
c.execute("""DELETE FROM dbo.cdc_inbox WHERE aggregate_id='_TEST_001'""")
c.execute("""INSERT INTO dbo.cdc_inbox
             (aggregate_id, aggregate_type, event_type, payload, source_table)
             VALUES ('_TEST_001', '_TEST_TYPE', 'INSERT', '{"k":"v"}', 'TEST_TABLE')""")
c.execute("SELECT id FROM dbo.cdc_inbox WHERE aggregate_id='_TEST_001'")
inbox_id = c.fetchone()[0]
print(f"  evento insertado: id={inbox_id}")

# Invocar dispatcher
c.execute("EXEC dbo.usp_process_cdc_inbox @inbox_id=?", inbox_id)
# Drain print messages
while c.nextset():
    pass

# Verificar processed
c.execute("SELECT processed, processed_at FROM dbo.cdc_inbox WHERE id=?", inbox_id)
r = c.fetchone()
print(f"  processed={r.processed} processed_at={r.processed_at}")

# Verificar errores
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE inbox_id=?", inbox_id)
n_err = c.fetchone()[0]
print(f"  errores: {n_err}")

# Limpiar test
c.execute("DELETE FROM dbo.cdc_inbox WHERE aggregate_id='_TEST_001'")
c.execute("DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type='_TEST_TYPE'")
c.execute("DROP PROCEDURE dbo.usp_inbox_TEST_PRUEBA")
print("  test cleanup ok")

print("\n" + "="*70)
print("[3.4] Test type sin configurar (debe quedar processed sin error)")
print("="*70)
c.execute("""INSERT INTO dbo.cdc_inbox
             (aggregate_id, aggregate_type, event_type, payload, source_table)
             VALUES ('_TEST_002', '_NO_CONFIGURED_TYPE', 'INSERT', '{}', 'X')""")
c.execute("SELECT id FROM dbo.cdc_inbox WHERE aggregate_id='_TEST_002'")
inbox_id2 = c.fetchone()[0]
c.execute("EXEC dbo.usp_process_cdc_inbox @inbox_id=?", inbox_id2)
while c.nextset(): pass
c.execute("SELECT processed FROM dbo.cdc_inbox WHERE id=?", inbox_id2)
print(f"  processed sin SP: {c.fetchone()[0]}")
c.execute("DELETE FROM dbo.cdc_inbox WHERE id=?", inbox_id2)

print("\n=== PASO 3 OK ===")
