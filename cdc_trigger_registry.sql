/* ============================================================================
   CDC Trigger Registry - tabla central para administrar todos los triggers CDC
   (F1 SQL Server + F2 Oracle) desde el servicio https://capa.federada:31756/modules/33

   Ubicacion: fcme_canonicos.dbo.cdc_trigger_registry

   Componentes:
     1. Tabla cdc_trigger_registry          (registro de triggers)
     2. SP usp_trigger_set_enabled          (prender/apagar SQL Server, marcar Oracle)
     3. SP usp_trigger_get_state            (obtener estado real desde sys.triggers)
     4. View vw_cdc_triggers_status         (registro + estado real consolidado)
   ============================================================================ */

USE fcme_canonicos;
GO

/* ----------------------------------------------------------------------------
   1) TABLA REGISTRY
   ---------------------------------------------------------------------------- */
IF OBJECT_ID('dbo.cdc_trigger_registry','U') IS NULL
BEGIN
    CREATE TABLE dbo.cdc_trigger_registry (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        engine          NVARCHAR(20)  NOT NULL,                      -- SQL_SERVER | ORACLE
        module_name     NVARCHAR(50)  NOT NULL,                      -- CARTERA, NOMINA, PARTICIPE, ...
        flow            NVARCHAR(2)   NOT NULL,                      -- F1 | F2
        database_name   NVARCHAR(50)  NOT NULL,                      -- dbCR, dbFC, FCME_USER, ...
        schema_name     NVARCHAR(50)  NOT NULL DEFAULT 'dbo',        -- dbo | FCME_USER
        table_name      NVARCHAR(128) NOT NULL,
        trigger_name    NVARCHAR(128) NOT NULL,
        aggregate_types NVARCHAR(MAX) NULL,                          -- JSON array de aggregate_types
        enabled         BIT           NOT NULL DEFAULT 1,
        description     NVARCHAR(500) NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSDATETIME(),
        updated_at      DATETIME2     NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT CK_cdc_trigger_engine  CHECK (engine IN ('SQL_SERVER','ORACLE')),
        CONSTRAINT CK_cdc_trigger_flow    CHECK (flow IN ('F1','F2')),
        CONSTRAINT UQ_cdc_trigger_unique  UNIQUE (engine, database_name, schema_name, trigger_name)
    );

    CREATE INDEX IX_cdc_trigger_registry_module ON dbo.cdc_trigger_registry(module_name, flow);
    CREATE INDEX IX_cdc_trigger_registry_table  ON dbo.cdc_trigger_registry(database_name, table_name);
    CREATE INDEX IX_cdc_trigger_registry_enabled ON dbo.cdc_trigger_registry(enabled);

    PRINT '[OK] Tabla dbo.cdc_trigger_registry creada';
END
ELSE
    PRINT '[SKIP] dbo.cdc_trigger_registry ya existe';
GO

/* ----------------------------------------------------------------------------
   2) SP usp_trigger_set_enabled
      - SQL Server: ejecuta ALTER TABLE ... ENABLE/DISABLE TRIGGER
      - Oracle:     solo marca en el registry (el servicio aplicara ALTER TRIGGER
                    Oracle via su conexion oracledb)
   ---------------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE dbo.usp_trigger_set_enabled
    @trigger_id  INT          = NULL,
    @trigger_name NVARCHAR(128) = NULL,   -- alternativa por nombre
    @enabled      BIT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @engine NVARCHAR(20), @db NVARCHAR(50), @schema NVARCHAR(50),
            @tbl NVARCHAR(128), @trg NVARCHAR(128), @id INT;

    SELECT @id=id, @engine=engine, @db=database_name, @schema=schema_name,
           @tbl=table_name, @trg=trigger_name
    FROM dbo.cdc_trigger_registry
    WHERE (@trigger_id IS NOT NULL AND id = @trigger_id)
       OR (@trigger_name IS NOT NULL AND trigger_name = @trigger_name);

    IF @id IS NULL
    BEGIN
        RAISERROR('Trigger no encontrado en registry', 16, 1);
        RETURN;
    END

    BEGIN TRY
        IF @engine = 'SQL_SERVER'
        BEGIN
            DECLARE @cmd NVARCHAR(500);
            IF @enabled = 1
                SET @cmd = N'USE [' + @db + N']; ENABLE TRIGGER ['
                         + @schema + N'].[' + @trg + N'] ON ['
                         + @schema + N'].[' + @tbl + N']';
            ELSE
                SET @cmd = N'USE [' + @db + N']; DISABLE TRIGGER ['
                         + @schema + N'].[' + @trg + N'] ON ['
                         + @schema + N'].[' + @tbl + N']';
            EXEC sp_executesql @cmd;
        END
        -- ORACLE: el servicio externo aplicara ALTER TRIGGER en Oracle.
        -- Aqui solo persistimos la intencion en el registry.

        UPDATE dbo.cdc_trigger_registry
        SET enabled = @enabled, updated_at = SYSDATETIME()
        WHERE id = @id;

        SELECT id, trigger_name, engine, database_name, table_name, enabled, updated_at
        FROM dbo.cdc_trigger_registry WHERE id = @id;
    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(2048) = ERROR_MESSAGE();
        RAISERROR(@err, 16, 1);
    END CATCH
END;
GO
PRINT '[OK] dbo.usp_trigger_set_enabled creada';

/* ----------------------------------------------------------------------------
   3) SP usp_trigger_get_state
      Devuelve estado real desde sys.triggers (SQL Server) o flag del registry
      (Oracle - el servicio puede consultar all_triggers via su conexion).
   ---------------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE dbo.usp_trigger_get_state
    @trigger_id INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @engine NVARCHAR(20), @db NVARCHAR(50), @schema NVARCHAR(50),
            @tbl NVARCHAR(128), @trg NVARCHAR(128);

    SELECT @engine=engine, @db=database_name, @schema=schema_name,
           @tbl=table_name, @trg=trigger_name
    FROM dbo.cdc_trigger_registry WHERE id = @trigger_id;

    IF @engine IS NULL
    BEGIN
        RAISERROR('Trigger no encontrado', 16, 1);
        RETURN;
    END

    IF @engine = 'SQL_SERVER'
    BEGIN
        DECLARE @sql NVARCHAR(500) = N'
            SELECT TOP 1
                t.name AS trigger_name,
                CASE WHEN t.is_disabled = 0 THEN 1 ELSE 0 END AS real_enabled,
                tb.name AS table_name
            FROM ['+@db+N'].sys.triggers t
            JOIN ['+@db+N'].sys.tables   tb ON tb.object_id = t.parent_id
            WHERE t.name = @trg AND tb.name = @tbl';
        EXEC sp_executesql @sql, N'@trg NVARCHAR(128), @tbl NVARCHAR(128)', @trg=@trg, @tbl=@tbl;
    END
    ELSE
    BEGIN
        SELECT trigger_name, enabled AS real_enabled, table_name
        FROM dbo.cdc_trigger_registry WHERE id = @trigger_id;
    END
END;
GO
PRINT '[OK] dbo.usp_trigger_get_state creada';

/* ----------------------------------------------------------------------------
   4) VIEW vw_cdc_triggers_status
      Para listar todos los triggers en un GET del servicio.
   ---------------------------------------------------------------------------- */
CREATE OR ALTER VIEW dbo.vw_cdc_triggers_status AS
SELECT
    id,
    engine,
    module_name,
    flow,
    database_name,
    schema_name,
    table_name,
    trigger_name,
    aggregate_types,
    enabled,
    description,
    created_at,
    updated_at
FROM dbo.cdc_trigger_registry;
GO
PRINT '[OK] dbo.vw_cdc_triggers_status creada';

/* ----------------------------------------------------------------------------
   5) Carga inicial: poblar el registry con triggers EXISTENTES
      Lee de fcme_canonicos.cdc_inbox_module_config para mapear aggregate_types
      a triggers correspondientes.
   ---------------------------------------------------------------------------- */

PRINT '';
PRINT '--- Carga inicial CARTERA F1 (60 triggers SQL Server) ---';

-- Insertar triggers F1 SQL Server (usar UNION de las 4 BDs cartera)
INSERT INTO dbo.cdc_trigger_registry
    (engine, module_name, flow, database_name, schema_name, table_name, trigger_name, aggregate_types, enabled, description)
SELECT 'SQL_SERVER', 'CARTERA', 'F1', src.db, 'dbo', src.tbl, 'trg_outbox_'+src.tbl,
       NULL, 1, 'CDC F1 outbox trigger'
FROM (
    -- dbCR
    SELECT 'dbCR' AS db, name AS tbl FROM dbCR.sys.tables t
    WHERE EXISTS (SELECT 1 FROM dbCR.sys.triggers tr WHERE tr.parent_id = t.object_id AND tr.name = 'trg_outbox_'+t.name)
    UNION ALL
    SELECT 'dbFC', name FROM dbFC.sys.tables t
    WHERE EXISTS (SELECT 1 FROM dbFC.sys.triggers tr WHERE tr.parent_id = t.object_id AND tr.name = 'trg_outbox_'+t.name)
    UNION ALL
    SELECT 'dbCG', name FROM dbCG.sys.tables t
    WHERE EXISTS (SELECT 1 FROM dbCG.sys.triggers tr WHERE tr.parent_id = t.object_id AND tr.name = 'trg_outbox_'+t.name)
    UNION ALL
    SELECT 'dbCT', name FROM dbCT.sys.tables t
    WHERE EXISTS (SELECT 1 FROM dbCT.sys.triggers tr WHERE tr.parent_id = t.object_id AND tr.name = 'trg_outbox_'+t.name)
) src
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.cdc_trigger_registry r
    WHERE r.database_name = src.db AND r.trigger_name = 'trg_outbox_'+src.tbl
);

PRINT '[OK] F1 SQL Server cartera insertados';

-- F2 Oracle (89 triggers TRG_OUTBOX_<X>) - se leen de cdc_inbox_module_config
-- (1 entry por aggregate_type; varios pueden compartir trigger por abreviacion)
INSERT INTO dbo.cdc_trigger_registry
    (engine, module_name, flow, database_name, schema_name, table_name, trigger_name, aggregate_types, enabled, description)
SELECT DISTINCT
    'ORACLE', m.module_name, 'F2', 'FCME_USER', 'FCME_USER',
    -- nombre tabla destino: derivar del sp_name (USP_INBOX_X -> dest TYPE)
    -- aproximacion: lo inserta el servicio o un script Python aparte (mas exacto)
    REPLACE(REPLACE(m.sp_name, 'USP_INBOX_', ''), 'dbo.','') + '_TYPE',
    'TRG_OUTBOX_' + REPLACE(REPLACE(m.sp_name, 'USP_INBOX_', ''), 'dbo.',''),
    m.aggregate_type,
    CAST(m.active AS BIT),
    'CDC F2 outbox trigger Oracle'
FROM dbo.cdc_inbox_module_config m
WHERE m.module_name = 'CARTERA'
  AND NOT EXISTS (
      SELECT 1 FROM dbo.cdc_trigger_registry r
      WHERE r.engine = 'ORACLE' AND r.aggregate_types = m.aggregate_type
  );

PRINT '[OK] F2 Oracle cartera insertados';

/* ----------------------------------------------------------------------------
   6) Resumen
   ---------------------------------------------------------------------------- */
PRINT '';
PRINT '--- RESUMEN cdc_trigger_registry ---';
SELECT engine, flow, module_name,
       COUNT(*) AS triggers_total,
       SUM(CAST(enabled AS INT)) AS triggers_enabled
FROM dbo.cdc_trigger_registry
GROUP BY engine, flow, module_name
ORDER BY module_name, flow, engine;

PRINT '';
PRINT '=== INSTALACION COMPLETA ===';
PRINT 'Servicio en https://capa.federada:31756/modules/33 puede:';
PRINT '  GET  /triggers              -> SELECT * FROM dbo.vw_cdc_triggers_status';
PRINT '  POST /triggers/{id}/enable  -> EXEC dbo.usp_trigger_set_enabled @trigger_id={id}, @enabled=1';
PRINT '  POST /triggers/{id}/disable -> EXEC dbo.usp_trigger_set_enabled @trigger_id={id}, @enabled=0';
PRINT '  GET  /triggers/{id}/state   -> EXEC dbo.usp_trigger_get_state @trigger_id={id}';
GO
