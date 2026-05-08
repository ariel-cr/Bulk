CREATE OR ALTER PROCEDURE dbo.usp_inbox_distribucionAfiliadoType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.CIRCUITO');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin CIRCUITO');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk VARCHAR(50) = CAST(JSON_VALUE(@payload,'$.CIRCUITO') AS VARCHAR(50));
        IF @pk IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'PK no convertible: ' + @pk_str);
            RETURN;
        END

        -- Audit
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @pk_str,
                JSON_VALUE(@payload,'$.CIRCUITO'));

        EXEC dbCT.dbo.sp_DISTRIBUCIONAFILIADO_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @Ciudad = JSON_VALUE(@payload,'$.CIUDAD'),
        @Distrito = JSON_VALUE(@payload,'$.DISTRITO'),
        @Estado = JSON_VALUE(@payload,'$.ESTADODISTRIBUCIONAFILIADO'),
        @Provincia = JSON_VALUE(@payload,'$.NOMBREPROVINCIA'),
        @NumeroAfiliado = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROAFILIADO') AS INT),
        @NumeroAfiliadoActualizado = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROAFILIADOACTUALIZADO') AS INT),
        @NumeroCADB = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROCADB') AS INT),
        @NumeroCAP = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROCAP') AS INT),
        @NumeroCreditoVigente = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROCREDITOVIGENTE') AS INT),
        @NumeroDirectivoNacional2008 = TRY_CAST(JSON_VALUE(@payload,'$.NUMERODIRECTIVONACIONAL2008') AS INT),
        @NumeroDirectivoNacional2010 = TRY_CAST(JSON_VALUE(@payload,'$.NUMERODIRECTIVONACIONAL2010') AS INT),
        @NumeroDirectivoProvincial2008 = TRY_CAST(JSON_VALUE(@payload,'$.NUMERODIRECTIVOPROVINCIAL2008') AS INT),
        @NumeroDirectivoProvincial2010 = TRY_CAST(JSON_VALUE(@payload,'$.NUMERODIRECTIVOPROVINCIAL2010') AS INT),
        @NumeroEjecutivoFinanciero = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROEJECUTIVOFINANCIERO') AS INT),
        @NumeroInstituciones = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROINSTITUCIONES') AS INT),
        @NumeroPresidenteEjecutivo2008 = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROPRESIDENTEEJECUTIVO2008') AS INT),
        @NumeroPresidenteEjecutivo2010 = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROPRESIDENTEEJECUTIVO2010') AS INT),
        @Parroquia = JSON_VALUE(@payload,'$.DESCRIPCIONPARROQUIA'),
        @trabajo = TRY_CAST(JSON_VALUE(@payload,'$.TRABAJO') AS INT),
        @Zona = JSON_VALUE(@payload,'$.ZONA');
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper distribucionAfiliadoType: ' + ERROR_MESSAGE());
    END CATCH
END