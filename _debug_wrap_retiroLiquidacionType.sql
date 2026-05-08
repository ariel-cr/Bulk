CREATE OR ALTER PROCEDURE dbo.usp_inbox_retiroLiquidacionType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.CODIGOEMPRESA');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin CODIGOEMPRESA');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk SMALLINT = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOEMPRESA') AS SMALLINT);
        IF @pk IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'PK no convertible: ' + @pk_str);
            RETURN;
        END

        -- Audit
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @pk_str,
                JSON_VALUE(@payload,'$.CODIGOTIPOIDENTIFICACION'));

        EXEC dbFC.dbo.sp_RETIROLIQUIDACION_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @ci_tipo = JSON_VALUE(@payload,'$.CODIGOTIPOIDENTIFICACION'),
        @ci_cedula = JSON_VALUE(@payload,'$.IDENTIFICACION'),
        @ci_retiro = TRY_CAST(JSON_VALUE(@payload,'$.SECUENCIARETIRO') AS INT),
        @fx_ingreso = TRY_CAST(JSON_VALUE(@payload,'$.FECHAINGRESO') AS DATETIME),
        @ci_institucion = JSON_VALUE(@payload,'$.CODIGOINSTITUCION'),
        @ci_provincia = JSON_VALUE(@payload,'$.CODIGOPROVINCIA'),
        @ci_pagador = JSON_VALUE(@payload,'$.CODIGOPAGADOR'),
        @va_credito = TRY_CAST(JSON_VALUE(@payload,'$.VALORCREDITO') AS DECIMAL(18,6)),
        @va_acciones = TRY_CAST(JSON_VALUE(@payload,'$.VALORINTERESACCIONES') AS DECIMAL(18,6)),
        @ci_motivo = JSON_VALUE(@payload,'$.MOTIVO'),
        @fe_conf = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOUSUARIOCONFIRMA') AS DATETIME),
        @fe_elim = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOELIMINA') AS DATETIME),
        @co_fond = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOFONDO') AS SMALLINT),
        @va_adic = TRY_CAST(JSON_VALUE(@payload,'$.VALORADICIONAL') AS DECIMAL(18,6)),
        @va_rete = TRY_CAST(JSON_VALUE(@payload,'$.VALORRETENCION') AS DECIMAL(18,6)),
        @va_sobr = TRY_CAST(JSON_VALUE(@payload,'$.SOBRANTEQUESELIQUIDA') AS DECIMAL(18,6)),
        @va_gast = TRY_CAST(JSON_VALUE(@payload,'$.MONTODESCUENTOGASTOSJUBILACION') AS DECIMAL(18,6)),
        @va_aporte = TRY_CAST(JSON_VALUE(@payload,'$.APORTEROLPARAAPERTURACUP') AS DECIMAL(18,6)),
        @co_orig = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOAPLICACIONORIGEN') AS SMALLINT),
        @fx_proceso = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOPROCESO') AS DATETIME);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper retiroLiquidacionType: ' + ERROR_MESSAGE());
    END CATCH
END