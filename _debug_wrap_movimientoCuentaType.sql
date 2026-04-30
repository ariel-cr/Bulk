CREATE OR ALTER PROCEDURE dbo.usp_inbox_movimientoCuentaType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.CODIGOTIPOIDENTIFICACION');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin CODIGOTIPOIDENTIFICACION');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk VARCHAR(1) = CAST(JSON_VALUE(@payload,'$.CODIGOTIPOIDENTIFICACION') AS VARCHAR(1));
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

        EXEC dbFC.dbo.sp_MOVIMIENTOCUENTA_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @ci_cedula = JSON_VALUE(@payload,'$.IDENTIFICACION'),
        @ci_motivo = JSON_VALUE(@payload,'$.CODIGOMOTIVO'),
        @fx_proceso = TRY_CAST(JSON_VALUE(@payload,'$.FECHAPROCESO') AS DATETIME),
        @ce_estado = JSON_VALUE(@payload,'$.ESTADO'),
        @ci_transaccion = JSON_VALUE(@payload,'$.TIPOTRANSACCION'),
        @ci_institucion = JSON_VALUE(@payload,'$.CODIGOINSTITUCION'),
        @fx_ajuste = TRY_CAST(JSON_VALUE(@payload,'$.FECHAINICIOAJUSTE') AS DATETIME),
        @fx_retiro = TRY_CAST(JSON_VALUE(@payload,'$.FECHARETIROFCME') AS DATETIME),
        @pr_porcentaje = TRY_CAST(JSON_VALUE(@payload,'$.PORCENTAJEDISTRIBUCIONVALORES') AS DECIMAL(18,6)),
        @fx_creacion = TRY_CAST(JSON_VALUE(@payload,'$.FECHACREACIONREGISTRO') AS DATETIME),
        @ce_capitalizado = JSON_VALUE(@payload,'$.INDICADORMOVIMIENTOCAPITALIZADO'),
        @qs_hora = TRY_CAST(JSON_VALUE(@payload,'$.HORAGENERACIONMOVIMIENTO') AS INT),
        @ci_provincia = JSON_VALUE(@payload,'$.CODIGOPROVINCIA'),
        @ci_pagador = JSON_VALUE(@payload,'$.CODIGOPAGADOR'),
        @co_empr = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOEMPRESA') AS SMALLINT);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper movimientoCuentaType: ' + ERROR_MESSAGE());
    END CATCH
END