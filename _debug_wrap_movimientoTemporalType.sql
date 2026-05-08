CREATE OR ALTER PROCEDURE dbo.usp_inbox_movimientoTemporalType
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
                JSON_VALUE(@payload,'$.CODIGOEMPRESA'));

        EXEC dbFC.dbo.sp_MOVIMIENTOTEMPORAL_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @ci_institucion = JSON_VALUE(@payload,'$.CODIGOINSTITUCION'),
        @ci_motivo = JSON_VALUE(@payload,'$.CODIGOMOTIVORETIRO'),
        @ci_provincia = JSON_VALUE(@payload,'$.CODIGOPROVINCIA'),
        @fe_veri = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOUSUARIOVERIFICA') AS DATETIME),
        @ds_movi = JSON_VALUE(@payload,'$.DESCRIPCIONMOVIMIENTO'),
        @ce_estado = JSON_VALUE(@payload,'$.ESTADOMOVIMIENTO'),
        @fx_creacion = TRY_CAST(JSON_VALUE(@payload,'$.FECHACREACIONREGISTRO') AS DATETIME),
        @fx_ajuste = TRY_CAST(JSON_VALUE(@payload,'$.FECHAFINAJUSTE') AS DATETIME),
        @fx_proceso = TRY_CAST(JSON_VALUE(@payload,'$.FECHAPROCESO') AS DATETIME),
        @fx_retiro = TRY_CAST(JSON_VALUE(@payload,'$.FECHARETIROFCME') AS DATETIME),
        @qs_hora = TRY_CAST(JSON_VALUE(@payload,'$.HORAGENERACIONMOVIMIENTO') AS INT),
        @ce_capitalizado = JSON_VALUE(@payload,'$.INDICADORMOVIMIENTOCAPITALIZADO'),
        @ci_tipo = JSON_VALUE(@payload,'$.INDICADORTIPOPROCESO'),
        @ci_cedula = JSON_VALUE(@payload,'$.NUMEROCEDULA'),
        @ti_comprobante = JSON_VALUE(@payload,'$.NUMEROCOMPROBANTECONTABLE'),
        @ci_transaccion = JSON_VALUE(@payload,'$.NUMEROTRANSACCION'),
        @ci_pagador = JSON_VALUE(@payload,'$.CODIGOPAGADOR'),
        @pr_porcentaje = TRY_CAST(JSON_VALUE(@payload,'$.PORCENTAJEDISTRIBUCIONVALORES') AS DECIMAL(18,6)),
        @co_usua_ingr = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOINGRESA') AS SMALLINT);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper movimientoTemporalType: ' + ERROR_MESSAGE());
    END CATCH
END