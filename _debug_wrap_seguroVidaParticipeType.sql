CREATE OR ALTER PROCEDURE dbo.usp_inbox_seguroVidaParticipeType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.CODIGOSECUENCIACAUSAFALLECIMIENTO');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin CODIGOSECUENCIACAUSAFALLECIMIENTO');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk SMALLINT = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOSECUENCIACAUSAFALLECIMIENTO') AS SMALLINT);
        IF @pk IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'PK no convertible: ' + @pk_str);
            RETURN;
        END

        -- Audit
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @pk_str,
                JSON_VALUE(@payload,'$.CODIGOSECUENCIACAUSAFALLECIMIENTO'));

        EXEC dbSV.dbo.sp_SEGUROVIDAPARTICIPE_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @co_usua_ingr = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOINGRESA') AS SMALLINT),
        @co_prov = JSON_VALUE(@payload,'$.EDADPROVEEDOR'),
        @co_afil = JSON_VALUE(@payload,'$.FECHAAFILIACIONCAM'),
        @co_disc = TRY_CAST(JSON_VALUE(@payload,'$.CODIGODISCAPACIDADFAMILIARES') AS SMALLINT),
        @co_efec = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOEFECTO') AS SMALLINT),
        @co_banco = JSON_VALUE(@payload,'$.CODIGOBANCOELCUALREALIZAPAGO'),
        @co_empr = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOEMPRESA') AS SMALLINT),
        @fe_conf = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOUSUARIOCONFIRMA') AS DATETIME),
        @sc_sine = TRY_CAST(JSON_VALUE(@payload,'$.ESTADOSINESTRO') AS INT),
        @fe_elim = TRY_CAST(JSON_VALUE(@payload,'$.FECHAELIMINACION') AS DATETIME),
        @fe_fall = TRY_CAST(JSON_VALUE(@payload,'$.FECHAFALLECIMIENTO') AS DATETIME),
        @fe_noti = TRY_CAST(JSON_VALUE(@payload,'$.FECHANOTIFICACIONSINIESTRO') AS DATETIME),
        @fe_pres = TRY_CAST(JSON_VALUE(@payload,'$.FECHAPRESENTACIONPAPELESSINIESTRADO') AS DATETIME);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper seguroVidaParticipeType: ' + ERROR_MESSAGE());
    END CATCH
END