CREATE OR ALTER PROCEDURE dbo.usp_inbox_agendaMailAfiliadoType
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

        EXEC dbFC.dbo.sp_AGENDAMAILAFILIADO_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @ci_cedu = JSON_VALUE(@payload,'$.CODIGOCEDU'),
        @sc_regi = TRY_CAST(JSON_VALUE(@payload,'$.SECUENCIAREGISTRO') AS SMALLINT),
        @ds_mail = JSON_VALUE(@payload,'$.DESCRIPCIONMAIL'),
        @in_prin = JSON_VALUE(@payload,'$.INDICADORPRIN'),
        @co_usua_ingr = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOINGRESA') AS INT),
        @fe_ingr = TRY_CAST(JSON_VALUE(@payload,'$.FECHAINGRESO') AS DATETIME),
        @co_usua_modi = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOMODIFICA') AS INT),
        @fe_modi = TRY_CAST(JSON_VALUE(@payload,'$.FECHAMODIFICACION') AS DATETIME),
        @fe_elim = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOUSUELIM') AS DATETIME),
        @st_regi = TRY_CAST(JSON_VALUE(@payload,'$.ESTADOREGISTRO') AS SMALLINT);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper agendaMailAfiliadoType: ' + ERROR_MESSAGE());
    END CATCH
END