CREATE OR ALTER PROCEDURE dbo.usp_inbox_retiroVoluntarioEstadoType
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
                JSON_VALUE(@payload,'$.ANIO'));

        EXEC dbFC.dbo.sp_RETIROVOLUNTARIOESTADO_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @nu_anio = TRY_CAST(JSON_VALUE(@payload,'$.ANIO') AS SMALLINT),
        @co_fond = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOFONDO') AS SMALLINT),
        @st_regi = TRY_CAST(JSON_VALUE(@payload,'$.ESTADOREGISTRO') AS SMALLINT),
        @fe_modi = TRY_CAST(JSON_VALUE(@payload,'$.FECHAMODIFICACION') AS DATETIME),
        @sc_deta = TRY_CAST(JSON_VALUE(@payload,'$.SECUENCIADETALLE') AS SMALLINT),
        @co_usua_ingr = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOINGRESA') AS INT),
        @co_usua_modi = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOMODIFICA') AS INT);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper retiroVoluntarioEstadoType: ' + ERROR_MESSAGE());
    END CATCH
END