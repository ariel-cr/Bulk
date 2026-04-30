CREATE OR ALTER PROCEDURE dbo.usp_inbox_auditoriaAfiliadoType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.IDENTIFICACION');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin IDENTIFICACION');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk CHAR(10) = CAST(JSON_VALUE(@payload,'$.IDENTIFICACION') AS CHAR(10));
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

        EXEC dbCT.dbo.sp_AUDITORIAAFILIADO_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @ds_audi = JSON_VALUE(@payload,'$.SECUENCIAAUDITORIA'),
        @ci_camp = TRY_CAST(JSON_VALUE(@payload,'$.CAMPOMODIFICADO') AS INT),
        @co_usua = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOUSUARIO') AS SMALLINT),
        @fe_crea = TRY_CAST(JSON_VALUE(@payload,'$.FECHACREACION') AS DATETIME),
        @ci_moti = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOMOTIVOMANTENIMIENTO') AS SMALLINT);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper auditoriaAfiliadoType: ' + ERROR_MESSAGE());
    END CATCH
END