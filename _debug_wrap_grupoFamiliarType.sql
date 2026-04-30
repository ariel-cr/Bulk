CREATE OR ALTER PROCEDURE dbo.usp_inbox_grupoFamiliarType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.APELLIDOSGRUPOFAMILIAR');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin APELLIDOSGRUPOFAMILIAR');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk VARCHAR(30) = CAST(JSON_VALUE(@payload,'$.APELLIDOSGRUPOFAMILIAR') AS VARCHAR(30));
        IF @pk IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'PK no convertible: ' + @pk_str);
            RETURN;
        END

        -- Audit
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @pk_str,
                JSON_VALUE(@payload,'$.APELLIDOSGRUPOFAMILIAR'));

        EXEC dbFC.dbo.sp_GRUPOFAMILIAR_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @ci_cedula = JSON_VALUE(@payload,'$.CEDULAFAMILIAR'),
        @ce_familiar = JSON_VALUE(@payload,'$.ESTADOFAMILIAR'),
        @fx_creacion = TRY_CAST(JSON_VALUE(@payload,'$.FECHACREACIONREGISTRO') AS DATETIME),
        @fx_modificacion = TRY_CAST(JSON_VALUE(@payload,'$.FECHAMODIFICACION') AS DATETIME),
        @fx_nacimiento = TRY_CAST(JSON_VALUE(@payload,'$.FECHANACIMIENTO') AS DATETIME),
        @in_discapacidad = JSON_VALUE(@payload,'$.INDICADORDISCAPACIDAD'),
        @no_nombre = JSON_VALUE(@payload,'$.NOMBRESGRUPOFAMILIAR');
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper grupoFamiliarType: ' + ERROR_MESSAGE());
    END CATCH
END