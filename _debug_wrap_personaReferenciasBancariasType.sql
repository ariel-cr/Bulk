CREATE OR ALTER PROCEDURE dbo.usp_inbox_personaReferenciasBancariasType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.NUCEMPRESABANCARIA');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin NUCEMPRESABANCARIA');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk SMALLINT = TRY_CAST(JSON_VALUE(@payload,'$.NUCEMPRESABANCARIA') AS SMALLINT);
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

        EXEC dbFC.dbo.sp_PERSONAREFERENCIASBANCARIASTYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @ci_tipo = JSON_VALUE(@payload,'$.CODIGOTIPOIDENTIFICACION'),
        @ci_cedula = JSON_VALUE(@payload,'$.IDENTIFICACION'),
        @no_nombre = JSON_VALUE(@payload,'$.NOMBRETITULAR'),
        @ci_institucion = JSON_VALUE(@payload,'$.NOMBREINSTITUCIONPARACACEL'),
        @ds_observaciones = JSON_VALUE(@payload,'$.OBSERVACIONESSOLOPARACACEL');
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper personaReferenciasBancariasType: ' + ERROR_MESSAGE());
    END CATCH
END