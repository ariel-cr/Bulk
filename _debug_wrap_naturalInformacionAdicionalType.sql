CREATE OR ALTER PROCEDURE dbo.usp_inbox_naturalInformacionAdicionalType
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
        DECLARE @pk CHAR(1) = CAST(JSON_VALUE(@payload,'$.CODIGOTIPOIDENTIFICACION') AS CHAR(1));
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

        EXEC dbNO.dbo.sp_NATURALINFORMACIONADICIONALTYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @fe_naci = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOPAISNACIMIENTO') AS DATETIME),
        @co_carg = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROCARGAS') AS SMALLINT),
        @co_prof = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOPROFESION') AS SMALLINT),
        @fe_ingr = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOFUENTEINGRESO') AS DATETIME),
        @co_empl = TRY_CAST(JSON_VALUE(@payload,'$.CANTIDADEMPLEADOS') AS SMALLINT),
        @no_sect = JSON_VALUE(@payload,'$.RELACIONSECTORPUBLICO');
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper naturalInformacionAdicionalType: ' + ERROR_MESSAGE());
    END CATCH
END