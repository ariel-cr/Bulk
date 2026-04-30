CREATE OR ALTER PROCEDURE dbo.usp_inbox_documentacionAfiliadoType
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

        EXEC dbFC.dbo.sp_DOCUMENTACIONAFILIADO_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @ci_cedu = JSON_VALUE(@payload,'$.IDENTIFICACION'),
        @co_inst = JSON_VALUE(@payload,'$.CODIGOINSTITUCION'),
        @st_regi = TRY_CAST(JSON_VALUE(@payload,'$.ESTADO') AS SMALLINT),
        @fe_ingr = TRY_CAST(JSON_VALUE(@payload,'$.FECHAINGRESO') AS DATETIME),
        @fe_naci = TRY_CAST(JSON_VALUE(@payload,'$.FECHAELIMINACION') AS DATETIME),
        @ds_amie = JSON_VALUE(@payload,'$.VIAAUTORIZACIONTRATAMIENTODATOS'),
        @fe_elim = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOELIMINA') AS DATETIME),
        @FE_VERI = TRY_CAST(JSON_VALUE(@payload,'$.INDICADORVERIFICACION') AS DATETIME),
        @fe_modi = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOMODIFICA') AS DATETIME),
        @in_afil = JSON_VALUE(@payload,'$.POSEECARTAANTIGUAAFILIACION');
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper documentacionAfiliadoType: ' + ERROR_MESSAGE());
    END CATCH
END