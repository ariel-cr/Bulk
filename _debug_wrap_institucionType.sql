CREATE OR ALTER PROCEDURE dbo.usp_inbox_institucionType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.CODIGOTIPOINSTITUCION');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin CODIGOTIPOINSTITUCION');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk CHAR(1) = CAST(JSON_VALUE(@payload,'$.CODIGOTIPOINSTITUCION') AS CHAR(1));
        IF @pk IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'PK no convertible: ' + @pk_str);
            RETURN;
        END

        -- Audit
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @pk_str,
                JSON_VALUE(@payload,'$.CODIGOINSTITUCION'));

        EXEC dbFC.dbo.sp_INSTITUCION_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @ci_institucion = JSON_VALUE(@payload,'$.CODIGOINSTITUCION'),
        @ci_provincia = JSON_VALUE(@payload,'$.CODIGOPROVINCIA'),
        @ci_ciudad = JSON_VALUE(@payload,'$.CODIGOCIUDAD'),
        @no_direccion = JSON_VALUE(@payload,'$.DIRECCION'),
        @nu_telefono = JSON_VALUE(@payload,'$.TELEFONO'),
        @ce_estado = JSON_VALUE(@payload,'$.ESTADO'),
        @fx_creacion = TRY_CAST(JSON_VALUE(@payload,'$.FECHAINGRESO') AS DATETIME),
        @ci_parroquia = JSON_VALUE(@payload,'$.CODIGOPARROQUIA'),
        @co_amie = JSON_VALUE(@payload,'$.CODIGOAMIE'),
        @co_dist = TRY_CAST(JSON_VALUE(@payload,'$.CODIGODISTRITO') AS INT),
        @co_circ = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOCIRCUITO') AS INT),
        @co_sect = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOSECTOR') AS INT),
        @ti_nivel = TRY_CAST(JSON_VALUE(@payload,'$.NIVEL') AS SMALLINT),
        @in_jornada = TRY_CAST(JSON_VALUE(@payload,'$.JORNADACLASES') AS SMALLINT),
        @ci_patronal = JSON_VALUE(@payload,'$.NUMEROPATRONAL'),
        @no_colector = JSON_VALUE(@payload,'$.NOMBRECOLECTOR'),
        @fx_modificacion = TRY_CAST(JSON_VALUE(@payload,'$.FECHAMODIFICACION') AS DATETIME),
        @co_usua_modi = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOMODIFICA') AS SMALLINT),
        @nu_zona = TRY_CAST(JSON_VALUE(@payload,'$.NUMEROZONA') AS SMALLINT),
        @co_usua_ingr = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOINGRESA') AS SMALLINT),
        @co_empr = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOEMPRESA') AS SMALLINT);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper institucionType: ' + ERROR_MESSAGE());
    END CATCH
END