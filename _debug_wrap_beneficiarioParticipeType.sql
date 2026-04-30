CREATE OR ALTER PROCEDURE dbo.usp_inbox_beneficiarioParticipeType
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
                JSON_VALUE(@payload,'$.APELLIDOSBENEFICIARIOS'));

        EXEC dbFC.dbo.sp_BENEFICIARIOPARTICIPE_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @no_beneficiario = JSON_VALUE(@payload,'$.APELLIDOSBENEFICIARIOS'),
        @ci_banco = JSON_VALUE(@payload,'$.CODIGOBANCOPAGO'),
        @st_regi = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOUSUARIOINGRESOREGISTRO') AS SMALLINT),
        @nu_cuenta = JSON_VALUE(@payload,'$.NUMEROCUENTA'),
        @pr_porcentaje = TRY_CAST(JSON_VALUE(@payload,'$.PORCENTAJEDISTRIBUCIONVALORES') AS DECIMAL(18,6)),
        @va_desembolso = TRY_CAST(JSON_VALUE(@payload,'$.CODIGOBANCODESEMBOLSO') AS DECIMAL(18,6)),
        @fe_elim = TRY_CAST(JSON_VALUE(@payload,'$.FECHAELIMINACION') AS DATETIME),
        @ci_retiro = TRY_CAST(JSON_VALUE(@payload,'$.SECUENCIARETIRO') AS INT),
        @co_usua_ingr = TRY_CAST(JSON_VALUE(@payload,'$.USUARIOINGRESA') AS INT);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper beneficiarioParticipeType: ' + ERROR_MESSAGE());
    END CATCH
END