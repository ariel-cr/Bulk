CREATE OR ALTER PROCEDURE dbo.usp_inbox_actualizacionDocumentosType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.SECUENCIA_ACTU_DOCS');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin SECUENCIA_ACTU_DOCS');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk INT = TRY_CAST(JSON_VALUE(@payload,'$.SECUENCIA_ACTU_DOCS') AS INT);
        IF @pk IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'PK no convertible: ' + @pk_str);
            RETURN;
        END

        -- Audit
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @pk_str,
                JSON_VALUE(@payload,'$.SECUENCIA_ACTU_DOCS'));

        EXEC dbFC.dbo.sp_ACTUALIZACION_DOCUMENTOS_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @co_empr = TRY_CAST(JSON_VALUE(@payload,'$.CODIGO_EMPRESA') AS SMALLINT),
        @co_cedu = JSON_VALUE(@payload,'$.CODIGO_CEDU'),
        @sc_actv_suje_cred = TRY_CAST(JSON_VALUE(@payload,'$.SECUENCIA_ACTV_SUJE_CRED') AS SMALLINT),
        @sc_orgn_ingr = TRY_CAST(JSON_VALUE(@payload,'$.SECUENCIA_ORGN_INGR') AS SMALLINT),
        @co_pers_poli_expu = TRY_CAST(JSON_VALUE(@payload,'$.CODIGO_PERS_POLI_EXPU') AS SMALLINT),
        @ds_ciud_naci = JSON_VALUE(@payload,'$.DESCRIPCION_CIUD_NACI'),
        @in_comi_serv = JSON_VALUE(@payload,'$.INDICADOR_COMI_SERV'),
        @ds_comi_serv = JSON_VALUE(@payload,'$.DESCRIPCION_COMI_SERV'),
        @fx_ingr = TRY_CAST(JSON_VALUE(@payload,'$.FECHA_INGR') AS DATETIME);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper actualizacionDocumentosType: ' + ERROR_MESSAGE());
    END CATCH
END