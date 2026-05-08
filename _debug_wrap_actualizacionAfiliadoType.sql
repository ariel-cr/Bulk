CREATE OR ALTER PROCEDURE dbo.usp_inbox_actualizacionAfiliadoType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.CODIGO_CEDU');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin CODIGO_CEDU');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk VARCHAR(10) = CAST(JSON_VALUE(@payload,'$.CODIGO_CEDU') AS VARCHAR(10));
        IF @pk IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'PK no convertible: ' + @pk_str);
            RETURN;
        END

        -- Audit
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @pk_str,
                JSON_VALUE(@payload,'$.CODIGO_CEDU'));

        EXEC dbFC.dbo.sp_ACTUALIZACION_AFILIADO_TYPE_CRUD
            @Accion = @accion,
            @PK = @pk,
        @co_prov = JSON_VALUE(@payload,'$.CODIGO_PROV'),
        @co_cant = JSON_VALUE(@payload,'$.CODIGO_CANT'),
        @co_parr = JSON_VALUE(@payload,'$.CODIGO_PARR'),
        @ds_call_prim = JSON_VALUE(@payload,'$.DESCRIPCION_CALL_PRIM'),
        @nu_call_prim = JSON_VALUE(@payload,'$.NUMERO_CALL_PRIM'),
        @ds_call_secu = JSON_VALUE(@payload,'$.DESCRIPCION_CALL_SECU'),
        @nu_call_secu = JSON_VALUE(@payload,'$.NUMERO_CALL_SECU'),
        @nu_manz = JSON_VALUE(@payload,'$.NUMERO_MANZ'),
        @nu_vill = JSON_VALUE(@payload,'$.NUMERO_VILL'),
        @ds_cdla = JSON_VALUE(@payload,'$.DESCRIPCION_CDLA'),
        @tx_telf_conv = JSON_VALUE(@payload,'$.TEXTO_TELF_CONVENIO'),
        @tx_telf_celu = JSON_VALUE(@payload,'$.TEXTO_TELF_CELU'),
        @ti_oper = TRY_CAST(JSON_VALUE(@payload,'$.TIPO_OPERACION') AS SMALLINT),
        @tx_mail = JSON_VALUE(@payload,'$.TEXTO_MAIL'),
        @ti_cont = JSON_VALUE(@payload,'$.NOMBRE_CONTABLE_ADIC'),
        @tx_telf_con1 = JSON_VALUE(@payload,'$.TEXTO_TELF_CON1'),
        @tx_telf_con2 = JSON_VALUE(@payload,'$.TEXTO_TELF_CON2'),
        @ti_rela = TRY_CAST(JSON_VALUE(@payload,'$.TIPO_RELA') AS SMALLINT),
        @ci_tipo = JSON_VALUE(@payload,'$.CODIGO_TIPO'),
        @co_inst = JSON_VALUE(@payload,'$.CODIGO_INST'),
        @co_carg = JSON_VALUE(@payload,'$.CODIGO_CARG'),
        @co_nive = JSON_VALUE(@payload,'$.CODIGO_NIVE'),
        @co_cate = JSON_VALUE(@payload,'$.CODIGO_CATE'),
        @ti_jorn = JSON_VALUE(@payload,'$.TIPO_JORN'),
        @co_zona_obsq = JSON_VALUE(@payload,'$.CODIGO_ZONA_OBSQ'),
        @in_reno_cred = JSON_VALUE(@payload,'$.INDICADOR_RENO_CREDITO'),
        @in_acci = TRY_CAST(JSON_VALUE(@payload,'$.INDICADOR_ACCI') AS SMALLINT),
        @fe_ingr = TRY_CAST(JSON_VALUE(@payload,'$.FECHA_INGRESO') AS DATETIME),
        @fe_modi = TRY_CAST(JSON_VALUE(@payload,'$.FECHA_MODIFICACION') AS DATETIME),
        @fe_ultm_envi = TRY_CAST(JSON_VALUE(@payload,'$.FECHA_ULTM_ENVI') AS DATETIME),
        @in_impr_docu = JSON_VALUE(@payload,'$.INDICADOR_IMPR_DOCUMENTO'),
        @in_vald_celu = JSON_VALUE(@payload,'$.INDICADOR_VALD_CELU'),
        @in_vald_mail = JSON_VALUE(@payload,'$.INDICADOR_VALD_MAIL'),
        @st_entr_obsq = JSON_VALUE(@payload,'$.ESTADO_ENTR_OBSQ'),
        @fe_entr_obsq = TRY_CAST(JSON_VALUE(@payload,'$.FECHA_ENTR_OBSQ') AS DATETIME),
        @fe_veri_dato = TRY_CAST(JSON_VALUE(@payload,'$.FECHA_VERI_DATO') AS DATETIME),
        @no_inst = JSON_VALUE(@payload,'$.NOMBRE_INST');
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper actualizacionAfiliadoType: ' + ERROR_MESSAGE());
    END CATCH
END