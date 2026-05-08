CREATE OR ALTER PROCEDURE dbo.usp_inbox_documentacionAfiliadoType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;
    BEGIN TRY
        -- event_type Oracle -> @accion del SP original (UPPERCASE)
        DECLARE @accion VARCHAR(10) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'INSERT'
            WHEN @event_type IN ('UPDATE','U') THEN 'UPDATE'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'DELETE'
            ELSE 'INSERT' END;

        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @aggregate_id, @aggregate_id);

        -- Cada parametro busca el valor en el payload con COALESCE de SNAKE_UPPER y CONCAT_UPPER
        DECLARE @v_codigoTipoIdentificacion INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_TIPO_IDENTIFICACION'), JSON_VALUE(@payload,'$.CODIGOTIPOIDENTIFICACION')) AS INT);
        DECLARE @v_identificacion NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.IDENTIFICACION'), JSON_VALUE(@payload,'$.IDENTIFICACION'));
        DECLARE @v_secuenciaDocumento INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.SECUENCIA_DOCUMENTO'), JSON_VALUE(@payload,'$.SECUENCIADOCUMENTO')) AS INT);
        DECLARE @v_codigoDocumento INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_DOCUMENTO'), JSON_VALUE(@payload,'$.CODIGODOCUMENTO')) AS INT);
        DECLARE @v_fechaFirmaDocumento DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_FIRMA_DOCUMENTO'), JSON_VALUE(@payload,'$.FECHAFIRMADOCUMENTO')) AS DATETIME2);
        DECLARE @v_cedulaUnificada NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.CEDULA_UNIFICADA'), JSON_VALUE(@payload,'$.CEDULAUNIFICADA'));
        DECLARE @v_fechaUnificacion DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_UNIFICACION'), JSON_VALUE(@payload,'$.FECHAUNIFICACION')) AS DATETIME2);
        DECLARE @v_tipoUnificacion NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.TIPO_UNIFICACION'), JSON_VALUE(@payload,'$.TIPOUNIFICACION'));
        DECLARE @v_codigoInstitucion INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_INSTITUCION'), JSON_VALUE(@payload,'$.CODIGOINSTITUCION')) AS INT);
        DECLARE @v_fechaFirmaCarta DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_FIRMA_CARTA'), JSON_VALUE(@payload,'$.FECHAFIRMACARTA')) AS DATETIME2);
        DECLARE @v_indicadorDescuentoRol NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.INDICADOR_DESCUENTO_ROL'), JSON_VALUE(@payload,'$.INDICADORDESCUENTOROL'));
        DECLARE @v_montoDescuento DECIMAL(18,6) = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.MONTO_DESCUENTO'), JSON_VALUE(@payload,'$.MONTODESCUENTO')) AS DECIMAL(18,6));
        DECLARE @v_codigoTipoDocumento INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_TIPO_DOCUMENTO'), JSON_VALUE(@payload,'$.CODIGOTIPODOCUMENTO')) AS INT);
        DECLARE @v_fechaDocumento DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_DOCUMENTO'), JSON_VALUE(@payload,'$.FECHADOCUMENTO')) AS DATETIME2);
        DECLARE @v_estado NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.ESTADO'), JSON_VALUE(@payload,'$.ESTADO'));
        DECLARE @v_fechaIngreso DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_INGRESO'), JSON_VALUE(@payload,'$.FECHAINGRESO')) AS DATETIME2);
        DECLARE @v_ingresoEgreso NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.INGRESO_EGRESO'), JSON_VALUE(@payload,'$.INGRESOEGRESO'));
        DECLARE @v_codigoTipoIngresoEgreso INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_TIPO_INGRESO_EGRESO'), JSON_VALUE(@payload,'$.CODIGOTIPOINGRESOEGRESO')) AS INT);
        DECLARE @v_secuenciaIngresoEgreso INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.SECUENCIA_INGRESO_EGRESO'), JSON_VALUE(@payload,'$.SECUENCIAINGRESOEGRESO')) AS INT);
        DECLARE @v_montoMensual DECIMAL(18,6) = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.MONTO_MENSUAL'), JSON_VALUE(@payload,'$.MONTOMENSUAL')) AS DECIMAL(18,6));
        DECLARE @v_fijo NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.FIJO'), JSON_VALUE(@payload,'$.FIJO'));
        DECLARE @v_codigoEmpresa SMALLINT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_EMPRESA'), JSON_VALUE(@payload,'$.CODIGOEMPRESA')) AS SMALLINT);
        DECLARE @v_fechaEliminacion DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_ELIMINACION'), JSON_VALUE(@payload,'$.FECHAELIMINACION')) AS DATETIME2);
        DECLARE @v_viaAutorizacionTratamientoDatos NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.VIA_AUTORIZACION_TRATAMIENTO_DATOS'), JSON_VALUE(@payload,'$.VIAAUTORIZACIONTRATAMIENTODATOS'));
        DECLARE @v_codigoUsuarioCrea INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_USUARIO_CREA'), JSON_VALUE(@payload,'$.CODIGOUSUARIOCREA')) AS INT);
        DECLARE @v_usuarioElimina NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.USUARIO_ELIMINA'), JSON_VALUE(@payload,'$.USUARIOELIMINA'));
        DECLARE @v_numeroCedulaRecibeCoreo NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.NUMERO_CEDULA_RECIBE_COREO'), JSON_VALUE(@payload,'$.NUMEROCEDULARECIBECOREO'));
        DECLARE @v_secuenciaReactivacionParticipe INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.SECUENCIA_REACTIVACION_PARTICIPE'), JSON_VALUE(@payload,'$.SECUENCIAREACTIVACIONPARTICIPE')) AS INT);
        DECLARE @v_indicadorVerificacion NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.INDICADOR_VERIFICACION'), JSON_VALUE(@payload,'$.INDICADORVERIFICACION'));
        DECLARE @v_usuarioIngresa NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.USUARIO_INGRESA'), JSON_VALUE(@payload,'$.USUARIOINGRESA'));
        DECLARE @v_codigoUsuarioProceso INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_USUARIO_PROCESO'), JSON_VALUE(@payload,'$.CODIGOUSUARIOPROCESO')) AS INT);
        DECLARE @v_cedulaPromotorPorProceso NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.CEDULA_PROMOTOR_POR_PROCESO'), JSON_VALUE(@payload,'$.CEDULAPROMOTORPORPROCESO'));
        DECLARE @v_tipoProceso INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.TIPO_PROCESO'), JSON_VALUE(@payload,'$.TIPOPROCESO')) AS INT);
        DECLARE @v_codigoFondo SMALLINT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_FONDO'), JSON_VALUE(@payload,'$.CODIGOFONDO')) AS SMALLINT);
        DECLARE @v_codigoProceso INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_PROCESO'), JSON_VALUE(@payload,'$.CODIGOPROCESO')) AS INT);
        DECLARE @v_codigoFormaDescuento INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_FORMA_DESCUENTO'), JSON_VALUE(@payload,'$.CODIGOFORMADESCUENTO')) AS INT);
        DECLARE @v_codigoTipoSeguimiento INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_TIPO_SEGUIMIENTO'), JSON_VALUE(@payload,'$.CODIGOTIPOSEGUIMIENTO')) AS INT);
        DECLARE @v_fechaCreacion DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_CREACION'), JSON_VALUE(@payload,'$.FECHACREACION')) AS DATETIME2);
        DECLARE @v_usuarioModifica NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.USUARIO_MODIFICA'), JSON_VALUE(@payload,'$.USUARIOMODIFICA'));
        DECLARE @v_fechaModificacion DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_MODIFICACION'), JSON_VALUE(@payload,'$.FECHAMODIFICACION')) AS DATETIME2);
        DECLARE @v_cedulaEjecutivos NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.CEDULA_EJECUTIVOS'), JSON_VALUE(@payload,'$.CEDULAEJECUTIVOS'));
        DECLARE @v_poseeCartaAntiguaAfiliacion NVARCHAR(MAX) = COALESCE(JSON_VALUE(@payload,'$.POSEE_CARTA_ANTIGUA_AFILIACION'), JSON_VALUE(@payload,'$.POSEECARTAANTIGUAAFILIACION'));
        DECLARE @v_secuenciaFichaAfiliacion INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.SECUENCIA_FICHA_AFILIACION'), JSON_VALUE(@payload,'$.SECUENCIAFICHAAFILIACION')) AS INT);

        EXEC participes.sp_documentacionAfiliado_type_crud
            @accion = @accion,
            @codigoTipoIdentificacion = @v_codigoTipoIdentificacion,
            @identificacion = @v_identificacion,
            @secuenciaDocumento = @v_secuenciaDocumento,
            @codigoDocumento = @v_codigoDocumento,
            @fechaFirmaDocumento = @v_fechaFirmaDocumento,
            @cedulaUnificada = @v_cedulaUnificada,
            @fechaUnificacion = @v_fechaUnificacion,
            @tipoUnificacion = @v_tipoUnificacion,
            @codigoInstitucion = @v_codigoInstitucion,
            @fechaFirmaCarta = @v_fechaFirmaCarta,
            @indicadorDescuentoRol = @v_indicadorDescuentoRol,
            @montoDescuento = @v_montoDescuento,
            @codigoTipoDocumento = @v_codigoTipoDocumento,
            @fechaDocumento = @v_fechaDocumento,
            @estado = @v_estado,
            @fechaIngreso = @v_fechaIngreso,
            @ingresoEgreso = @v_ingresoEgreso,
            @codigoTipoIngresoEgreso = @v_codigoTipoIngresoEgreso,
            @secuenciaIngresoEgreso = @v_secuenciaIngresoEgreso,
            @montoMensual = @v_montoMensual,
            @fijo = @v_fijo,
            @codigoEmpresa = @v_codigoEmpresa,
            @fechaEliminacion = @v_fechaEliminacion,
            @viaAutorizacionTratamientoDatos = @v_viaAutorizacionTratamientoDatos,
            @codigoUsuarioCrea = @v_codigoUsuarioCrea,
            @usuarioElimina = @v_usuarioElimina,
            @numeroCedulaRecibeCoreo = @v_numeroCedulaRecibeCoreo,
            @secuenciaReactivacionParticipe = @v_secuenciaReactivacionParticipe,
            @indicadorVerificacion = @v_indicadorVerificacion,
            @usuarioIngresa = @v_usuarioIngresa,
            @codigoUsuarioProceso = @v_codigoUsuarioProceso,
            @cedulaPromotorPorProceso = @v_cedulaPromotorPorProceso,
            @tipoProceso = @v_tipoProceso,
            @codigoFondo = @v_codigoFondo,
            @codigoProceso = @v_codigoProceso,
            @codigoFormaDescuento = @v_codigoFormaDescuento,
            @codigoTipoSeguimiento = @v_codigoTipoSeguimiento,
            @fechaCreacion = @v_fechaCreacion,
            @usuarioModifica = @v_usuarioModifica,
            @fechaModificacion = @v_fechaModificacion,
            @cedulaEjecutivos = @v_cedulaEjecutivos,
            @poseeCartaAntiguaAfiliacion = @v_poseeCartaAntiguaAfiliacion,
            @secuenciaFichaAfiliacion = @v_secuenciaFichaAfiliacion;
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type,
                N'wrapper documentacionAfiliadoType: ' + ERROR_MESSAGE());
    END CATCH
END
