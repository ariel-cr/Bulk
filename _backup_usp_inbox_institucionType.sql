-- Backup ORIGINAL pre-fix
CREATE   PROCEDURE dbo.usp_inbox_institucionType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;
    BEGIN TRY
        -- Mapear event_type Oracle -> @accion del SP original (UPPERCASE)
        DECLARE @accion VARCHAR(10) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'INSERT'
            WHEN @event_type IN ('UPDATE','U') THEN 'UPDATE'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'DELETE'
            ELSE 'INSERT' END;

        -- Audit del wrapper (mantenemos cdc_inbox_parsed)
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @aggregate_id, @aggregate_id);

        -- Extraer parametros desde payload JSON
        DECLARE @v_codigoInstitucion VARCHAR(6) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_INSTITUCION'), JSON_VALUE(@payload,'$.CODIGOINSTITUCION'));
        DECLARE @v_nombreInstitucion VARCHAR(100) = COALESCE(JSON_VALUE(@payload,'$.NOMBRE_INSTITUCION'), JSON_VALUE(@payload,'$.NOMBREINSTITUCION'));
        DECLARE @v_rucInstitucion VARCHAR(13) = COALESCE(JSON_VALUE(@payload,'$.RUC_INSTITUCION'), JSON_VALUE(@payload,'$.RUCINSTITUCION'));
        DECLARE @v_codigoTipoInstitucion CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_TIPO_INSTITUCION'), JSON_VALUE(@payload,'$.CODIGOTIPOINSTITUCION'));
        DECLARE @v_codigoProvincia CHAR(2) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_PROVINCIA'), JSON_VALUE(@payload,'$.CODIGOPROVINCIA'));
        DECLARE @v_codigoCiudad VARCHAR(5) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_CIUDAD'), JSON_VALUE(@payload,'$.CODIGOCIUDAD'));
        DECLARE @v_direccion VARCHAR(200) = JSON_VALUE(@payload,'$.DIRECCION');
        DECLARE @v_telefono VARCHAR(10) = JSON_VALUE(@payload,'$.TELEFONO');
        DECLARE @v_representanteLegal VARCHAR(50) = COALESCE(JSON_VALUE(@payload,'$.REPRESENTANTE_LEGAL'), JSON_VALUE(@payload,'$.REPRESENTANTELEGAL'));
        DECLARE @v_correoElectronico VARCHAR(100) = COALESCE(JSON_VALUE(@payload,'$.CORREO_ELECTRONICO'), JSON_VALUE(@payload,'$.CORREOELECTRONICO'));
        DECLARE @v_estado CHAR(1) = JSON_VALUE(@payload,'$.ESTADO');
        DECLARE @v_fechaIngreso DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_INGRESO'), JSON_VALUE(@payload,'$.FECHAINGRESO')) AS DATETIME2);
        DECLARE @v_codigoConvenio CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_CONVENIO'), JSON_VALUE(@payload,'$.CODIGOCONVENIO'));
        DECLARE @v_indicadorDescuentoRol CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.INDICADOR_DESCUENTO_ROL'), JSON_VALUE(@payload,'$.INDICADORDESCUENTOROL'));
        DECLARE @v_codigoParroquia VARCHAR(5) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_PARROQUIA'), JSON_VALUE(@payload,'$.CODIGOPARROQUIA'));
        DECLARE @v_codigoAmie VARCHAR(20) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_AMIE'), JSON_VALUE(@payload,'$.CODIGOAMIE'));
        DECLARE @v_codigoDistrito INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_DISTRITO'), JSON_VALUE(@payload,'$.CODIGODISTRITO')) AS INT);
        DECLARE @v_codigoCircuito INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_CIRCUITO'), JSON_VALUE(@payload,'$.CODIGOCIRCUITO')) AS INT);
        DECLARE @v_codigoSector INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_SECTOR'), JSON_VALUE(@payload,'$.CODIGOSECTOR')) AS INT);
        DECLARE @v_tipoSostenimiento CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.TIPO_SOSTENIMIENTO'), JSON_VALUE(@payload,'$.TIPOSOSTENIMIENTO'));
        DECLARE @v_nivel SMALLINT = TRY_CAST(JSON_VALUE(@payload,'$.NIVEL') AS SMALLINT);
        DECLARE @v_jornadaClases SMALLINT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.JORNADA_CLASES'), JSON_VALUE(@payload,'$.JORNADACLASES')) AS SMALLINT);
        DECLARE @v_numeroPatronal VARCHAR(11) = COALESCE(JSON_VALUE(@payload,'$.NUMERO_PATRONAL'), JSON_VALUE(@payload,'$.NUMEROPATRONAL'));
        DECLARE @v_numeroCuentaBancoCentral VARCHAR(20) = COALESCE(JSON_VALUE(@payload,'$.NUMERO_CUENTA_BANCO_CENTRAL'), JSON_VALUE(@payload,'$.NUMEROCUENTABANCOCENTRAL'));
        DECLARE @v_indicadorInstitucionMunicipal CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.INDICADOR_INSTITUCION_MUNICIPAL'), JSON_VALUE(@payload,'$.INDICADORINSTITUCIONMUNICIPAL'));
        DECLARE @v_nombreColector VARCHAR(80) = COALESCE(JSON_VALUE(@payload,'$.NOMBRE_COLECTOR'), JSON_VALUE(@payload,'$.NOMBRECOLECTOR'));
        DECLARE @v_cedulaColector VARCHAR(10) = COALESCE(JSON_VALUE(@payload,'$.CEDULA_COLECTOR'), JSON_VALUE(@payload,'$.CEDULACOLECTOR'));
        DECLARE @v_telefonoColector VARCHAR(10) = COALESCE(JSON_VALUE(@payload,'$.TELEFONO_COLECTOR'), JSON_VALUE(@payload,'$.TELEFONOCOLECTOR'));
        DECLARE @v_direccionColector VARCHAR(80) = COALESCE(JSON_VALUE(@payload,'$.DIRECCION_COLECTOR'), JSON_VALUE(@payload,'$.DIRECCIONCOLECTOR'));
        DECLARE @v_porcentajeCam DECIMAL(18,6) = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.PORCENTAJE_CAM'), JSON_VALUE(@payload,'$.PORCENTAJECAM')) AS DECIMAL(18,6));
        DECLARE @v_numeroTelefono VARCHAR(10) = COALESCE(JSON_VALUE(@payload,'$.NUMERO_TELEFONO'), JSON_VALUE(@payload,'$.NUMEROTELEFONO'));
        DECLARE @v_codigoProvinciaColector VARCHAR(2) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_PROVINCIA_COLECTOR'), JSON_VALUE(@payload,'$.CODIGOPROVINCIACOLECTOR'));
        DECLARE @v_ciudadColector VARCHAR(5) = COALESCE(JSON_VALUE(@payload,'$.CIUDAD_COLECTOR'), JSON_VALUE(@payload,'$.CIUDADCOLECTOR'));
        DECLARE @v_codigoParroquiaColector VARCHAR(5) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_PARROQUIA_COLECTOR'), JSON_VALUE(@payload,'$.CODIGOPARROQUIACOLECTOR'));
        DECLARE @v_numeroTelefonoColector VARCHAR(10) = COALESCE(JSON_VALUE(@payload,'$.NUMERO_TELEFONO_COLECTOR'), JSON_VALUE(@payload,'$.NUMEROTELEFONOCOLECTOR'));
        DECLARE @v_tipoDireccionEntregaListados CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.TIPO_DIRECCION_ENTREGA_LISTADOS'), JSON_VALUE(@payload,'$.TIPODIRECCIONENTREGALISTADOS'));
        DECLARE @v_tipoDireccionPagos CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.TIPO_DIRECCION_PAGOS'), JSON_VALUE(@payload,'$.TIPODIRECCIONPAGOS'));
        DECLARE @v_indicadorImpresionEstadoCuenta CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.INDICADOR_IMPRESION_ESTADO_CUENTA'), JSON_VALUE(@payload,'$.INDICADORIMPRESIONESTADOCUENTA'));
        DECLARE @v_codigoSegunElSinec INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_SEGUN_EL_SINEC'), JSON_VALUE(@payload,'$.CODIGOSEGUNELSINEC')) AS INT);
        DECLARE @v_usuarioModifica SMALLINT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.USUARIO_MODIFICA'), JSON_VALUE(@payload,'$.USUARIOMODIFICA')) AS SMALLINT);
        DECLARE @v_emailColector VARCHAR(100) = COALESCE(JSON_VALUE(@payload,'$.EMAIL_COLECTOR'), JSON_VALUE(@payload,'$.EMAILCOLECTOR'));
        DECLARE @v_direccionProvincialQueCorresponde CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.DIRECCION_PROVINCIAL_QUE_CORRESPONDE'), JSON_VALUE(@payload,'$.DIRECCIONPROVINCIALQUECORRESPONDE'));
        DECLARE @v_numeroUte SMALLINT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.NUMERO_UTE'), JSON_VALUE(@payload,'$.NUMEROUTE')) AS SMALLINT);
        DECLARE @v_numeroZona SMALLINT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.NUMERO_ZONA'), JSON_VALUE(@payload,'$.NUMEROZONA')) AS SMALLINT);
        DECLARE @v_usuarioIngresa SMALLINT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.USUARIO_INGRESA'), JSON_VALUE(@payload,'$.USUARIOINGRESA')) AS SMALLINT);
        DECLARE @v_tieneContratoBceParaRecaudacion CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.TIENE_CONTRATO_BCE_PARA_RECAUDACION'), JSON_VALUE(@payload,'$.TIENECONTRATOBCEPARARECAUDACION'));
        DECLARE @v_fechaDirmaContrato DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_DIRMA_CONTRATO'), JSON_VALUE(@payload,'$.FECHADIRMACONTRATO')) AS DATETIME2);
        DECLARE @v_indicadorConfirmacionBancoCentral CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.INDICADOR_CONFIRMACION_BANCO_CENTRAL'), JSON_VALUE(@payload,'$.INDICADORCONFIRMACIONBANCOCENTRAL'));
        DECLARE @v_rolActualizacionInstitucion CHAR(6) = COALESCE(JSON_VALUE(@payload,'$.ROL_ACTUALIZACION_INSTITUCION'), JSON_VALUE(@payload,'$.ROLACTUALIZACIONINSTITUCION'));
        DECLARE @v_fechaUltimaActualizacion DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_ULTIMA_ACTUALIZACION'), JSON_VALUE(@payload,'$.FECHAULTIMAACTUALIZACION')) AS DATETIME2);
        DECLARE @v_horaUltimaActualizacion CHAR(8) = COALESCE(JSON_VALUE(@payload,'$.HORA_ULTIMA_ACTUALIZACION'), JSON_VALUE(@payload,'$.HORAULTIMAACTUALIZACION'));
        DECLARE @v_codigoUnidadEjecutora VARCHAR(8) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_UNIDAD_EJECUTORA'), JSON_VALUE(@payload,'$.CODIGOUNIDADEJECUTORA'));
        DECLARE @v_numeroCuentaRotativaIngreso VARCHAR(20) = COALESCE(JSON_VALUE(@payload,'$.NUMERO_CUENTA_ROTATIVA_INGRESO'), JSON_VALUE(@payload,'$.NUMEROCUENTAROTATIVAINGRESO'));
        DECLARE @v_tipoCuentaRotativaIngreso CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.TIPO_CUENTA_ROTATIVA_INGRESO'), JSON_VALUE(@payload,'$.TIPOCUENTAROTATIVAINGRESO'));
        DECLARE @v_codigoBancoCuentaRotativaIngreso VARCHAR(2) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_BANCO_CUENTA_ROTATIVA_INGRESO'), JSON_VALUE(@payload,'$.CODIGOBANCOCUENTAROTATIVAINGRESO'));
        DECLARE @v_codigoEmpresa SMALLINT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_EMPRESA'), JSON_VALUE(@payload,'$.CODIGOEMPRESA')) AS SMALLINT);
        DECLARE @v_tipoInstitucion CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.TIPO_INSTITUCION'), JSON_VALUE(@payload,'$.TIPOINSTITUCION'));
        DECLARE @v_telefonoConvencionalInstitucion VARCHAR(20) = COALESCE(JSON_VALUE(@payload,'$.TELEFONO_CONVENCIONAL_INSTITUCION'), JSON_VALUE(@payload,'$.TELEFONOCONVENCIONALINSTITUCION'));
        DECLARE @v_cedulaRepresentante VARCHAR(20) = COALESCE(JSON_VALUE(@payload,'$.CEDULA_REPRESENTANTE'), JSON_VALUE(@payload,'$.CEDULAREPRESENTANTE'));
        DECLARE @v_nombreRepresentante VARCHAR(100) = COALESCE(JSON_VALUE(@payload,'$.NOMBRE_REPRESENTANTE'), JSON_VALUE(@payload,'$.NOMBREREPRESENTANTE'));
        DECLARE @v_mailRepresentante VARCHAR(100) = COALESCE(JSON_VALUE(@payload,'$.MAIL_REPRESENTANTE'), JSON_VALUE(@payload,'$.MAILREPRESENTANTE'));
        DECLARE @v_telefonoRepresentante VARCHAR(20) = COALESCE(JSON_VALUE(@payload,'$.TELEFONO_REPRESENTANTE'), JSON_VALUE(@payload,'$.TELEFONOREPRESENTANTE'));
        DECLARE @v_tipoAcceso CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.TIPO_ACCESO'), JSON_VALUE(@payload,'$.TIPOACCESO'));
        DECLARE @v_numeroDocente INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.NUMERO_DOCENTE'), JSON_VALUE(@payload,'$.NUMERODOCENTE')) AS INT);
        DECLARE @v_numeroBonificacion INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.NUMERO_BONIFICACION'), JSON_VALUE(@payload,'$.NUMEROBONIFICACION')) AS INT);
        DECLARE @v_numeroAdministrador INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.NUMERO_ADMINISTRADOR'), JSON_VALUE(@payload,'$.NUMEROADMINISTRADOR')) AS INT);
        DECLARE @v_numeroAlumnos INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.NUMERO_ALUMNOS'), JSON_VALUE(@payload,'$.NUMEROALUMNOS')) AS INT);
        DECLARE @v_codigoCircuitoMinisterioEducacion INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_CIRCUITO_MINISTERIO_EDUCACION'), JSON_VALUE(@payload,'$.CODIGOCIRCUITOMINISTERIOEDUCACION')) AS INT);
        DECLARE @v_codigoDistritoMinisterioEducacion INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_DISTRITO_MINISTERIO_EDUCACION'), JSON_VALUE(@payload,'$.CODIGODISTRITOMINISTERIOEDUCACION')) AS INT);
        DECLARE @v_codigoModalidad INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_MODALIDAD'), JSON_VALUE(@payload,'$.CODIGOMODALIDAD')) AS INT);
        DECLARE @v_codigoEtnia INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_ETNIA'), JSON_VALUE(@payload,'$.CODIGOETNIA')) AS INT);
        DECLARE @v_codigoNacionalidad INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_NACIONALIDAD'), JSON_VALUE(@payload,'$.CODIGONACIONALIDAD')) AS INT);
        DECLARE @v_tipoEducacionMinisterioEducacion CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.TIPO_EDUCACION_MINISTERIO_EDUCACION'), JSON_VALUE(@payload,'$.TIPOEDUCACIONMINISTERIOEDUCACION'));
        DECLARE @v_codigoZonaMinisterioEducacion INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_ZONA_MINISTERIO_EDUCACION'), JSON_VALUE(@payload,'$.CODIGOZONAMINISTERIOEDUCACION')) AS INT);
        DECLARE @v_unidadAdministrativaCircuital CHAR(1) = COALESCE(JSON_VALUE(@payload,'$.UNIDAD_ADMINISTRATIVA_CIRCUITAL'), JSON_VALUE(@payload,'$.UNIDADADMINISTRATIVACIRCUITAL'));
        DECLARE @v_codigoMotivoModificacionInstitucion INT = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.CODIGO_MOTIVO_MODIFICACION_INSTITUCION'), JSON_VALUE(@payload,'$.CODIGOMOTIVOMODIFICACIONINSTITUCION')) AS INT);
        DECLARE @v_fechaModificacionAdicional DATETIME2 = TRY_CAST(COALESCE(JSON_VALUE(@payload,'$.FECHA_MODIFICACION_ADICIONAL'), JSON_VALUE(@payload,'$.FECHAMODIFICACIONADICIONAL')) AS DATETIME2);
        DECLARE @v_codigoRegistroEscolar VARCHAR(20) = COALESCE(JSON_VALUE(@payload,'$.CODIGO_REGISTRO_ESCOLAR'), JSON_VALUE(@payload,'$.CODIGOREGISTROESCOLAR'));

        -- Llamar SP original del equipo (schema participes.*)
        EXEC participes.sp_institucion_type_crud
        @accion = @accion,
        @codigoInstitucion = @v_codigoInstitucion,
        @nombreInstitucion = @v_nombreInstitucion,
        @rucInstitucion = @v_rucInstitucion,
        @codigoTipoInstitucion = @v_codigoTipoInstitucion,
        @codigoProvincia = @v_codigoProvincia,
        @codigoCiudad = @v_codigoCiudad,
        @direccion = @v_direccion,
        @telefono = @v_telefono,
        @representanteLegal = @v_representanteLegal,
        @correoElectronico = @v_correoElectronico,
        @estado = @v_estado,
        @fechaIngreso = @v_fechaIngreso,
        @codigoConvenio = @v_codigoConvenio,
        @indicadorDescuentoRol = @v_indicadorDescuentoRol,
        @codigoParroquia = @v_codigoParroquia,
        @codigoAmie = @v_codigoAmie,
        @codigoDistrito = @v_codigoDistrito,
        @codigoCircuito = @v_codigoCircuito,
        @codigoSector = @v_codigoSector,
        @tipoSostenimiento = @v_tipoSostenimiento,
        @nivel = @v_nivel,
        @jornadaClases = @v_jornadaClases,
        @numeroPatronal = @v_numeroPatronal,
        @numeroCuentaBancoCentral = @v_numeroCuentaBancoCentral,
        @indicadorInstitucionMunicipal = @v_indicadorInstitucionMunicipal,
        @nombreColector = @v_nombreColector,
        @cedulaColector = @v_cedulaColector,
        @telefonoColector = @v_telefonoColector,
        @direccionColector = @v_direccionColector,
        @porcentajeCam = @v_porcentajeCam,
        @numeroTelefono = @v_numeroTelefono,
        @codigoProvinciaColector = @v_codigoProvinciaColector,
        @ciudadColector = @v_ciudadColector,
        @codigoParroquiaColector = @v_codigoParroquiaColector,
        @numeroTelefonoColector = @v_numeroTelefonoColector,
        @tipoDireccionEntregaListados = @v_tipoDireccionEntregaListados,
        @tipoDireccionPagos = @v_tipoDireccionPagos,
        @indicadorImpresionEstadoCuenta = @v_indicadorImpresionEstadoCuenta,
        @codigoSegunElSinec = @v_codigoSegunElSinec,
        @usuarioModifica = @v_usuarioModifica,
        @emailColector = @v_emailColector,
        @direccionProvincialQueCorresponde = @v_direccionProvincialQueCorresponde,
        @numeroUte = @v_numeroUte,
        @numeroZona = @v_numeroZona,
        @usuarioIngresa = @v_usuarioIngresa,
        @tieneContratoBceParaRecaudacion = @v_tieneContratoBceParaRecaudacion,
        @fechaDirmaContrato = @v_fechaDirmaContrato,
        @indicadorConfirmacionBancoCentral = @v_indicadorConfirmacionBancoCentral,
        @rolActualizacionInstitucion = @v_rolActualizacionInstitucion,
        @fechaUltimaActualizacion = @v_fechaUltimaActualizacion,
        @horaUltimaActualizacion = @v_horaUltimaActualizacion,
        @codigoUnidadEjecutora = @v_codigoUnidadEjecutora,
        @numeroCuentaRotativaIngreso = @v_numeroCuentaRotativaIngreso,
        @tipoCuentaRotativaIngreso = @v_tipoCuentaRotativaIngreso,
        @codigoBancoCuentaRotativaIngreso = @v_codigoBancoCuentaRotativaIngreso,
        @codigoEmpresa = @v_codigoEmpresa,
        @tipoInstitucion = @v_tipoInstitucion,
        @telefonoConvencionalInstitucion = @v_telefonoConvencionalInstitucion,
        @cedulaRepresentante = @v_cedulaRepresentante,
        @nombreRepresentante = @v_nombreRepresentante,
        @mailRepresentante = @v_mailRepresentante,
        @telefonoRepresentante = @v_telefonoRepresentante,
        @tipoAcceso = @v_tipoAcceso,
        @numeroDocente = @v_numeroDocente,
        @numeroBonificacion = @v_numeroBonificacion,
        @numeroAdministrador = @v_numeroAdministrador,
        @numeroAlumnos = @v_numeroAlumnos,
        @codigoCircuitoMinisterioEducacion = @v_codigoCircuitoMinisterioEducacion,
        @codigoDistritoMinisterioEducacion = @v_codigoDistritoMinisterioEducacion,
        @codigoModalidad = @v_codigoModalidad,
        @codigoEtnia = @v_codigoEtnia,
        @codigoNacionalidad = @v_codigoNacionalidad,
        @tipoEducacionMinisterioEducacion = @v_tipoEducacionMinisterioEducacion,
        @codigoZonaMinisterioEducacion = @v_codigoZonaMinisterioEducacion,
        @unidadAdministrativaCircuital = @v_unidadAdministrativaCircuital,
        @codigoMotivoModificacionInstitucion = @v_codigoMotivoModificacionInstitucion,
        @fechaModificacionAdicional = @v_fechaModificacionAdicional,
        @codigoRegistroEscolar = @v_codigoRegistroEscolar;
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type,
                N'wrapper institucionType: ' + ERROR_MESSAGE());
    END CATCH
END