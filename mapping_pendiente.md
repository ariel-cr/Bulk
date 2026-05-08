# Mapeo pendiente legacy -> Oracle FCME_USER

Para cada `oracle_col`, completar `mapeo_final` con el nombre de la columna legacy correcta.
Si no existe en legacy, escribir `NULL` o `<expresion>`.

## agendaMailAfiliadoType  ->  `AGENDAMAILAFILIADO_TYPE`
Tablas legacy origen: `fctbagen_mail`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOEMPRESA` | `?` |  |
| `CODIGOCEDU` | `?` |  |
| `SECUENCIAREGISTRO` | `?` |  |
| `DESCRIPCIONMAIL` | `?` |  |
| `INDICADORPRIN` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `FECHAINGRESO` | `?` |  |
| `USUARIOMODIFICA` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `CODIGOUSUELIM` | `?` |  |
| `FECHAELIMINACION` | `?` |  |
| `ESTADOREGISTRO` | `?` |  |

## areaLaboralParticipeType  ->  `AREALABORALPARTICIPE_TYPE`
Tablas legacy origen: `fctbarea_lbrl`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOAREALABORAL` | `?` |  |
| `DESCRIPCIONAREALABORAL` | `?` |  |
| `ESTADOREGISTRO` | `?` |  |

## auditoriaAfiliadoType  ->  `AUDITORIAAFILIADO_TYPE`
Tablas legacy origen: `cttbafil_audi,cttbtabl_afil,fctbaudi_actu_afil,fctbaudi_movi,sfct_afiliado_auditor,sfct_motivo_mant_afiliados`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `SECUENCIAAUDITORIA` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `CAMPOMODIFICADO` | `?` |  |
| `VALORANTERIOR` | `?` |  |
| `VALORNUEVO` | `?` |  |
| `CODIGOUSUARIO` | `?` |  |
| `TIPOOPERACION` | `?` |  |
| `ORIGENMODIFICACION` | `?` |  |
| `ESTADO` | `ce_estado` |  |
| `CODIGOCUENTA` | `?` |  |
| `TIPOIDENTIFICACION` | `?` |  |
| `RELACIONPRODUCTO` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `SECUENCIAACTUALIZACION` | `?` |  |
| `NUMEROCEDULARECIBECOREO` | `?` |  |
| `NUMEROTELEFONOCONVENCIONAL` | `?` |  |
| `NUMEROTELEFONOCELULAR` | `?` |  |
| `DIRECCIONPATRONO` | `?` |  |
| `DESCRIPCIONINSTITUCION` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `FECHACREACION` | `?` |  |
| `HORAINGRESO` | `?` |  |
| `CODIGOCAMPOAMODIFICAR` | `?` |  |
| `NOMBRECAMPOAMODIFICAR` | `?` |  |
| `DESCRIPCIONCAMPOAMODIFICAR` | `?` |  |
| `HORACREACION` | `?` |  |
| `DESCRIPCIONADICIONAL` | `?` |  |
| `CODIGOMOTIVOMANTENIMIENTO` | `?` |  |
| `NUMEROTRANSACCION` | `?` |  |
| `CODIGOTRANSACCIONUTILIZADA` | `?` |  |
| `HORAGENERACIONREGISTRO` | `?` |  |
| `INDICADORCONTRATOCESANTIA` | `?` |  |
| `NIVELAPORTE` | `ci_nivelaporte` |  |
| `VALORDESCUENTOHIPOTECARIO` | `?` |  |
| `CODIGOCATEGORIA` | `?` |  |
| `CEDULACOORDINADOR` | `?` |  |
| `PORCENTAJECAM` | `?` |  |
| `PORCENTAJEFUNCIONAL` | `?` |  |

## beneficiarioParticipeType  ->  `BENEFICIARIOPARTICIPE_TYPE`
Tablas legacy origen: `sfct_beneficiario,sfct_beneficiario_retiro`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `APELLIDOSBENEFICIARIOS` | `?` |  |
| `CODIGOBANCOPAGO` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `CODIGOUSUARIOINGRESOREGISTRO` | `?` |  |
| `CODIGOUSUARIOMODIFICOREGISTRO` | `?` |  |
| `ESTATUSDELBENEFICIARIO` | `?` |  |
| `FECHACREACIONREGISTRO` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `NOMBRESBENEFICIARIO` | `?` |  |
| `NUMEROCEDULA` | `?` |  |
| `NUMEROCEDULABENEFICIARIO` | `?` |  |
| `NUMEROCUENTA` | `?` |  |
| `PORCENTAJEDISTRIBUCIONVALORES` | `?` |  |
| `SECUENCIABENEFICIARIO` | `?` |  |
| `TIPOCUENTAPAGO` | `?` |  |
| `CODIGOBANCO` | `?` |  |
| `CODIGOBANCODESEMBOLSO` | `?` |  |
| `ESTADOREGISTRO` | `?` |  |
| `FECHACREACION` | `?` |  |
| `FECHADESEMBOLSOLIQUIDACION` | `?` |  |
| `FECHAELIMINACION` | `?` |  |
| `MONTOARECIBIR` | `?` |  |
| `NOMBREBENEFICIARIO` | `?` |  |
| `SECUENCIADESEMBOLSOPORLIQUIDACION` | `?` |  |
| `SECUENCIARETIRO` | `?` |  |
| `TIPOCUENTA` | `?` |  |
| `TIPODESEMBOLSO` | `?` |  |
| `USUARIOELIMINA` | `?` |  |
| `USUARIOINGRESA` | `?` |  |

## cuentaBancariaAfiliadoType  ->  `CUENTABANCARIAAFILIADO_TYPE`
Tablas legacy origen: `sfct_padbs`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CEDULABENEFICIARIO` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `CUENTABANCARIA` | `?` |  |
| `CUENTABANCODESTINO` | `?` |  |
| `ESTADOREGISTRO` | `?` |  |
| `FECHACREACION` | `?` |  |
| `FECHAELIMINACION` | `?` |  |
| `MONTOMOVIMIENTO` | `?` |  |
| `NOMBREBENEFICIARIOPAGO` | `?` |  |
| `NUMEROCUENTA` | `?` |  |
| `SECUENCIALIQUIDACION` | `?` |  |
| `SECUENCIAPAGO` | `?` |  |
| `TIPOCUENTA` | `?` |  |
| `TIPOPAGO` | `?` |  |
| `USUARIOELIMINA` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `CODIGOBANCO` | `?` |  |

## distribucionAfiliadoType  ->  `DISTRIBUCIONAFILIADO_TYPE`
Tablas legacy origen: `cttbmatr_dist_afil`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CIRCUITO` | `Circuito` |  |
| `CIUDAD` | `Ciudad` |  |
| `DISTRITO` | `Distrito` |  |
| `ESTADODISTRIBUCIONAFILIADO` | `Estado` |  |
| `MONTOCREDITOVIGENTE` | `?` |  |
| `MONTOCUENTAUNICA` | `?` |  |
| `NOMBREPROVINCIA` | `Provincia` |  |
| `NUMEROAFILIADO` | `NumeroAfiliado` |  |
| `NUMEROAFILIADOACTUALIZADO` | `NumeroAfiliadoActualizado` |  |
| `NUMEROCADB` | `NumeroCADB` |  |
| `NUMEROCAP` | `NumeroCAP` |  |
| `NUMEROCREDITOVIGENTE` | `NumeroCreditoVigente` |  |
| `NUMERODIRECTIVONACIONAL2008` | `NumeroDirectivoNacional2008` |  |
| `NUMERODIRECTIVONACIONAL2010` | `NumeroDirectivoNacional2010` |  |
| `NUMERODIRECTIVOPROVINCIAL2008` | `NumeroDirectivoProvincial2008` |  |
| `NUMERODIRECTIVOPROVINCIAL2010` | `NumeroDirectivoProvincial2010` |  |
| `NUMEROEJECUTIVOFINANCIERO` | `NumeroEjecutivoFinanciero` |  |
| `NUMEROINSTITUCIONES` | `NumeroInstituciones` |  |
| `NUMEROLEGADOCONVENCION2008` | `?` |  |
| `NUMEROLEGADOCONVENCION2010` | `?` |  |
| `NUMEROLIDEROPINION` | `?` |  |
| `NUMEROPRESIDENTEEJECUTIVO2008` | `NumeroPresidenteEjecutivo2008` |  |
| `NUMEROPRESIDENTEEJECUTIVO2010` | `NumeroPresidenteEjecutivo2010` |  |
| `NUMEROPRESIDENTESPROVINCIALES2008` | `Provincia` |  |
| `NUMEROPRESIDENTESPROVINCIALES2010` | `Provincia` |  |
| `NUMEROSOLOCAM` | `?` |  |
| `DESCRIPCIONPARROQUIA` | `Parroquia` |  |
| `TRABAJO` | `trabajo` |  |
| `ZONA` | `Zona` |  |

## documentacionAfiliadoType  ->  `DOCUMENTACIONAFILIADO_TYPE`
Tablas legacy origen: `fctbafil_auto_docs,fctbafil_unif,fctbcart_rpag,fctbfcha_afil,fctbfcha_afil_dcto`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `SECUENCIADOCUMENTO` | `?` |  |
| `CODIGODOCUMENTO` | `?` |  |
| `FECHAFIRMADOCUMENTO` | `?` |  |
| `CEDULAUNIFICADA` | `?` |  |
| `FECHAUNIFICACION` | `?` |  |
| `TIPOUNIFICACION` | `?` |  |
| `CODIGOINSTITUCION` | `?` |  |
| `FECHAFIRMACARTA` | `?` |  |
| `INDICADORDESCUENTOROL` | `?` |  |
| `MONTODESCUENTO` | `?` |  |
| `CODIGOTIPODOCUMENTO` | `?` |  |
| `FECHADOCUMENTO` | `?` |  |
| `ESTADO` | `?` |  |
| `FECHAINGRESO` | `?` |  |
| `INGRESOEGRESO` | `?` |  |
| `CODIGOTIPOINGRESOEGRESO` | `?` |  |
| `SECUENCIAINGRESOEGRESO` | `?` |  |
| `MONTOMENSUAL` | `?` |  |
| `FIJO` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `FECHAELIMINACION` | `?` |  |
| `VIAAUTORIZACIONTRATAMIENTODATOS` | `?` |  |
| `CODIGOUSUARIOCREA` | `?` |  |
| `USUARIOELIMINA` | `?` |  |
| `NUMEROCEDULARECIBECOREO` | `?` |  |
| `SECUENCIAREACTIVACIONPARTICIPE` | `?` |  |
| `INDICADORVERIFICACION` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `CODIGOUSUARIOPROCESO` | `?` |  |
| `CEDULAPROMOTORPORPROCESO` | `?` |  |
| `TIPOPROCESO` | `?` |  |
| `CODIGOFONDO` | `?` |  |
| `CODIGOPROCESO` | `?` |  |
| `CODIGOFORMADESCUENTO` | `?` |  |
| `CODIGOTIPOSEGUIMIENTO` | `?` |  |
| `FECHACREACION` | `?` |  |
| `USUARIOMODIFICA` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `CEDULAEJECUTIVOS` | `?` |  |
| `POSEECARTAANTIGUAAFILIACION` | `?` |  |
| `SECUENCIAFICHAAFILIACION` | `?` |  |

## firmanteParticipeType  ->  `FIRMANTEPARTICIPE_TYPE`
Tablas legacy origen: `sfct_firmante`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOEMPRESA` | `?` |  |
| `CODIGOINSTITUCION` | `?` |  |
| `NOFIRMANTE` | `no_firmante` |  |
| `NUMEROCEDULA` | `?` |  |
| `SECUENCIAFIRMANTE` | `?` |  |
| `TIPOINSTITUCION` | `?` |  |

## grupoFamiliarType  ->  `GRUPOFAMILIAR_TYPE`
Tablas legacy origen: `sfct_grupo_fami`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `APELLIDOSGRUPOFAMILIAR` | `?` |  |
| `CEDULAFAMILIAR` | `ci_cedula_familiar` |  |
| `CODIGOUSUARIOINGRESOREGISTRO` | `?` |  |
| `CODIGOUSUARIOMODIFICOREGISTRO` | `?` |  |
| `ESTADOFAMILIAR` | `?` |  |
| `FECHACREACIONREGISTRO` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `FECHANACIMIENTO` | `?` |  |
| `INDICADORDISCAPACIDAD` | `?` |  |
| `NOMBRESGRUPOFAMILIAR` | `?` |  |
| `TIPOCREDITO` | `?` |  |
| `TIPOREALCIONFAMILIAR` | `?` |  |
| `NUMEROCEDULA` | `?` |  |

## informacionAdicionalAfiliadoType  ->  `INFORMACIONADICIONALAFILIADO_TYPE`
Tablas legacy origen: `fctbactv_suje_cred,fctbafil_dcap,fctbafil_gast_pers,fctbafil_info_adic,sfct_afiliado_fondos`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOGENERO` | `?` |  |
| `DESCRIPCIONGENERO` | `?` |  |
| `ESTADOGENERO` | `?` |  |
| `SECUENCIAREGISTRO` | `?` |  |
| `CODIGOPRODUCTO` | `?` |  |
| `ESTADODESCUENTO` | `?` |  |
| `FECHACREACION` | `?` |  |
| `FECHAELIMINACION` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `NUMEROCEDULA` | `?` |  |
| `ROLFINDESCUENTO` | `?` |  |
| `ROLINICIODESCUENTO` | `?` |  |
| `SECUENCIADESCUENTOCAP` | `?` |  |
| `TIPODESCUENTOCAP` | `?` |  |
| `USUARIOELIMINA` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `USUARIOMODIFICA` | `?` |  |
| `VALORDESCUENTO` | `?` |  |
| `CODIGOELEMENTOFINANCIERO` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `MONTOMONETARIOCUENTAENMENCION` | `?` |  |
| `CODIGOAREALABORAL` | `?` |  |
| `CODIGODISTRITOAMIE` | `?` |  |
| `CODIGODISTRITOMINS` | `?` |  |
| `CODIGOPAISNACIONALIDAD` | `?` |  |
| `DESCRIPCIONCALLEPRINCIPAL` | `?` |  |
| `DESCRIPCIONCALLESECUNDARIA` | `?` |  |
| `DESCRIPCIONVIVIENDA` | `?` |  |
| `INDICADORCORRECCIONCEDULA` | `?` |  |
| `NOMBRECONTACTOADICIONAL` | `?` |  |
| `NUMEROCALLEPRINCIPAL` | `?` |  |
| `NUMEROCALLESECUNDARIA` | `?` |  |
| `NUMEROMANZANA` | `?` |  |
| `NUMEROVILLA` | `?` |  |
| `TELEFONOCONTACTO1` | `?` |  |
| `TELEFONOCONTACTO2` | `?` |  |
| `TIPOJORNADA` | `?` |  |
| `TIPOOPERADORACELULAR` | `?` |  |
| `TIPORELACION` | `?` |  |
| `CODIGOFONDO` | `?` |  |
| `ESTADOAFILIADO` | `?` |  |
| `FECHAINGRESOMAGISTERIO` | `?` |  |
| `FECHAREINGRESOFCME` | `?` |  |
| `FECHARETIROFCME` | `?` |  |
| `FECHAULTIMAIMPRESIONESTADOCUENTA` | `?` |  |
| `INDICADORCOBROPRESTACION` | `?` |  |
| `SALDOQUEPASOALHISTORICO` | `?` |  |
| `SALDOTRANSFERENRCIAFONDO` | `?` |  |

## institucionType  ->  `INSTITUCION_TYPE`
Tablas legacy origen: `fctbinst_info_adic,sfct_institucion`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOINSTITUCION` | `?` |  |
| `NOMBREINSTITUCION` | `?` |  |
| `RUCINSTITUCION` | `?` |  |
| `CODIGOTIPOINSTITUCION` | `?` |  |
| `CODIGOPROVINCIA` | `?` |  |
| `CODIGOCIUDAD` | `?` |  |
| `DIRECCION` | `no_direccion` |  |
| `TELEFONO` | `nu_telefono` |  |
| `REPRESENTANTELEGAL` | `?` |  |
| `CORREOELECTRONICO` | `?` |  |
| `ESTADO` | `ce_estado` |  |
| `FECHAINGRESO` | `?` |  |
| `CODIGOCONVENIO` | `?` |  |
| `INDICADORDESCUENTOROL` | `?` |  |
| `CODIGOPARROQUIA` | `?` |  |
| `CODIGOAMIE` | `?` |  |
| `CODIGODISTRITO` | `?` |  |
| `CODIGOCIRCUITO` | `?` |  |
| `CODIGOSECTOR` | `?` |  |
| `TIPOSOSTENIMIENTO` | `?` |  |
| `NIVEL` | `ti_nivel` |  |
| `JORNADACLASES` | `?` |  |
| `NUMEROPATRONAL` | `?` |  |
| `NUMERORUC` | `?` |  |
| `MAILINSTITUCION` | `?` |  |
| `NUMEROCUENTABANCOCENTRAL` | `?` |  |
| `INDICADORDESCUENTOBCE` | `?` |  |
| `INDICADORINSTITUCIONMUNICIPAL` | `?` |  |
| `NOMBRECOLECTOR` | `?` |  |
| `CEDULACOLECTOR` | `?` |  |
| `TELEFONOCOLECTOR` | `no_colector` |  |
| `DIRECCIONCOLECTOR` | `no_direccion_colector` |  |
| `PORCENTAJECAM` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `NUMEROTELEFONO` | `?` |  |
| `CODIGOPROVINCIACOLECTOR` | `?` |  |
| `CIUDADCOLECTOR` | `ci_ciudad_colector` |  |
| `CODIGOPARROQUIACOLECTOR` | `?` |  |
| `NUMEROTELEFONOCOLECTOR` | `no_colector` |  |
| `TIPODIRECCIONENTREGALISTADOS` | `?` |  |
| `TIPODIRECCIONPAGOS` | `?` |  |
| `INDICADORIMPRESIONESTADOCUENTA` | `?` |  |
| `CODIGOSEGUNELSINEC` | `?` |  |
| `USUARIOMODIFICA` | `?` |  |
| `EMAILCOLECTOR` | `?` |  |
| `DIRECCIONPROVINCIALQUECORRESPONDE` | `?` |  |
| `NUMEROUTE` | `?` |  |
| `NUMEROZONA` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `TIENECONTRATOBCEPARARECAUDACION` | `?` |  |
| `FECHADIRMACONTRATO` | `?` |  |
| `INDICADORCONFIRMACIONBANCOCENTRAL` | `?` |  |
| `ROLACTUALIZACIONINSTITUCION` | `?` |  |
| `FECHAULTIMAACTUALIZACION` | `?` |  |
| `HORAULTIMAACTUALIZACION` | `?` |  |
| `CODIGOUNIDADEJECUTORA` | `?` |  |
| `NUMEROCUENTAROTATIVAINGRESO` | `?` |  |
| `TIPOCUENTAROTATIVAINGRESO` | `?` |  |
| `CODIGOBANCOCUENTAROTATIVAINGRESO` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `TIPOINSTITUCION` | `?` |  |
| `TELEFONOCONVENCIONALINSTITUCION` | `?` |  |
| `CEDULAREPRESENTANTE` | `?` |  |
| `NOMBREREPRESENTANTE` | `?` |  |
| `MAILREPRESENTANTE` | `?` |  |
| `TELEFONOREPRESENTANTE` | `?` |  |
| `TIPOACCESO` | `?` |  |
| `NUMERODOCENTE` | `?` |  |
| `NUMEROBONIFICACION` | `?` |  |
| `NUMEROADMINISTRADOR` | `?` |  |
| `NUMEROALUMNOS` | `?` |  |
| `CODIGOCIRCUITOMINISTERIOEDUCACION` | `?` |  |
| `CODIGODISTRITOMINISTERIOEDUCACION` | `?` |  |
| `CODIGOMODALIDAD` | `?` |  |
| `CODIGOETNIA` | `?` |  |
| `CODIGONACIONALIDAD` | `?` |  |
| `TIPOEDUCACIONMINISTERIOEDUCACION` | `?` |  |
| `CODIGOZONAMINISTERIOEDUCACION` | `?` |  |
| `UNIDADADMINISTRATIVACIRCUITAL` | `?` |  |
| `CODIGOMOTIVOMODIFICACIONINSTITUCION` | `?` |  |
| `CODIGOREGISTROESCOLAR` | `?` |  |

## motivoContableType  ->  `MOTIVOCONTABLE_TYPE`
Tablas legacy origen: `sfct_motivo_cnta_cble`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOEMPRESA` | `?` |  |
| `CODIGOFONDO` | `?` |  |
| `CUENTAAUTOMATICADEBE` | `?` |  |
| `CUENTAAUTOMATICAHABER` | `?` |  |
| `MOTIVO` | `ci_motivo` |  |
| `RUBROROL` | `ci_rubro_rol` |  |
| `TIPOTRANSACCION` | `ci_tipo_transaccion` |  |

## movimientoCuentaType  ->  `MOVIMIENTOCUENTA_TYPE`
Tablas legacy origen: `fctbagru_moti_repo,fctbrubr_rent,sfct_afiliado,sfct_afiliado_referencias,sfct_banco,sfct_motivo,sfct_movimiento`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `SECUENCIAMOVIMIENTO` | `?` |  |
| `CODIGOTIPOMOVIMIENTO` | `?` |  |
| `CODIGOMOTIVO` | `?` |  |
| `FECHAMOVIMIENTO` | `?` |  |
| `FECHAPROCESO` | `?` |  |
| `MONTOMOVIMIENTO` | `?` |  |
| `CODIGORUBRO` | `?` |  |
| `CODIGOFONDO` | `?` |  |
| `CODIGOCUENTACONTABLE` | `?` |  |
| `DESCRIPCION` | `?` |  |
| `CODIGOUSUARIO` | `?` |  |
| `ESTADO` | `ce_estado` |  |
| `CUENTABANCARIA` | `?` |  |
| `CUENTABANCODESTINO` | `?` |  |
| `TIPOPAGO` | `?` |  |
| `NOMBREBENEFICIARIOPAGO` | `?` |  |
| `TIPOCUENTA` | `?` |  |
| `TIPOTRANSACCION` | `ci_tipo_transaccion` |  |
| `CODIGOINSTITUCION` | `?` |  |
| `CODIGOROL` | `?` |  |
| `CODIGOMOTIVORETIRO` | `?` |  |
| `FECHAINICIOAJUSTE` | `?` |  |
| `FECHAFINAJUSTE` | `?` |  |
| `FECHARETIROFCME` | `?` |  |
| `PORCENTAJEDISTRIBUCIONVALORES` | `?` |  |
| `SALDOANTERIOR` | `va_saldo_anterior` |  |
| `FECHACREACIONREGISTRO` | `?` |  |
| `INDICADORMOVIMIENTOCAPITALIZADO` | `?` |  |
| `HORAGENERACIONMOVIMIENTO` | `?` |  |
| `TIPOCOMPROBANTECONTABLE` | `?` |  |
| `CODIGOPROVINCIA` | `?` |  |
| `CODIGOPAGADOR` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |

## movimientoTemporalType  ->  `MOVIMIENTOTEMPORAL_TYPE`
Tablas legacy origen: `sfct_movimiento_temp`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOEMPRESA` | `?` |  |
| `CODIGOINSTITUCION` | `?` |  |
| `CODIGOMOTIVORETIRO` | `?` |  |
| `CODIGOPROVINCIA` | `?` |  |
| `CODIGOROL` | `?` |  |
| `CODIGOUSUARIOCONFIRMA` | `?` |  |
| `CODIGOUSUARIOVERIFICA` | `?` |  |
| `DESCRIPCIONMOVIMIENTO` | `?` |  |
| `ESTADOMOVIMIENTO` | `?` |  |
| `FECHAAUTORIZACION` | `?` |  |
| `FECHACREACIONREGISTRO` | `?` |  |
| `FECHAFINAJUSTE` | `?` |  |
| `FECHAINICIOAJUSTE` | `?` |  |
| `FECHAPROCESO` | `?` |  |
| `FECHARETIROFCME` | `?` |  |
| `FECHAVERIFICACION` | `?` |  |
| `HORAGENERACIONMOVIMIENTO` | `?` |  |
| `INDICADORMOVIMIENTOCAPITALIZADO` | `?` |  |
| `INDICADORMOVIMIENTOIMPRESO` | `?` |  |
| `INDICADORTIPOPROCESO` | `?` |  |
| `MOTIVO` | `ci_motivo` |  |
| `NUMEROCEDULA` | `?` |  |
| `NUMEROCOMPROBANTECONTABLE` | `?` |  |
| `NUMEROTRANSACCION` | `?` |  |
| `CODIGOPAGADOR` | `?` |  |
| `PORCENTAJEDISTRIBUCIONVALORES` | `?` |  |
| `RUBROROL` | `ci_rubro_rol` |  |
| `SALDOANTERIOR` | `va_saldo_anterior` |  |
| `SECUENCIACARGAMOVIMIENTOSMASIVOS` | `?` |  |
| `SECUENCIAMOVIMIENTOND52` | `?` |  |
| `SECUENCIAROL` | `?` |  |
| `TIPOCOMPROBANTECONTABLE` | `?` |  |
| `TIPOINSTITUCION` | `?` |  |
| `TIPOTRANSACCION` | `ci_tipo_transaccion` |  |
| `USUARIOINGRESA` | `?` |  |
| `VALORMOVIMIENTO` | `?` |  |

## naturalInformacionAdicionalType  ->  `NATURALINFORMACIONADICIONALTYPE`
Tablas legacy origen: `fctbafil_info_actu_docs,notbcgfm,notbempl,sfct_afiliado,sfct_banco,sfct_beneficiario,sfct_ciudad,sfct_institucion`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `LICENCIACONDUCIR` | `?` |  |
| `CODIGOTIPOIDENTIFICACIONADICIONAL` | `?` |  |
| `IDENTIFICACIONADICIONAL` | `?` |  |
| `CODIGOPAISNACIMIENTO` | `?` |  |
| `CODIGOPROVINCIANACIMIENTO` | `?` |  |
| `CODIGOCIUDADNACIMIENTO` | `?` |  |
| `LUGARTRABAJO` | `?` |  |
| `NUMEROCARGAS` | `?` |  |
| `CODIGOPROFESION` | `?` |  |
| `CODIGONIVELEDUCACION` | `?` |  |
| `CODIGOFUENTEINGRESO` | `?` |  |
| `MONTOVENTASESPERADO` | `?` |  |
| `CANTIDADEMPLEADOS` | `?` |  |
| `NEGOCIOPROPIO` | `?` |  |
| `CODIGOBARRIONACIMIENTO` | `?` |  |
| `OCUPACARGOPUBLICO` | `?` |  |
| `RELACIONSECTORPUBLICO` | `?` |  |
| `OBLIGADOCONTABILIDAD` | `?` |  |
| `FECHAULTIMADECLARACION` | `?` |  |
| `FECHAINICIONEGOCIO` | `?` |  |
| `NUMEROCARGASESCOLARES` | `?` |  |
| `DISCAPACITADO` | `?` |  |
| `PORCENTAJEDISCAPACIDAD` | `?` |  |
| `OBSERVACIONES` | `ds_observaciones` |  |
| `SEGUNDANACIONALIDAD` | `?` |  |
| `CODIGOIMAGENFOTO` | `?` |  |

## naturalInformacionBasicaType  ->  `NATURALINFORMACIONBASICATYPE`
Tablas legacy origen: `fctbafil_info_actu_docs,notbcgfm,notbempl,sfct_afiliado,sfct_beneficiario,sfct_ciudad`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `GENERO` | `?` |  |
| `CODIGOESTADOCIVIL` | `?` |  |
| `APELLIDOCASADA` | `?` |  |
| `FECHANACIMIENTO` | `?` |  |
| `TIPODOMICILIO` | `?` |  |
| `EMANCIPADO` | `?` |  |
| `HOMONIMIA` | `?` |  |
| `FECHARESIDENCIA` | `?` |  |
| `DEPENDENCIA` | `?` |  |
| `CODIGOTIPOVIVIENDA` | `?` |  |
| `CODIGOETNIA` | `?` |  |

## naturalIngresosEgresosType  ->  `NATURALINGRESOSEGRESOSTYPE`
Tablas legacy origen: `sfct_afiliado_otros,sfct_afiliado_rubro`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `INGRESOEGRESO` | `?` |  |
| `CODIGOTIPOINGRESOEGRESO` | `?` |  |
| `SECUENCIAINGRESOEGRESO` | `?` |  |
| `MONTOMENSUAL` | `?` |  |
| `FIJO` | `?` |  |

## naturalTrabajoType  ->  `NATURALTRABAJOTYPE`
Tablas legacy origen: `fctbafil_actu,sfct_afiliado`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `SECUENCIATRABAJO` | `?` |  |
| `CODIGOCARGOPERSONA` | `?` |  |
| `CODIGOCODIGOCARGO` | `?` |  |
| `FECHAINGRESOTRABAJO` | `?` |  |
| `FECHASALIDA` | `?` |  |
| `NOMBREEMPLEADOR` | `?` |  |
| `PROPIETARIO` | `?` |  |
| `TIPOCONTRATO` | `?` |  |
| `CARGOPUBLICO` | `?` |  |
| `SUELDO` | `va_sueldo` |  |
| `CANTIDADEMPLEADOS` | `?` |  |
| `CODIGOCOCUPACION` | `?` |  |
| `TIEMPOPARCIAL` | `?` |  |

## otrosIngresosAfiliadoType  ->  `OTROSINGRESOSAFILIADO_TYPE`
Tablas legacy origen: `fctbotro_ingr_afil,fctbotro_ingr_cony`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOROL` | `?` |  |
| `CODIGOCEDU` | `?` |  |
| `CODIGOOTROINGRRUBR` | `?` |  |
| `MONTORUBR` | `?` |  |
| `FECHAINGRESO` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `USUARIOMODIFICA` | `?` |  |
| `FECHAELIMINACION` | `?` |  |
| `USUARIOELIMINA` | `?` |  |
| `DESCRIPCIONADIC` | `?` |  |
| `ESTADOREGISTRO` | `?` |  |
| `CODIGOROLOTROINGR` | `?` |  |
| `CODIGOCEDUOTROINGR` | `?` |  |
| `CODIGOCEDUCONY` | `?` |  |
| `CODIGOOTROINGRRUBROTROINGR` | `?` |  |
| `MONTORUBROTROINGR` | `?` |  |
| `FECHAINGRESOOTROINGR` | `?` |  |
| `USUARIOINGRESAOTROINGR` | `?` |  |
| `FECHAMODIFICACIONOTROINGR` | `?` |  |
| `USUARIOMODIFICAOTROINGR` | `?` |  |
| `FECHAELIMINACIONOTROINGR` | `?` |  |
| `USUARIOELIMINAOTROINGR` | `?` |  |
| `DESCRIPCIONADICOTROINGR` | `?` |  |
| `ESTADOREGISTROOTROINGR` | `?` |  |

## personaDireccionesType  ->  `PERSONADIRECCIONESTYPE`
Tablas legacy origen: `cgtbprvd,fctbafil_info_actu_docs,fctbagen_mail,notbempl,sfct_afiliado,sfct_banco,sfct_ciudad,sfct_institucion`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `NUMERODIRECCION` | `?` |  |
| `CODIGOTIPODIRECCION` | `?` |  |
| `CODIGOPAIS` | `?` |  |
| `CODIGOPROVINCIA` | `?` |  |
| `CODIGOCIUDAD` | `?` |  |
| `DIRECCIONPRINCIPAL` | `?` |  |
| `CALLE` | `?` |  |
| `NUMERO` | `?` |  |
| `INMUEBLE` | `?` |  |
| `DEPARTAMENTO` | `?` |  |
| `URBANIZACION` | `?` |  |
| `DIRECCION` | `no_direccion` |  |
| `FECHAINGRESO` | `?` |  |
| `FECHAINGRESORESIDENCIA` | `?` |  |
| `OBSERVACIONES` | `ds_observaciones` |  |
| `CODIGOPARROQUIA` | `?` |  |
| `CODIGOCANTON` | `?` |  |
| `CODIGOBARRIO` | `?` |  |
| `TRANSVERSAL` | `?` |  |
| `CODIGOTIPOSITIO` | `?` |  |
| `SECTOR` | `ti_sector` |  |
| `CODIGOZIP5` | `?` |  |
| `NOMBREPROPIETARIO` | `?` |  |

## personaReferenciasBancariasType  ->  `PERSONAREFERENCIASBANCARIASTYPE`
Tablas legacy origen: `cgtbprvd,sfct_afiliado,sfct_afiliado_referencias,sfct_banco`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `SECUENCIAREFERENCIABANCARIA` | `?` |  |
| `CODIGOTIPOCUENTAREFERENCIA` | `?` |  |
| `TIPOIDENTIFICACIONIFINANCIERA` | `?` |  |
| `IDENTIFICACIONIFINANCIERA` | `?` |  |
| `NUCEMPRESABANCARIA` | `?` |  |
| `CODIGOCUENTA` | `?` |  |
| `NOMBRETITULAR` | `?` |  |
| `NUMEROCIFRAS` | `?` |  |
| `CODIGOCIFRASALDO` | `?` |  |
| `FECHAAPERTURA` | `?` |  |
| `NUMEROPROTESTOS` | `?` |  |
| `CERRADA` | `?` |  |
| `NOMBREINSTITUCIONPARACACEL` | `?` |  |
| `OBSERVACIONESSOLOPARACACEL` | `?` |  |

## personaReferenciasPersonalesType  ->  `PERSONAREFERENCIASPERSONALESTYPE`
Tablas legacy origen: `fctbafil_ahor_refe`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `SECUENCIAREFERENCIAPERSONAL` | `?` |  |
| `NOMBRESPERSONA` | `?` |  |
| `APELLIDOPATERNO` | `?` |  |
| `APELLIDOMATERNO` | `?` |  |
| `DIRECCION` | `?` |  |
| `TELEFONO` | `?` |  |
| `CODIGOTIPOVINCULACION` | `?` |  |
| `IDENTIFICACIONREFERENCIA` | `?` |  |

## personaTelefonosType  ->  `PERSONATELEFONOSTYPE`
Tablas legacy origen: `fctbafil_actu,fctbagen_telf_part,sfct_afiliado`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `SECUENCIATELEFONO` | `?` |  |
| `CODIGOTIPOTELEFONO` | `?` |  |
| `NUMEROTELEFONO` | `?` |  |
| `EXTENSION` | `?` |  |
| `CODIGOTIPOUBICACION` | `?` |  |
| `NUMERODIRECCION` | `?` |  |
| `FECHAINGRESO` | `?` |  |
| `EMPRESAOPERADORA` | `?` |  |
| `CODIGOAREA` | `?` |  |

## personaType  ->  `PERSONATYPE`
Tablas legacy origen: `cgtbprvd,fctbafil_info_actu_docs,notbcgfm,notbempl,sfct_afiliado,sfct_banco,sfct_beneficiario,sfct_ciudad,sfct_institucion`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `CODIGOTIPOPERSONA` | `?` |  |
| `CODIGOSECTORECONOMICO` | `?` |  |
| `PRIMERAPELLIDO` | `?` |  |
| `SEGUNDOAPELLIDO` | `?` |  |
| `PRIMERNOMBRE` | `?` |  |
| `SEGUNDONOMBRE` | `?` |  |
| `NOMBRELEGAL` | `?` |  |
| `FECHAINGRESO` | `?` |  |
| `CODIGOPAIS` | `?` |  |
| `CODIGORESIDENCIA` | `?` |  |
| `CODIGOACTIVIDAD` | `?` |  |
| `CODIGOACTIVIDADDETALLE` | `?` |  |
| `OBSERVACIONES` | `ds_observaciones` |  |
| `SUCURSALINGRESO` | `?` |  |
| `OFICINAINGRESO` | `?` |  |
| `CODIGOESTATUSPERSONA` | `?` |  |
| `NUMEROSOCIO` | `?` |  |
| `CODIGOUSUARIOOFICIALPERSONA` | `?` |  |
| `CODIGOCATEGORIATRATO` | `?` |  |
| `CODIGORAZONAPERTURAFINALIDAD` | `?` |  |
| `SUJETOOBLIGADO` | `?` |  |
| `NOMBREPREFERIDO` | `?` |  |
| `EXONERADOIMPUESTO` | `?` |  |
| `FECHACALIFICADO` | `?` |  |

## personaVinculacionesType  ->  `PERSONAVINCULACIONESTYPE`
Tablas legacy origen: `crtboper_cony,crtoblig,imtbmiem_cony,notbempl,sfct_afiliado,sfct_beneficiario,sfct_conyuge`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA` | `?` |  |
| `IDENTIFICACIONPERSONAVINCULADA` | `?` |  |
| `CODIGOTIPOVINCULACION` | `?` |  |
| `SECUENCIAPERSONAVINCULACION` | `?` |  |
| `FECHAVINCULACION` | `?` |  |
| `FECHASEPARACION` | `?` |  |

## referenciaParticipeType  ->  `REFERENCIAPARTICIPE_TYPE`
Tablas legacy origen: `sfct_referencias`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOREFERENCIA` | `?` |  |
| `DESCRIPCIONTIPOREFERENCIA` | `?` |  |

## reporteSIBSParticipeType  ->  `REPORTESIBSPARTICIPE_TYPE`
Tablas legacy origen: `fctbcinf_part_sibs,fctbdinf_liqd_cnta_sibs,fctbdinf_part_sibs`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOENTIDAD` | `?` |  |
| `CODIGOESTRUCTURA` | `?` |  |
| `FECHACORTE` | `?` |  |
| `FECHAGENERACION` | `?` |  |
| `NUMEROREGISTRO` | `?` |  |
| `SECUENCIAREGISTRO` | `?` |  |
| `NUMEROCEDULA` | `?` |  |
| `TIPOIDENTIFICACION` | `?` |  |
| `CODIGOTIPOAPORTANTE` | `?` |  |
| `MONTOAPORTE` | `?` |  |
| `SALDODISPONIBLE` | `?` |  |
| `SALDOBLOQUEADO` | `?` |  |
| `MONTOLIQUIDACION` | `?` |  |
| `FECHALIQUIDACION` | `?` |  |
| `ESTADO` | `?` |  |
| `CODIGOUSUARIOGENERACION` | `?` |  |

## retiroLiquidacionType  ->  `RETIROLIQUIDACION_TYPE`
Tablas legacy origen: `sfct_afiliado_referencias,sfct_retiro`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `SECUENCIARETIRO` | `?` |  |
| `CODIGOTIPORETIRO` | `?` |  |
| `CODIGOMOTIVORETIRO` | `?` |  |
| `FECHASOLICITUD` | `?` |  |
| `FECHAAPROBACION` | `?` |  |
| `FECHALIQUIDACION` | `?` |  |
| `MONTOSOLICITADO` | `?` |  |
| `MONTOAPROBADO` | `?` |  |
| `MONTOLIQUIDADO` | `?` |  |
| `CODIGOESTADO` | `?` |  |
| `CODIGOCUENTADESTINO` | `?` |  |
| `CODIGOBANCODESTINO` | `?` |  |
| `OBSERVACIONES` | `?` |  |
| `CODIGOUSUARIOAPRUEBA` | `?` |  |
| `CODIGOUSUARIOLIQUIDA` | `?` |  |
| `FECHAINGRESO` | `?` |  |
| `MONTOSALDO` | `?` |  |
| `MONTOINTERESGENERADO` | `?` |  |
| `PORCENTAJETASAINTERES` | `?` |  |
| `TIPOLIQUIDACION` | `?` |  |
| `SECUENCIALIQUIDACIONHIPO` | `?` |  |
| `TIPOPROCESO` | `?` |  |
| `CODIGOROL` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `CODIGOINSTITUCION` | `?` |  |
| `CODIGOPROVINCIA` | `?` |  |
| `CODIGOPAGADOR` | `?` |  |
| `MONTOFAS` | `?` |  |
| `MONTOCAPITALINSTITUCIONAL` | `?` |  |
| `VALORSALDOINCIAL` | `?` |  |
| `FECHARETIROFCME` | `?` |  |
| `FECHACONCESION` | `?` |  |
| `VALORCREDITO` | `?` |  |
| `VALORINTERESCAPITALINICIAL` | `?` |  |
| `VALORINTERESACCIONES` | `?` |  |
| `MOTIVO` | `ci_motivo_retiro` |  |
| `FECHAVERIFICACION` | `?` |  |
| `CODIGOUSUARIOCONFIRMA` | `?` |  |
| `USUARIOELIMINA` | `?` |  |
| `FECHAELIMINACION` | `?` |  |
| `CODIGOUSUARIOAUTORIZAPROVISION` | `?` |  |
| `FECHAAUTORIZAPROVISION` | `?` |  |
| `ESTADOANTERIOR` | `ce_estado_anterior` |  |
| `CODIGOFONDO` | `?` |  |
| `VALORRESEVAS` | `?` |  |
| `VALORADICIONAL` | `?` |  |
| `VALORRETENCION` | `?` |  |
| `VALORCONSULCREDITO` | `?` |  |
| `SOBRANTEQUESELIQUIDA` | `?` |  |
| `MONTODESCUENTOGASTOSJUBILACION` | `?` |  |
| `MONTOPAGOCREDITOOTROFONDO` | `?` |  |
| `MONTOPAGOSOBREGIROOTROFONDO` | `?` |  |
| `VALORCREDITOSCONSULCREDITO` | `?` |  |
| `MONTOAPERTURACUP` | `?` |  |
| `APORTEROLPARAAPERTURACUP` | `?` |  |
| `MONTOINVERSIONHIDROELECTRICA` | `?` |  |
| `MONTOCAPITALIZACIONCDP` | `?` |  |
| `MONTORENTABILIDADCDP` | `?` |  |
| `MONTOGARANTIAPORCREDITO` | `?` |  |
| `MONTOGARANTIAPORCUP` | `?` |  |
| `CODIGOTASACUP` | `?` |  |
| `CODIGOPLAZACUP` | `?` |  |
| `CODIGOTIPOCAPITALIZACION` | `?` |  |
| `MONTORENTABILIDADCUP` | `?` |  |
| `CEDULAUSUARIOHIDROELECTRICA` | `?` |  |
| `CEDULAUSUARIOCAPTACUP` | `?` |  |
| `CODIGOAPLICACIONORIGEN` | `?` |  |
| `COBROPRESTANO` | `?` |  |
| `CODIGOPROCESO` | `?` |  |
| `INDICADORPROCESO` | `?` |  |

## retiroVoluntarioEstadoType  ->  `RETIROVOLUNTARIOESTADO_TYPE`
Tablas legacy origen: `fctbrvol_esta_afil`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `ANIO` | `nu_anio` |  |
| `CODIGOEMPRESA` | `?` |  |
| `CODIGOFONDO` | `?` |  |
| `ESTADOAFILIADO` | `?` |  |
| `ESTADOREGISTRO` | `?` |  |
| `FECHACREACION` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `SECUENCIADETALLE` | `?` |  |
| `TIPORETIROVOLUNTARIO` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `USUARIOMODIFICA` | `?` |  |
| `CODIGOINSTITUCION` | `?` |  |

## rolNominaType  ->  `ROLNOMINA_TYPE`
Tablas legacy origen: `sfct_cabecera_rol,sfct_detalle_rol,sfct_rubro_rol`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `CODIGOROL` | `?` |  |
| `PERIODOANIO` | `?` |  |
| `PERIODOMES` | `?` |  |
| `CODIGORUBRO` | `?` |  |
| `DESCRIPCIONRUBRO` | `?` |  |
| `MONTORUBRO` | `?` |  |
| `CODIGOTIPORUBRO` | `?` |  |
| `INDICADORDESCUENTO` | `?` |  |
| `ESTADO` | `?` |  |
| `FECHAPROCESO` | `?` |  |
| `SECUENCIAROL` | `?` |  |
| `CODIGOCATEGORIA` | `?` |  |
| `INDICADORCONTABLE` | `?` |  |
| `NOMBRERUBROABREVIADO` | `?` |  |
| `NUMEROPRIORIDAD` | `?` |  |
| `ROLINICIAL` | `ci_rol_inicial` |  |
| `NUMERODEBITOS` | `?` |  |
| `CUENTACONTABLEAUTOMATICAMOVIMIENTO` | `?` |  |
| `CUENTACONTABLEAUTOMATICATRANSACCION` | `?` |  |
| `TOTAL` | `tx_total` |  |
| `RUBROACUMULADOR` | `?` |  |
| `INDICADORINTERES` | `?` |  |
| `INDICADORRUBROCONSALDOUNIFICADO` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `TIPODATODESCUENTO` | `?` |  |
| `ESTADOSDEUDOR` | `?` |  |
| `TIPOINSTITUCION` | `?` |  |
| `CODIGOINSTITUCION` | `?` |  |
| `CODIGOPROVINCIA` | `?` |  |

## saldoDiarioRubroType  ->  `SALDODIARIORUBRO_TYPE`
Tablas legacy origen: `fctbsald_diar_rubr`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `FECHASALDO` | `?` |  |
| `CODIGOTIPOTRANSACCION` | `?` |  |
| `CODIGOMOTIVO` | `?` |  |
| `CODIGORUBROROL` | `?` |  |
| `VASALDO` | `va_saldo` |  |
| `CODIGOEMPRESA` | `?` |  |

## saldoDiarioType  ->  `SALDODIARIO_TYPE`
Tablas legacy origen: `fctbrubr_rent,fctbsald_diar_afil_rubr,sfct_saldos_diarios_afiliados`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `FECHASALDO` | `?` |  |
| `CODIGORUBRO` | `?` |  |
| `CODIGOFONDO` | `?` |  |
| `SALDOANTERIOR` | `?` |  |
| `MONTODEBITO` | `?` |  |
| `MONTOCREDITO` | `?` |  |
| `SALDOACTUAL` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |

## seguroVidaParticipeType  ->  `SEGUROVIDAPARTICIPE_TYPE`
Tablas legacy origen: `svtbcaus,svtbdisc,svtbefec,svtbfmpg,svtbstro,svtbstro_bene,svtbstro_cred,svtbstro_deta,svtbstro_exte`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOSECUENCIACAUSAFALLECIMIENTO` | `?` |  |
| `DESCRIPCIONCAUSAFALLECIMIENTO` | `?` |  |
| `ESTADOCAUSA` | `?` |  |
| `FECHACREACION` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `USUARIOMODIFICA` | `?` |  |
| `CODIGOFONDO` | `?` |  |
| `CODIGOTIPOFAMILIAR` | `?` |  |
| `CODIGOTIPOPLAN` | `?` |  |
| `EDADPROVEEDOR` | `?` |  |
| `ESTADOCOBERTURA` | `?` |  |
| `FECHAAFILIACIONCAM` | `?` |  |
| `FECHAFINALVIGENCIATASA` | `?` |  |
| `FECHAINICIO` | `?` |  |
| `MONTOCOBERTURA` | `?` |  |
| `NUMEROANIOSAFILIACIONFCME` | `?` |  |
| `TIPOCOBERTURA` | `?` |  |
| `CODIGODISCAPACIDADFAMILIARES` | `?` |  |
| `DISCAPACIDADFAMILIARESAFILIADO` | `?` |  |
| `ESTADODISCAPACIDAD` | `?` |  |
| `CODIGOEFECTO` | `?` |  |
| `DESCRIPCIONEFECTO` | `?` |  |
| `ESTADOEFECTO` | `?` |  |
| `TIPOEFECTO` | `?` |  |
| `DESCRIPCIONFORMAPAGO` | `?` |  |
| `ESTADOFORMAPAGO` | `?` |  |
| `TIPOFORMAPAGO` | `?` |  |
| `CEDULAIDENTIDADAFILIADO` | `?` |  |
| `CEDULAIDENTIDADSINIESTRADO` | `?` |  |
| `CODIGOBANCOELCUALREALIZAPAGO` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `CODIGOPROVINCIA` | `?` |  |
| `CODIGOUSUARIOCONFIRMA` | `?` |  |
| `ESTADOANTERIORSINIESTRADO` | `?` |  |
| `ESTADOSINESTRO` | `?` |  |
| `FECHAAUTORIZACION` | `?` |  |
| `FECHAELIMINACION` | `?` |  |
| `FECHAFALLECIMIENTO` | `?` |  |
| `FECHANOTIFICACIONSINIESTRO` | `?` |  |
| `FECHAPRESENTACIONPAPELESSINIESTRADO` | `?` |  |
| `FECHAVERIFICACION` | `?` |  |
| `SECUENCIASINIESTRO` | `?` |  |
| `TIPOSINIESTRO` | `?` |  |
| `USUARIOAUTORIZACION` | `?` |  |
| `USUARIOELIMINA` | `?` |  |
| `CEDULABENEFICIARIO` | `?` |  |
| `CODIGOTIPOFAMILIARSINIESTRADO` | `?` |  |
| `MONTOQUERECIBIOBENEFICIARIO` | `?` |  |
| `PORCENTAJEDISTRUBUCION` | `?` |  |
| `ABONOPROPUESTOCREDITODESGRAVAMEN` | `?` |  |
| `ANIOCREDITO` | `?` |  |
| `INDICADORPAGODESGRAVAMEN` | `?` |  |
| `MONTOCREDITOACANCELAR` | `?` |  |
| `MONTOPAGARSOBREGIROOTROFONDO` | `?` |  |
| `SECUENCIACREDITO` | `?` |  |
| `TIPOCREDITO` | `?` |  |
| `VALORDESGRAVAMEN` | `?` |  |
| `MONTOCUBIERTOPORDESGRAVAMEN` | `?` |  |
| `MONTONOCUBIERTOCREDITO` | `?` |  |
| `MONTOPAGOSOBREGIROOTROFONDO` | `?` |  |
| `MONTOPARACUBRIRSALDOCREDITO` | `?` |  |
| `MONTOREALAPERCIBIRPORSINIESTRO` | `?` |  |
| `ESTADOSINIESTROEXTEMPORANEO` | `?` |  |
| `FECHASINIESTRO` | `?` |  |

## servicioAdicionalType  ->  `SERVICIOADICIONAL_TYPE`
Tablas legacy origen: `fctbcser_adic,fctbesta_civi,fctbgene_sibs,fctbpara_serv_adic,sfct_afiliado`

| oracle_col | sugerencia | mapeo_final |
|---|---|---|
| `CODIGOTIPOIDENTIFICACION` | `?` |  |
| `IDENTIFICACION` | `?` |  |
| `SECUENCIASERVICIO` | `?` |  |
| `CODIGOTIPOSERVICIO` | `?` |  |
| `DESCRIPCIONSERVICIO` | `?` |  |
| `MONTOSERVICIO` | `?` |  |
| `FECHAINICIO` | `?` |  |
| `FECHAFIN` | `?` |  |
| `ESTADO` | `ce_estado` |  |
| `CODIGOUSUARIOAUTORIZA` | `?` |  |
| `FECHAAUTORIZACION` | `?` |  |
| `CODIGOEMPRESA` | `?` |  |
| `SECUENCIA` | `?` |  |
| `NUMEROCEDULARECIBECOREO` | `?` |  |
| `TIPOSERVICIO` | `?` |  |
| `USUARIOINGRESA` | `?` |  |
| `FECHACREACION` | `?` |  |
| `USUARIOAUTORIZACION` | `?` |  |
| `USUARIOMODIFICA` | `?` |  |
| `FECHAMODIFICACION` | `?` |  |
| `ESTADOREGISTRO` | `?` |  |

