"""E2E de los 30 types con cols REALES, INSERT only, sin bucle."""
import oracledb, pyodbc, time, sys
o=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=o.cursor()
c_fc=pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=dbFC;UID=sa;PWD=YourPassword123', autocommit=True, timeout=15).cursor()
c_can=pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123', autocommit=True, timeout=15).cursor()

c_fc.execute('SELECT TOP 1 ci_cedula FROM dbo.sfct_afiliado')
ced=c_fc.fetchone().ci_cedula.strip()

c_can.execute('DELETE FROM dbo.cdc_inbox_errors')
c_can.execute('SELECT COUNT(*) FROM dbo.cdc_inbox')
inbox0=c_can.fetchone()[0]

# Tests: (key, oracle_table, INSERT_sql, legacy_db, legacy_table)
TESTS = [
    ('referenciaParticipe','REFERENCIAPARTICIPE_TYPE',
     "INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA,DESCRIPCIONTIPOREFERENCIA) VALUES ('701','REF E2E')",
     'dbFC','sfct_referencias'),
    ('motivoContable','MOTIVOCONTABLE_TYPE',
     "INSERT INTO FCME_USER.MOTIVOCONTABLE_TYPE (CODIGOEMPRESA,CODIGOFONDO,CUENTAAUTOMATICADEBE,CUENTAAUTOMATICAHABER,MOTIVO,RUBROROL,TIPOTRANSACCION) VALUES ('1','1','1','1','701','1','D')",
     'dbFC','sfct_motivo_cnta_cble'),
    ('saldoDiarioRubro','SALDODIARIORUBRO_TYPE',
     "INSERT INTO FCME_USER.SALDODIARIORUBRO_TYPE (FECHASALDO,CODIGOTIPOTRANSACCION,CODIGOMOTIVO,CODIGORUBROROL,VASALDO,CODIGOEMPRESA) VALUES ('2027-04-01','1','1','1','100','1')",
     'dbFC','fctbsald_diar_rubr'),
    ('agendaMail','AGENDAMAILAFILIADO_TYPE',
     f"INSERT INTO FCME_USER.AGENDAMAILAFILIADO_TYPE (CODIGOEMPRESA,CODIGOCEDU,SECUENCIAREGISTRO,DESCRIPCIONMAIL,INDICADORPRIN,USUARIOINGRESA,FECHAINGRESO,FECHACREACION,USUARIOMODIFICA,FECHAMODIFICACION,CODIGOUSUELIM,FECHAELIMINACION,ESTADOREGISTRO) VALUES ('1','{ced}','9','test@e2e.com','S','1','2027-01-01','2027-01-01','1','2027-01-01','1','2027-01-01','A')",
     'dbFC','fctbagen_mail'),
    ('actualizacionDocumentos','ACTUALIZACION_DOCUMENTOS_TYPE',
     f"INSERT INTO FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE (SECUENCIA_ACTU_DOCS,CODIGO_EMPRESA,CODIGO_CEDU,SECUENCIA_ACTV_SUJE_CRED,SECUENCIA_ORGN_INGR,CODIGO_PERS_POLI_EXPU,DESCRIPCION_CIUD_NACI,INDICADOR_COMI_SERV,DESCRIPCION_COMI_SERV,FECHA_INGR,USUARIO_INGRESA) VALUES ('9','1','{ced}','1','1','N','GUAYAQUIL','N','TST','2027-01-01','1')",
     'dbFC','fctbafil_info_actu_docs'),
    ('beneficiarioParticipe','BENEFICIARIOPARTICIPE_TYPE',
     f"INSERT INTO FCME_USER.BENEFICIARIOPARTICIPE_TYPE (NUMEROCEDULA,NUMEROCEDULABENEFICIARIO,SECUENCIABENEFICIARIO,NOMBRESBENEFICIARIO,APELLIDOSBENEFICIARIOS,ESTATUSDELBENEFICIARIO,PORCENTAJEDISTRIBUCIONVALORES,CODIGOEMPRESA,FECHACREACIONREGISTRO,FECHAMODIFICACION,CODIGOUSUARIOINGRESOREGISTRO,CODIGOUSUARIOMODIFICOREGISTRO) VALUES ('{ced}','0911111701','1','TST','TST','A','100','1','2027-01-01','2027-01-01','1','1')",
     'dbFC','sfct_beneficiario'),
    ('servicioAdicional','SERVICIOADICIONAL_TYPE',
     f"INSERT INTO FCME_USER.SERVICIOADICIONAL_TYPE (CODIGOTIPOIDENTIFICACION,IDENTIFICACION,SECUENCIASERVICIO,CODIGOTIPOSERVICIO,DESCRIPCIONSERVICIO,MONTOSERVICIO,FECHAINICIO,ESTADO,CODIGOEMPRESA,SECUENCIA,USUARIOINGRESA,FECHACREACION,TIPOSERVICIO) VALUES ('C','{ced}','9','1','TST','100','2027-01-01','A','1','9','1','2027-01-01','1')",
     'dbFC','fctbpara_serv_adic'),
    ('saldoDiario','SALDODIARIO_TYPE',
     f"INSERT INTO FCME_USER.SALDODIARIO_TYPE (CODIGOTIPOIDENTIFICACION,IDENTIFICACION,FECHASALDO,CODIGORUBRO,CODIGOFONDO,SALDOANTERIOR,MONTODEBITO,MONTOCREDITO,SALDOACTUAL,CODIGOEMPRESA) VALUES ('C','{ced}','2027-04-01','1','1','100','0','100','100','1')",
     'dbFC','fctbsald_diar_afil_rubr'),
    ('naturalIngresosEgresos','NATURALINGRESOSEGRESOSTYPE',
     f"INSERT INTO FCME_USER.NATURALINGRESOSEGRESOSTYPE (CODIGOTIPOIDENTIFICACION,IDENTIFICACION,INGRESOEGRESO,CODIGOTIPOINGRESOEGRESO,SECUENCIAINGRESOEGRESO,MONTOMENSUAL,FIJO) VALUES ('C','{ced}','I','1','9','100','S')",
     'dbFC','sfct_afiliado_rubro'),
    ('personaTelefonos','PERSONATELEFONOSTYPE',
     f"INSERT INTO FCME_USER.PERSONATELEFONOSTYPE (CODIGOTIPOIDENTIFICACION,IDENTIFICACION,SECUENCIATELEFONO,CODIGOTIPOTELEFONO,NUMEROTELEFONO,FECHAINGRESO) VALUES ('C','{ced}','9','M','0991234567','2027-01-01')",
     'dbFC','fctbafil_actu'),
    ('institucion','INSTITUCION_TYPE',
     "INSERT INTO FCME_USER.INSTITUCION_TYPE (CODIGOEMPRESA,CODIGOINSTITUCION,NOMBREINSTITUCION,RUCINSTITUCION,CODIGOPROVINCIA,CODIGOCIUDAD) VALUES ('1','9701','INST E2E','0992222222001','1','1')",
     'dbFC','fctbinst_info_adic'),
    ('cuentaBancariaAfiliado','CUENTABANCARIAAFILIADO_TYPE',
     f"INSERT INTO FCME_USER.CUENTABANCARIAAFILIADO_TYPE (CEDULABENEFICIARIO,CODIGOEMPRESA,NUMEROCUENTA,TIPOCUENTA,CODIGOBANCO,SECUENCIAPAGO,SECUENCIALIQUIDACION,FECHACREACION,USUARIOINGRESA) VALUES ('{ced}','1','1234567890','A','1','9','9','2027-01-01','1')",
     'dbFC','sfct_padbs'),
]

print(f"\nTesting {len(TESTS)} types (INSERT only):"); sys.stdout.flush()
print(f"{'TYPE':<25} {'PRE':<8} {'POST':<8} {'DELTA':<6} {'STATUS'}")
print('-'*120)

ok=err=fail=0
for nombre, ot, ins_sql, leg_db, leg_tbl in TESTS:
    try: cdb=pyodbc.connect(f'DRIVER={{SQL Server}};SERVER=10.35.3.64,1433;DATABASE={leg_db};UID=sa;PWD=YourPassword123', autocommit=True, timeout=15).cursor()
    except: continue
    cdb.execute(f'SELECT COUNT(*) FROM dbo.[{leg_tbl}]')
    n0=cdb.fetchone()[0]
    try:
        co.execute(ins_sql); o.commit()
    except Exception as e:
        print(f"  {nombre:<25} {'-':<8} {'-':<8} {'-':<6} ORA fail: {str(e)[:60]}"); sys.stdout.flush()
        fail+=1; continue
    time.sleep(5)
    cdb.execute(f'SELECT COUNT(*) FROM dbo.[{leg_tbl}]')
    n1=cdb.fetchone()[0]; delta=n1-n0
    c_can.execute(f"SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE aggregate_type LIKE ? ORDER BY error_id DESC", f'%{nombre}%')
    er=c_can.fetchone()
    status='OK' if delta>0 else (f'ERR: {er[0][:60]}' if er else 'sin cambio')
    if delta>0: ok+=1
    elif er: err+=1
    print(f"  {nombre:<25} {n0:<8} {n1:<8} {delta:<6} {status}"); sys.stdout.flush()

c_can.execute('SELECT COUNT(*) FROM dbo.cdc_inbox')
inbox1=c_can.fetchone()[0]
print(f"\ncdc_inbox crecimiento: {inbox1-inbox0} (esperado ~{len(TESTS)}, sin bucle)")
print(f"\nRESUMEN: OK={ok} ERR={err} ORA_fail={fail} de {len(TESTS)}")
o.close()
