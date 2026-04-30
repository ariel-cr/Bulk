"""Test e2e de los 30 types con INSERT que apunte a guardar en legacy."""
import pyodbc, oracledb, time

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db, retries=3):
    for i in range(retries):
        try:
            return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=15)
        except Exception as e:
            if i == retries-1: raise
            time.sleep(3)

orcl=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=orcl.cursor()
c_can=sql('fcme_canonicos').cursor()
c_fc=sql('dbFC').cursor()

# Cedula real para FK
c_fc.execute("SELECT TOP 1 ci_cedula FROM dbo.sfct_afiliado WHERE ci_cedula IS NOT NULL")
ced = c_fc.fetchone().ci_cedula.strip()
print(f"Cedula real para FK: {ced}\n")

c_can.execute("DELETE FROM dbo.cdc_inbox_errors")

# 30 INSERTs - cada uno con datos pensados para pasar validaciones
INSERTS = [
    # (type, oracle_table, oracle_INSERT_sql, [legacy_db, legacy_table, expected_pk_col, expected_pk_value])
    ('referenciaParticipeType', 'REFERENCIAPARTICIPE_TYPE',
     "INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('301','E2E REF')",
     ('dbFC','sfct_referencias','co_tref',301)),
    ('motivoContableType', 'MOTIVOCONTABLE_TYPE',
     "INSERT INTO FCME_USER.MOTIVOCONTABLE_TYPE (CODIGOEMPRESA,CODIGOFONDO,CODIGOMOTIVO,DESCRIPCION,TIPOMOVIMIENTO,ESTADO,CODIGOMODULO,CODIGOAUXILIAR) VALUES ('1','1','301','E2E MOTIVO','D','A','P','1')",
     ('dbFC','sfct_motivo_cnta_cble','ci_moti','301')),
    ('grupoFamiliarType', 'GRUPOFAMILIAR_TYPE',
     f"INSERT INTO FCME_USER.GRUPOFAMILIAR_TYPE (CODIGOTIPOIDENTIFICACION,IDENTIFICACION,CEDULAFAMILIAR,NOMBRESGRUPOFAMILIAR,APELLIDOSGRUPOFAMILIAR,ESTADOFAMILIAR,FECHANACIMIENTO,FECHACREACIONREGISTRO,FECHAMODIFICACION,INDICADORDISCAPACIDAD,CODIGOUSUARIOINGRESOREGISTRO,CODIGOUSUARIOMODIFICOREGISTRO,TIPOCREDITO,TIPOREALCIONFAMILIAR,NUMEROCEDULA) VALUES ('C','{ced}','0922222301','TST','TST','A','1990-01-01','2027-01-01','2027-01-01','N','1','1','P','C','{ced}')",
     ('dbFC','sfct_grupo_fami','ci_cedula_familiar','0922222301')),
    ('firmanteParticipeType', 'FIRMANTEPARTICIPE_TYPE',
     f"INSERT INTO FCME_USER.FIRMANTEPARTICIPE_TYPE (CODIGOEMPRESA,CODIGOTIPOIDENTIFICACION,IDENTIFICACION,SECUENCIAFIRMANTE,NOMBREFIRMANTE,APELLIDOFIRMANTE) VALUES ('1','C','{ced}','9','TST','TST')",
     ('dbFC','sfct_firmante','ci_cedula',ced)),
    ('auditoriaAfiliadoType', 'AUDITORIAAFILIADO_TYPE',
     f"INSERT INTO FCME_USER.AUDITORIAAFILIADO_TYPE (CODIGOEMPRESA,CODIGOTIPOIDENTIFICACION,IDENTIFICACION,SECUENCIAAUDITORIA,FECHAMODIFICACION,CAMPOMODIFICADO,VALORANTERIOR,VALORNUEVO,CODIGOUSUARIO,TIPOOPERACION,ESTADO) VALUES ('1','C','{ced}','9','2027-01-01','TST','A','B','1','U','A')",
     ('dbFC','fctbaudi_actu_afil','ci_cedu',ced)),
    ('agendaMailAfiliadoType', 'AGENDAMAILAFILIADO_TYPE',
     f"INSERT INTO FCME_USER.AGENDAMAILAFILIADO_TYPE (CODIGOEMPRESA,CODIGOCEDU,SECUENCIAREGISTRO,DESCRIPCIONMAIL,INDICADORPRIN,USUARIOINGRESA,FECHACREACION,FECHAINGRESO,USUARIOMODIFICA,FECHAMODIFICACION,USUARIOELIMINA,FECHAELIMINACION) VALUES ('1','{ced}','9','test@test.com','S','1','2027-01-01','2027-01-01','1','2027-01-01','1','2027-01-01')",
     ('dbFC','fctbagen_mail','ci_cedu',ced)),
    ('cuentaBancariaAfiliadoType', 'CUENTABANCARIAAFILIADO_TYPE',
     f"INSERT INTO FCME_USER.CUENTABANCARIAAFILIADO_TYPE (CODIGOEMPRESA,CODIGOTIPOIDENTIFICACION,IDENTIFICACION,SECUENCIACUENTA,CODIGOBANCO,NUMEROCUENTA,TIPOCUENTA,ESTADO) VALUES ('1','C','{ced}','9','1','1234567890','A','A')",
     ('dbFC','sfct_padbs','ci_cedu',ced)),
    ('movimientoCuentaType', 'MOVIMIENTOCUENTA_TYPE',
     f"INSERT INTO FCME_USER.MOVIMIENTOCUENTA_TYPE (CODIGOEMPRESA,CODIGOFONDO,CODIGOTIPOIDENTIFICACION,IDENTIFICACION,SECUENCIAMOVIMIENTO,FECHAMOVIMIENTO,MONTO,CODIGOMOTIVO,TIPOMOVIMIENTO,ESTADO) VALUES ('1','1','C','{ced}','9','2027-01-01','100','1','D','A')",
     ('dbFC','sfct_movimiento','ci_cedu',ced)),
]

print(f"Probando {len(INSERTS)} types representativos...")
print(f"{'TYPE':<35} {'oracle':<10} {'legacy_delta':<14} {'STATUS'}")
print('-'*120)

for at, ot, ins_sql, (leg_db, leg_tbl, leg_pk, leg_pk_val) in INSERTS:
    try:
        c_db = sql(leg_db).cursor()
        c_db.execute(f"SELECT COUNT(*) FROM dbo.[{leg_tbl}]")
        n0 = c_db.fetchone()[0]
    except Exception as e:
        print(f"  {at:<35} pre-count fail: {str(e)[:60]}")
        continue

    # Insert Oracle
    try:
        co.execute(ins_sql)
        orcl.commit()
        ora_status = 'OK'
    except Exception as e:
        msg = str(e)[:90]
        print(f"  {at:<35} ORA fail   {msg}")
        continue

    time.sleep(5)

    # Verificar legacy
    try:
        c_db.execute(f"SELECT COUNT(*) FROM dbo.[{leg_tbl}]")
        n1 = c_db.fetchone()[0]
        delta = n1 - n0
    except:
        delta = '?'

    c_can.execute("SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE aggregate_type=? ORDER BY error_id DESC", at)
    er = c_can.fetchone()

    if delta and delta > 0:
        # Buscar la fila concreta
        try:
            c_db.execute(f"SELECT TOP 1 * FROM dbo.[{leg_tbl}] WHERE CONVERT(NVARCHAR(50),[{leg_pk}])=?", str(leg_pk_val))
            r = c_db.fetchone()
            sample = str(r[:3]) if r else 'no rows match PK'
        except:
            sample = ''
        print(f"  {at:<35} {ora_status:<10} +{delta:<13} OK -> {sample[:70]}")
    elif er:
        print(f"  {at:<35} {ora_status:<10} 0             ERR: {er[0][:80]}")
    else:
        print(f"  {at:<35} {ora_status:<10} 0             sin cambio (UPSERT silencioso)")

print("\n=== FIN === Filas insertadas quedan en legacy.")
orcl.close()
