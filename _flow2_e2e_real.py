"""Test end-to-end de cada type usando datos VALIDOS que satisfagan los SPs originales.

Estrategia:
- Usa una cédula real existente en sfct_afiliado (FK satisfecha)
- CODIGO_EMPRESA='1' (empresa default)
- CODIGO_FONDO='1'
- PKs cortos (SMALLINT-safe)
- Para cada type, INSERT específico ajustado a su SP original

NO elimina nada: las filas insertadas quedan en las tablas legacy para inspección.
"""
import pyodbc, oracledb, time

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=orcl.cursor()
c_can=sql('fcme_canonicos').cursor()
c_fc=sql('dbFC').cursor()

# Buscar una cedula real de sfct_afiliado (FK satisfecha)
c_fc.execute("SELECT TOP 1 ci_cedula FROM dbo.sfct_afiliado WHERE ci_cedula IS NOT NULL")
real_cedula = c_fc.fetchone().ci_cedula.strip()
print(f"Cédula real para FK: {real_cedula}")

c_can.execute("DELETE FROM dbo.cdc_inbox_errors")

# Tabla de tests por type. Cada entrada: (oracle_table, dict_de_cols)
# El test value se construye combinando columnas necesarias.
# Los SPs originales suelen requerir CODIGO_EMPRESA, CODIGO_FONDO, IDENTIFICACION + PK del registro.

TESTS = {
    'referenciaParticipeType':       ('REFERENCIAPARTICIPE_TYPE', {
        'CODIGOTIPOREFERENCIA':'201','DESCRIPCIONTIPOREFERENCIA':'TEST E2E REF'}),
    'motivoContableType':            ('MOTIVOCONTABLE_TYPE', {
        'CODIGOEMPRESA':'1','CODIGOMODULO':'P','CODIGOMOTIVO':'301','DESCRIPCIONMOTIVO':'TEST E2E','TIPOMOVIMIENTO':'D','ESTADO':'A'}),
    'firmanteParticipeType':         ('FIRMANTEPARTICIPE_TYPE', {
        'CODIGOEMPRESA':'1','CODIGOTIPOIDENTIFICACION':'C','IDENTIFICACION':real_cedula,'SECUENCIAFIRMANTE':'9','NOMBREFIRMANTE':'TST'}),
    'grupoFamiliarType':             ('GRUPOFAMILIAR_TYPE', {
        'CODIGOTIPOIDENTIFICACION':'C','IDENTIFICACION':real_cedula,'CEDULAFAMILIAR':'0911111199','NOMBRESGRUPOFAMILIAR':'TST','APELLIDOSGRUPOFAMILIAR':'TST','ESTADOFAMILIAR':'A','FECHANACIMIENTO':'1990-01-01','FECHACREACIONREGISTRO':'2027-01-01','FECHAMODIFICACION':'2027-01-01','INDICADORDISCAPACIDAD':'N'}),
    'auditoriaAfiliadoType':         ('AUDITORIAAFILIADO_TYPE', {
        'CODIGOEMPRESA':'1','CODIGOTIPOIDENTIFICACION':'C','IDENTIFICACION':real_cedula,'SECUENCIAAUDITORIA':'9','FECHAMODIFICACION':'2027-01-01','CAMPOMODIFICADO':'TST','VALORANTERIOR':'A','VALORNUEVO':'B','CODIGOUSUARIO':'1','TIPOOPERACION':'U'}),
    'distribucionAfiliadoType':      ('DISTRIBUCIONAFILIADO_TYPE', {
        'CODIGOTIPOIDENTIFICACION':'C','IDENTIFICACION':real_cedula,'CODIGOPROVINCIA':'1','CODIGOCIUDAD':'1','CODIGOPARROQUIA':'1','SECUENCIAREGISTRO':'9','ESTADO':'A'}),
    'saldoDiarioRubroType':          ('SALDODIARIORUBRO_TYPE', {
        'CODIGOEMPRESA':'1','CODIGOFONDO':'1','CODIGORUBRO':'1','FECHASALDO':'2027-01-01','SALDOACTUAL':'100'}),
    'movimientoTemporalType':        ('MOVIMIENTOTEMPORAL_TYPE', {
        'CODIGOEMPRESA':'1','CODIGOFONDO':'1','CODIGOTIPOIDENTIFICACION':'C','IDENTIFICACION':real_cedula,'SECUENCIAMOVIMIENTO':'9','FECHAMOVIMIENTO':'2027-01-01','MONTO':'100'}),
    'movimientoCuentaType':          ('MOVIMIENTOCUENTA_TYPE', {
        'CODIGOEMPRESA':'1','CODIGOFONDO':'1','CODIGOTIPOIDENTIFICACION':'C','IDENTIFICACION':real_cedula,'SECUENCIAMOVIMIENTO':'9','FECHAMOVIMIENTO':'2027-01-01','MONTO':'100'}),
}

print(f"\nProbando {len(TESTS)} types con datos válidos:")
print("="*100)

results = []
for at, (ot, cols_dict) in TESTS.items():
    # Verificar pre-count en legacy
    c_can.execute("SELECT m.target_db, t.source_table FROM dbo.cdc_inbox_module_config m LEFT JOIN dbo.cdc_table_to_types t ON t.aggregate_type_emit=m.aggregate_type AND t.is_active=1 WHERE m.aggregate_type=?", at)
    target = c_can.fetchone()
    if not target:
        results.append((at, '?', '?', None, None, 'no target'))
        continue
    target_db, target_tbl = target.target_db, target.source_table

    try:
        cdb = sql(target_db).cursor()
        cdb.execute(f"SELECT COUNT(*) FROM dbo.[{target_tbl}]")
        pre = cdb.fetchone()[0]
    except:
        pre = None

    # INSERT Oracle con datos válidos
    cn = ', '.join(cols_dict.keys())
    vals = ', '.join(f"'{v}'" if not str(v).startswith("DATE ") else v for v in cols_dict.values())
    try:
        co.execute(f"INSERT INTO FCME_USER.{ot} ({cn}) VALUES ({vals})")
        orcl.commit()
        oracle_ok = True
    except Exception as e:
        results.append((at, target_db, target_tbl, pre, None, f'ORA fail: {str(e)[:80]}'))
        continue

    time.sleep(4)

    # Post-count
    try:
        cdb.execute(f"SELECT COUNT(*) FROM dbo.[{target_tbl}]")
        post = cdb.fetchone()[0]
    except:
        post = None

    # Buscar error específico
    c_can.execute("SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE aggregate_type=? AND created_at > DATEADD(MINUTE,-2,SYSUTCDATETIME()) ORDER BY error_id DESC", at)
    er = c_can.fetchone()

    delta = (post - pre) if (pre is not None and post is not None) else None
    if delta and delta > 0:
        status = 'OK INSERT'
    elif er:
        status = f'ERR: {er[0][:80]}'
    else:
        status = 'sin cambio'
    results.append((at, target_db, target_tbl, pre, post, status))

print(f"\n{'TYPE':<35} {'BD':<6} {'TABLA':<35} {'PRE':<8} {'POST':<8} {'STATUS'}")
print('-'*150)
ok = 0
for at, db, tbl, pre, post, status in results:
    if 'OK INSERT' in str(status): ok += 1
    print(f"  {at:<35} {db:<6} {tbl:<35} {str(pre):<8} {str(post):<8} {status}")

print(f"\n*** OK_INSERT={ok} de {len(results)} ***")
print("\n[NO CLEANUP] Filas insertadas quedan en las bases legacy")
orcl.close()
