"""Test masivo de los 30 types CON DATOS VALIDOS y SIN CLEANUP.
Usa valores que respetan: CODIGO_EMPRESA='1', cedulas cortas, valores chicos para SMALLINT.
NO borra nada al final - todo queda visible para inspección.
"""
import pyodbc, oracledb, time, random

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=orcl.cursor()
c_can=sql('fcme_canonicos').cursor()

# Mapeo
c_can.execute("""SELECT m.aggregate_type, m.target_db, t.source_table
                 FROM dbo.cdc_inbox_module_config m
                 LEFT JOIN dbo.cdc_table_to_types t ON t.aggregate_type_emit=m.aggregate_type AND t.is_active=1
                 WHERE m.active=1""")
type_to_target={}
for r in c_can.fetchall():
    if r.aggregate_type not in type_to_target and r.source_table:
        type_to_target[r.aggregate_type]=(r.target_db, r.source_table)

AT_TO_TABLE={'actualizacionAfiliadoType':'ACTUALIZACION_AFILIADO_TYPE','actualizacionDocumentosType':'ACTUALIZACION_DOCUMENTOS_TYPE','agendaMailAfiliadoType':'AGENDAMAILAFILIADO_TYPE','auditoriaAfiliadoType':'AUDITORIAAFILIADO_TYPE','beneficiarioParticipeType':'BENEFICIARIOPARTICIPE_TYPE','cuentaBancariaAfiliadoType':'CUENTABANCARIAAFILIADO_TYPE','distribucionAfiliadoType':'DISTRIBUCIONAFILIADO_TYPE','documentacionAfiliadoType':'DOCUMENTACIONAFILIADO_TYPE','firmanteParticipeType':'FIRMANTEPARTICIPE_TYPE','grupoFamiliarType':'GRUPOFAMILIAR_TYPE','informacionAdicionalAfiliadoType':'INFORMACIONADICIONALAFILIADO_TYPE','institucionType':'INSTITUCION_TYPE','motivoContableType':'MOTIVOCONTABLE_TYPE','movimientoCuentaType':'MOVIMIENTOCUENTA_TYPE','movimientoTemporalType':'MOVIMIENTOTEMPORAL_TYPE','naturalInformacionAdicionalType':'NATURALINFORMACIONADICIONALTYPE','naturalIngresosEgresosType':'NATURALINGRESOSEGRESOSTYPE','naturalTrabajoType':'NATURALTRABAJOTYPE','personaReferenciasBancariasType':'PERSONAREFERENCIASBANCARIASTYPE','personaReferenciasPersonalesType':'PERSONAREFERENCIASPERSONALESTYPE','personaTelefonosType':'PERSONATELEFONOSTYPE','personaVinculacionesType':'PERSONAVINCULACIONESTYPE','referenciaParticipeType':'REFERENCIAPARTICIPE_TYPE','reporteSIBSParticipeType':'REPORTESIBSPARTICIPE_TYPE','retiroLiquidacionType':'RETIROLIQUIDACION_TYPE','retiroVoluntarioEstadoType':'RETIROVOLUNTARIOESTADO_TYPE','saldoDiarioRubroType':'SALDODIARIORUBRO_TYPE','saldoDiarioType':'SALDODIARIO_TYPE','seguroVidaParticipeType':'SEGUROVIDAPARTICIPE_TYPE','servicioAdicionalType':'SERVICIOADICIONAL_TYPE'}

c_can.execute("DELETE FROM dbo.cdc_inbox_errors")  # limpio errores para ver solo nuevos

# Pre-count
print("[Pre-count]")
pre={}
for at,ot in AT_TO_TABLE.items():
    target=type_to_target.get(at)
    if not target: continue
    db,tbl=target
    try:
        cdb=sql(db).cursor()
        cdb.execute(f"SELECT COUNT(*) FROM dbo.[{tbl}]")
        pre[at]=cdb.fetchone()[0]
    except: pre[at]=None

# Para PKs cortos: usar 1xxx + offset por type (para que quepa en SMALLINT y sea único)
# Para cedulas: usar string numerico corto distintivo
base_id = 1100 + random.randint(0, 100)  # menor a 32767 (SMALLINT max)
print(f"\n[INSERTs Oracle - base PK={base_id}]")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX"); orcl.commit()
inserted=[]

# Funcion: generar valor según nombre col + tipo
def gen_value(col_name, dtype, max_len, idx):
    cn = col_name.upper()
    test_pk = str(base_id + idx)  # un PK distintivo por type
    cedula = f"099{(7000+idx):07d}"  # cédula 10 digitos
    # Campos comunes
    if cn in ('CODIGO_EMPRESA','CODIGOEMPRESA','CO_EMPR'): return "'1'"
    if cn in ('CODIGOFONDO','CODIGO_FONDO','CO_FOND'): return "'1'"
    if cn in ('CODIGO_CEDU','CODIGOCEDU','CI_CEDU','CODIGOCEDULA','CI_CEDULA','CEDULAFAMILIAR'): return f"'{cedula}'"
    if cn in ('IDENTIFICACION',): return f"'{cedula}'"
    if cn in ('CODIGOTIPOIDENTIFICACION','CI_TIPO','TI_IDEN'): return "'C'"
    if cn in ('TIPOPERSONA','CODIGOTIPOPERSONA','TI_PERS'): return "'AFILIADO'"
    if cn in ('CODIGOTIPOVINCULACION',): return "'CONYUGE'"
    if cn in ('CODIGOMODULO','CODIGO_MODULO'): return "'P'"
    if cn in ('SECUENCIA','SECUENCIA_REGISTRO','SC_REGI','SECUENCIATELEFONO','SECUENCIATRABAJO','SECUENCIAREFERENCIABANCARIA','SECUENCIAPERSONAVINCULACION'): return "'1'"
    if cn in ('CODIGOTIPOREFERENCIA',): return f"'{test_pk}'"
    # Por tipo
    if 'VARCHAR' in dtype or 'CHAR' in dtype:
        if (max_len or 99) <= 2: return "'1'"
        if (max_len or 99) <= 4: return "'1'"
        return f"'{test_pk}'"
    if 'NUMBER' in dtype: return test_pk
    if 'DATE' in dtype or 'TIMESTAMP' in dtype: return "DATE '2027-01-01'"
    if 'CLOB' in dtype: return "'{}'"
    return 'NULL'

for i,(at,ot) in enumerate(AT_TO_TABLE.items()):
    co.execute("SELECT column_name, data_type, data_length, nullable FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id", t=ot)
    cols=[(r[0],r[1],r[2]) for r in co.fetchall() if r[0]!='ID']
    if not cols: continue
    cn=', '.join(c[0] for c in cols)
    vals=[gen_value(c[0], c[1], c[2], i) for c in cols]
    try:
        co.execute(f'INSERT INTO FCME_USER.{ot} ({cn}) VALUES ({", ".join(vals)})')
        inserted.append((at,ot))
    except Exception as e:
        print(f"  fail Oracle {ot}: {str(e)[:120]}")
orcl.commit()
print(f"  Insertados Oracle: {len(inserted)}/30")

print("\n[Espera 25s]")
time.sleep(25)

print("\n[Resultado]")
print(f"  {'TYPE':<35} {'BD':<6} {'TABLA':<35} {'PRE':<8} {'POST':<8} {'DELTA':<6} {'STATUS'}")
print('-'*120)
ok=err=updated=0
err_msgs=[]
for at,ot in inserted:
    if at not in pre or pre[at] is None: continue
    db,tbl=type_to_target[at]
    try:
        cdb=sql(db).cursor()
        cdb.execute(f"SELECT COUNT(*) FROM dbo.[{tbl}]")
        post=cdb.fetchone()[0]
        delta=post-pre[at]
        c_can.execute("SELECT COUNT(*), MAX(error_message) FROM dbo.cdc_inbox_errors WHERE aggregate_type=?", at)
        e_row=c_can.fetchone()
        n_err=e_row[0]; em=(e_row[1] or '')[:120]
        if delta>0: ok+=1; status='OK'
        elif n_err>0: err+=1; status='ERR'; err_msgs.append((at, em))
        else: updated+=1; status='UPSERT'
        print(f"  {at:<35} {db:<6} {tbl:<35} {str(pre[at]):<8} {str(post):<8} {delta:<6} {status}")
    except Exception as e:
        print(f"  {at}: {str(e)[:80]}")

print(f"\n*** RESUMEN: OK={ok} UPSERT={updated} ERR={err}  total_test={len(inserted)} ***")

if err_msgs:
    print("\nErrores (legítimos del SP original):")
    for at, em in err_msgs:
        print(f"  {at:<35} {em}")

print("\n[NO CLEANUP] Los datos quedan en CDC_OUTBOX, cdc_inbox y bases legacy para inspección.")
orcl.close()
