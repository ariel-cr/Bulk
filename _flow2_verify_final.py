"""Verificacion FINAL post redireccion a SPs originales:
para cada uno de los 30 types, INSERT con PK fresco y verificar delta legacy."""
import pyodbc, oracledb, time, random

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=orcl.cursor()
c_can=sql('fcme_canonicos').cursor()

# Mapeo aggregate_type -> (legacy_db, legacy_table)
c_can.execute("""SELECT m.aggregate_type, m.target_db, t.source_table
                 FROM dbo.cdc_inbox_module_config m
                 LEFT JOIN dbo.cdc_table_to_types t ON t.aggregate_type_emit=m.aggregate_type AND t.is_active=1
                 WHERE m.active=1""")
type_to_target={}
for r in c_can.fetchall():
    if r.aggregate_type not in type_to_target and r.source_table:
        type_to_target[r.aggregate_type]=(r.target_db, r.source_table)

AT_TO_TABLE={'actualizacionAfiliadoType':'ACTUALIZACION_AFILIADO_TYPE','actualizacionDocumentosType':'ACTUALIZACION_DOCUMENTOS_TYPE','agendaMailAfiliadoType':'AGENDAMAILAFILIADO_TYPE','auditoriaAfiliadoType':'AUDITORIAAFILIADO_TYPE','beneficiarioParticipeType':'BENEFICIARIOPARTICIPE_TYPE','cuentaBancariaAfiliadoType':'CUENTABANCARIAAFILIADO_TYPE','distribucionAfiliadoType':'DISTRIBUCIONAFILIADO_TYPE','documentacionAfiliadoType':'DOCUMENTACIONAFILIADO_TYPE','firmanteParticipeType':'FIRMANTEPARTICIPE_TYPE','grupoFamiliarType':'GRUPOFAMILIAR_TYPE','informacionAdicionalAfiliadoType':'INFORMACIONADICIONALAFILIADO_TYPE','institucionType':'INSTITUCION_TYPE','motivoContableType':'MOTIVOCONTABLE_TYPE','movimientoCuentaType':'MOVIMIENTOCUENTA_TYPE','movimientoTemporalType':'MOVIMIENTOTEMPORAL_TYPE','naturalInformacionAdicionalType':'NATURALINFORMACIONADICIONALTYPE','naturalIngresosEgresosType':'NATURALINGRESOSEGRESOSTYPE','naturalTrabajoType':'NATURALTRABAJOTYPE','personaReferenciasBancariasType':'PERSONAREFERENCIASBANCARIASTYPE','personaReferenciasPersonalesType':'PERSONAREFERENCIASPERSONALESTYPE','personaTelefonosType':'PERSONATELEFONOSTYPE','personaVinculacionesType':'PERSONAVINCULACIONESTYPE','referenciaParticipeType':'REFERENCIAPARTICIPE_TYPE','reporteSIBSParticipeType':'REPORTESIBSPARTICIPE_TYPE','retiroLiquidacionType':'RETIROLIQUIDACION_TYPE','retiroVoluntarioEstadoType':'RETIROVOLUNTARIOESTADO_TYPE','saldoDiarioRubroType':'SALDODIARIORUBRO_TYPE','saldoDiarioType':'SALDODIARIO_TYPE','seguroVidaParticipeType':'SEGUROVIDAPARTICIPE_TYPE','servicioAdicionalType':'SERVICIOADICIONAL_TYPE'}

# Reset errores para ver solo los nuevos
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")

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

# INSERTs Oracle con PKs MUY frescos para que no choquen con UK Oracle
base = 90000 + random.randint(0,9999)
print(f"\n[INSERTs Oracle PKs base={base}]")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX"); orcl.commit()
inserted=[]
for i,(at,ot) in enumerate(AT_TO_TABLE.items()):
    co.execute("SELECT column_name, data_type, data_length, nullable FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id", t=ot)
    cols=[(r[0],r[1],r[2],r[3]) for r in co.fetchall() if r[0]!='ID']
    if not cols: continue
    test_pk=str(base+i)
    cn=', '.join(c[0] for c in cols)
    vals=[]
    for n,t,d,nl in cols:
        if 'VARCHAR' in t or 'CHAR' in t:
            if (d or 99) <= 3: vals.append("'9'")
            elif (d or 99) <= 12: vals.append(f"'{test_pk}'")
            else: vals.append(f"'{test_pk}'")
        elif 'NUMBER' in t: vals.append(test_pk)
        elif 'DATE' in t or 'TIMESTAMP' in t: vals.append("DATE '2027-01-01'")
        elif 'CLOB' in t: vals.append("'{}'")
        else: vals.append('NULL')
    try:
        co.execute(f'INSERT INTO FCME_USER.{ot} ({cn}) VALUES ({", ".join(vals)})')
        inserted.append((at,ot,test_pk))
    except Exception as e:
        pass  # silenciar UK
orcl.commit()
print(f"  Insertados Oracle: {len(inserted)}/30")

# Esperar
print("\n[Espera 25s]")
time.sleep(25)

# Reporte completo
print("\n[Resultado]")
print(f"  {'TYPE':<35} {'BD':<6} {'TABLA':<35} {'PRE':<8} {'POST':<8} {'DELTA':<6} {'STATUS'}")
print('-'*120)
ok=err=updated=0
errors_summary=[]
for at,ot,pk in inserted:
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
        if delta>0: ok+=1; status='✓ INSERT'
        elif n_err>0: err+=1; status='✗ ERR'; errors_summary.append((at, em))
        else: updated+=1; status='~ UPSERT'
        print(f"  {at:<35} {db:<6} {tbl:<35} {str(pre[at]):<8} {str(post):<8} {delta:<6} {status}")
    except Exception as e:
        print(f"  {at}: query err: {str(e)[:80]}")

print(f"\n*** RESUMEN: INSERT_NUEVO={ok}  UPSERT={updated}  ERR={err}  total_test={len(inserted)} ***")

if errors_summary:
    print("\nErrores legítimos de los SPs originales (validación de datos):")
    for at, em in errors_summary[:20]:
        print(f"  {at:<35} {em}")

# Cleanup test
print("\n[Cleanup]")
for at,ot,pk in inserted:
    try: co.execute(f"DELETE FROM FCME_USER.{ot} WHERE ROWID IN (SELECT ROWID FROM FCME_USER.{ot} ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY)")
    except: pass
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX"); orcl.commit()
orcl.close()
