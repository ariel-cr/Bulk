"""Test masivo con PKs SUPER frescos (no reusados de runs previos)."""
import pyodbc, oracledb, time
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=orcl.cursor()
c_can=sql('fcme_canonicos').cursor()

c_can.execute("""SELECT m.aggregate_type, m.target_db, t.source_table
                 FROM dbo.cdc_inbox_module_config m
                 LEFT JOIN dbo.cdc_table_to_types t ON t.aggregate_type_emit=m.aggregate_type AND t.is_active=1
                 WHERE m.active=1""")
type_to_target={}
for r in c_can.fetchall():
    if r.aggregate_type not in type_to_target and r.source_table:
        type_to_target[r.aggregate_type]=(r.target_db, r.source_table)

AT_TO_TABLE={'actualizacionAfiliadoType':'ACTUALIZACION_AFILIADO_TYPE','actualizacionDocumentosType':'ACTUALIZACION_DOCUMENTOS_TYPE','agendaMailAfiliadoType':'AGENDAMAILAFILIADO_TYPE','auditoriaAfiliadoType':'AUDITORIAAFILIADO_TYPE','beneficiarioParticipeType':'BENEFICIARIOPARTICIPE_TYPE','cuentaBancariaAfiliadoType':'CUENTABANCARIAAFILIADO_TYPE','distribucionAfiliadoType':'DISTRIBUCIONAFILIADO_TYPE','documentacionAfiliadoType':'DOCUMENTACIONAFILIADO_TYPE','firmanteParticipeType':'FIRMANTEPARTICIPE_TYPE','grupoFamiliarType':'GRUPOFAMILIAR_TYPE','informacionAdicionalAfiliadoType':'INFORMACIONADICIONALAFILIADO_TYPE','institucionType':'INSTITUCION_TYPE','motivoContableType':'MOTIVOCONTABLE_TYPE','movimientoCuentaType':'MOVIMIENTOCUENTA_TYPE','movimientoTemporalType':'MOVIMIENTOTEMPORAL_TYPE','naturalInformacionAdicionalType':'NATURALINFORMACIONADICIONALTYPE','naturalIngresosEgresosType':'NATURALINGRESOSEGRESOSTYPE','naturalTrabajoType':'NATURALTRABAJOTYPE','personaReferenciasBancariasType':'PERSONAREFERENCIASBANCARIASTYPE','personaReferenciasPersonalesType':'PERSONAREFERENCIASPERSONALESTYPE','personaTelefonosType':'PERSONATELEFONOSTYPE','personaVinculacionesType':'PERSONAVINCULACIONESTYPE','referenciaParticipeType':'REFERENCIAPARTICIPE_TYPE','reporteSIBSParticipeType':'REPORTESIBSPARTICIPE_TYPE','retiroLiquidacionType':'RETIROLIQUIDACION_TYPE','retiroVoluntarioEstadoType':'RETIROVOLUNTARIOESTADO_TYPE','saldoDiarioRubroType':'SALDODIARIORUBRO_TYPE','saldoDiarioType':'SALDODIARIO_TYPE','seguroVidaParticipeType':'SEGUROVIDAPARTICIPE_TYPE','servicioAdicionalType':'SERVICIOADICIONAL_TYPE'}

# Borrar errores viejos para ver solo los del test actual
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")

# Pre-count
print("[Pre-count]")
pre={}
for at, ot in AT_TO_TABLE.items():
    target=type_to_target.get(at)
    if not target: continue
    db,tbl=target
    try:
        cdb=sql(db).cursor()
        cdb.execute(f"SELECT COUNT(*) FROM dbo.[{tbl}]")
        pre[at]=cdb.fetchone()[0]
    except: pre[at]=None

# Insertar en Oracle con PKs SUPER frescos
import random
base=8500 + random.randint(0,500)  # rango muy fresco
print(f"\n[INSERTs Oracle con PKs base={base}]")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX"); orcl.commit()
inserted=[]; pk_used={}
for i,(at,ot) in enumerate(AT_TO_TABLE.items()):
    co.execute("SELECT column_name, data_type, data_length FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id", t=ot)
    cols=[(r[0],r[1],r[2]) for r in co.fetchall() if r[0]!='ID']
    if not cols: continue
    test_pk=str(base+i)
    pk_used[at]=test_pk
    cn=', '.join(c[0] for c in cols)
    vals=[]
    for n,t,d in cols:
        if 'VARCHAR' in t or 'CHAR' in t:
            # Para campos cortos (<3 chars), usar '9'
            if (d or 99) <= 3: vals.append("'9'")
            elif (d or 99) <= 10: vals.append(f"'{test_pk}'")
            else: vals.append(f"'{test_pk}'")
        elif 'NUMBER' in t: vals.append(test_pk)
        elif 'DATE' in t or 'TIMESTAMP' in t: vals.append("DATE '2027-01-01'")
        elif 'CLOB' in t: vals.append("'{}'")
        else: vals.append('NULL')
    try:
        co.execute(f'INSERT INTO FCME_USER.{ot} ({cn}) VALUES ({", ".join(vals)})')
        inserted.append((at,ot))
    except Exception as e:
        # silenciar UK violations
        pass
orcl.commit()
print(f"  Insertados Oracle: {len(inserted)}/30")

# Esperar
print("\n[Espera 18s]")
time.sleep(18)

# Post-count y reporte
print("\n[Resultado]")
ok=update_silent=err=0
for at,ot in inserted:
    if at not in pre or pre[at] is None: continue
    db,tbl=type_to_target[at]
    try:
        cdb=sql(db).cursor()
        cdb.execute(f"SELECT COUNT(*) FROM dbo.[{tbl}]")
        post=cdb.fetchone()[0]
        delta=post-pre[at]
        c_can.execute("SELECT COUNT(*), MAX(error_message) FROM dbo.cdc_inbox_errors WHERE aggregate_type=? OR aggregate_type=?", at, at.upper().replace('TYPE','_TYPE'))
        er_row=c_can.fetchone()
        n_err=er_row[0]; err_msg=(er_row[1] or '')[:150]
        if delta>0: ok+=1; status='OK'
        elif n_err>0: err+=1; status='ERR'
        else: update_silent+=1; status='UPSERT'
        print(f"  {at:<35} {db:<6} {tbl:<35} pre={pre[at]:<6} post={post:<6} delta={delta:<3} {status}")
        if status=='ERR': print(f"      err: {err_msg}")
    except Exception as e:
        print(f"  {at}: query err: {str(e)[:80]}")

print(f"\n*** RESUMEN: OK={ok} ERR={err} UPSERT={update_silent} ***")

# Cleanup
for at,ot in inserted:
    try: co.execute(f"DELETE FROM FCME_USER.{ot} WHERE ROWID IN (SELECT ROWID FROM FCME_USER.{ot} ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY)")
    except: pass
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX"); orcl.commit()
orcl.close()
