"""Verificacion SOLIDA: pre-count + INSERT controlado + post-count en legacy."""
import pyodbc, oracledb, time

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=orcl.cursor()
c_can=sql('fcme_canonicos').cursor()

# Mapping: aggregate_type -> (legacy_db, legacy_table)
c_can.execute("""SELECT m.aggregate_type, m.target_db, t.source_table
                 FROM dbo.cdc_inbox_module_config m
                 LEFT JOIN dbo.cdc_table_to_types t ON t.aggregate_type_emit=m.aggregate_type AND t.is_active=1
                 WHERE m.active=1""")
type_to_target = {}
for r in c_can.fetchall():
    if r.aggregate_type not in type_to_target and r.source_table:
        type_to_target[r.aggregate_type] = (r.target_db, r.source_table)

AT_TO_TABLE = {'actualizacionAfiliadoType':'ACTUALIZACION_AFILIADO_TYPE','actualizacionDocumentosType':'ACTUALIZACION_DOCUMENTOS_TYPE','agendaMailAfiliadoType':'AGENDAMAILAFILIADO_TYPE','auditoriaAfiliadoType':'AUDITORIAAFILIADO_TYPE','beneficiarioParticipeType':'BENEFICIARIOPARTICIPE_TYPE','cuentaBancariaAfiliadoType':'CUENTABANCARIAAFILIADO_TYPE','distribucionAfiliadoType':'DISTRIBUCIONAFILIADO_TYPE','documentacionAfiliadoType':'DOCUMENTACIONAFILIADO_TYPE','firmanteParticipeType':'FIRMANTEPARTICIPE_TYPE','grupoFamiliarType':'GRUPOFAMILIAR_TYPE','informacionAdicionalAfiliadoType':'INFORMACIONADICIONALAFILIADO_TYPE','institucionType':'INSTITUCION_TYPE','motivoContableType':'MOTIVOCONTABLE_TYPE','movimientoCuentaType':'MOVIMIENTOCUENTA_TYPE','movimientoTemporalType':'MOVIMIENTOTEMPORAL_TYPE','naturalInformacionAdicionalType':'NATURALINFORMACIONADICIONALTYPE','naturalIngresosEgresosType':'NATURALINGRESOSEGRESOSTYPE','naturalTrabajoType':'NATURALTRABAJOTYPE','personaReferenciasBancariasType':'PERSONAREFERENCIASBANCARIASTYPE','personaReferenciasPersonalesType':'PERSONAREFERENCIASPERSONALESTYPE','personaTelefonosType':'PERSONATELEFONOSTYPE','personaVinculacionesType':'PERSONAVINCULACIONESTYPE','referenciaParticipeType':'REFERENCIAPARTICIPE_TYPE','reporteSIBSParticipeType':'REPORTESIBSPARTICIPE_TYPE','retiroLiquidacionType':'RETIROLIQUIDACION_TYPE','retiroVoluntarioEstadoType':'RETIROVOLUNTARIOESTADO_TYPE','saldoDiarioRubroType':'SALDODIARIORUBRO_TYPE','saldoDiarioType':'SALDODIARIO_TYPE','seguroVidaParticipeType':'SEGUROVIDAPARTICIPE_TYPE','servicioAdicionalType':'SERVICIOADICIONAL_TYPE'}

# PRE-COUNT por cada tabla legacy
print("="*70)
print("[1] PRE-COUNT: filas actuales en cada tabla legacy")
print("="*70)
pre_counts={}
for at, ot in AT_TO_TABLE.items():
    target = type_to_target.get(at)
    if not target: continue
    db, tbl = target
    try:
        c_db = sql(db).cursor()
        c_db.execute(f"SELECT COUNT(*) FROM dbo.[{tbl}]")
        pre_counts[at] = (db, tbl, c_db.fetchone()[0])
    except Exception as e:
        pre_counts[at] = (db, tbl, f'err: {str(e)[:60]}')

# INSERTs en Oracle
print("\n[2] Insertando 30 eventos en Oracle FCME_USER")
co.execute('DELETE FROM FCME_USER.CDC_OUTBOX'); orcl.commit()
inserted=[]
for at, ot in AT_TO_TABLE.items():
    co.execute("SELECT column_name, data_type, data_length FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id", t=ot)
    cols=[(r[0],r[1],r[2]) for r in co.fetchall() if r[0]!='ID']
    if not cols: continue
    cn=', '.join(c[0] for c in cols)
    vals=[]
    for n,t,d in cols:
        if 'VARCHAR' in t or 'CHAR' in t: vals.append("'9'" if (d or 99)<=2 else "'99'")
        elif 'NUMBER' in t: vals.append('9')
        elif 'DATE' in t or 'TIMESTAMP' in t: vals.append('SYSTIMESTAMP')
        elif 'CLOB' in t: vals.append("'{}'")
        else: vals.append('NULL')
    try: co.execute(f'INSERT INTO FCME_USER.{ot} ({cn}) VALUES ({", ".join(vals)})'); inserted.append(at)
    except: pass
orcl.commit()
print(f"  Oracle INSERTs: {len(inserted)}/30")

# Esperar
print("\n[3] Esperando propagacion 15s")
time.sleep(15)

# POST-COUNT
print("\n[4] POST-COUNT y DELTA por tabla legacy")
print("="*100)
print(f"  {'TYPE':<35} {'BD':<6} {'TABLA':<35} {'PRE':<8} {'POST':<8} {'DELTA'}")
print("-"*100)
guardados=0; sin_delta=0
for at, ot in AT_TO_TABLE.items():
    if at not in pre_counts: continue
    db, tbl, pre = pre_counts[at]
    try:
        c_db = sql(db).cursor()
        c_db.execute(f"SELECT COUNT(*) FROM dbo.[{tbl}]")
        post = c_db.fetchone()[0]
        if isinstance(pre, int):
            delta = post - pre
            mark = "OK" if delta > 0 else "(no cambio)"
            if delta > 0: guardados+=1
            else: sin_delta+=1
        else:
            delta = pre; post = pre; mark = ""
        print(f"  {at:<35} {db:<6} {tbl:<35} {str(pre):<8} {str(post):<8} {delta} {mark}")
    except Exception as e:
        print(f"  {at:<35} {db:<6} {tbl:<35} err: {str(e)[:40]}")

print(f"\n  RESUMEN: GUARDADOS_NUEVOS={guardados}  SIN_DELTA={sin_delta}")

# Errores y parsed
c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors"); print(f"  cdc_inbox_errors total: {c_can.fetchone()[0]}")
c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_parsed"); print(f"  cdc_inbox_parsed total: {c_can.fetchone()[0]}")

orcl.close()
