"""Test masivo Flujo 2 - SIN limpiar CDC_OUTBOX ni cdc_inbox al final."""
import pyodbc, oracledb, time

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=orcl.cursor()
c=sql('fcme_canonicos').cursor()

# Estado inicial (sin reset)
co.execute('SELECT COUNT(*), NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX')
ox0, ox_mx0 = co.fetchone()
c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox'); ib0 = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_parsed'); ps0 = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors'); er0 = c.fetchone()[0]
print(f"[Estado inicial] outbox={ox0} (max_id={ox_mx0}) inbox={ib0} parsed={ps0} errors={er0}")

AT_TO_TABLE = {'actualizacionAfiliadoType':'ACTUALIZACION_AFILIADO_TYPE','actualizacionDocumentosType':'ACTUALIZACION_DOCUMENTOS_TYPE','agendaMailAfiliadoType':'AGENDAMAILAFILIADO_TYPE','auditoriaAfiliadoType':'AUDITORIAAFILIADO_TYPE','beneficiarioParticipeType':'BENEFICIARIOPARTICIPE_TYPE','cuentaBancariaAfiliadoType':'CUENTABANCARIAAFILIADO_TYPE','distribucionAfiliadoType':'DISTRIBUCIONAFILIADO_TYPE','documentacionAfiliadoType':'DOCUMENTACIONAFILIADO_TYPE','firmanteParticipeType':'FIRMANTEPARTICIPE_TYPE','grupoFamiliarType':'GRUPOFAMILIAR_TYPE','informacionAdicionalAfiliadoType':'INFORMACIONADICIONALAFILIADO_TYPE','institucionType':'INSTITUCION_TYPE','motivoContableType':'MOTIVOCONTABLE_TYPE','movimientoCuentaType':'MOVIMIENTOCUENTA_TYPE','movimientoTemporalType':'MOVIMIENTOTEMPORAL_TYPE','naturalInformacionAdicionalType':'NATURALINFORMACIONADICIONALTYPE','naturalIngresosEgresosType':'NATURALINGRESOSEGRESOSTYPE','naturalTrabajoType':'NATURALTRABAJOTYPE','personaReferenciasBancariasType':'PERSONAREFERENCIASBANCARIASTYPE','personaReferenciasPersonalesType':'PERSONAREFERENCIASPERSONALESTYPE','personaTelefonosType':'PERSONATELEFONOSTYPE','personaVinculacionesType':'PERSONAVINCULACIONESTYPE','referenciaParticipeType':'REFERENCIAPARTICIPE_TYPE','reporteSIBSParticipeType':'REPORTESIBSPARTICIPE_TYPE','retiroLiquidacionType':'RETIROLIQUIDACION_TYPE','retiroVoluntarioEstadoType':'RETIROVOLUNTARIOESTADO_TYPE','saldoDiarioRubroType':'SALDODIARIORUBRO_TYPE','saldoDiarioType':'SALDODIARIO_TYPE','seguroVidaParticipeType':'SEGUROVIDAPARTICIPE_TYPE','servicioAdicionalType':'SERVICIOADICIONAL_TYPE'}

inserted=[]
for at,ot in AT_TO_TABLE.items():
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
    try: co.execute(f'INSERT INTO FCME_USER.{ot} ({cn}) VALUES ({", ".join(vals)})'); inserted.append((at,ot))
    except: pass
orcl.commit()
print(f"\n[Oracle INSERTs realizados] {len(inserted)}/30")

# Esperar propagacion
print("\n[Esperando propagacion]")
prev=ib0; stable=0
for i in range(60):
    time.sleep(2)
    co.execute('SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX'); ob=co.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox'); ib=c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_parsed'); ps=c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors'); er=c.fetchone()[0]
    print(f'  T+{(i+1)*2}s: outbox={ob} inbox={ib} parsed={ps} errors={er}')
    if ib == prev:
        stable+=1
        if stable>=5 and (ib-ib0)>=len(inserted): break
    else: stable=0
    prev=ib

# Reporte por type del test actual
print("\n[Resultado por type - solo eventos generados ahora]")
ok=err=miss=0
type_status=[]
# Buscar mensajes nuevos por aggregate_id que coincida con los test (los IDs Oracle nuevos)
for at,ot in inserted:
    c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox WHERE aggregate_type=?', at); n=c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE aggregate_type=?', at); e=c.fetchone()[0]
    if n==0: miss+=1; status='MISS'
    elif e>0: err+=1; status='ERR'
    else: ok+=1; status='OK'
    type_status.append((at,status,n,e))
print(f"\n  *** RESUMEN: OK={ok} ERR={err} MISS={miss} total={len(inserted)} ***\n")
for at,st,n,e in sorted(type_status, key=lambda x:x[1]):
    print(f"  {st:<5} {at:<35} inbox={n} err={e}")

# Snapshot final - SIN LIMPIAR
print("\n[Snapshot final - SIN LIMPIAR]")
co.execute('SELECT COUNT(*), NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX')
ox, ox_mx = co.fetchone()
c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox'); ib_f = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=1'); ib_pr = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_parsed'); ps_f = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors'); er_f = c.fetchone()[0]
print(f"  FCME_USER.CDC_OUTBOX: {ox} filas (max_id={ox_mx})")
print(f"  fcme_canonicos.cdc_inbox: {ib_f} filas (processed={ib_pr})")
print(f"  fcme_canonicos.cdc_inbox_parsed: {ps_f} filas")
print(f"  fcme_canonicos.cdc_inbox_errors: {er_f} filas")

print("\n[Eventos en CDC_OUTBOX (Oracle)]")
co.execute('SELECT ID, AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, SOURCE_TABLE, CREATED_AT FROM FCME_USER.CDC_OUTBOX ORDER BY ID')
for r in co.fetchall():
    print(f"  id={r[0]} agg={r[1]} type={r[2]} ev={r[3]} src={r[4]} created={r[5]}")

print("\n[Eventos en cdc_inbox (SQL Server canonicos)]")
c.execute('SELECT id, aggregate_id, aggregate_type, event_type, processed, processed_at FROM dbo.cdc_inbox ORDER BY id')
for r in c.fetchall():
    print(f"  id={r.id} agg={r.aggregate_id} type={r.aggregate_type} ev={r.event_type} processed={r.processed}")

orcl.close()
print("\n=== FIN (sin cleanup de outbox/inbox) ===")
