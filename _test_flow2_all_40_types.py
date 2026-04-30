"""Flujo 2 (Newcore -> Legacy): test masivo 40 types.
- UPDATE no-op en cada FCME_USER.<TABLE>
- Watch CDC_OUTBOX -> Kafka -> cdc_inbox -> wrapper -> sp_*Type_CRUD -> legacy
- Sin bucle (anti-loop CLIENT_INFO + SESSION_CONTEXT)
"""
import pyodbc, oracledb, time, sys

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# (agg_type, oracle_table, oracle_pk_col, legacy_db, legacy_table)
TYPES_40 = [
    ('actualizacionAfiliadoType','ACTUALIZACION_AFILIADO_TYPE','CODIGO_CEDU','dbFC','fctbafil_actu'),
    ('actualizacionDocumentosType','ACTUALIZACION_DOCUMENTOS_TYPE','SECUENCIA_ACTU_DOCS','dbFC','fctbafil_info_actu_docs'),
    ('agendaMailAfiliadoType','AGENDAMAILAFILIADO_TYPE','CODIGOEMPRESA','dbFC','fctbagen_mail'),
    ('areaLaboralParticipeType','AREALABORALPARTICIPE_TYPE','DESCRIPCIONAREALABORAL','dbFC','fctbarea_lbrl'),
    ('auditoriaAfiliadoType','AUDITORIAAFILIADO_TYPE','IDENTIFICACION','dbFC','sfct_motivo_mant_afiliados'),
    ('beneficiarioParticipeType','BENEFICIARIOPARTICIPE_TYPE','APELLIDOSBENEFICIARIOS','dbFC','sfct_beneficiario'),
    ('comisionParticipe_type','COMISIONPARTICIPE_TYPE','CEDULAPROMOTOR','dbCT','cttbcomi_cred'),
    ('cuentaBancariaAfiliadoType','CUENTABANCARIAAFILIADO_TYPE','CEDULABENEFICIARIO','dbFC','sfct_padbs'),
    ('distribucionAfiliadoType','DISTRIBUCIONAFILIADO_TYPE','CIRCUITO','dbCT','cttbmatr_dist_afil'),
    ('documentacionAfiliadoType','DOCUMENTACIONAFILIADO_TYPE','IDENTIFICACION','dbFC','fctbcart_rpag'),
    ('firmanteParticipeType','FIRMANTEPARTICIPE_TYPE','CODIGOEMPRESA','dbFC','sfct_firmante'),
    ('grupoFamiliarType','GRUPOFAMILIAR_TYPE','APELLIDOSGRUPOFAMILIAR','dbFC','sfct_grupo_fami'),
    ('imagenesType','IMAGENESTYPE','CODIGOIMAGEN','dbFC','fctbpart_foto'),
    ('informacionAdicionalAfiliadoType','INFORMACIONADICIONALAFILIADO_TYPE','CODIGOGENERO','dbFC','fctbafil_info_adic'),
    ('institucionType','INSTITUCION_TYPE','NOMBREINSTITUCION','dbFC','sfct_institucion'),
    ('juridicoInformacionBasicaType','JURIDICOINFORMACIONBASICATYPE','IDENTIFICACION','dbFC','fctbjuri_inst'),
    ('motivoContableType','MOTIVOCONTABLE_TYPE','CODIGOEMPRESA','dbFC','sfct_motivo_cnta_cble'),
    ('movimientoCuentaType','MOVIMIENTOCUENTA_TYPE','IDENTIFICACION','dbFC','sfct_movimiento'),
    ('movimientoTemporalType','MOVIMIENTOTEMPORAL_TYPE','FECHARETIROFCME','dbFC','sfct_movimiento_temp'),
    ('naturalInformacionAdicionalType','NATURALINFORMACIONADICIONALTYPE','IDENTIFICACION','dbFC','fctbafil_info_actu_docs'),
    ('naturalInformacionBasicaType','NATURALINFORMACIONBASICATYPE','IDENTIFICACION','dbFC','sfct_afiliado'),
    ('naturalIngresosEgresosType','NATURALINGRESOSEGRESOSTYPE','IDENTIFICACION','dbFC','sfct_afiliado_otros'),
    ('naturalTrabajoType','NATURALTRABAJOTYPE','IDENTIFICACION','dbFC','sfct_afiliado'),
    ('otrosIngresosAfiliadoType','OTROSINGRESOSAFILIADO_TYPE','CODIGOCEDU','dbFC','fctbotro_ingr_afil'),
    ('personaDireccionesType','PERSONADIRECCIONESTYPE','IDENTIFICACION','dbFC','sfct_afiliado'),
    ('personaFirmasType','PERSONAFIRMASTYPE','IDENTIFICACION','dbIM','imtbbene_firm'),
    ('personaReferenciasBancariasType','PERSONAREFERENCIASBANCARIASTYPE','IDENTIFICACION','dbFC','sfct_afiliado_referencias'),
    ('personaReferenciasPersonalesType','PERSONAREFERENCIASPERSONALESTYPE','IDENTIFICACION','dbFC','fctbafil_ahor_refe'),
    ('personaTelefonosType','PERSONATELEFONOSTYPE','IDENTIFICACION','dbFC','fctbagen_telf_part'),
    ('personaType','PERSONATYPE','IDENTIFICACION','dbFC','sfct_afiliado'),
    ('personaVinculacionesType','PERSONAVINCULACIONESTYPE','IDENTIFICACION','dbIM','imtbmiem_cony'),
    ('referenciaParticipeType','REFERENCIAPARTICIPE_TYPE','DESCRIPCIONTIPOREFERENCIA','dbFC','sfct_referencias'),
    ('reporteSIBSParticipeType','REPORTESIBSPARTICIPE_TYPE','CODIGOUSUARIOGENERACION','dbFC','fctbcinf_part_sibs'),
    ('retiroLiquidacionType','RETIROLIQUIDACION_TYPE','CODIGOTIPOIDENTIFICACION','dbFC','sfct_retiro'),
    ('retiroVoluntarioEstadoType','RETIROVOLUNTARIOESTADO_TYPE','ANIO','dbFC','fctbrvol_esta_afil'),
    ('rolNominaType','ROLNOMINA_TYPE','CODIGORUBRO','dbFC','sfct_rubro_rol'),
    ('saldoDiarioRubroType','SALDODIARIORUBRO_TYPE','FECHASALDO','dbFC','fctbsald_diar_rubr'),
    ('saldoDiarioType','SALDODIARIO_TYPE','IDENTIFICACION','dbFC','fctbsald_diar_afil_rubr'),
    ('seguroVidaParticipeType','SEGUROVIDAPARTICIPE_TYPE','CODIGODISCAPACIDADFAMILIARES','dbSV','svtbcaus'),
    ('servicioAdicionalType','SERVICIOADICIONAL_TYPE','IDENTIFICACION','dbFC','fctbgene_sibs'),
]

orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()
can=sql('fcme_canonicos').cursor()

print(f'TYPES = {len(TYPES_40)}', flush=True)

# baseline
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX')
out_max=o.fetchone()[0]
can.execute('SELECT ISNULL(MAX(id),0) FROM dbo.cdc_inbox')
inb_max=can.fetchone()[0]
can.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors')
err_b=can.fetchone()[0]
print(f'baseline outbox.max={out_max} inbox.max={inb_max} errors={err_b}', flush=True)

# UPDATE no-op por type
print('\n[DISPAROS]', flush=True)
fired=0
for i,(agg,otbl,opk_col,ldb,ltbl) in enumerate(TYPES_40):
    try:
        # Tomar 1 fila existente; UPDATE col=col
        o.execute(f"SELECT ID FROM FCME_USER.{otbl} WHERE ROWNUM<=1 ORDER BY ID")
        r=o.fetchone()
        if not r:
            print(f'  [{i+1:>2}/40] {agg:<35} {otbl}: tabla vacia, skip', flush=True)
            continue
        rid=r[0]
        o.execute(f"UPDATE FCME_USER.{otbl} SET {opk_col} = {opk_col} WHERE ID = :1", [rid])
        orcl.commit()
        fired+=1
        print(f'  [{i+1:>2}/40] {agg:<35} {otbl} (ID={rid}) UPDATE noop', flush=True)
    except Exception as e:
        print(f'  [{i+1:>2}/40] {agg:<35} ERR: {str(e)[:200]}', flush=True)

print(f'\nDisparados: {fired}/40', flush=True)

# Esperar propagacion
print('\n[PROPAGACION] esperando 90s...', flush=True)
deadline=time.time()+90
while time.time()<deadline:
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
    out_n=o.fetchone()[0]
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max}')
    inb_n=can.fetchone()[0]
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND processed=1')
    pr_n=can.fetchone()[0]
    can.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors')
    er_n=can.fetchone()[0]
    print(f'  out+={out_n} inb+={inb_n} processed+={pr_n} errors_total={er_n} ({int(deadline-time.time())}s rest)', flush=True)
    if inb_n>=fired and pr_n>=inb_n:
        break
    time.sleep(8)

# Validar por type
print('\n[RESULTADOS]', flush=True)
print(f"{'#':>3} {'aggregate_type':<38} {'inbox':<6} {'proc':<5} {'err':<5} status")
print('-'*100)
ok=0
for i,(agg,otbl,opk_col,ldb,ltbl) in enumerate(TYPES_40):
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=?", agg)
    inb_n=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=? AND processed=1", agg)
    pr_n=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=?", agg)
    er_n=can.fetchone()[0]
    # error msg
    em=None
    if er_n>0:
        can.execute(f"SELECT TOP 1 error_message FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=? ORDER BY error_id DESC", agg)
        em=can.fetchone()[0][:120]
    status='OK' if (inb_n>=1 and pr_n==inb_n and er_n==0) else (f'ERR: {em}' if em else ('NO INBOX' if inb_n==0 else 'PARTIAL'))
    if inb_n>=1 and pr_n==inb_n and er_n==0: ok+=1
    print(f'{i+1:>3} {agg:<38} {inb_n:<6} {pr_n:<5} {er_n:<5} {status[:60]}', flush=True)

print(f'\n[RESUMEN] OK={ok}/40', flush=True)

# Anti-loop check
print('\n[ANTI-LOOP CHECK]', flush=True)
time.sleep(5)
o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
out_late=o.fetchone()[0]
print(f'  CDC_OUTBOX 5s mas tarde: +{out_late} (deberia coincidir con inicial)', flush=True)
print('=== FIN ===', flush=True)
orcl.close()
