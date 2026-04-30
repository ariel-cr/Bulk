"""Pre-check del segmento 2 del Flujo 1 (Kafka -> CDC_INBOX -> TRG_PROCESS -> MODULE_CONFIG).

Solo lectura. Verifica:
  1) Conector Kafka legacy (REST API en 10.35.3.64:8083)
  2) Tabla FCME_USER.CDC_INBOX existe + estructura
  3) TRG_PROCESS_CDC_INBOX existe + status ENABLED
  4) CDC_INBOX_MODULE_CONFIG existe + schema (que columnas tiene)
  5) Cuenta entradas existentes en MODULE_CONFIG
  6) Cuantas filas hay en CDC_INBOX y CDC_OUTBOX hoy (para baseline)
"""
import json, urllib.request
import oracledb, pyodbc

ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}
DB  = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
KAFKA_REST = "http://10.35.3.64:8083"

print("="*70)
print("PRE-CHECK Segmento 2 Flujo 1")
print("="*70)

# 1) Kafka REST
print("\n[1] Kafka Connect REST API")
try:
    with urllib.request.urlopen(f"{KAFKA_REST}/connectors", timeout=8) as r:
        connectors = json.loads(r.read())
    print(f"  REST OK; {len(connectors)} conectores: {connectors}")
    for cn in connectors:
        try:
            with urllib.request.urlopen(f"{KAFKA_REST}/connectors/{cn}/status", timeout=8) as r:
                st = json.loads(r.read())
            print(f"    {cn}: type={st.get('type')} state={st['connector']['state']}  tasks={[t['state'] for t in st.get('tasks',[])]}")
        except Exception as e:
            print(f"    {cn}: ERR status -> {str(e)[:120]}")
except Exception as e:
    print(f"  KAFKA REST no accesible: {str(e)[:160]}")

# 2-6) Oracle
print("\n[2-6] Oracle FCME_USER")
orcl = oracledb.connect(**ORA)
o = orcl.cursor()

# CDC_INBOX
o.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name='CDC_INBOX'")
print(f"\n  [2] CDC_INBOX existe: {'SI' if o.fetchone()[0]>0 else 'NO'}")
o.execute("SELECT column_name, data_type FROM all_tab_columns WHERE owner='FCME_USER' AND table_name='CDC_INBOX' ORDER BY column_id")
cols = o.fetchall()
print(f"      Columnas: {len(cols)}")
for col, dt in cols[:12]:
    print(f"        {col} ({dt})")
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
print(f"      Filas actuales: {o.fetchone()[0]:,}")
try:
    o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=0")
    print(f"      Filas pendientes (PROCESSED=0): {o.fetchone()[0]:,}")
except Exception as e:
    print(f"      [no existe columna PROCESSED: {str(e)[:80]}]")

# TRG_PROCESS_CDC_INBOX
print(f"\n  [3] TRG_PROCESS_CDC_INBOX")
o.execute("SELECT status, trigger_type, triggering_event FROM all_triggers WHERE owner='FCME_USER' AND trigger_name='TRG_PROCESS_CDC_INBOX'")
r = o.fetchone()
if r:
    print(f"      status={r[0]}  type={r[1]}  event={r[2]}")
else:
    print(f"      NO EXISTE")

# CDC_INBOX_MODULE_CONFIG
print(f"\n  [4] CDC_INBOX_MODULE_CONFIG")
o.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name='CDC_INBOX_MODULE_CONFIG'")
exists = o.fetchone()[0] > 0
print(f"      Existe: {'SI' if exists else 'NO'}")
if exists:
    o.execute("SELECT column_name, data_type, nullable FROM all_tab_columns WHERE owner='FCME_USER' AND table_name='CDC_INBOX_MODULE_CONFIG' ORDER BY column_id")
    for col, dt, nn in o.fetchall():
        print(f"        {col} ({dt}, nullable={nn})")
    o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG")
    print(f"      Filas actuales: {o.fetchone()[0]:,}")

    # Cuantos types de cartera ya tienen entry?
    CARTERA_TYPES = [
        "abonoExtraordinario_type","autorizacionCreditoDetalle_type","autorizacionCredito_type",
        "auxDatosCobrosAdicionalesType","calificacionCarteraDetalle_type","calificacionCartera_type",
        "cancelacionCredito_type","caucionCredito_type","cobranzaJudicialDetalle_type",
        "cobranzaJudicialDistribucion_type","cobranzaJudicial_type","conceptoGastoJudicialType",
        "contabilizacionCredito_type","convenioPagoCredito_type","costoFinancieroCredito_type",
        "creditoType","cuentaAutomaticaDetalle_type","cuentaAutomatica_type","cuentaCuotasType",
        "cuentaCxPCxCType","cuentaPersonasType","cuentaPorCobrarType","cuentaType",
        "cuentasEnLegalType","cuotaConvenio_type","cuotaCreditoType","desembolsoCredito_type",
        "desembolsoDevolucion_type","detalleRecuperacion_type","devengamientoCarteraDetalle_type",
        "devengamientoCartera_type","devolucionCredito_type","devolucionMasivaDetalle_type",
        "devolucionMasiva_type","documentoCredito_type","estadoConvenioCredito_type",
        "estadoLegalType","etapaJudicialCredito_type","fechasProcesoType","flujoTrabajoCredito_type",
        "garantiaCredito_type","gestionCobranzaAsignacion_type","gestionComunicacionCredito_type",
        "grupoCreditoDetalle_type","grupoCreditoDocumento_type","informacionLegal_type",
        "liquidacionDiariaCredito_type","medidaJudicialType","movimientoContableCredito_type",
        "obligacionRol_type","operacionConyugal_type","pagoCredito_type","pagosCreditoType",
        "personaCreditoType","personaCxPCxCType","planPagoAjuste_type","planPago_type",
        "plazoVencido_type","precalificacionCredito_type","procesoAccionType",
        "recuperacionConvenio_type","recuperacionCredito_type","referenciaCliente_type",
        "referenciaDeudor_type","refinanciamientoCreditoType","reporteSBSCabecera_type",
        "reporteSBSDetalle_type","reporteSBSGaranteCodeudor_type","reporteSBSGarantiaReal_type",
        "reporteSBSOperacionAnterior_type","reporteSBSOperacionCancelada_type",
        "reporteSBSOperacionConcedida_type","reporteSBSSaldoOperacion_type","reporteSBSSujetoRiesgo_type",
        "rubroCobranza_type","rubrosCobranzaDetalle_type","saldoCarteraDetalle_type","saldoCartera_type",
        "saldoCxPCxCType","saldoVinculadoType","seguimientoAutorizacion_type","seguroCredito_type",
        "sobranteCaucion_type","sobranteCredito_type","sobranteDistribucion_type",
        "solidarioCredito_type","tasaInteresCredito_type","tipoCredito_type","tipoSobrante_type",
        "transaccionRecuperacion_type","unidadJudicialType",
    ]
    placeholders = ",".join(f":{i+1}" for i in range(len(CARTERA_TYPES)))
    o.execute(f"SELECT AGGREGATE_TYPE FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE IN ({placeholders})", CARTERA_TYPES)
    existing = [r[0] for r in o.fetchall()]
    missing = sorted(set(CARTERA_TYPES) - set(existing))
    print(f"      Cartera types con entry: {len(existing)}/{len(CARTERA_TYPES)}")
    print(f"      Cartera types SIN entry (a insertar): {len(missing)}")

# 6) Baseline counts
print(f"\n  [6] Baseline metrics")
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
print(f"      CDC_INBOX rows         : {o.fetchone()[0]:,}")
try:
    o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
    print(f"      CDC_INBOX_ERRORS rows  : {o.fetchone()[0]:,}")
except Exception:
    pass

# canonicos cdc_outbox
sg = pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=fcme_canonicos;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=30).cursor()
sg.execute("SELECT COUNT(*) FROM cdc_outbox")
print(f"      canonicos cdc_outbox   : {sg.fetchone()[0]:,}")

orcl.close()
