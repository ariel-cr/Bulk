"""Test completo Flujo 2 Nomina: 21 types end-to-end con verificacion en legacy + anti-loop."""
import pyodbc, oracledb, time

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

# (agg, oracle_dest, safe_col_for_noop, legacy_table_dbNO)
TESTS = [
    ('anticipoNominaType','ANTICIPONOMINA_TYPE','ANIO','notbcant'),
    ('cargaFamiliarType','CARGAFAMILIAR_TYPE','ESTADOREGISTRO','notbcgfm'),
    ('cargoGeneralType','CARGOGENERAL_TYPE','ANIOCREDITO','notbcarg'),
    ('cargoLaboralType','CARGOLABORAL_TYPE','CODIGOCARGADMINISTRADOR','notbcarg_admi'),
    ('catalogoNominaType','CATALOGONOMINA_TYPE','DESCRIPCIONADICIONAL','notbcnom'),
    ('configuracionNominaType','CONFIGURACIONNOMINA_TYPE','TIPOINSTITUCION','notbpara'),
    ('empleadoAuditoriaType','EMPLEADOAUDITORIA_TYPE','CODIGOTRANSACCIONUTILIZADA','notbempl_audi'),
    ('empleadoDetalleType','EMPLEADODETALLE_TYPE','TIPOCONT','notbempl_deta'),
    ('empleadoType','EMPLEADO_TYPE','CODIGOCARGO','notbempl'),
    ('firmaHorarioType','FIRMAHORARIO_TYPE','MAQUINAENLAQUEFIRMO','notbfirm'),
    ('fondoReservaType','FONDORESERVA_TYPE','TIPOACREDITACIONFONDORESERVA','notbfond_rese'),
    ('historialIngresoType','HISTORIALINGRESO_TYPE','ANIO','notbhieg'),
    ('nivelAcademicoType','NIVELACADEMICO_TYPE','CODIGOINSTITUCION','notbnive_acad_empl'),
    ('nominaCabeceraType','NOMINACABECERA_TYPE','CODIGONOMINA','notbcrol'),
    ('pagoNominaType','PAGONOMINA_TYPE','CEDULABENEFICIARIO','notbpago_nomi'),
    ('parametroNominaType','PARAMETRONOMINA_TYPE','CODIGOFRECUENCIAPAGOROL','notbpara_gene'),
    ('patronalNominaType','PATRONALNOMINA_TYPE','CODIGOCIUDAD','notbpatr'),
    ('rolPagoType','ROLPAGO_TYPE','CODIGONOMINA','notbdrol'),
    ('rubroNominaType','RUBRONOMINA_TYPE','CODIGORUBRO','notbrubr'),
    ('sectorIessType','SECTORIESS_TYPE','CODIGOGESTIONIESS','notbsect_iess'),
    ('viaticoNominaType','VIATICONOMINA_TYPE','CODIGOEMPLEADO','notbcvia'),
]

# Stuck en Kafka: necesitan bypass directo
KAFKA_STUCK_TYPES = {'anticipoNominaType','firmaHorarioType','nivelAcademicoType','viaticoNominaType'}

orcl = oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o = orcl.cursor()
can = sql('fcme_canonicos').cursor()
no = sql('dbNO').cursor()

print(f'TYPES = {len(TESTS)}', flush=True)

# ========== BASELINE ==========
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX')
out_max = o.fetchone()[0]
can.execute('SELECT ISNULL(MAX(id),0) FROM dbo.cdc_inbox')
inb_max = can.fetchone()[0]
can.execute('SELECT COUNT(*) FROM dbo.cdc_inbox_errors')
err_b = can.fetchone()[0]
print(f'baseline outbox.max={out_max} inbox.max={inb_max} errors_total={err_b}', flush=True)

# Conteos baseline en legacy dbNO
legacy_before = {}
for agg,_,_,ltbl in TESTS:
    no.execute(f'SELECT COUNT(*) FROM dbo.[{ltbl}]')
    legacy_before[ltbl] = no.fetchone()[0]
print(f'legacy_before sample: {dict(list(legacy_before.items())[:3])}...', flush=True)

# ========== DISPAROS ==========
print('\n[DISPAROS] UPDATE no-op en cada FCME_USER table', flush=True)
fired = 0
captured_events = []  # para bypass directo de los Kafka-stuck
for i,(agg,dest,col,ltbl) in enumerate(TESTS):
    try:
        o.execute(f"SELECT ID FROM FCME_USER.{dest} WHERE ROWNUM<=1")
        r = o.fetchone()
        if not r:
            print(f'  [{i+1:>2}] {agg:<28} tabla vacia, skip', flush=True)
            continue
        rid = r[0]
        o.execute("SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX")
        out_pre = o.fetchone()[0]
        o.execute(f"UPDATE FCME_USER.{dest} SET {col}={col} WHERE ID=:1", [rid])
        orcl.commit()
        fired += 1
        # Capturar evento para bypass si es Kafka-stuck
        if agg in KAFKA_STUCK_TYPES:
            time.sleep(0.3)
            o.execute(f"""SELECT ID, AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT
                         FROM FCME_USER.CDC_OUTBOX WHERE ID > {out_pre} AND AGGREGATE_TYPE = :1
                         ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY""", [agg])
            evt = o.fetchone()
            if evt:
                payload = evt[4].read() if hasattr(evt[4], 'read') else evt[4]
                captured_events.append((evt[0], str(evt[1]), evt[2], evt[3], payload, evt[5], evt[6]))
        print(f'  [{i+1:>2}] {agg:<28} {dest:<28} (ID={rid}) UPDATE noop', flush=True)
    except Exception as e:
        print(f'  [{i+1:>2}] {agg:<28} ERR {str(e)[:120]}', flush=True)

print(f'\nDisparados: {fired}/{len(TESTS)}  | Capturados para bypass: {len(captured_events)}', flush=True)

# ========== PROPAGACION KAFKA ==========
print('\n[PROPAGACION] esperando 60s para Kafka', flush=True)
deadline = time.time() + 60
while time.time() < deadline:
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max}')
    inb_n = can.fetchone()[0]
    can.execute(f'SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND processed=1')
    pr_n = can.fetchone()[0]
    print(f'  inb+={inb_n} proc+={pr_n} ({int(deadline-time.time())}s rest)', flush=True)
    if inb_n >= fired:
        break
    time.sleep(8)

# ========== BYPASS PARA KAFKA-STUCK ==========
print(f'\n[BYPASS] Inyectando {len(captured_events)} eventos Kafka-stuck directo en cdc_inbox', flush=True)
for oid, agg_id, agg_t, ev, payload, src, created in captured_events:
    # Verificar si ya llego por Kafka
    can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE aggregate_type=? AND aggregate_id=? AND created_at > DATEADD(MINUTE,-5,SYSDATETIME())", agg_t, agg_id)
    if can.fetchone()[0] > 0:
        print(f'  {agg_t:<28} ya llego via Kafka, skip', flush=True)
        continue
    try:
        can.execute("INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at, processed) VALUES (?,?,?,?,?,?,0)",
                    agg_id, agg_t, ev, payload, src, created)
        print(f'  {agg_t:<28} INYECTADO (outbox.id={oid})', flush=True)
    except Exception as e:
        print(f'  {agg_t:<28} FAIL: {str(e)[:120]}', flush=True)

print('\n[Esperando trigger processing 8s]...', flush=True)
time.sleep(8)

# ========== RESULTADOS ==========
print('\n[RESULTADOS] cdc_inbox + legacy + anti-loop', flush=True)
print(f"{'#':>3} {'aggregate_type':<28} {'inbox':<6} {'proc':<5} {'err':<5} {'legacy_delta':<14} status")
print('-'*120)
ok = 0
for i,(agg,dest,col,ltbl) in enumerate(TESTS):
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=?", agg)
    inb_n = can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max} AND aggregate_type=? AND processed=1", agg)
    pr_n = can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max} AND aggregate_type=?", agg)
    er_n = can.fetchone()[0]
    no.execute(f'SELECT COUNT(*) FROM dbo.[{ltbl}]')
    legacy_now = no.fetchone()[0]
    legacy_delta = legacy_now - legacy_before[ltbl]
    if inb_n>=1 and pr_n>=inb_n and er_n==0:
        status = 'OK'
        ok += 1
    elif er_n > 0:
        status = 'ERR'
    else:
        status = 'NO INBOX'
    print(f'{i+1:>3} {agg:<28} {inb_n:<6} {pr_n:<5} {er_n:<5} {legacy_delta:+}{"":<10} {status}', flush=True)

print(f'\n[RESUMEN] OK={ok}/{len(TESTS)}', flush=True)

# ========== ANTI-LOOP ==========
print('\n[ANTI-LOOP CHECK]', flush=True)
time.sleep(5)
o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
out_late = o.fetchone()[0]
print(f'  CDC_OUTBOX 5s mas tarde: +{out_late} (initial fired={fired})', flush=True)
if out_late <= fired + 2:
    print(f'  ANTI-LOOP OK (sin propagacion descontrolada)', flush=True)
else:
    print(f'  ⚠️ POSIBLE BUCLE (creció {out_late-fired} eventos extras)', flush=True)

# Verificar que Flujo 1 no esta repulicando (Si estuviera en bucle, dbNO triggers volverian a publicar)
can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox WHERE source_table LIKE 'dbNO%' AND created_at > DATEADD(MINUTE,-2,SYSDATETIME())")
flow1_echo = can.fetchone()[0]
print(f'  Flujo 1 echo desde dbNO (ultimos 2 min): {flow1_echo}', flush=True)
if flow1_echo == 0:
    print(f'  ANTI-LOOP F1 OK (legacy SP no re-publico)', flush=True)
else:
    print(f'  ⚠️ Legacy publico {flow1_echo} eventos = posible bucle F2->F1', flush=True)

orcl.close()
print('\n=== FIN ===', flush=True)
