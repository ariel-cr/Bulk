"""Test masivo Flujo 2: 30 types con DATOS VALIDOS (tipos correctos por col)."""
import pyodbc, oracledb, time, json

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
c_can = sql("fcme_canonicos").cursor()

# Reset
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX"); orcl.commit()
c_can.execute("DELETE FROM dbo.cdc_inbox")
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")
c_can.execute("DELETE FROM dbo.cdc_inbox_parsed")
print("[Reset] outbox/inbox/errors/parsed limpios")

# Cargar config: para cada type, leer wrapper, target_db
c_can.execute("""SELECT m.aggregate_type, m.target_db, t.source_table
                 FROM dbo.cdc_inbox_module_config m
                 LEFT JOIN dbo.cdc_table_to_types t ON t.aggregate_type_emit=m.aggregate_type AND t.is_active=1
                 WHERE m.active=1""")
configs = {}
for r in c_can.fetchall():
    if r.aggregate_type not in configs:
        configs[r.aggregate_type] = (r.target_db, r.source_table)

AT_TO_TABLE = {
    "actualizacionAfiliadoType":"ACTUALIZACION_AFILIADO_TYPE",
    "actualizacionDocumentosType":"ACTUALIZACION_DOCUMENTOS_TYPE",
    "agendaMailAfiliadoType":"AGENDAMAILAFILIADO_TYPE",
    "auditoriaAfiliadoType":"AUDITORIAAFILIADO_TYPE",
    "beneficiarioParticipeType":"BENEFICIARIOPARTICIPE_TYPE",
    "cuentaBancariaAfiliadoType":"CUENTABANCARIAAFILIADO_TYPE",
    "distribucionAfiliadoType":"DISTRIBUCIONAFILIADO_TYPE",
    "documentacionAfiliadoType":"DOCUMENTACIONAFILIADO_TYPE",
    "firmanteParticipeType":"FIRMANTEPARTICIPE_TYPE",
    "grupoFamiliarType":"GRUPOFAMILIAR_TYPE",
    "informacionAdicionalAfiliadoType":"INFORMACIONADICIONALAFILIADO_TYPE",
    "institucionType":"INSTITUCION_TYPE",
    "motivoContableType":"MOTIVOCONTABLE_TYPE",
    "movimientoCuentaType":"MOVIMIENTOCUENTA_TYPE",
    "movimientoTemporalType":"MOVIMIENTOTEMPORAL_TYPE",
    "naturalInformacionAdicionalType":"NATURALINFORMACIONADICIONALTYPE",
    "naturalIngresosEgresosType":"NATURALINGRESOSEGRESOSTYPE",
    "naturalTrabajoType":"NATURALTRABAJOTYPE",
    "personaReferenciasBancariasType":"PERSONAREFERENCIASBANCARIASTYPE",
    "personaReferenciasPersonalesType":"PERSONAREFERENCIASPERSONALESTYPE",
    "personaTelefonosType":"PERSONATELEFONOSTYPE",
    "personaVinculacionesType":"PERSONAVINCULACIONESTYPE",
    "referenciaParticipeType":"REFERENCIAPARTICIPE_TYPE",
    "reporteSIBSParticipeType":"REPORTESIBSPARTICIPE_TYPE",
    "retiroLiquidacionType":"RETIROLIQUIDACION_TYPE",
    "retiroVoluntarioEstadoType":"RETIROVOLUNTARIOESTADO_TYPE",
    "saldoDiarioRubroType":"SALDODIARIORUBRO_TYPE",
    "saldoDiarioType":"SALDODIARIO_TYPE",
    "seguroVidaParticipeType":"SEGUROVIDAPARTICIPE_TYPE",
    "servicioAdicionalType":"SERVICIOADICIONAL_TYPE",
}

# Para cada type, construir INSERT con valores numericos cortos en strings cortas
# Esto ayuda a que las PKs SMALLINT/INT no fallen
print("\n" + "="*70)
print("[1] INSERTs Oracle FCME_USER con valores que respetan tipos")
print("="*70)
inserted = []
for at, ot in AT_TO_TABLE.items():
    co.execute("""SELECT column_name, data_type, data_length, nullable FROM all_tab_columns
                  WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id""", t=ot)
    cols = [(r[0], r[1], r[2], r[3]) for r in co.fetchall() if r[0] != "ID"]
    if not cols: continue
    col_names = ", ".join(c[0] for c in cols)
    vals = []
    for cn, ct, dl, nl in cols:
        if "VARCHAR" in ct or "CHAR" in ct:
            # valores cortos numericos en strings (compatibles con SMALLINT/INT al castear)
            v = "'9'" if (dl or 99) <= 2 else "'99'"
        elif "NUMBER" in ct: v = "9"
        elif "DATE" in ct or "TIMESTAMP" in ct: v = "SYSTIMESTAMP"
        elif "CLOB" in ct: v = "'{}'"
        else: v = "NULL"
        vals.append(v)
    insert = f"INSERT INTO FCME_USER.{ot} ({col_names}) VALUES ({', '.join(vals)})"
    try:
        co.execute(insert)
        orcl.commit()
        inserted.append((at, ot))
    except Exception as e:
        print(f"  fail Oracle {ot}: {str(e)[:100]}")

print(f"  inserted Oracle: {len(inserted)}/{len(AT_TO_TABLE)}")

# Esperar
print("\n" + "="*70)
print("[2] Esperando propagacion")
print("="*70)
prev_inb = 0
stable_count = 0
for i in range(40):
    time.sleep(2)
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX"); ob=co.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); ib=c_can.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=1"); pr=c_can.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors"); er=c_can.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_parsed"); ps=c_can.fetchone()[0]
    print(f"  T+{(i+1)*2}s: outbox={ob} inbox={ib} processed={pr} parsed={ps} errors={er}")
    if ib == prev_inb:
        stable_count += 1
        if stable_count >= 5 and ib >= len(inserted): break
    else:
        stable_count = 0
    prev_inb = ib

# Reporte detallado
print("\n" + "="*70)
print("[3] Resultado por type")
print("="*70)
header = f"  {'TYPE':<35} {'INBOX':<6} {'PROC':<5} {'PARSE':<6} {'ERR':<4} {'STATUS'}"
print(header)
ok_count = 0; err_count = 0; missing_count = 0
for at, ot in inserted:
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE aggregate_type=?", at)
    n_inb = c_can.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE aggregate_type=? AND processed=1", at)
    n_pr = c_can.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_parsed WHERE aggregate_type=?", at)
    n_ps = c_can.fetchone()[0]
    c_can.execute("SELECT COUNT(*), MAX(error_message) FROM dbo.cdc_inbox_errors WHERE aggregate_type=?", at)
    er_row = c_can.fetchone()
    n_er = er_row[0]; err_msg = er_row[1] or ""

    if n_er > 0:
        status = "ERR"; err_count += 1
    elif n_ps > 0:
        status = "OK"; ok_count += 1
    elif n_inb == 0:
        status = "MISSING"; missing_count += 1
    else:
        status = "?"
    print(f"  {at:<35} {n_inb:<6} {n_pr:<5} {n_ps:<6} {n_er:<4} {status}")
    if status == "ERR": print(f"      err: {err_msg[:150]}")

print(f"\nRESUMEN: OK={ok_count}  ERR={err_count}  MISSING={missing_count}  total={len(inserted)}")

# Stats globales
print("\nGlobal:")
c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); print(f"  cdc_inbox total: {c_can.fetchone()[0]}")
c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_parsed"); print(f"  cdc_inbox_parsed total: {c_can.fetchone()[0]}")
c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors"); print(f"  cdc_inbox_errors total: {c_can.fetchone()[0]}")

# Cleanup
print("\n" + "="*70)
print("[4] Cleanup")
print("="*70)
for at, ot in inserted:
    try:
        co.execute(f"DELETE FROM FCME_USER.{ot} WHERE ROWID IN (SELECT ROWID FROM FCME_USER.{ot} ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY)")
    except: pass
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX"); orcl.commit()
c_can.execute("DELETE FROM dbo.cdc_inbox")
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")
c_can.execute("DELETE FROM dbo.cdc_inbox_parsed")
print("  ok")
orcl.close()
