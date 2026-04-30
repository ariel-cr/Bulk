"""Test masivo Flujo 2: 30 types end-to-end via Kafka.
Para cada type: INSERT en tabla Oracle TYPE -> Kafka -> cdc_inbox -> wrapper -> CRUD -> tabla legacy
"""
import pyodbc, oracledb, time

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
c_can = sql("fcme_canonicos").cursor()

# Reset
c_can.execute("DELETE FROM dbo.cdc_inbox")
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")
c_can.execute("DELETE FROM dbo.cdc_inbox_parsed")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
orcl.commit()
print("[Reset] outbox/inbox/errors/parsed limpios")

# Cargar config: para cada type, ver wrapper, PK Oracle, source_table legacy, db
c_can.execute("""SELECT m.aggregate_type, m.target_db, t.source_table
                 FROM dbo.cdc_inbox_module_config m
                 LEFT JOIN dbo.cdc_table_to_types t ON t.aggregate_type_emit=m.aggregate_type AND t.is_active=1
                 WHERE m.active=1
                 ORDER BY m.aggregate_type""")
configs_raw = c_can.fetchall()
configs = {}
for r in configs_raw:
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

# Disparar 30 INSERTs en Oracle, uno por type
print("\n" + "="*70)
print("[1] INSERTs en Oracle FCME_USER (30 types)")
print("="*70)

inserted = []
for at, ot in AT_TO_TABLE.items():
    co.execute("""SELECT column_name, data_type FROM all_tab_columns
                  WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id""", t=ot)
    cols = [(r[0], r[1]) for r in co.fetchall() if r[0] != "ID"]
    if not cols:
        print(f"  skip {at}: tabla {ot} sin cols")
        continue
    col_names = ", ".join(c[0] for c in cols)
    vals = []
    for cn, ct in cols:
        if "VARCHAR" in ct or "CHAR" in ct or "CLOB" in ct:
            # Para PKs y campos cortos, valor unico por type
            val = f"'TST_{at[:4].upper()}'"
        elif "NUMBER" in ct: val = "999"
        elif "DATE" in ct or "TIMESTAMP" in ct: val = "SYSTIMESTAMP"
        else: val = "NULL"
        vals.append(val)
    insert = f"INSERT INTO FCME_USER.{ot} ({col_names}) VALUES ({', '.join(vals)})"
    try:
        co.execute(insert)
        orcl.commit()
        inserted.append((at, ot))
    except Exception as e:
        print(f"  fail Oracle {ot}: {str(e)[:120]}")

print(f"  insertados: {len(inserted)}/{len(AT_TO_TABLE)}")

# Esperar propagacion
print("\n" + "="*70)
print("[2] Esperando propagacion Kafka")
print("="*70)
for i in range(20):
    time.sleep(3)
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX"); ob=co.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); ib=c_can.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=1"); pr=c_can.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors"); er=c_can.fetchone()[0]
    c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_parsed"); ps=c_can.fetchone()[0]
    print(f"  T+{(i+1)*3}s: outbox={ob} inbox={ib} processed={pr} parsed={ps} errors={er}")
    if ib >= len(inserted) and pr == ib: break

# Reporte por type
print("\n" + "="*70)
print("[3] Reporte por type")
print("="*70)
ok_legacy = 0; ok_processed_only = 0; with_error = 0
report = []
for at, ot in inserted:
    legacy_db, legacy_table = configs.get(at, (None, None))
    # Filas en cdc_inbox para este type
    c_can.execute("""SELECT COUNT(*) FROM dbo.cdc_inbox
                     WHERE aggregate_type=? AND processed=1""", at)
    n_proc = c_can.fetchone()[0]
    # Errores
    c_can.execute("SELECT COUNT(*), MAX(error_message) FROM dbo.cdc_inbox_errors WHERE aggregate_type=?", at)
    er_row = c_can.fetchone()
    n_err = er_row[0]; err_msg = er_row[1] or ""
    # Fila en legacy
    legacy_found = "?"
    if legacy_db and legacy_table:
        try:
            c_leg = sql(legacy_db).cursor()
            c_leg.execute(f"SELECT COUNT(*) FROM dbo.[{legacy_table}] WHERE EXISTS (SELECT 1 FROM dbo.[{legacy_table}] x)")
            # Buscar la fila TST_ insertada (heuristica)
            c_leg.execute(f"""SELECT TOP 1 1 FROM dbo.[{legacy_table}] x
                              WHERE EXISTS (SELECT 1 FROM sys.columns c
                                            WHERE c.object_id=OBJECT_ID('dbo.{legacy_table}'))""")
            # mejor: contar filas que sospechosamente sean del test (con cualquier varchar conteniendo TST_)
            c_leg.execute(f"""DECLARE @col SYSNAME, @sql NVARCHAR(MAX), @cnt INT = 0;
                              DECLARE cur CURSOR FAST_FORWARD FOR
                                SELECT TOP 5 c.name FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
                                WHERE c.object_id=OBJECT_ID('dbo.{legacy_table}')
                                  AND t.name IN ('varchar','nvarchar','char','nchar')
                                ORDER BY c.column_id;
                              OPEN cur; FETCH NEXT FROM cur INTO @col;
                              WHILE @@FETCH_STATUS=0
                              BEGIN
                                  SET @sql = N'SELECT @cnt = COUNT(*) FROM dbo.[{legacy_table}] WHERE [' + @col + '] LIKE ''TST_%'' ';
                                  EXEC sp_executesql @sql, N'@cnt INT OUTPUT', @cnt OUTPUT;
                                  IF @cnt > 0 BREAK;
                                  FETCH NEXT FROM cur INTO @col;
                              END
                              CLOSE cur; DEALLOCATE cur;
                              SELECT @cnt""")
            legacy_found = c_leg.fetchone()[0]
        except Exception as e:
            legacy_found = f"err({str(e)[:60]})"

    if n_err > 0:
        with_error += 1
        status = f"ERR"
    elif n_proc > 0 and (isinstance(legacy_found, int) and legacy_found > 0):
        ok_legacy += 1
        status = "OK"
    elif n_proc > 0:
        ok_processed_only += 1
        status = "PROC"
    else:
        status = "?"

    report.append((at, status, n_proc, n_err, legacy_found, err_msg[:120], legacy_db, legacy_table))

# Print summary
print(f"  {'TYPE':<35} {'STATUS':<6} {'PROC':<5} {'ERR':<4} {'LEG':<10} {'TARGET':<25}")
for at, st, pr, er, lf, msg, db, tbl in sorted(report, key=lambda x: x[1]):
    print(f"  {at:<35} {st:<6} {pr:<5} {er:<4} {str(lf):<10} {db}.{tbl}")
    if msg: print(f"      err: {msg}")

print(f"\n  RESUMEN: OK_legacy={ok_legacy}  PROC_only={ok_processed_only}  ERR={with_error}")

# Cleanup
print("\n" + "="*70)
print("[4] Cleanup")
print("="*70)
for at, ot in inserted:
    try:
        co.execute(f"DELETE FROM FCME_USER.{ot} WHERE 1=1 AND ROWID IN (SELECT ROWID FROM FCME_USER.{ot} ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY)")
    except: pass
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
orcl.commit()
c_can.execute("DELETE FROM dbo.cdc_inbox")
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")
c_can.execute("DELETE FROM dbo.cdc_inbox_parsed")

# Limpiar TST_ rows en legacy
LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
for db in LEG_DBS:
    try:
        c_db = sql(db).cursor()
        c_db.execute("EXEC sp_set_session_context N'is_replicating', 1")
        # Buscar tablas con cols TST_
        # No vamos a hacer cleanup masivo aqui - los TST_ pueden quedar y se limpian manualmente
        c_db.execute("EXEC sp_set_session_context N'is_replicating', 0")
    except: pass

orcl.close()
print("  cleanup ok (filas TST_ en legacy quedan, requieren limpieza manual)")
