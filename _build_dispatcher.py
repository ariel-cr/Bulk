"""Construye el dispatcher completo para TODO el modulo PARTICIPE:
1) Clasifica tablas Oracle _TYPE en SNAKE_CASE (mapeo auto) vs CAMELCASE (TODO).
2) Genera USP_INBOX_PARTICIPE con CASE WHEN por cada tabla legacy.
3) Limpia data de test.
4) Pobla CDC_INBOX_MODULE_CONFIG.
5) Reprocesa pending.
"""
import pyodbc, oracledb, re
from collections import defaultdict

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# ============================================================
# PASO 1: Mapeo aggregate_type -> Oracle table
# ============================================================
AT_TO_TABLE = {
    "actualizacionAfiliadoType": "ACTUALIZACION_AFILIADO_TYPE",
    "actualizacionDocumentosType": "ACTUALIZACION_DOCUMENTOS_TYPE",
    "agendaMailAfiliadoType": "AGENDAMAILAFILIADO_TYPE",
    "areaLaboralParticipeType": "AREALABORALPARTICIPE_TYPE",
    "auditoriaAfiliadoType": "AUDITORIAAFILIADO_TYPE",
    "beneficiarioParticipeType": "BENEFICIARIOPARTICIPE_TYPE",
    "cuentaBancariaAfiliadoType": "CUENTABANCARIAAFILIADO_TYPE",
    "distribucionAfiliadoType": "DISTRIBUCIONAFILIADO_TYPE",
    "documentacionAfiliadoType": "DOCUMENTACIONAFILIADO_TYPE",
    "firmanteParticipeType": "FIRMANTEPARTICIPE_TYPE",
    "grupoFamiliarType": "GRUPOFAMILIAR_TYPE",
    "informacionAdicionalAfiliadoType": "INFORMACIONADICIONALAFILIADO_TYPE",
    "institucionType": "INSTITUCION_TYPE",
    "motivoContableType": "MOTIVOCONTABLE_TYPE",
    "movimientoCuentaType": "MOVIMIENTOCUENTA_TYPE",
    "movimientoTemporalType": "MOVIMIENTOTEMPORAL_TYPE",
    "naturalInformacionAdicionalType": "NATURALINFORMACIONADICIONALTYPE",
    "naturalInformacionBasicaType": "NATURALINFORMACIONBASICATYPE",
    "naturalIngresosEgresosType": "NATURALINGRESOSEGRESOSTYPE",
    "naturalTrabajoType": "NATURALTRABAJOTYPE",
    "otrosIngresosAfiliadoType": "OTROSINGRESOSAFILIADO_TYPE",
    "personaDireccionesType": "PERSONADIRECCIONESTYPE",
    "personaReferenciasBancariasType": "PERSONAREFERENCIASBANCARIASTYPE",
    "personaReferenciasPersonalesType": "PERSONAREFERENCIASPERSONALESTYPE",
    "personaTelefonosType": "PERSONATELEFONOSTYPE",
    "personaType": "PERSONATYPE",
    "personaVinculacionesType": "PERSONAVINCULACIONESTYPE",
    "referenciaParticipeType": "REFERENCIAPARTICIPE_TYPE",
    "reporteSIBSParticipeType": "REPORTESIBSPARTICIPE_TYPE",
    "retiroLiquidacionType": "RETIROLIQUIDACION_TYPE",
    "retiroVoluntarioEstadoType": "RETIROVOLUNTARIOESTADO_TYPE",
    "rolNominaType": "ROLNOMINA_TYPE",
    "saldoDiarioRubroType": "SALDODIARIORUBRO_TYPE",
    "saldoDiarioType": "SALDODIARIO_TYPE",
    "seguroVidaParticipeType": "SEGUROVIDAPARTICIPE_TYPE",
    "servicioAdicionalType": "SERVICIOADICIONAL_TYPE",
}

# ============================================================
# PASO 2: tabla_legacy -> [Oracle tables] desde cdc_table_to_types
# ============================================================
c = sql("fcme_canonicos").cursor()
c.execute("""
  SELECT source_table, aggregate_type_emit FROM dbo.cdc_table_to_types
  WHERE aggregate_type_emit IS NOT NULL
""")
leg_to_ora = defaultdict(list)  # source_table -> [(at_emit, oracle_table)]
for r in c.fetchall():
    ot = AT_TO_TABLE.get(r.aggregate_type_emit)
    if ot: leg_to_ora[r.source_table].append((r.aggregate_type_emit, ot))

print(f"tablas legacy con target Oracle: {len(leg_to_ora)}")

# ============================================================
# PASO 3: inspeccion columnas Oracle y estilo
# ============================================================
def get_oracle_cols(tbl):
    co.execute("""
      SELECT column_name, data_type, nullable
      FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t
      ORDER BY column_id
    """, t=tbl)
    return co.fetchall()

oracle_tables_info = {}
for at, ot in AT_TO_TABLE.items():
    cols = get_oracle_cols(ot)
    has_underscore = any("_" in c[0] and c[0] != "ID" for c in cols)
    style = "SNAKE" if has_underscore else "CAMEL"
    oracle_tables_info[ot] = {"style": style, "cols": [(c[0], c[1], c[2]) for c in cols]}

snake_tables = [ot for ot,info in oracle_tables_info.items() if info["style"]=="SNAKE"]
camel_tables = [ot for ot,info in oracle_tables_info.items() if info["style"]=="CAMEL"]
print(f"Oracle tables SNAKE_CASE (mapeo auto): {len(snake_tables)}")
for t in snake_tables: print(f"  {t}")
print(f"Oracle tables CAMEL_CASE (requieren mapeo manual): {len(camel_tables)}")
for t in camel_tables: print(f"  {t}")

# ============================================================
# PASO 4: para cada tabla legacy, obtener columnas (desde SQL Server)
# ============================================================
# Necesito conectar a cada db legacy para obtener columnas
DBS_FOR_TABLES = {
    "cgtbprvd":"dbCG",
    "crtboper_cony":"dbCR", "crtoblig":"dbCR",
    "cttbafil_audi":"dbCT", "cttbmatr_dist_afil":"dbCT", "cttbtabl_afil":"dbCT",
    "imtbmiem_cony":"dbIM",
    "notbempl":"dbNO", "notbcgfm":"dbNO",
    "svtbcaus":"dbSV","svtbdisc":"dbSV","svtbefec":"dbSV","svtbfmpg":"dbSV",
    "svtbstro":"dbSV","svtbstro_bene":"dbSV","svtbstro_cred":"dbSV",
    "svtbstro_deta":"dbSV","svtbstro_exte":"dbSV",
}
def get_legacy_cols(tbl):
    db = DBS_FOR_TABLES.get(tbl, "dbFC")
    c = sql(db).cursor()
    c.execute("""SELECT name FROM sys.columns
                 WHERE object_id=OBJECT_ID(?) ORDER BY column_id""", f"dbo.{tbl}")
    return [r.name for r in c.fetchall()]

# ============================================================
# PASO 5: regla de renombrado legacy -> snake_case Oracle
# ============================================================
PREFIX_MAP = {
    "ci_": "CODIGO_", "co_": "CODIGO_", "ds_": "DESCRIPCION_",
    "nu_": "NUMERO_", "tx_": "TEXTO_", "in_": "INDICADOR_",
    "fx_": "FECHA_", "fe_": "FECHA_",
    "ti_": "TIPO_", "no_": "NOMBRE_",
    "qs_": "SECUENCIA_", "sc_": "SECUENCIA_",
    "va_": "VALOR_", "mn_": "MONTO_", "es_": "ESTADO_",
    "st_": "ESTADO_",
}
def legacy_to_snake_upper(col):
    for p, r in PREFIX_MAP.items():
        if col.lower().startswith(p):
            return r + col[len(p):].upper()
    return col.upper()

# ============================================================
# PASO 6: generar SP
# ============================================================
def build_insert_snake(legacy_tbl, oracle_tbl, legacy_cols, oracle_cols):
    """Genera INSERT a Oracle tabla snake_case, mapeando por reglas de prefijo."""
    mapped = []
    for ol, dt, null in oracle_cols:
        if ol == "ID": continue
        # buscar en legacy_cols una columna que al renombrar matchee ol
        found = None
        for lc in legacy_cols:
            if legacy_to_snake_upper(lc) == ol:
                found = lc; break
        mapped.append((ol, found))
    # construir pares
    cols_sql = ", ".join(ol for ol,_ in mapped)
    vals_sql = ", ".join(
        f"JSON_VALUE(p_payload,'$.{lc}')" if lc else "NULL"
        for _,lc in mapped
    )
    return cols_sql, vals_sql

# Construir body del SP
sp_body = []
sp_body.append("""CREATE OR REPLACE PROCEDURE USP_INBOX_PARTICIPE(
    p_id             IN NUMBER,
    p_aggregate_type IN VARCHAR2,
    p_event_type     IN VARCHAR2,
    p_payload        IN CLOB
) AS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN""")

covered_tables = []
skipped_tables = []
for legacy_tbl, targets in sorted(leg_to_ora.items()):
    # obtener cols legacy
    try:
        legacy_cols = get_legacy_cols(legacy_tbl)
    except Exception as e:
        skipped_tables.append((legacy_tbl, str(e)[:100]))
        continue

    snake_targets = [(at, ot) for at, ot in targets if oracle_tables_info[ot]["style"] == "SNAKE"]
    if not snake_targets:
        skipped_tables.append((legacy_tbl, "todos targets son CAMEL_CASE (requieren mapeo manual)"))
        continue

    sp_body.append(f"\n    IF p_aggregate_type = '{legacy_tbl}' THEN")
    for at, ot in snake_targets:
        oracle_cols = oracle_tables_info[ot]["cols"]
        cols_sql, vals_sql = build_insert_snake(legacy_tbl, ot, legacy_cols, oracle_cols)
        # Para INSERT/UPDATE usamos MERGE por id virtual (concat PKs del payload). Simplificado: solo INSERT.
        sp_body.append(f"""
        -- target: {ot}
        BEGIN
            IF p_event_type IN ('DELETE','DELETED') THEN
                NULL; -- DELETE handler per-table, no implementado aun
            ELSE
                INSERT INTO {ot} ({cols_sql})
                VALUES ({vals_sql});
            END IF;
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    '{ot}: ' || SUBSTR(SQLERRM,1,3900));
        END;""")
    sp_body.append("    END IF;")
    covered_tables.append(legacy_tbl)

sp_body.append("""
    UPDATE CDC_INBOX SET PROCESSED=1, PROCESSED_AT=SYSTIMESTAMP WHERE ID=p_id;
    COMMIT;
EXCEPTION WHEN OTHERS THEN
    INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, SUBSTR(SQLERRM,1,4000));
    COMMIT;
END;""")

full_sp = "\n".join(sp_body)

# Escribir a archivo para inspección
with open(r"C:\Users\Usuario\Downloads\Bulk\_usp_inbox_participe.sql","w",encoding="utf-8") as f:
    f.write(full_sp)

print(f"\n== SP Generado ==")
print(f"  tablas legacy cubiertas: {len(covered_tables)}")
for t in covered_tables: print(f"    {t}")
print(f"  tablas legacy saltadas (requieren mapeo manual): {len(skipped_tables)}")
for t, reason in skipped_tables[:20]: print(f"    {t}  ({reason})")

# ============================================================
# PASO 7: desplegar SP
# ============================================================
print("\n== [7] Desplegando USP_INBOX_PARTICIPE ==")
try:
    co.execute(full_sp)
    print("  ok")
except Exception as e:
    print(f"  err: {str(e)[:500]}")
    # guardar error para inspeccion
    with open(r"C:\Users\Usuario\Downloads\Bulk\_sp_error.txt","w") as f: f.write(str(e))

# ============================================================
# PASO 8: poblar CDC_INBOX_MODULE_CONFIG
# ============================================================
print("\n== [8] Pobando CDC_INBOX_MODULE_CONFIG ==")
co.execute("DELETE FROM CDC_INBOX_MODULE_CONFIG")
for legacy_tbl in covered_tables:
    co.execute("""INSERT INTO CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE)
                  VALUES (:1, 'USP_INBOX_PARTICIPE', 1)""", [legacy_tbl])
orcl.commit()
co.execute("SELECT COUNT(*) FROM CDC_INBOX_MODULE_CONFIG"); print(f"  filas: {co.fetchone()[0]}")

# ============================================================
# PASO 9: Limpieza de data de test
# ============================================================
print("\n== [9] Limpieza ==")
c_can = sql("fcme_canonicos").cursor()
c_can.execute("DELETE FROM dbo.cdc_outbox"); print(f"  canonicos.cdc_outbox borrados: {c_can.rowcount}")
c_can.execute("DELETE FROM dbo.cdc_inbox"); print(f"  canonicos.cdc_inbox borrados: {c_can.rowcount}")

# Oracle cleanup
co.execute("DELETE FROM CDC_INBOX"); print(f"  Oracle CDC_INBOX borrados: {co.rowcount}")
co.execute("DELETE FROM CDC_INBOX_ERRORS"); print(f"  Oracle CDC_INBOX_ERRORS borrados: {co.rowcount}")
# limpiar tablas target (solo las cubiertas)
for legacy_tbl in covered_tables:
    for at, ot in leg_to_ora[legacy_tbl]:
        if oracle_tables_info[ot]["style"] == "SNAKE":
            try:
                co.execute(f"DELETE FROM {ot}")
            except Exception as e:
                print(f"    fail {ot}: {str(e)[:80]}")
orcl.commit()
print("  tablas Oracle _TYPE (snake) limpiadas")
