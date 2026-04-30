"""Regenera SP USP_INBOX_PARTICIPES con firma:
   (p_id, p_aggregate_type, p_source_table, p_event_type, p_payload)
y dispatch por (source_table + aggregate_type) -> tabla Oracle.

Tambien actualiza TRG_PROCESS_CDC_INBOX para pasar source_table.
"""
import pyodbc, oracledb
from collections import defaultdict

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

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

# 1) Mapeo source_table -> [(aggregate_type, oracle_table)]
c = sql("fcme_canonicos").cursor()
c.execute("""
  SELECT source_table, aggregate_type_emit
  FROM dbo.cdc_table_to_types
  WHERE is_active=1 AND aggregate_type_emit IS NOT NULL
""")
src_to_targets = defaultdict(list)
for r in c.fetchall():
    ot = AT_TO_TABLE.get(r.aggregate_type_emit)
    if ot:
        src_to_targets[r.source_table].append((r.aggregate_type_emit, ot))

# 2) Localizar BD legacy de cada tabla y obtener cols Oracle
LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
table_to_db = {}
for db in LEG_DBS:
    cc = sql(db).cursor()
    cc.execute("""SELECT t.name FROM sys.tables t
                  JOIN sys.schemas s ON t.schema_id=s.schema_id
                  WHERE s.name='dbo'""")
    for r in cc.fetchall():
        if r.name in src_to_targets:
            table_to_db.setdefault(r.name, db)

def get_legacy_cols(db, tbl):
    c = sql(db).cursor()
    c.execute("""SELECT name FROM sys.columns WHERE object_id=OBJECT_ID(?) ORDER BY column_id""",
              f"dbo.{tbl}")
    return [r.name for r in c.fetchall()]

def get_oracle_cols(tbl):
    co.execute("""SELECT column_name FROM all_tab_columns
                  WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id""", t=tbl)
    return [r[0] for r in co.fetchall()]

def is_snake(cols):
    return any("_" in c and c != "ID" for c in cols)

# 3) Generar SP
sp_lines = ["""CREATE OR REPLACE PROCEDURE USP_INBOX_PARTICIPES(
    p_id             IN NUMBER,
    p_aggregate_type IN VARCHAR2,
    p_source_table   IN VARCHAR2,
    p_event_type     IN VARCHAR2,
    p_payload        IN CLOB
) AS
    v_err VARCHAR2(4000);
    v_pk  VARCHAR2(200);
BEGIN
"""]

covered = []
skipped = []
for src_tbl, targets in sorted(src_to_targets.items()):
    db = table_to_db.get(src_tbl)
    if not db:
        skipped.append((src_tbl, "no localizado"))
        continue
    legacy_cols = get_legacy_cols(db, src_tbl)

    snake_targets = []
    for at, ot in targets:
        ocols = get_oracle_cols(ot)
        if not ocols: continue
        if is_snake(ocols):
            snake_targets.append((at, ot, ocols))
    if not snake_targets:
        skipped.append((src_tbl, "todos targets son CAMEL"))
        continue

    for at, ot, ocols in snake_targets:
        # construir mapeo cols
        mapped = []
        pk_col = "ID"
        for ol in ocols:
            if ol == "ID": continue
            found = None
            for lc in legacy_cols:
                if legacy_to_snake_upper(lc) == ol:
                    found = lc; break
            mapped.append((ol, found))

        # buscar PK candidato (primer CODIGO_*) para MERGE
        pk_col = None
        pk_legacy = None
        for ol, lc in mapped:
            if ol.startswith("CODIGO_") and lc:
                pk_col = ol
                pk_legacy = lc
                break
        if not pk_col:
            for ol, lc in mapped:
                if lc:
                    pk_col = ol
                    pk_legacy = lc
                    break

        cols_csv = ", ".join(ol for ol,_ in mapped)
        vals_csv = ", ".join(
            f"JSON_VALUE(p_payload,'$.{lc}')" if lc else "NULL"
            for _, lc in mapped
        )
        update_set = ", ".join(
            f"{ol} = " + (f"JSON_VALUE(p_payload,'$.{lc}')" if lc else "NULL")
            for ol, lc in mapped if ol != pk_col
        )

        sp_lines.append(f"""
    IF p_source_table = '{src_tbl}' AND p_aggregate_type = '{at}' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload,'$.{pk_legacy}');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM {ot} WHERE {pk_col} = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO {ot} t
                USING (SELECT v_pk AS k FROM dual) s
                ON (t.{pk_col} = s.k)
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({cols_csv}) VALUES ({vals_csv});
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    '{ot}: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;""")
    covered.append(src_tbl)

sp_lines.append("END;")
sp_full = "\n".join(sp_lines)

# Persistir para debug
with open(r"C:\Users\Usuario\Downloads\Bulk\_usp_inbox_full.sql","w",encoding="utf-8") as f:
    f.write(sp_full)

print(f"SP generado: {len(covered)} tablas cubiertas, {len(skipped)} skipped")
for t, why in skipped[:10]:
    print(f"  skipped {t}: {why}")

# 4) Deploy SP
print("\n[1] Deploying USP_INBOX_PARTICIPES")
try:
    co.execute(sp_full)
    co.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='USP_INBOX_PARTICIPES'")
    print(f"  status: {co.fetchone()[0]}")
except Exception as e:
    print(f"  err: {str(e)[:300]}")
    co.execute("SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPES' ORDER BY sequence FETCH FIRST 5 ROWS ONLY")
    for r in co.fetchall(): print(f"    line={r[0]} col={r[1]}: {r[2]}")
    raise SystemExit(1)

# 5) Recrear trigger compound (ahora pasa source_table)
print("\n[2] Recreando TRG_PROCESS_CDC_INBOX")
trg = """CREATE OR REPLACE TRIGGER TRG_PROCESS_CDC_INBOX
FOR INSERT ON CDC_INBOX
COMPOUND TRIGGER

    TYPE t_id_arr IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    TYPE t_str_arr IS TABLE OF VARCHAR2(200) INDEX BY PLS_INTEGER;
    TYPE t_clob_arr IS TABLE OF CLOB INDEX BY PLS_INTEGER;

    g_ids       t_id_arr;
    g_types     t_str_arr;
    g_sources   t_str_arr;
    g_events    t_str_arr;
    g_payloads  t_clob_arr;
    g_idx       PLS_INTEGER := 0;

    BEFORE STATEMENT IS
    BEGIN
        g_ids.DELETE; g_types.DELETE; g_sources.DELETE; g_events.DELETE; g_payloads.DELETE;
        g_idx := 0;
    END BEFORE STATEMENT;

    AFTER EACH ROW IS
    BEGIN
        g_idx := g_idx + 1;
        g_ids(g_idx) := :NEW.ID;
        g_types(g_idx) := :NEW.AGGREGATE_TYPE;
        g_sources(g_idx) := :NEW.SOURCE_TABLE;
        g_events(g_idx) := :NEW.EVENT_TYPE;
        g_payloads(g_idx) := :NEW.PAYLOAD;
    END AFTER EACH ROW;

    AFTER STATEMENT IS
        v_sp  VARCHAR2(300);
        v_err VARCHAR2(4000);
        v_src VARCHAR2(200);
    BEGIN
        FOR i IN 1 .. g_idx LOOP
            BEGIN
                v_sp := NULL;
                BEGIN
                    SELECT SP_NAME INTO v_sp FROM CDC_INBOX_MODULE_CONFIG
                    WHERE AGGREGATE_TYPE = g_types(i) AND ACTIVE = 1;
                EXCEPTION WHEN NO_DATA_FOUND THEN v_sp := NULL; END;

                IF v_sp IS NOT NULL THEN
                    -- Extrae solo el nombre de tabla (quita prefijo "dbXX.dbo.")
                    v_src := g_sources(i);
                    IF v_src LIKE '%.%' THEN
                        v_src := SUBSTR(v_src, INSTR(v_src,'.',-1)+1);
                    END IF;

                    EXECUTE IMMEDIATE 'BEGIN '||v_sp||'(:1, :2, :3, :4, :5); END;'
                        USING g_ids(i), g_types(i), v_src, g_events(i), g_payloads(i);

                    UPDATE CDC_INBOX SET PROCESSED=1, PROCESSED_AT=SYSTIMESTAMP
                    WHERE ID = g_ids(i);
                END IF;
            EXCEPTION WHEN OTHERS THEN
                v_err := SQLERRM;
                INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
                VALUES (g_ids(i), g_types(i), g_events(i), SUBSTR(v_err,1,4000));
            END;
        END LOOP;
    END AFTER STATEMENT;

END TRG_PROCESS_CDC_INBOX;"""
try:
    co.execute(trg)
    co.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='TRG_PROCESS_CDC_INBOX'")
    print(f"  status: {co.fetchone()[0]}")
except Exception as e:
    print(f"  err: {str(e)[:300]}")
    raise SystemExit(1)

# 6) Re-procesar pendientes
print("\n[3] Re-procesar pendientes")
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=0")
print(f"  pendientes antes: {co.fetchone()[0]}")
# El trigger es FOR INSERT, no UPDATE. Hay que reinsertar.
co.execute("""
DECLARE
    CURSOR cur IS
        SELECT ID, AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT
        FROM CDC_INBOX WHERE PROCESSED=0 ORDER BY ID;
BEGIN
    FOR r IN cur LOOP
        DELETE FROM CDC_INBOX WHERE ID = r.ID;
        INSERT INTO CDC_INBOX (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT, PROCESSED)
        VALUES (r.AGGREGATE_ID, r.AGGREGATE_TYPE, r.EVENT_TYPE, r.PAYLOAD, r.SOURCE_TABLE, r.CREATED_AT, 0);
    END LOOP;
    COMMIT;
END;
""")
import time; time.sleep(2)
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=0")
print(f"  pendientes despues: {co.fetchone()[0]}")
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
print(f"  errores: {co.fetchone()[0]}")

orcl.close()
