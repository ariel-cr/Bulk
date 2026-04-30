"""Paso A: Despliega USP_INBOX_PARTICIPES (dispatch interno por aggregate_type)
+ TRG_PROCESS_CDC_INBOX (lee CDC_INBOX_MODULE_CONFIG y llama al SP).

Empieza con UN SOLO type funcionando: actualizacionAfiliadoType (SNAKE_CASE auto).
"""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# Mapeo legacy snake -> Oracle SNAKE_UPPER
PREFIX_MAP = {
    "ci_": "CODIGO_", "co_": "CODIGO_", "ds_": "DESCRIPCION_",
    "nu_": "NUMERO_", "tx_": "TEXTO_", "in_": "INDICADOR_",
    "fx_": "FECHA_", "fe_": "FECHA_",
    "ti_": "TIPO_", "no_": "NOMBRE_",
    "qs_": "SECUENCIA_", "sc_": "SECUENCIA_",
    "va_": "VALOR_", "mn_": "MONTO_", "es_": "ESTADO_",
    "st_": "ESTADO_",
}
def lg2or(col):
    for p, r in PREFIX_MAP.items():
        if col.lower().startswith(p):
            return r + col[len(p):].upper()
    return col.upper()

# 1) Cols Oracle de ACTUALIZACION_AFILIADO_TYPE
co.execute("""SELECT column_name FROM all_tab_columns
              WHERE owner='FCME_USER' AND table_name='ACTUALIZACION_AFILIADO_TYPE'
              ORDER BY column_id""")
ora_cols = [r[0] for r in co.fetchall()]

# 2) Cols legacy de fctbafil_actu
c = sql("dbFC").cursor()
c.execute("""SELECT name FROM sys.columns WHERE object_id=OBJECT_ID('dbo.fctbafil_actu') ORDER BY column_id""")
leg_cols = [r.name for r in c.fetchall()]
leg_to_ora = {lc: lg2or(lc) for lc in leg_cols}

# 3) Mapear cada Oracle col -> legacy col (si existe)
mapped = []
pk_oracle = None
pk_legacy = None
for oc in ora_cols:
    if oc == "ID":
        continue
    found = next((lc for lc, ocname in leg_to_ora.items() if ocname == oc), None)
    mapped.append((oc, found))
    if oc.startswith("CODIGO_") and found and not pk_oracle:
        pk_oracle = oc
        pk_legacy = found

print(f"Oracle cols (sin ID): {len(mapped)}")
print(f"Mapeadas: {sum(1 for _,lc in mapped if lc)}")
print(f"PK: oracle={pk_oracle}  legacy={pk_legacy}")

cols_csv = ", ".join(oc for oc,_ in mapped)
vals_csv = ", ".join(
    f"JSON_VALUE(p_payload,'$.{lc}')" if lc else "NULL"
    for _, lc in mapped
)
update_set = ", ".join(
    f"{oc} = " + (f"JSON_VALUE(p_payload,'$.{lc}')" if lc else "NULL")
    for oc, lc in mapped if oc != pk_oracle
)

sp_sql = f"""CREATE OR REPLACE PROCEDURE USP_INBOX_PARTICIPES(
    p_id             IN NUMBER,
    p_aggregate_type IN VARCHAR2,
    p_source_table   IN VARCHAR2,
    p_event_type     IN VARCHAR2,
    p_payload        IN CLOB
) AS
    v_err VARCHAR2(4000);
    v_pk  VARCHAR2(200);
BEGIN
    -- ============================================================
    -- actualizacionAfiliadoType  <-  fctbafil_actu
    -- target: ACTUALIZACION_AFILIADO_TYPE
    -- ============================================================
    IF p_aggregate_type = 'actualizacionAfiliadoType' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.{pk_legacy}');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM ACTUALIZACION_AFILIADO_TYPE WHERE {pk_oracle} = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO ACTUALIZACION_AFILIADO_TYPE t
                USING (SELECT v_pk AS k FROM dual) s
                ON (t.{pk_oracle} = s.k)
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({cols_csv}) VALUES ({vals_csv});
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'ACTUALIZACION_AFILIADO_TYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;
END USP_INBOX_PARTICIPES;
"""

with open(r"C:\Users\Usuario\Downloads\Bulk\_usp_inbox_participes.sql","w",encoding="utf-8") as f:
    f.write(sp_sql)

print("\n[1] Deploy USP_INBOX_PARTICIPES")
co.execute(sp_sql)
co.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='USP_INBOX_PARTICIPES'")
status = co.fetchone()[0]
print(f"  status: {status}")
if status != "VALID":
    co.execute("""SELECT line, position, text FROM all_errors
                  WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPES' ORDER BY sequence
                  FETCH FIRST 10 ROWS ONLY""")
    for r in co.fetchall(): print(f"    line={r[0]} col={r[1]}: {r[2]}")
    raise SystemExit(1)

# 4) TRG_PROCESS_CDC_INBOX
print("\n[2] Deploy TRG_PROCESS_CDC_INBOX")
trg_sql = """CREATE OR REPLACE TRIGGER TRG_PROCESS_CDC_INBOX
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
        v_src VARCHAR2(200);
        v_err VARCHAR2(4000);
    BEGIN
        FOR i IN 1 .. g_idx LOOP
            BEGIN
                v_sp := NULL;
                BEGIN
                    SELECT SP_NAME INTO v_sp FROM CDC_INBOX_MODULE_CONFIG
                    WHERE AGGREGATE_TYPE = g_types(i) AND ACTIVE = 1;
                EXCEPTION WHEN NO_DATA_FOUND THEN v_sp := NULL; END;

                IF v_sp IS NOT NULL THEN
                    v_src := g_sources(i);
                    IF v_src LIKE '%.%' THEN
                        v_src := SUBSTR(v_src, INSTR(v_src,'.',-1)+1);
                    END IF;

                    EXECUTE IMMEDIATE 'BEGIN '||v_sp||'(:1, :2, :3, :4, :5); END;'
                        USING g_ids(i), g_types(i), v_src, g_events(i), g_payloads(i);
                END IF;

                UPDATE CDC_INBOX SET PROCESSED=1, PROCESSED_AT=SYSTIMESTAMP
                WHERE ID = g_ids(i);
            EXCEPTION WHEN OTHERS THEN
                v_err := SQLERRM;
                INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
                VALUES (g_ids(i), g_types(i), g_events(i), SUBSTR(v_err,1,4000));
            END;
        END LOOP;
    END AFTER STATEMENT;

END TRG_PROCESS_CDC_INBOX;"""
co.execute(trg_sql)
co.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='TRG_PROCESS_CDC_INBOX'")
status = co.fetchone()[0]
print(f"  status: {status}")
if status != "VALID":
    co.execute("""SELECT line, position, text FROM all_errors
                  WHERE owner='FCME_USER' AND name='TRG_PROCESS_CDC_INBOX' ORDER BY sequence
                  FETCH FIRST 10 ROWS ONLY""")
    for r in co.fetchall(): print(f"    line={r[0]} col={r[1]}: {r[2]}")
    raise SystemExit(1)

# 5) Asegurar CDC_INBOX_MODULE_CONFIG solo tiene actualizacionAfiliadoType activo
print("\n[3] CDC_INBOX_MODULE_CONFIG (limitado a piloto)")
co.execute("UPDATE CDC_INBOX_MODULE_CONFIG SET ACTIVE=0")
co.execute("UPDATE CDC_INBOX_MODULE_CONFIG SET ACTIVE=1 WHERE AGGREGATE_TYPE='actualizacionAfiliadoType'")
orcl.commit()
co.execute("SELECT AGGREGATE_TYPE, SP_NAME, ACTIVE FROM CDC_INBOX_MODULE_CONFIG WHERE ACTIVE=1")
for r in co.fetchall():
    print(f"  ACTIVE: {r[0]} -> {r[1]}")

print("\n=== DEPLOY OK ===")
orcl.close()
