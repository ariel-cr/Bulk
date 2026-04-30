"""Paso B: Agrega personaTelefonosType y naturalTrabajoType al SP USP_INBOX_PARTICIPES.
Ambos vienen de fctbafil_actu. Mapeo manual a tablas Oracle CAMEL_CASE.
"""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# ============ PASO A (existente, regenerado) ============
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

co.execute("""SELECT column_name FROM all_tab_columns
              WHERE owner='FCME_USER' AND table_name='ACTUALIZACION_AFILIADO_TYPE'
              ORDER BY column_id""")
ora_cols = [r[0] for r in co.fetchall()]
c = sql("dbFC").cursor()
c.execute("""SELECT name FROM sys.columns WHERE object_id=OBJECT_ID('dbo.fctbafil_actu') ORDER BY column_id""")
leg_cols = [r.name for r in c.fetchall()]
leg_to_ora = {lc: lg2or(lc) for lc in leg_cols}

mapped_aaf = []
pk_oracle_aaf = None
pk_legacy_aaf = None
for oc in ora_cols:
    if oc == "ID": continue
    found = next((lc for lc, ocname in leg_to_ora.items() if ocname == oc), None)
    mapped_aaf.append((oc, found))
    if oc.startswith("CODIGO_") and found and not pk_oracle_aaf:
        pk_oracle_aaf = oc
        pk_legacy_aaf = found

cols_csv_aaf = ", ".join(oc for oc,_ in mapped_aaf)
vals_csv_aaf = ", ".join(
    f"JSON_VALUE(p_payload,'$.{lc}')" if lc else "NULL"
    for _, lc in mapped_aaf
)
update_set_aaf = ", ".join(
    f"{oc} = " + (f"JSON_VALUE(p_payload,'$.{lc}')" if lc else "NULL")
    for oc, lc in mapped_aaf if oc != pk_oracle_aaf
)

# ============ PASO B - Mapeo manual ============
# personaTelefonosType <- fctbafil_actu (multiples telefonos: conv, celu, con1, con2)
# Estrategia: 1 fila por cada numero presente, SECUENCIATELEFONO distinto.
# PK compuesto: IDENTIFICACION + SECUENCIATELEFONO

# naturalTrabajoType <- fctbafil_actu (un trabajo)
# PK: IDENTIFICACION + SECUENCIATRABAJO='1'

# ============ Construir SP completo ============
sp_sql = f"""CREATE OR REPLACE PROCEDURE USP_INBOX_PARTICIPES(
    p_id             IN NUMBER,
    p_aggregate_type IN VARCHAR2,
    p_source_table   IN VARCHAR2,
    p_event_type     IN VARCHAR2,
    p_payload        IN CLOB
) AS
    v_err VARCHAR2(4000);
    v_pk  VARCHAR2(200);
    v_telf VARCHAR2(50);
    v_tipo VARCHAR2(10);
BEGIN
    -- ============================================================
    -- actualizacionAfiliadoType  <-  fctbafil_actu  ->  ACTUALIZACION_AFILIADO_TYPE
    -- ============================================================
    IF p_aggregate_type = 'actualizacionAfiliadoType' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.{pk_legacy_aaf}');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM ACTUALIZACION_AFILIADO_TYPE WHERE {pk_oracle_aaf} = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO ACTUALIZACION_AFILIADO_TYPE t
                USING (SELECT v_pk AS k FROM dual) s
                ON (t.{pk_oracle_aaf} = s.k)
                WHEN MATCHED THEN UPDATE SET {update_set_aaf}
                WHEN NOT MATCHED THEN INSERT ({cols_csv_aaf}) VALUES ({vals_csv_aaf});
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type, 'ACTUALIZACION_AFILIADO_TYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;

    -- ============================================================
    -- personaTelefonosType  <-  fctbafil_actu  ->  PERSONATELEFONOSTYPE
    -- 1 fila por cada telefono no nulo (CONV, CEL, CON1, CON2)
    -- PK = IDENTIFICACION + SECUENCIATELEFONO
    -- ============================================================
    IF p_aggregate_type = 'personaTelefonosType' AND p_source_table = 'fctbafil_actu' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.ci_cedu');
            v_tipo := JSON_VALUE(p_payload, '$.ci_tipo');

            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM PERSONATELEFONOSTYPE WHERE IDENTIFICACION = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                -- Convencional
                v_telf := JSON_VALUE(p_payload, '$.tx_telf_conv');
                IF v_telf IS NOT NULL AND TRIM(v_telf) IS NOT NULL THEN
                    MERGE INTO PERSONATELEFONOSTYPE t
                    USING (SELECT v_pk AS k, '1' AS sec FROM dual) s
                    ON (t.IDENTIFICACION = s.k AND t.SECUENCIATELEFONO = s.sec)
                    WHEN MATCHED THEN UPDATE SET
                        CODIGOTIPOIDENTIFICACION = v_tipo,
                        CODIGOTIPOTELEFONO = 'CONV',
                        NUMEROTELEFONO = v_telf,
                        FECHAINGRESO = JSON_VALUE(p_payload,'$.fe_ingr'),
                        EMPRESAOPERADORA = NULL
                    WHEN NOT MATCHED THEN INSERT
                        (CODIGOTIPOIDENTIFICACION, IDENTIFICACION, SECUENCIATELEFONO,
                         CODIGOTIPOTELEFONO, NUMEROTELEFONO, FECHAINGRESO, EMPRESAOPERADORA)
                    VALUES
                        (v_tipo, v_pk, '1', 'CONV', v_telf,
                         JSON_VALUE(p_payload,'$.fe_ingr'), NULL);
                END IF;

                -- Celular
                v_telf := JSON_VALUE(p_payload, '$.tx_telf_celu');
                IF v_telf IS NOT NULL AND TRIM(v_telf) IS NOT NULL THEN
                    MERGE INTO PERSONATELEFONOSTYPE t
                    USING (SELECT v_pk AS k, '2' AS sec FROM dual) s
                    ON (t.IDENTIFICACION = s.k AND t.SECUENCIATELEFONO = s.sec)
                    WHEN MATCHED THEN UPDATE SET
                        CODIGOTIPOIDENTIFICACION = v_tipo,
                        CODIGOTIPOTELEFONO = 'CEL',
                        NUMEROTELEFONO = v_telf,
                        FECHAINGRESO = JSON_VALUE(p_payload,'$.fe_ingr'),
                        EMPRESAOPERADORA = JSON_VALUE(p_payload,'$.ti_oper')
                    WHEN NOT MATCHED THEN INSERT
                        (CODIGOTIPOIDENTIFICACION, IDENTIFICACION, SECUENCIATELEFONO,
                         CODIGOTIPOTELEFONO, NUMEROTELEFONO, FECHAINGRESO, EMPRESAOPERADORA)
                    VALUES
                        (v_tipo, v_pk, '2', 'CEL', v_telf,
                         JSON_VALUE(p_payload,'$.fe_ingr'),
                         JSON_VALUE(p_payload,'$.ti_oper'));
                END IF;

                -- Contacto 1
                v_telf := JSON_VALUE(p_payload, '$.tx_telf_con1');
                IF v_telf IS NOT NULL AND TRIM(v_telf) IS NOT NULL THEN
                    MERGE INTO PERSONATELEFONOSTYPE t
                    USING (SELECT v_pk AS k, '3' AS sec FROM dual) s
                    ON (t.IDENTIFICACION = s.k AND t.SECUENCIATELEFONO = s.sec)
                    WHEN MATCHED THEN UPDATE SET
                        CODIGOTIPOIDENTIFICACION = v_tipo,
                        CODIGOTIPOTELEFONO = 'CON1',
                        NUMEROTELEFONO = v_telf,
                        FECHAINGRESO = JSON_VALUE(p_payload,'$.fe_ingr')
                    WHEN NOT MATCHED THEN INSERT
                        (CODIGOTIPOIDENTIFICACION, IDENTIFICACION, SECUENCIATELEFONO,
                         CODIGOTIPOTELEFONO, NUMEROTELEFONO, FECHAINGRESO)
                    VALUES (v_tipo, v_pk, '3', 'CON1', v_telf,
                            JSON_VALUE(p_payload,'$.fe_ingr'));
                END IF;

                -- Contacto 2
                v_telf := JSON_VALUE(p_payload, '$.tx_telf_con2');
                IF v_telf IS NOT NULL AND TRIM(v_telf) IS NOT NULL THEN
                    MERGE INTO PERSONATELEFONOSTYPE t
                    USING (SELECT v_pk AS k, '4' AS sec FROM dual) s
                    ON (t.IDENTIFICACION = s.k AND t.SECUENCIATELEFONO = s.sec)
                    WHEN MATCHED THEN UPDATE SET
                        CODIGOTIPOIDENTIFICACION = v_tipo,
                        CODIGOTIPOTELEFONO = 'CON2',
                        NUMEROTELEFONO = v_telf,
                        FECHAINGRESO = JSON_VALUE(p_payload,'$.fe_ingr')
                    WHEN NOT MATCHED THEN INSERT
                        (CODIGOTIPOIDENTIFICACION, IDENTIFICACION, SECUENCIATELEFONO,
                         CODIGOTIPOTELEFONO, NUMEROTELEFONO, FECHAINGRESO)
                    VALUES (v_tipo, v_pk, '4', 'CON2', v_telf,
                            JSON_VALUE(p_payload,'$.fe_ingr'));
                END IF;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type, 'PERSONATELEFONOSTYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;

    -- ============================================================
    -- naturalTrabajoType  <-  fctbafil_actu  ->  NATURALTRABAJOTYPE
    -- PK = IDENTIFICACION + SECUENCIATRABAJO='1'
    -- ============================================================
    IF p_aggregate_type = 'naturalTrabajoType' AND p_source_table = 'fctbafil_actu' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.ci_cedu');
            v_tipo := JSON_VALUE(p_payload, '$.ci_tipo');

            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM NATURALTRABAJOTYPE WHERE IDENTIFICACION = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO NATURALTRABAJOTYPE t
                USING (SELECT v_pk AS k, '1' AS sec FROM dual) s
                ON (t.IDENTIFICACION = s.k AND t.SECUENCIATRABAJO = s.sec)
                WHEN MATCHED THEN UPDATE SET
                    CODIGOTIPOIDENTIFICACION = v_tipo,
                    CODIGOCARGOPERSONA = JSON_VALUE(p_payload,'$.co_carg'),
                    CODIGOCODIGOCARGO = JSON_VALUE(p_payload,'$.co_carg'),
                    FECHAINGRESOTRABAJO = JSON_VALUE(p_payload,'$.fe_ingr'),
                    FECHASALIDA = NULL,
                    NOMBREEMPLEADOR = JSON_VALUE(p_payload,'$.no_inst'),
                    PROPIETARIO = NULL,
                    TIPOCONTRATO = JSON_VALUE(p_payload,'$.ti_cont'),
                    CARGOPUBLICO = NULL,
                    SUELDO = NULL,
                    CANTIDADEMPLEADOS = NULL,
                    CODIGOCOCUPACION = JSON_VALUE(p_payload,'$.co_inst'),
                    TIEMPOPARCIAL = JSON_VALUE(p_payload,'$.ti_jorn')
                WHEN NOT MATCHED THEN INSERT
                    (CODIGOTIPOIDENTIFICACION, IDENTIFICACION, SECUENCIATRABAJO,
                     CODIGOCARGOPERSONA, CODIGOCODIGOCARGO, FECHAINGRESOTRABAJO,
                     FECHASALIDA, NOMBREEMPLEADOR, PROPIETARIO, TIPOCONTRATO,
                     CARGOPUBLICO, SUELDO, CANTIDADEMPLEADOS, CODIGOCOCUPACION, TIEMPOPARCIAL)
                VALUES
                    (v_tipo, v_pk, '1',
                     JSON_VALUE(p_payload,'$.co_carg'),
                     JSON_VALUE(p_payload,'$.co_carg'),
                     JSON_VALUE(p_payload,'$.fe_ingr'),
                     NULL,
                     JSON_VALUE(p_payload,'$.no_inst'),
                     NULL,
                     JSON_VALUE(p_payload,'$.ti_cont'),
                     NULL, NULL, NULL,
                     JSON_VALUE(p_payload,'$.co_inst'),
                     JSON_VALUE(p_payload,'$.ti_jorn'));
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type, 'NATURALTRABAJOTYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;
END USP_INBOX_PARTICIPES;
"""

with open(r"C:\Users\Usuario\Downloads\Bulk\_usp_inbox_participes.sql","w",encoding="utf-8") as f:
    f.write(sp_sql)

print("[1] Deploy USP_INBOX_PARTICIPES (ahora con 3 types)")
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

print("\n[2] Activar nuevos types en CDC_INBOX_MODULE_CONFIG")
co.execute("UPDATE CDC_INBOX_MODULE_CONFIG SET ACTIVE=1 WHERE AGGREGATE_TYPE IN ('personaTelefonosType','naturalTrabajoType')")
orcl.commit()
co.execute("SELECT AGGREGATE_TYPE, ACTIVE FROM CDC_INBOX_MODULE_CONFIG WHERE ACTIVE=1 ORDER BY AGGREGATE_TYPE")
print("  Types activos:")
for r in co.fetchall():
    print(f"    {r[0]}")

print("\n=== DEPLOY OK ===")
orcl.close()
