"""Paso D: Mapea los 4 pares criticos validos y desactiva los 8 invalidos.

VALIDOS (agregar al SP):
1. imtbmiem_cony      -> PERSONAVINCULACIONESTYPE  (vinculacion conyuge)
2. fctbafil_info_actu_docs -> NATURALINFORMACIONBASICATYPE  (parcial, solo cedula)
3. sfct_referencias   -> REFERENCIAPARTICIPE_TYPE  (catalogo a catalogo)
4. sfct_banco         -> PERSONATYPE  (banco como persona juridica via RUC)

INVALIDOS (desactivar en cdc_table_to_types):
- crtboper_cony, crtoblig -> personaVinculacionesType  (catalogos creditos sin cedula afiliado)
- fctbactv_suje_cred -> informacionAdicionalAfiliadoType  (catalogo)
- fctbesta_civi -> servicioAdicionalType  (catalogo estados civiles)
- fctbgene_sibs -> servicioAdicionalType  (catalogo generos)
- sfct_motivo_mant_afiliados -> auditoriaAfiliadoType  (catalogo motivos)
- sfct_banco -> personaReferenciasBancariasType  (no tiene cedula afiliado)
- svtbfmpg -> seguroVidaParticipeType  (catalogo formas pago)
"""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# 1) Desactivar los 8 invalidos en cdc_table_to_types
print("[1] Desactivando pares invalidos en cdc_table_to_types")
INVALID = [
    ("crtboper_cony","personaVinculacionesType"),
    ("crtoblig","personaVinculacionesType"),
    ("fctbactv_suje_cred","informacionAdicionalAfiliadoType"),
    ("fctbesta_civi","servicioAdicionalType"),
    ("fctbgene_sibs","servicioAdicionalType"),
    ("sfct_motivo_mant_afiliados","auditoriaAfiliadoType"),
    ("sfct_banco","personaReferenciasBancariasType"),
    ("svtbfmpg","seguroVidaParticipeType"),
]
c = sql("fcme_canonicos").cursor()
for src, at in INVALID:
    c.execute("""UPDATE dbo.cdc_table_to_types SET is_active=0
                 WHERE source_table=? AND aggregate_type_emit=?""", src, at)
    print(f"  off: {src} -> {at} (rows={c.rowcount})")

# 2) Leer SP actual
print("\n[2] Leyendo SP actual y agregando 4 branches validos antes del END")
co.execute("""SELECT text FROM all_source
              WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPES' AND type='PROCEDURE'
              ORDER BY line""")
sp_lines = [r[0] for r in co.fetchall()]
sp_body = "".join(sp_lines)

# Quitar "END USP_INBOX_PARTICIPES;" final
end_marker = "END USP_INBOX_PARTICIPES;"
if end_marker in sp_body:
    sp_body = sp_body.split(end_marker)[0]
else:
    # fallback
    if sp_body.rstrip().endswith("END;"):
        sp_body = sp_body.rstrip()[:-4]

# Quitar el header "PROCEDURE" inicial para usar CREATE OR REPLACE
sp_body = "CREATE OR REPLACE " + sp_body if not sp_body.startswith("CREATE") else sp_body

# 3) Branch 1: imtbmiem_cony -> PERSONAVINCULACIONESTYPE
b1 = """
    -- imtbmiem_cony  ->  PERSONAVINCULACIONESTYPE  (vinculacion conyuge)
    IF p_aggregate_type = 'personaVinculacionesType' AND p_source_table = 'imtbmiem_cony' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.co_miem');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM PERSONAVINCULACIONESTYPE
                    WHERE IDENTIFICACION = v_pk AND CODIGOTIPOVINCULACION = 'CONYUGE';
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO PERSONAVINCULACIONESTYPE t
                USING (SELECT v_pk AS k, 'CONYUGE' AS tv, '1' AS sec FROM dual) s
                ON (t.IDENTIFICACION = s.k AND t.CODIGOTIPOVINCULACION = s.tv AND t.SECUENCIAPERSONAVINCULACION = s.sec)
                WHEN MATCHED THEN UPDATE SET
                    CODIGOTIPOIDENTIFICACION = NULL,
                    CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA = NULL,
                    IDENTIFICACIONPERSONAVINCULADA = JSON_VALUE(p_payload,'$.co_cony'),
                    FECHAVINCULACION = NULL,
                    FECHASEPARACION = NULL
                WHEN NOT MATCHED THEN INSERT
                    (CODIGOTIPOIDENTIFICACION, IDENTIFICACION, CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA,
                     IDENTIFICACIONPERSONAVINCULADA, CODIGOTIPOVINCULACION, SECUENCIAPERSONAVINCULACION,
                     FECHAVINCULACION, FECHASEPARACION)
                VALUES
                    (NULL, v_pk, NULL, JSON_VALUE(p_payload,'$.co_cony'), 'CONYUGE', '1', NULL, NULL);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type, 'PERSONAVINCULACIONESTYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;
"""

# 4) Branch 2: fctbafil_info_actu_docs -> NATURALINFORMACIONBASICATYPE  (parcial)
b2 = """
    -- fctbafil_info_actu_docs  ->  NATURALINFORMACIONBASICATYPE  (parcial: solo cedula + observaciones)
    IF p_aggregate_type = 'naturalInformacionBasicaType' AND p_source_table = 'fctbafil_info_actu_docs' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.co_cedu');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM NATURALINFORMACIONBASICATYPE WHERE IDENTIFICACION = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO NATURALINFORMACIONBASICATYPE t
                USING (SELECT v_pk AS k FROM dual) s
                ON (t.IDENTIFICACION = s.k)
                WHEN MATCHED THEN UPDATE SET
                    HOMONIMIA = JSON_VALUE(p_payload,'$.in_comi_serv')
                WHEN NOT MATCHED THEN INSERT
                    (IDENTIFICACION, HOMONIMIA)
                VALUES (v_pk, JSON_VALUE(p_payload,'$.in_comi_serv'));
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type, 'NATURALINFORMACIONBASICATYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;
"""

# 5) Branch 3: sfct_referencias -> REFERENCIAPARTICIPE_TYPE  (catalogo)
b3 = """
    -- sfct_referencias  ->  REFERENCIAPARTICIPE_TYPE  (catalogo tipos referencia)
    IF p_aggregate_type = 'referenciaParticipeType' AND p_source_table = 'sfct_referencias' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.co_tref');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO REFERENCIAPARTICIPE_TYPE t
                USING (SELECT v_pk AS k FROM dual) s
                ON (t.CODIGOTIPOREFERENCIA = s.k)
                WHEN MATCHED THEN UPDATE SET
                    DESCRIPCIONTIPOREFERENCIA = JSON_VALUE(p_payload,'$.ds_tref')
                WHEN NOT MATCHED THEN INSERT
                    (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA)
                VALUES (v_pk, JSON_VALUE(p_payload,'$.ds_tref'));
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type, 'REFERENCIAPARTICIPE_TYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;
"""

# 6) Branch 4: sfct_banco -> PERSONATYPE  (banco como persona juridica)
b4 = """
    -- sfct_banco  ->  PERSONATYPE  (banco como persona juridica via RUC)
    IF p_aggregate_type = 'personaType' AND p_source_table = 'sfct_banco' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.nu_ruc');
            IF v_pk IS NULL OR TRIM(v_pk) IS NULL THEN
                v_pk := JSON_VALUE(p_payload, '$.ci_banco');
            END IF;
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM PERSONATYPE WHERE IDENTIFICACION = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO PERSONATYPE t
                USING (SELECT v_pk AS k FROM dual) s
                ON (t.IDENTIFICACION = s.k)
                WHEN MATCHED THEN UPDATE SET
                    CODIGOTIPOIDENTIFICACION = 'R',
                    CODIGOTIPOPERSONA = 'JUR',
                    NOMBRELEGAL = JSON_VALUE(p_payload,'$.no_banco'),
                    PRIMERAPELLIDO = JSON_VALUE(p_payload,'$.no_banco'),
                    FECHAINGRESO = JSON_VALUE(p_payload,'$.fx_creacion'),
                    CODIGOESTATUSPERSONA = JSON_VALUE(p_payload,'$.ce_estado'),
                    OBSERVACIONES = JSON_VALUE(p_payload,'$.no_cont')
                WHEN NOT MATCHED THEN INSERT
                    (CODIGOTIPOIDENTIFICACION, IDENTIFICACION, CODIGOTIPOPERSONA, NOMBRELEGAL,
                     PRIMERAPELLIDO, FECHAINGRESO, CODIGOESTATUSPERSONA, OBSERVACIONES)
                VALUES
                    ('R', v_pk, 'JUR', JSON_VALUE(p_payload,'$.no_banco'),
                     JSON_VALUE(p_payload,'$.no_banco'),
                     JSON_VALUE(p_payload,'$.fx_creacion'),
                     JSON_VALUE(p_payload,'$.ce_estado'),
                     JSON_VALUE(p_payload,'$.no_cont'));
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type, 'PERSONATYPE(banco): ' || SUBSTR(v_err,1,3900));
        END;
    END IF;
"""

new_sp = sp_body + b1 + b2 + b3 + b4 + "\nEND USP_INBOX_PARTICIPES;\n"

print(f"  SP nuevo: {len(new_sp)} chars")
co.execute(new_sp)
co.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='USP_INBOX_PARTICIPES'")
status = co.fetchone()[0]
print(f"  status: {status}")
if status != "VALID":
    co.execute("""SELECT line, position, text FROM all_errors
                  WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPES' ORDER BY sequence
                  FETCH FIRST 10 ROWS ONLY""")
    for r in co.fetchall(): print(f"    line={r[0]} col={r[1]}: {r[2]}")
    raise SystemExit(1)

orcl.commit()
orcl.close()
print("\n=== DEPLOY OK ===")
