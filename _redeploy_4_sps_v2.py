"""SPs sin MERGE — INSERT directo con catch DUP_VAL_ON_INDEX -> UPDATE."""
import oracledb
o = oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
c = o.cursor()

SPs = {
"USP_INBOX_PERSONAFIRMAS": ("personaFirmas", r"""
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PERSONAFIRMAS(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_id  VARCHAR2(50);
    v_sec VARCHAR2(50);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_id  := JSON_VALUE(p_payload,'$.co_bene');
    v_sec := JSON_VALUE(p_payload,'$.sc_vivi');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PERSONAFIRMASTYPE
         WHERE IDENTIFICACION = v_id AND SECUENCIAPERSONAFIRMA = v_sec;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PERSONAFIRMASTYPE (IDENTIFICACION, SECUENCIAPERSONAFIRMA, ACTIVADO)
            VALUES (v_id, v_sec, 'S');
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PERSONAFIRMASTYPE SET ACTIVADO = 'S'
             WHERE IDENTIFICACION = v_id AND SECUENCIAPERSONAFIRMA = v_sec;
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper personaFirmas: ' || v_err);
END;"""),

"USP_INBOX_IMAGENES": ("imagenes", r"""
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_IMAGENES(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_id   VARCHAR2(100);
    v_arch VARCHAR2(200);
    v_err  VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_id   := JSON_VALUE(p_payload,'$.ci_cedu');
    v_arch := JSON_VALUE(p_payload,'$.no_arch');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.IMAGENESTYPE WHERE CODIGOIMAGEN = v_id;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.IMAGENESTYPE (CODIGOIMAGEN, NOMBREARCHIVO)
            VALUES (v_id, v_arch);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.IMAGENESTYPE SET NOMBREARCHIVO = v_arch
             WHERE CODIGOIMAGEN = v_id;
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper imagenes: ' || v_err);
END;"""),

"USP_INBOX_COMISIONPARTICIPE": ("comisionParticipe", r"""
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_COMISIONPARTICIPE(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_seq VARCHAR2(100);
    v_ced VARCHAR2(50);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_seq := JSON_VALUE(p_payload,'$.ti_cred')||'|'||JSON_VALUE(p_payload,'$.aa_cred')||'|'||JSON_VALUE(p_payload,'$.qs_cred');
    v_ced := JSON_VALUE(p_payload,'$.ci_ejec');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.COMISIONPARTICIPE_TYPE WHERE CODIGOSECUENCIACOMISION = v_seq;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.COMISIONPARTICIPE_TYPE (CODIGOSECUENCIACOMISION, CEDULAPROMOTOR)
            VALUES (v_seq, v_ced);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.COMISIONPARTICIPE_TYPE SET CEDULAPROMOTOR = v_ced
             WHERE CODIGOSECUENCIACOMISION = v_seq;
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper comisionParticipe: ' || v_err);
END;"""),

"USP_INBOX_JURIDICOINFOBASICA": ("juridicoInfoBasica", r"""
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_JURIDICOINFOBASICA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_id  VARCHAR2(50);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_id := JSON_VALUE(p_payload,'$.co_juri');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.JURIDICOINFORMACIONBASICATYPE WHERE IDENTIFICACION = v_id;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.JURIDICOINFORMACIONBASICATYPE (IDENTIFICACION, CODIGOTIPOIDENTIFICACION)
            VALUES (v_id, 'J');
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.JURIDICOINFORMACIONBASICATYPE SET CODIGOTIPOIDENTIFICACION = 'J'
             WHERE IDENTIFICACION = v_id;
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper juridicoInfoBasica: ' || v_err);
END;"""),
}

for name, (tag, ddl) in SPs.items():
    try:
        c.execute(ddl)
        c.execute(f"SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='{name}' AND object_type='PROCEDURE'")
        st = c.fetchone()
        print(f"  {name}: {st[0]}")
        if st[0] != 'VALID':
            c.execute(f"SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='{name}'")
            for e in c.fetchall(): print(f"    L{e[0]}:{e[1]} {e[2][:200]}")
    except Exception as e:
        print(f"  {name} FAIL: {str(e)[:200]}")
o.commit(); o.close()
