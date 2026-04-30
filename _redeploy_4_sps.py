"""Re-deploy de los 4 SPs con SQLERRM via variable local."""
import oracledb
o = oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
c = o.cursor()

WRAPPERS = [
    ("USP_INBOX_PERSONAFIRMAS", "personaFirmas",
     "PERSONAFIRMASTYPE", "SEQ_PERSONAFIRMASTYPE_ID",
     r"""
    v_id  := JSON_VALUE(p_payload,'$.co_bene');
    v_sec := JSON_VALUE(p_payload,'$.sc_vivi');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PERSONAFIRMASTYPE
         WHERE IDENTIFICACION = v_id AND SECUENCIAPERSONAFIRMA = v_sec;
    ELSE
        MERGE INTO FCME_USER.PERSONAFIRMASTYPE t
        USING (SELECT v_id AS k, v_sec AS s FROM dual) src
           ON (t.IDENTIFICACION = src.k AND t.SECUENCIAPERSONAFIRMA = src.s)
        WHEN MATCHED THEN UPDATE SET ACTIVADO = 'S'
        WHEN NOT MATCHED THEN INSERT (IDENTIFICACION, SECUENCIAPERSONAFIRMA, ACTIVADO)
            VALUES (v_id, v_sec, 'S');
    END IF;""",
     "    v_id    VARCHAR2(50);\n    v_sec   VARCHAR2(50);"),

    ("USP_INBOX_IMAGENES", "imagenes",
     "IMAGENESTYPE", "SEQ_IMAGENESTYPE_ID",
     r"""
    v_id  := JSON_VALUE(p_payload,'$.ci_cedu');
    v_arch:= JSON_VALUE(p_payload,'$.no_arch');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.IMAGENESTYPE WHERE CODIGOIMAGEN = v_id;
    ELSE
        MERGE INTO FCME_USER.IMAGENESTYPE t
        USING (SELECT v_id AS k FROM dual) src
           ON (t.CODIGOIMAGEN = src.k)
        WHEN MATCHED THEN UPDATE SET NOMBREARCHIVO = v_arch
        WHEN NOT MATCHED THEN INSERT (CODIGOIMAGEN, NOMBREARCHIVO)
            VALUES (v_id, v_arch);
    END IF;""",
     "    v_id   VARCHAR2(100);\n    v_arch VARCHAR2(200);"),

    ("USP_INBOX_COMISIONPARTICIPE", "comisionParticipe",
     "COMISIONPARTICIPE_TYPE", "SEQ_COMISIONPARTICIPE_TYPE_ID",
     r"""
    v_seq := JSON_VALUE(p_payload,'$.ti_cred')||'|'||JSON_VALUE(p_payload,'$.aa_cred')||'|'||JSON_VALUE(p_payload,'$.qs_cred');
    v_ced := JSON_VALUE(p_payload,'$.ci_ejec');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.COMISIONPARTICIPE_TYPE WHERE CODIGOSECUENCIACOMISION = v_seq;
    ELSE
        MERGE INTO FCME_USER.COMISIONPARTICIPE_TYPE t
        USING (SELECT v_seq AS k FROM dual) src
           ON (t.CODIGOSECUENCIACOMISION = src.k)
        WHEN MATCHED THEN UPDATE SET CEDULAPROMOTOR = v_ced
        WHEN NOT MATCHED THEN INSERT (CODIGOSECUENCIACOMISION, CEDULAPROMOTOR)
            VALUES (v_seq, v_ced);
    END IF;""",
     "    v_seq VARCHAR2(100);\n    v_ced VARCHAR2(50);"),

    ("USP_INBOX_JURIDICOINFOBASICA", "juridicoInfoBasica",
     "JURIDICOINFORMACIONBASICATYPE", "SEQ_JURIDICOINFORMACIONBASICATYPE_ID",
     r"""
    v_id := JSON_VALUE(p_payload,'$.co_juri');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.JURIDICOINFORMACIONBASICATYPE WHERE IDENTIFICACION = v_id;
    ELSE
        MERGE INTO FCME_USER.JURIDICOINFORMACIONBASICATYPE t
        USING (SELECT v_id AS k FROM dual) src
           ON (t.IDENTIFICACION = src.k)
        WHEN MATCHED THEN UPDATE SET CODIGOTIPOIDENTIFICACION = 'J'
        WHEN NOT MATCHED THEN INSERT (IDENTIFICACION, CODIGOTIPOIDENTIFICACION)
            VALUES (v_id, 'J');
    END IF;""",
     "    v_id VARCHAR2(50);"),
]

TPL = """CREATE OR REPLACE PROCEDURE FCME_USER.{name}(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
{decls}
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
{body}
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper {tag}: ' || v_err);
END;"""

for name, tag, _, _, body, decls in WRAPPERS:
    ddl = TPL.format(name=name, tag=tag, decls=decls, body=body)
    try:
        c.execute(ddl)
        # check status
        c.execute(f"SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='{name}' AND object_type='PROCEDURE'")
        st = c.fetchone()
        print(f"  {name}: status={st[0] if st else '?'}")
        if st and st[0] != 'VALID':
            c.execute(f"SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='{name}' ORDER BY sequence")
            for e in c.fetchall(): print(f"    L{e[0]}:{e[1]} {e[2][:200]}")
    except Exception as e:
        print(f"  {name} FAIL: {str(e)[:200]}")

o.commit()
o.close()
