"""Cablea Flujo 1 para los types huérfanos sin tocar USP_INBOX_PARTICIPES.

Para cada type:
  1) Crea SP dedicado USP_INBOX_<TYPE> en Oracle (parsea JSON, INSERT minimal).
  2) Inserta entry en FCME_USER.CDC_INBOX_MODULE_CONFIG -> el SP propio.
  3) Crea trigger outbox legacy con anti-loop SESSION_CONTEXT.

Types implementados:
  - personaFirmasType            <- dbIM.imtbbene_firm
  - imagenesType                 <- dbFC.fctbpart_foto
  - comisionParticipe_type       <- dbCT.cttbcomi_cred
  - juridicoInformacionBasicaType<- dbFC.fctbjuri_inst

Out-of-scope (sin origen legacy):
  - naturalReferenciasComercialesType
  - prueba
"""
import pyodbc, oracledb, sys

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o = orcl.cursor()

# ---------------------------------------------------------------
# 1) SECUENCIAS para ID (si no existen)
# ---------------------------------------------------------------
print("[1] Sequences en Oracle")
for tbl in ['PERSONAFIRMASTYPE','IMAGENESTYPE','COMISIONPARTICIPE_TYPE','JURIDICOINFORMACIONBASICATYPE']:
    seq=f'SEQ_{tbl}_ID'
    o.execute(f"SELECT COUNT(*) FROM all_sequences WHERE sequence_owner='FCME_USER' AND sequence_name='{seq}'")
    if o.fetchone()[0]==0:
        o.execute(f"SELECT NVL(MAX(ID),0)+1 FROM FCME_USER.{tbl}")
        start=o.fetchone()[0]
        o.execute(f"CREATE SEQUENCE FCME_USER.{seq} START WITH {start} INCREMENT BY 1 NOCACHE")
        print(f"  CREATE {seq} (start {start})")
    else:
        print(f"  exists {seq}")

# ---------------------------------------------------------------
# 2) WRAPPER SPs en Oracle
# ---------------------------------------------------------------
print("\n[2] SPs de inbox en Oracle")

WRAPPERS = {
'USP_INBOX_PERSONAFIRMAS': """
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PERSONAFIRMAS(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_id    VARCHAR2(50);
    v_sec   VARCHAR2(50);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_id  := JSON_VALUE(p_payload,'$.co_bene');
    v_sec := JSON_VALUE(p_payload,'$.sc_vivi');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PERSONAFIRMASTYPE
         WHERE IDENTIFICACION = v_id AND SECUENCIAPERSONAFIRMA = v_sec;
    ELSE
        MERGE INTO FCME_USER.PERSONAFIRMASTYPE t
        USING (SELECT v_id AS k, v_sec AS s FROM dual) s
           ON (t.IDENTIFICACION = s.k AND t.SECUENCIAPERSONAFIRMA = s.s)
        WHEN MATCHED THEN UPDATE SET ACTIVADO = 'S'
        WHEN NOT MATCHED THEN INSERT (ID, IDENTIFICACION, SECUENCIAPERSONAFIRMA, ACTIVADO)
            VALUES (FCME_USER.SEQ_PERSONAFIRMASTYPE_ID.NEXTVAL, v_id, v_sec, 'S');
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id,p_aggregate_type,p_event_type,'wrapper personaFirmas: '||SUBSTR(SQLERRM,1,500));
END;
""",
'USP_INBOX_IMAGENES': """
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_IMAGENES(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_codigo VARCHAR2(100);
    v_arch   VARCHAR2(200);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_codigo := JSON_VALUE(p_payload,'$.ci_cedu');
    v_arch   := JSON_VALUE(p_payload,'$.no_arch');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.IMAGENESTYPE WHERE CODIGOIMAGEN = v_codigo;
    ELSE
        MERGE INTO FCME_USER.IMAGENESTYPE t
        USING (SELECT v_codigo AS k FROM dual) s
           ON (t.CODIGOIMAGEN = s.k)
        WHEN MATCHED THEN UPDATE SET NOMBREARCHIVO = v_arch
        WHEN NOT MATCHED THEN INSERT (ID, CODIGOIMAGEN, NOMBREARCHIVO)
            VALUES (FCME_USER.SEQ_IMAGENESTYPE_ID.NEXTVAL, v_codigo, v_arch);
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id,p_aggregate_type,p_event_type,'wrapper imagenes: '||SUBSTR(SQLERRM,1,500));
END;
""",
'USP_INBOX_COMISIONPARTICIPE': """
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_COMISIONPARTICIPE(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_seq VARCHAR2(100);
    v_ced VARCHAR2(50);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_seq := JSON_VALUE(p_payload,'$.ti_cred')||'|'||JSON_VALUE(p_payload,'$.aa_cred')||'|'||JSON_VALUE(p_payload,'$.qs_cred');
    v_ced := JSON_VALUE(p_payload,'$.ci_ejec');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.COMISIONPARTICIPE_TYPE WHERE CODIGOSECUENCIACOMISION = v_seq;
    ELSE
        MERGE INTO FCME_USER.COMISIONPARTICIPE_TYPE t
        USING (SELECT v_seq AS k FROM dual) s
           ON (t.CODIGOSECUENCIACOMISION = s.k)
        WHEN MATCHED THEN UPDATE SET CEDULAPROMOTOR = v_ced
        WHEN NOT MATCHED THEN INSERT (ID, CODIGOSECUENCIACOMISION, CEDULAPROMOTOR)
            VALUES (FCME_USER.SEQ_COMISIONPARTICIPE_TYPE_ID.NEXTVAL, v_seq, v_ced);
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id,p_aggregate_type,p_event_type,'wrapper comisionParticipe: '||SUBSTR(SQLERRM,1,500));
END;
""",
'USP_INBOX_JURIDICOINFOBASICA': """
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_JURIDICOINFOBASICA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_id  VARCHAR2(50);
    v_des VARCHAR2(200);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_id  := JSON_VALUE(p_payload,'$.co_juri');
    v_des := JSON_VALUE(p_payload,'$.ds_juri');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.JURIDICOINFORMACIONBASICATYPE WHERE IDENTIFICACION = v_id;
    ELSE
        MERGE INTO FCME_USER.JURIDICOINFORMACIONBASICATYPE t
        USING (SELECT v_id AS k FROM dual) s
           ON (t.IDENTIFICACION = s.k)
        WHEN MATCHED THEN UPDATE SET NOMBRELEGAL = v_des
        WHEN NOT MATCHED THEN INSERT (ID, IDENTIFICACION, NOMBRELEGAL)
            VALUES (FCME_USER.SEQ_JURIDICOINFORMACIONBASICATYPE_ID.NEXTVAL, v_id, v_des);
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id,p_aggregate_type,p_event_type,'wrapper juridicoInfoBasica: '||SUBSTR(SQLERRM,1,500));
END;
""",
}

for name, ddl in WRAPPERS.items():
    try:
        o.execute(ddl)
        print(f"  CREATE OR REPLACE {name}")
    except Exception as e:
        print(f"  FAIL {name}: {str(e)[:200]}")

# Verificar columnas reales para JURIDICOINFORMACIONBASICATYPE — NOMBRELEGAL podria no existir
o.execute("SELECT column_name FROM all_tab_columns WHERE owner='FCME_USER' AND table_name='JURIDICOINFORMACIONBASICATYPE' AND column_name LIKE 'NOMBRE%'")
nombre_cols=[r[0] for r in o.fetchall()]
print(f"  cols NOMBRE en juridico: {nombre_cols}")

orcl.commit()

# ---------------------------------------------------------------
# 3) module_config en Oracle
# ---------------------------------------------------------------
print("\n[3] module_config en Oracle (FCME_USER.CDC_INBOX_MODULE_CONFIG)")
ENTRIES = [
    ('personaFirmasType',             'USP_INBOX_PERSONAFIRMAS'),
    ('imagenesType',                  'USP_INBOX_IMAGENES'),
    ('comisionParticipe_type',        'USP_INBOX_COMISIONPARTICIPE'),
    ('juridicoInformacionBasicaType', 'USP_INBOX_JURIDICOINFOBASICA'),
]
# Ver cols
o.execute("SELECT column_name FROM all_tab_columns WHERE owner='FCME_USER' AND table_name='CDC_INBOX_MODULE_CONFIG' ORDER BY column_id")
cfg_cols=[r[0] for r in o.fetchall()]
print(f"  cols module_config: {cfg_cols}")

for at, sp in ENTRIES:
    o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE = :1", [at])
    if o.fetchone()[0] == 0:
        o.execute("""INSERT INTO FCME_USER.CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE)
                     VALUES (:1, :2, 1)""", [at, sp])
        print(f"  + {at:<35} -> {sp}")
    else:
        print(f"  exists {at}")
orcl.commit()

# ---------------------------------------------------------------
# 4) Triggers outbox legacy
# ---------------------------------------------------------------
print("\n[4] Triggers outbox legacy")

TRIGGERS = [
    # (db_legacy, source_table, aggregate_type, [pk_cols], [payload_cols])
    ('dbIM','imtbbene_firm','personaFirmasType',
     ['co_bene','sc_vivi'], ['co_prog','co_bene','sc_vivi','fe_firm','ds_obse']),
    ('dbFC','fctbpart_foto','imagenesType',
     ['co_empr','ci_cedu'], ['co_empr','ci_cedu','ds_ruta','no_arch','fe_ingr_foto']),
    ('dbCT','cttbcomi_cred','comisionParticipe_type',
     ['ti_cred','aa_cred','qs_cred','ci_ejec'], ['ti_cred','aa_cred','qs_cred','ci_ejec','st_comi']),
    ('dbFC','fctbjuri_inst','juridicoInformacionBasicaType',
     ['co_empr','co_juri'], ['co_empr','co_juri','ds_juri','st_regi']),
]

TRIG_TPL = """
CREATE TRIGGER dbo.trg_outbox_{tbl}
ON dbo.[{tbl}]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;
    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;
    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            {agg_id_expr},
            N'{agg_type}',
            @op,
            (SELECT {payload_cols_q} FROM inserted x WHERE {pk_match} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{db}.dbo.{tbl}',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            {agg_id_expr_d},
            N'{agg_type}',
            N'DELETE',
            (SELECT {payload_cols_q} FROM deleted x WHERE {pk_match_d} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{db}.dbo.{tbl}',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
"""

for db, tbl, agg, pks, payload_cols in TRIGGERS:
    c = sql(db).cursor()
    # Drop si existe
    c.execute(f"IF OBJECT_ID(N'dbo.trg_outbox_{tbl}', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_{tbl}")
    # Construir expresion aggregate_id (concat de PKs)
    agg_id_expr = "CONCAT_WS('|'," + ",".join([f"CONVERT(NVARCHAR(200), i.[{p}])" for p in pks]) + ")"
    agg_id_expr_d = agg_id_expr.replace("i.[","d.[")
    pk_match = " AND ".join([f"x.[{p}]=i.[{p}]" for p in pks])
    pk_match_d = " AND ".join([f"x.[{p}]=d.[{p}]" for p in pks])
    payload_cols_q = ",".join([f"x.[{c}]" for c in payload_cols])
    ddl = TRIG_TPL.format(
        tbl=tbl, db=db, agg_type=agg,
        agg_id_expr=agg_id_expr, agg_id_expr_d=agg_id_expr_d,
        pk_match=pk_match, pk_match_d=pk_match_d,
        payload_cols_q=payload_cols_q,
    )
    try:
        c.execute(ddl)
        print(f"  CREATE trg_outbox_{tbl} on {db}.dbo.{tbl} (-> {agg})")
    except Exception as e:
        print(f"  FAIL {db}.{tbl}: {str(e)[:200]}")

print("\n=== DEPLOY OK ===")
orcl.close()
