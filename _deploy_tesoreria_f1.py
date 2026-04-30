"""Deploy Flujo 1 Tesoreria - 7 types."""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

SPECS=[
    {'agg':'bancoTesoreria_type','dest':'BANCOTESORERIA_TYPE','sp':'USP_INBOX_BANCOTESORERIA',
     'ltbl':'tstbbnco','lkey':['co_empr','qs_cnta_bnco'],
     'pcols':['co_empr','qs_cnta_bnco','ci_bnco','nu_cta','no_cuenta'],
     'dest_match':[('CODIGOBNCO','ci_bnco'),('NUMEROCTA','nu_cta'),('CODIGOEMPRESA','co_empr')]},

    {'agg':'cheque_type','dest':'CHEQUE_TYPE','sp':'USP_INBOX_CHEQUE',
     'ltbl':'tstbochq','lkey':['co_empr','sc_pago'],
     'pcols':['co_empr','co_orig','sc_pago','ci_bnco','qs_cnta_bnco'],
     'dest_match':[('NUMEROCHEQ','sc_pago'),('CODIGOBNCO','ci_bnco')]},

    {'agg':'estadoRegistroTesoreria_type','dest':'ESTADOREGISTROTESORERIA_TYPE','sp':'USP_INBOX_ESTREGTESORERIA',
     'ltbl':'tstbesta_regi','lkey':['st_pago'],
     'pcols':['st_pago','ds_pago','in_pago'],
     'dest_match':[('ESTADOPAGO','st_pago'),('DESCRIPCIONPAGO','ds_pago'),('INDICADORPAGO','in_pago')]},

    {'agg':'facturaTesoreria_type','dest':'FACTURATESORERIA_TYPE','sp':'USP_INBOX_FACTURATESORERIA',
     'ltbl':'tstbfact_teso','lkey':['co_empr','sc_pago','nu_fact','qs_dbso'],
     'pcols':['co_empr','sc_pago','nu_fact','qs_dbso','sc_cmpb_regi'],
     'dest_match':[('NUMEROFACT','nu_fact'),('CODIGOEMPRESA','co_empr'),('SECUENCIAPAGO','sc_pago'),('SECUENCIADBSO','qs_dbso')]},

    {'agg':'ordenPago_type','dest':'ORDENPAGO_TYPE','sp':'USP_INBOX_ORDENPAGO',
     'ltbl':'tstborde','lkey':['co_empr','co_proc','sc_orde'],
     'pcols':['co_empr','co_proc','sc_orde','fe_gene','ho_gene'],
     'dest_match':[('SECUENCIAORDE','sc_orde'),('CODIGOEMPRESA','co_empr'),('CODIGOPROC','co_proc')]},

    {'agg':'reversaDesembolso_type','dest':'REVERSADESEMBOLSO_TYPE','sp':'USP_INBOX_REVERSADESEMBOLSO',
     'ltbl':'tstbreve_dsmb','lkey':['co_empr','sc_pago'],
     'pcols':['co_empr','sc_pago','co_proc','st_regi'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('SECUENCIAPAGO','sc_pago'),('CODIGOPROC','co_proc')]},

    {'agg':'transferenciaOrden_type','dest':'TRANSFERENCIAORDEN_TYPE','sp':'USP_INBOX_TRANSFERENCIAORDEN',
     'ltbl':'tstbtord','lkey':['co_tord'],
     'pcols':['co_tord','no_tord','st_tord'],
     'dest_match':[('CODIGOTORD','co_tord'),('NOMBRETORD','no_tord'),('ESTADOTORD','st_tord')]},
]

for s in SPECS: assert len(s['sp'])<=30, f"{s['sp']} > 30"
print(f'TYPES = {len(SPECS)}')

# === 1) Triggers legacy en dbTS ===
print('\n[1] Triggers legacy dbTS')
c=sql('dbTS').cursor()
def make_trg(s):
    trg=f"trg_outbox_{s['ltbl']}"
    if len(s['lkey'])>1:
        agg_id_i="CONCAT_WS('|',"+",".join(f"CONVERT(NVARCHAR(200), i.[{k}])" for k in s['lkey'])+")"
        agg_id_d=agg_id_i.replace("i.[","d.[")
    else:
        agg_id_i=f"CONVERT(NVARCHAR(200), i.[{s['lkey'][0]}])"
        agg_id_d=f"CONVERT(NVARCHAR(200), d.[{s['lkey'][0]}])"
    pkmatch_i=" AND ".join(f"x.[{k}]=i.[{k}]" for k in s['lkey'])
    pkmatch_d=" AND ".join(f"x.[{k}]=d.[{k}]" for k in s['lkey'])
    pcols_q=",".join(f"x.[{c}]" for c in s['pcols'])
    body=f"""
CREATE TRIGGER dbo.{trg}
ON dbo.[{s['ltbl']}]
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
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_i}, N'{s['agg']}', @op,
            (SELECT {pcols_q} FROM inserted x WHERE {pkmatch_i} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbTS.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_d}, N'{s['agg']}', N'DELETE',
            (SELECT {pcols_q} FROM deleted x WHERE {pkmatch_d} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbTS.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM deleted d;
END
"""
    return trg,body
for s in SPECS:
    trg,body=make_trg(s)
    try:
        c.execute(f"IF OBJECT_ID(N'dbo.{trg}', N'TR') IS NOT NULL DROP TRIGGER dbo.{trg}")
        c.execute(body)
        print(f"  CREATE {trg}")
    except Exception as e:
        print(f"  FAIL {trg}: {str(e)[:200]}")

# === 2) Wrappers Oracle ===
print('\n[2] Wrappers Oracle USP_INBOX_*')
o=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
oc=o.cursor()
WRAPPER_TPL="""CREATE OR REPLACE PROCEDURE FCME_USER.{sp}(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
{decls}
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    {extracts}
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.{dest} WHERE {del_match};
    ELSE
        BEGIN
            INSERT INTO FCME_USER.{dest} ({ins_cols}) VALUES ({ins_vals});
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.{dest} SET {upd_set} WHERE {upd_match};
        WHEN OTHERS THEN
            BEGIN
                INSERT INTO FCME_USER.{dest} ({pkonly_cols}) VALUES ({pkonly_vals});
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper {agg}: ' || v_err);
END;"""
for s in SPECS:
    decls=""; extracts=""; var_for={}
    for ocol, lcol in s['dest_match']:
        var=f"v_{lcol}"
        var_for[ocol]=var
        decls+=f"    {var} VARCHAR2(200);\n"
        extracts+=f"{var} := JSON_VALUE(p_payload, '$.{lcol}');\n    "
    ins_cols=",".join(o for o,_ in s['dest_match'])
    ins_vals=",".join(var_for[o] for o,_ in s['dest_match'])
    if len(s['dest_match'])>1:
        upd_set=", ".join(f"{o}={var_for[o]}" for o,_ in s['dest_match'][1:])
    else:
        upd_set=f"{s['dest_match'][0][0]}={var_for[s['dest_match'][0][0]]}"
    upd_match=" AND ".join(f"{o}={var_for[o]}" for o,_ in s['dest_match'][:1])
    del_match=upd_match
    pkonly_cols=s['dest_match'][0][0]
    pkonly_vals=var_for[pkonly_cols]
    ddl=WRAPPER_TPL.format(sp=s['sp'], dest=s['dest'], agg=s['agg'],
        decls=decls.rstrip(), extracts=extracts.strip(),
        ins_cols=ins_cols, ins_vals=ins_vals, upd_set=upd_set, upd_match=upd_match, del_match=del_match,
        pkonly_cols=pkonly_cols, pkonly_vals=pkonly_vals)
    try:
        oc.execute(ddl)
        oc.execute(f"SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='{s['sp']}' AND object_type='PROCEDURE'")
        st=oc.fetchone()
        flag=st[0] if st else '?'
        print(f"  CREATE {s['sp']:<32}  status={flag}")
        if flag!='VALID':
            oc.execute(f"SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='{s['sp']}'")
            for e in oc.fetchall(): print(f"    L{e[0]}:{e[1]} {e[2][:200]}")
    except Exception as e:
        print(f"  FAIL {s['sp']}: {str(e)[:200]}")
o.commit()

# === 3) module_config Oracle ===
print('\n[3] module_config Oracle')
for s in SPECS:
    oc.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE = :1", [s['agg']])
    if oc.fetchone()[0]==0:
        oc.execute("INSERT INTO FCME_USER.CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE) VALUES (:1, :2, 1)",
                   [s['agg'], s['sp']])
        print(f"  + {s['agg']:<32} -> {s['sp']}")
    else:
        print(f"  exists {s['agg']}")
o.commit()
o.close()
print('\n=== DEPLOY OK ===')
