"""Deploy Flujo 1 Recaudaciones - 11 types.

Para cada type cablea:
  1) dbRC.dbo.trg_outbox_<tabla>             (anti-loop SESSION_CONTEXT)
  2) FCME_USER.USP_INBOX_<TYPE>              (wrapper, INSERT...EXCEPTION DUP_VAL_ON_INDEX)
  3) FCME_USER.CDC_INBOX_MODULE_CONFIG entry (active=1)

Aplica TODAS las lecciones de Participe + Nomina.
"""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

# Spec por type:
# (agg, fcme_dest, sp_short, legacy_tbl, legacy_keys[para aggregate_id], payload_cols, dest_match[(oracle,legacy)])
SPECS = [
    {'agg':'aplicacionRecaudacion_type','dest':'APLICACIONRECAUDACION_TYPE','sp':'USP_INBOX_APLICACIONRECAUD',
     'ltbl':'rctbapli_reca','lkey':['sc_apli_reca','ci_cedu','ti_reca','co_rol'],
     'pcols':['sc_apli_reca','ci_cedu','ti_reca','co_rol','co_inst','co_prov','mo_reca','mo_rubr'],
     'dest_match':[('SECUENCIAAPLIRECA','sc_apli_reca'),('CODIGOCEDU','ci_cedu'),('TIPORECA','ti_reca'),('CODIGOROL','co_rol')]},

    {'agg':'caucionRecaudacion_type','dest':'CAUCIONRECAUDACION_TYPE','sp':'USP_INBOX_CAUCIONRECAUD',
     'ltbl':'rctbcaut','lkey':['co_empr','ti_reca','ti_dsto'],
     'pcols':['co_empr','ti_reca','ti_dsto','co_cnta_auto_noid','co_cnta_auto_papl'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('TIPORECA','ti_reca'),('TIPODSTO','ti_dsto')]},

    {'agg':'devolucionRendicion_type','dest':'DEVOLUCIONRENDICION_TYPE','sp':'USP_INBOX_DEVOLUCIONRENDICION',
     'ltbl':'rctbdevo_rind','lkey':['sc_devo'],
     'pcols':['sc_devo','ci_cedu','ti_reca','ti_dsto','nu_cpbt','co_cnta','mo_devo','fe_depo'],
     'dest_match':[('SECUENCIADEVO','sc_devo'),('CODIGOCEDU','ci_cedu')]},

    {'agg':'estadoCuenta_type','dest':'ESTADOCUENTA_TYPE','sp':'USP_INBOX_ESTADOCUENTA',
     'ltbl':'rctbesta_cnta','lkey':['co_empr','sc_cpbt'],
     'pcols':['co_empr','sc_cpbt','co_cnta','nu_cpbt','fe_depo','mo_depo'],
     'dest_match':[('SECUENCIACPBT','sc_cpbt'),('CODIGOCNTA','co_cnta'),('NUMEROCPBT','nu_cpbt')]},

    {'agg':'estadoRecaudacion_type','dest':'ESTADORECAUDACION_TYPE','sp':'USP_INBOX_ESTADORECAUDACION',
     'ltbl':'rctbesta_reca','lkey':['st_reca'],
     'pcols':['st_reca','ds_estado','no_corto','ci_tipo','ti_esta'],
     'dest_match':[('ESTADORECA','st_reca'),('DESCRIPCIONESTADO','ds_estado'),('NOMBRECORTO','no_corto')]},

    {'agg':'recaudacion_type','dest':'RECAUDACION_TYPE','sp':'USP_INBOX_RECAUDACION',
     'ltbl':'rctbreca','lkey':['sc_reca'],
     'pcols':['ti_reca','st_reca','mo_reca','sc_reca','fe_ingr','fe_autr'],
     'dest_match':[('SECUENCIA_RECA','sc_reca'),('TIPO_RECA','ti_reca'),('ESTADO_RECA','st_reca'),('MONTO_RECA','mo_reca')]},

    {'agg':'rendicion_type','dest':'RENDICION_TYPE','sp':'USP_INBOX_RENDICION',
     'ltbl':'rctbrind','lkey':['co_empr','sc_reca'],
     'pcols':['co_empr','sc_reca','ci_cedula','co_rol','co_inst','co_prov','ti_reca','nu_cpbt'],
     'dest_match':[('CODIGO_CEDULA','ci_cedula'),('CODIGO_ROL','co_rol'),('CODIGO_INST','co_inst'),('CODIGO_PROV','co_prov')]},

    {'agg':'saldoRecaudacion_type','dest':'SALDORECAUDACION_TYPE','sp':'USP_INBOX_SALDORECAUDACION',
     'ltbl':'rctbcsal_reca','lkey':['co_empr','co_fond','fe_cort','sc_rol','co_rol','ti_reca'],
     'pcols':['co_empr','co_fond','fe_cort','sc_rol','co_rol','ti_reca','mo_gene'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOFONDO','co_fond'),('SECUENCIAROL','sc_rol'),('CODIGOROL','co_rol'),('TIPORECA','ti_reca')]},

    {'agg':'sciRecaudacion_type','dest':'SCIRECAUDACION_TYPE','sp':'USP_INBOX_SCIRECAUDACION',
     'ltbl':'rctbcsci_bce','lkey':['sc_scib'],
     'pcols':['sc_scib','ci_rol','fe_gene','ho_gene','nu_envi','nu_regi_tota','mo_gene_tota'],
     'dest_match':[('SECUENCIASCIB','sc_scib'),('CODIGOROL','ci_rol'),('NUMEROENVI','nu_envi')]},

    {'agg':'tipoDescuento_type','dest':'TIPODESCUENTO_TYPE','sp':'USP_INBOX_TIPODESCUENTO',
     'ltbl':'rctbtipo_desc','lkey':['ti_desc'],
     'pcols':['ti_desc','no_tipo_desc','ds_tipo_desc','st_regi'],
     'dest_match':[('TIPODESC','ti_desc'),('NOMBRETIPODESC','no_tipo_desc'),('DESCRIPCIONTIPODESC','ds_tipo_desc'),('ESTADOREGISTRO','st_regi')]},

    {'agg':'transaccionRecaudacion_type','dest':'TRANSACCIONRECAUDACION_TYPE','sp':'USP_INBOX_TRANSACCIONRECAUD',
     'ltbl':'rctbtrec','lkey':['ti_reca'],
     'pcols':['ti_reca','no_reca','co_fond','ti_proc'],
     'dest_match':[('TIPORECA','ti_reca'),('NOMBRERECA','no_reca'),('CODIGOFONDO','co_fond'),('TIPOPROC','ti_proc')]},
]

# Validacion naming Oracle <=30 chars
for s in SPECS:
    assert len(s['sp'])<=30, f"SP name too long: {s['sp']} ({len(s['sp'])})"

# === 1) Triggers legacy en dbRC ===
print('[1] Triggers legacy dbRC')
c = sql('dbRC').cursor()

def make_trigger(s):
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
    body = f"""
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
            N'dbRC.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_d}, N'{s['agg']}', N'DELETE',
            (SELECT {pcols_q} FROM deleted x WHERE {pkmatch_d} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM deleted d;
END
"""
    return trg, body

for s in SPECS:
    trg, body = make_trigger(s)
    try:
        c.execute(f"IF OBJECT_ID(N'dbo.{trg}', N'TR') IS NOT NULL DROP TRIGGER dbo.{trg}")
        c.execute(body)
        print(f"  CREATE {trg} ON dbo.{s['ltbl']}")
    except Exception as e:
        print(f"  FAIL {trg}: {str(e)[:200]}")

# === 2) Wrappers Oracle ===
print('\n[2] Wrappers Oracle USP_INBOX_*')
o = oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
oc = o.cursor()

WRAPPER_TPL = """CREATE OR REPLACE PROCEDURE FCME_USER.{sp}(
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
    decls=""
    extracts=""
    var_for={}
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
    ddl = WRAPPER_TPL.format(
        sp=s['sp'], dest=s['dest'], agg=s['agg'],
        decls=decls.rstrip(),
        extracts=extracts.strip(),
        ins_cols=ins_cols, ins_vals=ins_vals,
        upd_set=upd_set, upd_match=upd_match, del_match=del_match,
        pkonly_cols=pkonly_cols, pkonly_vals=pkonly_vals,
    )
    try:
        oc.execute(ddl)
        oc.execute(f"SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='{s['sp']}' AND object_type='PROCEDURE'")
        st=oc.fetchone()
        flag=st[0] if st else '?'
        print(f"  CREATE {s['sp']:<30}  status={flag}")
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
