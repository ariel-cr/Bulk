"""Deploy Flujo 1 Seguridad - 11 types.
Out-of-scope: correoServicio_type, saldoBancoType (sin origen legacy claro)
"""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

# Spec: agg, dest, sp, ltbl, lkey, payload_cols, dest_match
SPECS = [
    {'agg':'aplicacionFuncion_type','dest':'APLICACIONFUNCION_TYPE','sp':'USP_INBOX_APLICACIONFUNCION',
     'ltbl':'sgtbapli','lkey':['co_apli','ti_loca'],
     'pcols':['co_apli','ti_loca','no_apli','st_apli'],
     'dest_match':[('CODIGOAPLICACION','co_apli'),('TIPOLOCALIDAD','ti_loca'),('NOMBREAPLICACION','no_apli'),('ESTADOAPLICACION','st_apli')]},

    {'agg':'auditoriaFlujo_type','dest':'AUDITORIAFLUJO_TYPE','sp':'USP_INBOX_AUDITORIAFLUJO',
     'ltbl':'sgtbtran','lkey':['co_apli','co_func','nu_tran','ti_loca'],
     'pcols':['co_apli','co_func','nu_tran','ti_loca','no_tran'],
     'dest_match':[('CODIGOPROCESO','co_apli'),('CODIGOSUBPROCESO','co_func')]},

    {'agg':'cuentaNostroType','dest':'CUENTANOSTRO_TYPE','sp':'USP_INBOX_CUENTANOSTRO',
     'ltbl':'sgtbcnts','lkey':['co_cnts'],
     'pcols':['co_cnts','nu_iden','no_cnts','st_cnts','ds_mail'],
     'dest_match':[('CODIGO','co_cnts'),('NOMBRE','no_cnts'),('ESTADO','st_cnts')]},

    {'agg':'empresa_type','dest':'EMPRESA_TYPE','sp':'USP_INBOX_EMPRESA',
     'ltbl':'sgtbempr','lkey':['co_empr'],
     'pcols':['co_empr','no_empr','st_empr','nu_ruc','no_desc'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('NOMBREEMPRESA','no_empr'),('ESTADOEMPRESA','st_empr'),('NUMERORUC','nu_ruc')]},

    {'agg':'firmaSeguridad_type','dest':'FIRMASEGURIDAD_TYPE','sp':'USP_INBOX_FIRMASEGURIDAD',
     'ltbl':'sgtbfirm','lkey':['co_empr','co_usua','fe_firm','ho_firm'],
     'pcols':['co_empr','co_usua','no_maqu','no_usua_nt','fe_firm','ho_firm'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOUSUARIO','co_usua'),('NOMBREMAQUINAUSUARIO','no_maqu'),('USUARIOWINDOWS','no_usua_nt')]},

    {'agg':'fondoSeguridad_type','dest':'FONDOSEGURIDAD_TYPE','sp':'USP_INBOX_FONDOSEGURIDAD',
     'ltbl':'sgtbfond','lkey':['co_fond'],
     'pcols':['co_fond','no_fond','st_fond','in_part','co_rubr_prim'],
     'dest_match':[('CODIGOFONDO','co_fond'),('NOMBREFONDO','no_fond'),('ESTADOFONDO','st_fond'),('CODIGORUBROPRIMARIO','co_rubr_prim')]},

    {'agg':'localidad_type','dest':'LOCALIDAD_TYPE','sp':'USP_INBOX_LOCALIDAD',
     'ltbl':'sgtbloca','lkey':['co_empr','co_loca'],
     'pcols':['co_empr','co_loca','no_loca','co_prov','ti_loca','ci_repr','no_repr'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOLOCALIDAD','co_loca'),('CODIGOPROVINCIA','co_prov'),('CEDULAREPRESENTANTE','ci_repr')]},

    {'agg':'parametroSeguridad_type','dest':'PARAMETROSEGURIDAD_TYPE','sp':'USP_INBOX_PARAMETROSEGURIDAD',
     'ltbl':'sgtbpara','lkey':['co_empr','co_para'],
     'pcols':['co_empr','co_para','no_para','va_para','in_prov'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOPARAMETRO','co_para'),('NOMBREPARAMETRO','no_para'),('VALORPARAMETRO','va_para')]},

    {'agg':'passwordSeguridad_type','dest':'PASSWORDSEGURIDAD_TYPE','sp':'USP_INBOX_PASSWORDSEGURIDAD',
     'ltbl':'sgtbpass','lkey':['co_empr','co_usua','sc_pass'],
     'pcols':['co_empr','co_usua','sc_pass','ds_pass','fe_ingr','st_pass','ti_pass'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOUSUARIO','co_usua'),('CONTRASENIA','ds_pass'),('SECUENCIACAMBIOCONTRASENIA','sc_pass')]},

    {'agg':'usuarioSeguridad_type','dest':'USUARIOSEGURIDAD_TYPE','sp':'USP_INBOX_USUARIOSEGURIDAD',
     'ltbl':'sgtbusua','lkey':['co_empr','co_usua'],
     'pcols':['co_empr','co_usua','no_usua','fe_ingr','fe_expi','ds_pass','nu_cedu'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOUSUARIO','co_usua'),('CONTRASENIA','ds_pass')]},

    {'agg':'usuarioServicio_type','dest':'USUARIOSERVICIO_TYPE','sp':'USP_INBOX_USUARIOSERVICIO',
     'ltbl':'sgtbconf_serv_apli','lkey':['sc_serv'],
     'pcols':['sc_serv','co_serv_apli','no_serv_apli','sc_tipo','no_usua','ds_pass','st_regi'],
     'dest_match':[('CODIGOUSUARIO','no_usua'),('CONTRASENIA','ds_pass')]},
]

# Validacion naming Oracle <=30 chars
for s in SPECS:
    assert len(s['sp'])<=30, f"SP name too long: {s['sp']} ({len(s['sp'])})"
print(f'TYPES = {len(SPECS)}, all SP names <=30 chars OK')

# === 1) Triggers legacy en dbSG ===
print('\n[1] Triggers legacy dbSG')
c = sql('dbSG').cursor()

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
            N'dbSG.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_d}, N'{s['agg']}', N'DELETE',
            (SELECT {pcols_q} FROM deleted x WHERE {pkmatch_d} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM deleted d;
END
"""
    return trg, body

for s in SPECS:
    trg, body = make_trigger(s)
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
    ddl=WRAPPER_TPL.format(
        sp=s['sp'], dest=s['dest'], agg=s['agg'],
        decls=decls.rstrip(), extracts=extracts.strip(),
        ins_cols=ins_cols, ins_vals=ins_vals,
        upd_set=upd_set, upd_match=upd_match, del_match=del_match,
        pkonly_cols=pkonly_cols, pkonly_vals=pkonly_vals,
    )
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
