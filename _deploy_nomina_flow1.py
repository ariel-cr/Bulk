"""Deploy Flujo 1 Nomina - 12 types.

Para cada type cablea:
  1) dbNO.dbo.trg_outbox_<tabla>             (anti-loop SESSION_CONTEXT)
  2) FCME_USER.USP_INBOX_<TYPE>              (wrapper dedicado, INSERT...EXCEPTION DUP_VAL_ON_INDEX)
  3) FCME_USER.CDC_INBOX_MODULE_CONFIG entry (active=1)

Lecciones aplicadas de Participe:
  - Oracle name max 30 chars (cuidado truncado)
  - SQLERRM via variable
  - Omit ID en INSERT (identity column)
  - SET CLIENT_INFO('is_replicating') anti-loop antes de DML
  - INSERT...EXCEPTION DUP_VAL_ON_INDEX UPDATE en vez de MERGE
  - Sin tocar USP_INBOX_PARTICIPES ni triggers preexistentes
"""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# Spec por type
# (agg, fcme_dest, sp_short, legacy_tbl, legacy_pk_cols, payload_cols, dest_pk_cols_oracle, mapping {oracle:legacy})
SPECS = [
    # 1. anticipoNominaType
    {'agg':'anticipoNominaType','dest':'ANTICIPONOMINA_TYPE','sp':'USP_INBOX_ANTICIPONOMINA',
     'ltbl':'notbcant','lpk':['co_empr','nu_anio','sc_anti','co_empl'],
     'pcols':['co_empr','nu_anio','sc_anti','co_empl','fe_rol_idst','mo_soli','mo_dese','in_autr'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('ANIO','nu_anio'),('SECUENCIAANTICIPO','sc_anti'),('CODIGOEMPLEADO','co_empl')]},
    # 2. anticipoPagoType
    {'agg':'anticipoPagoType','dest':'ANTICIPOPAGO_TYPE','sp':'USP_INBOX_ANTICIPOPAGO',
     'ltbl':'notbpant','lpk':['nu_anio','sc_anti'],
     'pcols':['nu_anio','sc_anti','co_tord','mo_dese','nu_cnta','no_bene'],
     'dest_match':[('NUMEROFACTURAREFERENCIA','nu_anio'),('NUMEROFACTURAANTICIPO','sc_anti'),('MONTOPAGO','mo_dese')]},
    # 3. catalogoNominaType
    {'agg':'catalogoNominaType','dest':'CATALOGONOMINA_TYPE','sp':'USP_INBOX_CATALOGONOMINA',
     'ltbl':'notbcnom','lpk':['co_empr','co_nomi'],
     'pcols':['co_empr','co_nomi','no_nomi','nu_peri_pago','ti_pago','st_nomi'],
     'dest_match':[('CODIGOMOTIVOAUDITORIA','co_nomi'),('DESCRIPCIONADICIONAL','no_nomi'),('ESTADOREGISTRO','st_nomi')]},
    # 4. configuracionNominaType
    {'agg':'configuracionNominaType','dest':'CONFIGURACIONNOMINA_TYPE','sp':'USP_INBOX_CONFIGURACIONNOMINA',
     'ltbl':'notbpara','lpk':['co_empr','co_frec_pago_rol'],
     'pcols':['co_empr','co_frec_pago_rol','qs_cnta_bnco','co_rubr_pres','co_rubr_sobg'],
     'dest_match':[('CODIGOINSTITUCION','co_frec_pago_rol'),('NOMBREINSTITUCION','co_rubr_pres')]},
    # 5. nominaCabeceraType
    {'agg':'nominaCabeceraType','dest':'NOMINACABECERA_TYPE','sp':'USP_INBOX_NOMINACABECERA',
     'ltbl':'notbcrol','lpk':['co_empr','nu_rol'],
     'pcols':['co_empr','nu_rol','co_nomi','nu_peri_pago','fe_gene','fe_ingr','fe_conf'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGONOMINA','co_nomi')]},
    # 6. obligacionRolType
    {'agg':'obligacionRolType','dest':'OBLIGACIONROL_TYPE','sp':'USP_INBOX_OBLIGACIONROL',
     'ltbl':'notbcgrl','lpk':['co_empr','co_empl','sc_cgrl','co_rubr'],
     'pcols':['co_empr','co_empl','sc_cgrl','co_rubr','mo_cgrl'],
     'dest_match':[('CODIGOROL','sc_cgrl'),('SECUENCIACREDITO','co_rubr')]},
    # 7. pagoNominaType
    {'agg':'pagoNominaType','dest':'PAGONOMINA_TYPE','sp':'USP_INBOX_PAGONOMINA',
     'ltbl':'notbpago_nomi','lpk':['co_empr','sc_rol','co_bene','rf_pago','mo_pago','sc_deta'],
     'pcols':['co_empr','sc_rol','fe_rol','nu_rol','co_orig','ci_bnco','mo_pago'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CEDULABENEFICIARIO','co_bene'),('CODIGOBANCO','ci_bnco')]},
    # 8. parametroNominaType
    {'agg':'parametroNominaType','dest':'PARAMETRONOMINA_TYPE','sp':'USP_INBOX_PARAMETRONOMINA',
     'ltbl':'notbpara_gene','lpk':['co_para'],
     'pcols':['co_para','co_tipo_para','ti_valo','ds_par1','ds_par2','st_regi'],
     'dest_match':[('CODIGOEMPRESA','co_para'),('CODIGOFRECUENCIAPAGOROL','co_tipo_para')]},
    # 9. patronalNominaType
    {'agg':'patronalNominaType','dest':'PATRONALNOMINA_TYPE','sp':'USP_INBOX_PATRONALNOMINA',
     'ltbl':'notbpatr','lpk':['co_empr','nu_patr'],
     'pcols':['co_empr','nu_patr','no_patr','ti_iden_patr','nu_iden_patr'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOPROVINCIA','nu_patr')]},
    # 10. rolPagoType
    {'agg':'rolPagoType','dest':'ROLPAGO_TYPE','sp':'USP_INBOX_ROLPAGO',
     'ltbl':'notbdrol','lpk':['co_empr','nu_rol','co_empl','co_rubr'],
     'pcols':['co_empr','nu_rol','co_empl','co_rubr','mo_rol_pago'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGONOMINA','nu_rol')]},
    # 11. rubroNominaType
    {'agg':'rubroNominaType','dest':'RUBRONOMINA_TYPE','sp':'USP_INBOX_RUBRONOMINA',
     'ltbl':'notbrubr','lpk':['co_empr','co_rubr'],
     'pcols':['co_empr','co_rubr','no_rubr_abre','no_rubr','fe_ingr','in_dbcr','ti_rubr'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGORUBRO','co_rubr')]},
    # 12. viaticoNominaType
    {'agg':'viaticoNominaType','dest':'VIATICONOMINA_TYPE','sp':'USP_INBOX_VIATICONOMINA',
     'ltbl':'notbcvia','lpk':['co_empr','co_empl','sc_viat'],
     'pcols':['co_empr','co_empl','sc_viat','st_viat','mo_viat','fe_ingr'],
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOEMPLEADO','co_empl')]},
]

# === 1) Triggers legacy en dbNO ===
print('[1] Triggers legacy dbNO')
c = sql('dbNO').cursor()
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
            {agg_id_i},
            N'{agg}',
            @op,
            (SELECT {pcols_q} FROM inserted x WHERE {pkmatch_i} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.{tbl}',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            {agg_id_d},
            N'{agg}',
            N'DELETE',
            (SELECT {pcols_q} FROM deleted x WHERE {pkmatch_d} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.{tbl}',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
"""
for s in SPECS:
    trg=f"trg_outbox_{s['ltbl']}"
    c.execute(f"IF OBJECT_ID(N'dbo.{trg}', N'TR') IS NOT NULL DROP TRIGGER dbo.{trg}")
    agg_id_i = "CONCAT_WS('|'," + ",".join([f"CONVERT(NVARCHAR(200), i.[{p}])" for p in s['lpk']]) + ")"
    agg_id_d = agg_id_i.replace("i.[","d.[")
    pkmatch_i = " AND ".join([f"x.[{p}]=i.[{p}]" for p in s['lpk']])
    pkmatch_d = " AND ".join([f"x.[{p}]=d.[{p}]" for p in s['lpk']])
    pcols_q = ",".join([f"x.[{p}]" for p in s['pcols']])
    ddl = TRIG_TPL.format(tbl=s['ltbl'], agg=s['agg'], agg_id_i=agg_id_i, agg_id_d=agg_id_d,
                          pkmatch_i=pkmatch_i, pkmatch_d=pkmatch_d, pcols_q=pcols_q)
    try:
        c.execute(ddl)
        print(f"  CREATE {trg} ON dbo.{s['ltbl']}")
    except Exception as e:
        print(f"  FAIL {trg}: {str(e)[:200]}")

# === 2) Wrappers Oracle USP_INBOX_<TYPE> ===
print('\n[2] Wrappers Oracle')
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
            -- try insert con menos cols
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
    var_for = {}
    for ocol, lcol in s['dest_match']:
        var = f"v_{lcol}"
        var_for[ocol] = var
        decls += f"    {var} VARCHAR2(100);\n"
        extracts += f"{var} := JSON_VALUE(p_payload, '$.{lcol}');\n    "
    if not s['dest_match']:
        # fallback
        var_for = {}
    ins_cols = ",".join(o for o,_ in s['dest_match'])
    ins_vals = ",".join(var_for[o] for o,_ in s['dest_match'])
    upd_set = ", ".join(f"{o}={var_for[o]}" for o,_ in s['dest_match'][1:]) or f"{s['dest_match'][0][0]}={var_for[s['dest_match'][0][0]]}"
    upd_match = " AND ".join(f"{o}={var_for[o]}" for o,_ in s['dest_match'][:1])
    del_match = upd_match
    pkonly_cols = s['dest_match'][0][0]
    pkonly_vals = var_for[pkonly_cols]

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
        st = oc.fetchone()
        print(f"  CREATE {s['sp']:<35}  status={st[0] if st else '?'}")
        if st and st[0] != 'VALID':
            oc.execute(f"SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='{s['sp']}'")
            for e in oc.fetchall(): print(f"    L{e[0]}:{e[1]} {e[2][:200]}")
    except Exception as e:
        print(f"  FAIL {s['sp']}: {str(e)[:200]}")
o.commit()

# === 3) module_config Oracle ===
print('\n[3] module_config Oracle')
for s in SPECS:
    oc.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE = :1", [s['agg']])
    if oc.fetchone()[0] == 0:
        oc.execute("INSERT INTO FCME_USER.CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE) VALUES (:1, :2, 1)",
                   [s['agg'], s['sp']])
        print(f"  + {s['agg']:<30} -> {s['sp']}")
    else:
        print(f"  exists {s['agg']}")
o.commit()
o.close()

print('\n=== DEPLOY OK ===')
