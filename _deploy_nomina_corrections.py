"""Correcciones Nomina:
  PASO 1: Drop anticipoPagoType + obligacionRolType (no estan en lista del usuario)
  PASO 2: Cablear los 11 types faltantes
  PASO 3: Para notbempl + notbcgfm (ya tienen trigger Participe), crear trigger ADICIONAL
"""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# ============================================================
# PASO 1: Remover los 2 que sobran
# ============================================================
print("[PASO 1] Drop anticipoPagoType + obligacionRolType")
print("-" * 60)

c_no = sql('dbNO').cursor()
o = oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
oc = o.cursor()

REMOVE = [
    ('anticipoPagoType', 'notbpant', 'USP_INBOX_ANTICIPOPAGO'),
    ('obligacionRolType', 'notbcgrl', 'USP_INBOX_OBLIGACIONROL'),
]
for agg, ltbl, sp in REMOVE:
    # 1.1 Drop trigger legacy
    c_no.execute(f"IF OBJECT_ID(N'dbo.trg_outbox_{ltbl}', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_{ltbl}")
    print(f"  DROP dbNO.dbo.trg_outbox_{ltbl}")
    # 1.2 Drop entry module_config
    oc.execute("DELETE FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE = :1", [agg])
    print(f"  DELETE module_config[{agg}]")
    # 1.3 Drop SP wrapper Oracle
    try:
        oc.execute(f"DROP PROCEDURE FCME_USER.{sp}")
        print(f"  DROP PROCEDURE {sp}")
    except Exception as e:
        print(f"  DROP {sp} skip: {str(e)[:100]}")
o.commit()

# ============================================================
# PASO 2 + 3: Cablear 11 types faltantes
# ============================================================
print("\n[PASO 2+3] Cablear 11 types faltantes")
print("-" * 60)

# (agg, dest_table, sp_name, legacy_table, pk_cols, payload_cols, trigger_suffix, dest_match[(oracle_col, legacy_col)])
NEW_SPECS = [
    {'agg':'cargaFamiliarType','dest':'CARGAFAMILIAR_TYPE','sp':'USP_INBOX_CARGAFAMILIAR',
     'ltbl':'notbcgfm','lpk':['co_empr','co_empl','sc_cgfm'],
     'pcols':['co_empr','co_empl','sc_cgfm','ti_rela','no_nomb'],
     'trg_suffix':'_carga',  # adicional, ya tiene trg_outbox_notbcgfm de Participe
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOEMPLEADO','co_empl')]},

    {'agg':'cargoGeneralType','dest':'CARGOGENERAL_TYPE','sp':'USP_INBOX_CARGOGENERAL',
     'ltbl':'notbcarg','lpk':['co_empr','co_carg'],
     'pcols':['co_empr','co_carg','no_carg','co_carg_iess','mo_suel'],
     'trg_suffix':'',
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGONOMINA','co_carg')]},

    {'agg':'cargoLaboralType','dest':'CARGOLABORAL_TYPE','sp':'USP_INBOX_CARGOLABORAL',
     'ltbl':'notbcarg_admi','lpk':['co_empr','co_carg_admi'],
     'pcols':['co_empr','co_carg_admi','ds_carg_admi','st_regi'],
     'trg_suffix':'',
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOCARGADMINISTRADOR','co_carg_admi')]},

    {'agg':'empleadoAuditoriaType','dest':'EMPLEADOAUDITORIA_TYPE','sp':'USP_INBOX_EMPLEADOAUDITORIA',
     'ltbl':'notbempl_audi','lpk':['co_empr','co_empl','fe_ingr','ho_ingr'],
     'pcols':['co_empr','co_empl','fe_ingr','ho_ingr','ti_cont'],
     'trg_suffix':'',
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOEMPLEADO','co_empl')]},

    {'agg':'empleadoDetalleType','dest':'EMPLEADODETALLE_TYPE','sp':'USP_INBOX_EMPLEADODETALLE',
     'ltbl':'notbempl_deta','lpk':['co_empr','co_empl'],
     'pcols':['co_empr','co_empl','ti_cont','in_prue','ti_peri_cont'],
     'trg_suffix':'',
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOEMPL','co_empl')]},

    {'agg':'empleadoType','dest':'EMPLEADO_TYPE','sp':'USP_INBOX_EMPLEADO',
     'ltbl':'notbempl','lpk':['co_empr','co_empl'],
     'pcols':['co_empr','co_empl','no_empl','no_dire','co_carg'],
     'trg_suffix':'_empleado',  # adicional, ya tiene trg_outbox_notbempl de Participe
     'dest_match':[('CODIGOCARGO','co_carg')]},

    {'agg':'firmaHorarioType','dest':'FIRMAHORARIO_TYPE','sp':'USP_INBOX_FIRMAHORARIO',
     'ltbl':'notbfirm','lpk':['co_empr','co_empl','sc_firm'],
     'pcols':['co_empr','co_empl','sc_firm','ti_regi','fe_firm'],
     'trg_suffix':'',
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOEMPLEADO','co_empl')]},

    {'agg':'fondoReservaType','dest':'FONDORESERVA_TYPE','sp':'USP_INBOX_FONDORESERVA',
     'ltbl':'notbfond_rese','lpk':['co_empr','co_empl'],
     'pcols':['co_empr','co_empl','ti_acre','co_usua_ingr','fe_ingr'],
     'trg_suffix':'',
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOEMPLEADO','co_empl'),('TIPOACREDITACIONFONDORESERVA','ti_acre')]},

    {'agg':'historialIngresoType','dest':'HISTORIALINGRESO_TYPE','sp':'USP_INBOX_HISTORIALINGRESO',
     'ltbl':'notbhieg','lpk':['co_empr','co_empl','nu_ano','nu_mes'],
     'pcols':['co_empr','co_empl','nu_ano','nu_mes','mo_suel'],
     'trg_suffix':'',
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOEMPLEADO','co_empl'),('ANIO','nu_ano'),('MES','nu_mes')]},

    {'agg':'nivelAcademicoType','dest':'NIVELACADEMICO_TYPE','sp':'USP_INBOX_NIVELACADEMICO',
     'ltbl':'notbnive_acad_empl','lpk':['co_empr','co_empl'],
     'pcols':['co_empr','co_empl','co_inst','co_titu','in_egre'],
     'trg_suffix':'',
     'dest_match':[('CODIGOEMPRESA','co_empr'),('CODIGOEMPLEADO','co_empl'),('CODIGOINSTITUCION','co_inst'),('CODIGOTITULO','co_titu')]},

    {'agg':'sectorIessType','dest':'SECTORIESS_TYPE','sp':'USP_INBOX_SECTORIESS',
     'ltbl':'notbsect_iess','lpk':['co_sect'],
     'pcols':['co_sect','co_sect_iess','ds_sect_iess','co_tact_sect','co_estr_ocup'],
     'trg_suffix':'',
     'dest_match':[('CODIGOSECTOR','co_sect'),('CODIGOGESTIONIESS','co_sect_iess'),('DESCRIPCIONSECTORIESS','ds_sect_iess')]},
]

# Trigger template (handle single-PK case)
def make_trigger_ddl(s):
    trg = f"trg_outbox_{s['ltbl']}{s['trg_suffix']}"
    if len(s['lpk']) > 1:
        agg_id_i = "CONCAT_WS('|'," + ",".join([f"CONVERT(NVARCHAR(200), i.[{p}])" for p in s['lpk']]) + ")"
        agg_id_d = agg_id_i.replace("i.[","d.[")
    else:
        agg_id_i = f"CONVERT(NVARCHAR(200), i.[{s['lpk'][0]}])"
        agg_id_d = f"CONVERT(NVARCHAR(200), d.[{s['lpk'][0]}])"
    pkmatch_i = " AND ".join([f"x.[{p}]=i.[{p}]" for p in s['lpk']])
    pkmatch_d = " AND ".join([f"x.[{p}]=d.[{p}]" for p in s['lpk']])
    pcols_q = ",".join([f"x.[{p}]" for p in s['pcols']])
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
            N'dbNO.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_d}, N'{s['agg']}', N'DELETE',
            (SELECT {pcols_q} FROM deleted x WHERE {pkmatch_d} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM deleted d;
END
"""
    return trg, body

# 2.1 Crear triggers legacy
print("\n[2.1] Triggers legacy")
for s in NEW_SPECS:
    trg, body = make_trigger_ddl(s)
    try:
        c_no.execute(f"IF OBJECT_ID(N'dbo.{trg}', N'TR') IS NOT NULL DROP TRIGGER dbo.{trg}")
        c_no.execute(body)
        print(f"  CREATE {trg} ON dbo.{s['ltbl']}")
    except Exception as e:
        print(f"  FAIL {trg}: {str(e)[:200]}")

# 2.2 Wrappers Oracle
print("\n[2.2] Wrappers Oracle")
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

for s in NEW_SPECS:
    decls = ""
    extracts = ""
    var_for = {}
    for ocol, lcol in s['dest_match']:
        var = f"v_{lcol}"
        var_for[ocol] = var
        decls += f"    {var} VARCHAR2(100);\n"
        extracts += f"{var} := JSON_VALUE(p_payload, '$.{lcol}');\n    "
    ins_cols = ",".join(o for o,_ in s['dest_match'])
    ins_vals = ",".join(var_for[o] for o,_ in s['dest_match'])
    upd_set = ", ".join(f"{o}={var_for[o]}" for o,_ in s['dest_match'][1:]) if len(s['dest_match'])>1 else f"{s['dest_match'][0][0]}={var_for[s['dest_match'][0][0]]}"
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
        flag = st[0] if st else '?'
        print(f"  CREATE {s['sp']:<32}  status={flag}")
        if flag != 'VALID':
            oc.execute(f"SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='{s['sp']}'")
            for e in oc.fetchall(): print(f"    L{e[0]}:{e[1]} {e[2][:200]}")
    except Exception as e:
        print(f"  FAIL {s['sp']}: {str(e)[:200]}")
o.commit()

# 2.3 module_config Oracle
print("\n[2.3] module_config Oracle")
for s in NEW_SPECS:
    oc.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE = :1", [s['agg']])
    if oc.fetchone()[0] == 0:
        oc.execute("INSERT INTO FCME_USER.CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE) VALUES (:1, :2, 1)",
                   [s['agg'], s['sp']])
        print(f"  + {s['agg']:<28} -> {s['sp']}")
    else:
        print(f"  exists {s['agg']}")
o.commit()
o.close()

print("\n=== DEPLOY OK ===")
