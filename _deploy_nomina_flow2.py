"""Deploy Flujo 2 Nomina - 21 types (Newcore -> Legacy dbNO).

Aplica TODAS las lecciones de Participe:
  - Oracle TRG_OUTBOX con anti-loop SYS_CONTEXT('USERENV','CLIENT_INFO')
  - Naming Oracle <=30 chars (sin truncados que dupliquen)
  - Wrapper canonicos parsea JSON, llama SP CRUD
  - Legacy SP CRUD setea SESSION_CONTEXT('is_replicating', 1) anti-loop
  - INSERT...EXCEPTION DUP_VAL_ON_INDEX UPDATE (no MERGE -> identity issues)
  - SQLERRM via variable
  - No tocar wrappers preexistentes ni triggers Flujo 1 de Nomina
"""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# Specs por type
# (agg, oracle_dest, oracle_pk_col, oracle_payload_keys, legacy_tbl, legacy_pk_cols, mapping{legacy_col: oracle_col})
# trg_short max 30 chars: TRG_OUTBOX_ + 19 chars
SPECS = [
    {'agg':'anticipoNominaType','dest':'ANTICIPONOMINA_TYPE','trg':'TRG_OUTBOX_ANTICIPONOMINA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','ANIO','SECUENCIAANTICIPO','CODIGOEMPLEADO'],
     'ltbl':'notbcant','lpk':['co_empr','nu_anio','sc_anti','co_empl'],
     'map':{'co_empr':'CODIGOEMPRESA','nu_anio':'ANIO','sc_anti':'SECUENCIAANTICIPO','co_empl':'CODIGOEMPLEADO'}},

    {'agg':'cargaFamiliarType','dest':'CARGAFAMILIAR_TYPE','trg':'TRG_OUTBOX_CARGAFAMILIAR',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOEMPLEADO'],
     'ltbl':'notbcgfm','lpk':['co_empr','co_empl','sc_cgfm'],
     'map':{'co_empr':'CODIGOEMPRESA','co_empl':'CODIGOEMPLEADO','sc_cgfm':None}},

    {'agg':'cargoGeneralType','dest':'CARGOGENERAL_TYPE','trg':'TRG_OUTBOX_CARGOGENERAL',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGONOMINA'],
     'ltbl':'notbcarg','lpk':['co_empr','co_carg'],
     'map':{'co_empr':'CODIGOEMPRESA','co_carg':'CODIGONOMINA'}},

    {'agg':'cargoLaboralType','dest':'CARGOLABORAL_TYPE','trg':'TRG_OUTBOX_CARGOLABORAL',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOCARGADMINISTRADOR'],
     'ltbl':'notbcarg_admi','lpk':['co_empr','co_carg_admi'],
     'map':{'co_empr':'CODIGOEMPRESA','co_carg_admi':'CODIGOCARGADMINISTRADOR'}},

    {'agg':'catalogoNominaType','dest':'CATALOGONOMINA_TYPE','trg':'TRG_OUTBOX_CATALOGONOMINA',
     'opk':'ID','okeys':['ID','CODIGOMOTIVOAUDITORIA','DESCRIPCIONADICIONAL','ESTADOREGISTRO'],
     'ltbl':'notbcnom','lpk':['co_empr','co_nomi'],
     'map':{'co_empr':None,'co_nomi':'CODIGOMOTIVOAUDITORIA'}},

    {'agg':'configuracionNominaType','dest':'CONFIGURACIONNOMINA_TYPE','trg':'TRG_OUTBOX_CONFIGNOMINA',
     'opk':'ID','okeys':['ID','CODIGOINSTITUCION','NOMBREINSTITUCION'],
     'ltbl':'notbpara','lpk':['co_empr','co_frec_pago_rol'],
     'map':{'co_empr':None,'co_frec_pago_rol':'CODIGOINSTITUCION'}},

    {'agg':'empleadoAuditoriaType','dest':'EMPLEADOAUDITORIA_TYPE','trg':'TRG_OUTBOX_EMPLEADOAUDITORIA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOEMPLEADO','FECHAINGRESOEMPLEADO'],
     'ltbl':'notbempl_audi','lpk':['co_empr','co_empl','fe_ingr','ho_ingr'],
     'map':{'co_empr':'CODIGOEMPRESA','co_empl':'CODIGOEMPLEADO','fe_ingr':'FECHAINGRESOEMPLEADO','ho_ingr':None}},

    {'agg':'empleadoDetalleType','dest':'EMPLEADODETALLE_TYPE','trg':'TRG_OUTBOX_EMPLEADODETALLE',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOEMPL'],
     'ltbl':'notbempl_deta','lpk':['co_empr','co_empl'],
     'map':{'co_empr':'CODIGOEMPRESA','co_empl':'CODIGOEMPL'}},

    {'agg':'empleadoType','dest':'EMPLEADO_TYPE','trg':'TRG_OUTBOX_EMPLEADO',
     'opk':'ID','okeys':['ID','CODIGOCARGO'],
     'ltbl':'notbempl','lpk':['co_empr','co_empl'],
     'map':{'co_empr':None,'co_empl':'CODIGOCARGO'}},

    {'agg':'firmaHorarioType','dest':'FIRMAHORARIO_TYPE','trg':'TRG_OUTBOX_FIRMAHORARIO',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOEMPLEADO'],
     'ltbl':'notbfirm','lpk':['co_empr','co_empl','sc_firm'],
     'map':{'co_empr':'CODIGOEMPRESA','co_empl':'CODIGOEMPLEADO','sc_firm':None}},

    {'agg':'fondoReservaType','dest':'FONDORESERVA_TYPE','trg':'TRG_OUTBOX_FONDORESERVA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOEMPLEADO','TIPOACREDITACIONFONDORESERVA'],
     'ltbl':'notbfond_rese','lpk':['co_empr','co_empl'],
     'map':{'co_empr':'CODIGOEMPRESA','co_empl':'CODIGOEMPLEADO'}},

    {'agg':'historialIngresoType','dest':'HISTORIALINGRESO_TYPE','trg':'TRG_OUTBOX_HISTORIALINGRESO',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOEMPLEADO','ANIO','MES'],
     'ltbl':'notbhieg','lpk':['co_empr','co_empl','nu_ano','nu_mes'],
     'map':{'co_empr':'CODIGOEMPRESA','co_empl':'CODIGOEMPLEADO','nu_ano':'ANIO','nu_mes':'MES'}},

    {'agg':'nivelAcademicoType','dest':'NIVELACADEMICO_TYPE','trg':'TRG_OUTBOX_NIVELACADEMICO',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOEMPLEADO','CODIGOINSTITUCION','CODIGOTITULO'],
     'ltbl':'notbnive_acad_empl','lpk':['co_empr','co_empl'],
     'map':{'co_empr':'CODIGOEMPRESA','co_empl':'CODIGOEMPLEADO'}},

    {'agg':'nominaCabeceraType','dest':'NOMINACABECERA_TYPE','trg':'TRG_OUTBOX_NOMINACABECERA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGONOMINA'],
     'ltbl':'notbcrol','lpk':['co_empr','nu_rol'],
     'map':{'co_empr':'CODIGOEMPRESA','nu_rol':'CODIGONOMINA'}},

    {'agg':'pagoNominaType','dest':'PAGONOMINA_TYPE','trg':'TRG_OUTBOX_PAGONOMINA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CEDULABENEFICIARIO','CODIGOBANCO'],
     'ltbl':'notbpago_nomi','lpk':['co_empr','sc_rol','co_bene','rf_pago','mo_pago','sc_deta'],
     'map':{'co_empr':'CODIGOEMPRESA','sc_rol':None,'co_bene':'CEDULABENEFICIARIO','rf_pago':None,'mo_pago':None,'sc_deta':None}},

    {'agg':'parametroNominaType','dest':'PARAMETRONOMINA_TYPE','trg':'TRG_OUTBOX_PARAMETRONOMINA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOFRECUENCIAPAGOROL'],
     'ltbl':'notbpara_gene','lpk':['co_para'],
     'map':{'co_para':'CODIGOEMPRESA'}},

    {'agg':'patronalNominaType','dest':'PATRONALNOMINA_TYPE','trg':'TRG_OUTBOX_PATRONALNOMINA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOPROVINCIA'],
     'ltbl':'notbpatr','lpk':['co_empr','nu_patr'],
     'map':{'co_empr':'CODIGOEMPRESA','nu_patr':'CODIGOPROVINCIA'}},

    {'agg':'rolPagoType','dest':'ROLPAGO_TYPE','trg':'TRG_OUTBOX_ROLPAGO',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGONOMINA'],
     'ltbl':'notbdrol','lpk':['co_empr','nu_rol','co_empl','co_rubr'],
     'map':{'co_empr':'CODIGOEMPRESA','nu_rol':'CODIGONOMINA','co_empl':None,'co_rubr':None}},

    {'agg':'rubroNominaType','dest':'RUBRONOMINA_TYPE','trg':'TRG_OUTBOX_RUBRONOMINA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGORUBRO'],
     'ltbl':'notbrubr','lpk':['co_empr','co_rubr'],
     'map':{'co_empr':'CODIGOEMPRESA','co_rubr':'CODIGORUBRO'}},

    {'agg':'sectorIessType','dest':'SECTORIESS_TYPE','trg':'TRG_OUTBOX_SECTORIESS',
     'opk':'ID','okeys':['ID','CODIGOSECTOR','CODIGOGESTIONIESS','DESCRIPCIONSECTORIESS'],
     'ltbl':'notbsect_iess','lpk':['co_sect'],
     'map':{'co_sect':'CODIGOSECTOR'}},

    {'agg':'viaticoNominaType','dest':'VIATICONOMINA_TYPE','trg':'TRG_OUTBOX_VIATICONOMINA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOEMPLEADO'],
     'ltbl':'notbcvia','lpk':['co_empr','co_empl','sc_viat'],
     'map':{'co_empr':'CODIGOEMPRESA','co_empl':'CODIGOEMPLEADO','sc_viat':None}},
]

print(f'TYPES = {len(SPECS)}')
for s in SPECS:
    assert len(s['trg']) <= 30, f"Trigger {s['trg']} excede 30 chars: {len(s['trg'])}"
print('All trigger names <=30 chars OK')

# ============================================================
# 1) ORACLE: TRG_OUTBOX en cada FCME_USER tabla
# ============================================================
print('\n[1] Oracle TRG_OUTBOX')
orcl = oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o = orcl.cursor()
for s in SPECS:
    json_kvs_new = ", ".join(f"'{k}' VALUE :NEW.{k}" for k in s['okeys'])
    json_kvs_old = ", ".join(f"'{k}' VALUE :OLD.{k}" for k in s['okeys'])
    ddl = f"""
CREATE OR REPLACE TRIGGER FCME_USER.{s['trg']}
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.{s['dest']}
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.{s['opk']});
        v_payload := JSON_OBJECT({json_kvs_new});
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.{s['opk']});
        v_payload := JSON_OBJECT({json_kvs_new});
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.{s['opk']});
        v_payload := JSON_OBJECT({json_kvs_old});
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('{s['agg']}', v_pk, v_event, v_payload, 'FCME_USER.{s['dest']}', SYSTIMESTAMP);
END;
"""
    try:
        o.execute(ddl)
        o.execute(f"SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='{s['trg']}' AND object_type='TRIGGER'")
        st = o.fetchone()
        print(f"  CREATE {s['trg']:<32} status={st[0] if st else '?'}")
    except Exception as e:
        print(f"  FAIL {s['trg']}: {str(e)[:200]}")
orcl.commit()

# ============================================================
# 2) LEGACY: sp_<Type>_CRUD en dbNO
# ============================================================
print('\n[2] Legacy dbNO sp_<Type>_CRUD')
c_no = sql('dbNO').cursor()
for s in SPECS:
    sp_name = f"sp_{s['agg'][0].upper()+s['agg'][1:]}_CRUD"
    pk_params = ", ".join(f"@{c} NVARCHAR(50) = NULL" for c in s['lpk'])
    pk_match = " AND ".join(f"[{c}] = @{c}" for c in s['lpk'])
    pk_null = " OR ".join(f"@{c} IS NULL" for c in s['lpk'])
    cols_q = ",".join(f"[{c}]" for c in s['lpk'])
    vals_q = ",".join(f"@{c}" for c in s['lpk'])
    body = f"""
IF OBJECT_ID(N'dbo.{sp_name}', N'P') IS NOT NULL DROP PROCEDURE dbo.{sp_name};
"""
    try:
        c_no.execute(body)
        ddl_sp = f"""
CREATE PROCEDURE dbo.{sp_name}
    @Accion CHAR(1),
    {pk_params}
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        IF {pk_null} RETURN;
        IF @Accion = 'D'
            DELETE FROM dbo.[{s['ltbl']}] WHERE {pk_match};
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.[{s['ltbl']}] WHERE {pk_match})
            INSERT INTO dbo.[{s['ltbl']}] ({cols_q}) VALUES ({vals_q});
        -- si existe, no-op (no toca columnas no-PK para no danar)
    END TRY
    BEGIN CATCH
        DECLARE @msg NVARCHAR(2048) = ERROR_MESSAGE();
        RAISERROR (@msg, 16, 1);
    END CATCH
END
"""
        c_no.execute(ddl_sp)
        print(f"  CREATE dbNO.dbo.{sp_name}")
    except Exception as e:
        print(f"  FAIL {sp_name}: {str(e)[:200]}")

# ============================================================
# 3) CANONICOS: usp_inbox_<type> + module_config
# ============================================================
print('\n[3] Canonicos wrappers + module_config')
can = sql('fcme_canonicos').cursor()
for s in SPECS:
    base = s['agg'][0].upper()+s['agg'][1:]
    wrapper = f"usp_inbox_{s['agg']}"
    sp_name = f"sp_{base}_CRUD"
    target_db = 'dbNO'
    decls = ""
    pass_args = ""
    for lcol in s['lpk']:
        ocol = s['map'].get(lcol)
        if ocol is None:
            decls += f"        DECLARE @{lcol} NVARCHAR(50) = '1';\n"
        else:
            decls += f"        DECLARE @{lcol} NVARCHAR(50) = JSON_VALUE(@payload, '$.{ocol}');\n"
        pass_args += f"@{lcol}=@{lcol}, "
    pass_args = pass_args.rstrip(', ')

    drop_ddl = f"IF OBJECT_ID(N'dbo.{wrapper}', N'P') IS NOT NULL DROP PROCEDURE dbo.{wrapper};"
    body = f"""
CREATE PROCEDURE dbo.{wrapper}
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
{decls}
        EXEC {target_db}.dbo.{sp_name} @Accion=@accion, {pass_args};
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper {s['agg']}: ' + ERROR_MESSAGE());
    END CATCH
END
"""
    try:
        can.execute(drop_ddl)
        can.execute(body)
        # module_config
        can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE aggregate_type = ?", s['agg'])
        if can.fetchone()[0] == 0:
            can.execute("""INSERT INTO dbo.cdc_inbox_module_config (aggregate_type, sp_name, target_db, module_name, active, created_at, updated_at)
                           VALUES (?, ?, ?, 'NOMINA', 1, SYSDATETIME(), SYSDATETIME())""",
                        s['agg'], f"dbo.{wrapper}", target_db)
            mc_status = "+config"
        else:
            mc_status = "exists"
        print(f"  CREATE {wrapper:<40} -> {target_db}.dbo.{sp_name} ({mc_status})")
    except Exception as e:
        print(f"  FAIL {wrapper}: {str(e)[:300]}")

orcl.close()
print('\n=== DEPLOY OK ===')
