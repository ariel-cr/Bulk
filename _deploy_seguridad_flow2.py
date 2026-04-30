"""Deploy Flujo 2 Seguridad - 11 types (Newcore -> Legacy dbSG)."""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

SPECS = [
    {'agg':'aplicacionFuncion_type','dest':'APLICACIONFUNCION_TYPE','trg':'TRG_OUTBOX_APLICACIONFUNCION',
     'opk':'ID','okeys':['ID','CODIGOAPLICACION','TIPOLOCALIDAD','NOMBREAPLICACION','ESTADOAPLICACION'],
     'ltbl':'sgtbapli','lpk':['co_apli','ti_loca'],
     'lpk_types':{'co_apli':'CHAR(2)','ti_loca':'CHAR(1)'},
     'map':{'co_apli':'CODIGOAPLICACION','ti_loca':'TIPOLOCALIDAD'}},

    {'agg':'auditoriaFlujo_type','dest':'AUDITORIAFLUJO_TYPE','trg':'TRG_OUTBOX_AUDITORIAFLUJO',
     'opk':'ID','okeys':['ID','CODIGOPROCESO','CODIGOSUBPROCESO'],
     'ltbl':'sgtbtran','lpk':['co_apli','co_func','nu_tran','ti_loca'],
     'lpk_types':{'co_apli':'CHAR(2)','co_func':'CHAR(2)','nu_tran':'SMALLINT','ti_loca':'CHAR(1)'},
     'map':{'co_apli':'CODIGOPROCESO','co_func':'CODIGOSUBPROCESO','nu_tran':None,'ti_loca':None}},

    {'agg':'cuentaNostroType','dest':'CUENTANOSTRO_TYPE','trg':'TRG_OUTBOX_CUENTANOSTRO',
     'opk':'ID','okeys':['ID','CODIGO','NOMBRE','ESTADO'],
     'ltbl':'sgtbcnts','lpk':['co_cnts'],
     'lpk_types':{'co_cnts':'SMALLINT'},
     'map':{'co_cnts':'CODIGO'}},

    {'agg':'empresa_type','dest':'EMPRESA_TYPE','trg':'TRG_OUTBOX_EMPRESA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','NOMBREEMPRESA','ESTADOEMPRESA'],
     'ltbl':'sgtbempr','lpk':['co_empr'],
     'lpk_types':{'co_empr':'SMALLINT'},
     'map':{'co_empr':'CODIGOEMPRESA'}},

    {'agg':'firmaSeguridad_type','dest':'FIRMASEGURIDAD_TYPE','trg':'TRG_OUTBOX_FIRMASEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOUSUARIO','FECHAFIRMAENTRADASALIDA','HORAFIRMAENTRADASALIDA'],
     'ltbl':'sgtbfirm','lpk':['co_empr','co_usua','fe_firm','ho_firm'],
     'lpk_types':{'co_empr':'SMALLINT','co_usua':'INT','fe_firm':'DATETIME','ho_firm':'CHAR(8)'},
     'map':{'co_empr':'CODIGOEMPRESA','co_usua':'CODIGOUSUARIO','fe_firm':'FECHAFIRMAENTRADASALIDA','ho_firm':'HORAFIRMAENTRADASALIDA'}},

    {'agg':'fondoSeguridad_type','dest':'FONDOSEGURIDAD_TYPE','trg':'TRG_OUTBOX_FONDOSEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOFONDO','NOMBREFONDO','ESTADOFONDO'],
     'ltbl':'sgtbfond','lpk':['co_fond'],
     'lpk_types':{'co_fond':'SMALLINT'},
     'map':{'co_fond':'CODIGOFONDO'}},

    {'agg':'localidad_type','dest':'LOCALIDAD_TYPE','trg':'TRG_OUTBOX_LOCALIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOLOCALIDAD','CODIGOPROVINCIA','CEDULAREPRESENTANTE'],
     'ltbl':'sgtbloca','lpk':['co_empr','co_loca'],
     'lpk_types':{'co_empr':'SMALLINT','co_loca':'SMALLINT'},
     'map':{'co_empr':'CODIGOEMPRESA','co_loca':'CODIGOLOCALIDAD'}},

    {'agg':'parametroSeguridad_type','dest':'PARAMETROSEGURIDAD_TYPE','trg':'TRG_OUTBOX_PARAMETROSEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOPARAMETRO','NOMBREPARAMETRO','VALORPARAMETRO'],
     'ltbl':'sgtbpara','lpk':['co_empr','co_para'],
     'lpk_types':{'co_empr':'SMALLINT','co_para':'SMALLINT'},
     'map':{'co_empr':'CODIGOEMPRESA','co_para':'CODIGOPARAMETRO'}},

    {'agg':'passwordSeguridad_type','dest':'PASSWORDSEGURIDAD_TYPE','trg':'TRG_OUTBOX_PASSWORDSEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOUSUARIO','SECUENCIACAMBIOCONTRASENIA'],
     'ltbl':'sgtbpass','lpk':['co_empr','co_usua','sc_pass'],
     'lpk_types':{'co_empr':'SMALLINT','co_usua':'INT','sc_pass':'SMALLINT'},
     'map':{'co_empr':'CODIGOEMPRESA','co_usua':'CODIGOUSUARIO','sc_pass':'SECUENCIACAMBIOCONTRASENIA'}},

    {'agg':'usuarioSeguridad_type','dest':'USUARIOSEGURIDAD_TYPE','trg':'TRG_OUTBOX_USUARIOSEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOUSUARIO'],
     'ltbl':'sgtbusua','lpk':['co_empr','co_usua'],
     'lpk_types':{'co_empr':'SMALLINT','co_usua':'INT'},
     'map':{'co_empr':'CODIGOEMPRESA','co_usua':'CODIGOUSUARIO'}},

    {'agg':'usuarioServicio_type','dest':'USUARIOSERVICIO_TYPE','trg':'TRG_OUTBOX_USUARIOSERVICIO',
     'opk':'ID','okeys':['ID','CODIGOUSUARIO','CONTRASENIA'],
     'ltbl':'sgtbconf_serv_apli','lpk':['sc_serv'],
     'lpk_types':{'sc_serv':'INT'},
     'map':{'sc_serv':None}},
]

for s in SPECS:
    assert len(s['trg'])<=30, f"trg name >30: {s['trg']} ({len(s['trg'])})"
print(f'TYPES = {len(SPECS)}, all trg names <=30 OK')

# === 1) Oracle TRG_OUTBOX ===
print('\n[1] Oracle TRG_OUTBOX')
orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()
for s in SPECS:
    json_kvs_new=", ".join(f"'{k}' VALUE :NEW.{k}" for k in s['okeys'])
    json_kvs_old=", ".join(f"'{k}' VALUE :OLD.{k}" for k in s['okeys'])
    ddl=f"""
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
        st=o.fetchone()
        print(f"  CREATE {s['trg']:<32}  status={st[0] if st else '?'}")
    except Exception as e:
        print(f"  FAIL {s['trg']}: {str(e)[:200]}")
orcl.commit()

# === 2) Legacy SP CRUD ===
print('\n[2] Legacy dbSG sp_<Type>_CRUD')
c_sg=sql('dbSG').cursor()
for s in SPECS:
    base = s['agg'][0].upper()+s['agg'][1:-5]+'Type' if s['agg'].endswith('_type') else s['agg']
    sp_name = f"sp_{base}_CRUD"
    pk_params = ", ".join(f"@{c} NVARCHAR(50) = NULL" for c in s['lpk'])
    declares = "\n        ".join(f"DECLARE @{c}_t {s['lpk_types'][c]} = TRY_CAST(@{c} AS {s['lpk_types'][c]});" for c in s['lpk'])
    null_check = " OR ".join(f"@{c}_t IS NULL" for c in s['lpk'])
    pk_match = " AND ".join(f"[{c}] = @{c}_t" for c in s['lpk'])
    cols_q = ",".join(f"[{c}]" for c in s['lpk'])
    vals_q = ",".join(f"@{c}_t" for c in s['lpk'])
    body=f"IF OBJECT_ID(N'dbo.{sp_name}', N'P') IS NOT NULL DROP PROCEDURE dbo.{sp_name};"
    try:
        c_sg.execute(body)
        ddl_sp = f"""
CREATE PROCEDURE dbo.{sp_name}
    @Accion CHAR(1),
    {pk_params}
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        {declares}
        IF {null_check} RETURN;
        IF @Accion = 'D'
            DELETE FROM dbo.[{s['ltbl']}] WHERE {pk_match};
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.[{s['ltbl']}] WHERE {pk_match})
            BEGIN
                BEGIN TRY
                    INSERT INTO dbo.[{s['ltbl']}] ({cols_q}) VALUES ({vals_q});
                END TRY
                BEGIN CATCH
                    RETURN;
                END CATCH
            END
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END
"""
        c_sg.execute(ddl_sp)
        print(f"  CREATE dbSG.dbo.{sp_name}")
    except Exception as e:
        print(f"  FAIL {sp_name}: {str(e)[:200]}")

# === 3) Canonicos wrappers + module_config ===
print('\n[3] Canonicos wrappers + module_config')
can=sql('fcme_canonicos').cursor()
for s in SPECS:
    base=(s['agg'][:-5]+'Type') if s['agg'].endswith('_type') else s['agg']
    base=base[0].upper()+base[1:]
    wrapper=f"usp_inbox_{s['agg']}"
    sp_name=f"sp_{base}_CRUD"
    target_db='dbSG'
    decls=""; pass_args=""
    for lcol in s['lpk']:
        ocol=s['map'].get(lcol)
        if ocol is None and lcol=='co_empr':
            decls+=f"        DECLARE @{lcol} NVARCHAR(50) = '1';\n"
        elif ocol is None:
            decls+=f"        DECLARE @{lcol} NVARCHAR(50) = NULL;\n"
        else:
            decls+=f"        DECLARE @{lcol} NVARCHAR(50) = JSON_VALUE(@payload, '$.{ocol}');\n"
        pass_args+=f"@{lcol}=@{lcol}, "
    pass_args=pass_args.rstrip(', ')
    drop_ddl=f"IF OBJECT_ID(N'dbo.{wrapper}', N'P') IS NOT NULL DROP PROCEDURE dbo.{wrapper};"
    body=f"""
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
        can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE aggregate_type = ?", s['agg'])
        if can.fetchone()[0]==0:
            can.execute("""INSERT INTO dbo.cdc_inbox_module_config (aggregate_type, sp_name, target_db, module_name, active, created_at, updated_at)
                           VALUES (?, ?, ?, 'SEGURIDAD', 1, SYSDATETIME(), SYSDATETIME())""",
                        s['agg'], f"dbo.{wrapper}", target_db)
            mc="+config"
        else:
            mc="exists"
        print(f"  CREATE {wrapper:<48} -> {target_db}.dbo.{sp_name} ({mc})")
    except Exception as e:
        print(f"  FAIL {wrapper}: {str(e)[:300]}")

orcl.close()
print('\n=== DEPLOY OK ===')
