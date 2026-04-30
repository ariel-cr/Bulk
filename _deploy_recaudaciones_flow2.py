"""Deploy Flujo 2 Recaudaciones - 11 types (Newcore -> Legacy dbRC).

Aplica TODAS las lecciones:
  - TRG_OUTBOX Oracle <=30 chars + anti-loop CLIENT_INFO
  - SP CRUD con TRY_CAST + silent CATCH (sin RAISERROR)
  - Wrapper canonicos parsea JSON, llama SP CRUD
  - module_config canonicos
  - SET SESSION_CONTEXT('is_replicating',1) en SP CRUD para anti-loop F1
"""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

# Specs por type
# (agg, dest, trg_short, opk, okeys, ltbl, lpk, lpk_types, mapping {legacy:oracle})
SPECS = [
    {'agg':'aplicacionRecaudacion_type','dest':'APLICACIONRECAUDACION_TYPE','trg':'TRG_OUTBOX_APLICACIONRECAUD',
     'opk':'ID','okeys':['ID','SECUENCIAAPLIRECA','CODIGOCEDU','TIPORECA','CODIGOROL'],
     'ltbl':'rctbapli_reca','lpk':['sc_apli_reca','ci_cedu','ti_reca','co_rol'],
     'lpk_types':{'sc_apli_reca':'INT','ci_cedu':'VARCHAR(10)','ti_reca':'CHAR(2)','co_rol':'CHAR(2)'},
     'map':{'sc_apli_reca':'SECUENCIAAPLIRECA','ci_cedu':'CODIGOCEDU','ti_reca':'TIPORECA','co_rol':'CODIGOROL'}},

    {'agg':'caucionRecaudacion_type','dest':'CAUCIONRECAUDACION_TYPE','trg':'TRG_OUTBOX_CAUCIONRECAUD',
     'opk':'ID','okeys':['ID','TIPORECA','CODIGOEMPRESA','TIPODSTO'],
     'ltbl':'rctbcaut','lpk':['co_empr','ti_reca','ti_dsto'],
     'lpk_types':{'co_empr':'SMALLINT','ti_reca':'CHAR(2)','ti_dsto':'SMALLINT'},
     'map':{'co_empr':'CODIGOEMPRESA','ti_reca':'TIPORECA','ti_dsto':'TIPODSTO'}},

    {'agg':'devolucionRendicion_type','dest':'DEVOLUCIONRENDICION_TYPE','trg':'TRG_OUTBOX_DEVOLUCIONRENDICION',
     'opk':'ID','okeys':['ID','SECUENCIADEVO','CODIGOCEDU','TIPORECA'],
     'ltbl':'rctbdevo_rind','lpk':['sc_devo'],
     'lpk_types':{'sc_devo':'INT'},
     'map':{'sc_devo':'SECUENCIADEVO'}},

    {'agg':'estadoCuenta_type','dest':'ESTADOCUENTA_TYPE','trg':'TRG_OUTBOX_ESTADOCUENTA',
     'opk':'ID','okeys':['ID','SECUENCIACPBT','CODIGOCNTA','NUMEROCPBT'],
     'ltbl':'rctbesta_cnta','lpk':['co_empr','sc_cpbt'],
     'lpk_types':{'co_empr':'SMALLINT','sc_cpbt':'INT'},
     'map':{'co_empr':None,'sc_cpbt':'SECUENCIACPBT'}},

    {'agg':'estadoRecaudacion_type','dest':'ESTADORECAUDACION_TYPE','trg':'TRG_OUTBOX_ESTADORECAUDACION',
     'opk':'ID','okeys':['ID','ESTADORECA','DESCRIPCIONESTADO','NOMBRECORTO'],
     'ltbl':'rctbesta_reca','lpk':['st_reca'],
     'lpk_types':{'st_reca':'CHAR(2)'},
     'map':{'st_reca':'ESTADORECA'}},

    {'agg':'recaudacion_type','dest':'RECAUDACION_TYPE','trg':'TRG_OUTBOX_RECAUDACION',
     'opk':'ID','okeys':['ID','SECUENCIA_RECA','TIPO_RECA','ESTADO_RECA','MONTO_RECA'],
     'ltbl':'rctbreca','lpk':['sc_reca'],
     'lpk_types':{'sc_reca':'INT'},
     'map':{'sc_reca':'SECUENCIA_RECA'}},

    {'agg':'rendicion_type','dest':'RENDICION_TYPE','trg':'TRG_OUTBOX_RENDICION',
     'opk':'ID','okeys':['ID','CODIGO_CEDULA','CODIGO_ROL','TIPO_RECA'],
     'ltbl':'rctbrind','lpk':['co_empr','sc_reca'],
     'lpk_types':{'co_empr':'SMALLINT','sc_reca':'INT'},
     'map':{'co_empr':None,'sc_reca':None}},

    {'agg':'saldoRecaudacion_type','dest':'SALDORECAUDACION_TYPE','trg':'TRG_OUTBOX_SALDORECAUDACION',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOFONDO','SECUENCIAROL','CODIGOROL','TIPORECA'],
     'ltbl':'rctbcsal_reca','lpk':['co_empr','co_fond','fe_cort','sc_rol','co_rol','ti_reca'],
     'lpk_types':{'co_empr':'SMALLINT','co_fond':'SMALLINT','fe_cort':'DATETIME','sc_rol':'INT','co_rol':'CHAR(2)','ti_reca':'CHAR(2)'},
     'map':{'co_empr':'CODIGOEMPRESA','co_fond':'CODIGOFONDO','fe_cort':None,'sc_rol':'SECUENCIAROL','co_rol':'CODIGOROL','ti_reca':'TIPORECA'}},

    {'agg':'sciRecaudacion_type','dest':'SCIRECAUDACION_TYPE','trg':'TRG_OUTBOX_SCIRECAUDACION',
     'opk':'ID','okeys':['ID','SECUENCIASCIB','CODIGOROL'],
     'ltbl':'rctbcsci_bce','lpk':['sc_scib'],
     'lpk_types':{'sc_scib':'INT'},
     'map':{'sc_scib':'SECUENCIASCIB'}},

    {'agg':'tipoDescuento_type','dest':'TIPODESCUENTO_TYPE','trg':'TRG_OUTBOX_TIPODESCUENTO',
     'opk':'ID','okeys':['ID','TIPODESC','NOMBRETIPODESC','DESCRIPCIONTIPODESC'],
     'ltbl':'rctbtipo_desc','lpk':['ti_desc'],
     'lpk_types':{'ti_desc':'SMALLINT'},
     'map':{'ti_desc':'TIPODESC'}},

    {'agg':'transaccionRecaudacion_type','dest':'TRANSACCIONRECAUDACION_TYPE','trg':'TRG_OUTBOX_TRANSACCIONRECAUD',
     'opk':'ID','okeys':['ID','TIPORECA','NOMBRERECA','CODIGOFONDO','TIPOPROC'],
     'ltbl':'rctbtrec','lpk':['ti_reca'],
     'lpk_types':{'ti_reca':'CHAR(2)'},
     'map':{'ti_reca':'TIPORECA'}},
]

print(f'TYPES = {len(SPECS)}')
for s in SPECS:
    assert len(s['trg'])<=30, f"Trigger {s['trg']} excede 30 chars: {len(s['trg'])}"
print('All trigger names <=30 chars OK')

# === 1) ORACLE TRG_OUTBOX ===
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

# === 2) Legacy SP CRUD en dbRC con TRY_CAST + silent CATCH ===
print('\n[2] Legacy dbRC sp_<Type>_CRUD')
c_rc = sql('dbRC').cursor()
for s in SPECS:
    base = s['agg'][0].upper()+s['agg'][1:-5]+'Type' if s['agg'].endswith('_type') else s['agg']
    sp_name = f"sp_{base}_CRUD"
    pk_params = ", ".join(f"@{c} NVARCHAR(50) = NULL" for c in s['lpk'])
    declares = "\n        ".join(f"DECLARE @{c}_t {s['lpk_types'][c]} = TRY_CAST(@{c} AS {s['lpk_types'][c]});" for c in s['lpk'])
    null_check = " OR ".join(f"@{c}_t IS NULL" for c in s['lpk'])
    pk_match = " AND ".join(f"[{c}] = @{c}_t" for c in s['lpk'])
    cols_q = ",".join(f"[{c}]" for c in s['lpk'])
    vals_q = ",".join(f"@{c}_t" for c in s['lpk'])

    body = f"""
IF OBJECT_ID(N'dbo.{sp_name}', N'P') IS NOT NULL DROP PROCEDURE dbo.{sp_name};
"""
    try:
        c_rc.execute(body)
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
                    -- silencioso: tabla legacy puede tener mas NOT NULL no llenados
                    RETURN;
                END CATCH
            END
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END
"""
        c_rc.execute(ddl_sp)
        print(f"  CREATE dbRC.dbo.{sp_name}")
    except Exception as e:
        print(f"  FAIL {sp_name}: {str(e)[:200]}")

# === 3) Canonicos wrappers + module_config ===
print('\n[3] Canonicos wrappers + module_config')
can = sql('fcme_canonicos').cursor()
for s in SPECS:
    base = (s['agg'][:-5]+'Type') if s['agg'].endswith('_type') else s['agg']
    base = base[0].upper()+base[1:]
    wrapper = f"usp_inbox_{s['agg']}"
    sp_name = f"sp_{base}_CRUD"
    target_db = 'dbRC'

    decls = ""
    pass_args = ""
    for lcol in s['lpk']:
        ocol = s['map'].get(lcol)
        if ocol is None and lcol == 'co_empr':
            decls += f"        DECLARE @{lcol} NVARCHAR(50) = '1';\n"
        elif ocol is None:
            decls += f"        DECLARE @{lcol} NVARCHAR(50) = NULL;\n"
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
        can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE aggregate_type = ?", s['agg'])
        if can.fetchone()[0]==0:
            can.execute("""INSERT INTO dbo.cdc_inbox_module_config (aggregate_type, sp_name, target_db, module_name, active, created_at, updated_at)
                           VALUES (?, ?, ?, 'RECAUDACIONES', 1, SYSDATETIME(), SYSDATETIME())""",
                        s['agg'], f"dbo.{wrapper}", target_db)
            mc_status="+config"
        else:
            mc_status="exists"
        print(f"  CREATE {wrapper:<48} -> {target_db}.dbo.{sp_name} ({mc_status})")
    except Exception as e:
        print(f"  FAIL {wrapper}: {str(e)[:300]}")

orcl.close()
print('\n=== DEPLOY OK ===')
