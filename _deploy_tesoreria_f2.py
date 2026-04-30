"""Deploy Flujo 2 Tesoreria - 7 types Newcore -> Legacy."""
import pyodbc, oracledb
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

SPECS=[
    {'agg':'bancoTesoreria_type','dest':'BANCOTESORERIA_TYPE','trg':'TRG_OUTBOX_BANCOTESORERIA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOBNCO','NUMEROCTA'],
     'ltbl':'tstbbnco','lpk':['co_empr','qs_cnta_bnco'],
     'lpk_types':{'co_empr':'SMALLINT','qs_cnta_bnco':'CHAR(2)'},
     'map':{'co_empr':'CODIGOEMPRESA','qs_cnta_bnco':None}},

    {'agg':'cheque_type','dest':'CHEQUE_TYPE','trg':'TRG_OUTBOX_CHEQUE',
     'opk':'ID','okeys':['ID','NUMEROCHEQ','CODIGOBNCO'],
     'ltbl':'tstbochq','lpk':['co_empr','sc_pago'],
     'lpk_types':{'co_empr':'SMALLINT','sc_pago':'INT'},
     'map':{'co_empr':None,'sc_pago':'NUMEROCHEQ'}},

    {'agg':'estadoRegistroTesoreria_type','dest':'ESTADOREGISTROTESORERIA_TYPE','trg':'TRG_OUTBOX_ESTREGTESORERIA',
     'opk':'ID','okeys':['ID','ESTADOPAGO','DESCRIPCIONPAGO','INDICADORPAGO'],
     'ltbl':'tstbesta_regi','lpk':['st_pago'],
     'lpk_types':{'st_pago':'CHAR(1)'},
     'map':{'st_pago':'ESTADOPAGO'}},

    {'agg':'facturaTesoreria_type','dest':'FACTURATESORERIA_TYPE','trg':'TRG_OUTBOX_FACTURATESORERIA',
     'opk':'ID','okeys':['ID','NUMEROFACT','CODIGOEMPRESA','SECUENCIAPAGO','SECUENCIADBSO'],
     'ltbl':'tstbfact_teso','lpk':['co_empr','sc_pago','nu_fact','qs_dbso'],
     'lpk_types':{'co_empr':'SMALLINT','sc_pago':'INT','nu_fact':'VARCHAR(20)','qs_dbso':'INT'},
     'map':{'co_empr':'CODIGOEMPRESA','sc_pago':'SECUENCIAPAGO','nu_fact':'NUMEROFACT','qs_dbso':'SECUENCIADBSO'}},

    {'agg':'ordenPago_type','dest':'ORDENPAGO_TYPE','trg':'TRG_OUTBOX_ORDENPAGO',
     'opk':'ID','okeys':['ID','SECUENCIAORDE','CODIGOEMPRESA','CODIGOPROC'],
     'ltbl':'tstborde','lpk':['co_empr','co_proc','sc_orde'],
     'lpk_types':{'co_empr':'SMALLINT','co_proc':'CHAR(2)','sc_orde':'INT'},
     'map':{'co_empr':'CODIGOEMPRESA','co_proc':'CODIGOPROC','sc_orde':'SECUENCIAORDE'}},

    {'agg':'reversaDesembolso_type','dest':'REVERSADESEMBOLSO_TYPE','trg':'TRG_OUTBOX_REVERSADESEMBOLSO',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','SECUENCIAPAGO','CODIGOPROC'],
     'ltbl':'tstbreve_dsmb','lpk':['co_empr','sc_pago'],
     'lpk_types':{'co_empr':'SMALLINT','sc_pago':'INT'},
     'map':{'co_empr':'CODIGOEMPRESA','sc_pago':'SECUENCIAPAGO'}},

    {'agg':'transferenciaOrden_type','dest':'TRANSFERENCIAORDEN_TYPE','trg':'TRG_OUTBOX_TRANSFERENCIAORDEN',
     'opk':'ID','okeys':['ID','CODIGOTORD','NOMBRETORD','ESTADOTORD'],
     'ltbl':'tstbtord','lpk':['co_tord'],
     'lpk_types':{'co_tord':'SMALLINT'},
     'map':{'co_tord':'CODIGOTORD'}},
]

for s in SPECS: assert len(s['trg'])<=30, f"{s['trg']} >30"
print(f'TYPES = {len(SPECS)}')

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
    IF INSERTING THEN v_event := 'INSERT'; v_pk := TO_CHAR(:NEW.{s['opk']}); v_payload := JSON_OBJECT({json_kvs_new});
    ELSIF UPDATING THEN v_event := 'UPDATE'; v_pk := TO_CHAR(:NEW.{s['opk']}); v_payload := JSON_OBJECT({json_kvs_new});
    ELSE v_event := 'DELETE'; v_pk := TO_CHAR(:OLD.{s['opk']}); v_payload := JSON_OBJECT({json_kvs_old}); END IF;
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
print('\n[2] Legacy dbTS sp_<Type>_CRUD')
c_ts=sql('dbTS').cursor()
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
        c_ts.execute(body)
        ddl_sp=f"""
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
                BEGIN CATCH RETURN; END CATCH
            END
    END TRY
    BEGIN CATCH RETURN; END CATCH
END
"""
        c_ts.execute(ddl_sp)
        print(f"  CREATE dbTS.dbo.{sp_name}")
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
    target_db='dbTS'
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
                           VALUES (?, ?, ?, 'TESORERIA', 1, SYSDATETIME(), SYSDATETIME())""",
                        s['agg'], f"dbo.{wrapper}", target_db)
            mc="+config"
        else: mc="exists"
        print(f"  CREATE {wrapper:<48} -> {target_db}.dbo.{sp_name} ({mc})")
    except Exception as e:
        print(f"  FAIL {wrapper}: {str(e)[:300]}")

orcl.close()
print('\n=== DEPLOY OK ===')
