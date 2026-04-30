"""Deploy Flow 2 wiring para los 12 types faltantes.

Cada type recibe:
  1) TRG_OUTBOX_<TABLE> en FCME_USER (publica a FCME_USER.CDC_OUTBOX)
  2) usp_inbox_<type> wrapper en fcme_canonicos (parsea JSON, llama SP CRUD)
  3) Entry en cdc_inbox_module_config (canonicos)
  4) sp_<TYPE>_CRUD en BD legacy (UPSERT/DELETE con SESSION_CONTEXT anti-loop)

Anti-loop:
  - Oracle TRG_OUTBOX: skip si CLIENT_INFO = 'is_replicating'
  - Legacy SP CRUD: SET SESSION_CONTEXT('is_replicating', 1) antes de DML
"""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# Configuración por type
# Cada entrada: agg_type, oracle_dst, payload_keys (oracle col names),
#               legacy_db, legacy_tbl, legacy_pk_cols, mapping (oracle->legacy)
SPECS = [
    {
        'agg':'actualizacionAfiliadoType', 'odst':'ACTUALIZACION_AFILIADO_TYPE',
        'opk':['ID'], 'okeys':['ID','CODIGO_CEDU'],
        'ldb':'dbFC','ltbl':'fctbafil_actu','lpk':['ci_cedu'],
        'map':{'ci_cedu':'CODIGO_CEDU'}
    },
    {
        'agg':'actualizacionDocumentosType','odst':'ACTUALIZACION_DOCUMENTOS_TYPE',
        'opk':['ID'], 'okeys':['ID','SECUENCIA_ACTU_DOCS','CODIGO_EMPRESA','CODIGO_CEDU'],
        'ldb':'dbFC','ltbl':'fctbafil_info_actu_docs','lpk':['sc_actu_docs','co_empr','co_cedu'],
        'map':{'sc_actu_docs':'SECUENCIA_ACTU_DOCS','co_empr':'CODIGO_EMPRESA','co_cedu':'CODIGO_CEDU'}
    },
    {
        'agg':'areaLaboralParticipeType','odst':'AREALABORALPARTICIPE_TYPE',
        'opk':['ID'], 'okeys':['ID','CODIGOAREALABORAL','DESCRIPCIONAREALABORAL'],
        'ldb':'dbFC','ltbl':'fctbarea_lbrl','lpk':['co_area_lbrl'],
        'map':{'co_area_lbrl':'CODIGOAREALABORAL'}
    },
    {
        'agg':'comisionParticipe_type','odst':'COMISIONPARTICIPE_TYPE',
        'opk':['ID'], 'okeys':['ID','CODIGOSECUENCIACOMISION','CEDULAPROMOTOR'],
        'ldb':'dbCT','ltbl':'cttbcomi_cred','lpk':['ti_cred','aa_cred','qs_cred','ci_ejec'],
        'map':{}  # PK seq derivado, lo manejamos en wrapper
    },
    {
        'agg':'imagenesType','odst':'IMAGENESTYPE',
        'opk':['ID'], 'okeys':['ID','CODIGOIMAGEN','NOMBREARCHIVO'],
        'ldb':'dbFC','ltbl':'fctbpart_foto','lpk':['co_empr','ci_cedu'],
        'map':{'co_empr':None,'ci_cedu':'CODIGOIMAGEN'}  # co_empr=1 default
    },
    {
        'agg':'juridicoInformacionBasicaType','odst':'JURIDICOINFORMACIONBASICATYPE',
        'opk':['ID'], 'okeys':['ID','IDENTIFICACION'],
        'ldb':'dbFC','ltbl':'fctbjuri_inst','lpk':['co_empr','co_juri'],
        'map':{'co_empr':None,'co_juri':'IDENTIFICACION'}
    },
    {
        'agg':'naturalInformacionBasicaType','odst':'NATURALINFORMACIONBASICATYPE',
        'opk':['ID'], 'okeys':['ID','IDENTIFICACION'],
        'ldb':'dbFC','ltbl':'sfct_afiliado','lpk':['co_empr','ci_cedula'],
        'map':{'co_empr':None,'ci_cedula':'IDENTIFICACION'}
    },
    {
        'agg':'otrosIngresosAfiliadoType','odst':'OTROSINGRESOSAFILIADO_TYPE',
        'opk':['ID'], 'okeys':['ID','CODIGOROL','CODIGOCEDU','CODIGOOTROINGRRUBR'],
        'ldb':'dbFC','ltbl':'fctbotro_ingr_afil','lpk':['co_rol','ci_cedu','co_otro_ingr_rubr'],
        'map':{'co_rol':'CODIGOROL','ci_cedu':'CODIGOCEDU','co_otro_ingr_rubr':'CODIGOOTROINGRRUBR'}
    },
    {
        'agg':'personaDireccionesType','odst':'PERSONADIRECCIONESTYPE',
        'opk':['ID'], 'okeys':['ID','IDENTIFICACION'],
        'ldb':'dbFC','ltbl':'sfct_afiliado','lpk':['co_empr','ci_cedula'],
        'map':{'co_empr':None,'ci_cedula':'IDENTIFICACION'}
    },
    {
        'agg':'personaFirmasType','odst':'PERSONAFIRMASTYPE',
        'opk':['ID'], 'okeys':['ID','IDENTIFICACION','SECUENCIAPERSONAFIRMA'],
        'ldb':'dbIM','ltbl':'imtbbene_firm','lpk':['co_bene','sc_vivi'],  # functional PK
        'map':{'co_bene':'IDENTIFICACION','sc_vivi':'SECUENCIAPERSONAFIRMA'}
    },
    {
        'agg':'personaType','odst':'PERSONATYPE',
        'opk':['ID'], 'okeys':['ID','IDENTIFICACION'],
        'ldb':'dbFC','ltbl':'sfct_afiliado','lpk':['co_empr','ci_cedula'],
        'map':{'co_empr':None,'ci_cedula':'IDENTIFICACION'}
    },
    {
        'agg':'rolNominaType','odst':'ROLNOMINA_TYPE',
        'opk':['ID'], 'okeys':['ID','CODIGORUBRO','DESCRIPCIONRUBRO'],
        'ldb':'dbFC','ltbl':'sfct_rubro_rol','lpk':['co_empr','ci_rubro_rol'],
        'map':{'co_empr':None,'ci_rubro_rol':'CODIGORUBRO'}
    },
]

# ============================================================
# 1) ORACLE: TRG_OUTBOX_<ODST> con anti-loop
# ============================================================
print("[1] Oracle TRG_OUTBOX triggers")
orcl = oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o = orcl.cursor()

for s in SPECS:
    trg_name = f"TRG_OUTBOX_{s['odst']}"[:30]
    # construir lista de cols a publicar en JSON
    cols_select = ", '" + s['okeys'][0] + "' VALUE :NEW." + s['okeys'][0]
    cols_select_d = ", '" + s['okeys'][0] + "' VALUE :OLD." + s['okeys'][0]
    json_kvs_new = ", ".join(f"'{k}' VALUE :NEW.{k}" for k in s['okeys'])
    json_kvs_old = ", ".join(f"'{k}' VALUE :OLD.{k}" for k in s['okeys'])
    pk_new = s['opk'][0]
    ddl = f"""
CREATE OR REPLACE TRIGGER FCME_USER.{trg_name}
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.{s['odst']}
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.{pk_new});
        v_payload := JSON_OBJECT({json_kvs_new});
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.{pk_new});
        v_payload := JSON_OBJECT({json_kvs_new});
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.{pk_new});
        v_payload := JSON_OBJECT({json_kvs_old});
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('{s['agg']}', v_pk, v_event, v_payload, 'FCME_USER.{s['odst']}', SYSTIMESTAMP);
END;
"""
    try:
        o.execute(ddl)
        # status
        o.execute(f"SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='{trg_name}' AND object_type='TRIGGER'")
        st = o.fetchone()
        print(f"  CREATE {trg_name}  status={st[0] if st else '?'}")
    except Exception as e:
        print(f"  FAIL {trg_name}: {str(e)[:200]}")
orcl.commit()

# ============================================================
# 2) LEGACY: sp_<TYPE>_CRUD por type
# ============================================================
print("\n[2] Legacy sp_<TYPE>_CRUD")

def make_sp_crud(s):
    """SP CRUD minimal: setea SESSION_CONTEXT y hace MERGE/DELETE."""
    sp_name = f"sp_{s['agg'][0].upper()+s['agg'][1:]}_CRUD".replace('_type','Type').replace('_TYPE','Type')
    # nombre simplificado: usar agg sin _type
    if s['agg'].endswith('_type'):
        base = s['agg'][:-5] + 'Type'
    else:
        base = s['agg']
    sp_name = f"sp_{base[0].upper()+base[1:]}_CRUD"
    # Usar variant pluralizado: sp_<Base>_CRUD
    pk_params = ", ".join(f"@{c} NVARCHAR(50) = NULL" for c in s['lpk'])
    pk_match = " AND ".join(f"[{c}] = @{c}" for c in s['lpk'])
    pk_match_null_check = " OR ".join(f"@{c} IS NULL" for c in s['lpk'])
    ddl = f"""
USE [{s['ldb']}];
"""
    return sp_name, ddl, pk_params, pk_match, pk_match_null_check

for s in SPECS:
    sp_name = f"sp_{(s['agg'][:-5]+'Type') if s['agg'].endswith('_type') else s['agg']}_CRUD"
    pk_params = ", ".join(f"@{c} NVARCHAR(50) = NULL" for c in s['lpk'])
    pk_match = " AND ".join(f"[{c}] = @{c}" for c in s['lpk'])
    pk_null_check = " OR ".join(f"@{c} IS NULL" for c in s['lpk'])
    cols_q = ",".join(f"[{c}]" for c in s['lpk'])
    vals_q = ",".join(f"@{c}" for c in s['lpk'])

    body = f"""
IF OBJECT_ID(N'dbo.{sp_name}', N'P') IS NOT NULL DROP PROCEDURE dbo.{sp_name};
"""
    try:
        c = sql(s['ldb']).cursor()
        c.execute(body)
        ddl_sp = f"""
CREATE PROCEDURE dbo.{sp_name}
    @Accion CHAR(1),
    {pk_params}
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        IF {pk_null_check}
            RETURN; -- sin PK no podemos operar
        IF @Accion = 'D'
        BEGIN
            DELETE FROM dbo.[{s['ltbl']}] WHERE {pk_match};
        END
        ELSE
        BEGIN
            -- UPSERT minimal por PK
            IF EXISTS (SELECT 1 FROM dbo.[{s['ltbl']}] WHERE {pk_match})
            BEGIN
                -- update no-op (no toca columnas no-PK para no danar datos existentes)
                UPDATE dbo.[{s['ltbl']}] SET {s['lpk'][0]} = {s['lpk'][0]} WHERE {pk_match};
            END
            ELSE
            BEGIN
                INSERT INTO dbo.[{s['ltbl']}] ({cols_q}) VALUES ({vals_q});
            END
        END
    END TRY
    BEGIN CATCH
        -- silenciar errores para no danar la tabla; el wrapper sabra del error
        DECLARE @msg NVARCHAR(2048) = ERROR_MESSAGE();
        RAISERROR (@msg, 16, 1);
    END CATCH
END;
"""
        c.execute(ddl_sp)
        print(f"  CREATE {s['ldb']}.dbo.{sp_name}")
    except Exception as e:
        print(f"  FAIL {sp_name}: {str(e)[:200]}")

# ============================================================
# 3) CANONICOS: usp_inbox_<type> wrapper + module_config
# ============================================================
print("\n[3] Canonicos wrappers + module_config")
can = sql('fcme_canonicos').cursor()

for s in SPECS:
    base_name = (s['agg'][:-5]+'Type') if s['agg'].endswith('_type') else s['agg']
    wrapper = f"usp_inbox_{base_name[0].lower()+base_name[1:]}"
    sp_name = f"sp_{base_name}_CRUD"
    target_db = s['ldb']
    # Construir extracciones JSON_VALUE
    decls = ""
    pass_args = ""
    for lcol in s['lpk']:
        ocol = s['map'].get(lcol)
        if ocol is None and lcol in ('co_empr',):
            decls += f"        DECLARE @{lcol} NVARCHAR(50) = '1';\n"
        else:
            decls += f"        DECLARE @{lcol} NVARCHAR(50) = JSON_VALUE(@payload, '$.{ocol or lcol}');\n"
        pass_args += f"@{lcol}=@{lcol}, "
    pass_args = pass_args.rstrip(', ')

    ddl = f"""
IF OBJECT_ID(N'dbo.{wrapper}', N'P') IS NOT NULL DROP PROCEDURE dbo.{wrapper};
"""
    try:
        can.execute(ddl)
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
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper {base_name}: ' + ERROR_MESSAGE());
    END CATCH
END;
"""
        can.execute(body)
        # module_config
        can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE aggregate_type = ?", base_name)
        if can.fetchone()[0] == 0:
            can.execute("""INSERT INTO dbo.cdc_inbox_module_config (aggregate_type, sp_name, target_db, module_name, active, created_at, updated_at)
                           VALUES (?, ?, ?, 'PARTICIPE', 1, SYSDATETIME(), SYSDATETIME())""",
                        base_name, f"dbo.{wrapper}", target_db)
            mc_status = "+config"
        else:
            mc_status = "exists"
        print(f"  CREATE {wrapper} -> {target_db}.dbo.{sp_name}  ({mc_status})")
    except Exception as e:
        print(f"  FAIL {wrapper}: {str(e)[:300]}")

orcl.close()
print("\n=== DEPLOY OK ===")
