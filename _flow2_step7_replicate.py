"""Paso 7: Replica el patron Newcore -> Legacy a todos los types factibles.

Para cada aggregate_type:
  - Trigger Oracle en la tabla TYPE -> CDC_OUTBOX (serializa todas las cols a JSON)
  - sp_<Type>_CRUD en la BD legacy (recibe @Accion + @PK + @PayloadJSON)
  - usp_inbox_<Type> wrapper en canonicos (extrae PK, llama CRUD)
  - Entrada en cdc_inbox_module_config

Estrategia:
- Tabla Oracle SNAKE_CASE: mapeo PREFIX bilateral automatico.
- Tabla Oracle CAMEL_CASE simple (catalogo 2 cols): mapeo directo.
- Multi-source legacy: escoge la tabla con mas columnas mapeables.
- Tipos SQL: inspeccion de sys.columns + sys.types.
- Anti-loop: SESSION_CONTEXT en SQL, CLIENT_INFO en Oracle.
- Wrappers TRY/CATCH (regla obligatoria).
"""
import pyodbc, oracledb, re, json
from collections import defaultdict

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

AT_TO_TABLE = {
    "actualizacionAfiliadoType": "ACTUALIZACION_AFILIADO_TYPE",
    "actualizacionDocumentosType": "ACTUALIZACION_DOCUMENTOS_TYPE",
    "agendaMailAfiliadoType": "AGENDAMAILAFILIADO_TYPE",
    "areaLaboralParticipeType": "AREALABORALPARTICIPE_TYPE",
    "auditoriaAfiliadoType": "AUDITORIAAFILIADO_TYPE",
    "beneficiarioParticipeType": "BENEFICIARIOPARTICIPE_TYPE",
    "cuentaBancariaAfiliadoType": "CUENTABANCARIAAFILIADO_TYPE",
    "distribucionAfiliadoType": "DISTRIBUCIONAFILIADO_TYPE",
    "documentacionAfiliadoType": "DOCUMENTACIONAFILIADO_TYPE",
    "firmanteParticipeType": "FIRMANTEPARTICIPE_TYPE",
    "grupoFamiliarType": "GRUPOFAMILIAR_TYPE",
    "informacionAdicionalAfiliadoType": "INFORMACIONADICIONALAFILIADO_TYPE",
    "institucionType": "INSTITUCION_TYPE",
    "motivoContableType": "MOTIVOCONTABLE_TYPE",
    "movimientoCuentaType": "MOVIMIENTOCUENTA_TYPE",
    "movimientoTemporalType": "MOVIMIENTOTEMPORAL_TYPE",
    "naturalInformacionAdicionalType": "NATURALINFORMACIONADICIONALTYPE",
    "naturalInformacionBasicaType": "NATURALINFORMACIONBASICATYPE",
    "naturalIngresosEgresosType": "NATURALINGRESOSEGRESOSTYPE",
    "naturalTrabajoType": "NATURALTRABAJOTYPE",
    "otrosIngresosAfiliadoType": "OTROSINGRESOSAFILIADO_TYPE",
    "personaDireccionesType": "PERSONADIRECCIONESTYPE",
    "personaReferenciasBancariasType": "PERSONAREFERENCIASBANCARIASTYPE",
    "personaReferenciasPersonalesType": "PERSONAREFERENCIASPERSONALESTYPE",
    "personaTelefonosType": "PERSONATELEFONOSTYPE",
    "personaType": "PERSONATYPE",
    "personaVinculacionesType": "PERSONAVINCULACIONESTYPE",
    "referenciaParticipeType": "REFERENCIAPARTICIPE_TYPE",
    "reporteSIBSParticipeType": "REPORTESIBSPARTICIPE_TYPE",
    "retiroLiquidacionType": "RETIROLIQUIDACION_TYPE",
    "retiroVoluntarioEstadoType": "RETIROVOLUNTARIOESTADO_TYPE",
    "rolNominaType": "ROLNOMINA_TYPE",
    "saldoDiarioRubroType": "SALDODIARIORUBRO_TYPE",
    "saldoDiarioType": "SALDODIARIO_TYPE",
    "seguroVidaParticipeType": "SEGUROVIDAPARTICIPE_TYPE",
    "servicioAdicionalType": "SERVICIOADICIONAL_TYPE",
}

PREFIX_MAP_OR2LG = {
    "CODIGO_": "ci_", "DESCRIPCION_": "ds_", "NUMERO_": "nu_",
    "TEXTO_": "tx_", "INDICADOR_": "in_", "FECHA_": "fe_",
    "TIPO_": "ti_", "NOMBRE_": "no_", "SECUENCIA_": "sc_",
    "VALOR_": "va_", "MONTO_": "mn_", "ESTADO_": "st_",
}
def or2lg_snake(oc):
    for p, r in PREFIX_MAP_OR2LG.items():
        if oc.startswith(p):
            return r + oc[len(p):].lower()
    return oc.lower()

# Diccionario CAMEL Oracle -> [legacy candidates] (mismo del flujo 1)
DOMAIN_MAP = {
    "IDENTIFICACION": ["ci_cedu","ci_cedula","ci_iden","ci_id","ci_rcfd","co_cedu"],
    "CODIGOTIPOIDENTIFICACION": ["ci_tipo","ti_iden","ti_id"],
    "PRIMERAPELLIDO": ["no_prim_apel","no_apellido","no_apel","no_apel_prim"],
    "SEGUNDOAPELLIDO": ["no_segu_apel","no_apel_segu"],
    "PRIMERNOMBRE": ["no_prim_nomb","no_nombre","no_nomb_prim","no_nomb"],
    "SEGUNDONOMBRE": ["no_segu_nomb","no_nomb_segu"],
    "NOMBRELEGAL": ["no_lega","no_legal","no_razo_soci","no_inst","no_banco"],
    "NOMBREPREFERIDO": ["no_pref"],
    "FECHANACIMIENTO": ["fe_naci","fe_nacimiento"],
    "FECHAINGRESO": ["fe_ingr","fe_ingreso","fe_alta","fx_creacion"],
    "FECHAMODIFICACION": ["fe_modi","fe_actu"],
    "FECHASALIDA": ["fe_sali","fe_baja"],
    "FECHACREACION": ["fe_crea","fx_creacion"],
    "CODIGOSEXO": ["ti_sexo","co_sexo","ti_gene","co_gene"],
    "CODIGOESTADOCIVIL": ["es_civi","st_civi","ti_esta_civi","co_esta_civi"],
    "CODIGOPAIS": ["co_pais"],
    "CODIGOPROVINCIA": ["co_prov"],
    "CODIGOCANTON": ["co_cant"],
    "CODIGOPARROQUIA": ["co_parr"],
    "CODIGOCIUDAD": ["co_ciud","co_ciuda"],
    "CODIGOBARRIO": ["co_barr"],
    "CALLE": ["ds_call_prim","ds_call"],
    "DIRECCION": ["ds_dire","ds_direc"],
    "NUMERO": ["nu_call_prim","nu_dire"],
    "URBANIZACION": ["ds_cdla","ds_urba"],
    "INMUEBLE": ["nu_vill","nu_inmu"],
    "DEPARTAMENTO": ["nu_dpto","co_depa"],
    "TRANSVERSAL": ["ds_call_secu"],
    "SECTOR": ["ti_sector","ti_sect"],
    "OBSERVACIONES": ["ds_observaciones","ds_obse","tx_obse","ds_obser","no_cont"],
    "NUMEROTELEFONO": ["tx_telf","tx_telf_conv","tx_fono","nu_tele"],
    "EMPRESAOPERADORA": ["ti_oper"],
    "CODIGOTIPOTELEFONO": ["ti_telf"],
    "EXTENSION": ["nu_exte"],
    "CODIGOTIPODIRECCION": ["ti_dire"],
    "CODIGOTIPOUBICACION": ["ti_ubic"],
    "DIRECCIONPRINCIPAL": ["in_dire_prin","in_principal"],
    "FECHAINGRESORESIDENCIA": ["fe_resi"],
    "NOMBREPROPIETARIO": ["no_propi"],
    "CODIGOTIPOSITIO": ["ti_siti"],
    "CODIGOZIP5": ["co_zip"],
    "NOMBREEMPLEADOR": ["no_empl","no_inst"],
    "CODIGOCARGO": ["co_carg"],
    "CODIGOCARGOPERSONA": ["co_carg"],
    "CODIGOCODIGOCARGO": ["co_carg"],
    "TIPOCONTRATO": ["ti_cont"],
    "CODIGONIVELLABORAL": ["co_nive"],
    "CODIGOCATEGORIATRABAJO": ["co_cate"],
    "TIEMPOPARCIAL": ["ti_jorn"],
    "FECHAINGRESOTRABAJO": ["fe_ingr_trab","fe_ingr"],
    "SUELDO": ["mn_suel","va_suel"],
    "PROPIETARIO": ["in_propi"],
    "CARGOPUBLICO": ["in_carg_publ"],
    "CANTIDADEMPLEADOS": ["nu_empl"],
    "CODIGOCOCUPACION": ["co_inst","co_ocup"],
    "CODIGOACTIVIDAD": ["co_acti"],
    "CODIGOACTIVIDADDETALLE": ["co_acti_deta"],
    "CODIGOSECTORECONOMICO": ["co_sect","co_sect_econ"],
    "CODIGOTIPOPERSONA": ["ti_pers"],
    "CODIGOESTATUSPERSONA": ["st_prvd","st_pers","es_pers","ce_estado"],
    "CORREO": ["tx_mail","tx_corr","tx_email"],
    "MAIL": ["tx_mail","tx_corr","tx_email"],
    "EMAIL": ["tx_mail","tx_corr","tx_email"],
    "CODIGOCUENTA": ["nu_cnta","co_cnta","nu_cuenta"],
    "TIPOIDENTIFICACIONIFINANCIERA": ["ti_iden_inst"],
    "IDENTIFICACIONIFINANCIERA": ["ci_inst","nu_ruc"],
    "NUCEMPRESABANCARIA": ["co_banc","nu_banc","ci_banco"],
    "NOMBRETITULAR": ["no_titu","no_titular"],
    "FECHAAPERTURA": ["fe_aper"],
    "CODIGOTIPOCUENTAREFERENCIA": ["ti_cnta","ti_cnta_spi"],
    "SECUENCIAREFERENCIABANCARIA": ["sc_refe","sc_regi"],
    "NUMERODIRECCION": ["sc_dire","sc_regi"],
    "SECUENCIATELEFONO": ["sc_telf","sc_regi"],
    "SECUENCIATRABAJO": ["sc_trab","sc_regi"],
    "SECUENCIAPERSONAVINCULACION": ["sc_vinc","sc_regi"],
    "CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA": ["ti_iden_vinc"],
    "IDENTIFICACIONPERSONAVINCULADA": ["ci_vinc","ci_cedu_cony","co_cony"],
    "CODIGOTIPOVINCULACION": ["ti_vinc","ti_rela"],
    "FECHAVINCULACION": ["fe_vinc","fe_rela"],
    "FECHASEPARACION": ["fe_sepa"],
    "CODIGOAREA": ["co_area"],
    "EXONERADOIMPUESTO": ["in_exon_impu"],
    "FECHACALIFICADO": ["fe_cali"],
    "CODIGOCATEGORIATRATO": ["co_cate_trat"],
    "CODIGORAZONAPERTURAFINALIDAD": ["co_razo_aper"],
    "SUJETOOBLIGADO": ["in_suje_obli"],
    "CODIGOUSUARIOOFICIALPERSONA": ["co_usua","co_ofic"],
    "NUMEROSOCIO": ["nu_soci","co_soci"],
    "SUCURSALINGRESO": ["co_sucu","sc_sucu"],
    "OFICINAINGRESO": ["co_ofic","sc_ofic"],
    "CODIGORESIDENCIA": ["co_resi"],
    "ACTIVO": ["in_acti"],
    "ESTADO": ["st_regi","es_regi","ce_estado"],
    "ESTADOREGISTRO": ["st_regi","es_regi","ce_estado"],
    "SECUENCIAREGISTRO": ["sc_regi","qs_regi"],
    "CODIGOTIPOREFERENCIA": ["co_tref"],
    "DESCRIPCIONTIPOREFERENCIA": ["ds_tref"],
}

# ===== Cargar pares activos =====
c = sql("fcme_canonicos").cursor()
c.execute("""SELECT source_table, aggregate_type_emit
             FROM dbo.cdc_table_to_types
             WHERE is_active=1 AND aggregate_type_emit IS NOT NULL""")
pairs = [(r.source_table, r.aggregate_type_emit) for r in c.fetchall()]

# Localizar BD legacy
LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
table_to_db = {}
for db in LEG_DBS:
    cc = sql(db).cursor()
    cc.execute("""SELECT t.name FROM sys.tables t
                  JOIN sys.schemas s ON t.schema_id=s.schema_id
                  WHERE s.name='dbo'""")
    for r in cc.fetchall():
        table_to_db.setdefault(r.name, db)

def get_legacy_cols(db, tbl):
    """devuelve [(name, sql_type, max_len, is_nullable, is_identity, has_default)]"""
    c = sql(db).cursor()
    c.execute("""SELECT c.name, t.name AS tp, c.max_length, c.is_nullable, c.is_identity,
                        CASE WHEN dc.object_id IS NULL THEN 0 ELSE 1 END AS has_default
                 FROM sys.columns c
                 JOIN sys.types t ON c.user_type_id=t.user_type_id
                 LEFT JOIN sys.default_constraints dc ON dc.parent_object_id=c.object_id AND dc.parent_column_id=c.column_id
                 WHERE c.object_id=OBJECT_ID(?) ORDER BY c.column_id""", f"dbo.{tbl}")
    return [(r.name, r.tp, r.max_length, r.is_nullable, r.is_identity, r.has_default) for r in c.fetchall()]

def get_legacy_pk(db, tbl):
    c = sql(db).cursor()
    c.execute("""SELECT col.name
                 FROM sys.indexes i
                 JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
                 JOIN sys.columns col ON ic.object_id=col.object_id AND ic.column_id=col.column_id
                 WHERE i.is_primary_key=1 AND i.object_id=OBJECT_ID(?)
                 ORDER BY ic.key_ordinal""", f"dbo.{tbl}")
    return [r.name for r in c.fetchall()]

def get_oracle_cols(tbl):
    co.execute("""SELECT column_name, data_type, nullable
                  FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t
                  ORDER BY column_id""", t=tbl)
    return [(r[0], r[1], r[2]) for r in co.fetchall()]

def is_snake(ora_cols):
    return all((c[0]=="ID") or "_" in c[0] for c in ora_cols)

def sql_type_clause(tp, max_len):
    """Construye la clausula T-SQL del tipo (NVARCHAR(50), SMALLINT, etc.)."""
    tp = tp.lower()
    if tp in ("varchar","nvarchar","char","nchar"):
        if max_len < 0 or max_len > 4000: return f"{tp.upper()}(MAX)" if "n" in tp else "VARCHAR(MAX)"
        actual = max_len // 2 if "n" in tp else max_len
        return f"{tp.upper()}({actual})"
    return tp.upper()

# ===== Mapeo Oracle col -> legacy col por par =====
def map_oracle_to_legacy(oracle_cols, legacy_cols, snake_mode):
    """Devuelve [(oracle_col, ora_type, legacy_col_or_None, legacy_meta)]"""
    leg_dict = {lc[0]: lc for lc in legacy_cols}
    mapped = []
    for oc, ot, on in oracle_cols:
        if oc == "ID": continue
        chosen = None

        # 1) SNAKE prefix (CODIGO_X -> ci_x)
        cand = or2lg_snake(oc)
        if cand in leg_dict:
            chosen = cand

        # 2) Diccionario dominio
        if not chosen and oc in DOMAIN_MAP:
            for c in DOMAIN_MAP[oc]:
                if c in leg_dict:
                    chosen = c
                    break

        # 3) Match exacto upper-case
        if not chosen:
            for lc_name in leg_dict:
                if lc_name.upper() == oc:
                    chosen = lc_name
                    break

        # 4) Substring fuzzy (stem legacy en oracle col)
        if not chosen:
            for lc_name in leg_dict:
                stem = lc_name.split("_",1)[1] if "_" in lc_name else lc_name
                if len(stem) >= 4 and stem.upper() in oc:
                    chosen = lc_name
                    break

        leg_meta = leg_dict.get(chosen) if chosen else None
        mapped.append((oc, ot, chosen, leg_meta))
    return mapped

# ===== Decidir source_table por type (escoge el con mas cols mapeadas) =====
type_to_best_source = {}
for src, at in pairs:
    ot = AT_TO_TABLE.get(at)
    if not ot: continue
    db = table_to_db.get(src)
    if not db: continue
    try:
        leg_cols = get_legacy_cols(db, src)
        ora_cols = get_oracle_cols(ot)
        if not ora_cols or not leg_cols: continue
        snake = is_snake(ora_cols)
        m = map_oracle_to_legacy(ora_cols, leg_cols, snake)
        n_mapped = sum(1 for _,_,lc,_ in m if lc)
        cur = type_to_best_source.get(at)
        if cur is None or n_mapped > cur[3]:
            type_to_best_source[at] = (src, db, ot, n_mapped, len(m), snake)
    except Exception as e:
        pass

print(f"types con mejor source identificado: {len(type_to_best_source)}")

# ===== Generadores =====
def gen_oracle_trigger(at, ot, ora_cols):
    """Trigger Oracle que serializa todas las cols a JSON y emite a CDC_OUTBOX."""
    # PK de Oracle: usamos ID column para el aggregate_id si no hay otra mejor
    pk_col = "ID"
    for c, _, _ in ora_cols:
        if c.upper() in ("CODIGO_CEDU","CODIGOTIPOREFERENCIA","IDENTIFICACION"):
            pk_col = c
            break

    # JSON object con todas las cols (excepto LOBs grandes? por ahora todas)
    fields = [c for c, _, _ in ora_cols]
    new_pairs = ", ".join([f"'{c}' VALUE :NEW.{c}" for c in fields])
    old_pairs = ", ".join([f"'{c}' VALUE :OLD.{c}" for c in fields])
    new_pk = f":NEW.{pk_col}"
    old_pk = f":OLD.{pk_col}"
    trg_name = f"TRG_OUTBOX_{ot[:25]}"  # truncar para Oracle 30char limit

    return f"""CREATE OR REPLACE TRIGGER FCME_USER.{trg_name}
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.{ot}
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR({new_pk});
        v_payload := JSON_OBJECT({new_pairs});
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR({new_pk});
        v_payload := JSON_OBJECT({new_pairs});
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR({old_pk});
        v_payload := JSON_OBJECT({old_pairs});
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, '{at}', v_event, v_payload, 'FCME_USER.{ot}');
END;""", trg_name

def gen_legacy_crud(src, db, ot, mapped, leg_pk, legacy_cols_all):
    """sp_<Type>_CRUD que recibe @Accion + @PK + @PayloadJSON."""
    # Dedup: si dos oracle cols mapean al mismo legacy, conservar el primero
    seen = set()
    deduped = []
    for m in mapped:
        if m[2] is None:
            deduped.append(m); continue
        if m[2] in seen: continue
        seen.add(m[2])
        deduped.append(m)
    mapped = deduped

    # PK: 1) declarada en sys 2) col que oracle col CODIGO/IDENTIFICACION mapea 3) primer mapeado
    pk_legacy = leg_pk[0] if leg_pk else None
    if pk_legacy:
        # asegurar que el PK declarado tiene un mapeo en `mapped`; si no, lo agregamos
        if not any(m[2]==pk_legacy for m in mapped):
            # intentar encontrar mapeo desde Oracle a este pk_legacy
            pass
    if not pk_legacy:
        for oc, ot_typ, lc, lc_meta in mapped:
            if lc and oc.startswith(("CODIGO_","IDENTIFICACION")):
                pk_legacy = lc; break
    if not pk_legacy:
        # primera col mapeada
        for oc, ot_typ, lc, lc_meta in mapped:
            if lc:
                pk_legacy = lc; break
    if not pk_legacy:
        return None, None, None, None
    # tipo SQL del PK: del legacy (no del mapped si pk no esta mapeado)
    pk_meta_full = next((lc for lc in legacy_cols_all if lc[0]==pk_legacy), None)
    if not pk_meta_full: return None, None, None, None
    pk_tp = sql_type_clause(pk_meta_full[1], pk_meta_full[2])

    # Cols mapeadas para INSERT/UPDATE (excluir identity y la PK del SET)
    work_cols = [m for m in mapped if m[2] is not None and not m[3][4]]  # not identity
    # tambien excluir si la legacy col es identity (no se asigna)
    col_names = [m[2] for m in work_cols]
    insert_cols = ", ".join(f"[{n}]" for n in col_names)

    def jv(oc, lc_meta):
        """JSON_VALUE casted al tipo correcto."""
        tp = lc_meta[1].lower()
        if tp in ("smallint","int","bigint","tinyint"):
            return f"TRY_CAST(JSON_VALUE(@PayloadJSON,'$.{oc}') AS {tp.upper()})"
        if tp in ("decimal","numeric","money","float","real"):
            return f"TRY_CAST(JSON_VALUE(@PayloadJSON,'$.{oc}') AS DECIMAL(18,6))"
        if tp in ("bit",):
            return f"TRY_CAST(JSON_VALUE(@PayloadJSON,'$.{oc}') AS BIT)"
        if tp in ("date","datetime","datetime2","smalldatetime"):
            return f"TRY_CAST(JSON_VALUE(@PayloadJSON,'$.{oc}') AS DATETIME2)"
        return f"JSON_VALUE(@PayloadJSON,'$.{oc}')"

    insert_vals_list = []
    for oc, ot_typ, lc, lc_meta in work_cols:
        v = jv(oc, lc_meta)
        # NOT NULL sin default y sin mapeo: ISNULL fallback
        if not lc_meta[3] and not lc_meta[5]:
            tp = lc_meta[1].lower()
            if tp in ("smallint","int","bigint","tinyint","bit","decimal","numeric","money","float","real"):
                v = f"ISNULL({v}, 0)"
            elif tp in ("date","datetime","datetime2","smalldatetime"):
                v = f"ISNULL({v}, '1900-01-01')"
            else:
                v = f"ISNULL({v}, '')"
        insert_vals_list.append(v)
    insert_vals = ", ".join(insert_vals_list)

    update_set_list = []
    for oc, ot_typ, lc, lc_meta in work_cols:
        if lc == pk_legacy: continue
        v = jv(oc, lc_meta)
        if not lc_meta[3] and not lc_meta[5]:
            tp = lc_meta[1].lower()
            if tp in ("smallint","int","bigint","tinyint","bit","decimal","numeric","money","float","real"):
                v = f"COALESCE({v}, [{lc}])"
            elif tp in ("date","datetime","datetime2","smalldatetime"):
                v = f"COALESCE({v}, [{lc}])"
            else:
                v = f"COALESCE({v}, [{lc}])"
        update_set_list.append(f"[{lc}] = {v}")
    update_set = ", ".join(update_set_list) if update_set_list else f"[{pk_legacy}]=[{pk_legacy}]"

    sp_name = f"sp_{ot}_CRUD"
    body = f"""CREATE OR ALTER PROCEDURE dbo.{sp_name}
    @Accion CHAR(1),
    @PK {pk_tp},
    @PayloadJSON NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        IF @Accion = 'I'
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM dbo.[{src}] WHERE [{pk_legacy}]=@PK)
                INSERT INTO dbo.[{src}] ({insert_cols}) VALUES ({insert_vals});
            ELSE
                UPDATE dbo.[{src}] SET {update_set} WHERE [{pk_legacy}]=@PK;
        END
        ELSE IF @Accion = 'U'
        BEGIN
            IF EXISTS (SELECT 1 FROM dbo.[{src}] WHERE [{pk_legacy}]=@PK)
                UPDATE dbo.[{src}] SET {update_set} WHERE [{pk_legacy}]=@PK;
            ELSE
                INSERT INTO dbo.[{src}] ({insert_cols}) VALUES ({insert_vals});
        END
        ELSE IF @Accion = 'D'
            DELETE FROM dbo.[{src}] WHERE [{pk_legacy}]=@PK;
    END TRY
    BEGIN CATCH
        EXEC sp_set_session_context N'is_replicating', 0;
        THROW;
    END CATCH
    EXEC sp_set_session_context N'is_replicating', 0;
END"""
    return body, sp_name, pk_tp, pk_legacy

def gen_wrapper(at, db, sp_name, pk_tp, pk_legacy_oc, mapped):
    """Wrapper canonicos: extrae PK del JSON y llama el CRUD cross-DB."""
    # buscar el oracle col que mapea al pk_legacy
    pk_oracle = next((oc for oc,_,lc,_ in mapped if lc == pk_legacy_oc), None)
    if not pk_oracle: return None, None

    cast_pk = ""
    if pk_tp.startswith(("INT","SMALLINT","BIGINT","TINYINT","DECIMAL","NUMERIC","BIT")):
        cast_pk = f"CAST(JSON_VALUE(@payload,'$.{pk_oracle}') AS {pk_tp})"
    else:
        cast_pk = f"CAST(JSON_VALUE(@payload,'$.{pk_oracle}') AS {pk_tp})"

    wrap_name = f"usp_inbox_{at}"
    body = f"""CREATE OR ALTER PROCEDURE dbo.{wrap_name}
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @pk_str NVARCHAR(200) = JSON_VALUE(@payload,'$.{pk_oracle}');
        IF @pk_str IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin {pk_oracle}');
            RETURN;
        END
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @pk {pk_tp} = TRY_CAST(@pk_str AS {pk_tp});
        IF @pk IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'PK no convertible: ' + @pk_str);
            RETURN;
        END
        EXEC {db}.dbo.{sp_name} @Accion=@accion, @PK=@pk, @PayloadJSON=@payload;
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper {at}: ' + ERROR_MESSAGE());
    END CATCH
END"""
    return body, wrap_name

# ===== Procesar y desplegar =====
deployed = []
skipped = []

for at, info in sorted(type_to_best_source.items()):
    src, db, ot, n_mapped, n_total, snake = info
    if at == "referenciaParticipeType":
        continue  # ya desplegado en piloto
    leg_cols = get_legacy_cols(db, src)
    ora_cols = get_oracle_cols(ot)
    if n_mapped < 1:
        skipped.append((at, src, "0 cols mapeadas"))
        continue
    leg_pk = get_legacy_pk(db, src)
    mapped = map_oracle_to_legacy(ora_cols, leg_cols, snake)

    # Generar CRUD
    crud_res = gen_legacy_crud(src, db, ot, mapped, leg_pk, leg_cols)
    if crud_res[0] is None:
        skipped.append((at, src, "no PK generable"))
        continue
    crud_body, crud_name, pk_tp, pk_legacy = crud_res

    # Wrapper
    wrap_res = gen_wrapper(at, db, crud_name, pk_tp, pk_legacy, mapped)
    if wrap_res[0] is None:
        skipped.append((at, src, "wrapper sin PK Oracle"))
        continue
    wrap_body, wrap_name = wrap_res

    # Trigger Oracle
    trg_body, trg_name = gen_oracle_trigger(at, ot, ora_cols)

    # Deploy CRUD
    try:
        c_db = sql(db).cursor()
        c_db.execute(crud_body)
    except Exception as e:
        skipped.append((at, src, f"CRUD fail: {str(e)[:120]}"))
        continue

    # Deploy wrapper
    try:
        c_can = sql("fcme_canonicos").cursor()
        c_can.execute(wrap_body)
    except Exception as e:
        skipped.append((at, src, f"wrapper fail: {str(e)[:120]}"))
        continue

    # Deploy trigger Oracle
    try:
        co.execute(trg_body)
        co.execute(f"SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='{trg_name}' AND object_type='TRIGGER'")
        st = co.fetchone()
        if not st or st[0] != "VALID":
            skipped.append((at, src, f"trigger oracle status={st}"))
            continue
    except Exception as e:
        skipped.append((at, src, f"trigger Oracle fail: {str(e)[:200]}"))
        continue

    # Module config
    c_can.execute("DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type=?", at)
    c_can.execute("""INSERT INTO dbo.cdc_inbox_module_config
                     (aggregate_type, sp_name, target_db, module_name, active)
                     VALUES (?, ?, ?, 'PARTICIPE', 1)""", at, f"dbo.{wrap_name}", db)

    deployed.append((at, src, db, ot, n_mapped, n_total))

print(f"\n=== DEPLOY DONE ===")
print(f"Desplegados: {len(deployed)}")
for at, src, db, ot, nm, nt in deployed[:30]:
    print(f"  {at:<35} {db}.{src:<30} -> {ot:<35} ({nm}/{nt})")
print(f"\nSkipped: {len(skipped)}")
for at, src, why in skipped[:20]:
    print(f"  {at:<35} {src:<30} : {why}")

# Resumen module_config
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE active=1")
print(f"\ncdc_inbox_module_config activos: {c.fetchone()[0]}")
c.execute("SELECT TOP 50 aggregate_type, sp_name, target_db FROM dbo.cdc_inbox_module_config WHERE active=1 ORDER BY aggregate_type")
for r in c.fetchall():
    print(f"  {r.aggregate_type:<35} -> {r.target_db}.{r.sp_name}")

orcl.close()
print("\n=== PASO 7 OK ===")
