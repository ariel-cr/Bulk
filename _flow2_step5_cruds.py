"""Ultimo paso: crear sp_<TYPE>_CRUD (con @Accion + params explicitos por columna)
en cada BD legacy, y actualizar los 30 wrappers para que llamen el CRUD.

Estrategia anti-bug del paso 7:
 - TODAS las cols legacy NOT NULL aparecen en el INSERT.
 - Cols mapeadas: parametro recibido del wrapper (TRY_CAST a tipo correcto).
 - Cols NOT NULL sin mapeo: default razonable por tipo (0, '', '1900-01-01').
 - Cols nullable sin mapeo: NULL.
"""
import pyodbc, oracledb
from collections import defaultdict

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

AT_TO_TABLE = {
    "actualizacionAfiliadoType":"ACTUALIZACION_AFILIADO_TYPE",
    "actualizacionDocumentosType":"ACTUALIZACION_DOCUMENTOS_TYPE",
    "agendaMailAfiliadoType":"AGENDAMAILAFILIADO_TYPE",
    "auditoriaAfiliadoType":"AUDITORIAAFILIADO_TYPE",
    "beneficiarioParticipeType":"BENEFICIARIOPARTICIPE_TYPE",
    "cuentaBancariaAfiliadoType":"CUENTABANCARIAAFILIADO_TYPE",
    "distribucionAfiliadoType":"DISTRIBUCIONAFILIADO_TYPE",
    "documentacionAfiliadoType":"DOCUMENTACIONAFILIADO_TYPE",
    "firmanteParticipeType":"FIRMANTEPARTICIPE_TYPE",
    "grupoFamiliarType":"GRUPOFAMILIAR_TYPE",
    "informacionAdicionalAfiliadoType":"INFORMACIONADICIONALAFILIADO_TYPE",
    "institucionType":"INSTITUCION_TYPE",
    "motivoContableType":"MOTIVOCONTABLE_TYPE",
    "movimientoCuentaType":"MOVIMIENTOCUENTA_TYPE",
    "movimientoTemporalType":"MOVIMIENTOTEMPORAL_TYPE",
    "naturalInformacionAdicionalType":"NATURALINFORMACIONADICIONALTYPE",
    "naturalIngresosEgresosType":"NATURALINGRESOSEGRESOSTYPE",
    "naturalTrabajoType":"NATURALTRABAJOTYPE",
    "personaReferenciasBancariasType":"PERSONAREFERENCIASBANCARIASTYPE",
    "personaReferenciasPersonalesType":"PERSONAREFERENCIASPERSONALESTYPE",
    "personaTelefonosType":"PERSONATELEFONOSTYPE",
    "personaVinculacionesType":"PERSONAVINCULACIONESTYPE",
    "referenciaParticipeType":"REFERENCIAPARTICIPE_TYPE",
    "reporteSIBSParticipeType":"REPORTESIBSPARTICIPE_TYPE",
    "retiroLiquidacionType":"RETIROLIQUIDACION_TYPE",
    "retiroVoluntarioEstadoType":"RETIROVOLUNTARIOESTADO_TYPE",
    "saldoDiarioRubroType":"SALDODIARIORUBRO_TYPE",
    "saldoDiarioType":"SALDODIARIO_TYPE",
    "seguroVidaParticipeType":"SEGUROVIDAPARTICIPE_TYPE",
    "servicioAdicionalType":"SERVICIOADICIONAL_TYPE",
}

PREFIX_MAP_OR2LG = {"CODIGO_":"ci_","DESCRIPCION_":"ds_","NUMERO_":"nu_","TEXTO_":"tx_",
                    "INDICADOR_":"in_","FECHA_":"fe_","TIPO_":"ti_","NOMBRE_":"no_",
                    "SECUENCIA_":"sc_","VALOR_":"va_","MONTO_":"mn_","ESTADO_":"st_"}
def or2lg_snake(oc):
    for p, r in PREFIX_MAP_OR2LG.items():
        if oc.startswith(p): return r + oc[len(p):].lower()
    return oc.lower()

DOMAIN_MAP = {
    "IDENTIFICACION":["ci_cedu","ci_cedula","ci_iden","ci_id","ci_rcfd","co_cedu"],
    "CODIGOTIPOIDENTIFICACION":["ci_tipo","ti_iden","ti_id"],
    "PRIMERAPELLIDO":["no_prim_apel","no_apellido","no_apel","no_apel_prim"],
    "SEGUNDOAPELLIDO":["no_segu_apel","no_apel_segu"],
    "PRIMERNOMBRE":["no_prim_nomb","no_nombre","no_nomb_prim","no_nomb"],
    "SEGUNDONOMBRE":["no_segu_nomb","no_nomb_segu"],
    "NOMBRELEGAL":["no_lega","no_legal","no_razo_soci","no_inst","no_banco"],
    "NOMBREPREFERIDO":["no_pref"],
    "FECHANACIMIENTO":["fe_naci","fe_nacimiento"],
    "FECHAINGRESO":["fe_ingr","fe_ingreso","fe_alta","fx_creacion"],
    "FECHAMODIFICACION":["fe_modi","fe_actu"],
    "FECHASALIDA":["fe_sali","fe_baja"],
    "FECHACREACION":["fe_crea","fx_creacion"],
    "CODIGOPAIS":["co_pais"],"CODIGOPROVINCIA":["co_prov"],"CODIGOCANTON":["co_cant"],
    "CODIGOPARROQUIA":["co_parr"],"CODIGOCIUDAD":["co_ciud","co_ciuda"],"CODIGOBARRIO":["co_barr"],
    "CALLE":["ds_call_prim","ds_call"],"DIRECCION":["ds_dire","ds_direc"],
    "NUMERO":["nu_call_prim","nu_dire"],"URBANIZACION":["ds_cdla","ds_urba"],
    "INMUEBLE":["nu_vill","nu_inmu"],"DEPARTAMENTO":["nu_dpto","co_depa"],
    "TRANSVERSAL":["ds_call_secu"],"SECTOR":["ti_sector","ti_sect"],
    "OBSERVACIONES":["ds_observaciones","ds_obse","tx_obse","ds_obser","no_cont"],
    "NUMEROTELEFONO":["tx_telf","tx_telf_conv","tx_fono","nu_tele"],
    "EMPRESAOPERADORA":["ti_oper"],"CODIGOTIPOTELEFONO":["ti_telf"],
    "EXTENSION":["nu_exte"],"CODIGOTIPODIRECCION":["ti_dire"],"CODIGOTIPOUBICACION":["ti_ubic"],
    "DIRECCIONPRINCIPAL":["in_dire_prin","in_principal"],"FECHAINGRESORESIDENCIA":["fe_resi"],
    "NOMBREPROPIETARIO":["no_propi"],"CODIGOTIPOSITIO":["ti_siti"],"CODIGOZIP5":["co_zip"],
    "NOMBREEMPLEADOR":["no_empl","no_inst"],"CODIGOCARGO":["co_carg"],
    "CODIGOCARGOPERSONA":["co_carg"],"CODIGOCODIGOCARGO":["co_carg"],
    "TIPOCONTRATO":["ti_cont"],"CODIGONIVELLABORAL":["co_nive"],
    "CODIGOCATEGORIATRABAJO":["co_cate"],"TIEMPOPARCIAL":["ti_jorn"],
    "FECHAINGRESOTRABAJO":["fe_ingr_trab","fe_ingr"],"SUELDO":["mn_suel","va_suel"],
    "PROPIETARIO":["in_propi"],"CARGOPUBLICO":["in_carg_publ"],
    "CANTIDADEMPLEADOS":["nu_empl"],"CODIGOCOCUPACION":["co_inst","co_ocup"],
    "CODIGOACTIVIDAD":["co_acti"],"CODIGOACTIVIDADDETALLE":["co_acti_deta"],
    "CODIGOSECTORECONOMICO":["co_sect","co_sect_econ"],"CODIGOTIPOPERSONA":["ti_pers"],
    "CODIGOESTATUSPERSONA":["st_prvd","st_pers","es_pers","ce_estado"],
    "CORREO":["tx_mail","tx_corr","tx_email"],"MAIL":["tx_mail","tx_corr","tx_email"],
    "EMAIL":["tx_mail","tx_corr","tx_email"],"CODIGOCUENTA":["nu_cnta","co_cnta","nu_cuenta"],
    "TIPOIDENTIFICACIONIFINANCIERA":["ti_iden_inst"],"IDENTIFICACIONIFINANCIERA":["ci_inst","nu_ruc"],
    "NUCEMPRESABANCARIA":["co_banc","nu_banc","ci_banco"],"NOMBRETITULAR":["no_titu","no_titular"],
    "FECHAAPERTURA":["fe_aper"],"CODIGOTIPOCUENTAREFERENCIA":["ti_cnta","ti_cnta_spi"],
    "SECUENCIAREFERENCIABANCARIA":["sc_refe","sc_regi"],"NUMERODIRECCION":["sc_dire","sc_regi"],
    "SECUENCIATELEFONO":["sc_telf","sc_regi"],"SECUENCIATRABAJO":["sc_trab","sc_regi"],
    "SECUENCIAPERSONAVINCULACION":["sc_vinc","sc_regi"],
    "CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA":["ti_iden_vinc"],
    "IDENTIFICACIONPERSONAVINCULADA":["ci_vinc","ci_cedu_cony","co_cony"],
    "CODIGOTIPOVINCULACION":["ti_vinc","ti_rela"],"FECHAVINCULACION":["fe_vinc","fe_rela"],
    "FECHASEPARACION":["fe_sepa"],"CODIGOAREA":["co_area"],
    "EXONERADOIMPUESTO":["in_exon_impu"],"FECHACALIFICADO":["fe_cali"],
    "CODIGOCATEGORIATRATO":["co_cate_trat"],"CODIGORAZONAPERTURAFINALIDAD":["co_razo_aper"],
    "SUJETOOBLIGADO":["in_suje_obli"],"CODIGOUSUARIOOFICIALPERSONA":["co_usua","co_ofic"],
    "NUMEROSOCIO":["nu_soci","co_soci"],"SUCURSALINGRESO":["co_sucu","sc_sucu"],
    "OFICINAINGRESO":["co_ofic","sc_ofic"],"CODIGORESIDENCIA":["co_resi"],
    "ACTIVO":["in_acti"],"ESTADO":["st_regi","es_regi","ce_estado"],
    "ESTADOREGISTRO":["st_regi","es_regi","ce_estado"],"SECUENCIAREGISTRO":["sc_regi","qs_regi"],
    "CODIGOTIPOREFERENCIA":["co_tref"],"DESCRIPCIONTIPOREFERENCIA":["ds_tref"],
    "CODIGOEMPRESA":["co_empr"],"CODIGOFONDO":["co_fond"],"CODIGOMOTIVOCONTABLE":["co_moti","ci_moti"],
    "DESCRIPCIONMOTIVOCONTABLE":["ds_moti"],"CODIGOMODULO":["co_modu"],
    "USUARIOINGRESA":["co_usua_ingr","ci_usua_ingr"],"USUARIOMODIFICA":["co_usua_modi","ci_usua_modi"],
}

# ==== Helpers ====
def get_legacy_cols(db, tbl):
    c = sql(db).cursor()
    c.execute("""SELECT c.name, t.name AS tp, c.max_length, c.is_nullable, c.is_identity,
                   CASE WHEN dc.object_id IS NULL THEN 0 ELSE 1 END AS has_default
                 FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
                 LEFT JOIN sys.default_constraints dc ON dc.parent_object_id=c.object_id AND dc.parent_column_id=c.column_id
                 WHERE c.object_id=OBJECT_ID(?) ORDER BY c.column_id""", f"dbo.{tbl}")
    return [(r.name, r.tp, r.max_length, r.is_nullable, r.is_identity, r.has_default) for r in c.fetchall()]

def get_legacy_pk(db, tbl):
    c = sql(db).cursor()
    c.execute("""SELECT col.name FROM sys.indexes i
                 JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
                 JOIN sys.columns col ON ic.object_id=col.object_id AND ic.column_id=col.column_id
                 WHERE i.is_primary_key=1 AND i.object_id=OBJECT_ID(?)
                 ORDER BY ic.key_ordinal""", f"dbo.{tbl}")
    return [r.name for r in c.fetchall()]

def get_oracle_cols(tbl):
    co.execute("""SELECT column_name, data_type FROM all_tab_columns
                  WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id""", t=tbl)
    return [(r[0], r[1]) for r in co.fetchall()]

def sql_type_clause(tp, max_len):
    tp = tp.lower()
    if tp in ("varchar","nvarchar","char","nchar"):
        if max_len < 0 or max_len > 4000: return f"{tp.upper()}(MAX)"
        actual = max_len // 2 if "n" in tp else max_len
        return f"{tp.upper()}({actual})"
    return tp.upper()

def default_for(tp):
    tp = tp.lower()
    if tp in ("smallint","int","bigint","tinyint","bit"): return "0"
    if tp in ("decimal","numeric","money","float","real"): return "0"
    if tp in ("date","datetime","datetime2","smalldatetime"): return "'1900-01-01'"
    return "''"

def map_oracle_to_legacy(oracle_cols, legacy_cols_meta):
    leg_dict = {lc[0]: lc for lc in legacy_cols_meta}
    mapped = []
    for oc, ot in oracle_cols:
        if oc == "ID": continue
        chosen = None
        cand = or2lg_snake(oc)
        if cand in leg_dict: chosen = cand
        if not chosen and oc in DOMAIN_MAP:
            for c in DOMAIN_MAP[oc]:
                if c in leg_dict: chosen = c; break
        if not chosen:
            for lc_name in leg_dict:
                if lc_name.upper() == oc: chosen = lc_name; break
        if not chosen:
            for lc_name in leg_dict:
                stem = lc_name.split("_",1)[1] if "_" in lc_name else lc_name
                if len(stem) >= 4 and stem.upper() in oc:
                    chosen = lc_name; break
        mapped.append((oc, chosen, leg_dict.get(chosen) if chosen else None))
    return mapped

# ==== Cargar pares activos ====
print("="*70)
print("[1] Cargar pares (aggregate_type, source_table)")
print("="*70)
c_can = sql("fcme_canonicos").cursor()
c_can.execute("""SELECT source_table, aggregate_type_emit FROM dbo.cdc_table_to_types
                 WHERE is_active=1 AND aggregate_type_emit IS NOT NULL""")
all_pairs = [(r.source_table, r.aggregate_type_emit) for r in c_can.fetchall()]

# Localizar BD legacy de cada source_table
LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
table_to_db = {}
for db in LEG_DBS:
    cc = sql(db).cursor()
    cc.execute("""SELECT t.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo'""")
    for r in cc.fetchall():
        table_to_db.setdefault(r.name, db)

# Para cada aggregate_type, escoger el source con mas cols mapeadas
type_best = {}
for src, at in all_pairs:
    ot = AT_TO_TABLE.get(at)
    if not ot: continue
    db = table_to_db.get(src)
    if not db: continue
    try:
        leg_meta = get_legacy_cols(db, src)
        ora = get_oracle_cols(ot)
        if not leg_meta or not ora: continue
        mapped = map_oracle_to_legacy(ora, leg_meta)
        n_mapped = sum(1 for _,lc,_ in mapped if lc)
        if at not in type_best or n_mapped > type_best[at][3]:
            type_best[at] = (src, db, ot, n_mapped, len(mapped))
    except: pass
print(f"  types con source asignado: {len(type_best)}")

# ==== Generar y desplegar ====
print("\n" + "="*70)
print("[2] Generar CRUDs y wrappers")
print("="*70)

deployed = []
skipped = []

for at in sorted(type_best.keys()):
    src, db, ot, _, _ = type_best[at]
    leg_meta = get_legacy_cols(db, src)
    leg_pk = get_legacy_pk(db, src)
    ora = get_oracle_cols(ot)
    mapped = map_oracle_to_legacy(ora, leg_meta)

    # Dedup: si dos oracle cols mapean al mismo legacy, quedarnos con la primera
    seen_leg = set()
    deduped = []
    for oc, lc, lc_meta in mapped:
        if lc and lc in seen_leg: deduped.append((oc, None, None)); continue
        if lc: seen_leg.add(lc)
        deduped.append((oc, lc, lc_meta))
    mapped = deduped
    leg_to_oracle = {lc: (oc, lc_meta) for oc, lc, lc_meta in mapped if lc}

    # PK legacy: 1) primera col del PK declarado 2) primera CODIGO_*/IDENTIFICACION mapeada
    pk_legacy = leg_pk[0] if leg_pk else None
    if not pk_legacy:
        for oc, lc, lc_meta in mapped:
            if lc and oc.startswith(("CODIGO","IDENTIFICACION")):
                pk_legacy = lc; break
    if not pk_legacy and mapped:
        pk_legacy = next((lc for _, lc, _ in mapped if lc), None)
    if not pk_legacy:
        skipped.append((at, src, "no PK legacy")); continue
    pk_meta = next((lcm for lcm in leg_meta if lcm[0]==pk_legacy), None)
    pk_tp = sql_type_clause(pk_meta[1], pk_meta[2])

    # Identificar oracle col que mapea al PK (para pasarlo desde wrapper)
    pk_oracle = None
    for oc, lc, _ in mapped:
        if lc == pk_legacy: pk_oracle = oc; break
    if not pk_oracle:
        # PK legacy no fue mapeada por Oracle -> intentar via DOMAIN_MAP inverso
        for ora_field, candidates in DOMAIN_MAP.items():
            if pk_legacy in candidates and ora_field in [oc for oc,_ in ora]:
                pk_oracle = ora_field; break
    if not pk_oracle:
        skipped.append((at, src, f"PK legacy {pk_legacy} sin mapeo Oracle")); continue

    # Construir parametros del CRUD: TODAS las cols legacy not identity, not pk, included
    sp_name_crud = f"sp_{ot}_CRUD"
    params = [f"    @Accion CHAR(1)", f"    @PK {pk_tp}"]
    insert_cols = []
    insert_vals = []
    update_sets = []

    for lc_name, lc_tp, lc_max, lc_null, lc_id, lc_def in leg_meta:
        if lc_id: continue
        if lc_name == pk_legacy: continue
        param = f"@{lc_name}"
        tp_clause = sql_type_clause(lc_tp, lc_max)
        # default: si tiene mapeo Oracle, default=NULL (wrapper la pasa);
        # si NOT NULL sin default y sin mapeo, usar default por tipo
        is_mapped = lc_name in leg_to_oracle
        if not is_mapped and not lc_null and not lc_def:
            default = default_for(lc_tp)
        else:
            default = "NULL"
        params.append(f"    {param} {tp_clause} = {default}")
        insert_cols.append(f"[{lc_name}]")
        # value: si NOT NULL y param es NULL, usar default; si tiene mapeo, usar param
        if not lc_null and not lc_def:
            insert_vals.append(f"ISNULL({param}, {default_for(lc_tp)})")
            update_sets.append(f"[{lc_name}] = COALESCE({param}, [{lc_name}])")
        else:
            insert_vals.append(param)
            update_sets.append(f"[{lc_name}] = COALESCE({param}, [{lc_name}])")

    insert_cols_str = ", ".join(insert_cols)
    insert_vals_str = ", ".join(insert_vals)
    update_set_str = ", ".join(update_sets) if update_sets else f"[{pk_legacy}]=[{pk_legacy}]"

    crud_body = f"""CREATE OR ALTER PROCEDURE dbo.{sp_name_crud}
{",\n".join(params)}
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        IF @Accion = 'I'
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM dbo.[{src}] WHERE [{pk_legacy}]=@PK)
                INSERT INTO dbo.[{src}] ([{pk_legacy}], {insert_cols_str}) VALUES (@PK, {insert_vals_str});
            ELSE
                UPDATE dbo.[{src}] SET {update_set_str} WHERE [{pk_legacy}]=@PK;
        END
        ELSE IF @Accion = 'U'
        BEGIN
            IF EXISTS (SELECT 1 FROM dbo.[{src}] WHERE [{pk_legacy}]=@PK)
                UPDATE dbo.[{src}] SET {update_set_str} WHERE [{pk_legacy}]=@PK;
            ELSE
                INSERT INTO dbo.[{src}] ([{pk_legacy}], {insert_cols_str}) VALUES (@PK, {insert_vals_str});
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

    # Wrapper: extrae cada campo del JSON y los pasa como params al CRUD
    wrap_name = f"usp_inbox_{at}"
    pk_cast = ""
    if pk_tp.startswith(("INT","SMALLINT","BIGINT","TINYINT","DECIMAL","NUMERIC","BIT")):
        pk_cast = f"TRY_CAST(JSON_VALUE(@payload,'$.{pk_oracle}') AS {pk_tp})"
    elif pk_tp.startswith(("DATE","DATETIME","SMALLDATETIME")):
        pk_cast = f"TRY_CAST(JSON_VALUE(@payload,'$.{pk_oracle}') AS {pk_tp})"
    else:
        pk_cast = f"CAST(JSON_VALUE(@payload,'$.{pk_oracle}') AS {pk_tp})"

    # Construir DECLARE @v_<col> + EXEC @col=@v_<col> (T-SQL no acepta expresiones en EXEC params)
    wrapper_decls = []
    wrapper_call_args = []
    seen_vars = set()
    for oc, lc, lc_meta in mapped:
        if not lc or lc == pk_legacy: continue
        if lc in seen_vars: continue
        seen_vars.add(lc)
        tp_clause = sql_type_clause(lc_meta[1], lc_meta[2])
        tp_low = lc_meta[1].lower()
        if tp_low in ("smallint","int","bigint","tinyint","bit"):
            val_expr = f"TRY_CAST(JSON_VALUE(@payload,'$.{oc}') AS {tp_clause})"
        elif tp_low in ("decimal","numeric","money","float","real"):
            val_expr = f"TRY_CAST(JSON_VALUE(@payload,'$.{oc}') AS DECIMAL(18,6))"
        elif tp_low in ("date","datetime","datetime2","smalldatetime"):
            val_expr = f"TRY_CAST(JSON_VALUE(@payload,'$.{oc}') AS {tp_clause})"
        else:
            val_expr = f"JSON_VALUE(@payload,'$.{oc}')"
        wrapper_decls.append(f"        DECLARE @v_{lc} {tp_clause} = {val_expr};")
        wrapper_call_args.append(f"            @{lc} = @v_{lc}")

    decls_block = "\n".join(wrapper_decls) if wrapper_decls else ""
    call_args_str = ",\n".join(wrapper_call_args) if wrapper_call_args else ""
    full_call = f",\n{call_args_str}" if call_args_str else ""

    wrap_body = f"""CREATE OR ALTER PROCEDURE dbo.{wrap_name}
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
        DECLARE @pk {pk_tp} = {pk_cast};
        IF @pk IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'PK no convertible: ' + @pk_str);
            RETURN;
        END

{decls_block}

        -- Audit
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @pk_str, @pk_str);

        EXEC {db}.dbo.{sp_name_crud}
            @Accion = @accion,
            @PK = @pk{full_call};
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper {at}: ' + ERROR_MESSAGE());
    END CATCH
END"""

    # Deploy
    try:
        c_db = sql(db).cursor()
        c_db.execute(crud_body)
    except Exception as e:
        with open(rf"C:\Users\Usuario\Downloads\Bulk\_debug_crud_{at}.sql","w",encoding="utf-8") as f: f.write(crud_body)
        skipped.append((at, src, f"CRUD: {str(e)[:120]}")); continue
    try:
        c_can.execute(wrap_body)
    except Exception as e:
        with open(rf"C:\Users\Usuario\Downloads\Bulk\_debug_wrap_{at}.sql","w",encoding="utf-8") as f: f.write(wrap_body)
        skipped.append((at, src, f"wrapper: {str(e)[:120]}")); continue

    deployed.append((at, src, db, ot, sp_name_crud, wrap_name))

print(f"  desplegados (CRUD + wrapper): {len(deployed)}")
print(f"  skipped: {len(skipped)}")
for at, src, why in skipped[:30]:
    print(f"    - {at} ({src}): {why}")

# ==== Update module_config ====
print("\n" + "="*70)
print("[3] Refrescar module_config")
print("="*70)
for at, src, db, ot, sp_crud, wrap_name in deployed:
    c_can.execute("""UPDATE dbo.cdc_inbox_module_config
                     SET sp_name=?, target_db=?, active=1, updated_at=SYSUTCDATETIME()
                     WHERE aggregate_type=?""", f"dbo.{wrap_name}", db, at)
c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE active=1")
print(f"  active rows: {c_can.fetchone()[0]}")

# ==== Canary test: 1 type confiable ====
print("\n" + "="*70)
print("[4] Canary test: referenciaParticipeType")
print("="*70)
c_can.execute("DELETE FROM dbo.cdc_inbox")
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")
c_can.execute("DELETE FROM dbo.cdc_inbox_parsed")
c_fc = sql("dbFC").cursor()
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 1")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref=88")
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 0")

c_can.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
                 VALUES ('88','referenciaParticipeType','INSERT',
                         '{"CODIGOTIPOREFERENCIA":"88","DESCRIPCIONTIPOREFERENCIA":"CRUD test"}',
                         'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")
c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref=88")
for r in c_fc.fetchall(): print(f"  legacy: co_tref={r.co_tref} ds_tref={r.ds_tref}")
c_can.execute("SELECT inbox_id, error_message FROM dbo.cdc_inbox_errors")
errs = c_can.fetchall()
if errs:
    for r in errs: print(f"  ERR: {r.error_message[:200]}")
else:
    print(f"  0 errores")

# Cleanup
c_can.execute("DELETE FROM dbo.cdc_inbox")
c_can.execute("DELETE FROM dbo.cdc_inbox_errors")
c_can.execute("DELETE FROM dbo.cdc_inbox_parsed")
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 1")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref=88")
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 0")

orcl.close()
print("\n=== Paso 5 OK: CRUDs y wrappers desplegados ===")
