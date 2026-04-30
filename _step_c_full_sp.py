"""Paso C: Genera USP_INBOX_PARTICIPES con TODOS los types funcionando.

Estrategia de mapeo:
1) SNAKE_CASE: prefijo ci_/co_/ds_/etc -> CODIGO_/DESCRIPCION_/etc.
2) Diccionario de dominio (sinonimos legacy -> oracle CAMEL).
3) Fuzzy: substring match.
4) Lo no matcheado queda NULL.

Caso especial: PERSONATELEFONOSTYPE explota multi-row (CONV/CEL/CON1/CON2).
"""
import pyodbc, oracledb, re, json
from collections import defaultdict

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# ============ MAPEO Type canonico -> tabla Oracle ============
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

PREFIX_MAP = {
    "ci_": "CODIGO_", "co_": "CODIGO_", "ds_": "DESCRIPCION_",
    "nu_": "NUMERO_", "tx_": "TEXTO_", "in_": "INDICADOR_",
    "fx_": "FECHA_", "fe_": "FECHA_",
    "ti_": "TIPO_", "no_": "NOMBRE_",
    "qs_": "SECUENCIA_", "sc_": "SECUENCIA_",
    "va_": "VALOR_", "mn_": "MONTO_", "es_": "ESTADO_",
    "st_": "ESTADO_",
}
def lg2or_snake(col):
    for p, r in PREFIX_MAP.items():
        if col.lower().startswith(p):
            return r + col[len(p):].upper()
    return col.upper()

# ============ Diccionario dominio (CAMEL Oracle -> [legacy candidates]) ============
DOMAIN_MAP = {
    "IDENTIFICACION": ["ci_cedu","ci_cedula","ci_iden","ci_id","ci_rcfd"],
    "CODIGOTIPOIDENTIFICACION": ["ci_tipo","ti_iden","ti_id"],
    "PRIMERAPELLIDO": ["no_prim_apel","no_apellido","no_apel","no_apel_prim"],
    "SEGUNDOAPELLIDO": ["no_segu_apel","no_apel_segu"],
    "PRIMERNOMBRE": ["no_prim_nomb","no_nombre","no_nomb_prim","no_nomb"],
    "SEGUNDONOMBRE": ["no_segu_nomb","no_nomb_segu"],
    "NOMBRELEGAL": ["no_lega","no_legal","no_razo_soci"],
    "NOMBREPREFERIDO": ["no_pref"],
    "FECHANACIMIENTO": ["fe_naci","fe_nacimiento"],
    "FECHAINGRESO": ["fe_ingr","fe_ingreso","fe_alta"],
    "FECHAMODIFICACION": ["fe_modi","fe_actu"],
    "FECHASALIDA": ["fe_sali","fe_baja"],
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
    "OBSERVACIONES": ["ds_observaciones","ds_obse","tx_obse","ds_obser"],
    "NUMEROTELEFONO": ["tx_telf","tx_telf_conv","tx_fono"],
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
    "CODIGOESTATUSPERSONA": ["st_prvd","st_pers","es_pers"],
    "CORREO": ["tx_mail","tx_corr","tx_email"],
    "MAIL": ["tx_mail","tx_corr","tx_email"],
    "EMAIL": ["tx_mail","tx_corr","tx_email"],
    "CODIGOCUENTA": ["nu_cnta","co_cnta","nu_cuenta"],
    "TIPOIDENTIFICACIONIFINANCIERA": ["ti_iden_inst"],
    "IDENTIFICACIONIFINANCIERA": ["ci_inst"],
    "NUCEMPRESABANCARIA": ["co_banc","nu_banc"],
    "NOMBRETITULAR": ["no_titu","no_titular"],
    "FECHAAPERTURA": ["fe_aper"],
    "CODIGOTIPOCUENTAREFERENCIA": ["ti_cnta"],
    "SECUENCIAREFERENCIABANCARIA": ["sc_refe","sc_regi"],
    "NUMERODIRECCION": ["sc_dire","sc_regi"],
    "SECUENCIATELEFONO": ["sc_telf","sc_regi"],
    "SECUENCIATRABAJO": ["sc_trab","sc_regi"],
    "SECUENCIAPERSONAVINCULACION": ["sc_vinc","sc_regi"],
    "CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA": ["ti_iden_vinc"],
    "IDENTIFICACIONPERSONAVINCULADA": ["ci_vinc","ci_cony"],
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
    "ESTADO": ["st_regi","es_regi"],
    "SECUENCIAREGISTRO": ["sc_regi","qs_regi"],
}

def get_legacy_cols(db, tbl):
    c = sql(db).cursor()
    c.execute("""SELECT name FROM sys.columns WHERE object_id=OBJECT_ID(?) ORDER BY column_id""",
              f"dbo.{tbl}")
    return [r.name for r in c.fetchall()]

def get_oracle_cols(tbl):
    co.execute("""SELECT column_name FROM all_tab_columns
                  WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id""", t=tbl)
    return [r[0] for r in co.fetchall()]

def is_snake(cols):
    return any("_" in c and c != "ID" for c in cols)

def map_columns(legacy_cols, oracle_cols, snake_mode):
    """Devuelve [(oracle_col, legacy_col_or_None)] excluyendo ID."""
    legacy_set = set(legacy_cols)
    mapped = []
    for oc in oracle_cols:
        if oc == "ID":
            continue
        chosen = None

        # 1) Si Oracle es SNAKE_CASE: regla prefijo
        if snake_mode and "_" in oc:
            for lc in legacy_cols:
                if lg2or_snake(lc) == oc:
                    chosen = lc
                    break

        # 2) Diccionario dominio
        if not chosen and oc in DOMAIN_MAP:
            for cand in DOMAIN_MAP[oc]:
                if cand in legacy_set:
                    chosen = cand
                    break

        # 3) Substring exact (oracle col contiene legacy col upper sin prefijo)
        if not chosen:
            for lc in legacy_cols:
                stem = lc.split("_",1)[1] if "_" in lc else lc
                if len(stem) >= 4 and stem.upper() in oc:
                    chosen = lc
                    break

        mapped.append((oc, chosen))
    return mapped

def detect_pk(mapped, prefer=("IDENTIFICACION","CODIGO_CEDU","CODIGO_CEDULA","CODIGO_IDEN")):
    """Detecta PK Oracle prefiriendo IDENTIFICACION; si no, primer CODIGO_*."""
    for ocname in prefer:
        for oc, lc in mapped:
            if oc == ocname and lc:
                return oc, lc
    for oc, lc in mapped:
        if oc.startswith("CODIGO_") and lc:
            return oc, lc
    for oc, lc in mapped:
        if oc.startswith("IDENTIFICACION") and lc:
            return oc, lc
    for oc, lc in mapped:
        if lc:
            return oc, lc
    return None, None

def detect_secuencia(mapped):
    """Si Oracle tiene SECUENCIA*, devuelve (oracle, legacy_or_None)."""
    for oc, lc in mapped:
        if oc.startswith("SECUENCIA") or oc == "NUMERODIRECCION":
            return oc, lc
    return None, None

def emit_branch(src_tbl, at, ot, mapped, snake_mode):
    pk_o, pk_l = detect_pk(mapped)
    if not pk_o:
        return None, "no PK"

    sec_o, sec_l = detect_secuencia(mapped)

    cols_csv = ", ".join(oc for oc,_ in mapped)
    vals_csv = ", ".join(
        f"JSON_VALUE(p_payload,'$.{lc}')" if lc else "NULL"
        for _, lc in mapped
    )
    skip_set = {pk_o}
    if sec_o: skip_set.add(sec_o)
    update_set = ", ".join(
        f"{oc} = " + (f"JSON_VALUE(p_payload,'$.{lc}')" if lc else "NULL")
        for oc, lc in mapped if oc not in skip_set
    )

    if sec_o and sec_l:
        # MERGE con PK compuesto pk + secuencia
        on_clause = f"t.{pk_o} = s.k AND t.{sec_o} = s.sec"
        using_clause = f"SELECT v_pk AS k, JSON_VALUE(p_payload,'$.{sec_l}') AS sec FROM dual"
    elif sec_o and not sec_l:
        # Secuencia constante '1'
        on_clause = f"t.{pk_o} = s.k AND t.{sec_o} = s.sec"
        using_clause = "SELECT v_pk AS k, '1' AS sec FROM dual"
    else:
        on_clause = f"t.{pk_o} = s.k"
        using_clause = "SELECT v_pk AS k FROM dual"

    cond = f"p_aggregate_type = '{at}' AND p_source_table = '{src_tbl}'"

    branch = f"""
    -- {at}  <-  {src_tbl}  ->  {ot}
    IF {cond} THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.{pk_l}');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM {ot} WHERE {pk_o} = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO {ot} t
                USING ({using_clause}) s
                ON ({on_clause})
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({cols_csv}) VALUES ({vals_csv});
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type, '{ot}: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;
"""
    return branch, None

# Especial: PERSONATELEFONOSTYPE multi-row para fctbafil_actu
SPECIAL_TELEFONOS = """
    -- personaTelefonosType  <-  fctbafil_actu  ->  PERSONATELEFONOSTYPE  (multi-row)
    IF p_aggregate_type = 'personaTelefonosType' AND p_source_table = 'fctbafil_actu' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload, '$.ci_cedu');
            v_tipo := JSON_VALUE(p_payload, '$.ci_tipo');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM PERSONATELEFONOSTYPE WHERE IDENTIFICACION = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                FOR f IN (
                    SELECT '1' AS sec, 'CONV' AS tip, JSON_VALUE(p_payload,'$.tx_telf_conv') AS num FROM dual
                    UNION ALL SELECT '2','CEL', JSON_VALUE(p_payload,'$.tx_telf_celu') FROM dual
                    UNION ALL SELECT '3','CON1', JSON_VALUE(p_payload,'$.tx_telf_con1') FROM dual
                    UNION ALL SELECT '4','CON2', JSON_VALUE(p_payload,'$.tx_telf_con2') FROM dual
                ) LOOP
                    IF f.num IS NOT NULL AND TRIM(f.num) IS NOT NULL THEN
                        MERGE INTO PERSONATELEFONOSTYPE t
                        USING (SELECT v_pk AS k, f.sec AS sec FROM dual) s
                        ON (t.IDENTIFICACION = s.k AND t.SECUENCIATELEFONO = s.sec)
                        WHEN MATCHED THEN UPDATE SET
                            CODIGOTIPOIDENTIFICACION = v_tipo,
                            CODIGOTIPOTELEFONO = f.tip,
                            NUMEROTELEFONO = f.num,
                            FECHAINGRESO = JSON_VALUE(p_payload,'$.fe_ingr'),
                            EMPRESAOPERADORA = CASE WHEN f.tip='CEL' THEN JSON_VALUE(p_payload,'$.ti_oper') ELSE NULL END
                        WHEN NOT MATCHED THEN INSERT
                            (CODIGOTIPOIDENTIFICACION, IDENTIFICACION, SECUENCIATELEFONO,
                             CODIGOTIPOTELEFONO, NUMEROTELEFONO, FECHAINGRESO, EMPRESAOPERADORA)
                        VALUES (v_tipo, v_pk, f.sec, f.tip, f.num,
                                JSON_VALUE(p_payload,'$.fe_ingr'),
                                CASE WHEN f.tip='CEL' THEN JSON_VALUE(p_payload,'$.ti_oper') ELSE NULL END);
                    END IF;
                END LOOP;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type, 'PERSONATELEFONOSTYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;
"""

# ============ Cargar pares (source_table, aggregate_type) ============
c = sql("fcme_canonicos").cursor()
c.execute("""SELECT source_table, aggregate_type_emit
             FROM dbo.cdc_table_to_types
             WHERE is_active=1 AND aggregate_type_emit IS NOT NULL""")
pairs = [(r.source_table, r.aggregate_type_emit) for r in c.fetchall()]
print(f"Pares totales: {len(pairs)}")

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

# ============ Generar branches ============
branches = []
covered = []
skipped = []

# branch especial telefonos
branches.append(SPECIAL_TELEFONOS)

for src_tbl, at in pairs:
    if at == "personaTelefonosType" and src_tbl == "fctbafil_actu":
        continue  # ya en SPECIAL
    ot = AT_TO_TABLE.get(at)
    if not ot:
        skipped.append((src_tbl, at, "no oracle table en AT_TO_TABLE"))
        continue
    db = table_to_db.get(src_tbl)
    if not db:
        skipped.append((src_tbl, at, "no localizada en BDs legacy"))
        continue
    legacy_cols = get_legacy_cols(db, src_tbl)
    if not legacy_cols:
        skipped.append((src_tbl, at, "sin columnas"))
        continue
    oracle_cols = get_oracle_cols(ot)
    if not oracle_cols:
        skipped.append((src_tbl, at, f"oracle table {ot} sin columnas"))
        continue

    snake_mode = is_snake(oracle_cols)
    mapped = map_columns(legacy_cols, oracle_cols, snake_mode)
    n_mapped = sum(1 for _, lc in mapped if lc)
    n_total = len(mapped)
    pk_o, pk_l = detect_pk(mapped)

    if not pk_o or not pk_l:
        skipped.append((src_tbl, at, f"no PK detectable"))
        continue

    branch, err = emit_branch(src_tbl, at, ot, mapped, snake_mode)
    if branch:
        branches.append(branch)
        covered.append((src_tbl, at, ot, n_mapped, n_total))
    else:
        skipped.append((src_tbl, at, err))

print(f"\nCubiertos: {len(covered)}")
print(f"Skipped:   {len(skipped)}")
for s in skipped[:30]:
    print(f"  - {s[0]} -> {s[1]}: {s[2]}")

# ============ SP completo ============
sp_full = """CREATE OR REPLACE PROCEDURE USP_INBOX_PARTICIPES(
    p_id             IN NUMBER,
    p_aggregate_type IN VARCHAR2,
    p_source_table   IN VARCHAR2,
    p_event_type     IN VARCHAR2,
    p_payload        IN CLOB
) AS
    v_err  VARCHAR2(4000);
    v_pk   VARCHAR2(200);
    v_tipo VARCHAR2(50);
BEGIN
""" + "\n".join(branches) + """
END USP_INBOX_PARTICIPES;
"""

with open(r"C:\Users\Usuario\Downloads\Bulk\_usp_inbox_participes.sql","w",encoding="utf-8") as f:
    f.write(sp_full)
print(f"\nSP escrito: {len(sp_full)} chars")

# Estadistica de cobertura
print("\nCobertura por par (mapeadas/total):")
covered_sorted = sorted(covered, key=lambda r: -r[3])
for src,at,ot,n,t in covered_sorted[:20]:
    print(f"  {src:<25} {at:<35} -> {ot:<35} {n}/{t}")

# ============ Deploy ============
print("\n[Deploy] USP_INBOX_PARTICIPES")
try:
    co.execute(sp_full)
except Exception as e:
    print(f"  ERROR ejecucion: {str(e)[:300]}")
co.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='USP_INBOX_PARTICIPES'")
status = co.fetchone()[0]
print(f"  status: {status}")
if status != "VALID":
    co.execute("""SELECT line, position, text FROM all_errors
                  WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPES' ORDER BY sequence
                  FETCH FIRST 15 ROWS ONLY""")
    for r in co.fetchall(): print(f"    line={r[0]} col={r[1]}: {r[2]}")
    raise SystemExit(1)

# Activar TODOS los types
print("\n[Activar todos los types en MODULE_CONFIG]")
co.execute("UPDATE CDC_INBOX_MODULE_CONFIG SET ACTIVE=1")
orcl.commit()
co.execute("SELECT COUNT(*) FROM CDC_INBOX_MODULE_CONFIG WHERE ACTIVE=1")
print(f"  types activos: {co.fetchone()[0]}")

# Persistir resumen
with open(r"C:\Users\Usuario\Downloads\Bulk\_step_c_cobertura.json","w",encoding="utf-8") as f:
    json.dump({"covered":[(s,a,o,n,t) for s,a,o,n,t in covered],
               "skipped":[(s,a,r) for s,a,r in skipped]}, f, indent=2)

orcl.close()
print("\n=== DEPLOY OK ===")
