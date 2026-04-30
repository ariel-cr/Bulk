"""Mapeo agresivo automatico legacy -> Oracle FCME_USER columnas.
Reglas:
1. snake legacy a partir de prefijos -> CAMEL Oracle.
2. Synonimos comunes (ci_cedula <-> identificacion, no_empl <-> nombre, etc.)
3. Substring matching.
4. Tipos especiales: CODIGO_EMPRESA -> '1', fechas sin match -> NULL.
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

c = sql("fcme_canonicos").cursor()
c.execute("SELECT source_table, aggregate_type_emit FROM dbo.cdc_table_to_types WHERE aggregate_type_emit IS NOT NULL")
at_to_legacies = defaultdict(set)
for r in c.fetchall(): at_to_legacies[r.aggregate_type_emit].add(r.source_table)

DBS = {"cgtbprvd":"dbCG","crtboper_cony":"dbCR","crtoblig":"dbCR",
       "cttbafil_audi":"dbCT","cttbmatr_dist_afil":"dbCT","cttbtabl_afil":"dbCT",
       "imtbmiem_cony":"dbIM","notbempl":"dbNO","notbcgfm":"dbNO",
       "svtbcaus":"dbSV","svtbdisc":"dbSV","svtbefec":"dbSV","svtbfmpg":"dbSV",
       "svtbstro":"dbSV","svtbstro_bene":"dbSV","svtbstro_cred":"dbSV",
       "svtbstro_deta":"dbSV","svtbstro_exte":"dbSV"}
def get_legacy_cols(tbl):
    db = DBS.get(tbl, "dbFC")
    cur = sql(db).cursor()
    cur.execute("SELECT name FROM sys.columns WHERE object_id=OBJECT_ID(?) ORDER BY column_id", f"dbo.{tbl}")
    return [r.name for r in cur.fetchall()]

def get_oracle_cols(tbl):
    co.execute("""SELECT column_name, data_type FROM all_tab_columns
                  WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id""", t=tbl)
    return co.fetchall()

# Mapeo synonimos: token Oracle -> patrones Legacy
SYNONYMS = {
    "identificacion": ["ci_cedula","ci_cedu","co_cedu","cedula","ci_iden","nu_iden"],
    "personaid": ["ci_cedula","ci_cedu","co_cedu"],
    "primerapellido": ["ape_pater","no_apellido"],
    "segundoapellido": ["ape_mater"],
    "primernombre": ["nom","no_nombre","no_empl"],
    "segundonombre": ["seg_nombre"],
    "fechainscripcion": ["fe_ingr","fx_ingr","fe_afil"],
    "fechaingreso": ["fe_ingr","fx_ingr"],
    "fechamodificacion": ["fe_modi","fx_modi","fe_mvto_modi"],
    "fechaeliminacion": ["fe_elim","fx_elim"],
    "fechacreacion": ["fe_crea","fx_crea","fe_ingr"],
    "usuarioingresa": ["co_usua_ingr","co_usua_crea","co_user_ingr"],
    "usuariomodifica": ["co_usua_modi"],
    "usuarioelimina": ["co_usua_elim"],
    "codigoempresa": ["co_empr"],
    "codigotipoidentificacion": ["ti_iden","ci_tipo"],
    "codigotipopersona": ["ti_pers"],
    "tipoidentificacion": ["ti_iden","ci_tipo"],
    "tipopersona": ["ti_pers"],
    "codigopais": ["co_pais","ci_pais"],
    "codigoresidencia": ["co_resi","co_pais"],
    "codigoactividad": ["co_acti","ci_acti"],
    "codigosectoreconomico": ["co_sect","ci_sect"],
    "codigoestatuspersona": ["st_prvd","ci_esta","st_esta","co_esta"],
    "estatuspersona": ["st_prvd","ci_esta","st_esta"],
    "estado": ["ci_esta","st_esta","co_esta","ci_estado"],
    "fechanacimiento": ["fe_naci","fx_naci"],
    "genero": ["in_sexo","co_sexo"],
    "estadocivil": ["in_esta_civi","co_esta_civi"],
    "numerotelefono": ["nu_tel1","nu_telf","tx_telf_celu","tx_telf_conv"],
    "telefono": ["nu_tel1","nu_telf"],
    "extension": ["nu_exte"],
    "secuencia": ["sc_regi","sc_secu","qs_secu"],
    "numerocedula": ["ci_cedula","ci_cedu","co_cedu"],
    "numerodireccion": ["sc_regi","nu_dire"],
    "calle": ["ds_call_prim","ds_dire","no_dire"],
    "direccion": ["ds_dire","ds_dirc","ds_call_prim"],
    "ciudad": ["co_cant","ci_ciud","co_ciud"],
    "provincia": ["co_prov","ci_prov"],
    "parroquia": ["co_parr","ci_parr"],
    "cantidadempleados": ["nu_empl"],
    "observaciones": ["ds_obse","tx_obse"],
    "codigocuenta": ["nu_cnta","co_cnta"],
    "tipocuenta": ["ti_cnta"],
    "monto": ["mn_movi","va_mont"],
    "fecha": ["fe_movi","fx_movi"],
    "estaturapersona": [],
}

def normalize(s): return re.sub(r'[_]','', s).lower()

def split_camel(s):
    # PERSONA_ID -> persona, id; CODIGOTIPOIDENTIFICACION -> codigo,tipo,identificacion
    s = s.lower()
    parts = s.split("_")
    if len(parts) > 1: return parts
    # split by changes case (no aplica si todo es lower) - usar dict de keywords
    keys = sorted(["codigo","tipo","numero","fecha","descripcion","indicador","estado",
                   "primer","segundo","apellido","nombre","persona","empresa","monto","valor",
                   "secuencia","identificacion","direccion","actividad","categoria","sucursal",
                   "oficina","categoria","trato","calificado","preferido","exonerado","sujeto",
                   "obligado","razon","apertura","finalidad","usuarioOficial","oficial",
                   "natural","juridico","sectoreconomico","sector","economico","pais","residencia",
                   "telefono","numero","extension","ubicacion","empresa","operadora","area","mail"], key=len, reverse=True)
    out = []; rest = s
    while rest:
        matched = False
        for k in keys:
            if rest.startswith(k):
                out.append(k); rest = rest[len(k):]; matched=True; break
        if not matched:
            out.append(rest); break
    return out

FORCE_MAP = {
    # oracle col norm -> candidatos legacy en orden
    "codigocedu": ["ci_cedu","co_cedu","ci_cedula"],
    "identificacion": ["ci_cedula","ci_cedu","co_cedu","nu_iden","ci_iden"],
    "personaid": ["ci_cedula","ci_cedu","co_cedu"],
    "numerocedula": ["ci_cedu","ci_cedula","co_cedu"],
    "codigoempresa": ["co_empr"],
    "secuencia": ["sc_regi","sc_secu","qs_secu"],
    "fechaingreso": ["fe_ingr","fx_ingr","fe_afil"],
    "fechamodificacion": ["fe_modi","fx_modi","fe_mvto_modi"],
    "fechaeliminacion": ["fe_elim","fx_elim"],
    "fechacreacion": ["fe_crea","fx_crea","fe_ingr"],
    "usuarioingresa": ["co_usua_ingr","co_user_ingr","co_usua_crea"],
    "usuariomodifica": ["co_usua_modi"],
    "usuarioelimina": ["co_usua_elim"],
}

def best_match(oracle_col, legacy_cols):
    """Heuristica: forzados + synonimos + substring + prefix mapping"""
    on = normalize(oracle_col)
    # 0) forzados
    if on in FORCE_MAP:
        for cand in FORCE_MAP[on]:
            for lc in legacy_cols:
                if normalize(lc) == normalize(cand): return lc
    # 1) exact match (case-insensitive)
    for lc in legacy_cols:
        if normalize(lc) == on: return lc
    # 2) synonimos
    if on in SYNONYMS:
        for cand in SYNONYMS[on]:
            for lc in legacy_cols:
                if normalize(lc) == normalize(cand): return lc
    # 2b) intentar matchear quitando prefijos comunes
    # codigo_xxx -> co_xxx o ci_xxx
    for prefix_o, prefix_l in [("codigo","co_"),("codigo","ci_"),("descripcion","ds_"),
                                ("numero","nu_"),("texto","tx_"),("indicador","in_"),
                                ("fecha","fe_"),("fecha","fx_"),("tipo","ti_"),
                                ("nombre","no_"),("estado","st_"),("estado","es_"),
                                ("monto","mn_"),("valor","va_"),("secuencia","sc_")]:
        if on.startswith(prefix_o):
            tail = on[len(prefix_o):]
            for lc in legacy_cols:
                ln = normalize(lc)
                if ln.startswith(prefix_l) and (ln[len(prefix_l):] == tail
                                                 or ln[len(prefix_l):].startswith(tail[:5])):
                    return lc
    # 3) substring
    for lc in legacy_cols:
        ln = normalize(lc)
        if (on in ln or ln in on) and len(on)>3 and len(ln)>3:
            return lc
    return None

# Generar mapeos completos
mappings = {}  # at -> oracle_table -> [(oracle_col, legacy_col_or_NULL, expr)]
for at, ot in AT_TO_TABLE.items():
    legacies = sorted(at_to_legacies.get(at, []))
    legacy_cols = []
    for lt in legacies:
        try: legacy_cols.extend(get_legacy_cols(lt))
        except: pass
    legacy_cols = list(dict.fromkeys(legacy_cols))
    oracle_cols = get_oracle_cols(ot)
    mapped = []
    for ocol, dt in oracle_cols:
        if ocol == "ID":
            mapped.append((ocol, None, "skip-id")); continue
        m = best_match(ocol, legacy_cols)
        if m:
            mapped.append((ocol, m, f"JSON_VALUE(p_payload,'$.{m}')"))
        else:
            # heuristica defaults
            on = normalize(ocol)
            if "fechacreacion" in on or "fechaingreso" in on:
                mapped.append((ocol, None, "SYSDATE"))
            elif "estado" in on and dt in ("VARCHAR2","CHAR"):
                mapped.append((ocol, None, "'A'"))
            elif "codigoempresa" in on:
                mapped.append((ocol, None, "'1'"))
            else:
                mapped.append((ocol, None, "NULL"))
    mappings[at] = (ot, mapped, legacies)

# Generar SP completo
sp_lines = ["CREATE OR REPLACE PROCEDURE USP_INBOX_PARTICIPES("]
sp_lines.append("    p_id             IN NUMBER,")
sp_lines.append("    p_aggregate_type IN VARCHAR2,")
sp_lines.append("    p_event_type     IN VARCHAR2,")
sp_lines.append("    p_payload        IN CLOB")
sp_lines.append(") AS")
sp_lines.append("    v_err VARCHAR2(4000);")
sp_lines.append("    v_pk VARCHAR2(200);")
sp_lines.append("BEGIN")

# fctbafil_actu (mapeo ya hecho - lo mantengo)
# Para los nuevos: itero todas las tablas legacy y uso mappings de cada aggregate_type
# Pero como aggregate_type = nombre tabla legacy, y un agg_type apunta a 1 oracle_table:
# tengo que buscar aggregate_type -> targets (multiple posibles)
c.execute("""SELECT source_table, aggregate_type_emit FROM dbo.cdc_table_to_types
             WHERE aggregate_type_emit IS NOT NULL""")
src_to_at = defaultdict(set)
for r in c.fetchall(): src_to_at[r.source_table].add(r.aggregate_type_emit)

# fctbafil_actu y fctbafil_info_actu_docs ya estan en cubierto (tablas snake)
# Mantengo el codigo original para esos 2 + agrego el resto via mappings

for src_tbl in sorted(src_to_at):
    sp_lines.append(f"\n    IF p_aggregate_type = '{src_tbl}' THEN")
    for at in sorted(src_to_at[src_tbl]):
        if at not in mappings: continue
        ot, mapped, _ = mappings[at]
        # Build MERGE: use first non-id field as match key
        # Para simplicidad, usar identificacion / persona_id / cedula como key
        key_col = None
        for oc, lc, expr in mapped:
            if oc.upper() in ("IDENTIFICACION","CODIGO_CEDU","CODIGOCEDU","PERSONA_ID","PERSONAID"):
                key_col = (oc, expr); break
        if not key_col:
            # tomar primera columna no-NULL
            for oc, lc, expr in mapped:
                if expr != "NULL" and "skip" not in (expr or ""):
                    key_col = (oc, expr); break

        if not key_col:
            sp_lines.append(f"        -- {ot}: no key, INSERT plano")
            cols = [oc for oc,_,_ in mapped if oc != "ID"]
            vals = [expr for _,_,expr in mapped if "skip" not in expr]
            sp_lines.append(f"        BEGIN")
            sp_lines.append(f"            IF p_event_type IN ('DELETE','DELETED') THEN NULL;")
            sp_lines.append(f"            ELSE INSERT INTO {ot} ({', '.join(cols)}) VALUES ({', '.join(vals)});")
            sp_lines.append(f"            END IF;")
            sp_lines.append(f"        EXCEPTION WHEN OTHERS THEN v_err := SQLERRM; INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE) VALUES (p_id, p_aggregate_type, p_event_type, '{ot}: ' || SUBSTR(v_err,1,3900));")
            sp_lines.append(f"        END;")
            continue

        ock, expr_k = key_col
        # MERGE
        upd_sets = [f"{oc} = {expr}" for oc,_,expr in mapped if oc != ock and oc != "ID" and "skip" not in expr]
        ins_cols = [oc for oc,_,expr in mapped if oc != "ID" and "skip" not in expr]
        ins_vals = [expr if oc != ock else "v_pk" for oc,_,expr in mapped if oc != "ID" and "skip" not in expr]

        sp_lines.append(f"        -- {ot}")
        sp_lines.append(f"        BEGIN")
        sp_lines.append(f"            v_pk := {expr_k};")
        sp_lines.append(f"            IF p_event_type IN ('DELETE','DELETED') THEN")
        sp_lines.append(f"                DELETE FROM {ot} WHERE {ock} = v_pk;")
        sp_lines.append(f"            ELSIF v_pk IS NOT NULL THEN")
        sp_lines.append(f"                MERGE INTO {ot} t USING (SELECT v_pk AS k FROM dual) s ON (t.{ock} = s.k)")
        sp_lines.append(f"                WHEN MATCHED THEN UPDATE SET {', '.join(upd_sets) if upd_sets else 'PROCESSED=PROCESSED'}")
        sp_lines.append(f"                WHEN NOT MATCHED THEN INSERT ({', '.join(ins_cols)}) VALUES ({', '.join(ins_vals)});")
        sp_lines.append(f"            END IF;")
        sp_lines.append(f"        EXCEPTION WHEN OTHERS THEN v_err := SQLERRM; INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE) VALUES (p_id, p_aggregate_type, p_event_type, '{ot}: ' || SUBSTR(v_err,1,3900));")
        sp_lines.append(f"        END;")
    sp_lines.append("    END IF;")

sp_lines.append("\nEXCEPTION WHEN OTHERS THEN")
sp_lines.append("    v_err := SQLERRM;")
sp_lines.append("    INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)")
sp_lines.append("    VALUES (p_id, p_aggregate_type, p_event_type, SUBSTR(v_err,1,4000));")
sp_lines.append("END;")

full_sp = "\n".join(sp_lines)
with open(r"C:\Users\Usuario\Downloads\Bulk\_usp_inbox_full.sql","w",encoding="utf-8") as f:
    f.write(full_sp)
print(f"SP generado, lineas: {len(sp_lines)}")
print(f"  archivo: C:\\Users\\Usuario\\Downloads\\Bulk\\_usp_inbox_full.sql")

# Guardar JSON con mapeos para revisar
out = {at: {"oracle": mappings[at][0],
            "legacies": mappings[at][2],
            "cols": [{"oracle_col":oc, "legacy":lc, "expr":expr} for oc,lc,expr in mappings[at][1]]}
       for at in mappings}
with open(r"C:\Users\Usuario\Downloads\Bulk\mapping_done.json","w",encoding="utf-8") as f:
    json.dump(out, f, indent=2, ensure_ascii=False)
print("  mappings JSON: mapping_done.json")

# Stats
stats = defaultdict(int)
for at, (ot, mapped, _) in mappings.items():
    for oc, lc, expr in mapped:
        if expr == "skip-id": stats["id_skip"] += 1
        elif lc: stats["mapped"] += 1
        elif "JSON_VALUE" in expr: stats["mapped"] += 1
        else: stats["null_or_default"] += 1
print(f"  stats: {dict(stats)}")
