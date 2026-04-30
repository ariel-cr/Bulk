"""Genera + aplica los 91 wrappers Oracle USP_INBOX_<X> para cartera.

Cada wrapper:
  - Recibe (p_id, p_aggregate_type, p_source_table, p_event_type, p_payload CLOB)
  - SET_CLIENT_INFO('is_replicating') anti-loop F2
  - JSON_VALUE extrae cada legacy col del payload
  - DELETE / INSERT (con DUP_VAL_ON_INDEX -> UPDATE) / fallback PK-only INSERT
  - Catch global -> log a CDC_INBOX_ERRORS

Modos:
  --gen     : solo genera el .sql (no toca BD)
  --apply   : crea los 91 wrappers en Oracle
  --check   : verifica cuales existen / cuantos compilan VALID

Convencion PK del wrapper (para WHERE/UPDATE):
  Preferir el primer dest_match cuyo oracle_col esta en dest_pk.
  Fallback: primer dest_match entry.
"""
import sys, json, argparse
import oracledb

ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)

# Reusar usp_name del generador anterior (mismo dict de abreviaciones)
ABBR = [
    ("AUTORIZACIONCREDITODETALLE","AUTRCREDDETA"),("COBRANZAJUDICIALDISTRIBUCION","COBJUDDIST"),
    ("COBRANZAJUDICIALDETALLE","COBJUDDETA"),("AUXDATOSCOBROSADICIONALES","AUXDATOSCOB"),
    ("CALIFICACIONCARTERADETALLE","CALFCARTDETA"),("DEVENGAMIENTOCARTERADETALLE","DVGOCARTDETA"),
    ("CONTABILIZACIONCREDITO","CONTABCRED"),("DEVOLUCIONMASIVADETALLE","DEVOMASDETA"),
    ("REPORTESBSOPERACIONANTERIOR","RPTSBSOPANT"),("REPORTESBSOPERACIONCANCELADA","RPTSBSOPCANC"),
    ("REPORTESBSOPERACIONCONCEDIDA","RPTSBSOPCONC"),("REPORTESBSGARANTECODEUDOR","RPTSBSGRCOD"),
    ("REPORTESBSSALDOOPERACION","RPTSBSSALOP"),("REPORTESBSSUJETORIESGO","RPTSBSSJTO"),
    ("REPORTESBSGARANTIAREAL","RPTSBSGARR"),("REPORTESBSCABECERA","RPTSBSCAB"),
    ("REPORTESBSDETALLE","RPTSBSDETA"),("LIQUIDACIONDIARIACREDITO","LIQDIARIACRED"),
    ("MOVIMIENTOCONTABLECREDITO","MOVCONTACRED"),("GESTIONCOMUNICACIONCREDITO","GESTCOMUCRED"),
    ("GESTIONCOBRANZAASIGNACION","GESTCOBRASIG"),("ESTADOCONVENIOCREDITO","ESTCONVCRED"),
    ("RUBROSCOBRANZADETALLE","RUBRCOBRDETA"),("PRECALIFICACIONCREDITO","PRECALIFCRED"),
    ("REFINANCIAMIENTOCREDITO","REFICRED"),("ETAPAJUDICIALCREDITO","ETAPJUDCRED"),
    ("CONCEPTOGASTOJUDICIAL","CONCEPGSTOJUD"),("AUTORIZACIONCREDITO","AUTRCRED"),
    ("CONVENIOPAGOCREDITO","CONVPAGOCRED"),("FLUJOTRABAJOCREDITO","FLUJOTRABCRED"),
    ("DESEMBOLSODEVOLUCION","DESEMBDEVO"),("COSTOFINANCIEROCREDITO","COSTOFINCRED"),
    ("DETALLERECUPERACION","DETARECUP"),("RECUPERACIONCONVENIO","RECUPCONV"),
    ("RECUPERACIONCREDITO","RECUPCRED"),("TRANSACCIONRECUPERACION","TRANSRECUP"),
    ("CALIFICACIONCARTERA","CALFCART"),("DEVENGAMIENTOCARTERA","DVGOCART"),
    ("DEVOLUCIONMASIVA","DEVOMAS"),("DEVOLUCIONCREDITO","DEVOCRED"),
    ("DESEMBOLSOCREDITO","DESEMBCRED"),("CANCELACIONCREDITO","CANCCRED"),
    ("CAUCIONCREDITO","CAUCCRED"),("DOCUMENTOCREDITO","DOCCRED"),
    ("GARANTIACREDITO","GARCRED"),("OPERACIONCONYUGAL","OPCONYU"),
    ("ABONOEXTRAORDINARIO","ABNEXTR"),("REFERENCIACLIENTE","REFCLIE"),
    ("REFERENCIADEUDOR","REFDEUD"),("OBLIGACIONROL","OBLIROL"),
    ("PERSONACREDITO","PERSCRED"),("CUOTACREDITO","CUOTACRED"),
    ("PAGOSCREDITO","PAGOSCRED"),("PLANPAGOAJUSTE","PLPGAJUS"),
    ("SOBRANTECAUCION","SOBRCAUC"),("SOBRANTECREDITO","SOBRCRED"),
    ("SOBRANTEDISTRIBUCION","SOBRDIST"),("SOLIDARIOCREDITO","SOLIDCRED"),
    ("TASAINTERESCREDITO","TASAINTCRED"),("FECHASPROCESO","FCHPROC"),
    ("INFORMACIONLEGAL","INFOLEGAL"),("MEDIDAJUDICIAL","MEDJUD"),
    ("UNIDADJUDICIAL","UNIJUD"),("ESTADOLEGAL","ESTLEGAL"),
    ("CUENTASENLEGAL","CTASLEGAL"),("CUENTACUOTAS","CTACUOTAS"),
    ("CUENTAPERSONAS","CTAPERS"),("CUENTAPORCOBRAR","CTAPORCOBR"),
    ("CUENTAAUTOMATICADETALLE","CTAAUTODETA"),("CUENTAAUTOMATICA","CTAAUTO"),
    ("CUENTACXPCXC","CTACXPCXC"),("PERSONACXPCXC","PERSCXPCXC"),
    ("SALDOCXPCXC","SLDCXPCXC"),("SALDOCARTERADETALLE","SLDCARTDETA"),
    ("SALDOCARTERA","SLDCART"),("SALDOVINCULADO","SLDVINC"),
    ("PAGOCREDITO","PAGOCRED"),("PLAZOVENCIDO","PLZOVENC"),
    ("PROCESOACCION","PROCACC"),("RUBROCOBRANZA","RUBRCOBR"),
    ("CUOTACONVENIO","CUOTACONV"),("PLANPAGO","PLPG"),("CREDITO","CRED"),
    ("SEGUIMIENTOAUTORIZACION","SEGAUTR"),("SEGUROCREDITO","SEGUCRED"),
    ("TIPOCREDITO","TIPOCRED"),("TIPOSOBRANTE","TIPOSOBR"),
    ("CUENTA","CTA"),("COBRANZAJUDICIAL","COBJUD"),
]
def usp_name(agg):
    s = agg
    if s.endswith("_type"): s = s[:-5]
    elif s.endswith("Type"): s = s[:-4]
    base = s.upper()
    name = f"USP_INBOX_{base}"
    if len(name) <= 30: return name
    for lf, sf in ABBR:
        if lf in base:
            base = base.replace(lf, sf)
            if len(f"USP_INBOX_{base}") <= 30:
                return f"USP_INBOX_{base}"
    return f"USP_INBOX_{base}"[:30]

def pick_pk_match(dest_match, dest_pk):
    """Devuelve el dest_match entry a usar como PK (oracle_col, legacy_col).
    Preferir el primero cuyo oracle_col esta en dest_pk; fallback primer entry
    cuyo lcol parezca numerico (evita mapear PK a flags 'in_*' / 'st_*' / 'es_*')."""
    for o, l in dest_match:
        if o in dest_pk:
            return (o, l)
    for o, l in dest_match:
        if not _looks_non_numeric(l):
            return (o, l)
    return dest_match[0] if dest_match else None


# Prefijos legacy que casi siempre son flags / texto corto (no aptos para PK numerico)
_NON_NUMERIC_PREFIXES = ("in_", "st_", "es_", "ti_iden", "ds_", "tx_", "no_")
def _looks_non_numeric(lcol):
    s = (lcol or "").lower()
    return any(s.startswith(p) for p in _NON_NUMERIC_PREFIXES)


def _is_bad_id_mapping(ocol, lcol):
    """ID en FCME_USER es GENERATED ALWAYS / BY DEFAULT AS IDENTITY -> nunca debe
    incluirse en INSERTs (ORA-32795 si es ALWAYS). Excluimos ID universalmente,
    sin importar a que lcol mapee."""
    return ocol.upper() == "ID"

def build_wrapper(spec):
    agg = spec["agg"]
    dest = spec["dest"]
    dest_pk = spec.get("dest_pk", [])
    dest_match = spec.get("dest_match", [])
    sp = usp_name(agg)

    if not dest_match:
        # No tenemos columnas mapeadas -> stub que solo logea (no inserta nada)
        body = f"""CREATE OR REPLACE PROCEDURE FCME_USER.{sp}(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_err VARCHAR2(500);
BEGIN
    -- STUB: sin column mapping disponible para {agg}
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper {agg}: STUB sin column mapping');
EXCEPTION WHEN OTHERS THEN NULL;
END;"""
        return sp, body, "STUB"

    # Filtrar mapeos malos antes de derivar PK / INSERT
    clean_match = [(o, l) for (o, l) in dest_match if not _is_bad_id_mapping(o, l)]
    if not clean_match:
        # Todo el dest_match mapeaba solo a ID (autogenerated) -> emitir STUB
        body = f"""CREATE OR REPLACE PROCEDURE FCME_USER.{sp}(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
BEGIN
    -- STUB: dest_match solo mapea a ID (GENERATED ALWAYS) tras filtro. Sin cols insertables.
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper {agg}: STUB - solo mapea a ID');
EXCEPTION WHEN OTHERS THEN NULL;
END;"""
        return sp, body, "STUB-IDONLY"

    pk = pick_pk_match(clean_match, dest_pk)
    pk_ocol, pk_lcol = pk

    var_for = {}
    decls = []
    extracts = []
    seen_lcol = set()
    for ocol, lcol in clean_match:
        if lcol in seen_lcol:
            continue  # no declarar twice
        seen_lcol.add(lcol)
        var = f"v_{lcol.lower().replace('-','_')}"
        var_for[lcol] = var
        decls.append(f"    {var} VARCHAR2(4000);")
        extracts.append(f"    {var} := JSON_VALUE(p_payload, '$.{lcol}');")

    ins_cols = []
    ins_vals = []
    seen_ocol = set()
    for ocol, lcol in clean_match:
        if ocol in seen_ocol:
            continue
        seen_ocol.add(ocol)
        ins_cols.append(ocol)
        ins_vals.append(var_for[lcol])

    if len(clean_match) > 1:
        # UPDATE SET cols (excluir PK)
        upd_pairs = [f"{o}={var_for[l]}" for o, l in clean_match if o != pk_ocol and l in var_for]
        upd_set = ", ".join(upd_pairs) if upd_pairs else f"{pk_ocol}={var_for[pk_lcol]}"
    else:
        upd_set = f"{pk_ocol}={var_for[pk_lcol]}"

    upd_match = f"{pk_ocol}={var_for[pk_lcol]}"
    del_match = upd_match

    # Sin fallback PK-only: en tablas con ID GENERATED ALWAYS AS IDENTITY
    # ese fallback siempre falla con ORA-32795. Mejor logear el SQLERRM real
    # del INSERT principal para diagnostico.
    fallback_block = f"""WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper {agg} insert: ' || v_err);"""

    body = f"""CREATE OR REPLACE PROCEDURE FCME_USER.{sp}(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
{chr(10).join(decls)}
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
{chr(10).join(extracts)}
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.{dest} WHERE {del_match};
    ELSE
        BEGIN
            INSERT INTO FCME_USER.{dest} ({",".join(ins_cols)})
            VALUES ({",".join(ins_vals)});
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.{dest} SET {upd_set} WHERE {upd_match};
        {fallback_block}
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper {agg}: ' || v_err);
END;"""
    return sp, body, "OK"

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--gen",   action="store_true")
    g.add_argument("--apply", action="store_true")
    g.add_argument("--check", action="store_true")
    args = ap.parse_args()

    wrappers = []
    for s in SPECS:
        sp, body, status = build_wrapper(s)
        wrappers.append({"agg":s["agg"],"sp":sp,"body":body,"status":status,"dest":s.get("dest")})

    if args.gen:
        out = []
        out.append("/* CARTERA Flow1 wrappers - 91 USP_INBOX_<X> Oracle */\n\n")
        n_stub = 0
        for w in wrappers:
            if w["status"] == "STUB":
                n_stub += 1
            out.append(f"/* {w['agg']} -> {w['sp']} ({w['status']}) */\n")
            out.append(w["body"])
            out.append("\n/\n\n")
        path = r"C:\Users\Usuario\Downloads\Bulk\cartera_flow1_wrappers.sql"
        with open(path,"w",encoding="utf-8") as f:
            f.write("".join(out))
        print(f"OK  wrappers: {len(wrappers)}  stubs: {n_stub}")
        print(f"Archivo: {path}")
        return

    orcl = oracledb.connect(**ORA); orcl.autocommit = False
    o = orcl.cursor()

    if args.check:
        n_exists=0; n_valid=0; n_invalid=0
        for w in wrappers:
            o.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_type='PROCEDURE' AND object_name=:1", [w["sp"]])
            r = o.fetchone()
            if r:
                n_exists += 1
                if r[0] == "VALID":
                    n_valid += 1
                else:
                    n_invalid += 1
                    print(f"  INVALID {w['sp']}")
        print(f"\nExisting: {n_exists}/{len(wrappers)}  VALID={n_valid}  INVALID={n_invalid}")
        return

    if args.apply:
        n_ok=0; n_err=0
        for w in wrappers:
            try:
                o.execute(w["body"])
                o.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_type='PROCEDURE' AND object_name=:1", [w["sp"]])
                st = o.fetchone()
                flag = st[0] if st else "?"
                if flag == "VALID":
                    print(f"  OK   {w['sp']:<32}  ({w['status']})")
                    n_ok += 1
                else:
                    print(f"  WARN {w['sp']:<32}  status={flag}")
                    o.execute("SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name=:1 AND type='PROCEDURE'", [w["sp"]])
                    for ln, pos, txt in o.fetchall()[:3]:
                        print(f"     L{ln}:{pos} {txt[:160]}")
                    n_err += 1
            except Exception as e:
                print(f"  ERR  {w['sp']}: {str(e)[:200]}")
                n_err += 1
        orcl.commit()
        print(f"\nResultado: OK={n_ok}  ERR/INVALID={n_err}")
    orcl.close()

if __name__ == "__main__":
    main()
