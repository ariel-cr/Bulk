"""Auto-mapper Cartera (Fase 2):
Para cada uno de los 91 types, encuentra la tabla legacy con mejor solapamiento
de columnas (Oracle <-> legacy) usando expansion de abreviaturas SP -> Spanish
canonico. Produce _cartera_specs.json listo para Fase 3.

Algoritmo:
  1) Introspect Oracle (cargado de _introspect_cartera.json) ya tiene cols+PK
  2) Introspect TODAS las tablas legacy en dbCR/dbFC/dbCG/dbCT (cols+PK)
  3) Para cada type, score(O_cols, L_cols) y elige el mejor legacy
  4) Genera SPEC con mapping columna->columna usando best match Oracle col -> legacy col
"""
import json, re, sys
from difflib import SequenceMatcher
import pyodbc

DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
LEGACY_DBS = ["dbCR","dbFC","dbCG","dbCT","dbIM","dbNO","dbSV"]

# Expansion abreviatura legacy -> token canonico Spanish
ABBR = {
    "co":"codigo","nu":"numero","fe":"fecha","ho":"hora","ds":"descripcion",
    "mo":"monto","no":"nombre","ti":"tipo","st":"estado","va":"valor",
    "sc":"secuencia","qs":"secuencia","ci":"cedula","po":"porcentaje","aa":"anio",
    "in":"indicador","us":"usuario","cnta":"cuenta","empr":"empresa","afil":"afiliado",
    "cred":"credito","obli":"obligacion","oblig":"obligacion","oper":"operacion",
    "soli":"solidario","solid":"solidario","recu":"recuperacion","rec":"recuperacion",
    "rubr":"rubro","cuot":"cuota","fond":"fondo","rol":"rol","auto":"automatico",
    "calf":"calificacion","calif":"calificacion","canc":"cancelacion","cauci":"caucion",
    "cauc":"caucion","desem":"desembolso","deve":"devengamiento","devo":"devolucion",
    "docu":"documento","fluj":"flujo","gar":"garantia","gara":"garantia",
    "gest":"gestion","info":"informacion","liqd":"liquidacion","liq":"liquidacion",
    "movi":"movimiento","perso":"persona","plpag":"planpago","plzv":"plazovencido",
    "preca":"precalificacion","sobr":"sobrante","trans":"transaccion","etap":"etapa",
    "concep":"concepto","med":"medida","uni":"unidad","proc":"proceso",
    "acci":"accion","lega":"legal","conv":"convenio","abno":"abono","abono":"abono",
    "ext":"extraordinario","audit":"auditoria","vinc":"vinculado","ref":"referencia",
    "refer":"referencia","deud":"deudor","clien":"cliente","refi":"refinanciamiento",
    "comu":"comunicacion","asig":"asignacion","grupo":"grupo","deta":"detalle",
    "cabe":"cabecera","ante":"anterior","conce":"concedida","diar":"diaria",
    "mas":"masiva","indi":"individuales","sbs":"sbs","rep":"reporte","tasa":"tasa",
    "intr":"interes","inte":"interes","judi":"judicial","cobr":"cobranza",
    "ajus":"ajuste","saldo":"saldo","cart":"cartera","carter":"cartera",
    "ejec":"ejecutivo","prod":"producto","prov":"provincia","prvd":"proveedor",
    "moti":"motivo","band":"banda","fech":"fecha","tarj":"tarjeta","cdio":"credito",
    "auxi":"auxiliar","aux":"auxiliar","tran":"transaccion","aval":"aval",
    "diar":"diaria","fund":"fundamento","scib":"sci","esbs":"sbs","apli":"aplicacion",
    "asis":"asistencia","mens":"mensualidad","mont":"monto",
    "trab":"trabajo","aplic":"aplicacion","gen":"genera","gene":"genera",
    "elim":"eliminacion","creac":"creacion","creaci":"creacion","cancl":"cancelacion",
    "ingr":"ingreso","reg":"registro","regi":"registro","insti":"institucion",
    "inst":"institucion","caus":"causa","susp":"suspendido","cncl":"cancelacion",
    "decl":"declaracion","cre":"creacion","reti":"retiro","liq":"liquidacion",
    "rese":"reserva","cgfm":"cargafamiliar","empl":"empleado","camp":"campo",
    "depo":"deposito","ven":"vencimiento","vcm":"vencimiento","cpag":"cuotapago",
    "expi":"expiracion","cup":"cupo","aut":"autorizacion",
    "moro":"mora","stop":"parada","obs":"observacion","sg":"seguridad",
    "fact":"factura","reca":"recaudacion","fct":"factura","gn":"general","sal":"saldo",
    "ord":"orden","cont":"contable","fct":"factura",
}

def normalize(col: str) -> str:
    """Convierte col a forma canonica lowercase sin guiones, expandiendo abreviaturas."""
    parts = col.lower().split("_")
    out = []
    for p in parts:
        out.append(ABBR.get(p, p))
    return "".join(out)

def col_score(o_norm: str, l_norm: str) -> float:
    if o_norm == l_norm: return 1.0
    if o_norm in l_norm or l_norm in o_norm: return 0.85
    return SequenceMatcher(None, o_norm, l_norm).ratio()

def best_match(ora_col: str, leg_cols: list[str], threshold=0.65):
    """Mejor columna legacy para una columna Oracle."""
    o = normalize(ora_col)
    best = None; best_s = 0
    for lc in leg_cols:
        l = normalize(lc)
        s = col_score(o, l)
        if s > best_s:
            best_s = s; best = lc
    if best_s >= threshold:
        return best, best_s
    return None, 0

def sql(db):
    s = f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}"
    return pyodbc.connect(s, autocommit=True, timeout=30)

# 1) Cargar introspeccion Oracle
with open("_introspect_cartera.json","r",encoding="utf-8") as f:
    oracle_data = json.load(f)

# 2) Introspect legacy tablas (cols + PK)
print("[*] Introspeccion legacy DBs...")
legacy_meta = {}  # "dbCR.crtoblig" -> {"cols":[...], "pk":[...]}
for db in LEGACY_DBS:
    try:
        c = sql(db).cursor()
        c.execute("""SELECT t.name AS tbl,
                            c.name AS col,
                            ty.name AS data_type,
                            CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_pk
                     FROM sys.tables t
                     JOIN sys.columns c ON c.object_id=t.object_id
                     JOIN sys.types ty ON ty.user_type_id=c.user_type_id
                     LEFT JOIN sys.indexes i ON i.object_id=t.object_id AND i.is_primary_key=1
                     LEFT JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id AND ic.column_id=c.column_id
                     ORDER BY t.name, c.column_id""")
        for tbl, col, dtyp, is_pk in c.fetchall():
            key = f"{db}.{tbl}"
            if key not in legacy_meta:
                legacy_meta[key] = {"db":db,"tbl":tbl,"cols":[], "pk":[]}
            legacy_meta[key]["cols"].append({"name":col,"type":dtyp})
            if is_pk:
                legacy_meta[key]["pk"].append(col)
        print(f"  {db}: {sum(1 for k in legacy_meta if k.startswith(db+'.'))} tablas")
    except Exception as e:
        print(f"  [!] {db} fallo: {str(e)[:120]}")

# 3) Para cada type, score legacy candidates
def table_score(ora_cols, leg_cols, leg_tbl_name=""):
    """Suma de best-match scores por columna Oracle, normalizado."""
    if not ora_cols: return 0
    total = 0
    matched = 0
    for oc in ora_cols:
        _, s = best_match(oc, [c["name"] for c in leg_cols])
        if s > 0:
            total += s
            matched += 1
    # bonus si nombre tabla contiene tokens del type
    return total / max(len(ora_cols), 1)

# Tablas a excluir (infraestructura CDC, no son datos)
EXCLUDE_TBLS = {"cdc_outbox","cdc_inbox","cdc_inbox_errors","cdc_inbox_module_config",
                "cdc_outbox_archive","cdc_outbox_local"}

# Token abbreviation mapping para nombre tabla legacy (snake) -> tokens semanticos
TBL_TOKEN_ABBR = {
    "cred":"credito","oblig":"obligacion","oper":"operacion","cobr":"cobranza",
    "judi":"judicial","reca":"recaudacion","recu":"recuperacion","sobr":"sobrante",
    "cauci":"caucion","cauc":"caucion","desem":"desembolso","gara":"garantia",
    "soli":"solidario","solid":"solidario","conv":"convenio","plpag":"planpago",
    "plzv":"plazovencido","preca":"precalificacion","calif":"calificacion",
    "calf":"calificacion","cncl":"cancelacion","canc":"cancelacion","docu":"documento",
    "fluj":"flujo","gest":"gestion","info":"informacion","liqd":"liquidacion",
    "liq":"liquidacion","movi":"movimiento","perso":"persona","pago":"pago",
    "abno":"abono","ext":"extraordinario","audit":"auditoria","vinc":"vinculado",
    "ref":"referencia","refer":"referencia","deud":"deudor","clien":"cliente",
    "refi":"refinanciamiento","comu":"comunicacion","asig":"asignacion",
    "grupo":"grupo","deta":"detalle","cabe":"cabecera","ante":"anterior",
    "conce":"concedida","diar":"diaria","mas":"masiva","sbs":"sbs","esbs":"sbs",
    "rep":"reporte","repo":"reporte","tasa":"tasa","intr":"interes","inte":"interes",
    "ajus":"ajuste","saldo":"saldo","cart":"cartera","carter":"cartera",
    "dvgo":"devengamiento","dvg":"devengamiento","devo":"devolucion","etap":"etapa",
    "concep":"concepto","gasto":"gasto","med":"medida","medi":"medida",
    "uni":"unidad","proc":"proceso","acci":"accion","lega":"legal",
    "auto":"automatica","aut":"autorizacion","fech":"fecha","trab":"trabajo",
    "rubr":"rubro","cuot":"cuota","cuent":"cuenta","cnta":"cuenta","fond":"fondo",
    "rol":"rol","empr":"empresa","afil":"afiliado","obli":"obligacion",
    "tipo":"tipo","seg":"seguro","segu":"seguro","cobr_judi":"cobranzajudicial",
    "tarj":"tarjeta","band":"banda","prvd":"proveedor","cdio":"credito",
    "tran":"transaccion","trans":"transaccion",
}

def expand_tbl_tokens(tbl_name: str) -> str:
    """Expande tokens del nombre de tabla a forma canonica espanola."""
    n = tbl_name.lower()
    # quitar prefijo de schema/grupo (crt, fct, sfct, cgtb, cttb, etc.)
    for pref in ("crtb","crt","cgtb","cttb","sfct","fctb","fct","intb","sgtb","notb","svtb","im","ratb","rctb"):
        if n.startswith(pref):
            n = n[len(pref):]
            break
    parts = n.split("_")
    out = []
    for p in parts:
        out.append(TBL_TOKEN_ABBR.get(p, p))
    return "".join(out)

def agg_tokens(agg: str) -> set:
    """Tokens semanticos del agg_type."""
    s = re.sub(r'_?[Tt]ype$','',agg)
    s = re.sub(r'([A-Z])', r' \1', s).lower().strip()
    return set(t for t in s.split() if len(t) >= 3)

def name_score(agg: str, tbl: str) -> float:
    """Score 0..1 de overlap nombre type vs nombre tabla legacy expandido."""
    expanded = expand_tbl_tokens(tbl)
    toks = agg_tokens(agg)
    if not toks: return 0
    hits = sum(1 for t in toks if t in expanded)
    return hits / len(toks)

EXPECTED_DB = {
    # Reglas de preferencia por familia de agg_type
    "credito":"dbCR", "cobranza":"dbCR", "judicial":"dbCR", "garantia":"dbCR",
    "recuperacion":"dbCR", "convenio":"dbCR", "obligacion":"dbCR", "solidario":"dbCR",
    "saldo":"dbCR", "cartera":"dbCR", "plzpag":"dbCR", "abono":"dbCR",
    "rubro":"dbCR", "sobrante":"dbCR", "tasa":"dbCR", "calificacion":"dbCR",
    "devengamiento":"dbCR", "documento":"dbCR", "flujo":"dbCR", "gestion":"dbCR",
    "liquidacion":"dbCR", "movimiento":"dbCR", "etapa":"dbCR", "medida":"dbCR",
    "unidad":"dbCR", "proceso":"dbCR", "accion":"dbCR", "legal":"dbCR",
    "automatica":"dbCR", "autorizacion":"dbCR", "fecha":"dbCR", "concepto":"dbCR",
    "informacion":"dbCR", "referencia":"dbCR", "comunicacion":"dbCR",
    "transaccion":"dbCR", "cancelacion":"dbCR", "desembolso":"dbCR",
    "devolucion":"dbCR", "precalificacion":"dbCR", "refinanciamiento":"dbCR",
    "operacion":"dbCR", "rol":"dbCR", "ajuste":"dbCR", "vinculado":"dbCR",
    "deudor":"dbCR", "cliente":"dbCR", "persona":"dbCR", "cuota":"dbCR",
    "cuenta":"dbCR", "anterior":"dbCR", "concedida":"dbCR", "diaria":"dbCR",
    "masiva":"dbCR", "reporte":"dbCR", "sujeto":"dbCR", "riesgo":"dbCR",
    "garante":"dbCR", "codeudor":"dbCR", "real":"dbCR", "asignacion":"dbCR",
    "auxiliar":"dbCR", "extraordinario":"dbCR",
}

def expected_db_for(agg: str) -> str | None:
    toks = agg_tokens(agg)
    for t in toks:
        if t in EXPECTED_DB:
            return EXPECTED_DB[t]
    return None

specs = []
for entry in oracle_data:
    agg = entry["agg_type"]
    dest = entry["dest_table"]
    if not entry["dest_exists"] or not entry["dest_columns"]:
        continue
    o_cols = [c["name"] for c in entry["dest_columns"]]
    o_pk = entry["dest_pk"]

    exp_db = expected_db_for(agg)
    a_toks = agg_tokens(agg)

    candidates = []
    for key, meta in legacy_meta.items():
        if meta["tbl"].lower() in EXCLUDE_TBLS:
            continue

        # combinar 3 senales:
        n_sc = name_score(agg, meta["tbl"])    # 0..1 overlap nombre
        c_sc = table_score(o_cols, meta["cols"]) # 0..1 overlap cols
        db_bonus = 0.20 if (exp_db and meta["db"]==exp_db) else 0
        prefix_bonus = 0.10 if meta["tbl"].lower().startswith(("crt","crtb")) and exp_db=="dbCR" else 0

        # solo cuenta como candidato si nombre tabla comparte algun token
        if n_sc == 0 and c_sc < 0.3:
            continue

        # peso fuerte al name_score
        total = n_sc * 5 + c_sc * 1 + db_bonus + prefix_bonus
        candidates.append((total, n_sc, c_sc, key, meta))

    candidates.sort(reverse=True, key=lambda x: x[0])
    top = candidates[:5]
    if not top:
        specs.append({"agg":agg,"dest":dest,"status":"NO_LEGACY_MATCH"})
        continue

    best_total, best_n, best_c, best_key, best_meta = top[0]
    # confianza compuesta
    confidence = "HIGH" if best_n >= 0.5 and best_c >= 0.4 else \
                 "MEDIUM" if best_n >= 0.5 or (best_n >= 0.3 and best_c >= 0.3) else \
                 "LOW"

    # Mapping columna a columna usando best_match
    leg_cols = [c["name"] for c in best_meta["cols"]]
    leg_types_map = {c["name"]:c["type"] for c in best_meta["cols"]}
    dest_match = []  # [(oracle_col, legacy_col)]
    for oc in o_cols:
        lc, s = best_match(oc, leg_cols)
        if lc:
            dest_match.append((oc, lc, round(s,2)))

    # PK legacy: usar PK declarado, o si vacio, primeros lkey desde PK Oracle mapeada
    lkey = best_meta["pk"][:]
    if not lkey:
        # tomar columnas legacy mapeadas desde PK Oracle
        for opk in o_pk:
            lc, s = best_match(opk, leg_cols)
            if lc and lc not in lkey:
                lkey.append(lc)
        if not lkey:
            # fallback: primer column del legacy
            lkey = [leg_cols[0]] if leg_cols else []

    # Payload columns: usar todas las legacy (subset) o solo las mapeadas
    pcols = list({lc for _,lc,_ in dest_match})
    # asegurar que lkey esten en pcols
    for k in lkey:
        if k not in pcols:
            pcols.append(k)

    # Tipos lkey
    lkey_types = {k: leg_types_map.get(k,"NVARCHAR(50)") for k in lkey}

    # Mapping legacy->Oracle invertido para wrapper
    leg_to_ora = {lc: oc for oc, lc, _ in dest_match}

    specs.append({
        "agg": agg,
        "dest": dest,
        "dest_pk": o_pk,
        "ltbl": best_meta["tbl"],
        "ldb": best_meta["db"],
        "lkey": lkey,
        "lkey_types": lkey_types,
        "pcols": pcols,
        "dest_match": [(oc,lc) for oc,lc,_ in dest_match],
        "leg_to_ora": leg_to_ora,
        "name_score": round(best_n,3),
        "col_score": round(best_c,3),
        "total_score": round(best_total,3),
        "confidence": confidence,
        "candidates_top3": [{"tbl":f"{m['db']}.{m['tbl']}","total":round(t,3),"name":round(n,3),"col":round(c,3)} for t,n,c,_,m in top[:3]],
    })

with open("_cartera_specs.json","w",encoding="utf-8") as f:
    json.dump(specs, f, indent=2, ensure_ascii=False)

# Resumen
ok = [s for s in specs if "ltbl" in s]
miss = [s for s in specs if "status" in s]
n_high = sum(1 for s in ok if s.get("confidence")=="HIGH")
n_med  = sum(1 for s in ok if s.get("confidence")=="MEDIUM")
n_low  = sum(1 for s in ok if s.get("confidence")=="LOW")
print(f"\n=== AUTOMAP CARTERA ===")
print(f"  Total types         : {len(specs)}")
print(f"  Con match legacy    : {len(ok)}")
print(f"    HIGH   (cablear)  : {n_high}")
print(f"    MEDIUM (revisar)  : {n_med}")
print(f"    LOW    (no cablear): {n_low}")
print(f"  Sin candidato       : {len(miss)}")
print(f"\nSPECS guardados en _cartera_specs.json")

# CSV de revision
import csv
with open("_cartera_specs_review.csv","w",encoding="utf-8",newline="") as f:
    w = csv.writer(f)
    w.writerow(["confidence","agg_type","dest_table","ldb.ltbl","name_score","col_score","total","top2","top3"])
    for s in sorted(ok, key=lambda x: (x['confidence']!='HIGH', -x['total_score'])):
        cands = s.get("candidates_top3",[])
        w.writerow([s["confidence"], s["agg"], s["dest"], f"{s['ldb']}.{s['ltbl']}",
                    s["name_score"], s["col_score"], s["total_score"],
                    cands[1]["tbl"] if len(cands)>1 else "",
                    cands[2]["tbl"] if len(cands)>2 else ""])
print(f"CSV de revision: _cartera_specs_review.csv")

print(f"\nTop 5 HIGH:")
for s in sorted([x for x in ok if x['confidence']=='HIGH'], key=lambda x: -x['total_score'])[:5]:
    print(f"  {s['agg']:<40} -> {s['ldb']}.{s['ltbl']:<30}  n={s['name_score']} c={s['col_score']}")
print(f"\nLOW (revisar/no cablear):")
for s in [x for x in ok if x['confidence']=='LOW'][:10]:
    print(f"  {s['agg']:<40} -> {s['ldb']}.{s['ltbl']:<30}  n={s['name_score']} c={s['col_score']}")
