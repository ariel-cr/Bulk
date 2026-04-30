"""Genera planilla de mapeos para los 34 aggregate_types pendientes.
Output: mapping_pendiente.csv con columnas:
  aggregate_type | tabla_legacy | columna_legacy | oracle_table | oracle_col | sugerencia | mapeo_final
"""
import pyodbc, oracledb, csv, re
from collections import defaultdict

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

AT_TO_TABLE = {
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

# tabla_legacy -> aggregate_types (de cdc_table_to_types)
c = sql("fcme_canonicos").cursor()
c.execute("SELECT source_table, aggregate_type_emit FROM dbo.cdc_table_to_types WHERE aggregate_type_emit IS NOT NULL")
at_to_legacies = defaultdict(set)
for r in c.fetchall():
    at_to_legacies[r.aggregate_type_emit].add(r.source_table)

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
    co.execute("""SELECT column_name FROM all_tab_columns
                  WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id""", t=tbl)
    return [r[0] for r in co.fetchall()]

# heuristic: match oracle col to legacy col best-effort
def normalize(s):
    return re.sub(r'[_]','', s).lower()

def best_match(oracle_col, legacy_cols):
    on = normalize(oracle_col)
    # exact case-insensitive
    for lc in legacy_cols:
        if normalize(lc) == on: return lc
    # contains
    for lc in legacy_cols:
        ln = normalize(lc)
        if on in ln or ln in on: return lc
    return ""

rows = []
for at, ot in AT_TO_TABLE.items():
    legacies = sorted(at_to_legacies.get(at, []))
    oracle_cols = get_oracle_cols(ot)
    legacy_cols_combined = []
    legacy_per = {}
    for lt in legacies:
        try:
            cols = get_legacy_cols(lt)
            legacy_per[lt] = cols
            legacy_cols_combined.extend(cols)
        except: pass
    legacy_cols_combined = list(dict.fromkeys(legacy_cols_combined))
    for oc in oracle_cols:
        if oc.upper() == "ID": continue
        suggestion = best_match(oc, legacy_cols_combined)
        legacies_str = ",".join(legacies)
        rows.append([at, legacies_str, ot, oc, suggestion, ""])

# CSV
out_csv = r"C:\Users\Usuario\Downloads\Bulk\mapping_pendiente.csv"
with open(out_csv, "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["aggregate_type","tablas_legacy","oracle_table","oracle_col","sugerencia_legacy_col","mapeo_final"])
    w.writerows(rows)
print(f"CSV generado: {out_csv}")
print(f"  total filas: {len(rows)}")
print(f"  con sugerencia: {sum(1 for r in rows if r[4])}")
print(f"  sin sugerencia (requiere decision): {sum(1 for r in rows if not r[4])}")

# Markdown agrupado por aggregate_type
out_md = r"C:\Users\Usuario\Downloads\Bulk\mapping_pendiente.md"
with open(out_md, "w", encoding="utf-8") as f:
    f.write("# Mapeo pendiente legacy -> Oracle FCME_USER\n\n")
    f.write("Para cada `oracle_col`, completar `mapeo_final` con el nombre de la columna legacy correcta.\n")
    f.write("Si no existe en legacy, escribir `NULL` o `<expresion>`.\n\n")
    by_at = defaultdict(list)
    for r in rows: by_at[r[0]].append(r)
    for at in sorted(by_at):
        items = by_at[at]
        f.write(f"## {at}  ->  `{items[0][2]}`\n")
        f.write(f"Tablas legacy origen: `{items[0][1]}`\n\n")
        f.write("| oracle_col | sugerencia | mapeo_final |\n")
        f.write("|---|---|---|\n")
        for r in items:
            f.write(f"| `{r[3]}` | `{r[4] or '?'}` |  |\n")
        f.write("\n")
print(f"MD generado: {out_md}")

# Resumen consola
print("\n== Aggregate types con todas las columnas mapeadas automaticamente ==")
auto = []
partial = []
manual = []
for at in sorted(set(r[0] for r in rows)):
    items = [r for r in rows if r[0]==at]
    nfound = sum(1 for r in items if r[4])
    if nfound == len(items): auto.append(at)
    elif nfound == 0: manual.append(at)
    else: partial.append((at, nfound, len(items)))
print(f"  AUTO ({len(auto)}): {auto}")
print(f"  PARCIAL ({len(partial)}):")
for at,n,t in partial[:15]: print(f"    {at}: {n}/{t}")
print(f"  MANUAL TOTAL ({len(manual)}): {manual[:10]}")
