"""Diagnostico: por que ciertos TYPES de CARTERA no llegan a FCME_USER."""
import oracledb, json
from collections import defaultdict

ORA = dict(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
TYPES_NOK = [
    "FLUJOTRABAJOCREDITO_TYPE", "PERSONACREDITOTYPE", "LIQUIDACIONDIARIACREDITO_TYPE",
    "MOVIMIENTOCONTABLECREDITO_TYPE", "COSTOFINANCIEROCREDITO_TYPE", "CREDITOTYPE",
    "DESEMBOLSOCREDITO_TYPE", "PAGOSCREDITOTYPE", "REFINANCIAMIENTOCREDITOTYPE",
    "REFERENCIADEUDOR_TYPE", "DEVOLUCIONMASIVADETALLE_TYPE", "DEVOLUCIONMASIVA_TYPE",
    "PERSONACXPCXCTYPE", "REPORTESBSOPERACIONCANCELADA_TYPE",
    "REPORTESBSOPERACIONCONCEDIDA_TYPE", "REPORTESBSSALDOOPERACION_TYPE",
    "OPERACIONCONYUGAL_TYPE", "DETALLERECUPERACION_TYPE", "RECUPERACIONCONVENIO_TYPE",
    "RECUPERACIONCREDITO_TYPE", "TRANSACCIONRECUPERACION_TYPE",
    "REPORTESBSSUJETORIESGO_TYPE", "CUOTACREDITOTYPE", "SALDOCARTERADETALLE_TYPE",
    "PAGOCREDITO_TYPE", "SOLIDARIOCREDITO_TYPE", "ESTADOLEGALTYPE",
    "REPORTESBSGARANTECODEUDOR_TYPE", "AUXDATOSCOBROSADICIONALESTYPE",
    "GRUPOCREDITODETALLE_TYPE", "CUENTASENLEGALTYPE", "FECHASPROCESOTYPE",
]

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json", "r", encoding="utf-8") as f:
    specs = json.load(f)

# dest -> [(agg, ltbl)]
dest_to_agg = defaultdict(list)
for s in specs:
    dest_to_agg[s["dest"]].append((s["agg"], s["ltbl"]))

cn = oracledb.connect(**ORA)
c = cn.cursor()

print("=" * 80)
print("1) Wrappers que faltan / invalidos para los TYPES reportados")
print("=" * 80)

# usp_name (mismo algoritmo que _gen_cartera_wrappers.py)
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

for typ in TYPES_NOK:
    aggs = dest_to_agg.get(typ, [])
    print(f"\n[{typ}]")
    if not aggs:
        print("  (sin spec)")
        continue
    for agg, ltbl in aggs:
        sp = usp_name(agg)
        c.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_type='PROCEDURE' AND object_name=:1", [sp])
        r = c.fetchone()
        proc_status = r[0] if r else "MISSING"
        # CDC_INBOX_MODULE_CONFIG
        c.execute("SELECT SP_NAME, ACTIVE FROM CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE=:1", [agg])
        r2 = c.fetchone()
        cfg = f"SP={r2[0]} ACTIVE={r2[1]}" if r2 else "NO_CONFIG"
        # tabla destino existe?
        c.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name=:1", [typ])
        tbl_exists = c.fetchone()[0]
        # row count
        rc = "?"
        if tbl_exists:
            try:
                c.execute(f"SELECT COUNT(*) FROM FCME_USER.{typ}")
                rc = c.fetchone()[0]
            except Exception as e:
                rc = f"err({str(e)[:40]})"
        # errores recientes para este agg
        c.execute("""SELECT COUNT(*) FROM CDC_INBOX_ERRORS WHERE AGGREGATE_TYPE=:1""", [agg])
        n_err = c.fetchone()[0]
        # Pendientes (no procesados)
        c.execute("""SELECT COUNT(*) FROM CDC_INBOX WHERE AGGREGATE_TYPE=:1 AND PROCESSED=0""", [agg])
        n_pend = c.fetchone()[0]
        # Total inbox events
        c.execute("""SELECT COUNT(*) FROM CDC_INBOX WHERE AGGREGATE_TYPE=:1""", [agg])
        n_inbox = c.fetchone()[0]
        print(f"  agg={agg!r:<45} ltbl={ltbl}")
        print(f"    proc={sp:<30} status={proc_status}")
        print(f"    cfg : {cfg}")
        print(f"    tbl FCME_USER.{typ}: exists={bool(tbl_exists)} rows={rc}")
        print(f"    inbox events total={n_inbox}  pending={n_pend}  errors_logged={n_err}")

print("\n" + "=" * 80)
print("2) Estructura del dispatcher TRG_PROCESS_CDC_INBOX")
print("=" * 80)
c.execute("""SELECT text FROM all_source WHERE owner='FCME_USER'
             AND name='TRG_PROCESS_CDC_INBOX' AND type='TRIGGER' ORDER BY line""")
for (line,) in c.fetchall():
    print(line.rstrip())

print("\n" + "=" * 80)
print("3) Errores recientes en CDC_INBOX_ERRORS para los TYPES reportados")
print("=" * 80)
aggs_nok = [a for typ in TYPES_NOK for a, _ in dest_to_agg.get(typ, [])]
if aggs_nok:
    binds = ",".join(f":{i+1}" for i in range(len(aggs_nok)))
    c.execute(f"""SELECT AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,200), COUNT(*)
                  FROM CDC_INBOX_ERRORS WHERE AGGREGATE_TYPE IN ({binds})
                  GROUP BY AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,200)
                  ORDER BY 4 DESC FETCH FIRST 50 ROWS ONLY""", aggs_nok)
    for r in c.fetchall():
        print(f"  {r[0]:<45} {r[1]:<8} x{r[3]:<5} {r[2]}")

cn.close()
