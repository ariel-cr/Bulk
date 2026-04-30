"""Genera + aplica entries en FCME_USER.CDC_INBOX_MODULE_CONFIG para los 91 types de cartera.

Modos:
  --preview : solo muestra que SP_NAME usaria por type, no toca la BD
  --apply   : INSERT real con active=0 (seguro, no rutea hasta que activemos)
  --activate: UPDATE existing entries SET active=1 (despues de deploy de wrappers)

Convencion SP_NAME:
  USP_INBOX_<base.upper()>  truncado a 30 chars con dict de abreviaciones.
  El SP no necesita existir aun (active=0).
"""
import sys, json, argparse
import oracledb

ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)

CARTERA_TYPES = sorted({s["agg"] for s in SPECS})

# Abreviaciones para que SP_NAME entre en 30 chars
ABBR = [
    ("AUTORIZACIONCREDITODETALLE","AUTRCREDDETA"),
    ("COBRANZAJUDICIALDISTRIBUCION","COBJUDDIST"),
    ("COBRANZAJUDICIALDETALLE","COBJUDDETA"),
    ("AUXDATOSCOBROSADICIONALES","AUXDATOSCOB"),
    ("CALIFICACIONCARTERADETALLE","CALFCARTDETA"),
    ("DEVENGAMIENTOCARTERADETALLE","DVGOCARTDETA"),
    ("CONTABILIZACIONCREDITO","CONTABCRED"),
    ("DEVOLUCIONMASIVADETALLE","DEVOMASDETA"),
    ("REPORTESBSOPERACIONANTERIOR","RPTSBSOPANT"),
    ("REPORTESBSOPERACIONCANCELADA","RPTSBSOPCANC"),
    ("REPORTESBSOPERACIONCONCEDIDA","RPTSBSOPCONC"),
    ("REPORTESBSGARANTECODEUDOR","RPTSBSGRCOD"),
    ("REPORTESBSSALDOOPERACION","RPTSBSSALOP"),
    ("REPORTESBSSUJETORIESGO","RPTSBSSJTO"),
    ("REPORTESBSGARANTIAREAL","RPTSBSGARR"),
    ("REPORTESBSCABECERA","RPTSBSCAB"),
    ("REPORTESBSDETALLE","RPTSBSDETA"),
    ("LIQUIDACIONDIARIACREDITO","LIQDIARIACRED"),
    ("MOVIMIENTOCONTABLECREDITO","MOVCONTACRED"),
    ("GESTIONCOMUNICACIONCREDITO","GESTCOMUCRED"),
    ("GESTIONCOBRANZAASIGNACION","GESTCOBRASIG"),
    ("ESTADOCONVENIOCREDITO","ESTCONVCRED"),
    ("RUBROSCOBRANZADETALLE","RUBRCOBRDETA"),
    ("PRECALIFICACIONCREDITO","PRECALIFCRED"),
    ("REFINANCIAMIENTOCREDITO","REFICRED"),
    ("ETAPAJUDICIALCREDITO","ETAPJUDCRED"),
    ("CONCEPTOGASTOJUDICIAL","CONCEPGSTOJUD"),
    ("AUTORIZACIONCREDITO","AUTRCRED"),
    ("CONVENIOPAGOCREDITO","CONVPAGOCRED"),
    ("FLUJOTRABAJOCREDITO","FLUJOTRABCRED"),
    ("DESEMBOLSODEVOLUCION","DESEMBDEVO"),
    ("COSTOFINANCIEROCREDITO","COSTOFINCRED"),
    ("DETALLERECUPERACION","DETARECUP"),
    ("RECUPERACIONCONVENIO","RECUPCONV"),
    ("RECUPERACIONCREDITO","RECUPCRED"),
    ("TRANSACCIONRECUPERACION","TRANSRECUP"),
    ("CALIFICACIONCARTERA","CALFCART"),
    ("DEVENGAMIENTOCARTERA","DVGOCART"),
    ("DEVOLUCIONMASIVA","DEVOMAS"),
    ("DEVOLUCIONCREDITO","DEVOCRED"),
    ("DESEMBOLSOCREDITO","DESEMBCRED"),
    ("CANCELACIONCREDITO","CANCCRED"),
    ("CAUCIONCREDITO","CAUCCRED"),
    ("DOCUMENTOCREDITO","DOCCRED"),
    ("GARANTIACREDITO","GARCRED"),
    ("OPERACIONCONYUGAL","OPCONYU"),
    ("ABONOEXTRAORDINARIO","ABNEXTR"),
    ("REFERENCIACLIENTE","REFCLIE"),
    ("REFERENCIADEUDOR","REFDEUD"),
    ("OBLIGACIONROL","OBLIROL"),
    ("PERSONACREDITO","PERSCRED"),
    ("CUOTACREDITO","CUOTACRED"),
    ("PAGOSCREDITO","PAGOSCRED"),
    ("PLANPAGOAJUSTE","PLPGAJUS"),
    ("SOBRANTECAUCION","SOBRCAUC"),
    ("SOBRANTECREDITO","SOBRCRED"),
    ("SOBRANTEDISTRIBUCION","SOBRDIST"),
    ("SOLIDARIOCREDITO","SOLIDCRED"),
    ("TASAINTERESCREDITO","TASAINTCRED"),
    ("FECHASPROCESO","FCHPROC"),
    ("INFORMACIONLEGAL","INFOLEGAL"),
    ("MEDIDAJUDICIAL","MEDJUD"),
    ("UNIDADJUDICIAL","UNIJUD"),
    ("ESTADOLEGAL","ESTLEGAL"),
    ("CUENTASENLEGAL","CTASLEGAL"),
    ("CUENTACUOTAS","CTACUOTAS"),
    ("CUENTAPERSONAS","CTAPERS"),
    ("CUENTAPORCOBRAR","CTAPORCOBR"),
    ("CUENTAAUTOMATICA","CTAAUTO"),
    ("CUENTAAUTOMATICADETALLE","CTAAUTODETA"),
    ("CUENTACXPCXC","CTACXPCXC"),
    ("PERSONACXPCXC","PERSCXPCXC"),
    ("SALDOCXPCXC","SLDCXPCXC"),
    ("SALDOCARTERADETALLE","SLDCARTDETA"),
    ("SALDOCARTERA","SLDCART"),
    ("SALDOVINCULADO","SLDVINC"),
    ("PAGOCREDITO","PAGOCRED"),
    ("PLAZOVENCIDO","PLZOVENC"),
    ("PROCESOACCION","PROCACC"),
    ("RUBROCOBRANZA","RUBRCOBR"),
    ("CUOTACONVENIO","CUOTACONV"),
    ("PLANPAGO","PLPG"),
    ("CREDITO","CRED"),  # ya entra solo, pero por consistencia
    ("SEGUIMIENTOAUTORIZACION","SEGAUTR"),
    ("SEGUROCREDITO","SEGUCRED"),
    ("TIPOCREDITO","TIPOCRED"),
    ("TIPOSOBRANTE","TIPOSOBR"),
    ("CUENTA","CTA"),
    ("COBRANZAJUDICIAL","COBJUD"),
]

def usp_name(agg_type: str) -> str:
    s = agg_type
    if s.endswith("_type"): s = s[:-5]
    elif s.endswith("Type"): s = s[:-4]
    base = s.upper()
    name = f"USP_INBOX_{base}"
    if len(name) <= 30:
        return name
    # Abreviar
    for long_form, short_form in ABBR:
        if long_form in base:
            new_base = base.replace(long_form, short_form)
            new_name = f"USP_INBOX_{new_base}"
            if len(new_name) <= 30:
                return new_name
            base = new_base  # seguir abreviando
    name = f"USP_INBOX_{base}"
    return name[:30]

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--preview", action="store_true")
    g.add_argument("--apply",   action="store_true")
    g.add_argument("--activate", action="store_true")
    args = ap.parse_args()

    pairs = [(t, usp_name(t)) for t in CARTERA_TYPES]

    # Detectar duplicados / nombres > 30
    seen = {}
    for t, sp in pairs:
        if len(sp) > 30:
            print(f"  [!] {t} -> {sp} ({len(sp)} chars - SIN ESPACIO)")
        if sp in seen:
            print(f"  [!] COLISION sp_name '{sp}': {t} y {seen[sp]}")
        seen[sp] = t

    if args.preview:
        print(f"\n=== PREVIEW: 91 entries para CDC_INBOX_MODULE_CONFIG (active=0) ===")
        for i,(t,sp) in enumerate(pairs,1):
            mark = "" if len(sp) <= 30 else "   <<< OVERFLOW"
            print(f"  {i:3}. {t:<40} -> {sp:<32} ({len(sp)}){mark}")
        return

    orcl = oracledb.connect(**ORA); orcl.autocommit = False
    o = orcl.cursor()

    if args.apply:
        print(f"[*] APPLY: insert 91 entries con active=0")
        n_ins=0; n_skip=0
        for t, sp in pairs:
            o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE=:1", [t])
            if o.fetchone()[0] > 0:
                print(f"  SKIP {t} (ya existe)")
                n_skip += 1
                continue
            try:
                o.execute("INSERT INTO FCME_USER.CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE) VALUES (:1,:2,0)", [t, sp])
                print(f"  INS  {t:<40} -> {sp}")
                n_ins += 1
            except Exception as e:
                print(f"  ERR  {t}: {str(e)[:200]}")
        orcl.commit()
        print(f"\nResultado: INSERT={n_ins} SKIP={n_skip}")

    elif args.activate:
        print(f"[*] ACTIVATE: UPDATE active=1 para los 91 types")
        n=0
        for t, _sp in pairs:
            o.execute("UPDATE FCME_USER.CDC_INBOX_MODULE_CONFIG SET ACTIVE=1 WHERE AGGREGATE_TYPE=:1", [t])
            if o.rowcount > 0:
                n += 1
        orcl.commit()
        print(f"  Activated: {n}")

    orcl.close()

if __name__ == "__main__":
    main()
