"""Pre-check Flujo 2: verifica que la infraestructura comun existe.
Solo lectura."""
import sys, signal, atexit
import pyodbc, oracledb
class Tee:
    def __init__(self,*s):self.s=s
    def write(self,t):
        for x in self.s:
            try: x.write(t); x.flush()
            except: pass
    def flush(self):
        for x in self.s:
            try: x.flush()
            except: pass
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_verify_f2_out.txt","w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
ORA={'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

print("="*70)
print("PRE-CHECK INFRAESTRUCTURA FLUJO 2")
print("="*70)

# === ORACLE side ===
orcl=oracledb.connect(**ORA); o=orcl.cursor()
print("\n[Oracle FCME_USER]")
# CDC_OUTBOX
o.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name='CDC_OUTBOX'")
print(f"  CDC_OUTBOX exists: {'SI' if o.fetchone()[0] else 'NO'}")
o.execute("SELECT column_name, data_type FROM all_tab_columns WHERE owner='FCME_USER' AND table_name='CDC_OUTBOX' ORDER BY column_id")
for col, dt in o.fetchall():
    print(f"    {col} ({dt})")
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX")
print(f"  filas actuales: {o.fetchone()[0]:,}")

# Cuantos types de cartera ya tienen TRG_OUTBOX
import json
with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    specs=json.load(f)

types=[s["agg"] for s in specs]
# trg name from agg = TRG_OUTBOX_<X> with same abbrev as USP names
ABBR=[
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
def trg_name(agg):
    s=agg
    if s.endswith("_type"): s=s[:-5]
    elif s.endswith("Type"): s=s[:-4]
    base=s.upper()
    name=f"TRG_OUTBOX_{base}"
    if len(name)<=30: return name
    for lf,sf in ABBR:
        if lf in base:
            base=base.replace(lf,sf)
            if len(f"TRG_OUTBOX_{base}")<=30: return f"TRG_OUTBOX_{base}"
    return f"TRG_OUTBOX_{base}"[:30]

# Cuantos TRG_OUTBOX existen ya
trg_names=[trg_name(t) for t in types]
ph=",".join(f":{i+1}" for i in range(len(trg_names)))
o.execute(f"SELECT object_name, status FROM all_objects WHERE owner='FCME_USER' AND object_type='TRIGGER' AND object_name IN ({ph})", trg_names)
existing_trgs={r[0]: r[1] for r in o.fetchall()}
print(f"\n  TRG_OUTBOX_<X> Oracle preexistentes: {len(existing_trgs)}/{len(trg_names)}")
for n in trg_names:
    if n in existing_trgs:
        print(f"    EXISTS {n} ({existing_trgs[n]})")

# === SQL SERVER CANONICOS ===
print("\n[SQL Server fcme_canonicos]")
sg_cn=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=fcme_canonicos;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
c=sg_cn.cursor()

# cdc_inbox
c.execute("SELECT COUNT(*) FROM sys.tables WHERE name='cdc_inbox'")
print(f"  cdc_inbox exists: {'SI' if c.fetchone()[0] else 'NO'}")
c.execute("SELECT name, system_type_id FROM sys.columns WHERE object_id=OBJECT_ID('dbo.cdc_inbox') ORDER BY column_id")
for col, _ in c.fetchall():
    print(f"    {col}")
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
print(f"  filas actuales: {c.fetchone()[0]:,}")

# trg_process_cdc_inbox
c.execute("SELECT name, is_disabled FROM sys.triggers WHERE parent_id=OBJECT_ID('dbo.cdc_inbox')")
trgs=c.fetchall()
print(f"\n  Triggers en cdc_inbox: {len(trgs)}")
for nm, dis in trgs:
    print(f"    {nm} disabled={dis}")

# usp_process_cdc_inbox
c.execute("SELECT name FROM sys.objects WHERE type='P' AND name LIKE '%process_cdc_inbox%'")
procs=[r[0] for r in c.fetchall()]
print(f"  SPs *process_cdc_inbox*: {procs}")

# cdc_inbox_module_config
c.execute("SELECT COUNT(*) FROM sys.tables WHERE name='cdc_inbox_module_config'")
print(f"\n  cdc_inbox_module_config exists: {'SI' if c.fetchone()[0] else 'NO'}")
c.execute("SELECT name FROM sys.columns WHERE object_id=OBJECT_ID('dbo.cdc_inbox_module_config') ORDER BY column_id")
for r in c.fetchall():
    print(f"    {r[0]}")
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config")
print(f"  total entries: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE module_name='CARTERA'")
print(f"  entries module_name=CARTERA: {c.fetchone()[0]}")

# Cuantos wrappers usp_inbox_<agg> existen ya en canonicos
wrapper_names=[f"usp_inbox_{t}" for t in types]
ph2=",".join("?"*len(wrapper_names))
c.execute(f"SELECT name FROM sys.objects WHERE type='P' AND name IN ({ph2})", *wrapper_names)
existing_wrappers=[r[0] for r in c.fetchall()]
print(f"\n  Wrappers usp_inbox_<agg> preexistentes en canonicos: {len(existing_wrappers)}/{len(wrapper_names)}")

# Cuantos sp_<Type>_CRUD existen ya en cada legacy DB
print("\n[Legacy SPs sp_<Type>_CRUD]")
for ldb in ["dbCR","dbFC","dbCG","dbCT"]:
    cn=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={ldb};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
    cc=cn.cursor()
    cc.execute("SELECT name FROM sys.objects WHERE type='P' AND name LIKE 'sp\\_%' ESCAPE '\\' AND name LIKE '%\\_CRUD' ESCAPE '\\'")
    sps=[r[0] for r in cc.fetchall()]
    print(f"  {ldb}: {len(sps)} sp_*_CRUD existentes")
    cn.close()

orcl.close(); sg_cn.close()
print("\n=== PRE-CHECK COMPLETO ===")
