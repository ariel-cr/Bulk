"""Deploy Flujo 2 Cartera completo:
   1) Oracle TRG_OUTBOX_<X> en FCME_USER.<TYPE> (91)
   2) Legacy sp_<Type>_CRUD en cada ldb (91, skip si preexistente)
   3) Canonicos wrappers usp_inbox_<agg> (91)
   4) cdc_inbox_module_config canonicos entries (91, active=0)

Modos:
  --gen      : solo genera SQL files (no toca BDs)
  --apply    : ejecuta los CREATE / INSERT
  --check    : valida cuantos existen y compilan
  --activate : UPDATE active=1 en cdc_inbox_module_config (despues de smoke E2E)

Anti-zombi:
  - autocommit=True, LOCK_TIMEOUT 5000
  - 1 conexion por BD, cleanup atexit + signals
"""
import sys, json, argparse, signal, atexit
import pyodbc, oracledb
from collections import Counter

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

LOG=r"C:\Users\Usuario\Downloads\Bulk\_deploy_cartera_f2_out.txt"
sys.stdout=Tee(sys.__stdout__, open(LOG,"w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
ORA={'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}
_conns={}; _orcl=None
def sqlcn(db):
    if db in _conns: return _conns[db]
    cn=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
    cn.timeout=30
    c=cn.cursor(); c.execute("SET LOCK_TIMEOUT 5000")
    _conns[db]=cn
    return cn
def oracn():
    global _orcl
    if _orcl is None:
        _orcl=oracledb.connect(**ORA); _orcl.autocommit=True
    return _orcl

def cleanup():
    print("\n[cleanup]")
    global _orcl
    for db,cn in list(_conns.items()):
        try: cn.close(); print(f"  closed {db}")
        except: pass
    _conns.clear()
    if _orcl:
        try: _orcl.close(); print("  closed Oracle"); _orcl=None
        except: pass

atexit.register(cleanup)
for s in (signal.SIGINT, signal.SIGTERM):
    signal.signal(s, lambda *a:(cleanup(),sys.exit(130)))

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS=json.load(f)

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

def base_for(agg):
    """Devuelve nombre Type para sp_<Type>_CRUD (camelCase con primer letra UPPER)."""
    if agg.endswith("_type"):
        b=agg[:-5]+"Type"
    elif agg.endswith("Type"):
        b=agg
    else:
        b=agg
    return b[0].upper()+b[1:]

# ===========================================================================
# 1) Oracle TRG_OUTBOX_<X>
# ===========================================================================
def gen_oracle_triggers():
    out=[]
    for s in SPECS:
        if "dest" not in s: continue
        agg=s["agg"]; dest=s["dest"]
        dest_pk=s.get("dest_pk") or []
        dest_match=s.get("dest_match") or []
        if not dest_match:
            continue
        # PK Oracle: dest_pk[0] o primer dest_match[0][0]
        opk = dest_pk[0] if dest_pk else dest_match[0][0]
        # Cols a incluir en payload: union de dest_pk y oracle cols del dest_match
        ora_cols=[]
        for col in dest_pk:
            if col not in ora_cols: ora_cols.append(col)
        for ocol, lcol in dest_match:
            if ocol not in ora_cols: ora_cols.append(ocol)
        json_new=", ".join(f"'{c}' VALUE :NEW.{c}" for c in ora_cols)
        json_old=", ".join(f"'{c}' VALUE :OLD.{c}" for c in ora_cols)

        trg=trg_name(agg)
        ddl=f"""CREATE OR REPLACE TRIGGER FCME_USER.{trg}
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."{dest}"
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.{opk});
        v_payload := JSON_OBJECT({json_new});
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.{opk});
        v_payload := JSON_OBJECT({json_new});
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.{opk});
        v_payload := JSON_OBJECT({json_old});
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('{agg}', v_pk, v_event, v_payload, 'FCME_USER.{dest}', SYSTIMESTAMP);
END;"""
        out.append({"agg":agg,"trg":trg,"dest":dest,"ddl":ddl})
    return out

# ===========================================================================
# 2) Legacy sp_<Type>_CRUD
# ===========================================================================
def gen_legacy_sps():
    out=[]
    for s in SPECS:
        if "ltbl" not in s: continue
        agg=s["agg"]; ldb=s["ldb"]; ltbl=s["ltbl"]
        lkey=s.get("lkey") or []
        lkey_types=s.get("lkey_types") or {}
        if not lkey:
            continue

        sp_name=f"sp_{base_for(agg)}_CRUD"
        # Los tipos de las cols PK
        types=[lkey_types.get(c,"NVARCHAR(50)") for c in lkey]
        pk_params=", ".join(f"@{c} NVARCHAR(50) = NULL" for c in lkey)
        declares="\n        ".join(f"DECLARE @{c}_t {t} = TRY_CAST(@{c} AS {t});" for c,t in zip(lkey,types))
        null_check=" OR ".join(f"@{c}_t IS NULL" for c in lkey)
        pk_match=" AND ".join(f"[{c}] = @{c}_t" for c in lkey)
        cols_q=",".join(f"[{c}]" for c in lkey)
        vals_q=",".join(f"@{c}_t" for c in lkey)
        ddl=f"""CREATE OR ALTER PROCEDURE dbo.{sp_name}
    @Accion CHAR(1),
    {pk_params}
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        {declares}
        IF {null_check} RETURN;
        IF @Accion = 'D'
            DELETE FROM dbo.[{ltbl}] WHERE {pk_match};
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.[{ltbl}] WHERE {pk_match})
            BEGIN
                BEGIN TRY
                    INSERT INTO dbo.[{ltbl}] ({cols_q}) VALUES ({vals_q});
                END TRY
                BEGIN CATCH
                    RETURN;
                END CATCH
            END
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END"""
        out.append({"agg":agg,"sp":sp_name,"ldb":ldb,"ltbl":ltbl,"ddl":ddl})
    return out

# ===========================================================================
# 3) Canonicos wrappers usp_inbox_<agg>
# ===========================================================================
def gen_canonicos_wrappers():
    out=[]
    for s in SPECS:
        if "ltbl" not in s: continue
        agg=s["agg"]; ldb=s["ldb"]
        lkey=s.get("lkey") or []
        leg_to_ora=s.get("leg_to_ora") or {}
        if not lkey:
            continue

        wrapper=f"usp_inbox_{agg}"
        sp_name=f"sp_{base_for(agg)}_CRUD"
        decls=""
        pass_args=""
        for lcol in lkey:
            ocol=leg_to_ora.get(lcol)
            if ocol is None and lcol=='co_empr':
                decls+=f"        DECLARE @{lcol} NVARCHAR(50) = '1';\n"
            elif ocol is None:
                decls+=f"        DECLARE @{lcol} NVARCHAR(50) = NULL;\n"
            else:
                decls+=f"        DECLARE @{lcol} NVARCHAR(50) = JSON_VALUE(@payload, '$.{ocol}');\n"
            pass_args+=f"@{lcol}=@{lcol}, "
        pass_args=pass_args.rstrip(", ")

        ddl=f"""CREATE OR ALTER PROCEDURE dbo.{wrapper}
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
{decls}
        EXEC {ldb}.dbo.{sp_name} @Accion=@accion, {pass_args};
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper {agg}: ' + ERROR_MESSAGE());
    END CATCH
END"""
        out.append({"agg":agg,"wrapper":wrapper,"sp":sp_name,"ldb":ldb,"ddl":ddl})
    return out

def main():
    ap=argparse.ArgumentParser()
    g=ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--gen", action="store_true")
    g.add_argument("--apply", action="store_true")
    g.add_argument("--check", action="store_true")
    g.add_argument("--activate", action="store_true")
    args=ap.parse_args()

    ora_trgs=gen_oracle_triggers()
    leg_sps=gen_legacy_sps()
    can_wrappers=gen_canonicos_wrappers()

    print(f"Generados:")
    print(f"  Oracle TRG_OUTBOX     : {len(ora_trgs)}")
    print(f"  Legacy sp_<Type>_CRUD : {len(leg_sps)}")
    print(f"  Canonicos wrappers    : {len(can_wrappers)}")

    if args.gen:
        # Solo guardar SQL files
        with open(r"C:\Users\Usuario\Downloads\Bulk\cartera_f2_oracle_triggers.sql","w",encoding="utf-8") as f:
            f.write("/* Cartera F2 - Oracle TRG_OUTBOX_<X> */\n\n")
            for t in ora_trgs:
                f.write(f"/* {t['agg']} -> {t['trg']} on FCME_USER.{t['dest']} */\n")
                f.write(t["ddl"]+"\n/\n\n")
        with open(r"C:\Users\Usuario\Downloads\Bulk\cartera_f2_legacy_sps.sql","w",encoding="utf-8") as f:
            f.write("/* Cartera F2 - Legacy sp_<Type>_CRUD */\n\n")
            cur_db=None
            for sp in sorted(leg_sps, key=lambda x:(x["ldb"],x["sp"])):
                if sp["ldb"]!=cur_db:
                    f.write(f"\n--- USE [{sp['ldb']}]; GO ---\n\n")
                    cur_db=sp["ldb"]
                f.write(f"/* {sp['agg']} -> {sp['ldb']}.dbo.{sp['sp']} (target: {sp['ltbl']}) */\n")
                f.write(sp["ddl"]+"\nGO\n\n")
        with open(r"C:\Users\Usuario\Downloads\Bulk\cartera_f2_canonicos_wrappers.sql","w",encoding="utf-8") as f:
            f.write("/* Cartera F2 - Canonicos wrappers usp_inbox_<agg> */\n\n")
            for w in can_wrappers:
                f.write(f"/* {w['agg']} -> dbo.{w['wrapper']} -> {w['ldb']}.dbo.{w['sp']} */\n")
                f.write(w["ddl"]+"\nGO\n\n")
        print("\nArchivos SQL generados:")
        print("  cartera_f2_oracle_triggers.sql")
        print("  cartera_f2_legacy_sps.sql")
        print("  cartera_f2_canonicos_wrappers.sql")
        return

    if args.apply:
        # 1) Oracle TRG_OUTBOX
        print("\n[1/4] Oracle TRG_OUTBOX")
        oc=oracn().cursor()
        n_ok=n_err=0
        for t in ora_trgs:
            try:
                oc.execute(t["ddl"])
                oc.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_type='TRIGGER' AND object_name=:1", [t["trg"]])
                r=oc.fetchone()
                st=r[0] if r else "?"
                if st=="VALID":
                    n_ok+=1
                else:
                    print(f"  WARN {t['trg']:<32} status={st}")
                    oc.execute("SELECT line, position, SUBSTR(text,1,160) FROM all_errors WHERE owner='FCME_USER' AND name=:1 AND type='TRIGGER'",[t["trg"]])
                    for ln,pos,txt in oc.fetchall()[:3]:
                        print(f"     L{ln}:{pos} {txt}")
                    n_err+=1
            except Exception as e:
                print(f"  ERR {t['trg']}: {str(e)[:200]}")
                n_err+=1
        print(f"  OK={n_ok} ERR={n_err}")

        # 2) Legacy sp_<Type>_CRUD
        print("\n[2/4] Legacy sp_<Type>_CRUD")
        n_ok=n_skip=n_err=0
        for sp in leg_sps:
            cn=sqlcn(sp["ldb"]); c=cn.cursor()
            # Check si existe preexistente
            c.execute("SELECT COUNT(*) FROM sys.objects WHERE type='P' AND name=?", sp["sp"])
            if c.fetchone()[0]>0:
                # ya existe - no tocar (regla feedback_team_wrappers)
                print(f"  SKIP_PREEXIST {sp['ldb']}.{sp['sp']}")
                n_skip+=1
                continue
            try:
                c.execute(sp["ddl"])
                n_ok+=1
            except Exception as e:
                print(f"  ERR {sp['ldb']}.{sp['sp']}: {str(e)[:160]}")
                n_err+=1
        print(f"  OK={n_ok} SKIP={n_skip} ERR={n_err}")

        # 3) Canonicos wrappers
        print("\n[3/4] Canonicos wrappers usp_inbox_<agg>")
        cn=sqlcn("fcme_canonicos"); c=cn.cursor()
        n_ok=n_err=0
        for w in can_wrappers:
            try:
                c.execute(w["ddl"])
                n_ok+=1
            except Exception as e:
                print(f"  ERR {w['wrapper']}: {str(e)[:160]}")
                n_err+=1
        print(f"  OK={n_ok} ERR={n_err}")

        # 4) cdc_inbox_module_config entries (active=0)
        print("\n[4/4] cdc_inbox_module_config (active=0)")
        n_ins=n_skip=0
        for w in can_wrappers:
            c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE aggregate_type=?", w["agg"])
            if c.fetchone()[0]>0:
                n_skip+=1; continue
            c.execute("""INSERT INTO dbo.cdc_inbox_module_config
                         (aggregate_type, sp_name, target_db, module_name, active, created_at, updated_at)
                         VALUES (?, ?, ?, 'CARTERA', 0, SYSDATETIME(), SYSDATETIME())""",
                      w["agg"], f"dbo.{w['wrapper']}", w["ldb"])
            n_ins+=1
        print(f"  INSERT={n_ins} SKIP={n_skip}")

    if args.check:
        # Que existe actualmente
        oc=oracn().cursor()
        c=sqlcn("fcme_canonicos").cursor()
        # Oracle TRG_OUTBOX
        names=[t["trg"] for t in ora_trgs]
        ph=",".join(f":{i+1}" for i in range(len(names)))
        oc.execute(f"SELECT COUNT(*), SUM(CASE WHEN status='VALID' THEN 1 ELSE 0 END) FROM all_objects WHERE owner='FCME_USER' AND object_type='TRIGGER' AND object_name IN ({ph})", names)
        r=oc.fetchone()
        print(f"  Oracle TRG_OUTBOX: {r[0]}/{len(names)} existen, {r[1]} VALID")
        # Canonicos wrappers
        wn=[f"usp_inbox_{s['agg']}" for s in SPECS if 'ltbl' in s]
        ph2=",".join("?"*len(wn))
        c.execute(f"SELECT COUNT(*) FROM sys.objects WHERE type='P' AND name IN ({ph2})", *wn)
        print(f"  Canonicos wrappers: {c.fetchone()[0]}/{len(wn)} existen")
        # module_config
        c.execute("SELECT COUNT(*), SUM(CAST(active AS INT)) FROM dbo.cdc_inbox_module_config WHERE module_name='CARTERA'")
        r=c.fetchone()
        print(f"  module_config CARTERA: {r[0]} entries, {r[1]} con active=1")

    if args.activate:
        c=sqlcn("fcme_canonicos").cursor()
        c.execute("UPDATE dbo.cdc_inbox_module_config SET active=1, updated_at=SYSDATETIME() WHERE module_name='CARTERA'")
        print(f"  Activated rows: {c.rowcount}")

if __name__=="__main__":
    main()
