"""Genera cartera_triggers_dump.sql con F1+F2 en formato similar a nomina_triggers_dump.sql.
F1: SQL Server CREATE TRIGGER (sin wrapping EXEC)
F2: Oracle CREATE OR REPLACE TRIGGER
"""
import json
from collections import defaultdict, Counter

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

# === F1 triggers (uno por tabla legacy unica) ===
groups=defaultdict(list)
for s in SPECS:
    if "ltbl" not in s: continue
    groups[(s["ldb"], s["ltbl"])].append(s)

def f1_trigger_body(db, tbl, entries):
    aggs=[e["agg"] for e in entries]
    base=entries[0]
    lkey=base.get("lkey") or []
    pcols=[]
    for e in entries:
        for c in e.get("pcols",[]):
            if c not in pcols: pcols.append(c)
    if not lkey and pcols:
        lkey=[pcols[0]]
    if not lkey:
        return None

    if len(lkey)>1:
        agg_id_i="CONCAT_WS('|'," + ",".join(f"CONVERT(NVARCHAR(200), i.[{k}])" for k in lkey) + ")"
        agg_id_d=agg_id_i.replace("i.[","d.[")
    else:
        agg_id_i=f"CONVERT(NVARCHAR(200), i.[{lkey[0]}])"
        agg_id_d=f"CONVERT(NVARCHAR(200), d.[{lkey[0]}])"
    pcols_q=",".join(f"x.[{c}]" for c in pcols)
    types_values=",".join(f"(N'{a}')" for a in aggs)

    return f"""IF OBJECT_ID(N'dbo.trg_outbox_{tbl}', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_{tbl};
GO
CREATE TRIGGER dbo.trg_outbox_{tbl}
ON dbo.[{tbl}]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES {types_values};

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_i}, tt.t, @op,
            (SELECT {pcols_q} FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{db}.dbo.{tbl}', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_d}, tt.t, N'DELETE',
            (SELECT {pcols_q} FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{db}.dbo.{tbl}', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO"""

# === F2 Oracle triggers ===
def f2_trigger_body(spec):
    agg=spec["agg"]; dest=spec["dest"]
    dest_pk=spec.get("dest_pk") or []
    dest_match=spec.get("dest_match") or []
    if not dest_match: return None
    opk=dest_pk[0] if dest_pk else dest_match[0][0]
    ora_cols=[]
    for c in dest_pk:
        if c not in ora_cols: ora_cols.append(c)
    for ocol, lcol in dest_match:
        if ocol not in ora_cols: ora_cols.append(ocol)
    json_new=", ".join(f"'{c}' VALUE :NEW.{c}" for c in ora_cols)
    json_old=", ".join(f"'{c}' VALUE :OLD.{c}" for c in ora_cols)
    trg=trg_name(agg)
    return f"""/* --- {trg}  ON FCME_USER.{dest} --- */
CREATE OR REPLACE
TRIGGER FCME_USER.{trg}
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
END;
/"""

# === Build dump ===
out=[]
out.append("""/* ============================================================
   DUMP TRIGGERS CARTERA (F1 + F2) - DDL completo
   Snapshot generado del estado actual de las BDs
   ============================================================ */
""")

# F1
out.append("\n/* ############################################################")
out.append("   FLUJO 1 - Cartera Legacy (dbCR/dbFC/dbCG/dbCT) -> fcme_canonicos.cdc_outbox")
out.append("   ############################################################ */\n")

n_f1=0
dbs_f1=sorted(set(k[0] for k in groups))
for ldb in dbs_f1:
    grp=[(k,v) for k,v in groups.items() if k[0]==ldb]
    out.append(f"\n/* ----- BD: {ldb} ({len(grp)} triggers) ----- */")
    out.append(f"USE [{ldb}];")
    out.append("GO\n")
    out.append(f"/* TOTAL F1 ({ldb}) Cartera: {len(grp)} triggers */\n")
    for (db, tbl), entries in sorted(grp, key=lambda x:x[0][1]):
        body=f1_trigger_body(db, tbl, entries)
        if not body:
            out.append(f"/* SKIPPED {db}.{tbl}: sin lkey */\n"); continue
        aggs=[e["agg"] for e in entries]
        comment=f"/* --- trg_outbox_{tbl}  ON dbo.{tbl} ({len(aggs)} type{'s' if len(aggs)>1 else ''}) ---"
        for a in aggs:
            comment+=f"\n      - {a}"
        comment+="\n*/"
        out.append(comment)
        out.append(body)
        out.append("")
        n_f1+=1

# F2
out.append("\n/* ############################################################")
out.append("   FLUJO 2 - Cartera FCME_USER -> FCME_USER.CDC_OUTBOX")
out.append("   ############################################################ */\n")
n_f2=0
for s in sorted(SPECS, key=lambda x: x.get("agg","")):
    body=f2_trigger_body(s)
    if not body: continue
    out.append(body)
    out.append("")
    n_f2+=1

out.insert(1, f"/* RESUMEN: F1={n_f1} triggers SQL Server, F2={n_f2} triggers Oracle */\n")

content="\n".join(out)
with open(r"C:\Users\Usuario\Downloads\Bulk\cartera_triggers_dump.sql","w",encoding="utf-8") as f:
    f.write(content)

print(f"OK")
print(f"  F1 (SQL Server): {n_f1} triggers")
print(f"  F2 (Oracle):     {n_f2} triggers")
print(f"  Total:           {n_f1+n_f2}")
print(f"  bytes:           {len(content):,}")
print(f"\nArchivo: cartera_triggers_dump.sql")
