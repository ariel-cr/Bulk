"""Para cada TYPE problematico: toma 1 payload del CDC_INBOX y llama al wrapper
con SQLERRM expuesto para ver el error real que el WHEN OTHERS THEN NULL silencia."""
import oracledb, json
from collections import defaultdict

ORA = dict(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    specs = json.load(f)
agg_to_dest = {s["agg"]: s for s in specs}

# TYPES con events>0 pero rows=0 (los procesados sin insertar)
AGGS = [
    "solidarioCredito_type","estadoLegalType","reporteSBSGaranteCodeudor_type",
    "auxDatosCobrosAdicionalesType","grupoCreditoDetalle_type","cuentasEnLegalType",
    "fechasProcesoType",
]

cn = oracledb.connect(**ORA); cn.autocommit = False
c = cn.cursor()
print("Estrategia: invocar wrapper, contar rows ANTES y DESPUES, hacer rollback al final.\n")

for agg in AGGS:
    spec = agg_to_dest.get(agg)
    if not spec:
        print(f"[{agg}] sin spec, skip"); continue
    print(f"\n{'='*80}\n[AGG] {agg}  -> dest={spec['dest']}  ltbl={spec['ltbl']}\n{'='*80}")
    c.execute("""SELECT id, source_table, event_type, payload FROM CDC_INBOX
                 WHERE AGGREGATE_TYPE=:1 ORDER BY id FETCH FIRST 1 ROWS ONLY""", [agg])
    row = c.fetchone()
    if not row:
        print("  no inbox events"); continue
    inb_id, src, evt, payload = row
    if hasattr(payload,'read'): payload = payload.read()
    print(f"  inbox id={inb_id} src={src} evt={evt}")
    print(f"  payload[:250] = {payload[:250]}")
    # extraer JSON values del payload
    try:
        data = json.loads(payload) if isinstance(payload, str) else json.loads(payload.decode())
        print(f"  payload keys = {list(data.keys())[:20]}")
    except Exception as e:
        print(f"  json parse err: {e}")

    # Bloque PL/SQL que llama al wrapper SIN tragar el error
    plsql = f"""
    DECLARE
        v_err VARCHAR2(4000);
    BEGIN
        BEGIN
            {("USP_INBOX_" + spec['dest'].replace('_TYPE','').replace('TYPE',''))[:30]}(
                :p_id, :p_agg, :p_src, :p_evt, :p_payload
            );
            :result := 'OK';
        EXCEPTION WHEN OTHERS THEN
            :result := SQLERRM;
        END;
    END;
    """
    # Mejor: descubrir nombre real del wrapper desde CDC_INBOX_MODULE_CONFIG
    c.execute("SELECT SP_NAME FROM CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE=:1", [agg])
    sp_row = c.fetchone()
    if not sp_row:
        print("  sin SP_NAME en CDC_INBOX_MODULE_CONFIG"); continue
    sp_name = sp_row[0]
    plsql = f"""
    BEGIN
        BEGIN
            {sp_name}(:p_id, :p_agg, :p_src, :p_evt, :p_payload);
            :result := 'OK';
        EXCEPTION WHEN OTHERS THEN
            :result := SUBSTR(SQLCODE || ' :: ' || SQLERRM, 1, 4000);
        END;
    END;
    """
    res_var = c.var(oracledb.STRING, size=4000)
    try:
        c.execute(f"SELECT COUNT(*) FROM FCME_USER.{spec['dest']}")
        before = c.fetchone()[0]
    except Exception as e:
        print(f"  err count antes: {e}"); cn.rollback(); continue
    try:
        c.execute(plsql, p_id=inb_id, p_agg=agg, p_src=src, p_evt=evt,
                  p_payload=payload, result=res_var)
        ret = res_var.getvalue()
    except Exception as e:
        print(f"  ERR ejecutando: {str(e)[:300]}"); cn.rollback(); continue
    c.execute(f"SELECT COUNT(*) FROM FCME_USER.{spec['dest']}")
    after = c.fetchone()[0]
    delta = after - before
    print(f"  -> {sp_name}: ret={ret}  rows: {before} -> {after}  (delta={delta})")
    cn.rollback()

cn.close()
