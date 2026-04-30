"""Verifica que llego a FCME_USER de los inserts que hizo el usuario."""
import oracledb, json
ORA = dict(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
cn = oracledb.connect(**ORA); c = cn.cursor()

# Lista de los 32 TYPES y sus aggs
TYPES_NOK = [
    "FLUJOTRABAJOCREDITO_TYPE","PERSONACREDITOTYPE","LIQUIDACIONDIARIACREDITO_TYPE",
    "MOVIMIENTOCONTABLECREDITO_TYPE","COSTOFINANCIEROCREDITO_TYPE","CREDITOTYPE",
    "DESEMBOLSOCREDITO_TYPE","PAGOSCREDITOTYPE","REFINANCIAMIENTOCREDITOTYPE",
    "REFERENCIADEUDOR_TYPE","DEVOLUCIONMASIVADETALLE_TYPE","DEVOLUCIONMASIVA_TYPE",
    "PERSONACXPCXCTYPE","REPORTESBSOPERACIONCANCELADA_TYPE",
    "REPORTESBSOPERACIONCONCEDIDA_TYPE","REPORTESBSSALDOOPERACION_TYPE",
    "OPERACIONCONYUGAL_TYPE","DETALLERECUPERACION_TYPE","RECUPERACIONCONVENIO_TYPE",
    "RECUPERACIONCREDITO_TYPE","TRANSACCIONRECUPERACION_TYPE",
    "REPORTESBSSUJETORIESGO_TYPE","CUOTACREDITOTYPE","SALDOCARTERADETALLE_TYPE",
    "PAGOCREDITO_TYPE","SOLIDARIOCREDITO_TYPE","ESTADOLEGALTYPE",
    "REPORTESBSGARANTECODEUDOR_TYPE","AUXDATOSCOBROSADICIONALESTYPE",
    "GRUPOCREDITODETALLE_TYPE","CUENTASENLEGALTYPE","FECHASPROCESOTYPE",
]

# columnas de CDC_INBOX_ERRORS (descubrir el nombre del timestamp)
c.execute("""SELECT column_name FROM all_tab_columns
             WHERE owner='FCME_USER' AND table_name='CDC_INBOX_ERRORS'
             ORDER BY column_id""")
err_cols = [r[0] for r in c.fetchall()]
print("CDC_INBOX_ERRORS cols :", err_cols)
ts_col = next((cc for cc in err_cols if cc in ('ERROR_DATE','CREATED_AT','PROCESSED_AT','TIMESTAMP')), None)
print(f"timestamp col detectada: {ts_col}\n")

print("="*100)
print("PASO A) ROWS POR TYPE en FCME_USER")
print("="*100)
for t in sorted(TYPES_NOK):
    try:
        c.execute(f"SELECT COUNT(*) FROM FCME_USER.{t}")
        n = c.fetchone()[0]
        flag = "  <-- TIENE DATOS" if n > 0 else ""
        print(f"  {t:<45} rows={n}{flag}")
    except Exception as e:
        print(f"  {t:<45} ERR: {str(e)[:80]}")

print("\n" + "="*100)
print("PASO B) Eventos en CDC_INBOX por agg (los 32 TYPES)")
print("="*100)
with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    specs = json.load(f)
agg_to_dest = {s["agg"]: s["dest"] for s in specs}
target_aggs = sorted({a for a, d in agg_to_dest.items() if d in TYPES_NOK})

for agg in target_aggs:
    c.execute("""SELECT COUNT(*), SUM(CASE WHEN PROCESSED=1 THEN 1 ELSE 0 END), MAX(CREATED_AT)
                 FROM CDC_INBOX WHERE AGGREGATE_TYPE=:1""", [agg])
    cnt, proc, mx = c.fetchone()
    if (cnt or 0) > 0:
        print(f"  {agg:<45} total={cnt}  proc={proc}  last={mx}  -> {agg_to_dest[agg]}")

print("\n" + "="*100)
print("PASO C) Errores recientes (las ultimas 24h) en CDC_INBOX_ERRORS")
print("="*100)
ts_filter = f"WHERE {ts_col} > SYSDATE - 1" if ts_col else ""
sql_err = f"""SELECT AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,250), COUNT(*)
              FROM CDC_INBOX_ERRORS {ts_filter}
              GROUP BY AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,250)
              ORDER BY 4 DESC FETCH FIRST 30 ROWS ONLY"""
c.execute(sql_err)
rows = c.fetchall()
if not rows:
    print("  (sin errores nuevos -> los wrappers no estan rechazando)")
else:
    for r in rows:
        print(f"  x{r[3]:<3}  {r[0]:<40}  {r[1]:<8}  {r[2]}")

print("\n" + "="*100)
print("PASO D) Detalle: liquidacionDiariaCredito_type llego al inbox y se 'proceso',")
print("        pero LIQUIDACIONDIARIACREDITO_TYPE tiene rows=0. Veamos PROCESSED y un payload:")
print("="*100)
for agg in ("liquidacionDiariaCredito_type", "movimientoContableCredito_type"):
    c.execute("""SELECT id, processed, processed_at, source_table, event_type, payload
                 FROM CDC_INBOX WHERE AGGREGATE_TYPE=:1
                 ORDER BY id DESC FETCH FIRST 2 ROWS ONLY""", [agg])
    print(f"\n  -- {agg}")
    for r in c.fetchall():
        pid, p, pat, src, evt, pay = r
        if hasattr(pay, "read"): pay = pay.read()
        print(f"    id={pid} proc={p} proc_at={pat} src={src} evt={evt}")
        print(f"    payload={pay[:300]}")

# Invocar manualmente el wrapper para ver SQLERRM real (rollback al final)
print("\n" + "="*100)
print("PASO E) Reinvocar wrappers de prueba (rollback) para capturar SQLERRM real")
print("="*100)
for agg, sp in (("liquidacionDiariaCredito_type","USP_INBOX_LIQDIARIACRED"),
                ("movimientoContableCredito_type","USP_INBOX_MOVCONTACRED")):
    c.execute("""SELECT id, source_table, event_type, payload FROM CDC_INBOX
                 WHERE AGGREGATE_TYPE=:1 ORDER BY id DESC FETCH FIRST 1 ROWS ONLY""", [agg])
    r = c.fetchone()
    if not r: continue
    pid, src, evt, pay = r
    if hasattr(pay,"read"): pay = pay.read()
    dest = agg_to_dest[agg]
    c.execute(f"SELECT COUNT(*) FROM FCME_USER.{dest}")
    before = c.fetchone()[0]
    res = c.var(oracledb.STRING, size=4000)
    c.execute(f"""
        BEGIN
            BEGIN
                {sp}(:p_id,:p_agg,:p_src,:p_evt,:p_pay);
                :result := 'OK';
            EXCEPTION WHEN OTHERS THEN
                :result := SUBSTR(SQLCODE||' :: '||SQLERRM,1,4000);
            END;
        END;""", p_id=pid, p_agg=agg, p_src=src, p_evt=evt, p_pay=pay, result=res)
    c.execute(f"SELECT COUNT(*) FROM FCME_USER.{dest}")
    after = c.fetchone()[0]
    print(f"  {agg:<40} ret={res.getvalue()[:200]:<80}  rows {before}->{after} (delta={after-before})")
    cn.rollback()

cn.close()
