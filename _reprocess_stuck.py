"""Reprocesar los 20 eventos atascados (LIQUIDACIONDIARIACREDITO + MOVIMIENTOCONTABLECREDITO)
y mostrar el SQLERRM real ahora que el fallback PK-only fue eliminado."""
import oracledb, json
ORA = dict(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")

cn = oracledb.connect(**ORA); cn.autocommit = False
c = cn.cursor()

AGGS = ("liquidacionDiariaCredito_type","movimientoContableCredito_type")
SP_FOR = {"liquidacionDiariaCredito_type":"USP_INBOX_LIQDIARIACRED",
          "movimientoContableCredito_type":"USP_INBOX_MOVCONTACRED"}
DEST_FOR = {"liquidacionDiariaCredito_type":"LIQUIDACIONDIARIACREDITO_TYPE",
            "movimientoContableCredito_type":"MOVIMIENTOCONTABLECREDITO_TYPE"}

# limpiar errores viejos para que se vean los nuevos
c.execute("DELETE FROM CDC_INBOX_ERRORS WHERE AGGREGATE_TYPE IN (:1,:2)", AGGS)
print(f"old errors deleted: {c.rowcount}")

inserted_total = {a:0 for a in AGGS}
errors = []
for agg in AGGS:
    c.execute("""SELECT id, source_table, event_type, payload FROM CDC_INBOX
                 WHERE AGGREGATE_TYPE=:1 ORDER BY id""", [agg])
    rows = c.fetchall()
    print(f"\n[{agg}] {len(rows)} eventos a reprocesar -> {DEST_FOR[agg]}")
    sp = SP_FOR[agg]
    c.execute(f"SELECT COUNT(*) FROM FCME_USER.{DEST_FOR[agg]}")
    before = c.fetchone()[0]
    for inb_id, src, evt, pay in rows:
        if hasattr(pay,"read"): pay = pay.read()
        res = c.var(oracledb.STRING, size=4000)
        c.execute(f"""
            BEGIN
                BEGIN
                    {sp}(:p_id,:p_agg,:p_src,:p_evt,:p_pay);
                    :result := 'OK';
                EXCEPTION WHEN OTHERS THEN
                    :result := SUBSTR(SQLCODE||' :: '||SQLERRM,1,4000);
                END;
            END;""", p_id=inb_id, p_agg=agg, p_src=src, p_evt=evt, p_pay=pay, result=res)
    c.execute(f"SELECT COUNT(*) FROM FCME_USER.{DEST_FOR[agg]}")
    after = c.fetchone()[0]
    inserted_total[agg] = after - before
    print(f"  rows {before} -> {after}  (delta={after-before})")

cn.commit()

print("\n" + "="*100)
print("Errores capturados (con el patch nuevo, ya no se traga el SQLERRM)")
print("="*100)
c.execute("""SELECT AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,300), COUNT(*)
             FROM CDC_INBOX_ERRORS WHERE AGGREGATE_TYPE IN (:1,:2)
             GROUP BY AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,300)
             ORDER BY 4 DESC""", AGGS)
for r in c.fetchall():
    print(f"  x{r[3]:<3} {r[0]:<40} {r[1]:<8} {r[2]}")

print("\nResumen insertados:")
for a, n in inserted_total.items():
    print(f"  {a:<40} delta={n}")
cn.close()
