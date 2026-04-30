"""Diagnostico del pipeline despues de los inserts de prueba.
Recorre: legacy tables -> fcme_canonicos.cdc_outbox -> Oracle CDC_INBOX -> targets / errors."""
import oracledb, pyodbc, json
from collections import defaultdict

DB_SQL = {"server":"10.35.3.64,1433","driver":"{SQL Server}","username":"sa","password":"YourPassword123"}
ORA = dict(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")

def sql(db):
    s=(f"DRIVER={DB_SQL['driver']};SERVER={DB_SQL['server']};DATABASE={db};UID={DB_SQL['username']};PWD={DB_SQL['password']}")
    return pyodbc.connect(s, autocommit=True)

# 19 tablas legacy fuente del test
SRC_TABLES = [
    ("dbCR","crtbcred_liqd_diar"),("dbCR","crtbcred_part"),("dbCR","crtbcred_prea_whts"),
    ("dbCR","crtbdeud_conv"),("dbCR","crtbdevo_masi_deta"),("dbCR","crtbdsal_oper"),
    ("dbCR","crtboper_dref_liqd"),("dbCR","crtbrecu_conv"),("dbCR","crtbrepo_sobr"),
    ("dbCR","crtbrngo_autr_cred"),("dbCR","crtbsald_cart_deta"),("dbCR","crtpagos"),
    ("dbCR","crtsolid"),
    ("dbCT","cttbesta_docu_inve"),("dbCT","cttbrepo_gene"),("dbCT","cttbtran_inve_auxi"),
    ("dbFC","fctbdeta_liqd_cred"),("dbFC","fctbdinf_liqd_cnta_sibs"),("dbFC","fctbproc_tseg_noti"),
]

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    specs = json.load(f)
# tabla legacy -> [aggregate_type emitidos]
src_to_aggs = defaultdict(list)
for s in specs:
    src_to_aggs[s["ltbl"]].append(s["agg"])

print("="*100)
print("PASO 1: Existen filas X*_NN nuevas en las tablas legacy? (inserts del test)")
print("="*100)
for db, tbl in SRC_TABLES:
    try:
        c = sql(db).cursor()
        c.execute(f"SELECT COUNT(*) FROM dbo.{tbl}")
        total = c.fetchone()[0]
        # buscar las filas que dejamos con prefijo X o data 2024-01-* en col fe_*
        print(f"  [{db}.{tbl}]  total filas = {total}")
    except Exception as e:
        print(f"  [{db}.{tbl}]  ERR: {str(e)[:120]}")

print("\n" + "="*100)
print("PASO 2: Hay triggers trg_outbox_* sobre cada tabla?")
print("="*100)
for db, tbl in SRC_TABLES:
    try:
        c = sql(db).cursor()
        c.execute("""SELECT t.name, t.is_disabled FROM sys.triggers t
                     JOIN sys.tables tb ON t.parent_id = tb.object_id
                     WHERE tb.name = ?""", tbl)
        rows = c.fetchall()
        if not rows:
            print(f"  [{db}.{tbl}]  SIN TRIGGER")
        else:
            for tn, dis in rows:
                flag = "DISABLED" if dis else "ENABLED"
                print(f"  [{db}.{tbl}]  {tn} [{flag}]")
    except Exception as e:
        print(f"  [{db}.{tbl}]  ERR: {str(e)[:120]}")

print("\n" + "="*100)
print("PASO 3: Cuantos eventos hay en fcme_canonicos.dbo.cdc_outbox por aggregate_type (de los 32 TYPES)?")
print("="*100)
target_aggs = sorted({a for tbl in [t for _,t in SRC_TABLES] for a in src_to_aggs.get(tbl,[])})
c = sql("fcme_canonicos").cursor()
print(f"  total eventos en outbox: ", end="")
c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
print(c.fetchone()[0])
print(f"  ultimas 24h por aggregate_type:")
c.execute("""SELECT aggregate_type, COUNT(*), MAX(created_at)
             FROM dbo.cdc_outbox
             WHERE created_at > DATEADD(hh, -24, SYSUTCDATETIME())
             GROUP BY aggregate_type ORDER BY 2 DESC""")
seen = {}
for r in c.fetchall():
    seen[r[0]] = (r[1], r[2])
for agg in target_aggs:
    cnt, mx = seen.get(agg, (0, None))
    flag = "OK" if cnt > 0 else "(0 eventos)"
    print(f"    {agg:<45} {cnt:>4}  {mx}  {flag}")

print("\n" + "="*100)
print("PASO 4: CDC_INBOX en Oracle (kafka sink)")
print("="*100)
cn = oracledb.connect(**ORA); co = cn.cursor()
co.execute("SELECT COUNT(*) FROM CDC_INBOX")
print(f"  total CDC_INBOX: {co.fetchone()[0]}")
co.execute("""SELECT AGGREGATE_TYPE, COUNT(*), SUM(PROCESSED), MAX(CREATED_AT)
              FROM CDC_INBOX
              WHERE CREATED_AT > SYSDATE - 1
              GROUP BY AGGREGATE_TYPE
              ORDER BY 2 DESC""")
inbox = {r[0]:(r[1],r[2],r[3]) for r in co.fetchall()}
for agg in target_aggs:
    cnt, proc, mx = inbox.get(agg, (0, 0, None))
    flag = "OK" if cnt>0 else "(0)"
    print(f"    {agg:<45} total={cnt:<4} processed={proc or 0}  last={mx}  {flag}")

print("\n" + "="*100)
print("PASO 5: Filas en FCME_USER.<TYPE>")
print("="*100)
for s in specs:
    if s["agg"] in target_aggs:
        try:
            co.execute(f"SELECT COUNT(*) FROM FCME_USER.{s['dest']}")
            n = co.fetchone()[0]
            print(f"    {s['dest']:<45} rows={n}")
        except Exception as e:
            print(f"    {s['dest']:<45} ERR={str(e)[:80]}")

print("\n" + "="*100)
print("PASO 6: Errores recientes en FCME_USER.CDC_INBOX_ERRORS")
print("="*100)
co.execute("""SELECT COUNT(*) FROM CDC_INBOX_ERRORS
              WHERE CREATED_AT > SYSDATE - 1""")
print(f"  total ultimas 24h: {co.fetchone()[0]}")
co.execute("""SELECT AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,200), COUNT(*)
              FROM CDC_INBOX_ERRORS
              WHERE CREATED_AT > SYSDATE - 1
              GROUP BY AGGREGATE_TYPE, EVENT_TYPE, SUBSTR(ERROR_MESSAGE,1,200)
              ORDER BY 4 DESC FETCH FIRST 30 ROWS ONLY""")
for r in co.fetchall():
    print(f"  x{r[3]:<3}  {r[0]:<40}  {r[1]:<8}  {r[2]}")

cn.close()
