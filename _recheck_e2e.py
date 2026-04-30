"""Recheck cuantos eventos del batched test llegaron a cdc_inbox y replicaron a legacy."""
import sys, json, pyodbc
sys.stdout.reconfigure(line_buffering=True)
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
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_recheck_out.txt","w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def cn(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)

ccn=cn("fcme_canonicos"); cc=ccn.cursor()

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS=json.load(f)
testable=[s for s in SPECS if s.get("dest_match") and s.get("ltbl") and s.get("lkey")]

print("Recheck eventos en cdc_inbox (ultimos 15 min):\n")
n_in_inbox=0
n_processed=0
for spec in testable:
    agg=spec["agg"]
    cc.execute("""SELECT COUNT(*), SUM(CAST(processed AS INT))
                  FROM cdc_inbox
                  WHERE aggregate_type=? AND created_at >= DATEADD(MINUTE, -15, SYSDATETIME())""", agg)
    cnt, proc = cc.fetchone()
    proc = proc or 0
    if cnt > 0:
        n_in_inbox += 1
        if proc == cnt:
            n_processed += 1

print(f"Types con evento en cdc_inbox: {n_in_inbox}/{len(testable)}")
print(f"Types con todos PROCESSED=1: {n_processed}/{n_in_inbox}")

# Errores
cc.execute("""SELECT COUNT(*) FROM cdc_inbox_errors
              WHERE created_at >= DATEADD(MINUTE, -15, SYSDATETIME())""")
print(f"\nErrores cdc_inbox_errors ultimos 15 min: {cc.fetchone()[0]}")

# Sample errors
cc.execute("SELECT name FROM sys.columns WHERE object_id=OBJECT_ID('dbo.cdc_inbox_errors')")
err_cols=[r[0] for r in cc.fetchall()]
print(f"  cols cdc_inbox_errors: {err_cols}")
order_col = "created_at" if "created_at" in err_cols else err_cols[0]
cc.execute(f"""SELECT TOP 12 aggregate_type, event_type, LEFT(error_message, 200)
              FROM cdc_inbox_errors
              WHERE {order_col} >= DATEADD(MINUTE, -15, SYSDATETIME())
              ORDER BY {order_col} DESC""")
errs=cc.fetchall()
if errs:
    print("\nUltimos errores:")
    for r in errs:
        print(f"  [{r[1]}] {r[0]}: {r[2]}")

# Verificar legacy: cuantas filas con marker TEST_E2E hay en las tablas legacy
print("\n\nFilas con marker TEST_E2E en legacy:")
import collections
by_db = collections.defaultdict(list)
for s in testable:
    by_db[s["ldb"]].append((s["agg"], s["ltbl"], s["lkey"], s.get("dest_match",[])))

for db, items in by_db.items():
    cnn = cn(db); cc2 = cnn.cursor()
    db_total = 0
    for agg, ltbl, lkey, dm in items:
        # Necesitamos saber qué col de la tabla legacy tiene el marker
        # leg_to_ora: lcol -> ocol; nuestro INSERT puso valor en ocol -> a través del wrapper
        # llega a lcol. Si lcol acepta numero, valor=99001. Si string, 'TEST_E2E'.
        # Probemos en cualquier col del lkey con TEST_E2E
        try:
            cnt_match = 0
            for lc in lkey:
                try:
                    cc2.execute(f"SELECT COUNT(*) FROM dbo.[{ltbl}] WHERE [{lc}]=?", 99001)
                    cnt_match += cc2.fetchone()[0]
                except:
                    pass
            if cnt_match > 0:
                print(f"  {db}.{ltbl} ({agg}): {cnt_match} filas con marker")
                db_total += cnt_match
        except: pass
    if db_total > 0:
        print(f"  TOTAL {db}: {db_total} filas con marker E2E")
    cnn.close()

ccn.close()
