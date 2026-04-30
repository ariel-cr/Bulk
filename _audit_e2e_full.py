"""Test masivo: dispara UPDATE en multiples tablas y mide cobertura."""
import pyodbc, oracledb, time

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# Reset todo
print("[Reset]")
co.execute("DELETE FROM CDC_INBOX")
co.execute("DELETE FROM CDC_INBOX_ERRORS")
orcl.commit()
cs = sql("fcme_canonicos").cursor()
cs.execute("DELETE FROM dbo.cdc_outbox")
print("  outbox/inbox/errors limpiados")

# Tablas Oracle TYPE - count antes
co.execute("""SELECT table_name FROM all_tables WHERE owner='FCME_USER' AND table_name LIKE '%TYPE%'""")
type_tables = [r[0] for r in co.fetchall()]
counts_before = {}
for t in type_tables:
    try:
        co.execute(f"SELECT COUNT(*) FROM FCME_USER.{t}")
        counts_before[t] = co.fetchone()[0]
    except: pass

# Disparar UPDATE en cada tabla legacy con triggers activos
DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
triggers_fired = []
for db in DBS:
    c = sql(db).cursor()
    c.execute("""SELECT OBJECT_NAME(parent_id) AS tbl FROM sys.triggers
                 WHERE name LIKE 'trg_outbox_%' AND is_disabled=0""")
    tbls = [r.tbl for r in c.fetchall()]
    for tbl in tbls:
        try:
            cc = sql(db).cursor()
            cc.execute(f"SELECT COUNT(*) FROM dbo.[{tbl}]")
            n = cc.fetchone()[0]
            if n == 0: continue
            # UPDATE no-op de la primera fila
            cc.execute(f"""DECLARE @sql NVARCHAR(MAX);
                           SELECT TOP 1 @sql = N'UPDATE TOP (1) dbo.[{tbl}] SET [' + c.name + N']=[' + c.name + N']'
                           FROM sys.columns c WHERE c.object_id=OBJECT_ID('dbo.[{tbl}]')
                           ORDER BY c.column_id;
                           EXEC sp_executesql @sql""")
            triggers_fired.append((db, tbl))
        except Exception as e:
            pass

print(f"\n[Triggers disparados]: {len(triggers_fired)}")
for db, tbl in triggers_fired[:10]: print(f"  {db}.{tbl}")
if len(triggers_fired)>10: print(f"  ...y {len(triggers_fired)-10} mas")

print("\n[Esperando propagacion]")
for i in range(20):
    time.sleep(3)
    cs.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
    out = cs.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM CDC_INBOX")
    inb = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM CDC_INBOX WHERE PROCESSED=1")
    pr = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM CDC_INBOX_ERRORS")
    er = co.fetchone()[0]
    print(f"  T+{(i+1)*3}s: outbox={out} inbox={inb} processed={pr} errors={er}")
    if inb > 0 and pr == inb:
        time.sleep(2)
        break

# Conteo por tabla destino
print("\n[Filas insertadas por tabla destino]")
counts_after = {}
total_inserts = 0
for t in type_tables:
    try:
        co.execute(f"SELECT COUNT(*) FROM FCME_USER.{t}")
        counts_after[t] = co.fetchone()[0]
        delta = counts_after[t] - counts_before.get(t, 0)
        if delta > 0:
            print(f"  {t:<45} +{delta}")
            total_inserts += delta
    except: pass
print(f"\n  Total inserts/updates: {total_inserts}")

# Errores
co.execute("SELECT COUNT(*) FROM CDC_INBOX_ERRORS")
n_err = co.fetchone()[0]
print(f"\n[Errores]: {n_err}")
if n_err > 0:
    co.execute("""SELECT AGGREGATE_TYPE, ERROR_MESSAGE, COUNT(*) n
                  FROM CDC_INBOX_ERRORS GROUP BY AGGREGATE_TYPE, ERROR_MESSAGE
                  ORDER BY n DESC FETCH FIRST 15 ROWS ONLY""")
    for r in co.fetchall():
        print(f"  ({r[2]}x) {r[0]}: {r[1][:200]}")

# Eventos no procesados
co.execute("SELECT COUNT(*) FROM CDC_INBOX WHERE PROCESSED=0")
np = co.fetchone()[0]
print(f"\n[Pendientes sin procesar]: {np}")
if np > 0:
    co.execute("""SELECT AGGREGATE_TYPE, COUNT(*) n FROM CDC_INBOX WHERE PROCESSED=0
                  GROUP BY AGGREGATE_TYPE ORDER BY n DESC FETCH FIRST 10 ROWS ONLY""")
    for r in co.fetchall():
        print(f"  ({r[1]}x) {r[0]}")

orcl.close()
