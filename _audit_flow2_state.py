"""Inspecciona el estado actual de las piezas del Flujo 2 (Newcore -> Legacy)."""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

print("="*70)
print("[ORACLE FCME_USER] Estado de piezas para outbox saliente")
print("="*70)

# 1) cdc_outbox en Oracle
co.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name='CDC_OUTBOX'")
exists = co.fetchone()[0] > 0
print(f"\nFCME_USER.CDC_OUTBOX existe: {exists}")
if exists:
    co.execute("SELECT column_name, data_type FROM all_tab_columns WHERE owner='FCME_USER' AND table_name='CDC_OUTBOX' ORDER BY column_id")
    for r in co.fetchall(): print(f"  {r[0]:<25} {r[1]}")
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX")
    print(f"  filas: {co.fetchone()[0]}")

# 2) Triggers en tablas FCME_USER que escriban a cdc_outbox
co.execute("""SELECT trigger_name, table_name, status
              FROM all_triggers WHERE owner='FCME_USER'
                AND (trigger_name LIKE '%OUTBOX%' OR trigger_name LIKE 'TRG_%TO_LEG%')
              ORDER BY trigger_name FETCH FIRST 20 ROWS ONLY""")
trigs = co.fetchall()
print(f"\nTriggers FCME_USER hacia outbox: {len(trigs)}")
for r in trigs[:10]: print(f"  {r[0]:<40} on {r[1]:<35} {r[2]}")

# 3) Sequence para CDC_OUTBOX.ID
co.execute("SELECT sequence_name FROM all_sequences WHERE sequence_owner='FCME_USER' AND sequence_name LIKE '%OUTBOX%'")
seqs = co.fetchall()
print(f"\nSequences relacionadas: {[s[0] for s in seqs]}")

print("\n" + "="*70)
print("[SQL SERVER fcme_canonicos] Estado de piezas para inbox entrante")
print("="*70)

c = sql("fcme_canonicos").cursor()

# 4) cdc_inbox en canonicos
c.execute("""SELECT s.name sch, t.name tbl FROM sys.tables t
             JOIN sys.schemas s ON t.schema_id=s.schema_id
             WHERE t.name='cdc_inbox'""")
rows = c.fetchall()
print(f"\nfcme_canonicos.cdc_inbox existe: {len(rows)>0}")
if rows:
    c.execute("""SELECT c.name, t.name typ FROM sys.columns c
                 JOIN sys.types t ON c.user_type_id=t.user_type_id
                 WHERE c.object_id=OBJECT_ID('dbo.cdc_inbox') ORDER BY c.column_id""")
    for r in c.fetchall(): print(f"  {r.name:<25} {r.typ}")
    c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
    print(f"  filas: {c.fetchone()[0]}")

# 5) Triggers en cdc_inbox de canonicos
c.execute("""SELECT t.name, t.is_disabled FROM sys.triggers t
             WHERE t.parent_id=OBJECT_ID('dbo.cdc_inbox')""")
ts = c.fetchall()
print(f"\nTriggers en cdc_inbox: {len(ts)}")
for r in ts: print(f"  {r.name}  disabled={r.is_disabled}")

# 6) usp_process_cdc_inbox y wrappers usp_inbox_*
c.execute("""SELECT s.name sch, o.name nm, o.type_desc
             FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id
             WHERE o.type='P'
               AND (o.name LIKE 'usp_process_%' OR o.name LIKE 'usp_inbox_%')
             ORDER BY o.name""")
sps = c.fetchall()
print(f"\nSPs (usp_process_*, usp_inbox_*): {len(sps)}")
for r in sps[:20]: print(f"  {r.sch}.{r.nm:<40} {r.type_desc}")

# 7) module_config
c.execute("""SELECT s.name sch, t.name tbl FROM sys.tables t
             JOIN sys.schemas s ON t.schema_id=s.schema_id
             WHERE t.name LIKE '%module_config%'""")
mc = c.fetchall()
print(f"\nTablas module_config en canonicos: {len(mc)}")
for r in mc: print(f"  {r.sch}.{r.tbl}")

print("\n" + "="*70)
print("[SQL SERVER legacy DBs] Estado de SPs sp_*Type_CRUD")
print("="*70)

LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
total_crud = 0
for db in LEG_DBS:
    try:
        c2 = sql(db).cursor()
        c2.execute("""SELECT COUNT(*) FROM sys.objects
                      WHERE type='P' AND (name LIKE 'sp_%Type_CRUD' OR name LIKE 'sp_%_CRUD')""")
        n = c2.fetchone()[0]
        if n > 0:
            c2.execute("""SELECT TOP 5 name FROM sys.objects
                          WHERE type='P' AND (name LIKE 'sp_%Type_CRUD' OR name LIKE 'sp_%_CRUD')
                          ORDER BY name""")
            sample = [r.name for r in c2.fetchall()]
            print(f"  {db}: {n} SPs CRUD - sample {sample}")
        else:
            print(f"  {db}: 0 SPs CRUD")
        total_crud += n
    except Exception as e:
        print(f"  {db}: err {str(e)[:80]}")

print(f"\n  TOTAL sp_*_CRUD: {total_crud}")

orcl.close()
print("\n=== FIN AUDITORIA ===")
