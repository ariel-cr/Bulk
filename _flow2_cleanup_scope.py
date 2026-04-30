"""Limpia componentes fuera del scope actual.
SCOPE: Tabla Newcore (FCME_USER) -> Trigger outbox -> cdc_outbox (XEPDB1)
       -> Kafka -> cdc_inbox (canonicos)
SE ELIMINA:
  - 30 wrappers usp_inbox_* en canonicos
  - 30 sp_*Type_CRUD en bases legacy
  - usp_process_cdc_inbox (dispatcher)
  - trg_process_cdc_inbox (trigger AFTER INSERT)
  - cdc_inbox_module_config
  - cdc_inbox_errors
SE MANTIENE:
  - Triggers Oracle TRG_OUTBOX_* (30) en FCME_USER tablas TYPE
  - FCME_USER.CDC_OUTBOX
  - fcme_canonicos.cdc_inbox (tabla, sin trigger)
"""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

c = sql("fcme_canonicos").cursor()

print("="*70)
print("[1] DROP wrappers usp_inbox_* en canonicos")
print("="*70)
c.execute("""SELECT name FROM sys.objects WHERE type='P' AND name LIKE 'usp_inbox_%'""")
wrappers = [r.name for r in c.fetchall()]
for w in wrappers:
    try:
        c.execute(f"DROP PROCEDURE dbo.[{w}]")
        print(f"  DROP {w}")
    except Exception as e:
        print(f"  fail {w}: {str(e)[:100]}")
print(f"  total: {len(wrappers)} wrappers eliminados")

print("\n" + "="*70)
print("[2] DROP usp_process_cdc_inbox (dispatcher)")
print("="*70)
try:
    c.execute("DROP PROCEDURE dbo.usp_process_cdc_inbox")
    print("  DROP usp_process_cdc_inbox")
except Exception as e:
    print(f"  fail: {str(e)[:100]}")

print("\n" + "="*70)
print("[3] DROP trigger trg_process_cdc_inbox sobre cdc_inbox")
print("="*70)
try:
    c.execute("DROP TRIGGER dbo.trg_process_cdc_inbox")
    print("  DROP trg_process_cdc_inbox")
except Exception as e:
    print(f"  fail: {str(e)[:100]}")

print("\n" + "="*70)
print("[4] DROP tablas cdc_inbox_module_config y cdc_inbox_errors")
print("="*70)
for t in ["cdc_inbox_module_config","cdc_inbox_errors"]:
    try:
        c.execute(f"DROP TABLE dbo.{t}")
        print(f"  DROP {t}")
    except Exception as e:
        print(f"  fail {t}: {str(e)[:100]}")

print("\n" + "="*70)
print("[5] DROP sp_*Type_CRUD en bases legacy")
print("="*70)
LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
total = 0
for db in LEG_DBS:
    try:
        cd = sql(db).cursor()
        cd.execute("""SELECT name FROM sys.objects WHERE type='P'
                      AND (name LIKE 'sp_%_TYPE_CRUD' OR name LIKE 'sp_%Type_CRUD')""")
        cruds = [r.name for r in cd.fetchall()]
        for cr in cruds:
            try:
                cd.execute(f"DROP PROCEDURE dbo.[{cr}]")
                total += 1
            except: pass
        print(f"  {db}: {len(cruds)} CRUDs eliminados")
    except Exception as e:
        print(f"  {db}: {str(e)[:100]}")
print(f"  total: {total} CRUDs eliminados")

print("\n" + "="*70)
print("[6] Vaciar cdc_inbox y cdc_outbox (datos de tests)")
print("="*70)
c.execute("DELETE FROM dbo.cdc_inbox")
print(f"  cdc_inbox vaciado: {c.rowcount} filas")

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
print(f"  CDC_OUTBOX vaciado: {co.rowcount} filas")
orcl.commit()

print("\n" + "="*70)
print("ESTADO FINAL DEL SCOPE")
print("="*70)
co.execute("""SELECT trigger_name, table_name, status FROM all_triggers
              WHERE owner='FCME_USER' AND trigger_name LIKE 'TRG_OUTBOX_%' ORDER BY trigger_name""")
trgs = co.fetchall()
print(f"\n[ORACLE] Triggers en FCME_USER tablas TYPE -> CDC_OUTBOX: {len(trgs)}")
for r in trgs[:5]: print(f"  {r[0]:<35} on {r[1]:<32} {r[2]}")
if len(trgs)>5: print(f"  ...y {len(trgs)-5} mas")

co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX")
print(f"\n[ORACLE] FCME_USER.CDC_OUTBOX existe, filas: {co.fetchone()[0]}")

c.execute("""SELECT COUNT(*) FROM sys.tables WHERE name='cdc_inbox'""")
print(f"\n[SQL canonicos] cdc_inbox existe: {c.fetchone()[0]>0}")
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
print(f"  filas: {c.fetchone()[0]}")
c.execute("""SELECT COUNT(*) FROM sys.triggers WHERE parent_id=OBJECT_ID('dbo.cdc_inbox')""")
print(f"  triggers en cdc_inbox: {c.fetchone()[0]}  (debe ser 0)")

c.execute("""SELECT COUNT(*) FROM sys.objects WHERE type='P' AND name LIKE 'usp_inbox_%'""")
print(f"\n[SQL canonicos] wrappers usp_inbox_*: {c.fetchone()[0]}  (debe ser 0)")
c.execute("""SELECT COUNT(*) FROM sys.objects WHERE type='P' AND name='usp_process_cdc_inbox'""")
print(f"[SQL canonicos] usp_process_cdc_inbox: {c.fetchone()[0]}  (debe ser 0)")
c.execute("""SELECT COUNT(*) FROM sys.tables WHERE name IN ('cdc_inbox_module_config','cdc_inbox_errors')""")
print(f"[SQL canonicos] module_config + errors: {c.fetchone()[0]}  (debe ser 0)")

orcl.close()
print("\n=== CLEANUP OK ===")
