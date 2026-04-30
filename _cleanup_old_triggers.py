"""Limpia 62 triggers deshabilitados y el outbox huerfano en fcme_legacy."""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]

print("="*70)
print("[1] DROP triggers deshabilitados")
print("="*70)

dropped = 0
kept = 0
errors = []
for db in LEG_DBS:
    try:
        c = sql(db).cursor()
        c.execute("""
          SELECT t.name, OBJECT_NAME(t.parent_id) parent, t.is_disabled
          FROM sys.triggers t
          WHERE (t.name LIKE '%outbox%' OR t.name LIKE 'trg_%')
            AND t.is_disabled = 1
          ORDER BY t.name
        """)
        rows = c.fetchall()
        print(f"\n  {db}: {len(rows)} triggers deshabilitados a eliminar")
        for r in rows:
            try:
                c.execute(f"DROP TRIGGER [dbo].[{r.name}]")
                print(f"    DROP {r.name} (parent={r.parent})")
                dropped += 1
            except Exception as e:
                msg = str(e)[:120]
                errors.append((db, r.name, msg))
                print(f"    FAIL {r.name}: {msg}")
    except Exception as e:
        print(f"  {db}: ERROR {e}")

print(f"\n  Total triggers eliminados: {dropped}")
if errors:
    print(f"  Errores: {len(errors)}")

print("\n" + "="*70)
print("[2] Limpiar outbox huerfano en fcme_legacy")
print("="*70)
try:
    c = sql("fcme_legacy").cursor()
    c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
    n = c.fetchone()[0]
    print(f"  fcme_legacy.cdc_outbox tenia: {n} filas")
    c.execute("DELETE FROM dbo.cdc_outbox")
    print(f"  Borrados: {c.rowcount}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n" + "="*70)
print("[3] Verificacion final: solo deben quedar triggers HABILITADOS")
print("="*70)
total_enabled = 0
total_disabled = 0
for db in LEG_DBS:
    c = sql(db).cursor()
    c.execute("""
      SELECT
        SUM(CASE WHEN is_disabled=0 THEN 1 ELSE 0 END) ena,
        SUM(CASE WHEN is_disabled=1 THEN 1 ELSE 0 END) dis
      FROM sys.triggers
      WHERE name LIKE '%outbox%' OR name LIKE 'trg_%'
    """)
    r = c.fetchone()
    ena = r.ena or 0
    dis = r.dis or 0
    print(f"  {db}: enabled={ena}  disabled={dis}")
    total_enabled += ena
    total_disabled += dis

print(f"\n  TOTAL enabled={total_enabled}  disabled={total_disabled}")
