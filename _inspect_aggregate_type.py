"""Revisa cdc_table_to_types y el body del trigger fctbafil_actu para entender
por que aggregate_type sale como nombre de tabla en lugar del Type."""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

print("== fcme_canonicos.cdc_table_to_types ==")
c = sql("fcme_canonicos").cursor()
c.execute("""SELECT name FROM sys.columns
             WHERE object_id=OBJECT_ID('dbo.cdc_table_to_types') ORDER BY column_id""")
print("Columnas:", [r.name for r in c.fetchall()])

c.execute("SELECT TOP 10 * FROM dbo.cdc_table_to_types WHERE source_table='fctbafil_actu'")
cols = [d[0] for d in c.description]
print(" | ".join(cols))
for r in c.fetchall():
    print(" | ".join(str(v)[:40] for v in r))

print("\n== Triggers que insertan en cdc_outbox y su aggregate_type ==")
c.execute("SELECT TOP 5 source_table, aggregate_type, COUNT(*) n FROM dbo.cdc_outbox GROUP BY source_table, aggregate_type ORDER BY n DESC")
for r in c.fetchall():
    print(f"  src={r.source_table:<30} agg_type={r.aggregate_type:<25} n={r.n}")

print("\n== Body del trigger trg_outbox_fctbafil_actu (dbFC) ==")
c2 = sql("dbFC").cursor()
c2.execute("""SELECT OBJECT_DEFINITION(t.object_id) AS body
              FROM sys.triggers t
              WHERE t.name='trg_outbox_fctbafil_actu'""")
row = c2.fetchone()
if row:
    body = row.body
    print(body[:3000])
