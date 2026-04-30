"""Verifica DONDE escriben los triggers de outbox: canonicos o legacy."""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

print("="*70)
print("Triggers HABILITADOS y a que cdc_outbox apuntan")
print("="*70)

LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
total_enabled = 0
total_disabled = 0
target_canonicos = 0
target_legacy = 0
target_otro = 0

for db in LEG_DBS:
    try:
        c = sql(db).cursor()
        c.execute("""
          SELECT t.name, t.is_disabled, OBJECT_DEFINITION(t.object_id) AS body
          FROM sys.triggers t
          WHERE (t.name LIKE '%outbox%' OR t.name LIKE 'trg_%')
        """)
        rows = c.fetchall()
        for r in rows:
            if r.is_disabled:
                total_disabled += 1
            else:
                total_enabled += 1
                body = (r.body or "").lower()
                if "fcme_canonicos" in body:
                    target_canonicos += 1
                    where = "fcme_canonicos"
                elif "fcme_legacy" in body:
                    target_legacy += 1
                    where = "fcme_legacy"
                else:
                    target_otro += 1
                    where = "(local/otro)"
                print(f"  [{db}] {r.name:<40} -> {where}")
    except Exception as e:
        print(f"  {db}: ERROR {e}")

print()
print(f"  Habilitados: {total_enabled}   Deshabilitados: {total_disabled}")
print(f"  Apuntan a canonicos: {target_canonicos}")
print(f"  Apuntan a legacy:    {target_legacy}")
print(f"  Apuntan a otro:      {target_otro}")

# Verificar tabla physical cdc_outbox en cada BD
print("\n" + "="*70)
print("Existencia fisica de cdc_outbox en cada BD")
print("="*70)
for db in ["fcme_legacy","fcme_canonicos","fcme_newcore"]:
    try:
        c = sql(db).cursor()
        c.execute("""
          SELECT COUNT(*) FROM sys.tables t
          WHERE t.name='cdc_outbox'
        """)
        n = c.fetchone()[0]
        if n>0:
            c.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
            cnt = c.fetchone()[0]
            print(f"  {db}.dbo.cdc_outbox: existe   filas={cnt}")
        else:
            print(f"  {db}.dbo.cdc_outbox: NO existe")
    except Exception as e:
        print(f"  {db}: {e}")

# Conector Kafka source: usualmente lee de una sola BD; ver bridge
print("\n" + "="*70)
print("Routing/bridge entre canonicos y legacy")
print("="*70)
try:
    c = sql("fcme_canonicos").cursor()
    c.execute("""
      SELECT s.name sch, t.name tbl FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id=s.schema_id
      WHERE t.name LIKE 'cdc_%' OR t.name LIKE '%route%' OR t.name LIKE '%bridge%' OR t.name LIKE '%table_to%'
      ORDER BY t.name
    """)
    for r in c.fetchall():
        print(f"  fcme_canonicos.{r.sch}.{r.tbl}")
except Exception as e:
    print(f"  ERROR: {e}")
