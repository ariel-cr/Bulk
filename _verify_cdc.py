"""Verifica contenido REAL de cdc_outbox y cdc_inbox en las 3 BDs."""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

for db in ["fcme_legacy","fcme_canonicos","fcme_newcore"]:
    c = conn(db).cursor()
    print(f"\n===== {db} =====")
    for t in ["cdc_outbox","cdc_inbox"]:
        try:
            c.execute(f"SELECT COUNT(*) FROM dbo.{t}")
            n = c.fetchone()[0]
            c.execute(f"SELECT MIN(id), MAX(id) FROM dbo.{t}")
            mn, mx = c.fetchone()
            print(f"  {t:<20} rows={n:>7}   id range=[{mn}..{mx}]")
            if n > 0:
                c.execute(f"SELECT TOP 3 id, aggregate_type, event_type, source_table, created_at FROM dbo.{t} ORDER BY id DESC")
                for r in c.fetchall():
                    print(f"    sample: id={r.id} type={r.aggregate_type} event={r.event_type} src={r.source_table}")
        except Exception as e:
            print(f"  {t}: ERROR {e}")
