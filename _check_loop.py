"""Inspecciona cdc_outbox y CDC_INBOX para detectar bucles activos."""
import sys, pyodbc
# Tee stdout a archivo
class Tee:
    def __init__(self, *streams): self.streams = streams
    def write(self, s):
        for s2 in self.streams:
            s2.write(s); s2.flush()
    def flush(self):
        for s2 in self.streams: s2.flush()
_log = open(r"C:\Users\Usuario\Downloads\Bulk\_check_loop_out.txt","w",encoding="utf-8")
sys.stdout = Tee(sys.__stdout__, _log)
DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=30)

can = sql("fcme_canonicos").cursor()

print("="*70)
print("CHECK LOOP cdc_outbox")
print("="*70)

# Total
can.execute("SELECT COUNT(*) FROM cdc_outbox")
total = can.fetchone()[0]
print(f"\nTotal eventos en cdc_outbox: {total:,}")

# Ultimos 5 min
can.execute("""SELECT COUNT(*) FROM cdc_outbox
               WHERE created_at >= DATEADD(MINUTE, -5, SYSUTCDATETIME())""")
last5 = can.fetchone()[0]
print(f"Eventos ultimos 5 min: {last5:,}")

# Ultimos 1 min
can.execute("""SELECT COUNT(*) FROM cdc_outbox
               WHERE created_at >= DATEADD(MINUTE, -1, SYSUTCDATETIME())""")
last1 = can.fetchone()[0]
print(f"Eventos ultimo 1 min: {last1:,}")

# Top sources en ultimos 10 min
print("\nTop source_table ultimos 10 min:")
can.execute("""SELECT TOP 20 source_table, aggregate_type, COUNT(*) AS n,
                      MIN(created_at), MAX(created_at)
               FROM cdc_outbox
               WHERE created_at >= DATEADD(MINUTE, -10, SYSUTCDATETIME())
               GROUP BY source_table, aggregate_type
               ORDER BY n DESC""")
for src, agg, n, mn, mx in can.fetchall():
    print(f"  {n:>6}  {src:<35} {agg:<35} [{mn} - {mx}]")

# Eventos duplicados (mismo agg_id + timestamp = sospecha de bucle)
print("\nDuplicados sospechosos (mismo aggregate_id + created_at) ultimos 10 min:")
can.execute("""SELECT TOP 10 aggregate_id, aggregate_type, COUNT(*) AS dup_count, MAX(created_at)
               FROM cdc_outbox
               WHERE created_at >= DATEADD(MINUTE, -10, SYSUTCDATETIME())
               GROUP BY aggregate_id, aggregate_type, created_at
               HAVING COUNT(*) > 5
               ORDER BY dup_count DESC""")
dups = can.fetchall()
if not dups:
    print("  (ninguno)")
for aid, agg, n, ts in dups:
    print(f"  x{n}  agg={agg:<30} id={aid[:30]}  ts={ts}")

# Estado CDC_INBOX Oracle
import oracledb
ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}
o = oracledb.connect(**ORA).cursor()
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
inbox_total = o.fetchone()[0]
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=0")
pending = o.fetchone()[0]
print(f"\nCDC_INBOX Oracle: total={inbox_total:,}  pendientes={pending}")

# Verificar pausa cartera
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE ACTIVE=1")
active = o.fetchone()[0]
print(f"CDC_INBOX_MODULE_CONFIG entries con ACTIVE=1: {active}")
