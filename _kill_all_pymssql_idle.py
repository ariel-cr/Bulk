"""Mata todas las sesiones pymssql idle (no solo head blockers)."""
import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
c=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=master;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10).cursor()
c.execute("""SELECT s.session_id, s.status, DATEDIFF(SECOND, s.last_request_end_time, GETDATE()) AS idle_secs
             FROM sys.dm_exec_sessions s
             WHERE s.is_user_process=1 AND s.program_name LIKE '%pymssql%'""")
rows=c.fetchall()
print(f"Sesiones pymssql: {len(rows)}")
for sid, st, idle in rows:
    print(f"  spid={sid} status={st} idle={idle}s -> KILL")
    try:
        c.execute(f"KILL {sid}")
    except Exception as e:
        print(f"    KILL err: {e}")
c.execute("SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process=1 AND program_name LIKE '%pymssql%'")
print(f"\npymssql restantes: {c.fetchone()[0]}")
