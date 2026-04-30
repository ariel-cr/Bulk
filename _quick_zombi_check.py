"""Verificacion rapida: 0 sesiones bloqueadas + 0 sesiones pymssql idle."""
import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
c = pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=master;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10).cursor()

c.execute("""SELECT COUNT(*) FROM sys.dm_exec_requests
             WHERE blocking_session_id IS NOT NULL AND blocking_session_id <> 0""")
blocked = c.fetchone()[0]

c.execute("""SELECT COUNT(*) FROM sys.dm_exec_sessions
             WHERE is_user_process=1 AND program_name LIKE '%pymssql%'""")
pymssql_sessions = c.fetchone()[0]

c.execute("""SELECT COUNT(*) FROM sys.dm_exec_sessions s
             WHERE s.is_user_process=1 AND s.program_name='Python'""")
python_sessions = c.fetchone()[0]

print(f"Sesiones BLOQUEADAS  : {blocked}  {'OK' if blocked==0 else 'FAIL'}")
print(f"Sesiones pymssql     : {pymssql_sessions}  (zombis si > 0)")
print(f"Sesiones Python(odbc): {python_sessions}  (la actual cuenta)")
