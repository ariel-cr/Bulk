"""Mata las sesiones SQL Server idle con locks abiertos sobre fcme_canonicos.
Identifica y mata zombis pymssql que mantengan transacciones."""
import sys, pyodbc
class Tee:
    def __init__(self,*s):self.s=s
    def write(self,t):
        for x in self.s: x.write(t); x.flush()
    def flush(self):
        for x in self.s: x.flush()
sys.stdout = Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_kill_out.txt","w",encoding="utf-8"))

DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
c = pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=master;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10).cursor()

# Cadena de bloqueo - identificar 'head blocker'
print("=== CADENA DE BLOQUEOS ===")
c.execute("""SELECT s.session_id, s.login_name, s.program_name, s.status,
                    r.blocking_session_id, r.wait_type
             FROM sys.dm_exec_sessions s
             LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id
             WHERE s.is_user_process = 1
               AND (r.blocking_session_id IS NOT NULL AND r.blocking_session_id <> 0)""")
chain = c.fetchall()
print(f"Sesiones bloqueadas: {len(chain)}")
for row in chain:
    print(f"  spid={row[0]} login={row[1]} prog={row[2]} bloqueada por spid={row[4]} wait={row[5]}")

# Encontrar HEAD blocker: bloquea a alguien pero no es bloqueado
print("\n=== HEAD BLOCKERS (bloquean a otros pero estan idle) ===")
c.execute("""SELECT DISTINCT
    s.session_id, s.login_name, s.program_name, s.status, s.host_name,
    DATEDIFF(SECOND, s.last_request_end_time, GETDATE()) AS idle_secs
FROM sys.dm_exec_sessions s
WHERE s.session_id IN (
    SELECT DISTINCT blocking_session_id FROM sys.dm_exec_requests
    WHERE blocking_session_id IS NOT NULL AND blocking_session_id <> 0
)
AND s.session_id NOT IN (
    SELECT session_id FROM sys.dm_exec_requests
    WHERE blocking_session_id IS NOT NULL AND blocking_session_id <> 0
)
AND s.is_user_process = 1
ORDER BY s.session_id""")
heads = c.fetchall()
to_kill = []
for sid, login, prog, st, host, idle in heads:
    print(f"  spid={sid} login={login} prog={prog} status={st} host={host} idle={idle}s")
    if "pymssql" in (prog or "").lower():
        to_kill.append(sid)

print(f"\n=== KILL ZOMBIES PYMSSQL ===")
print(f"Sesiones a matar: {to_kill}")
for sid in to_kill:
    try:
        c.execute(f"KILL {sid}")
        print(f"  KILL {sid} -> OK")
    except Exception as e:
        print(f"  KILL {sid} -> ERR {e}")

# Verificar
print("\n=== VERIFICACION POST-KILL ===")
c.execute("""SELECT COUNT(*) FROM sys.dm_exec_requests
             WHERE blocking_session_id IS NOT NULL AND blocking_session_id <> 0""")
remaining = c.fetchone()[0]
print(f"Sesiones todavia bloqueadas: {remaining}")
