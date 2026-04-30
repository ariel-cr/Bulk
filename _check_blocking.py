"""Detecta sesiones SQL Server que estan bloqueando cdc_outbox o ejecutando queries
sobre cartera. Sale rapido (no toca cdc_outbox)."""
import sys, pyodbc
class Tee:
    def __init__(self,*s):self.s=s
    def write(self,t):
        for x in self.s: x.write(t); x.flush()
    def flush(self):
        for x in self.s: x.flush()
sys.stdout = Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_check_blocking_out.txt","w",encoding="utf-8"))

DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
c = pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=master;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10).cursor()

print("=== Sesiones activas (no system) en SQL Server ===")
c.execute("""SELECT
    s.session_id, s.login_name, s.host_name, s.program_name,
    s.status, s.cpu_time, s.last_request_start_time,
    r.command, r.blocking_session_id, r.wait_type, r.wait_resource,
    DB_NAME(r.database_id) AS db
FROM sys.dm_exec_sessions s
LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id
WHERE s.is_user_process = 1
ORDER BY s.session_id""")
rows = c.fetchall()
print(f"Total user sessions: {len(rows)}\n")
for r in rows:
    sid, login, host, prog, st, cpu, last_req, cmd, blkby, wt, wr, db = r
    flag = "BLOCKED" if blkby else ("RUN" if cmd else "idle")
    print(f"  spid={sid:>4} [{flag:<7}] login={login:<10} prog={(prog or '')[:25]:<25} cmd={cmd or '-':<15} blkby={blkby or '-':>4} wait={wt or '-':<15} db={db or '-'}")

print("\n=== Sesiones que tienen lock sobre fcme_canonicos.cdc_outbox ===")
c.execute("""SELECT TOP 50
    tl.request_session_id AS spid, tl.resource_type, tl.request_mode, tl.request_status,
    OBJECT_NAME(tl.resource_associated_entity_id, DB_ID('fcme_canonicos')) AS obj
FROM sys.dm_tran_locks tl
WHERE tl.resource_database_id = DB_ID('fcme_canonicos')
  AND tl.resource_type IN ('OBJECT','PAGE','RID','KEY','HOBT')
ORDER BY tl.request_session_id""")
locks = c.fetchall()
if not locks:
    print("  (ninguna)")
else:
    seen = set()
    for spid, rt, rm, rs, obj in locks:
        if obj == 'cdc_outbox':
            key = (spid, rt, rm)
            if key in seen: continue
            seen.add(key)
            print(f"  spid={spid} resource={rt} mode={rm} status={rs} obj={obj}")
