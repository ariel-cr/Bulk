"""Test end-to-end del flujo Legacy -> Newcore."""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

cl = conn("fcme_legacy").cursor()
cf = conn("dbFC").cursor()
cn = conn("fcme_newcore").cursor()

# Snapshots
cl.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); out_before = cl.fetchone()[0]
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox");  in_before = cn.fetchone()[0]
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=1"); proc_before = cn.fetchone()[0]
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors"); err_before = cn.fetchone()[0]
print(f"Antes: legacy.outbox={out_before}  newcore.inbox={in_before}  processed={proc_before}  errors={err_before}")

# 1) Disparar cambio en legacy
cf.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu ORDER BY ci_cedu")
ci = cf.fetchone()[0]
print(f"\n[1] UPDATE no-destructivo sobre dbFC.fctbafil_actu ci_cedu='{ci}'")
cf.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)
print("    ok")

cl.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); out_after = cl.fetchone()[0]
print(f"    legacy.outbox: {out_before} -> {out_after}  (delta={out_after-out_before})")

# 2) Bridge
print("\n[2] EXEC legacy.dbo.usp_cdc_bridge_to_newcore")
cl.execute("EXEC dbo.usp_cdc_bridge_to_newcore @max_rows = 1000")
r = cl.fetchone()
print(f"    copied={r.copied}  prev_last_id={r.prev_last_id}  new_last_id={r.new_last_id}")

# 3) Newcore inbox state
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); in_after = cn.fetchone()[0]
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=1"); proc_after = cn.fetchone()[0]
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors"); err_after = cn.fetchone()[0]
print(f"\n[3] Newcore inbox: {in_before} -> {in_after}  (delta={in_after-in_before})")
print(f"    processed: {proc_before} -> {proc_after}")
print(f"    errors:    {err_before} -> {err_after}")

# 4) Ultimas filas
print("\n[4] Ultimas 5 filas en newcore.cdc_inbox:")
cn.execute("""
  SELECT TOP 5 id, aggregate_type, aggregate_id, event_type, processed, processed_at, source_table
  FROM dbo.cdc_inbox ORDER BY id DESC
""")
for r in cn.fetchall():
    print(f"    id={r.id}  type={r.aggregate_type}  agg={r.aggregate_id}  op={r.event_type}  processed={r.processed}  src={r.source_table}")

# 5) Ver si quedo algo sin procesar y por que
cn.execute("""
  SELECT TOP 5 id, aggregate_type FROM dbo.cdc_inbox
  WHERE processed = 0 ORDER BY id DESC
""")
unp = cn.fetchall()
if unp:
    print(f"\n[5] Eventos sin procesar ({len(unp)} de los ultimos 5):")
    for r in unp:
        cn.execute("SELECT sp_name FROM dbo.cdc_inbox_module_config WHERE aggregate_type=?", r.aggregate_type)
        m = cn.fetchone()
        mapeo = m[0] if m else "(NO hay mapeo en cdc_inbox_module_config)"
        print(f"    id={r.id}  type={r.aggregate_type}  -> {mapeo}")

# 6) Ultimos errores
cn.execute("""
  SELECT TOP 5 id, inbox_id, aggregate_type, error_message, error_date
  FROM dbo.cdc_inbox_errors ORDER BY id DESC
""")
errs = cn.fetchall()
if errs:
    print(f"\n[6] Ultimos errores:")
    for r in errs:
        print(f"    err_id={r.id}  inbox={r.inbox_id}  type={r.aggregate_type}  date={r.error_date}")
        print(f"      {r.error_message[:200]}")
