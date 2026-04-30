"""Despliega SP + compound trigger, limpia data, valida flujo automatico."""
import oracledb, pyodbc, time

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
c = orcl.cursor()

# 1) Drop trigger viejo (puede estar disabled)
print("[1] Drop trigger viejo")
try: c.execute("DROP TRIGGER TRG_PROCESS_CDC_INBOX")
except: pass

# 2) Desplegar SP nuevo (con MERGE, sin AUTONOMOUS, sin UPDATE CDC_INBOX)
print("[2] Desplegando SP USP_INBOX_PARTICIPES")
with open(r"C:\Users\Usuario\Downloads\Bulk\_usp_inbox_participes.sql", encoding="utf-8") as f:
    sp = f.read()
c.execute(sp)
c.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='USP_INBOX_PARTICIPES'")
s = c.fetchone()[0]; print(f"  status: {s}")
if s == "INVALID":
    c.execute("SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPES' ORDER BY sequence")
    for r in c.fetchall()[:5]: print(f"    line={r[0]} col={r[1]}: {r[2][:200]}")
    raise SystemExit(1)

# 3) Desplegar compound trigger
print("[3] Desplegando compound trigger")
with open(r"C:\Users\Usuario\Downloads\Bulk\_trg_compound.sql", encoding="utf-8") as f:
    trg = f.read()
c.execute(trg)
c.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_name='TRG_PROCESS_CDC_INBOX'")
s = c.fetchone()[0]; print(f"  status: {s}")
if s == "INVALID":
    c.execute("SELECT line, position, text FROM all_errors WHERE owner='FCME_USER' AND name='TRG_PROCESS_CDC_INBOX' ORDER BY sequence")
    for r in c.fetchall()[:5]: print(f"    line={r[0]} col={r[1]}: {r[2][:200]}")
    raise SystemExit(1)

# 4) Limpiar data
print("[4] Limpieza")
c.execute("DELETE FROM CDC_INBOX")
c.execute("DELETE FROM CDC_INBOX_ERRORS")
c.execute("DELETE FROM ACTUALIZACION_AFILIADO_TYPE")
c.execute("DELETE FROM ACTUALIZACION_DOCUMENTOS_TYPE")
c_can = pyodbc.connect("DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123", autocommit=True).cursor()
c_can.execute("DELETE FROM dbo.cdc_outbox")
c_can.execute("DELETE FROM dbo.cdc_inbox")
orcl.commit()
print("  ok")

# 5) Test end-to-end
print("\n[5] Test end-to-end")
f = pyodbc.connect("DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=dbFC;UID=sa;PWD=YourPassword123", autocommit=True).cursor()
f.execute("UPDATE dbo.fctbafil_actu SET ci_cedu=ci_cedu WHERE ci_cedu=(SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu)")
print("  UPDATE 1 lanzado")

for i in range(8):
    time.sleep(7)
    c.execute("SELECT COUNT(*), MAX(ID) FROM CDC_INBOX"); inb=c.fetchone()
    c.execute("SELECT COUNT(*) FROM CDC_INBOX WHERE PROCESSED=1"); pr=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE"); at=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM CDC_INBOX_ERRORS"); er=c.fetchone()[0]
    print(f"  t={(i+1)*7}s  inbox={inb}  processed={pr}  actu_afil={at}  errors={er}")
    if inb[0]>0 and pr>0 and at>0: break

print("\n--- UPDATE 2 (ver que no duplica en actu_afil por MERGE) ---")
f.execute("UPDATE dbo.fctbafil_actu SET ci_cedu=ci_cedu WHERE ci_cedu=(SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu)")
for i in range(6):
    time.sleep(7)
    c.execute("SELECT COUNT(*), MAX(ID) FROM CDC_INBOX"); inb=c.fetchone()
    c.execute("SELECT COUNT(*) FROM CDC_INBOX WHERE PROCESSED=1"); pr=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE"); at=c.fetchone()[0]
    print(f"  t={(i+1)*7}s  inbox={inb}  processed={pr}  actu_afil={at}")
    if inb[0]>1: break

# Final state
print("\n[6] Estado FINAL")
c.execute("SELECT ID, AGGREGATE_TYPE, AGGREGATE_ID, PROCESSED, PROCESSED_AT FROM CDC_INBOX ORDER BY ID")
for r in c.fetchall(): print(f"  inbox: id={r[0]} type={r[1]} agg={r[2]} processed={r[3]} at={r[4]}")
c.execute("SELECT CODIGO_CEDU, CODIGO_PROV, DESCRIPCION_CALL_PRIM FROM ACTUALIZACION_AFILIADO_TYPE")
for r in c.fetchall(): print(f"  ACTU_AFIL: cedu={r[0]} prov={r[1]} call_prim={r[2]}")
