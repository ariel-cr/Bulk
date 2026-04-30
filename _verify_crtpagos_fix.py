"""Verifica que el fix de ROW_NUMBER funciona en crtpagos.
Limpia eventos previos del test, hace UPDATE de 1 fila, mide eventos resultantes."""
import time, pyodbc
DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=30)

cr = sql("dbCR").cursor()
can = sql("fcme_canonicos").cursor()

# Baseline
can.execute("SELECT COUNT(*) FROM cdc_outbox WHERE source_table='dbCR.dbo.crtpagos'")
before = can.fetchone()[0]
print(f"cdc_outbox crtpagos antes: {before:,}")

# Buscar 1 fila con combinacion mas unica posible (qs_abno + qs_cred + ti_cred)
cr.execute("SELECT TOP 1 qs_abno, qs_cred, ti_cred, ci_rol FROM dbo.crtpagos WHERE ci_rol='200108'")
row = cr.fetchone()
print(f"Fila probe: qs_abno={row[0]}, qs_cred={row[1]}, ti_cred={row[2]}, ci_rol='{row[3]}'")

# Cuantas filas matchearia con esa tupla mas especifica?
cr.execute("SELECT COUNT(*) FROM dbo.crtpagos WHERE qs_abno=? AND qs_cred=? AND ti_cred=?", row[0], row[1], row[2])
match = cr.fetchone()[0]
print(f"Filas que matchean (qs_abno,qs_cred,ti_cred): {match}")

# UPDATE no-op solo de esa tupla
cr.execute("UPDATE dbo.crtpagos SET ci_rol=ci_rol WHERE qs_abno=? AND qs_cred=? AND ti_cred=?", row[0], row[1], row[2])
print(f"Filas updated: {cr.rowcount}")

time.sleep(2)
can.execute("SELECT COUNT(*) FROM cdc_outbox WHERE source_table='dbCR.dbo.crtpagos'")
after = can.fetchone()[0]
delta = after - before
print(f"cdc_outbox crtpagos despues: {after:,}")
print(f"Delta: +{delta} eventos (esperado: {match})")

# Mostrar los nuevos
print("\nUltimos eventos crtpagos:")
can.execute("SELECT TOP 3 id, aggregate_id, LEN(payload), created_at FROM cdc_outbox WHERE source_table='dbCR.dbo.crtpagos' ORDER BY id DESC")
for id_, aid, plen, ts in can.fetchall():
    print(f"  id={id_} aggregate_id={aid} payload_len={plen} ts={ts}")
