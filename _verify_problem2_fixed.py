"""Verifica que problema 2 esta resuelto: smoke test que mata 1 fila real
con lkey correcta debe producir exactamente 1 evento en cdc_outbox."""
import time, pyodbc
DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=30)

cr = sql("dbCR").cursor()
can = sql("fcme_canonicos").cursor()

print("="*70)
print("VERIFICACION FIX PROBLEMA 2 (multi-eventos por lkey no unico)")
print("="*70)

# Test 1: crtpagos con la nueva lkey unica
print("\n[Test 1] crtpagos (sin PK, lkey=qs_abno+qs_cred+ti_cred+sc_reca)")
can.execute("SELECT COUNT(*) FROM cdc_outbox WHERE source_table='dbCR.dbo.crtpagos'")
b1 = can.fetchone()[0]
cr.execute("SELECT TOP 1 qs_abno,qs_cred,ti_cred,sc_reca FROM dbo.crtpagos")
row1 = cr.fetchone()
cr.execute("UPDATE dbo.crtpagos SET ci_rol=ci_rol WHERE qs_abno=? AND qs_cred=? AND ti_cred=? AND sc_reca=?", *row1)
n1 = cr.rowcount
time.sleep(2)
can.execute("SELECT COUNT(*) FROM cdc_outbox WHERE source_table='dbCR.dbo.crtpagos'")
a1 = can.fetchone()[0]
print(f"  Filas updated: {n1}  Eventos generados: {a1-b1}  {'OK' if a1-b1 == n1 else 'FAIL'}")

# Test 2: crtrubros_cobr (multi-type, sin PK, lkey corregida)
print("\n[Test 2] crtrubros_cobr (multi-type: rubroCobranza + rubrosCobranzaDetalle)")
can.execute("SELECT COUNT(*) FROM cdc_outbox WHERE source_table='dbCR.dbo.crtrubros_cobr'")
b2 = can.fetchone()[0]
cr.execute("SELECT TOP 1 ti_cred,co_empr,ti_rubr_pago,ti_pago FROM dbo.crtrubros_cobr")
row2 = cr.fetchone()
if row2:
    cr.execute("UPDATE dbo.crtrubros_cobr SET ti_cred=ti_cred WHERE ti_cred=? AND co_empr=? AND ti_rubr_pago=? AND ti_pago=?", *row2)
    n2 = cr.rowcount
    time.sleep(2)
    can.execute("SELECT COUNT(*) FROM cdc_outbox WHERE source_table='dbCR.dbo.crtrubros_cobr'")
    a2 = can.fetchone()[0]
    expected = n2 * 2  # 2 types
    print(f"  Filas updated: {n2}  Eventos esperados: {expected} ({n2}x2 types)  Eventos generados: {a2-b2}  {'OK' if a2-b2 == expected else 'FAIL'}")
else:
    print("  Tabla vacia, skip")

# Test 3: crtsolid (sin PK, lkey corregida)
print("\n[Test 3] crtsolid (sin PK, lkey=qs_cred+ti_cred+aa_cred)")
can.execute("SELECT COUNT(*) FROM cdc_outbox WHERE source_table='dbCR.dbo.crtsolid'")
b3 = can.fetchone()[0]
cr.execute("SELECT TOP 1 qs_cred,ti_cred,aa_cred FROM dbo.crtsolid")
row3 = cr.fetchone()
if row3:
    cr.execute("UPDATE dbo.crtsolid SET qs_cred=qs_cred WHERE qs_cred=? AND ti_cred=? AND aa_cred=?", *row3)
    n3 = cr.rowcount
    time.sleep(2)
    can.execute("SELECT COUNT(*) FROM cdc_outbox WHERE source_table='dbCR.dbo.crtsolid'")
    a3 = can.fetchone()[0]
    print(f"  Filas updated: {n3}  Eventos generados: {a3-b3}  {'OK' if a3-b3 == n3 else 'FAIL'}")

# Verificar payload (no concatenado)
print("\n[Test 4] Inspeccionar payload de los ultimos eventos (no concatenado)")
can.execute("SELECT TOP 5 source_table, aggregate_type, LEN(payload) AS payload_len, LEFT(payload, 80) FROM cdc_outbox WHERE source_table LIKE 'dbCR.dbo.crt%' ORDER BY id DESC")
for src, agg, plen, head in can.fetchall():
    obj_count = head.count('{"')
    flag = "OK " if obj_count == 1 else f"FAIL ({obj_count} objetos)"
    print(f"  [{flag}] {src:<35} {agg:<30} len={plen:>6}")
