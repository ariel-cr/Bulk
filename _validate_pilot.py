"""Valida end-to-end el trigger piloto sin modificar datos de negocio:
1) Conteo antes
2) UPDATE no-destructivo (SET col = col) para disparar trigger
3) Verificar fila en cdc_outbox
4) Test anti-loop con SESSION_CONTEXT
"""
import pyodbc, json

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}

def c(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

cur_fc = c("dbFC").cursor()
cur_can = c("fcme_canonicos").cursor()

# 1) conteo antes
cur_can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
before = cur_can.fetchone()[0]
print(f"[1] cdc_outbox antes: {before} filas")

# obtener un ci_cedu para usar de prueba
cur_fc.execute("SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu ORDER BY ci_cedu")
row = cur_fc.fetchone()
if not row:
    print("  NO HAY DATOS en dbFC.fctbafil_actu — no puedo validar"); raise SystemExit
ci = row[0]
print(f"    ci_cedu de prueba: {ci}")

# 2) UPDATE no-destructivo (dispara trigger sin alterar datos)
print("\n[2] UPDATE no-destructivo: SET ci_cedu = ci_cedu")
cur_fc.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)
print("    ok (no se altero ningun campo)")

# 3) Verificar que llego al outbox
cur_can.execute("""
  SELECT TOP 3 id, aggregate_type, aggregate_id, event_type, source_table, created_at,
         SUBSTRING(payload, 1, 300) AS payload_preview
  FROM dbo.cdc_outbox
  ORDER BY id DESC
""")
print("\n[3] Ultimas 3 filas de cdc_outbox:")
found = False
for r in cur_can.fetchall():
    print(f"    id={r.id}  type={r.aggregate_type}  agg_id={r.aggregate_id}  op={r.event_type}  src={r.source_table}")
    print(f"      payload: {r.payload_preview[:180]}...")
    if r.aggregate_type == "fctbafil_actu" and r.aggregate_id == ci:
        found = True
print(f"\n    -> evento de prueba detectado: {found}")

cur_can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
after = cur_can.fetchone()[0]
print(f"    cdc_outbox despues: {after} filas  (delta={after-before})")

# 4) Anti-loop: con SESSION_CONTEXT no debe escribir
print("\n[4] Test anti-loop con SESSION_CONTEXT('cdc_origin'):")
# IMPORTANTE: SESSION_CONTEXT es por conexion. Debo hacerlo en la MISMA conexion donde hago el UPDATE.
cur_fc.execute("EXEC sp_set_session_context N'cdc_origin', N'CDC_INBOX'")
cur_fc.execute("UPDATE dbo.fctbafil_actu SET ci_cedu = ci_cedu WHERE ci_cedu = ?", ci)
cur_fc.execute("EXEC sp_set_session_context N'cdc_origin', NULL")
cur_can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
after_antiloop = cur_can.fetchone()[0]
print(f"    cdc_outbox despues del UPDATE con SESSION_CONTEXT: {after_antiloop} filas  (delta desde paso 3 = {after_antiloop-after})")
print(f"    Esperado: 0 (el trigger debe NO haber insertado)")
print(f"    Resultado: {'OK ✓' if after_antiloop == after else 'FALLO ✗'}")

# Resumen
print("\n" + "="*60)
print("RESUMEN VALIDACION PILOTO")
print("="*60)
print(f"  Trigger dispara:   {'OK' if after > before else 'FALLO'}  ({after-before} evento(s) publicado(s))")
print(f"  Anti-loop:         {'OK' if after_antiloop == after else 'FALLO'}  (SESSION_CONTEXT bloquea)")
print(f"  Payload JSON:      {'presente' if found else 'ausente'}")
