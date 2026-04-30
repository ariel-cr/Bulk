"""Fix surgical SOLO del wrapper usp_inbox_institucionType.

Problema: el payload Newcore->Legacy trae CODIGOEMPRESA=null y la SP destino
participes.sp_institucion_type_crud lo rechaza con
'codigoEmpresa es obligatorio'.

Fix: en el wrapper, despues de parsear el JSON, si @v_codigoEmpresa es NULL:
  1. Intenta tomarlo de dbFC.dbo.sfct_institucion (lookup por ci_institucion)
  2. Si igual no hay, usa 1 (todos los 1102 registros legacy tienen co_empr=1)

NO toca:
- participes.sp_institucion_type_crud (la SP del equipo)
- Otros wrappers (Flujo 1 sigue intacto)
- Triggers, tablas o module_config
"""
import pyodbc, sys, re

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}',
    'username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};"
       f"UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = sql('fcme_canonicos').cursor()

# 1) Leer cuerpo actual
c.execute("SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.usp_inbox_institucionType'))")
body = c.fetchone()[0]
if not body:
    print("[FATAL] No existe dbo.usp_inbox_institucionType"); sys.exit(1)

# 2) Backup en texto (file local)
with open(r"C:/Users/Usuario/Downloads/Bulk/_backup_usp_inbox_institucionType.sql","w",encoding="utf-8") as f:
    f.write("-- Backup ORIGINAL pre-fix\n")
    f.write(body)
print("[BACKUP] _backup_usp_inbox_institucionType.sql")

# 3) Verificar que no este ya parcheado
if "fix codigoEmpresa null" in body:
    print("[SKIP] Ya esta parcheado.")
    sys.exit(0)

# 4) Inyectar bloque defensivo justo antes del comentario "-- Llamar SP original"
INJECT = """
        -- fix codigoEmpresa null: lookup en legacy o default 1
        IF @v_codigoEmpresa IS NULL AND @v_codigoInstitucion IS NOT NULL
        BEGIN
            SELECT TOP 1 @v_codigoEmpresa = co_empr
              FROM dbFC.dbo.sfct_institucion
              WHERE ci_institucion = @v_codigoInstitucion;
        END
        IF @v_codigoEmpresa IS NULL SET @v_codigoEmpresa = 1;

"""
marker = "-- Llamar SP original"
if marker not in body:
    print("[FATAL] No encuentro el marcador '-- Llamar SP original'"); sys.exit(2)
new_body = body.replace(marker, INJECT.lstrip("\n") + "        " + marker, 1)

# 5) Convertir CREATE -> ALTER
new_body = re.sub(r'\bCREATE\s+PROCEDURE\b', 'ALTER PROCEDURE', new_body, count=1, flags=re.I)

# 6) Aplicar ALTER
print("[APPLY] ALTER PROCEDURE dbo.usp_inbox_institucionType")
try:
    c.execute(new_body)
    print("[OK] Wrapper actualizado.")
except Exception as e:
    print(f"[FAIL] {e}"); sys.exit(3)

# 7) Reprocesar el evento que fallo (id=10045) sin volver a disparar el trigger
print("\n[REPLAY] Reintentando inbox_id=10045 directamente con el wrapper")
c.execute("""SELECT id, aggregate_id, aggregate_type, source_table, event_type, payload
             FROM dbo.cdc_inbox WHERE id=10045""")
r = c.fetchone()
if r:
    try:
        # llamada directa al wrapper actualizado
        c.execute("""EXEC dbo.usp_inbox_institucionType
                       @inbox_id=?, @aggregate_id=?, @aggregate_type=?,
                       @source_table=?, @event_type=?, @payload=?""",
                  r.id, r.aggregate_id, r.aggregate_type, r.source_table, r.event_type, r.payload)
        print("[REPLAY OK] sin errores")
    except Exception as e:
        print(f"[REPLAY FAIL] {e}")

    # ver si genero un nuevo error
    c.execute("SELECT TOP 1 inbox_id, error_message FROM dbo.cdc_inbox_errors ORDER BY error_id DESC")
    last_err = c.fetchone()
    if last_err and last_err.inbox_id == 10045:
        print(f"[NUEVO ERR] {last_err.error_message[:200]}")
    else:
        print("[OK] No hay error nuevo para id=10045")

print("\n=== FIN FIX ===")
