"""Paso 1: Deshabilitar los 76 triggers legacy, inspeccionar tablas canonicas.
Paso 2: Limpiar los 4 eventos de prueba de canonicos.cdc_outbox"""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

print("== [1] Deshabilitando los 76 triggers legacy ==")
disabled = 0
for db in ["dbCG","dbCR","dbCT","dbFC","dbIM","dbNO","dbSV"]:
    c = conn(db).cursor()
    c.execute("""
      SELECT s.name sch, o.name tbl, tr.name tr
      FROM sys.triggers tr
      JOIN sys.objects o ON tr.parent_id = o.object_id
      JOIN sys.schemas s ON o.schema_id=s.schema_id
      WHERE tr.name LIKE 'trg_outbox_%' AND tr.is_disabled = 0
    """)
    rows = c.fetchall()
    for r in rows:
        try:
            c.execute(f"DISABLE TRIGGER [{r.sch}].[{r.tr}] ON [{r.sch}].[{r.tbl}]")
            disabled += 1
        except Exception as e:
            print(f"  fail {db}.{r.tr}: {e}")
print(f"  deshabilitados: {disabled}")

print("\n== [2] Limpiando 4 eventos de prueba de canonicos.cdc_outbox ==")
c = conn("fcme_canonicos").cursor()
c.execute("""
  INSERT INTO dbo.cdc_outbox_archive
   (reason, id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
  SELECT N'test-events-from-76-triggers', id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at
  FROM dbo.cdc_outbox
""")
print(f"  archivados: {c.rowcount}")
c.execute("DELETE FROM dbo.cdc_outbox")
print(f"  borrados: {c.rowcount}")

print("\n== [3] Tablas en fcme_canonicos.participes ==")
c.execute("""
  SELECT t.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
  WHERE s.name='participes' ORDER BY t.name
""")
tables = [r.name for r in c.fetchall()]
print(f"  total: {len(tables)}")
for t in tables: print(f"    {t}")

# los 9 que newcore soporta
TARGET = ["personaType","direccionType","telefonoType","infoBasicaType","infoAdicionalType",
          "ingresosEgresosType","personaVinculadaType","saldoPersonaNaturalType","historicoEstadosType"]
print(f"\n  Necesitamos triggers en estas 9 (que newcore soporta):")
for t in TARGET:
    exists = t in tables
    print(f"    {t:<30} {'EXISTE' if exists else 'NO EXISTE en canonicos'}")

# estructura de personaType canonica
print("\n== [4] Estructura canonicos.participes.personaType (verificar camelCase) ==")
c.execute("""
  SELECT c.name, t.name tp, c.max_length, c.is_nullable
  FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
  WHERE c.object_id=OBJECT_ID('participes.personaType') ORDER BY c.column_id
""")
rows = c.fetchall()
print(f"  columnas: {len(rows)}")
for r in rows: print(f"    {r.name:<40} {r.tp:<15} null={r.is_nullable}")
