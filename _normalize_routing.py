"""1) Normaliza cdc_table_to_types: agrega columna aggregate_type_emit = nombre sin sp_
2) Actualiza nombres: sp_personaType -> personaType, sp_actualizacionAfiliado_type -> actualizacionAfiliadoType
"""
import pyodbc, re
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123","database":"fcme_canonicos"}
s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={DB['database']};UID={DB['username']};PWD={DB['password']}")
c = pyodbc.connect(s, autocommit=True).cursor()

# 1) agregar columna si no existe
c.execute("""
IF COL_LENGTH('dbo.cdc_table_to_types','aggregate_type_emit') IS NULL
    ALTER TABLE dbo.cdc_table_to_types ADD aggregate_type_emit NVARCHAR(200) NULL;
""")

# 2) obtener tipos actuales y normalizar
c.execute("SELECT DISTINCT canonical_type FROM dbo.cdc_table_to_types")
tipos = [r[0] for r in c.fetchall()]

def normalize(name):
    # sp_personaType -> personaType
    # sp_actualizacionAfiliado_type -> actualizacionAfiliadoType
    n = name
    if n.lower().startswith("sp_"):
        n = n[3:]
    if n.endswith("_type"):
        base = n[:-5]
        n = base + "Type"
    # asegurar que empieza con minuscula
    if n and n[0].isupper():
        n = n[0].lower() + n[1:]
    return n

print(f"{'canonical_type (BD)':<45}  aggregate_type_emit")
print("-" * 90)
for t in sorted(tipos):
    emit = normalize(t)
    print(f"{t:<45}  {emit}")
    c.execute("UPDATE dbo.cdc_table_to_types SET aggregate_type_emit = ? WHERE canonical_type = ?", emit, t)

c.execute("SELECT COUNT(*) FROM dbo.cdc_table_to_types WHERE aggregate_type_emit IS NOT NULL")
print(f"\nFilas con aggregate_type_emit poblado: {c.fetchone()[0]}")
