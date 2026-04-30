"""Extrae los JSON_VALUE($.campo) que usp_inbox_PARTICIPE usa para aggregate_type='personaType'
y compara con columnas de canonicos.participes.personaType."""
import pyodbc, re
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = conn("fcme_newcore").cursor()
c.execute("SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.usp_inbox_PARTICIPE')) AS b")
body = c.fetchone().b or ""

# Encontrar el bloque "IF @aggregate_type = 'personaType'" hasta el siguiente IF
m = re.search(r"IF\s+@aggregate_type\s*=\s*'personaType'\s*BEGIN(.*?)(?:IF\s+@aggregate_type\s*=|\Z)", body, re.S | re.I)
if m:
    bloque = m.group(1)
    # todos los $.campo
    campos = sorted(set(re.findall(r"JSON_VALUE\([^,]+,\s*'\$\.([A-Za-z0-9_]+)'", bloque)))
    # tabla destino
    tabla = re.search(r"INSERT\s+INTO\s+([A-Z\.\[\]]+)", bloque, re.I)
    print(f"Tabla destino en newcore: {tabla.group(1) if tabla else '?'}")
    print(f"\nCampos JSON que usp_inbox_PARTICIPE espera en personaType ({len(campos)}):")
    for f in campos: print(f"  $.{f}")
else:
    print("no encontrado bloque personaType"); raise SystemExit

# comparar con columnas canonicas
c2 = conn("fcme_canonicos").cursor()
c2.execute("""
  SELECT c.name FROM sys.columns c
  WHERE c.object_id=OBJECT_ID('participes.personaType') ORDER BY c.column_id
""")
canonicas = [r.name for r in c2.fetchall()]
print(f"\nColumnas canonicos.participes.personaType ({len(canonicas)}):")
for col in canonicas: print(f"  {col}")

# coincidencias
faltan_en_canonicas = [f for f in campos if f not in canonicas]
sobran_en_canonicas = [c for c in canonicas if c not in campos]
print(f"\nCampos que el SP espera y NO existen en canonicos: {len(faltan_en_canonicas)}")
for f in faltan_en_canonicas: print(f"  MISSING: $.{f}")
print(f"\nColumnas canonicas que el SP IGNORA (extra): {len(sobran_en_canonicas)}")
for f in sobran_en_canonicas: print(f"  EXTRA:   {f}")
