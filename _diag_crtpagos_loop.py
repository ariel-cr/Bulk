"""Diagnostico del loop en trg_outbox_crtpagos.

Hipotesis: 632 eventos con mismo timestamp = 1 UPDATE que matcheo 632 filas
porque el lkey usado no es PK unico. Verificar:
  1) PK real de dbCR.dbo.crtpagos
  2) Que lkey uso nuestro trigger (mirar la def actual)
  3) Cuantas filas tienen ci_rol=200108
  4) Otros triggers en la tabla que pudieran cascadear
"""
import pyodbc, json
DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}

def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=30)

c = sql("dbCR").cursor()

print("="*70)
print("DIAGNOSTICO trg_outbox_crtpagos")
print("="*70)

# 1) PK real de crtpagos
print("\n[1] PK de dbCR.dbo.crtpagos:")
c.execute("""SELECT c.name AS col_name, c.column_id
             FROM sys.indexes i
             JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
             JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
             JOIN sys.tables t ON t.object_id=i.object_id
             WHERE t.name='crtpagos' AND i.is_primary_key=1
             ORDER BY ic.key_ordinal""")
pk_cols = [r[0] for r in c.fetchall()]
if pk_cols:
    print(f"  PK declarada: {pk_cols}")
else:
    print(f"  *** NO TIENE PRIMARY KEY DECLARADA ***")

# 2) Indices unicos
print(f"\n[2] Indices UNICOS:")
c.execute("""SELECT i.name, STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal)
             FROM sys.indexes i
             JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
             JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
             JOIN sys.tables t ON t.object_id=i.object_id
             WHERE t.name='crtpagos' AND i.is_unique=1
             GROUP BY i.name""")
for nm, cols in c.fetchall():
    print(f"  {nm}: {cols}")

# 3) Cuantas filas matchean ci_rol=200108
print(f"\n[3] Filas en crtpagos con ci_rol='200108':")
c.execute("SELECT COUNT(*) FROM dbo.crtpagos WHERE ci_rol = '200108'")
cnt = c.fetchone()[0]
print(f"  COUNT = {cnt}")

# 4) Total filas
c.execute("SELECT COUNT(*) FROM dbo.crtpagos")
total = c.fetchone()[0]
print(f"  Total filas en crtpagos: {total:,}")

# 5) Otros triggers en crtpagos
print(f"\n[5] Triggers en dbCR.dbo.crtpagos:")
c.execute("""SELECT t.name, t.is_disabled,
                    OBJECT_DEFINITION(t.object_id) AS def
             FROM sys.triggers t
             JOIN sys.tables tb ON t.parent_id = tb.object_id
             WHERE tb.name = 'crtpagos'""")
for nm, dis, defn in c.fetchall():
    print(f"  - {nm}  disabled={dis}  def_size={len(defn or '')} chars")

# 6) Mirar nuestro spec para crtpagos
print(f"\n[6] Spec automap para crtpagos:")
with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    specs = json.load(f)
for s in specs:
    if s.get("ltbl") == "crtpagos":
        print(f"  agg          : {s['agg']}")
        print(f"  lkey         : {s.get('lkey')}")
        print(f"  pcols (count): {len(s.get('pcols',[]))}")
        break

# 7) Definicion actual del trigger
print(f"\n[7] Header del trg_outbox_crtpagos actual:")
c.execute("""SELECT OBJECT_DEFINITION(t.object_id)
             FROM sys.triggers t
             JOIN sys.tables tb ON t.parent_id = tb.object_id
             WHERE tb.name = 'crtpagos' AND t.name = 'trg_outbox_crtpagos'""")
r = c.fetchone()
if r and r[0]:
    # Print first 60 lines
    lines = r[0].split('\n')
    for i, ln in enumerate(lines[:50]):
        print(f"  L{i+1:3} {ln}")
