"""Validacion final: dispara UPDATE no-destructivo en varias BDs y verifica payload limpio."""
import pyodbc, json

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}

def c(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# muestras de varias BDs (una con PK simple, una con PK compuesta, una sin PK)
SAMPLES = [
    ("dbFC", "fctbafil_actu",    "ci_cedu"),        # PK 1 col
    ("dbFC", "sfct_afiliado",    "co_empr,ci_cedula"), # PK compuesta
    ("dbSV", "svtbcaus",         None),             # sin PK -> hash
]

cur_can = c("fcme_canonicos").cursor()
cur_can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
before = cur_can.fetchone()[0]
print(f"cdc_outbox antes: {before}\n")

for db, tbl, pk in SAMPLES:
    print(f"-- {db}.{tbl} (pk={pk or 'none'})")
    cur = c(db).cursor()
    # UPDATE no-destructivo: tocar una col cualquiera consigo misma sobre TOP 1
    try:
        if pk:
            first = pk.split(",")[0]
            cur.execute(f"UPDATE TOP (1) dbo.[{tbl}] SET [{first}] = [{first}]")
        else:
            # sin pk, tomar cualquier columna
            cur.execute(f"""
              SELECT TOP 1 name FROM sys.columns WHERE object_id = OBJECT_ID(?)
              ORDER BY column_id
            """, f"dbo.{tbl}")
            col = cur.fetchone()[0]
            cur.execute(f"UPDATE TOP (1) dbo.[{tbl}] SET [{col}] = [{col}]")
        print(f"   UPDATE ok")
    except Exception as e:
        print(f"   UPDATE FAIL: {e}")
        continue

    # leer ultimo evento
    cur_can.execute("""
      SELECT TOP 1 id, aggregate_type, aggregate_id, event_type, source_table,
             SUBSTRING(payload,1,250) AS p
      FROM dbo.cdc_outbox
      WHERE source_table = ?
      ORDER BY id DESC
    """, f"{db}.dbo.{tbl}")
    r = cur_can.fetchone()
    if not r:
        print(f"   !! no llego al outbox"); continue
    print(f"   id={r.id} type={r.aggregate_type} agg_id={r.aggregate_id} op={r.event_type}")
    print(f"   payload[0:250]: {r.p}")
    # verificar que NO aparezca "rn" como primer campo
    if r.p.strip().startswith('{"rn"') or '"rn":' in r.p[:20]:
        print(f"   !! BUG: 'rn' aparece en payload")
    else:
        print(f"   payload limpio (sin rn)")

cur_can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
after = cur_can.fetchone()[0]
print(f"\ncdc_outbox despues: {after}  (delta={after-before})")

# conteo por aggregate_type
print("\n== Conteo por aggregate_type ==")
cur_can.execute("""
  SELECT aggregate_type, COUNT(*) n, MAX(id) last_id
  FROM dbo.cdc_outbox GROUP BY aggregate_type ORDER BY n DESC
""")
for r in cur_can.fetchall():
    print(f"   {r.aggregate_type:<35} n={r.n}  last_id={r.last_id}")
