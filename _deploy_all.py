"""Despliega los 76 triggers leyendo cdc_outbox_triggers.sql.
Parseo por USE [db] y GO, ejecuta cada batch en la BD correcta."""
import re, pyodbc, sys, time

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}

def conn(db):
    s = (f"DRIVER={DB['driver']};SERVER={DB['server']};"
         f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

with open(r"C:\Users\Usuario\Downloads\Bulk\cdc_outbox_triggers.sql","r",encoding="utf-8") as f:
    text = f.read()

# split por 'GO' solo a principio de linea
batches_raw = re.split(r'(?im)^\s*GO\s*$', text)

# Agrupar por BD actual
connections = {}
def get_conn(db):
    if db not in connections:
        connections[db] = conn(db)
    return connections[db]

current_db = None
ok, fail = 0, 0
failures = []
created_triggers = []

USE_RE = re.compile(r"(?im)^\s*USE\s+\[?([A-Za-z0-9_]+)\]?\s*;?\s*$")

for i, raw in enumerate(batches_raw):
    batch = raw
    if not batch.strip(): continue
    # Extraer TODAS las sentencias USE, quedarse con la ULTIMA y quitarlas del batch
    uses = USE_RE.findall(batch)
    if uses:
        current_db = uses[-1]
        batch = USE_RE.sub("", batch)
    # limpiar lo que queda
    stripped = batch.strip()
    if not stripped: continue
    # si solo son comentarios, skip
    non_comment = re.sub(r"/\*.*?\*/", "", stripped, flags=re.S)
    non_comment = re.sub(r"^\s*--.*$", "", non_comment, flags=re.M).strip()
    if not non_comment: continue
    if current_db is None: continue
    # Ejecutar
    try:
        c = get_conn(current_db).cursor()
        c.execute(batch)
        if "CREATE TRIGGER" in batch.upper():
            trg = re.search(r"CREATE\s+TRIGGER\s+dbo\.(\w+)", batch, re.I)
            if trg:
                created_triggers.append((current_db, trg.group(1)))
                ok += 1
    except Exception as e:
        fail += 1
        head = stripped[:120].replace("\n"," ")
        failures.append((current_db, head, str(e)[:250]))

print(f"\n== DEPLOY SUMMARY ==")
print(f"  Triggers creados: {ok}")
print(f"  Fallos: {fail}")
if failures:
    print("\n  FALLOS:")
    for db, head, err in failures:
        print(f"   [{db}] {head}")
        print(f"       -> {err}")

# Validar contra sys.triggers
print("\n== Verificacion en sys.triggers (por BD) ==")
for db in ["dbCG","dbCR","dbCT","dbFC","dbIM","dbNO","dbSV"]:
    try:
        c = get_conn(db).cursor()
        c.execute("""
          SELECT COUNT(*) FROM sys.triggers
          WHERE name LIKE 'trg_outbox_%' AND is_disabled = 0
        """)
        n = c.fetchone()[0]
        print(f"  {db}: {n} triggers activos con prefijo trg_outbox_")
    except Exception as e:
        print(f"  {db}: ERROR {e}")
