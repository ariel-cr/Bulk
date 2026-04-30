"""Detecta una lkey realmente unica para cada una de las 60 tablas legacy
y actualiza _cartera_specs.json. Tres niveles de busqueda:

  1) PK declarada -> usar
  2) Unique index -> usar
  3) Ninguno: buscar una combinacion unica de cols clave-like
     (prefijos sc_, qs_, co_, nu_, ci_, ti_, aa_) por sampling
     usando COUNT(*) vs COUNT(DISTINCT ...)
"""
import json, pyodbc
from collections import defaultdict
from itertools import combinations

DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}

def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=30)

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)

# Agrupar por (ldb, ltbl) - una sola busqueda por tabla unica
tables = {}
for s in SPECS:
    if "ltbl" not in s: continue
    key = (s["ldb"], s["ltbl"])
    tables[key] = s

KEY_PREFIXES = ("sc_","qs_","co_","nu_","ci_","ti_","aa_","fe_","fx_","id_")

def find_pk_or_unique(c, tbl):
    # 1) PK
    c.execute("""SELECT c.name FROM sys.indexes i
                 JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
                 JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
                 JOIN sys.tables t ON t.object_id=i.object_id
                 WHERE t.name=? AND i.is_primary_key=1
                 ORDER BY ic.key_ordinal""", tbl)
    pk = [r[0] for r in c.fetchall()]
    if pk: return pk, "PK"

    # 2) Unique index
    c.execute("""SELECT i.name, c.name, ic.key_ordinal
                 FROM sys.indexes i
                 JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
                 JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
                 JOIN sys.tables t ON t.object_id=i.object_id
                 WHERE t.name=? AND i.is_unique=1 AND i.is_primary_key=0
                 ORDER BY i.index_id, ic.key_ordinal""", tbl)
    uq = defaultdict(list)
    for nm, col, ord_ in c.fetchall():
        uq[nm].append((ord_, col))
    if uq:
        # tomar el unique mas chico
        best = min(uq.items(), key=lambda x: len(x[1]))
        return [c for _, c in sorted(best[1])], f"UNIQUE_IDX={best[0]}"
    return None, None

def find_unique_combo(c, db, tbl, all_cols):
    """Busca por sampling combinacion unica entre cols 'key-like'."""
    keylike = [col for col in all_cols if col.lower().startswith(KEY_PREFIXES)]
    if not keylike:
        keylike = all_cols[:6]
    keylike = keylike[:8]  # limitar para combinaciones

    c.execute(f"SELECT COUNT(*) FROM dbo.[{tbl}]")
    total = c.fetchone()[0]
    if total == 0:
        return keylike[:3], f"EMPTY_TBL_USE_FIRST3"
    if total > 500_000:
        # tabla grande: solo testeamos combinaciones sin sampling exhaustivo
        return keylike[:5], "BIG_TBL_USE_FIRST5_KEYLIKE"

    # Probar combos de 1, 2, 3, 4 cols
    for r in range(1, min(5, len(keylike)+1)):
        for combo in combinations(keylike, r):
            cols_q = ",".join(f"[{x}]" for x in combo)
            try:
                c.execute(f"SELECT COUNT(*), COUNT(DISTINCT CONCAT_WS('|',{cols_q})) FROM dbo.[{tbl}]")
                cnt, dist = c.fetchone()
                if cnt == dist and cnt > 0:
                    return list(combo), f"UNIQUE_COMBO_TESTED({cnt}={dist})"
            except Exception:
                continue
    return keylike[:4], "NO_UNIQUE_USE_FIRST4_KEYLIKE"

print(f"Analizando {len(tables)} tablas legacy unicas...\n")

results = {}
for (ldb, ltbl), spec in sorted(tables.items()):
    c = sql(ldb).cursor()
    pk, src = find_pk_or_unique(c, ltbl)
    if pk:
        results[(ldb, ltbl)] = (pk, src)
        cur_lkey = spec.get("lkey", [])
        flag = "OK " if cur_lkey == pk else "CHG"
        print(f"  [{flag}] {ldb}.{ltbl:<35} pk={pk} src={src}  cur={cur_lkey}")
        continue
    # Sin PK ni unique index
    all_cols = [col["name"] for col in __import__("json").loads(open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8").read())[0].get("dest_columns",[])]  # placeholder, reemplazado abajo
    # Mejor: leer pcols del spec
    all_cols = spec.get("pcols", [])
    if not all_cols:
        # leer cols reales
        c.execute("SELECT name FROM sys.columns WHERE object_id=OBJECT_ID(?)", f"dbo.{ltbl}")
        all_cols = [r[0] for r in c.fetchall()]
    combo, src2 = find_unique_combo(c, ldb, ltbl, all_cols)
    results[(ldb, ltbl)] = (combo, src2)
    cur_lkey = spec.get("lkey", [])
    flag = "OK " if cur_lkey == combo else "CHG"
    print(f"  [{flag}] {ldb}.{ltbl:<35} key={combo} src={src2}  cur={cur_lkey}")

# Update SPECS
print(f"\nActualizando specs...")
n_changed = 0
for s in SPECS:
    if "ltbl" not in s: continue
    key = (s["ldb"], s["ltbl"])
    new_lkey, src = results.get(key, (s.get("lkey"), "NO_CHANGE"))
    if list(new_lkey) != list(s.get("lkey", [])):
        s["lkey"] = list(new_lkey)
        s["lkey_source"] = src
        n_changed += 1

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","w",encoding="utf-8") as f:
    json.dump(SPECS, f, indent=2, ensure_ascii=False)

print(f"Specs actualizados: {n_changed} types con nueva lkey")
