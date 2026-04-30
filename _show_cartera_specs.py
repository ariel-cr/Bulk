import json
with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    specs = json.load(f)

print(f"Total types: {len(specs)}\n")

# Lista 1: solo los types
print("=== LISTA 1: 91 TYPES (orden alfabetico) ===")
for i, s in enumerate(sorted(specs, key=lambda x: x.get("agg","")), 1):
    print(f"  {i:3}. {s['agg']}")

# Lista 2: type -> legacy table (automap, sin opinion)
print(f"\n=== LISTA 2: TYPE -> ldb.ltbl (segun automap) ===")
print(f"  {'#':>3}  {'agg_type':<40} {'ldb':<6} {'ltbl':<35} {'score':>6}  conf")
print("  " + "-"*100)
for i, s in enumerate(sorted(specs, key=lambda x: x.get("agg","")), 1):
    if "ltbl" not in s:
        print(f"  {i:3}  {s['agg']:<40} (sin match)")
        continue
    print(f"  {i:3}  {s['agg']:<40} {s['ldb']:<6} {s['ltbl']:<35} {s.get('total_score',0):>6.2f}  {s.get('confidence','?')}")

# Lista 3: distribucion por BD
print(f"\n=== LISTA 3: DISTRIBUCION POR BD LEGACY ===")
from collections import Counter
dbs = Counter(s.get("ldb","?") for s in specs if "ltbl" in s)
for db, n in dbs.most_common():
    print(f"  {db:<8}: {n} types")
