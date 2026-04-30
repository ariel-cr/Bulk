"""Para los 17 types reportados como MISS, buscar la tabla real en FCME_USER
por similitud token (substring + Levenshtein simple)."""
import oracledb, re, json
from difflib import SequenceMatcher

ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

MISSING = [
    "auxDatosCobrosAdicionalesType","conceptoGastoJudicialType","creditoType",
    "cuentaCuotasType","cuentaPersonasType","cuentaType","cuentasEnLegalType",
    "cuotaCreditoType","estadoLegalType","medidaJudicialType","pagosCreditoType",
    "personaCreditoType","procesoAccionType","refinanciamientoCreditoType",
    "saldoVinculadoType","sobranteCaucion_type","unidadJudicialType",
]

def split_camel(s):
    s = s.replace("_type","").replace("Type","")
    parts = re.findall(r'[A-Z]?[a-z]+|[A-Z]+(?=[A-Z]|$)', s)
    return [p.lower() for p in parts if p]

def score(type_tokens, table_name):
    """Score basado en cuantos tokens del type aparecen en la tabla, mas similitud global."""
    tn = table_name.lower().replace("_type","").replace("_","")
    hits = sum(1 for tk in type_tokens if tk in tn)
    sim = SequenceMatcher(None, "".join(type_tokens), tn).ratio()
    return hits, sim, hits*10 + sim

orcl = oracledb.connect(**ORA)
o = orcl.cursor()
o.execute("SELECT table_name FROM all_tables WHERE owner='FCME_USER' AND table_name LIKE '%TYPE' ORDER BY table_name")
tables = [r[0] for r in o.fetchall()]
print(f"[*] FCME_USER._TYPE tables: {len(tables)}")

results = {}
for agg in MISSING:
    tokens = split_camel(agg)
    scored = []
    for t in tables:
        h, s, total = score(tokens, t)
        if h > 0 or s > 0.5:
            scored.append((total, h, s, t))
    scored.sort(reverse=True)
    top = scored[:8]
    results[agg] = {"tokens": tokens, "candidates": [{"table":t,"hits":h,"sim":round(s,2),"score":round(tot,2)} for tot,h,s,t in top]}
    print(f"\n  {agg}  tokens={tokens}")
    for tot,h,s,t in top:
        print(f"    hits={h}  sim={s:.2f}  score={tot:.2f}  -> {t}")

with open("_introspect_cartera_missing.json","w",encoding="utf-8") as f:
    json.dump(results, f, indent=2)
print("\nGuardado: _introspect_cartera_missing.json")
o.close(); orcl.close()
