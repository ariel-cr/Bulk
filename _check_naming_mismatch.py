"""Verifica desincronia entre nombres en MODULE_CONFIG y procedures Oracle."""
import oracledb
ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}
o = oracledb.connect(**ORA).cursor()

# Cartera types
CARTERA = open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8").read()
import json
specs = json.loads(CARTERA)
types = sorted({s["agg"] for s in specs})
ph = ",".join(f":{i+1}" for i in range(len(types)))
o.execute(f"SELECT AGGREGATE_TYPE, SP_NAME, ACTIVE FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE IN ({ph})", types)
config = {r[0]: (r[1], r[2]) for r in o.fetchall()}

# Procedures existentes USP_INBOX_*
o.execute("SELECT object_name, status FROM all_objects WHERE owner='FCME_USER' AND object_type='PROCEDURE' AND object_name LIKE 'USP_INBOX_%'")
procs = {r[0]: r[1] for r in o.fetchall()}

print(f"Cartera types       : {len(types)}")
print(f"Module_config rows  : {len(config)}")
print(f"USP_INBOX_* procs   : {len(procs)}")

mismatches = []
for t in types:
    if t not in config:
        continue
    sp_in_cfg, active = config[t]
    if sp_in_cfg not in procs:
        mismatches.append((t, sp_in_cfg))

print(f"\nMismatches (config apunta a SP que NO existe): {len(mismatches)}")
for t, sp in mismatches:
    print(f"  {t:<40} -> {sp}")
