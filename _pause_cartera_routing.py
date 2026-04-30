"""Pausa: UPDATE active=0 para los 91 cartera types.
NO borra entries ni wrappers."""
import json, oracledb
ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS = json.load(f)
TYPES = sorted({s["agg"] for s in SPECS})

orcl = oracledb.connect(**ORA); orcl.autocommit=False
o = orcl.cursor()

ph = ",".join(f":{i+1}" for i in range(len(TYPES)))
o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE IN ({ph}) AND ACTIVE=1", TYPES)
before = o.fetchone()[0]
print(f"Entries cartera con active=1 ANTES: {before}/{len(TYPES)}")

o.execute(f"UPDATE FCME_USER.CDC_INBOX_MODULE_CONFIG SET ACTIVE=0 WHERE AGGREGATE_TYPE IN ({ph})", TYPES)
print(f"Updated rows: {o.rowcount}")
orcl.commit()

o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE IN ({ph}) AND ACTIVE=1", TYPES)
print(f"Entries cartera con active=1 DESPUES: {o.fetchone()[0]}/{len(TYPES)}")

# Estado CDC_INBOX
o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=0")
print(f"CDC_INBOX pendientes (PROCESSED=0): {o.fetchone()[0]}")

orcl.close()
print("\nROUTING PAUSADO. Los wrappers existen pero no se ejecutan.")
print("Triggers F1 legacy siguen activos (cdc_outbox sigue recibiendo eventos)")
print("pero TRG_PROCESS_CDC_INBOX devuelve v_sp=NULL -> no rutea.")
