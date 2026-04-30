"""Smoke test F2: INSERT en FCME_USER.<TYPE> Oracle, verificar propagacion a legacy.

Pasos:
  0) Activar routing F2 (UPDATE active=1)
  1) Para cada type:
     a) Introspect FCME_USER.<TYPE_TABLE> Oracle (cols, nullable, identity)
     b) Build INSERT con valores sinteticos
     c) Capturar baseline de FCME_USER.CDC_OUTBOX, fcme_canonicos.cdc_inbox y legacy table
     d) INSERT en Oracle
     e) Wait ~10s para propagacion E2E
     f) Verificar:
        - +1 evento en CDC_OUTBOX (TRG_OUTBOX disparo)
        - +1 evento en cdc_inbox (Kafka entrego)
        - +1 fila en legacy table (wrapper + sp_CRUD ejecutaron)
     g) DELETE en Oracle (cleanup) -> deberia generar evento DELETE
  2) Reportar matriz: cuantos completaron E2E full

Anti-loop verificado por construccion:
  - sp_<Type>_CRUD legacy hace sp_set_session_context 'is_replicating'=1
  - trg_outbox_<tabla> legacy: IF SESSION_CONTEXT='is_replicating' RETURN

Anti-zombi:
  - autocommit=True, LOCK_TIMEOUT 5000
  - Cleanup atexit + signal handlers
  - Solo testea UN type por iteracion
"""
import sys, time, json, signal, atexit
import pyodbc, oracledb
from collections import defaultdict, Counter

class Tee:
    def __init__(self,*s):self.s=s
    def write(self,t):
        for x in self.s:
            try: x.write(t); x.flush()
            except: pass
    def flush(self):
        for x in self.s:
            try: x.flush()
            except: pass
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_test_f2_insert_out.txt","w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
ORA={'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}
_conns={}; _orcl=None
def sqlcn(db):
    if db in _conns: return _conns[db]
    cn=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
    cn.timeout=30
    cn.cursor().execute("SET LOCK_TIMEOUT 5000")
    _conns[db]=cn
    return cn
def oracn():
    global _orcl
    if _orcl is None:
        _orcl=oracledb.connect(**ORA); _orcl.autocommit=True
    return _orcl

def cleanup():
    global _orcl
    print("\n[cleanup]")
    for db, cn in list(_conns.items()):
        try: cn.close(); print(f"  closed {db}")
        except: pass
    _conns.clear()
    if _orcl:
        try: _orcl.close(); print("  closed Oracle"); _orcl=None
        except: pass
atexit.register(cleanup)
for s in (signal.SIGINT, signal.SIGTERM):
    signal.signal(s, lambda *a:(cleanup(),sys.exit(130)))

from datetime import datetime
def synth(dt, ml):
    dt=dt.lower()
    if any(t in dt for t in ['number','int','float','double','binary_double','binary_float']):
        return 99
    if 'date' in dt or 'timestamp' in dt:
        return datetime(2099,12,31)
    if 'clob' in dt or 'blob' in dt:
        return 'TEST'
    s='TEST'
    if ml and 0<ml<4: s=s[:ml]
    return s

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS=json.load(f)

# Limitar a types que tengan dest_match no vacio (los 89 con TRG_OUTBOX desplegado)
testable=[s for s in SPECS if s.get("dest_match") and s.get("ltbl") and s.get("lkey")]
print(f"Types testables: {len(testable)}\n")

# === Activar routing F2 ===
print("="*80)
print("STEP 0: Activar F2 routing (active=1 cartera entries)")
print("="*80)
ccn=sqlcn("fcme_canonicos"); cc=ccn.cursor()
cc.execute("UPDATE dbo.cdc_inbox_module_config SET active=1 WHERE module_name='CARTERA'")
print(f"  Updated rows: {cc.rowcount}")

# === Helper: insert oracle row ===
o=oracn().cursor()
print("\n"+"="*80)
print(f"STEP 1: INSERT en {len(testable)} FCME_USER.<TYPE> tables")
print("="*80)

results=[]
DATE_LITERAL_TYPES = ('DATE','TIMESTAMP')

for idx, spec in enumerate(testable, 1):
    agg=spec["agg"]; dest=spec["dest"]
    ldb=spec["ldb"]; ltbl=spec["ltbl"]
    dest_pk=spec.get("dest_pk") or []

    # 1) Introspect Oracle dest (compatible Oracle XE)
    try:
        o.execute("""SELECT c.column_name, c.data_type, c.data_length, c.nullable, c.data_default,
                     CASE WHEN c.column_name IN (
                         SELECT acc.column_name FROM all_cons_columns acc
                         JOIN all_constraints ac ON ac.owner=acc.owner AND ac.constraint_name=acc.constraint_name
                         WHERE ac.owner='FCME_USER' AND ac.table_name=:t AND ac.constraint_type='P'
                     ) THEN 'Y' ELSE 'N' END AS is_pk
                     FROM all_tab_columns c
                     WHERE c.owner='FCME_USER' AND c.table_name=:t
                     ORDER BY c.column_id""", [dest, dest])
        cols=o.fetchall()
    except Exception as e:
        results.append((agg, dest, "ERR_INSP", 0, 0, 0, str(e)[:100]))
        print(f" {idx:3} {agg:<40} [ERR_INSP] {str(e)[:80]}")
        continue

    insert_cols=[]; insert_vals=[]; col_dt={}
    # Estrategia: cubrir cols NOT NULL. Si todas son nullable (caso de tablas TYPE
    # con ID identity y resto nullable), elegir UNA col cualquiera para forzar INSERT.
    candidates_nullable=[]
    for nm, dt, ml, nullable, defv, is_pk in cols:
        if nm == 'ID' and is_pk == 'Y':
            continue
        if nullable=='N':
            insert_cols.append(nm)
            insert_vals.append(synth(dt, ml))
            col_dt[nm]=dt
        else:
            candidates_nullable.append((nm, dt, ml))
    # Si no hay cols NOT NULL, usamos la primera nullable como fallback
    if not insert_cols and candidates_nullable:
        nm, dt, ml = candidates_nullable[0]
        insert_cols.append(nm)
        insert_vals.append(synth(dt, ml))
        col_dt[nm]=dt
    if not insert_cols:
        results.append((agg, dest, "NO_COLS", 0, 0, 0, "no required cols"))
        print(f" {idx:3} {agg:<40} [NO_COLS]")
        continue

    # Construir INSERT con bindings
    cols_q=",".join(f'"{c}"' for c in insert_cols)
    binds_q=",".join(f":{i+1}" for i in range(len(insert_cols)))
    insert_sql=f'INSERT INTO FCME_USER."{dest}" ({cols_q}) VALUES ({binds_q})'

    # baseline
    src=f"FCME_USER.{dest}"
    o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE source_table=:s AND aggregate_type=:a", [src, agg])
    out_b=o.fetchone()[0]
    cc.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE source_table=? AND aggregate_type=?", src, agg)
    inb_b=cc.fetchone()[0]
    # baseline en legacy table
    lcn=sqlcn(ldb); lc=lcn.cursor()
    try:
        lc.execute(f"SELECT COUNT(*) FROM dbo.[{ltbl}]")
        leg_b=lc.fetchone()[0]
    except Exception:
        leg_b=None

    # INSERT Oracle
    try:
        o.execute(insert_sql, insert_vals)
    except Exception as e:
        results.append((agg, dest, "ERR_ORA_INS", 0, 0, 0, str(e)[:100]))
        print(f" {idx:3} {agg:<40} [ERR_ORA_INS] {str(e)[:80]}")
        continue

    # esperar propagacion (Kafka + processing)
    time.sleep(4)

    # measure
    o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE source_table=:s AND aggregate_type=:a", [src, agg])
    out_a=o.fetchone()[0]
    cc.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE source_table=? AND aggregate_type=?", src, agg)
    inb_a=cc.fetchone()[0]
    leg_a=None
    if leg_b is not None:
        try:
            lc.execute(f"SELECT COUNT(*) FROM dbo.[{ltbl}]")
            leg_a=lc.fetchone()[0]
        except: pass

    out_d=out_a-out_b; inb_d=inb_a-inb_b
    leg_d=(leg_a-leg_b) if (leg_a is not None and leg_b is not None) else None

    # Cleanup: DELETE de la fila Oracle
    # Necesitamos el PK del row insertado. dest_pk[0] suele ser ID que es identity.
    # Mejor borrar por los cols que pusimos
    where_clauses=[]; where_vals=[]
    for c in insert_cols:
        v = insert_vals[insert_cols.index(c)]
        # solo cols simples
        if v is not None and not str(v).startswith('TEST_DELET'):
            # Skip date matching for safety
            if 'date' in col_dt[c].lower() or 'timestamp' in col_dt[c].lower():
                continue
            where_clauses.append(f'"{c}"=:p{len(where_clauses)+1}')
            where_vals.append(v)
        if len(where_clauses)>=4: break
    delete_err=None
    if where_clauses:
        try:
            o.execute(f'DELETE FROM FCME_USER."{dest}" WHERE '+ " AND ".join(where_clauses), where_vals)
        except Exception as e:
            delete_err=str(e)[:80]
    else:
        delete_err="no where for delete"

    # Status
    if out_d>=1 and inb_d>=1 and (leg_d is None or leg_d>=0):
        st="OK_E2E" if leg_d and leg_d>=1 else ("OK_PARTIAL" if out_d>=1 and inb_d>=1 else "?")
    elif out_d==0:
        st="NO_TRG_OUTBOX"
    elif inb_d==0:
        st="NO_KAFKA"
    else:
        st="?"

    note=f"out={out_d} inb={inb_d} leg={leg_d}"
    if delete_err: note+=f" del_err={delete_err}"
    results.append((agg, dest, st, out_d, inb_d, leg_d, note))
    print(f" {idx:3} {agg:<40} [{st:<13}] {note}")

# Resumen
print("\n"+"="*80)
print("RESUMEN")
print("="*80)
counts=Counter(r[2] for r in results)
for st, n in counts.most_common():
    print(f"  {st:<20} {n}/{len(testable)}")

# Errores nuevos en cdc_inbox_errors canonicos
print("\n--- Errores nuevos en cdc_inbox_errors (ultimos 60s) ---")
cc.execute("""SELECT TOP 20 aggregate_type, event_type, LEFT(error_message, 160), created_at
              FROM dbo.cdc_inbox_errors
              WHERE created_at >= DATEADD(MINUTE, -3, SYSDATETIME())
              ORDER BY id DESC""")
for r in cc.fetchall():
    print(f"  [{r[1]}] {r[0]}: {r[2]}")
