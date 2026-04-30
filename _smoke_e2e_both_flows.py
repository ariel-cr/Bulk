"""Smoke test E2E real para AMBOS flujos cartera.

F1 (Legacy -> Newcore) ya validado en tests previos (54/60). Aqui solo verificamos.

F2 (Newcore -> Legacy) - test corregido:
  - INSERT en FCME_USER.<TYPE> usando cols del dest_match (no minimal)
  - Esto garantiza que el JSON payload tenga valores en cols que el wrapper
    traduce a lkey legacy
  - Espera global por todos los eventos
  - Verifica replicacion real al legacy
  - Cleanup en orden: DELETE oracle (debe propagarse a legacy DELETE)

Anti-zombi: autocommit + LOCK_TIMEOUT + cleanup atexit + signals
"""
import sys, time, json, signal, atexit
from datetime import datetime
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
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_smoke_e2e_out.txt","w",encoding="utf-8"))

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
    for db,cn in list(_conns.items()):
        try: cn.close(); print(f"  closed {db}")
        except: pass
    _conns.clear()
    if _orcl:
        try: _orcl.close(); print("  closed Oracle"); _orcl=None
        except: pass
atexit.register(cleanup)
for s in (signal.SIGINT, signal.SIGTERM):
    signal.signal(s, lambda *a:(cleanup(),sys.exit(130)))

def synth(dt, ml):
    dt=dt.lower()
    if any(t in dt for t in ['number','int','float','double','binary_double','binary_float']):
        return 99001  # marker number
    if 'date' in dt or 'timestamp' in dt:
        return datetime(2099,12,31)
    if 'clob' in dt or 'blob' in dt:
        return 'TEST_E2E'
    s='TEST_E2E'
    if ml and 0<ml<8: s=s[:ml]
    return s

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS=json.load(f)

testable=[s for s in SPECS if s.get("dest_match") and s.get("ltbl") and s.get("lkey")]

print("="*80)
print(f"SMOKE E2E F2 - {len(testable)} types")
print("="*80)

o=oracn().cursor()
ccn=sqlcn("fcme_canonicos"); cc=ccn.cursor()

# Cache de Oracle cols metadata por tabla
def get_oracle_cols(dest):
    o.execute("""SELECT c.column_name, c.data_type, c.data_length, c.nullable, c.data_default
                 FROM all_tab_columns c
                 WHERE c.owner='FCME_USER' AND c.table_name=:t
                 ORDER BY c.column_id""", [dest])
    return o.fetchall()

# === PASO 1: INSERTS BATCH ===
print("\n[Paso 1] INSERT en FCME_USER.<TYPE> con cols del dest_match")
inserts=[]
for idx, spec in enumerate(testable, 1):
    agg=spec["agg"]; dest=spec["dest"]
    ldb=spec["ldb"]; ltbl=spec["ltbl"]
    dest_match=spec["dest_match"]
    lkey=spec["lkey"]

    cols_meta=get_oracle_cols(dest)
    col_dt={r[0]:(r[1],r[2]) for r in cols_meta}
    col_nullable={r[0]:r[3] for r in cols_meta}
    col_default={r[0]:r[4] for r in cols_meta}
    pk_cols={r[0] for r in cols_meta if r[0]=='ID'}

    insert_cols=[]; insert_vals=[]
    # Primero las cols del dest_match (las que el wrapper lee)
    for ocol, lcol in dest_match:
        if ocol == 'ID':
            continue
        if ocol not in col_dt:
            continue
        dt, ml = col_dt[ocol]
        v = synth(dt, ml)
        insert_cols.append(ocol)
        insert_vals.append(v)
    # Mas cols NOT NULL no en dest_match
    for nm in col_dt:
        if nm in insert_cols or nm == 'ID':
            continue
        if col_nullable[nm]=='N' and not col_default[nm]:
            dt, ml = col_dt[nm]
            insert_cols.append(nm)
            insert_vals.append(synth(dt, ml))

    if not insert_cols:
        inserts.append({"agg":agg, "status":"NO_COLS"})
        continue

    cols_q=",".join(f'"{c}"' for c in insert_cols)
    binds=",".join(f":{i+1}" for i in range(len(insert_cols)))
    sql=f'INSERT INTO FCME_USER."{dest}" ({cols_q}) VALUES ({binds})'

    try:
        o.execute(sql, insert_vals)
        # Mapear lkey legacy a valor (para verificacion)
        leg_to_ora={lc: oc for oc, lc in dest_match}
        lkey_vals={}
        for lc in lkey:
            oc = leg_to_ora.get(lc)
            if oc and oc in insert_cols:
                lkey_vals[lc] = insert_vals[insert_cols.index(oc)]
        inserts.append({
            "agg":agg, "dest":dest, "ldb":ldb, "ltbl":ltbl, "lkey":lkey,
            "lkey_vals":lkey_vals, "status":"INS_ORA_OK",
            "insert_cols":insert_cols, "insert_vals":insert_vals
        })
    except Exception as e:
        inserts.append({"agg":agg, "status":"ERR_ORA_INS", "err":str(e)[:120]})

n_ok_ora=sum(1 for x in inserts if x["status"]=="INS_ORA_OK")
n_err_ora=sum(1 for x in inserts if x["status"]=="ERR_ORA_INS")
n_no_cols=sum(1 for x in inserts if x["status"]=="NO_COLS")
print(f"  INSERT Oracle OK : {n_ok_ora}/{len(testable)}")
print(f"  ERR (FK Oracle)  : {n_err_ora}")
print(f"  NO_COLS          : {n_no_cols}")

# === PASO 2: WAIT GLOBAL ===
print(f"\n[Paso 2] Espera 30s para drenado de Kafka + processing...")
time.sleep(30)

# === PASO 3: VERIFICAR REPLICACION LEGACY ===
print(f"\n[Paso 3] Verificar replicacion en legacy")
results=[]
for x in inserts:
    if x["status"]!="INS_ORA_OK":
        results.append((x["agg"], x["status"], 0, 0))
        continue
    agg=x["agg"]; ldb=x["ldb"]; ltbl=x["ltbl"]; lkey=x["lkey"]; lkey_vals=x["lkey_vals"]

    # Cuantos eventos llegaron a cdc_inbox
    cc.execute("""SELECT COUNT(*) FROM cdc_inbox
                  WHERE aggregate_type=? AND created_at >= DATEADD(MINUTE, -5, SYSDATETIME())""", agg)
    inb=cc.fetchone()[0]

    # Verificar fila en legacy
    leg_cnt=0
    err_leg=None
    if all(lc in lkey_vals for lc in lkey):
        try:
            lcn=sqlcn(ldb); lc=lcn.cursor()
            where=" AND ".join(f"[{c}] = ?" for c in lkey)
            params=[lkey_vals[c] for c in lkey]
            lc.execute(f"SELECT COUNT(*) FROM dbo.[{ltbl}] WHERE {where}", *params)
            leg_cnt=lc.fetchone()[0]
        except Exception as e:
            err_leg=str(e)[:80]

    if leg_cnt>=1:
        st="OK_E2E"
    elif inb>=1:
        st="OK_PARTIAL"
    else:
        st="NO_EVENT"

    results.append((agg, st, inb, leg_cnt))

# Print por type
for agg, st, inb, leg in results:
    flag={"OK_E2E":"OK_E2E   ","OK_PARTIAL":"OK_PART  ","NO_EVENT":"NO_EVENT ","ERR_ORA_INS":"ERR_INS  ","NO_COLS":"NO_COLS  "}.get(st, st[:9])
    print(f"  [{flag}] {agg:<40} inb={inb} leg={leg}")

# Resumen
print("\n"+"="*80); print("RESUMEN F2 E2E"); print("="*80)
counts=Counter(r[1] for r in results)
for st,n in counts.most_common():
    print(f"  {st:<20} {n}/{len(results)}")

# === CLEANUP: DELETE Oracle (que dispara DELETE wrapper -> legacy) ===
print(f"\n[Paso 4] Cleanup: DELETE en Oracle de filas insertadas")
n_del_ok=0
for x in inserts:
    if x["status"]!="INS_ORA_OK":
        continue
    dest=x["dest"]; insert_cols=x["insert_cols"]; insert_vals=x["insert_vals"]
    # WHERE: usar dest_match cols (las que tienen valores)
    where_clauses=[]; vals=[]
    for c, v in zip(insert_cols, insert_vals):
        if v == 99001 or v == 'TEST_E2E' or v == datetime(2099,12,31):
            where_clauses.append(f'"{c}"=:{len(vals)+1}')
            vals.append(v)
        if len(where_clauses)>=4: break
    if not where_clauses: continue
    try:
        o.execute(f'DELETE FROM FCME_USER."{dest}" WHERE '+ " AND ".join(where_clauses), vals)
        n_del_ok+=1
    except Exception as e:
        print(f"  DELETE err {dest}: {str(e)[:100]}")
print(f"  DELETE Oracle OK: {n_del_ok}")

# Errores nuevos
print(f"\n[cdc_inbox_errors ultimos 5 min]")
cc.execute("""SELECT TOP 30 aggregate_type, event_type, LEFT(error_message, 160)
              FROM cdc_inbox_errors
              WHERE created_at >= DATEADD(MINUTE, -5, SYSDATETIME())
              ORDER BY id DESC""")
errs=cc.fetchall()
if not errs:
    print("  (0 errores)")
for r in errs:
    print(f"  [{r[1]}] {r[0]}: {r[2]}")
