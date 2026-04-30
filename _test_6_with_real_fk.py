"""Test las 6 tablas restantes usando valores FK reales de filas padres existentes.

Estrategia: en vez de insertar padres, traemos valores FK validos de SELECT TOP 1
del padre. Asi cumplimos FK sin tocar otras tablas.

Pasos por tabla:
  1) Detectar FKs salientes -> (parent_tbl, parent_col, child_col)
  2) SELECT TOP 1 parent_cols FROM parent_tbl  (valores FK validos)
  3) Build INSERT child:
       - cols FK: valor real del padre
       - cols NOT NULL no-FK no-identity: valor sintetico (99, 'TEST', '2099-12-31')
  4) Verificar evento INSERT
  5) DELETE la fila por nuestro lkey
  6) Verificar evento DELETE
"""
import sys, time, json, signal, atexit
import pyodbc
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

LOG=r"C:\Users\Usuario\Downloads\Bulk\_test_6_real_fk_out.txt"
sys.stdout=Tee(sys.__stdout__, open(LOG,"w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
_conns={}
def conn(db):
    if db in _conns: return _conns[db]
    cn=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
    cn.timeout=30
    c=cn.cursor(); c.execute("SET LOCK_TIMEOUT 5000")
    _conns[db]=cn
    return cn

def cleanup():
    print("\n[cleanup]")
    for db,cn in list(_conns.items()):
        try: cn.close(); print(f"  closed {db}")
        except: pass
    _conns.clear()

atexit.register(cleanup)
for s in (signal.SIGINT, signal.SIGTERM):
    signal.signal(s, lambda *a:(cleanup(),sys.exit(130)))

def synth(dt, ml):
    dt=dt.lower()
    if any(t in dt for t in ['int','numeric','decimal','money','float','real','bit']):
        return 99
    if 'date' in dt or 'time' in dt:
        return '2099-12-31'
    s='TEST'
    if ml and 0<ml<4: s=s[:ml]
    return s

TARGETS = [
    ("dbCR","crtbcdeb_cnta"),("dbCR","crtbdeud_conv"),
    ("dbCR","crtbgest_cred"),("dbCR","crtboper_dref_liqd"),
    ("dbCR","crtbrngo_intr_cred"),("dbCR","crtbsegu_cred"),
]

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    SPECS=json.load(f)

can_cn=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=fcme_canonicos;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10)
can_cn.timeout=30
can=can_cn.cursor(); can.execute("SET LOCK_TIMEOUT 5000")
_conns['fcme_canonicos']=can_cn

print("="*90)
print("TEST 6 TABLAS RESTANTES - INSERT con FK reales del padre")
print("="*90)

results=[]
for db, tbl in TARGETS:
    spec=next(s for s in SPECS if s.get("ldb")==db and s.get("ltbl")==tbl)
    aggs=[s["agg"] for s in SPECS if s.get("ldb")==db and s.get("ltbl")==tbl]
    lkey=spec.get("lkey",[])
    src=f"{db}.dbo.{tbl}"
    n_types=len(aggs)

    print(f"\n--- {db}.{tbl} ({n_types} types) ---")
    cn=conn(db); c=cn.cursor()

    # 1) FKs salientes: child_col -> parent_tbl(parent_col)
    c.execute("""SELECT fk.name AS fk_name,
                        OBJECT_NAME(fkc.referenced_object_id) AS parent_tbl,
                        col_p.name AS parent_col,
                        col_c.name AS child_col
                 FROM sys.foreign_keys fk
                 JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id=fk.object_id
                 JOIN sys.columns col_p ON col_p.object_id=fkc.referenced_object_id AND col_p.column_id=fkc.referenced_column_id
                 JOIN sys.columns col_c ON col_c.object_id=fkc.parent_object_id AND col_c.column_id=fkc.parent_column_id
                 WHERE fk.parent_object_id=OBJECT_ID(?)
                 ORDER BY fk.object_id, fkc.constraint_column_id""", f"dbo.{tbl}")
    fks=c.fetchall()

    # Agrupar por fk_name (un FK puede ser multi-col)
    fk_groups=defaultdict(list)
    for fk_name, ptbl, pcol, ccol in fks:
        fk_groups[fk_name].append((ptbl, pcol, ccol))
    print(f"  FK groups: {list(fk_groups.keys())}")

    # 2) Para cada FK group, conseguir valores reales del padre
    fk_values={}  # child_col -> value
    fk_failed=False
    for fk_name, items in fk_groups.items():
        ptbl=items[0][0]
        pcols=[item[1] for item in items]
        ccols=[item[2] for item in items]
        try:
            sel="["+"],[".join(pcols)+"]"
            c.execute(f"SELECT TOP 1 {sel} FROM dbo.[{ptbl}]")
            row=c.fetchone()
            if not row:
                print(f"  [SKIP] padre {ptbl} esta vacio - no hay FK validos")
                fk_failed=True; break
            for ccol, val in zip(ccols, row):
                fk_values[ccol]=val
            print(f"  FK {fk_name} -> {ptbl}({','.join(pcols)})={tuple(row)}")
        except Exception as e:
            print(f"  [FAIL] No pude obtener FK valido de {ptbl}: {str(e)[:120]}")
            fk_failed=True; break

    if fk_failed:
        results.append((db, tbl, "FK_PARENT_EMPTY", n_types, 0, 0, "padre vacio"))
        continue

    # 3) Build INSERT
    c.execute("""SELECT c.name, t.name AS dt, c.max_length, c.is_nullable, c.is_identity, c.is_computed,
                        CASE WHEN dc.definition IS NOT NULL THEN 1 ELSE 0 END AS has_default
                 FROM sys.columns c
                 JOIN sys.types t ON t.user_type_id=c.user_type_id
                 LEFT JOIN sys.default_constraints dc ON dc.parent_object_id=c.object_id AND dc.parent_column_id=c.column_id
                 WHERE c.object_id=OBJECT_ID(?)
                 ORDER BY c.column_id""", f"dbo.{tbl}")
    cols=c.fetchall()

    insert_cols=[]
    insert_vals=[]
    for nm, dt, ml, nullable, ident, comp, has_def in cols:
        if ident or comp: continue
        if nm in fk_values:
            insert_cols.append(nm)
            insert_vals.append(fk_values[nm])
        elif not nullable and not has_def:
            insert_cols.append(nm)
            insert_vals.append(synth(dt, ml))

    if not insert_cols:
        results.append((db, tbl, "NO_COLS", n_types, 0, 0, "no required cols"))
        continue

    cols_q=",".join(f"[{c}]" for c in insert_cols)
    placeholders=",".join("?"*len(insert_cols))
    print(f"  INSERT cols: {insert_cols}")

    # Baseline
    ph=",".join("?"*len(aggs))
    can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({ph})", src, *aggs)
    before=can.fetchone()[0]

    # INSERT
    inserted=False; err=None
    try:
        c.execute(f"INSERT INTO dbo.[{tbl}] ({cols_q}) VALUES ({placeholders})", *insert_vals)
        inserted=c.rowcount>0
    except Exception as e:
        err=str(e)[:200]
        print(f"  [ERR_INSERT] {err}")
        results.append((db, tbl, "ERR_INSERT", n_types, 0, 0, err))
        continue

    time.sleep(2)
    can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({ph})", src, *aggs)
    after_ins=can.fetchone()[0]
    n_ins=after_ins-before
    expected_ins=n_types

    payload_ok=None
    if n_ins>0:
        can.execute(f"SELECT TOP 1 LEFT(payload, 200) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({ph}) ORDER BY id DESC", src, *aggs)
        head=(can.fetchone() or [""])[0] or ""
        payload_ok=head.count('{"')==1
    print(f"  INSERT events: {n_ins}/{expected_ins}  payload_ok={payload_ok}")

    # DELETE cleanup
    del_err=None
    try:
        if lkey and all(k in insert_cols for k in lkey):
            where=" AND ".join(f"[{k}] = ?" for k in lkey)
            params=[insert_vals[insert_cols.index(k)] for k in lkey]
            c.execute(f"DELETE FROM dbo.[{tbl}] WHERE {where}", *params)
            print(f"  DELETE filas: {c.rowcount}")
        else:
            del_err="lkey not in insert_cols"
    except Exception as e:
        del_err=str(e)[:120]

    time.sleep(2)
    can.execute(f"SELECT COUNT(*) FROM cdc_outbox WHERE source_table=? AND aggregate_type IN ({ph})", src, *aggs)
    after_del=can.fetchone()[0]
    n_del=after_del-after_ins
    expected_del=n_types if not del_err else 0

    if n_ins==expected_ins and n_del==expected_del and (payload_ok or n_ins==0):
        st="OK"
    elif n_ins!=expected_ins:
        st="INS_MISMATCH"
    elif n_del!=expected_del:
        st="DEL_MISMATCH"
    else:
        st="BAD_PAYLOAD"

    note=f"ins={n_ins}/{expected_ins} del={n_del}/{expected_del}"+(f" del_err={del_err}" if del_err else "")
    results.append((db, tbl, st, n_types, n_ins, n_del, note))
    print(f"  {st} {note}")

print("\n"+"="*90)
print("RESUMEN")
print("="*90)
counts=Counter(r[2] for r in results)
for st, n in counts.most_common():
    print(f"  {st:<20} {n}/{len(TARGETS)}")
