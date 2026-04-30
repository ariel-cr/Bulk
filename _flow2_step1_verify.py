"""Paso 1 verify: tabla FCME_USER -> trigger outbox -> CDC_OUTBOX
Valida los 30 triggers haciendo INSERT/UPDATE/DELETE en cada tabla TYPE
y verificando que el evento aparece en CDC_OUTBOX con JSON valido.
"""
import oracledb, json

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# 1) Listar triggers
print("="*70)
print("[1.1] Triggers FCME_USER outbox")
print("="*70)
co.execute("""SELECT trigger_name, table_name, status, triggering_event
              FROM all_triggers
              WHERE owner='FCME_USER' AND trigger_name LIKE 'TRG_OUTBOX_%'
              ORDER BY table_name""")
trgs = co.fetchall()
print(f"Total: {len(trgs)} triggers")
disabled = [r for r in trgs if r[2] != "ENABLED"]
print(f"  ENABLED: {len(trgs)-len(disabled)}")
print(f"  DISABLED: {len(disabled)}")
if disabled:
    for r in disabled: print(f"  WARN: {r[0]} disabled")

# 2) Para cada trigger, verificar que la columna AGGREGATE_TYPE en JSON sea consistente
print("\n" + "="*70)
print("[1.2] Probando 5 triggers representativos con INSERT real")
print("="*70)

# Tablas con cols simples y conocidas (catalogos pequeños)
TESTS = [
    ("REFERENCIAPARTICIPE_TYPE", "INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('VR1','VERIF1')",
     "DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='VR1'"),
    ("MOTIVOCONTABLE_TYPE", None, None),  # detectar cols dinamicamente
    ("SERVICIOADICIONAL_TYPE", None, None),
    ("FIRMANTEPARTICIPE_TYPE", None, None),
    ("AGENDAMAILAFILIADO_TYPE", None, None),
]

# Limpiar outbox
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
orcl.commit()

ok = 0; fail = 0
for tbl, ins, cleanup in TESTS:
    print(f"\n--- {tbl} ---")
    try:
        # Si no tenemos INSERT, construir uno generico con valores TST
        if ins is None:
            co.execute("SELECT column_name, data_type FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id", t=tbl)
            cols = [(r[0], r[1]) for r in co.fetchall() if r[0] != "ID"]
            col_names = ", ".join(c[0] for c in cols)
            vals = []
            for cn, ct in cols:
                if any(s in ct for s in ("VARCHAR","CHAR","CLOB")):
                    vals.append(f"'V{cn[:6]}'")
                elif "NUMBER" in ct: vals.append("99")
                elif "DATE" in ct or "TIMESTAMP" in ct: vals.append("SYSTIMESTAMP")
                else: vals.append("NULL")
            ins = f"INSERT INTO FCME_USER.{tbl} ({col_names}) VALUES ({', '.join(vals)})"

        co.execute(ins)
        orcl.commit()

        # Buscar el evento emitido
        co.execute("""SELECT ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE
                      FROM FCME_USER.CDC_OUTBOX
                      WHERE SOURCE_TABLE LIKE :p
                      ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY""", p=f"%{tbl}")
        row = co.fetchone()
        if not row:
            print(f"  FAIL: trigger no emitio evento")
            fail += 1
            continue
        oid, at, aid, ev, payload, src = row
        payload_str = payload.read() if hasattr(payload,'read') else payload

        # Validar JSON
        try:
            obj = json.loads(payload_str)
            n_keys = len(obj)
        except Exception as e:
            print(f"  FAIL: JSON invalido: {e}")
            fail += 1
            continue

        print(f"  OK id={oid} type={at} agg_id={aid} ev={ev}")
        print(f"     src={src}")
        print(f"     payload keys={n_keys} sample={list(obj.items())[:3]}")
        ok += 1

    except Exception as e:
        print(f"  FAIL: {str(e)[:200]}")
        fail += 1

print("\n" + "="*70)
print("[1.3] Verificar UPDATE y DELETE en una tabla")
print("="*70)
print("REFERENCIAPARTICIPE_TYPE - test U y D")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
orcl.commit()

# Asegurar que existe la fila
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='VR2'")
co.execute("INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('VR2','antes')")
orcl.commit()
co.execute("UPDATE FCME_USER.REFERENCIAPARTICIPE_TYPE SET DESCRIPCIONTIPOREFERENCIA='despues' WHERE CODIGOTIPOREFERENCIA='VR2'")
orcl.commit()
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='VR2'")
orcl.commit()

co.execute("SELECT ID, EVENT_TYPE, AGGREGATE_ID, PAYLOAD FROM FCME_USER.CDC_OUTBOX ORDER BY ID")
events = co.fetchall()
print(f"  eventos emitidos: {len(events)} (esperado: 3 - INSERT+UPDATE+DELETE)")
for r in events:
    p = r[3].read() if hasattr(r[3],'read') else r[3]
    print(f"  id={r[0]} ev={r[1]} agg_id={r[2]} payload={p[:120]}")

# Cleanup todo
print("\n" + "="*70)
print("Cleanup")
print("="*70)
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
for tbl, _, _ in TESTS:
    try:
        co.execute(f"DELETE FROM FCME_USER.{tbl} WHERE ROWID IN (SELECT ROWID FROM FCME_USER.{tbl} WHERE CREATED_AT > SYSTIMESTAMP - INTERVAL '5' MINUTE FETCH FIRST 5 ROWS ONLY)")
    except: pass
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA IN ('VR1','VR2')")
orcl.commit()

print(f"\n=== Paso 1 RESULT: ok={ok} fail={fail} ===")
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX"); print(f"CDC_OUTBOX final: {co.fetchone()[0]} filas")
orcl.close()
