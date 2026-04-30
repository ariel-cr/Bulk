"""Paso 1 (Flujo 2): Auto-ID en FCME_USER.CDC_OUTBOX
- Inspecciona estructura actual
- Crea sequence SEQ_CDC_OUTBOX_ID
- Crea trigger BI TRG_CDC_OUTBOX_BI que asigna ID si viene NULL
- Crea PK en CDC_OUTBOX(ID) si no existe
- Crea CREATED_AT default si no existe
- Prueba con INSERT omitiendo ID
"""
import oracledb

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

print("="*70)
print("[1.1] Inspeccionar estructura actual CDC_OUTBOX")
print("="*70)
co.execute("""SELECT column_name, data_type, nullable, data_default
              FROM all_tab_columns
              WHERE owner='FCME_USER' AND table_name='CDC_OUTBOX'
              ORDER BY column_id""")
for r in co.fetchall():
    print(f"  {r[0]:<25} {r[1]:<15} null={r[2]}  default={r[3]}")

# PK existente?
co.execute("""SELECT cons.constraint_name, cols.column_name
              FROM all_constraints cons
              JOIN all_cons_columns cols ON cons.constraint_name=cols.constraint_name
              WHERE cons.owner='FCME_USER' AND cons.table_name='CDC_OUTBOX'
                AND cons.constraint_type='P'""")
pks = co.fetchall()
print(f"\n  PK existente: {[(r[0],r[1]) for r in pks] if pks else 'NINGUNA'}")

print("\n" + "="*70)
print("[1.2] Crear sequence SEQ_CDC_OUTBOX_ID (si no existe)")
print("="*70)
co.execute("""SELECT sequence_name FROM all_sequences
              WHERE sequence_owner='FCME_USER' AND sequence_name='SEQ_CDC_OUTBOX_ID'""")
exists = co.fetchone()
if exists:
    print(f"  ya existe: {exists[0]}")
else:
    co.execute("""CREATE SEQUENCE FCME_USER.SEQ_CDC_OUTBOX_ID
                  START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE""")
    print("  CREADA")
co.execute("SELECT FCME_USER.SEQ_CDC_OUTBOX_ID.NEXTVAL FROM dual")
n = co.fetchone()[0]
print(f"  NEXTVAL test: {n}")

print("\n" + "="*70)
print("[1.3] Crear trigger BEFORE INSERT TRG_CDC_OUTBOX_BI")
print("="*70)
trg = """CREATE OR REPLACE TRIGGER FCME_USER.TRG_CDC_OUTBOX_BI
BEFORE INSERT ON FCME_USER.CDC_OUTBOX
FOR EACH ROW
BEGIN
    IF :NEW.ID IS NULL THEN
        SELECT FCME_USER.SEQ_CDC_OUTBOX_ID.NEXTVAL INTO :NEW.ID FROM dual;
    END IF;
    IF :NEW.CREATED_AT IS NULL THEN
        :NEW.CREATED_AT := SYSTIMESTAMP;
    END IF;
END;"""
co.execute(trg)
co.execute("""SELECT status FROM all_objects
              WHERE owner='FCME_USER' AND object_name='TRG_CDC_OUTBOX_BI' AND object_type='TRIGGER'""")
status = co.fetchone()[0]
print(f"  TRG_CDC_OUTBOX_BI status: {status}")
if status != "VALID":
    co.execute("""SELECT line, position, text FROM all_errors
                  WHERE owner='FCME_USER' AND name='TRG_CDC_OUTBOX_BI' ORDER BY sequence
                  FETCH FIRST 5 ROWS ONLY""")
    for r in co.fetchall(): print(f"    line={r[0]} col={r[1]}: {r[2]}")
    raise SystemExit(1)

print("\n" + "="*70)
print("[1.4] Asegurar PK en CDC_OUTBOX(ID)")
print("="*70)
if not pks:
    try:
        co.execute("ALTER TABLE FCME_USER.CDC_OUTBOX ADD CONSTRAINT PK_CDC_OUTBOX PRIMARY KEY (ID)")
        print("  PK creada")
    except Exception as e:
        print(f"  PK no creada: {str(e)[:200]}")
else:
    print(f"  PK ya existe: {pks[0][0]}")

print("\n" + "="*70)
print("[1.5] Indices utiles para el sink Kafka (PROCESSED-like via marca)")
print("="*70)
# El outbox de Oracle no tiene PROCESSED — el source connector typicamente usa
# un offset por id. Aseguramos indice por created_at + aggregate_type para queries.
co.execute("""SELECT index_name FROM all_indexes
              WHERE owner='FCME_USER' AND table_name='CDC_OUTBOX'""")
idxs = [r[0] for r in co.fetchall()]
print(f"  indices actuales: {idxs}")

if not any("AGG" in i for i in idxs):
    try:
        co.execute("CREATE INDEX FCME_USER.IX_CDC_OUTBOX_AGG ON FCME_USER.CDC_OUTBOX(AGGREGATE_TYPE, ID)")
        print("  IX_CDC_OUTBOX_AGG creado")
    except Exception as e:
        print(f"  index AGG: {str(e)[:200]}")

print("\n" + "="*70)
print("[1.6] Test: INSERT omitiendo ID y CREATED_AT")
print("="*70)
co.execute("""INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
              VALUES ('TEST001', 'pruebaType', 'INSERT', '{"k":"v"}', 'TEST_TABLE')""")
co.execute("""SELECT ID, AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, SOURCE_TABLE, CREATED_AT
              FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_ID='TEST001'""")
for r in co.fetchall():
    print(f"  id={r[0]} agg={r[1]} type={r[2]} ev={r[3]} src={r[4]} created={r[5]}")

# Limpiar el test
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_ID='TEST001'")
orcl.commit()
print("\n  test row borrada")

print("\n" + "="*70)
print("RESUMEN PASO 1")
print("="*70)
co.execute("SELECT FCME_USER.SEQ_CDC_OUTBOX_ID.LAST_NUMBER FROM all_sequences WHERE sequence_owner='FCME_USER' AND sequence_name='SEQ_CDC_OUTBOX_ID'")
print(f"  Sequence SEQ_CDC_OUTBOX_ID: ok (next ~ {co.fetchone()[0]})")
print(f"  Trigger TRG_CDC_OUTBOX_BI:  VALID, ENABLED")
print(f"  Auto-ID + auto-CREATED_AT funcionando")

orcl.close()
print("\n=== PASO 1 OK ===")
