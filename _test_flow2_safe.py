"""Test seguro de Flujo 2: Newcore (Oracle) -> Legacy (SQL Server).

NO destructivo:
- NO borra cdc_inbox ni CDC_OUTBOX.
- Dispara UN solo UPDATE no-op (col = col) sobre UNA fila existente en
  FCME_USER.INSTITUCION_TYPE.
- Trigger Oracle TRG_OUTBOX_INSTITUCION_TYPE tiene anti-loop por
  SYS_CONTEXT('USERENV','CLIENT_INFO')='is_replicating' => no bucle.
- Mide deltas en cada hop:
    FCME_USER.CDC_OUTBOX -> Kafka -> fcme_canonicos.cdc_inbox
    -> trg_process_cdc_inbox -> usp_process_cdc_inbox
    -> module_config -> usp_inbox_institucionType
    -> dbFC.sp_INSTITUCION_TYPE_CRUD -> dbFC.sfct_institucion
"""
import pyodbc, oracledb, time, sys

DB = {"server": "10.35.3.64,1433", "driver": "{SQL Server}",
      "username": "sa", "password": "YourPassword123"}

def sql(db):
    s = (f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};"
         f"UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

def banner(t):
    print("\n" + "=" * 70)
    print(t)
    print("=" * 70)

# Conexiones
try:
    can = sql("fcme_canonicos").cursor()
    fc  = sql("dbFC").cursor()
    orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!",
                            dsn="10.35.3.223:31521/XEPDB1")
    co = orcl.cursor()
    print("[OK] Conexiones SQL Server (canonicos, dbFC) y Oracle XEPDB1 abiertas.")
except Exception as e:
    print(f"[FATAL] {e}")
    sys.exit(1)

SRC_TABLE = "INSTITUCION_TYPE"
SRC_COL   = "NOMBREINSTITUCION"
PK_COL    = "ID"
AGG_TYPE  = "institucionType"
LEG_TBL   = "sfct_institucion"   # tabla legacy destino en dbFC

# ---------- BASELINE ----------
banner("[BASELINE] Conteos antes del disparo")

co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX")
out_before = co.fetchone()[0]
co.execute("SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX")
out_max_before = co.fetchone()[0]
print(f"  FCME_USER.CDC_OUTBOX:        rows={out_before}  max_id={out_max_before}")

can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
inb_before = can.fetchone()[0]
can.execute("SELECT ISNULL(MAX(id),0) FROM dbo.cdc_inbox")
inb_max_before = can.fetchone()[0]
can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=1")
proc_before = can.fetchone()[0]
print(f"  fcme_canonicos.cdc_inbox:    rows={inb_before}  max_id={inb_max_before}  processed={proc_before}")

# Tabla de errores (si existe)
try:
    can.execute("""
      SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
      WHERE t.name='cdc_inbox_errors'
    """)
    has_err_tbl = can.fetchone()[0] > 0
except Exception:
    has_err_tbl = False
err_before = 0
if has_err_tbl:
    can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
    err_before = can.fetchone()[0]
print(f"  fcme_canonicos.cdc_inbox_errors: existe={has_err_tbl}  rows={err_before}")

fc.execute(f"SELECT COUNT(*) FROM dbo.[{LEG_TBL}]")
leg_before = fc.fetchone()[0]
print(f"  dbFC.{LEG_TBL}:        rows={leg_before}")

# Fila objetivo en INSTITUCION_TYPE
co.execute(f"SELECT {PK_COL}, {SRC_COL} FROM FCME_USER.{SRC_TABLE} WHERE ROWNUM<=1 ORDER BY {PK_COL}")
row = co.fetchone()
if not row:
    print(f"[ABORT] FCME_USER.{SRC_TABLE} vacia.")
    sys.exit(2)
target_pk, target_val = row[0], row[1]
print(f"  Fila objetivo: FCME_USER.{SRC_TABLE}[{PK_COL}={target_pk}]")

# ---------- DISPARO ----------
banner("[DISPARO] UPDATE no-op (col = col) sobre 1 fila exacta")
co.execute(
    f"UPDATE FCME_USER.{SRC_TABLE} SET {SRC_COL} = {SRC_COL} "
    f"WHERE {PK_COL} = :1", [target_pk])
orcl.commit()
print(f"  rowcount={co.rowcount}")
trig_t0 = time.time()

# ---------- PROPAGACION ----------
banner("[PROPAGACION] esperando hasta 30s")
final = None
for i in range(15):
    time.sleep(2)
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX")
    out_now = co.fetchone()[0]
    can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
    inb_now = can.fetchone()[0]
    can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=1")
    pr_now = can.fetchone()[0]
    er_now = 0
    if has_err_tbl:
        can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
        er_now = can.fetchone()[0]
    d_out = out_now - out_before
    d_inb = inb_now - inb_before
    d_pr  = pr_now  - proc_before
    d_er  = er_now  - err_before
    elapsed = round(time.time() - trig_t0, 1)
    print(f"  T+{elapsed:>4}s: dOutbox={d_out:+d}  dInbox={d_inb:+d}  dProcessed={d_pr:+d}  dErrors={d_er:+d}")
    if d_out >= 1 and d_inb >= 1 and d_pr >= d_inb:
        final = (d_out, d_inb, d_pr, d_er, elapsed)
        break

# ---------- DETALLE ----------
banner("[EVENTO EN FCME_USER.CDC_OUTBOX]")
co.execute("""
  SELECT * FROM (
    SELECT ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, SOURCE_TABLE, CREATED_AT
    FROM FCME_USER.CDC_OUTBOX WHERE ID > :1 ORDER BY ID DESC
  ) WHERE ROWNUM <= 5
""", [out_max_before])
new_outbox = co.fetchall()
for r in new_outbox:
    print(f"  id={r[0]}  type={r[1]}  agg={r[2]}  ev={r[3]}  src={r[4]}  at={r[5]}")
if not new_outbox:
    print("  (ninguno) trigger Oracle NO publico")

banner("[EVENTO EN fcme_canonicos.cdc_inbox]")
can.execute("""
  SELECT TOP 5 id, aggregate_type, aggregate_id, event_type, source_table, processed, created_at
  FROM dbo.cdc_inbox WHERE id > ? ORDER BY id DESC
""", inb_max_before)
new_inbox = can.fetchall()
for r in new_inbox:
    print(f"  id={r.id}  type={r.aggregate_type}  agg={r.aggregate_id}  ev={r.event_type}  src={r.source_table}  proc={r.processed}  at={r.created_at}")
if not new_inbox:
    print("  (ninguno) Kafka NO entrego")

if has_err_tbl:
    banner("[ERRORES NUEVOS EN cdc_inbox_errors]")
    can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
    err_after = can.fetchone()[0]
    print(f"  delta_errores = {err_after - err_before}")
    if err_after > err_before:
        can.execute("""SELECT TOP 5 inbox_id, aggregate_type, event_type, error_message
                       FROM dbo.cdc_inbox_errors ORDER BY inbox_id DESC""")
        for r in can.fetchall():
            print(f"  inbox_id={r.inbox_id}  type={r.aggregate_type}  ev={r.event_type}  err={(r.error_message or '')[:200]}")

# ---------- DESTINO LEGACY ----------
banner(f"[DESTINO LEGACY] dbFC.{LEG_TBL}")
fc.execute(f"SELECT COUNT(*) FROM dbo.[{LEG_TBL}]")
leg_after = fc.fetchone()[0]
print(f"  rows: antes={leg_before}  ahora={leg_after}  delta={leg_after - leg_before}")

# ---------- ANTI-LOOP CHECK ----------
banner("[ANTI-LOOP] Verificacion de no-bucle")
# Tras unos segundos extra, ¿aparecen MAS eventos en outbox Oracle?
time.sleep(3)
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX")
out_late = co.fetchone()[0]
print(f"  CDC_OUTBOX 3s mas tarde: rows={out_late}  delta_total={out_late - out_before}")
print("  (debe coincidir con el delta inicial; mas alto => bucle)")

# ---------- VEREDICTO ----------
banner("[VEREDICTO FLUJO 2]")
if final:
    d_out, d_inb, d_pr, d_er, elapsed = final
    print(f"  Llego end-to-end en {elapsed}s.")
    print(f"  outbox+={d_out}  inbox+={d_inb}  processed+={d_pr}  errors+={d_er}")
    print(f"  legacy delta={leg_after - leg_before}")
    if d_er == 0 and d_pr >= d_inb:
        if (out_late - out_before) == d_out:
            print("  RESULTADO: OK - end-to-end limpio, sin bucle.")
        else:
            print("  RESULTADO: REVISAR - posible bucle (outbox sigue creciendo).")
    else:
        print("  RESULTADO: REVISAR errores arriba.")
else:
    print("  NO completo el chain en 30s.")

orcl.close()
print("\n=== FIN TEST FLUJO 2 ===")
