"""Test seguro de Flujo 1: Legacy -> Newcore (Oracle).

NO destructivo:
- NO borra cdc_outbox ni CDC_INBOX.
- Dispara UN solo UPDATE no-op (col = col) sobre UNA fila existente.
- Mide deltas (antes vs despues) en cada hop.
- El trigger legacy tiene anti-loop via SESSION_CONTEXT, asi que si Newcore
  reenvia hacia Legacy (Flujo 2), no se vuelve a publicar = sin bucle.
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
    leg = sql("dbIM").cursor()
    orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!",
                            dsn="10.35.3.223:31521/XEPDB1")
    co = orcl.cursor()
    print("[OK] Conexiones SQL Server (canonicos, dbIM) y Oracle XEPDB1 abiertas.")
except Exception as e:
    print(f"[FATAL] No se pudo conectar: {e}")
    sys.exit(1)

# Tabla y target del test
SRC_DB    = "dbIM"
SRC_TABLE = "imtbmiem_cony"          # conyuges
SRC_COL   = "no_apel"                # col segura para UPDATE no-op
TARGET    = "PERSONAVINCULACIONESTYPE"  # destino en FCME_USER
PK_COL    = "co_miem"                 # para localizar fila

# ---------- BASELINE ----------
banner("[BASELINE] Conteos antes del disparo")

# 1 outbox SQL canonicos
can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
out_before = can.fetchone()[0]
can.execute("SELECT ISNULL(MAX(id),0) FROM dbo.cdc_outbox")
out_max_before = can.fetchone()[0]
print(f"  cdc_outbox (canonicos): rows={out_before}  max_id={out_max_before}")

# 2 inbox Oracle
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
inb_before = co.fetchone()[0]
co.execute("SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_INBOX")
inb_max_before = co.fetchone()[0]
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=1")
proc_before = co.fetchone()[0]
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
err_before = co.fetchone()[0]
print(f"  CDC_INBOX (Oracle):    rows={inb_before}  max_id={inb_max_before}  processed={proc_before}  errors={err_before}")

# 3 destino
co.execute(f"SELECT COUNT(*) FROM FCME_USER.{TARGET}")
dst_before = co.fetchone()[0]
print(f"  {TARGET}: rows={dst_before}")

# fila objetivo
leg.execute(f"SELECT TOP 1 [{PK_COL}], [{SRC_COL}] FROM dbo.[{SRC_TABLE}] ORDER BY [{PK_COL}]")
row = leg.fetchone()
if not row:
    print(f"[ABORT] {SRC_DB}.{SRC_TABLE} vacia. No se puede testear sin insertar (evitamos DAÑAR).")
    sys.exit(2)
target_pk, target_col_val = row[0], row[1]
print(f"  Fila objetivo: {SRC_DB}.{SRC_TABLE}[{PK_COL}={target_pk}] {SRC_COL}={target_col_val!r}")

# ---------- TRIGGER ----------
banner("[DISPARO] UPDATE no-op (col = col) sobre 1 fila exacta")
leg.execute(
    f"UPDATE dbo.[{SRC_TABLE}] SET [{SRC_COL}] = [{SRC_COL}] "
    f"WHERE [{PK_COL}] = ?", target_pk)
print(f"  rowcount={leg.rowcount}")
trig_t0 = time.time()

# ---------- OBSERVAR PROPAGACION ----------
banner("[PROPAGACION] esperando hasta 30s")
final = None
for i in range(15):
    time.sleep(2)
    can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
    out_now = can.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
    inb_now = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE PROCESSED=1")
    pr_now = co.fetchone()[0]
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
    er_now = co.fetchone()[0]
    d_out = out_now - out_before
    d_inb = inb_now - inb_before
    d_pr  = pr_now  - proc_before
    d_er  = er_now  - err_before
    elapsed = round(time.time() - trig_t0, 1)
    print(f"  T+{elapsed:>4}s: dOutbox={d_out:+d}  dInbox={d_inb:+d}  dProcessed={d_pr:+d}  dErrors={d_er:+d}")
    if d_out >= 1 and d_inb >= 1 and d_pr >= d_inb:
        final = (d_out, d_inb, d_pr, d_er, elapsed)
        break

# ---------- DETALLE EVENTO ----------
banner("[EVENTO EN cdc_outbox]")
can.execute("""
    SELECT TOP 5 id, aggregate_type, source_table, event_type, created_at
    FROM dbo.cdc_outbox WHERE id > ? ORDER BY id DESC
""", out_max_before)
new_outbox_rows = can.fetchall()
for r in new_outbox_rows:
    print(f"  id={r.id}  type={r.aggregate_type}  src={r.source_table}  ev={r.event_type}  at={r.created_at}")
if not new_outbox_rows:
    print("  (ninguno) trigger NO publico — revisar ese eslabon")

banner("[EVENTO EN CDC_INBOX]")
co.execute("""
  SELECT * FROM (
    SELECT ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PROCESSED, CREATED_AT
    FROM FCME_USER.CDC_INBOX WHERE ID > :1 ORDER BY ID DESC
  ) WHERE ROWNUM <= 5
""", [inb_max_before])
new_inbox_rows = co.fetchall()
for r in new_inbox_rows:
    print(f"  id={r[0]}  type={r[1]}  agg={r[2]}  ev={r[3]}  processed={r[4]}  created={r[5]}")
if not new_inbox_rows:
    print("  (ninguno) Kafka NO entrego — revisar conector source/sink")

banner("[ERRORES NUEVOS EN CDC_INBOX_ERRORS]")
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
err_after = co.fetchone()[0]
new_err = err_after - err_before
print(f"  delta_errores = {new_err}")
if new_err > 0:
    co.execute("""
      SELECT * FROM (
        SELECT INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE
        FROM FCME_USER.CDC_INBOX_ERRORS ORDER BY INBOX_ID DESC
      ) WHERE ROWNUM <= 5
    """)
    for r in co.fetchall():
        print(f"  inbox_id={r[0]} type={r[1]} ev={r[2]} err={(r[3] or '')[:200]}")

# ---------- DESTINO ----------
banner("[DESTINO] Tabla FCME_USER")
co.execute(f"SELECT COUNT(*) FROM FCME_USER.{TARGET}")
dst_after = co.fetchone()[0]
print(f"  {TARGET}: antes={dst_before}  ahora={dst_after}  delta={dst_after - dst_before}")

# Buscar la fila por la PK que disparamos
co.execute(
    f"SELECT IDENTIFICACION, IDENTIFICACIONPERSONAVINCULADA, CODIGOTIPOVINCULACION "
    f"FROM FCME_USER.{TARGET} WHERE IDENTIFICACION = :1 AND CODIGOTIPOVINCULACION='CONYUGE'",
    [str(target_pk)])
match = co.fetchall()
print(f"  filas con IDENTIFICACION={target_pk!r} CODIGOTIPOVINCULACION=CONYUGE -> {len(match)}")
for r in match[:5]:
    print(f"    {r[0]} -> {r[1]} ({r[2]})")

# ---------- VEREDICTO ----------
banner("[VEREDICTO FLUJO 1]")
if final:
    d_out, d_inb, d_pr, d_er, elapsed = final
    print(f"  SI llego end-to-end en {elapsed}s.")
    print(f"  outbox+={d_out}  inbox+={d_inb}  processed+={d_pr}  errors+={d_er}")
    print(f"  destino delta={dst_after - dst_before}  match_id={len(match)}")
    if d_er == 0 and d_pr >= d_inb and len(match) > 0:
        print("  RESULTADO: OK — guardo en tabla destino, sin errores nuevos.")
    elif d_er == 0 and d_pr >= d_inb and len(match) == 0 and dst_after - dst_before == 0:
        print("  RESULTADO: PARCIAL — fluyo y se proceso, pero la fila no figura en el destino")
        print("  (posible: el SP hizo MERGE/UPDATE sin cambios, o la fila ya existia con esos valores).")
    else:
        print("  RESULTADO: REVISAR — ver detalle arriba.")
else:
    print("  NO completo el chain en 30s. Hops alcanzados:")
    can.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
    co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX")
    print(f"    outbox+={can.fetchone()[0] - out_before}")
    print(f"    inbox+={co.fetchone()[0]  - inb_before}")

orcl.close()
print("\n=== FIN TEST FLUJO 1 ===")
