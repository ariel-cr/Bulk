"""Diagnostico: tablas PARTICIPE vacias a pesar de 9140 eventos processed=1."""
import pyodbc, json
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

cn = conn("fcme_newcore").cursor()

# 1) Conteo de tablas PARTICIPE
print("== [1] Conteo tablas PARTICIPE ==")
for tbl in ['PERSONATYPE','DIRECCIONTYPE','TELEFONOTYPE','INFOBASICATYPE','INFOADICIONALTYPE',
            'INGRESOSEGRESOSTYPE','PERSONAVINCULADATYPE','SALDOPERSONANATURALTYPE','HISTORICOESTADOSTYPE']:
    try:
        cn.execute(f"SELECT COUNT(*) FROM PARTICIPE.{tbl}")
        print(f"  PARTICIPE.{tbl:<30} {cn.fetchone()[0]} filas")
    except Exception as e:
        print(f"  PARTICIPE.{tbl}: ERROR {e}")

# 2) Errores en cdc_inbox_errors
print("\n== [2] cdc_inbox_errors (ultimos 10) ==")
cn.execute("""
  SELECT TOP 10 id, inbox_id, aggregate_type, event_type, error_message, error_date
  FROM dbo.cdc_inbox_errors ORDER BY id DESC
""")
errs = cn.fetchall()
print(f"  total mostrados: {len(errs)}")
for r in errs:
    print(f"  err_id={r.id}  inbox={r.inbox_id}  type={r.aggregate_type}  date={r.error_date}")
    print(f"    {str(r.error_message)[:300]}")

cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
print(f"  TOTAL cdc_inbox_errors: {cn.fetchone()[0]}")

# 3) Sample payload procesado (personaType)
print("\n== [3] Sample inbox personaType procesado ==")
cn.execute("""
  SELECT TOP 3 id, aggregate_type, event_type, payload
  FROM dbo.cdc_inbox
  WHERE aggregate_type='personaType' AND processed=1
  ORDER BY id DESC
""")
for r in cn.fetchall():
    print(f"  id={r.id} type={r.aggregate_type} event={r.event_type}")
    print(f"  payload: {r.payload[:500]}")
    print()

# 4) Ejecutar manualmente el SP con uno de esos payloads
print("\n== [4] Ejecutar usp_inbox_PARTICIPE manualmente con sample ==")
cn.execute("""
  SELECT TOP 1 aggregate_type, event_type, payload
  FROM dbo.cdc_inbox
  WHERE aggregate_type='personaType' AND processed=1
  ORDER BY id DESC
""")
r = cn.fetchone()
if r:
    try:
        # contar antes
        cn.execute("SELECT COUNT(*) FROM PARTICIPE.PERSONATYPE")
        before = cn.fetchone()[0]
        cn.execute("EXEC sp_set_session_context N'is_replicating', 1, @read_only=0")
        cn.execute("EXEC dbo.usp_inbox_PARTICIPE @aggregate_type=?, @event_type=?, @payload=?",
                   r.aggregate_type, r.event_type, r.payload)
        cn.execute("EXEC sp_set_session_context N'is_replicating', 0, @read_only=0")
        cn.execute("SELECT COUNT(*) FROM PARTICIPE.PERSONATYPE")
        after = cn.fetchone()[0]
        print(f"  personaType: {before} -> {after}  delta={after-before}")
    except Exception as e:
        print(f"  ERROR ejecutando SP: {e}")

# 5) Ver evento processed que corresponde, su created_at vs processed_at
print("\n== [5] Timing de eventos personaType ==")
cn.execute("""
  SELECT TOP 5 id, created_at, processed_at,
         DATEDIFF(ms, created_at, processed_at) AS ms
  FROM dbo.cdc_inbox WHERE aggregate_type='personaType' AND processed=1
  ORDER BY id DESC
""")
for r in cn.fetchall():
    print(f"  id={r.id}  created={r.created_at}  processed={r.processed_at}  delta_ms={r.ms}")

# 6) Ver PKs / unique constraints de PARTICIPE.PERSONATYPE
print("\n== [6] PARTICIPE.PERSONATYPE estructura rapida ==")
cn.execute("""
  SELECT c.name, t.name tp, c.is_nullable
  FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
  WHERE c.object_id=OBJECT_ID('PARTICIPE.PERSONATYPE') ORDER BY c.column_id
""")
for r in cn.fetchall():
    print(f"  {r.name:<30} {r.tp}")
