"""Diagnostico paso a paso del fallo en grupoFamiliarType."""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
c = sql("fcme_canonicos").cursor()
c_fc = sql("dbFC").cursor()

# Cleanup
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
orcl.commit()

print("="*70)
print("[1] Estructura de GRUPOFAMILIAR_TYPE (Oracle) y sfct_grupo_fami (legacy)")
print("="*70)
co.execute("""SELECT column_name, data_type, nullable FROM all_tab_columns
              WHERE owner='FCME_USER' AND table_name='GRUPOFAMILIAR_TYPE' ORDER BY column_id""")
print("Oracle GRUPOFAMILIAR_TYPE:")
for r in co.fetchall(): print(f"  {r[0]:<35} {r[1]:<15} null={r[2]}")

c_fc.execute("""SELECT c.name, t.name tp, c.max_length, c.is_nullable, c.is_identity
                FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
                WHERE c.object_id=OBJECT_ID('dbo.sfct_grupo_fami') ORDER BY c.column_id""")
print("\nLegacy sfct_grupo_fami:")
for r in c_fc.fetchall(): print(f"  {r.name:<25} {r.tp:<14} max={r.max_length} null={r.is_nullable} identity={r.is_identity}")

print("\n" + "="*70)
print("[2] Definicion del CRUD generado (sp_GRUPOFAMILIAR_TYPE_CRUD en dbFC)")
print("="*70)
c_fc.execute("""SELECT m.definition FROM sys.sql_modules m
                JOIN sys.objects o ON m.object_id=o.object_id
                WHERE o.name='sp_GRUPOFAMILIAR_TYPE_CRUD'""")
row = c_fc.fetchone()
if row:
    print(row[0][:3000])
else:
    print("NO EXISTE")

print("\n" + "="*70)
print("[3] Definicion del wrapper (usp_inbox_grupoFamiliarType)")
print("="*70)
c.execute("""SELECT m.definition FROM sys.sql_modules m
             JOIN sys.objects o ON m.object_id=o.object_id
             WHERE o.name='usp_inbox_grupoFamiliarType'""")
row = c.fetchone()
if row:
    print(row[0][:2500])

print("\n" + "="*70)
print("[4] Reproducir: INSERT Oracle, leer payload, intentar dispatcher MANUAL")
print("="*70)
# 1) Insert en Oracle TYPE
co.execute("DELETE FROM FCME_USER.GRUPOFAMILIAR_TYPE WHERE CEDULAFAMILIAR='9999998888'")
try:
    co.execute("""INSERT INTO FCME_USER.GRUPOFAMILIAR_TYPE
                  (CODIGOTIPOIDENTIFICACION, IDENTIFICACION, SECUENCIAPERSONA, CEDULAFAMILIAR, NOMBRESGRUPOFAMILIAR, APELLIDOSGRUPOFAMILIAR)
                  VALUES ('C','0915221477','1','9999998888','JUAN','PEREZ')""")
    orcl.commit()
    print("  Oracle INSERT OK")
except Exception as e:
    # No tenemos la lista exacta de cols, intento descubrir
    print(f"  Oracle INSERT fail: {e}")
    # Reintento sin cols extra
    co.execute("""INSERT INTO FCME_USER.GRUPOFAMILIAR_TYPE
                  (CEDULAFAMILIAR, NOMBRESGRUPOFAMILIAR, APELLIDOSGRUPOFAMILIAR)
                  VALUES ('9999998888','JUAN','PEREZ')""")
    orcl.commit()
    print("  Oracle INSERT (reintentado) OK")

# 2) Leer el payload generado
co.execute("""SELECT ID, AGGREGATE_ID, EVENT_TYPE, PAYLOAD FROM FCME_USER.CDC_OUTBOX
              ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY""")
r = co.fetchone()
ora_id, agg_id, ev, payload = r
payload_str = payload.read() if hasattr(payload,'read') else payload
print(f"\n  outbox: agg_id={agg_id} ev={ev}")
print(f"  payload={payload_str}")

# 3) Llamar el wrapper DIRECTAMENTE para capturar el error real
print("\n[5] Llamando wrapper DIRECTAMENTE (sin trigger, sin dispatcher)")
try:
    c.execute("""EXEC dbo.usp_inbox_grupoFamiliarType
                 @inbox_id=999, @aggregate_id=?, @aggregate_type='grupoFamiliarType',
                 @source_table='FCME_USER.GRUPOFAMILIAR_TYPE', @event_type='INSERT', @payload=?""",
              str(agg_id), payload_str)
    print("  EXEC wrapper OK")
    c.execute("SELECT inbox_id, error_message FROM dbo.cdc_inbox_errors WHERE inbox_id=999")
    for er in c.fetchall():
        print(f"  ERROR LOGGED en wrapper: {er.error_message[:500]}")
    c_fc.execute("SELECT TOP 3 * FROM dbo.sfct_grupo_fami WHERE co_fami=9999998888")
    rows = c_fc.fetchall()
    if rows:
        cols = [d[0] for d in c_fc.description]
        for rr in rows: print(f"  legacy row: {dict(zip(cols, rr))}")
    else:
        print("  legacy row no encontrada (PK puede no ser co_fami)")
except Exception as e:
    print(f"  EXC: {str(e)[:600]}")

# Cleanup
co.execute("DELETE FROM FCME_USER.GRUPOFAMILIAR_TYPE WHERE CEDULAFAMILIAR='9999998888'")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX")
orcl.commit()
c.execute("DELETE FROM dbo.cdc_inbox_errors WHERE inbox_id=999")
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 1")
c_fc.execute("DELETE FROM dbo.sfct_grupo_fami WHERE co_fami=9999998888")
c_fc.execute("EXEC sp_set_session_context N'is_replicating', 0")
orcl.close()
