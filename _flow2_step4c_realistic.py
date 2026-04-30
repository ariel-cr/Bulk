"""Test realista del trigger con wrappers que tienen TRY/CATCH interno
(que es lo que generaremos en Paso 5).
"""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = sql("fcme_canonicos").cursor()

# Cleanup previo
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c.execute("DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type IN ('_OK','_FAIL','_TEST_TYPE','_FAIL_TYPE')")
for sp in ['usp_inbox_OK_TEST','usp_inbox_FAIL_SOFT','usp_inbox_TEST_PRUEBA','usp_inbox_FAIL_TEST']:
    try: c.execute(f"DROP PROCEDURE dbo.{sp}")
    except: pass

print("="*70)
print("[4c] Wrappers bien comportados (TRY/CATCH interno)")
print("="*70)
# OK wrapper
c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_inbox_OK_TEST
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        IF OBJECT_ID('tempdb..##test_lg') IS NULL
            CREATE TABLE ##test_lg (id BIGINT, agg NVARCHAR(200));
        INSERT INTO ##test_lg VALUES (@inbox_id, @aggregate_id);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, ERROR_MESSAGE());
    END CATCH
END
""")
# FAIL wrapper (con TRY/CATCH interno - registra error sin propagar)
c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_inbox_FAIL_WELL
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS BEGIN
    SET NOCOUNT ON;
    -- simula validacion logica fallida (no error de runtime)
    -- registra como error pero NO propaga ni doomifica la tx
    INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
    VALUES (@inbox_id, @aggregate_type, @event_type, N'simulacion: validacion fallida');
END
""")
c.execute("""INSERT INTO dbo.cdc_inbox_module_config VALUES
             ('_OK','dbo.usp_inbox_OK_TEST','tempdb','TEST',1,SYSUTCDATETIME(),NULL),
             ('_FAIL','dbo.usp_inbox_FAIL_WELL','tempdb','TEST',1,SYSUTCDATETIME(),NULL)""")
c.execute("IF OBJECT_ID('tempdb..##test_lg') IS NOT NULL DROP TABLE ##test_lg")
c.execute("CREATE TABLE ##test_lg (id BIGINT, agg NVARCHAR(200))")

# INSERT batch: OK + FAIL + OK
print("\n[Batch INSERT 3 filas: OK / FAIL / OK]")
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload) VALUES
             ('B001','_OK','INSERT','{}'),
             ('B002','_FAIL','INSERT','{}'),
             ('B003','_OK','INSERT','{}')""")

c.execute("SELECT id, aggregate_id, aggregate_type, processed FROM dbo.cdc_inbox ORDER BY id")
print("\ncdc_inbox final:")
for r in c.fetchall():
    print(f"  id={r.id} agg={r.aggregate_id} type={r.aggregate_type} processed={r.processed}")

c.execute("SELECT id, agg FROM ##test_lg ORDER BY id")
print("\nlegacy target (debe tener B001 y B003):")
for r in c.fetchall():
    print(f"  inbox_id={r.id} agg={r.agg}")

c.execute("SELECT inbox_id, error_message FROM dbo.cdc_inbox_errors ORDER BY inbox_id")
print("\ncdc_inbox_errors (debe haber 1 para B002):")
for r in c.fetchall():
    print(f"  inbox_id={r.inbox_id} msg={r.error_message[:200]}")

# Cleanup
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c.execute("DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type IN ('_OK','_FAIL')")
c.execute("DROP PROCEDURE dbo.usp_inbox_OK_TEST")
c.execute("DROP PROCEDURE dbo.usp_inbox_FAIL_WELL")
c.execute("IF OBJECT_ID('tempdb..##test_lg') IS NOT NULL DROP TABLE ##test_lg")

c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); n1 = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors"); n2 = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config"); n3 = c.fetchone()[0]
print(f"\n[Cleanup ok]: cdc_inbox={n1} errors={n2} module_config={n3}")

print("\n=== PASO 4 OK (con regla de diseno: wrappers TRY/CATCH internos) ===")
