"""Despliega SOLO:
1) usp_process_cdc_inbox (generico) - minimal: lee evento, anti-loop, marca processed=1
2) trg_process_cdc_inbox AFTER INSERT - itera inserted, llama el SP por fila

NO incluye module_config, wrappers ni CRUDs. Cuando esos se agreguen en pasos
posteriores, el SP se actualizara con la logica de dispatch.
"""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

c = sql("fcme_canonicos").cursor()

print("="*70)
print("[1] Deploy usp_process_cdc_inbox (minimal)")
print("="*70)
c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_process_cdc_inbox
    @inbox_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @aggregate_type NVARCHAR(200);

    -- Leer evento (verificar que existe)
    SELECT @aggregate_type = aggregate_type
    FROM dbo.cdc_inbox WITH (NOLOCK)
    WHERE id = @inbox_id;

    IF @aggregate_type IS NULL RETURN;

    -- Anti-loop: marcar la sesion como replicacion para que los triggers
    -- de outbox legacy (trg_outbox_*) hagan RETURN y no re-emitan
    EXEC sp_set_session_context N'is_replicating', 1;

    -- Marcar procesado (placeholder hasta que tengamos module_config + wrappers)
    UPDATE dbo.cdc_inbox
    SET processed = 1, processed_at = SYSUTCDATETIME()
    WHERE id = @inbox_id;

    -- Liberar flag
    EXEC sp_set_session_context N'is_replicating', 0;
END
""")
c.execute("""SELECT name, type_desc FROM sys.objects
             WHERE name='usp_process_cdc_inbox' AND type='P'""")
for r in c.fetchall(): print(f"  {r.name} {r.type_desc}")

print("\n" + "="*70)
print("[2] Deploy trg_process_cdc_inbox AFTER INSERT")
print("="*70)
c.execute("""
CREATE OR ALTER TRIGGER dbo.trg_process_cdc_inbox
ON dbo.cdc_inbox
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @id BIGINT;
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT id FROM inserted ORDER BY id;
    OPEN cur;
    FETCH NEXT FROM cur INTO @id;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            EXEC dbo.usp_process_cdc_inbox @inbox_id = @id;
        END TRY
        BEGIN CATCH
            -- salvaguardia: registrar en error_log si existiera, sino silencioso
            IF OBJECT_ID('dbo.cdc_inbox_errors') IS NOT NULL
                INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
                SELECT @id, aggregate_type, event_type, N'trigger CATCH: ' + ERROR_MESSAGE()
                FROM dbo.cdc_inbox WHERE id = @id;
        END CATCH
        FETCH NEXT FROM cur INTO @id;
    END
    CLOSE cur;
    DEALLOCATE cur;
END
""")
c.execute("""SELECT t.name, t.is_disabled FROM sys.triggers t
             WHERE t.parent_id=OBJECT_ID('dbo.cdc_inbox')""")
for r in c.fetchall(): print(f"  trigger: {r.name}  disabled={r.is_disabled}")

print("\n" + "="*70)
print("[3] Test minimal: INSERT manual en cdc_inbox")
print("="*70)
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload)
             VALUES ('TEST01','testType','INSERT','{}')""")
c.execute("SELECT id, aggregate_id, processed, processed_at FROM dbo.cdc_inbox WHERE aggregate_id='TEST01'")
for r in c.fetchall():
    print(f"  id={r.id} agg={r.aggregate_id} processed={r.processed} processed_at={r.processed_at}")

# Test multi-row (simula sink Kafka en lote)
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload) VALUES
             ('B01','testType','INSERT','{}'),
             ('B02','testType','UPDATE','{}'),
             ('B03','testType','DELETE','{}')""")
c.execute("SELECT id, aggregate_id, event_type, processed FROM dbo.cdc_inbox WHERE aggregate_id LIKE 'B0%' ORDER BY id")
print("  multi-row batch:")
for r in c.fetchall():
    print(f"    id={r.id} agg={r.aggregate_id} ev={r.event_type} processed={r.processed}")

# Verificar eventos llegados via Kafka tambien procesados
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=0")
pending = c.fetchone()[0]
print(f"\n  total pendientes (debe ser 0): {pending}")

# Cleanup test
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
print(f"  cdc_inbox final: {c.fetchone()[0]} filas")

print("\n=== Paso OK: trigger + dispatcher minimal desplegados ===")
