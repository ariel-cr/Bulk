"""Recrea el bridge haciendo INSERT idempotente + setea checkpoint al max(outbox.id)
para arrancar desde cero sin re-enviar histórico."""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

cl = conn("fcme_legacy").cursor()

# 1) Reset del checkpoint al max id actual del outbox (para no re-enviar histórico)
cl.execute("SELECT ISNULL(MAX(id), 0) FROM dbo.cdc_outbox")
max_out = cl.fetchone()[0]
cl.execute("""
  UPDATE dbo.cdc_outbox_bridge_state
  SET last_sent_id = ?, last_run_at = SYSUTCDATETIME()
  WHERE channel = 'LEGACY_TO_NEWCORE'
""", max_out)
print(f"checkpoint seteado a id={max_out} (cdc_outbox max)")

# 2) Recrear SP con NOT EXISTS (idempotente) - por si la fila ya existe en newcore
cl.execute("IF OBJECT_ID('dbo.usp_cdc_bridge_to_newcore','P') IS NOT NULL DROP PROCEDURE dbo.usp_cdc_bridge_to_newcore")
cl.execute("""
CREATE PROCEDURE dbo.usp_cdc_bridge_to_newcore
    @max_rows INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @last_id BIGINT;
    SELECT @last_id = last_sent_id FROM dbo.cdc_outbox_bridge_state WHERE channel='LEGACY_TO_NEWCORE';
    IF @last_id IS NULL SET @last_id = 0;

    DECLARE @copied INT = 0;
    DECLARE @new_max BIGINT = @last_id;

    ;WITH nuevos AS (
        SELECT TOP (@max_rows)
            o.id, o.aggregate_id, o.aggregate_type, o.event_type, o.payload, o.source_table, o.created_at
        FROM dbo.cdc_outbox o
        WHERE o.id > @last_id
          AND NOT EXISTS (SELECT 1 FROM fcme_newcore.dbo.cdc_inbox i WHERE i.id = o.id)
        ORDER BY o.id
    )
    INSERT INTO fcme_newcore.dbo.cdc_inbox
        (id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, processed, processed_at)
    SELECT id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at, 0, NULL
    FROM nuevos;

    SET @copied = @@ROWCOUNT;

    SELECT @new_max = ISNULL(MAX(id), @last_id) FROM dbo.cdc_outbox WHERE id > @last_id;

    UPDATE dbo.cdc_outbox_bridge_state
    SET last_sent_id = @new_max,
        events_total = events_total + @copied,
        last_run_at  = SYSUTCDATETIME()
    WHERE channel = 'LEGACY_TO_NEWCORE';

    SELECT @copied AS copied, @last_id AS prev_last_id, @new_max AS new_last_id;
END
""")
print("SP recreado con NOT EXISTS (idempotente)")
