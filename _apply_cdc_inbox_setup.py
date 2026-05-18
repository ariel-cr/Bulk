"""Activa el flujo CDC_INBOX en fcme_migration: poblar config + crear trigger + sink Kafka.
Idempotente: salta inserts existentes, DROP IF EXISTS sobre trigger, sobreescribe sink si existe."""
import pymssql
import json
import urllib.request
import sys

SRV = "10.35.3.64"
USER = "sa"
PWD = "YourPassword123"
CONNECT = "http://10.35.3.223:30083"
SINK_NAME = "newcore-canonicos-cdc-inbox-jdbc-sink"

def conn(db, autocommit=False):
    return pymssql.connect(server=SRV, port=1433, user=USER, password=PWD, database=db, autocommit=autocommit)

# ====================================================================
# PASO 1: Poblar FCME_USER.CDC_INBOX_MODULE_CONFIG
# ====================================================================
print("\n[1/3] Poblando FCME_USER.CDC_INBOX_MODULE_CONFIG")
with open('_cdc_inbox_module_config.json') as f:
    data = json.load(f)
mapping = data['matched']

c = conn("fcme_migration")
cu = c.cursor()
inserted = 0
skipped = 0
for at, sp in mapping:
    cu.execute("""
        IF NOT EXISTS (SELECT 1 FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE=%s)
            INSERT INTO FCME_USER.CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE) VALUES (%s, %s, 1)
    """, (at, at, sp))
    if cu.rowcount > 0:
        inserted += 1
    else:
        skipped += 1
c.commit()
cu.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE ACTIVE=1")
total = cu.fetchone()[0]
c.close()
print(f"  inserted={inserted} skipped_existing={skipped} total_active={total}")

# ====================================================================
# PASO 2: Crear trigger TRG_PROCESS_CDC_INBOX (firma 5 params)
# ====================================================================
print("\n[2/3] Creando trigger FCME_USER.TRG_PROCESS_CDC_INBOX")
trigger_sql = """
IF OBJECT_ID('FCME_USER.TRG_PROCESS_CDC_INBOX','TR') IS NOT NULL
    DROP TRIGGER FCME_USER.TRG_PROCESS_CDC_INBOX;
"""
# CREATE TRIGGER debe ir como batch separado
trigger_create = """
CREATE TRIGGER FCME_USER.TRG_PROCESS_CDC_INBOX
ON FCME_USER.CDC_INBOX
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @id              BIGINT,
            @aggregate_type  NVARCHAR(200),
            @event_type      NVARCHAR(200),
            @source_table    NVARCHAR(200),
            @payload         NVARCHAR(MAX),
            @sp_name         NVARCHAR(300),
            @exec_sql        NVARCHAR(MAX);

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT id, aggregate_type, event_type, source_table, payload
        FROM inserted;

    OPEN cur;
    FETCH NEXT FROM cur INTO @id, @aggregate_type, @event_type, @source_table, @payload;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sp_name = NULL;

        SELECT @sp_name = sp_name
        FROM FCME_USER.CDC_INBOX_MODULE_CONFIG
        WHERE aggregate_type = @aggregate_type AND active = 1;

        IF @sp_name IS NOT NULL
        BEGIN
            SAVE TRANSACTION before_sp;
            BEGIN TRY
                SET @exec_sql = N'
                    SET NOCOUNT ON;
                    EXEC sys.sp_set_session_context @key=N''is_replicating'', @value=N''1'';
                    EXEC ' + @sp_name + N' @p_id=@id, @p_aggregate_type=@agg, @p_source_table=@src, @p_event_type=@evt, @p_payload=@pay;
                    EXEC sys.sp_set_session_context @key=N''is_replicating'', @value=NULL;
                ';
                EXEC sp_executesql @exec_sql,
                    N'@id BIGINT, @agg NVARCHAR(200), @src NVARCHAR(200), @evt NVARCHAR(200), @pay NVARCHAR(MAX)',
                    @id, @aggregate_type, @source_table, @event_type, @payload;

                UPDATE FCME_USER.CDC_INBOX
                SET processed = 1, processed_at = SYSUTCDATETIME()
                WHERE id = @id;
            END TRY
            BEGIN CATCH
                BEGIN TRY EXEC sys.sp_set_session_context @key=N'is_replicating', @value=NULL; END TRY BEGIN CATCH END CATCH
                IF XACT_STATE() = 1 ROLLBACK TRANSACTION before_sp;
                BEGIN TRY
                    INSERT INTO FCME_USER.CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
                    VALUES (@id, @aggregate_type, @event_type, N'trg_process_cdc_inbox: ' + LEFT(ERROR_MESSAGE(), 3900));
                    UPDATE FCME_USER.CDC_INBOX SET processed = 1, processed_at = SYSUTCDATETIME() WHERE id = @id;
                END TRY BEGIN CATCH END CATCH
            END CATCH
        END

        FETCH NEXT FROM cur INTO @id, @aggregate_type, @event_type, @source_table, @payload;
    END
    CLOSE cur;
    DEALLOCATE cur;
END;
"""
c = conn("fcme_migration", autocommit=True)
cu = c.cursor()
cu.execute(trigger_sql)
cu.execute(trigger_create)
cu.execute("""SELECT name, is_disabled FROM sys.triggers WHERE parent_id=OBJECT_ID('FCME_USER.CDC_INBOX')""")
print(f"  trigger creado: {cu.fetchall()}")
c.close()

# ====================================================================
# PASO 3: Crear sink Kafka Connect (idempotente: usa PUT /config)
# ====================================================================
print(f"\n[3/3] Creando sink Kafka '{SINK_NAME}'")

sink_cfg = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "3",
    "topics": "convivencia.canonicos.cdc.outbox",
    "connection.url": "jdbc:sqlserver://10.35.3.64:1433;databaseName=fcme_migration;encrypt=false;trustServerCertificate=true",
    "connection.user": "sa",
    "connection.password": PWD,
    "dialect.name": "SqlServerDatabaseDialect",
    "table.name.format": "FCME_USER.CDC_INBOX",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",
    "fields.whitelist": "id,aggregate_id,aggregate_type,event_type,payload,source_table,created_at",
    "auto.create": "false",
    "auto.evolve": "false",
    "quote.sql.identifiers": "NEVER",
    "batch.size": "5000",
    "linger.ms": "200",
    "max.retries": "10",
    "retry.backoff.ms": "3000",
    "consumer.override.max.poll.records": "5000",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.topic.name": "convivencia.canonicos.cdc.outbox.sink.dlq",
}

# Verifica si existe
def http(method, path, body=None):
    req = urllib.request.Request(f"{CONNECT}{path}", method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
        body = json.dumps(body).encode()
    try:
        with urllib.request.urlopen(req, data=body, timeout=30) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

# Listar para ver si ya existe
code, txt = http("GET", "/connectors")
existing = json.loads(txt)
if SINK_NAME in existing:
    print(f"  sink ya existe -> hago PUT /config (sobreescribe)")
    code, txt = http("PUT", f"/connectors/{SINK_NAME}/config", sink_cfg)
else:
    print(f"  sink no existe -> POST /connectors (crear)")
    code, txt = http("POST", "/connectors", {"name": SINK_NAME, "config": sink_cfg})
print(f"  HTTP {code}")
if code >= 400:
    print(f"  ERROR: {txt[:1500]}")
    sys.exit(1)

# Estado final
import time
time.sleep(2)
code, txt = http("GET", f"/connectors/{SINK_NAME}/status")
print(f"  status: {txt[:600]}")
