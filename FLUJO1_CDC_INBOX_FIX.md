# Flujo 1 (Legacy → Migración) — Activación del path CDC_INBOX

**Fecha:** 2026-05-18
**Objetivo:** Hacer que los eventos publicados en `fcme_canonicos.cdc_outbox` lleguen a `fcme_migration.FCME_USER.CDC_INBOX`, se procesen vía dispatcher y aterricen en las tablas TYPE correctas.

---

## 1. Diagrama del flujo

```
Tabla legacy (fcme_legacy.dbo.<tabla>)
  └─ trg_outbox_<tabla>  (anti-loop: SESSION_CONTEXT('is_replicating'))
       └─ fcme_canonicos.dbo.cdc_outbox
            └─ canonicos-convivencia-cdc-outbox-jdbc-source   [Kafka source, RUNNING]
                 └─ topic: convivencia.canonicos.cdc.outbox
                      └─ newcore-canonicos-cdc-inbox-jdbc-sink    [NUEVO, RUNNING]
                           └─ fcme_migration.FCME_USER.CDC_INBOX
                                └─ TRG_PROCESS_CDC_INBOX            [NUEVO]
                                     └─ CDC_INBOX_MODULE_CONFIG     [POBLADO 97 filas]
                                          └─ FCME_USER.USP_INBOX_<X>  (154 SPs ya existentes)
                                               └─ FCME_USER.<TYPE_TABLE>
```

---

## 2. Diagnóstico — qué estaba roto

| Eslabón | Estado encontrado |
|---|---|
| Trigger legacy → outbox | ✅ funcionaba (2 filas `bancoTesoreria_type` insertadas en outbox) |
| Source Kafka canonicos | ✅ RUNNING, había publicado al topic (offset=2) |
| Topic `convivencia.canonicos.cdc.outbox` | ✅ tenía los 2 mensajes |
| **Sink Kafka → CDC_INBOX** | ❌ **NO EXISTÍA** |
| `FCME_USER.CDC_INBOX` | vacío (0 filas) |
| `FCME_USER.CDC_INBOX_MODULE_CONFIG` | vacío (0 filas) |
| `FCME_USER.TRG_PROCESS_CDC_INBOX` | NO EXISTÍA |
| `FCME_USER.USP_INBOX_*` | ✅ 154 SPs ya desplegados, firma uniforme |

El catálogo de referencia en `fcme_newcore.dbo.cdc_inbox_module_config` apuntaba a SPs **por módulo** (`usp_inbox_COBRANZAS`, etc.), pero `fcme_migration` usa SPs **por tipo** (`USP_INBOX_BANCOTESORERIA`, `USP_INBOX_NOMINACABECERA`, etc.). El mapeo del flujo viejo no era reusable tal cual.

---

## 3. Acciones aplicadas (3, idempotentes)

### 3.1 Poblar `FCME_USER.CDC_INBOX_MODULE_CONFIG`

Mapeo automático `aggregate_type → FCME_USER.USP_INBOX_<X>` por normalización de nombre (quitar sufijo `_type`/`Type`, eliminar `_`, uppercase) y match contra los 154 SPs existentes en `fcme_migration`.

```sql
-- Patrón usado (97 filas insertadas):
INSERT INTO FCME_USER.CDC_INBOX_MODULE_CONFIG (AGGREGATE_TYPE, SP_NAME, ACTIVE)
VALUES ('bancoTesoreria_type', 'FCME_USER.USP_INBOX_BANCOTESORERIA', 1);
-- ...etc
```

- **Matched:** 97 aggregate_types con SP correspondiente.
- **Unmatched:** 264 aggregate_types sin SP en `fcme_migration` (quedarán en `CDC_INBOX.processed=0` hasta que sus SP wrappers existan o se ajuste el nombre en el catálogo).
- Mapeo completo guardado en `_cdc_inbox_module_config.json`.

### 3.2 Crear trigger `FCME_USER.TRG_PROCESS_CDC_INBOX`

Trigger `AFTER INSERT` sobre `FCME_USER.CDC_INBOX` adaptado a la firma real de los SPs de migración (5 parámetros).

**Firma esperada por los SPs:**
```
@p_id BIGINT, @p_aggregate_type NVARCHAR(200), @p_source_table NVARCHAR(200),
@p_event_type NVARCHAR(20), @p_payload NVARCHAR(MAX)
```

**Lógica del trigger:**
- Cursor sobre `inserted`.
- Lookup en `CDC_INBOX_MODULE_CONFIG` por `aggregate_type AND active=1`.
- Si `sp_name` existe: `sp_executesql` con `SAVE TRANSACTION`, set `is_replicating=1`, ejecuta SP, marca `processed=1, processed_at=SYSUTCDATETIME()`.
- Si falla: rollback al savepoint, escribe a `CDC_INBOX_ERRORS`, marca `processed=1` igualmente (para no reintentar en loop).
- Si NO hay `sp_name`: no hace nada → fila queda con `processed=0`.

### 3.3 Crear sink Kafka `newcore-canonicos-cdc-inbox-jdbc-sink`

POST a Kafka Connect (`http://10.35.3.223:30083/connectors`):

```json
{
  "name": "newcore-canonicos-cdc-inbox-jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "3",
    "topics": "convivencia.canonicos.cdc.outbox",
    "connection.url": "jdbc:sqlserver://10.35.3.64:1433;databaseName=fcme_migration;encrypt=false;trustServerCertificate=true",
    "connection.user": "sa",
    "dialect.name": "SqlServerDatabaseDialect",
    "table.name.format": "fcme_migration.FCME_USER.CDC_INBOX",
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
    "consumer.override.auto.offset.reset": "earliest",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.topic.name": "convivencia.canonicos.cdc.outbox.sink.dlq"
  }
}
```

**Detalle crítico aprendido:** `table.name.format` requiere **3 partes** (`db.schema.table`). Con sólo 2 partes (`FCME_USER.CDC_INBOX`) el driver JDBC interpreta `FCME_USER` como database y falla con:
```
com.microsoft.sqlserver.jdbc.SQLServerException: Database 'FCME_USER' does not exist.
```

---

## 4. Verificación end-to-end

Las 2 filas históricas (`bancoTesoreria_type`) en `fcme_canonicos.cdc_outbox` fluyeron completas:

| Métrica | Antes | Después |
|---|---|---|
| `FCME_USER.CDC_INBOX` rows | 0 | **2** (ambas `processed=1`) |
| `FCME_USER.CDC_INBOX_ERRORS` | 0 | 0 |
| `FCME_USER.BANCOTESORERIA_TYPE` | 2511 | **2513** |

---

## 5. Procedimiento de reset / reproceso (cuando los offsets quedan persistidos)

Si después de borrar/recrear el sink no consume, los offsets del consumer group quedaron registrados y `auto.offset.reset` ya no aplica. Secuencia para forzar reproceso desde el inicio:

```bash
CONNECT=http://10.35.3.223:30083
NAME=newcore-canonicos-cdc-inbox-jdbc-sink

curl -X PUT  $CONNECT/connectors/$NAME/stop
curl -X DELETE $CONNECT/connectors/$NAME/offsets
curl -X PUT  $CONNECT/connectors/$NAME/resume
```

Requiere Kafka Connect 7.4+.

---

## 6. Archivos del repo relacionados

| Archivo | Propósito |
|---|---|
| `_apply_cdc_inbox_setup.py` | Script idempotente con las 3 acciones (re-ejecutable). |
| `_cdc_inbox_module_config.json` | Mapeo `aggregate_type → SP` (matched + unmatched). |
| `_diag_cdc_flow.py` | Diagnóstico end-to-end (outbox/inbox/Kafka Connect). |
| `_verify_cdc.py` | Conteos/samples de cdc_outbox y cdc_inbox en las 3 DBs. |

---

## 7. URLs y endpoints

| Recurso | URL |
|---|---|
| Kafka Connect REST | `http://10.35.3.223:30083` |
| Kafka externo | `10.35.3.223:31092` |
| Kafka interno | `kafka-svc.fcme-infrastructure:9092` |
| Kafka UI | `http://10.35.3.223:30180` (cluster: `fcme-kafka`) |
| SQL Server | `10.35.3.64:1433` |

**Bases SQL Server relevantes:**
- `fcme_legacy` — tablas origen + triggers `trg_outbox_*` + tabla `dbo.cdc_outbox`
- `fcme_canonicos` — bridge: `dbo.cdc_outbox` (lo lee el source Kafka)
- `fcme_migration` — destino: schema `FCME_USER` con `CDC_INBOX`, `CDC_INBOX_MODULE_CONFIG`, `CDC_INBOX_ERRORS`, 154 SPs `USP_INBOX_*` y tablas `*_TYPE`

---

## 8. Pendientes y notas operativas

- **264 aggregate_types sin SP** — listados en `_cdc_inbox_module_config.json` bajo `unmatched`. Para activarlos hay que crear el SP wrapper correspondiente o ajustar el nombre esperado en el catálogo. Mientras tanto sus filas quedan `processed=0` (sin error).
- **41 connectors PAUSED** en Kafka Connect (todos los `migration-*-sink` salvo participe, todos los `normalizada-*`). Se respetaron — son pausa intencional.
- **2 connectors FAILED**: `normalizada-cartera-source`, `normalizada-participe-source`. No tocados, fuera del scope de Flujo 1.
- El sink corre con `errors.tolerance=all`; cualquier fila problemática termina en el DLQ `convivencia.canonicos.cdc.outbox.sink.dlq` con stacktrace en headers.
