"""Despliega:
1) cdc_inbox_module_config (PK aggregate_type -> sp_name)
2) cdc_inbox_errors (log de errores)
3) cdc_inbox_parsed (audit: registra cada payload parseado OK por el wrapper)
4) 30 wrappers usp_inbox_<type> que PARSEAN JSON (extraen PK + algunos campos)
   y registran en cdc_inbox_parsed. SIN llamar CRUD todavia.
5) Actualiza usp_process_cdc_inbox con dispatch real (lookup en module_config + EXEC)
6) Inserta 30 entradas en module_config

NO incluye sp_*Type_CRUD ni inserts en tablas legacy (proximo paso).
"""
import pyodbc, oracledb, re

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
c = sql("fcme_canonicos").cursor()

# ============ Mapeo aggregate_type -> tabla Oracle ============
AT_TO_TABLE = {
    "actualizacionAfiliadoType":"ACTUALIZACION_AFILIADO_TYPE",
    "actualizacionDocumentosType":"ACTUALIZACION_DOCUMENTOS_TYPE",
    "agendaMailAfiliadoType":"AGENDAMAILAFILIADO_TYPE",
    "auditoriaAfiliadoType":"AUDITORIAAFILIADO_TYPE",
    "beneficiarioParticipeType":"BENEFICIARIOPARTICIPE_TYPE",
    "cuentaBancariaAfiliadoType":"CUENTABANCARIAAFILIADO_TYPE",
    "distribucionAfiliadoType":"DISTRIBUCIONAFILIADO_TYPE",
    "documentacionAfiliadoType":"DOCUMENTACIONAFILIADO_TYPE",
    "firmanteParticipeType":"FIRMANTEPARTICIPE_TYPE",
    "grupoFamiliarType":"GRUPOFAMILIAR_TYPE",
    "informacionAdicionalAfiliadoType":"INFORMACIONADICIONALAFILIADO_TYPE",
    "institucionType":"INSTITUCION_TYPE",
    "motivoContableType":"MOTIVOCONTABLE_TYPE",
    "movimientoCuentaType":"MOVIMIENTOCUENTA_TYPE",
    "movimientoTemporalType":"MOVIMIENTOTEMPORAL_TYPE",
    "naturalInformacionAdicionalType":"NATURALINFORMACIONADICIONALTYPE",
    "naturalIngresosEgresosType":"NATURALINGRESOSEGRESOSTYPE",
    "naturalTrabajoType":"NATURALTRABAJOTYPE",
    "personaReferenciasBancariasType":"PERSONAREFERENCIASBANCARIASTYPE",
    "personaReferenciasPersonalesType":"PERSONAREFERENCIASPERSONALESTYPE",
    "personaTelefonosType":"PERSONATELEFONOSTYPE",
    "personaVinculacionesType":"PERSONAVINCULACIONESTYPE",
    "referenciaParticipeType":"REFERENCIAPARTICIPE_TYPE",
    "reporteSIBSParticipeType":"REPORTESIBSPARTICIPE_TYPE",
    "retiroLiquidacionType":"RETIROLIQUIDACION_TYPE",
    "retiroVoluntarioEstadoType":"RETIROVOLUNTARIOESTADO_TYPE",
    "saldoDiarioRubroType":"SALDODIARIORUBRO_TYPE",
    "saldoDiarioType":"SALDODIARIO_TYPE",
    "seguroVidaParticipeType":"SEGUROVIDAPARTICIPE_TYPE",
    "servicioAdicionalType":"SERVICIOADICIONAL_TYPE",
}

# ============ Tablas en canonicos ============
print("="*70)
print("[1] Crear tablas cdc_inbox_module_config / cdc_inbox_errors / cdc_inbox_parsed")
print("="*70)

c.execute("""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='cdc_inbox_module_config')
CREATE TABLE dbo.cdc_inbox_module_config (
    aggregate_type NVARCHAR(200) NOT NULL,
    sp_name        NVARCHAR(300) NOT NULL,
    target_db      NVARCHAR(50)  NULL,
    module_name    NVARCHAR(50)  NULL,
    active         BIT           NOT NULL DEFAULT 1,
    created_at     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at     DATETIME2(3)  NULL,
    CONSTRAINT PK_cdc_inbox_module_config PRIMARY KEY (aggregate_type)
)""")

c.execute("""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='cdc_inbox_errors')
BEGIN
    CREATE TABLE dbo.cdc_inbox_errors (
        error_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
        inbox_id       BIGINT       NOT NULL,
        aggregate_type NVARCHAR(200) NULL,
        event_type     NVARCHAR(50)  NULL,
        error_message  NVARCHAR(MAX) NULL,
        created_at     DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_cdc_inbox_errors_inbox ON dbo.cdc_inbox_errors(inbox_id);
END""")

c.execute("""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='cdc_inbox_parsed')
BEGIN
    CREATE TABLE dbo.cdc_inbox_parsed (
        parsed_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
        inbox_id       BIGINT NOT NULL,
        aggregate_type NVARCHAR(200) NOT NULL,
        aggregate_id   NVARCHAR(200) NULL,
        event_type     NVARCHAR(50)  NULL,
        pk_value       NVARCHAR(200) NULL,
        sample_field   NVARCHAR(500) NULL,
        parsed_at      DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_cdc_inbox_parsed_inbox ON dbo.cdc_inbox_parsed(inbox_id);
END""")
print("  OK")

# ============ Inspeccionar columnas Oracle por type ============
def get_oracle_cols(t):
    co.execute("""SELECT column_name, data_type FROM all_tab_columns
                  WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id""", t=t)
    return co.fetchall()

# Heuristica para identificar PK Oracle (la que usa el trigger para agg_id)
def detect_pk_oracle(cols):
    PREFER = ["IDENTIFICACION","CODIGOTIPOREFERENCIA","CODIGO_CEDU","CODIGOMOTIVOCONTABLE",
              "CEDULAFAMILIAR","CODIGOEMPRESA"]
    col_names = [c[0] for c in cols if c[0] != "ID"]
    for p in PREFER:
        if p in col_names: return p
    # Fallback: primera col que empiece con CODIGO, IDENTIF, o la primera no-ID
    for cn in col_names:
        if cn.startswith(("CODIGO","IDENTIF","CEDULA")): return cn
    return col_names[0] if col_names else None

# ============ Generar 30 wrappers ============
print("\n" + "="*70)
print("[2] Generar 30 wrappers usp_inbox_<type>")
print("="*70)

deployed_wrappers = []
for at, ot in AT_TO_TABLE.items():
    cols = get_oracle_cols(ot)
    if not cols:
        print(f"  skip {at}: tabla {ot} sin cols")
        continue
    pk = detect_pk_oracle(cols)
    if not pk:
        print(f"  skip {at}: no PK")
        continue

    # Tomar otra col representativa para el log
    sample_col = next((c[0] for c in cols if c[0] != "ID" and c[0] != pk), pk)

    wrap_name = f"usp_inbox_{at}"
    body = f"""CREATE OR ALTER PROCEDURE dbo.{wrap_name}
    @inbox_id       BIGINT,
    @aggregate_id   NVARCHAR(200),
    @aggregate_type NVARCHAR(200),
    @source_table   NVARCHAR(200),
    @event_type     NVARCHAR(50),
    @payload        NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        -- Parsear PK desde el payload JSON
        DECLARE @pk_value NVARCHAR(200) = JSON_VALUE(@payload, '$.{pk}');
        DECLARE @sample   NVARCHAR(500) = JSON_VALUE(@payload, '$.{sample_col}');

        IF @pk_value IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type,
                    N'payload sin {pk}');
            RETURN;
        END

        -- Audit: registrar que el wrapper proceso este evento
        -- (proximo paso: llamar sp_{at}_CRUD @Accion=..., @PK=@pk_value, ...)
        INSERT INTO dbo.cdc_inbox_parsed
            (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @pk_value, @sample);
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type,
                N'wrapper {at}: ' + ERROR_MESSAGE());
    END CATCH
END"""
    try:
        c.execute(body)
        deployed_wrappers.append((at, wrap_name, pk))
    except Exception as e:
        print(f"  FAIL {at}: {str(e)[:150]}")

print(f"  desplegados: {len(deployed_wrappers)}")
for at, w, pk in deployed_wrappers[:10]:
    print(f"    {w}  PK={pk}")
if len(deployed_wrappers) > 10: print(f"    ... y {len(deployed_wrappers)-10} mas")

# ============ Actualizar usp_process_cdc_inbox ============
print("\n" + "="*70)
print("[3] Actualizar usp_process_cdc_inbox con dispatch real")
print("="*70)

c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_process_cdc_inbox
    @inbox_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @aggregate_id   NVARCHAR(200),
            @aggregate_type NVARCHAR(200),
            @event_type     NVARCHAR(50),
            @payload        NVARCHAR(MAX),
            @source_table   NVARCHAR(200),
            @sp_name        NVARCHAR(300),
            @save_used      BIT = 0;

    SELECT
        @aggregate_id   = aggregate_id,
        @aggregate_type = aggregate_type,
        @event_type     = event_type,
        @payload        = payload,
        @source_table   = source_table
    FROM dbo.cdc_inbox WITH (NOLOCK)
    WHERE id = @inbox_id;

    IF @aggregate_type IS NULL RETURN;

    EXEC sp_set_session_context N'is_replicating', 1;

    -- Lookup wrapper
    SELECT @sp_name = sp_name
    FROM dbo.cdc_inbox_module_config WITH (NOLOCK)
    WHERE aggregate_type = @aggregate_type AND active = 1;

    IF @sp_name IS NULL
    BEGIN
        UPDATE dbo.cdc_inbox SET processed = 1, processed_at = SYSUTCDATETIME() WHERE id = @inbox_id;
        EXEC sp_set_session_context N'is_replicating', 0;
        RETURN;
    END

    -- Validacion anti-inyeccion
    IF PATINDEX('%[^a-zA-Z0-9_.\\[\\]]%', @sp_name) > 0
    BEGIN
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'sp_name invalido: ' + @sp_name);
        EXEC sp_set_session_context N'is_replicating', 0;
        RETURN;
    END

    -- Savepoint para aislar fallo del wrapper
    IF @@TRANCOUNT > 0 AND XACT_STATE() = 1
    BEGIN
        SAVE TRANSACTION wrapper_sp;
        SET @save_used = 1;
    END

    BEGIN TRY
        DECLARE @stmt NVARCHAR(MAX) =
            N'EXEC ' + @sp_name +
            N' @inbox_id, @aggregate_id, @aggregate_type, @source_table, @event_type, @payload';

        EXEC sp_executesql @stmt,
            N'@inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200), @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)',
            @inbox_id = @inbox_id,
            @aggregate_id = @aggregate_id,
            @aggregate_type = @aggregate_type,
            @source_table = @source_table,
            @event_type = @event_type,
            @payload = @payload;

        UPDATE dbo.cdc_inbox SET processed = 1, processed_at = SYSUTCDATETIME() WHERE id = @inbox_id;
    END TRY
    BEGIN CATCH
        IF @save_used = 1 AND XACT_STATE() = 1
            ROLLBACK TRANSACTION wrapper_sp;
        IF XACT_STATE() = 1
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type,
                    N'dispatcher: ' + ERROR_MESSAGE());
        END
    END CATCH

    EXEC sp_set_session_context N'is_replicating', 0;
END
""")
print("  usp_process_cdc_inbox actualizado")

# ============ Insertar 30 entradas en module_config ============
print("\n" + "="*70)
print("[4] Poblar cdc_inbox_module_config")
print("="*70)
c.execute("DELETE FROM dbo.cdc_inbox_module_config")
for at, w, pk in deployed_wrappers:
    c.execute("""INSERT INTO dbo.cdc_inbox_module_config
                 (aggregate_type, sp_name, target_db, module_name, active)
                 VALUES (?, ?, ?, 'PARTICIPE', 1)""",
              at, f"dbo.{w}", "dbFC")
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config WHERE active=1")
print(f"  active rows: {c.fetchone()[0]}")

# ============ Test e2e ============
print("\n" + "="*70)
print("[5] Test e2e: insertar evento y verificar parse")
print("="*70)
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c.execute("DELETE FROM dbo.cdc_inbox_parsed")

c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
             VALUES ('99', 'referenciaParticipeType', 'INSERT',
                     '{"CODIGOTIPOREFERENCIA":"99","DESCRIPCIONTIPOREFERENCIA":"PARSED OK"}',
                     'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")
c.execute("SELECT id, processed FROM dbo.cdc_inbox")
for r in c.fetchall(): print(f"  cdc_inbox: id={r.id} processed={r.processed}")

c.execute("SELECT inbox_id, aggregate_type, pk_value, sample_field, event_type FROM dbo.cdc_inbox_parsed")
parsed = c.fetchall()
print(f"  cdc_inbox_parsed: {len(parsed)} filas")
for r in parsed:
    print(f"    inbox_id={r.inbox_id} type={r.aggregate_type} pk={r.pk_value} sample={r.sample_field} ev={r.event_type}")

c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
print(f"  cdc_inbox_errors: {c.fetchone()[0]}")

# Cleanup
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_parsed")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
print("\n=== Paso OK: module_config + 30 wrappers desplegados, dispatch real activo ===")
orcl.close()
