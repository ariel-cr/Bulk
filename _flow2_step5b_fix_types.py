"""Ajusta CRUD y wrapper a los tipos reales: co_tref SMALLINT, ds_tref VARCHAR(50) NOT NULL."""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

c = sql("fcme_canonicos").cursor()
c_fc = sql("dbFC").cursor()

# CRUD: tipos correctos
c_fc.execute("""
CREATE OR ALTER PROCEDURE dbo.sp_referenciaParticipeType_CRUD
    @Accion CHAR(1),
    @CodigoTipoReferencia      SMALLINT,
    @DescripcionTipoReferencia VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    IF @Accion = 'I'
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM dbo.sfct_referencias WHERE co_tref=@CodigoTipoReferencia)
            INSERT INTO dbo.sfct_referencias (co_tref, ds_tref)
            VALUES (@CodigoTipoReferencia, ISNULL(@DescripcionTipoReferencia,''));
        ELSE
            UPDATE dbo.sfct_referencias SET ds_tref = ISNULL(@DescripcionTipoReferencia,'')
            WHERE co_tref=@CodigoTipoReferencia;
    END
    ELSE IF @Accion = 'U'
    BEGIN
        IF EXISTS (SELECT 1 FROM dbo.sfct_referencias WHERE co_tref=@CodigoTipoReferencia)
            UPDATE dbo.sfct_referencias SET ds_tref = ISNULL(@DescripcionTipoReferencia,'')
            WHERE co_tref=@CodigoTipoReferencia;
        ELSE
            INSERT INTO dbo.sfct_referencias (co_tref, ds_tref)
            VALUES (@CodigoTipoReferencia, ISNULL(@DescripcionTipoReferencia,''));
    END
    ELSE IF @Accion = 'D'
        DELETE FROM dbo.sfct_referencias WHERE co_tref=@CodigoTipoReferencia;
    EXEC sp_set_session_context N'is_replicating', 0;
END
""")
print("CRUD redeployado con SMALLINT")

# Wrapper: convertir JSON value a int
c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_inbox_referenciaParticipeType
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE
            @cod_str NVARCHAR(50)  = JSON_VALUE(@payload,'$.CODIGOTIPOREFERENCIA'),
            @desc    VARCHAR(50)   = JSON_VALUE(@payload,'$.DESCRIPCIONTIPOREFERENCIA'),
            @cod     SMALLINT,
            @accion  CHAR(1) = CASE
                WHEN @event_type IN ('INSERT','I') THEN 'I'
                WHEN @event_type IN ('UPDATE','U') THEN 'U'
                WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
                ELSE 'I' END;

        IF @cod_str IS NULL OR ISNUMERIC(@cod_str)=0
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type,
                    N'CODIGOTIPOREFERENCIA invalido o no numerico: ' + ISNULL(@cod_str,'<null>'));
            RETURN;
        END
        SET @cod = CAST(@cod_str AS SMALLINT);

        EXEC dbFC.dbo.sp_referenciaParticipeType_CRUD
             @Accion=@accion,
             @CodigoTipoReferencia=@cod,
             @DescripcionTipoReferencia=@desc;
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper: ' + ERROR_MESSAGE());
    END CATCH
END
""")
print("Wrapper redeployado con CAST a SMALLINT")

# Cleanup pre-test
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref IN (97,98,99)")
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")

print("\n[Test E2E] INSERT/UPDATE/DELETE via cdc_inbox")
# INSERT
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
             VALUES ('99','referenciaParticipeType','INSERT',
                     '{"CODIGOTIPOREFERENCIA":"99","DESCRIPCIONTIPOREFERENCIA":"REF TEST"}',
                     'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")
c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref=99")
for r in c_fc.fetchall(): print(f"  INSERT: co_tref={r.co_tref} ds_tref={r.ds_tref}")

# UPDATE
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
             VALUES ('99','referenciaParticipeType','UPDATE',
                     '{"CODIGOTIPOREFERENCIA":"99","DESCRIPCIONTIPOREFERENCIA":"REF MODIF"}',
                     'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")
c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref=99")
for r in c_fc.fetchall(): print(f"  UPDATE: co_tref={r.co_tref} ds_tref={r.ds_tref}")

# DELETE
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
             VALUES ('99','referenciaParticipeType','DELETE',
                     '{"CODIGOTIPOREFERENCIA":"99","DESCRIPCIONTIPOREFERENCIA":"REF MODIF"}',
                     'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")
c_fc.execute("SELECT COUNT(*) FROM dbo.sfct_referencias WHERE co_tref=99")
print(f"  DELETE: filas con co_tref=99 = {c_fc.fetchone()[0]}")

# Verificar processed y errores
c.execute("SELECT id, event_type, processed FROM dbo.cdc_inbox ORDER BY id")
print("\ncdc_inbox final:")
for r in c.fetchall():
    print(f"  id={r.id} ev={r.event_type} processed={r.processed}")

c.execute("SELECT inbox_id, error_message FROM dbo.cdc_inbox_errors")
errs = c.fetchall()
print(f"\nerrores: {len(errs)}")
for r in errs:
    print(f"  inbox_id={r.inbox_id}: {r.error_message[:200]}")

# Probar Oracle origin -> outbox
print("\n[Test Oracle -> CDC_OUTBOX]")
orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_TYPE='referenciaParticipeType'")
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='98'")
co.execute("INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA) VALUES ('98','Origen Oracle')")
orcl.commit()
co.execute("""SELECT ID, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE
              FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_TYPE='referenciaParticipeType'""")
for r in co.fetchall():
    print(f"  outbox: id={r[0]} agg={r[1]} ev={r[2]} src={r[4]}")
    print(f"    payload={r[3].read() if hasattr(r[3],'read') else r[3]}")

# Cleanup
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA='98'")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_TYPE='referenciaParticipeType'")
orcl.commit()
orcl.close()
c.execute("DELETE FROM dbo.cdc_inbox")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref IN (97,98,99)")

print("\n=== PASO 5 PILOTO OK ===")
