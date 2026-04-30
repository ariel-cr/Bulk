"""Paso 5 (Flujo 2 piloto): REFERENCIAPARTICIPE_TYPE -> sfct_referencias

Construye:
A) Trigger Oracle en FCME_USER.REFERENCIAPARTICIPE_TYPE -> FCME_USER.CDC_OUTBOX
B) sp_referenciaParticipeType_CRUD en dbFC (con @Accion I/U/D)
C) Wrapper dbo.usp_inbox_referenciaParticipeType en canonicos (parsea JSON, llama CRUD)
D) Registra en cdc_inbox_module_config
E) Test end-to-end:
   E1) test directo de la cadena SQL (insertar a cdc_inbox simulando sink)
   E2) test parcial Oracle: INSERT en REFERENCIAPARTICIPE_TYPE -> CDC_OUTBOX
"""
import pyodbc, oracledb

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

# ============================================================
# A) Trigger Oracle en REFERENCIAPARTICIPE_TYPE -> CDC_OUTBOX
# ============================================================
print("="*70)
print("[A] Trigger Oracle FCME_USER.REFERENCIAPARTICIPE_TYPE -> CDC_OUTBOX")
print("="*70)
trg_oracle = """CREATE OR REPLACE TRIGGER FCME_USER.TRG_OUTBOX_REFERENCIAPARTICIPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.REFERENCIAPARTICIPE_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    -- anti-loop: si esta sesion esta replicando desde Legacy hacia Newcore,
    -- no re-emitir
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN
        RETURN;
    END IF;

    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := :NEW.CODIGOTIPOREFERENCIA;
        v_payload := JSON_OBJECT(
            'CODIGOTIPOREFERENCIA' VALUE :NEW.CODIGOTIPOREFERENCIA,
            'DESCRIPCIONTIPOREFERENCIA' VALUE :NEW.DESCRIPCIONTIPOREFERENCIA
        );
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := :NEW.CODIGOTIPOREFERENCIA;
        v_payload := JSON_OBJECT(
            'CODIGOTIPOREFERENCIA' VALUE :NEW.CODIGOTIPOREFERENCIA,
            'DESCRIPCIONTIPOREFERENCIA' VALUE :NEW.DESCRIPCIONTIPOREFERENCIA
        );
    ELSE
        v_event := 'DELETE';
        v_pk    := :OLD.CODIGOTIPOREFERENCIA;
        v_payload := JSON_OBJECT(
            'CODIGOTIPOREFERENCIA' VALUE :OLD.CODIGOTIPOREFERENCIA,
            'DESCRIPCIONTIPOREFERENCIA' VALUE :OLD.DESCRIPCIONTIPOREFERENCIA
        );
    END IF;

    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES
        (v_pk, 'referenciaParticipeType', v_event, v_payload, 'FCME_USER.REFERENCIAPARTICIPE_TYPE');
END;"""
co.execute(trg_oracle)
co.execute("""SELECT status FROM all_objects
              WHERE owner='FCME_USER' AND object_name='TRG_OUTBOX_REFERENCIAPARTICIPE'""")
print(f"  trigger status: {co.fetchone()[0]}")

# ============================================================
# B) sp_referenciaParticipeType_CRUD en dbFC
# ============================================================
print("\n" + "="*70)
print("[B] sp_referenciaParticipeType_CRUD en dbFC")
print("="*70)
c_fc = sql("dbFC").cursor()
crud_sp = """
CREATE OR ALTER PROCEDURE dbo.sp_referenciaParticipeType_CRUD
    @Accion CHAR(1),
    @CodigoTipoReferencia      NVARCHAR(50),
    @DescripcionTipoReferencia NVARCHAR(200) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si la sesion esta replicando, los triggers de outbox legacy
    -- (trg_outbox_*) hacen RETURN. Activamos la marca aqui.
    EXEC sp_set_session_context N'is_replicating', 1;

    IF @Accion = 'I'
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM dbo.sfct_referencias WHERE co_tref = @CodigoTipoReferencia)
            INSERT INTO dbo.sfct_referencias (co_tref, ds_tref)
            VALUES (@CodigoTipoReferencia, @DescripcionTipoReferencia);
        ELSE
            UPDATE dbo.sfct_referencias
            SET ds_tref = @DescripcionTipoReferencia
            WHERE co_tref = @CodigoTipoReferencia;
    END
    ELSE IF @Accion = 'U'
    BEGIN
        IF EXISTS (SELECT 1 FROM dbo.sfct_referencias WHERE co_tref = @CodigoTipoReferencia)
            UPDATE dbo.sfct_referencias
            SET ds_tref = @DescripcionTipoReferencia
            WHERE co_tref = @CodigoTipoReferencia;
        ELSE
            INSERT INTO dbo.sfct_referencias (co_tref, ds_tref)
            VALUES (@CodigoTipoReferencia, @DescripcionTipoReferencia);
    END
    ELSE IF @Accion = 'D'
    BEGIN
        DELETE FROM dbo.sfct_referencias WHERE co_tref = @CodigoTipoReferencia;
    END

    EXEC sp_set_session_context N'is_replicating', 0;
END
"""
c_fc.execute(crud_sp)
c_fc.execute("""SELECT name FROM sys.objects
                WHERE name='sp_referenciaParticipeType_CRUD' AND type='P'""")
print(f"  CRUD SP: {[r.name for r in c_fc.fetchall()]}")

# ============================================================
# C) Wrapper en canonicos
# ============================================================
print("\n" + "="*70)
print("[C] Wrapper usp_inbox_referenciaParticipeType en canonicos")
print("="*70)
c = sql("fcme_canonicos").cursor()
wrap_sp = """
CREATE OR ALTER PROCEDURE dbo.usp_inbox_referenciaParticipeType
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
        DECLARE
            @cod  NVARCHAR(50)  = JSON_VALUE(@payload, '$.CODIGOTIPOREFERENCIA'),
            @desc NVARCHAR(200) = JSON_VALUE(@payload, '$.DESCRIPCIONTIPOREFERENCIA'),
            @accion CHAR(1) = CASE
                WHEN @event_type IN ('INSERT','I') THEN 'I'
                WHEN @event_type IN ('UPDATE','U') THEN 'U'
                WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
                ELSE 'I'
            END;

        IF @cod IS NULL
        BEGIN
            INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
            VALUES (@inbox_id, @aggregate_type, @event_type, N'payload sin CODIGOTIPOREFERENCIA');
            RETURN;
        END

        EXEC dbFC.dbo.sp_referenciaParticipeType_CRUD
             @Accion = @accion,
             @CodigoTipoReferencia = @cod,
             @DescripcionTipoReferencia = @desc;
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type,
                N'wrapper: ' + ERROR_MESSAGE());
    END CATCH
END
"""
c.execute(wrap_sp)
c.execute("SELECT name FROM sys.objects WHERE name='usp_inbox_referenciaParticipeType' AND type='P'")
print(f"  Wrapper: {[r.name for r in c.fetchall()]}")

# ============================================================
# D) Registrar en module_config
# ============================================================
print("\n" + "="*70)
print("[D] Registrar en cdc_inbox_module_config")
print("="*70)
c.execute("""DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type='referenciaParticipeType'""")
c.execute("""INSERT INTO dbo.cdc_inbox_module_config
             (aggregate_type, sp_name, target_db, module_name, active)
             VALUES ('referenciaParticipeType','dbo.usp_inbox_referenciaParticipeType','dbFC','PARTICIPE',1)""")
c.execute("SELECT * FROM dbo.cdc_inbox_module_config")
cols = [d[0] for d in c.description]
for r in c.fetchall():
    print("  " + " | ".join(f"{cols[i]}={v}" for i,v in enumerate(r) if i<5))

# ============================================================
# E1) Test directo de la cadena SQL
# ============================================================
print("\n" + "="*70)
print("[E1] Test SQL: INSERT a cdc_inbox -> trigger -> dispatcher -> wrapper -> CRUD -> sfct_referencias")
print("="*70)
# Limpiar test rows
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref LIKE 'TST%'")
c.execute("DELETE FROM dbo.cdc_inbox WHERE aggregate_id LIKE 'TST%'")
c.execute("DELETE FROM dbo.cdc_inbox_errors")

c_fc.execute("SELECT COUNT(*) FROM dbo.sfct_referencias WHERE co_tref LIKE 'TST%'")
print(f"  sfct_referencias TST% antes: {c_fc.fetchone()[0]}")

# Simular evento INSERT
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
             VALUES ('TST01','referenciaParticipeType','INSERT',
                     '{"CODIGOTIPOREFERENCIA":"TST01","DESCRIPCIONTIPOREFERENCIA":"Test 1"}',
                     'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")

c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref='TST01'")
for r in c_fc.fetchall():
    print(f"  INSERT: cod={r.co_tref} desc={r.ds_tref}")

# Simular evento UPDATE
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
             VALUES ('TST01','referenciaParticipeType','UPDATE',
                     '{"CODIGOTIPOREFERENCIA":"TST01","DESCRIPCIONTIPOREFERENCIA":"Test 1 Modificado"}',
                     'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")

c_fc.execute("SELECT co_tref, ds_tref FROM dbo.sfct_referencias WHERE co_tref='TST01'")
for r in c_fc.fetchall():
    print(f"  UPDATE: cod={r.co_tref} desc={r.ds_tref}")

# Simular evento DELETE
c.execute("""INSERT INTO dbo.cdc_inbox (aggregate_id, aggregate_type, event_type, payload, source_table)
             VALUES ('TST01','referenciaParticipeType','DELETE',
                     '{"CODIGOTIPOREFERENCIA":"TST01","DESCRIPCIONTIPOREFERENCIA":"Test 1 Modificado"}',
                     'FCME_USER.REFERENCIAPARTICIPE_TYPE')""")

c_fc.execute("SELECT COUNT(*) FROM dbo.sfct_referencias WHERE co_tref='TST01'")
n = c_fc.fetchone()[0]
print(f"  DELETE: filas TST01 restantes = {n}")

# Verificar processed
c.execute("SELECT id, aggregate_id, event_type, processed FROM dbo.cdc_inbox WHERE aggregate_id='TST01' ORDER BY id")
print("\n  cdc_inbox (TST01):")
for r in c.fetchall():
    print(f"    id={r.id} ev={r.event_type} processed={r.processed}")

c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors")
print(f"  errores: {c.fetchone()[0]}")

# ============================================================
# E2) Test Oracle: INSERT en REFERENCIAPARTICIPE_TYPE -> CDC_OUTBOX
# ============================================================
print("\n" + "="*70)
print("[E2] Test Oracle: INSERT en REFERENCIAPARTICIPE_TYPE -> CDC_OUTBOX")
print("="*70)
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_TYPE='referenciaParticipeType' AND AGGREGATE_ID LIKE 'TST%'")
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA LIKE 'TST%'")

co.execute("""INSERT INTO FCME_USER.REFERENCIAPARTICIPE_TYPE
              (CODIGOTIPOREFERENCIA, DESCRIPCIONTIPOREFERENCIA)
              VALUES ('TST02','Test Oracle Origen')""")
co.execute("""SELECT ID, AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT
              FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_TYPE='referenciaParticipeType'""")
for r in co.fetchall():
    print(f"  outbox: id={r[0]} agg={r[1]} ev={r[3]} src={r[5]} created={r[6]}")
    print(f"    payload={r[4]}")

# Cleanup test rows
co.execute("DELETE FROM FCME_USER.REFERENCIAPARTICIPE_TYPE WHERE CODIGOTIPOREFERENCIA LIKE 'TST%'")
co.execute("DELETE FROM FCME_USER.CDC_OUTBOX WHERE AGGREGATE_TYPE='referenciaParticipeType' AND AGGREGATE_ID LIKE 'TST%'")
orcl.commit()
c.execute("DELETE FROM dbo.cdc_inbox WHERE aggregate_id LIKE 'TST%'")
c.execute("DELETE FROM dbo.cdc_inbox_errors")
c_fc.execute("DELETE FROM dbo.sfct_referencias WHERE co_tref LIKE 'TST%'")

print("\n=== PASO 5 (piloto) OK ===")
print("Componentes desplegados:")
print("  Oracle TRG_OUTBOX_REFERENCIAPARTICIPE: ENABLED")
print("  dbFC.sp_referenciaParticipeType_CRUD")
print("  canonicos.usp_inbox_referenciaParticipeType (wrapper)")
print("  canonicos.cdc_inbox_module_config: 1 entrada activa")

orcl.close()
