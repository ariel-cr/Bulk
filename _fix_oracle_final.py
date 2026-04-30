"""Fix final: trigger AFTER INSERT FOR EACH ROW que llama USP con AUTONOMOUS_TRANSACTION.
El SP procesa el payload Y marca PROCESSED=1 en una transaccion autonoma (escapa mutating table)."""
import oracledb, pyodbc, time
orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
c = orcl.cursor()

# 1) SP con AUTONOMOUS_TRANSACTION que marca PROCESSED
print("[1] Recrear USP_INBOX_PARTICIPE con AUTONOMOUS_TRANSACTION")
c.execute("""
CREATE OR REPLACE PROCEDURE USP_INBOX_PARTICIPE(
    p_id             IN NUMBER,
    p_aggregate_type IN VARCHAR2,
    p_event_type     IN VARCHAR2,
    p_payload        IN CLOB
) AS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_cedu VARCHAR2(100);
    v_count NUMBER;
BEGIN
    IF p_aggregate_type = 'actualizacionAfiliadoType' THEN
        v_cedu := JSON_VALUE(p_payload, '$.ci_cedu');
        IF v_cedu IS NULL THEN
            UPDATE CDC_INBOX SET PROCESSED=1, PROCESSED_AT=SYSTIMESTAMP WHERE ID=p_id;
            COMMIT; RETURN;
        END IF;

        IF p_event_type IN ('DELETE','DELETED') THEN
            DELETE FROM ACTUALIZACION_AFILIADO_TYPE WHERE CODIGO_CEDU = v_cedu;
        ELSE
            SELECT COUNT(*) INTO v_count FROM ACTUALIZACION_AFILIADO_TYPE WHERE CODIGO_CEDU = v_cedu;
            IF v_count = 0 THEN
                INSERT INTO ACTUALIZACION_AFILIADO_TYPE (
                    CODIGO_CEDU, CODIGO_PROV, CODIGO_CANT, CODIGO_PARR,
                    DESCRIPCION_CALL_PRIM, NUMERO_CALL_PRIM, DESCRIPCION_CALL_SECU, NUMERO_CALL_SECU,
                    NUMERO_MANZ, NUMERO_VILL, DESCRIPCION_CDLA,
                    TEXTO_TELF_CONVENIO, TEXTO_TELF_CELU, TEXTO_MAIL,
                    FECHA_INGRESO, FECHA_MODIFICACION
                ) VALUES (
                    v_cedu,
                    JSON_VALUE(p_payload,'$.co_prov'), JSON_VALUE(p_payload,'$.co_cant'), JSON_VALUE(p_payload,'$.co_parr'),
                    JSON_VALUE(p_payload,'$.ds_call_prim'), JSON_VALUE(p_payload,'$.nu_call_prim'),
                    JSON_VALUE(p_payload,'$.ds_call_secu'), JSON_VALUE(p_payload,'$.nu_call_secu'),
                    JSON_VALUE(p_payload,'$.nu_manz'), JSON_VALUE(p_payload,'$.nu_vill'), JSON_VALUE(p_payload,'$.ds_cdla'),
                    JSON_VALUE(p_payload,'$.tx_telf_conv'), JSON_VALUE(p_payload,'$.tx_telf_celu'), JSON_VALUE(p_payload,'$.tx_mail'),
                    JSON_VALUE(p_payload,'$.fx_ingr'), JSON_VALUE(p_payload,'$.fx_modi')
                );
            ELSE
                UPDATE ACTUALIZACION_AFILIADO_TYPE SET
                    CODIGO_PROV = JSON_VALUE(p_payload,'$.co_prov'),
                    CODIGO_CANT = JSON_VALUE(p_payload,'$.co_cant'),
                    CODIGO_PARR = JSON_VALUE(p_payload,'$.co_parr'),
                    DESCRIPCION_CALL_PRIM = JSON_VALUE(p_payload,'$.ds_call_prim'),
                    NUMERO_CALL_PRIM = JSON_VALUE(p_payload,'$.nu_call_prim'),
                    DESCRIPCION_CALL_SECU = JSON_VALUE(p_payload,'$.ds_call_secu'),
                    NUMERO_CALL_SECU = JSON_VALUE(p_payload,'$.nu_call_secu'),
                    TEXTO_TELF_CONVENIO = JSON_VALUE(p_payload,'$.tx_telf_conv'),
                    TEXTO_TELF_CELU = JSON_VALUE(p_payload,'$.tx_telf_celu'),
                    TEXTO_MAIL = JSON_VALUE(p_payload,'$.tx_mail'),
                    FECHA_MODIFICACION = JSON_VALUE(p_payload,'$.fx_modi')
                WHERE CODIGO_CEDU = v_cedu;
            END IF;
        END IF;
    END IF;

    -- marcar procesado (autonomous => no mutating table)
    UPDATE CDC_INBOX SET PROCESSED=1, PROCESSED_AT=SYSTIMESTAMP WHERE ID=p_id;
    COMMIT;
EXCEPTION WHEN OTHERS THEN
    INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, SUBSTR(SQLERRM,1,4000));
    COMMIT;
END;
""")
print("  ok")

# 2) Trigger simple AFTER INSERT FOR EACH ROW que invoca el SP
print("[2] Recrear TRG_PROCESS_CDC_INBOX (simple, delega todo al SP)")
# primero drop el compound (ya no lo necesitamos)
try:
    c.execute("DROP TRIGGER TRG_PROCESS_CDC_INBOX")
except Exception as e:
    print(f"  drop: {str(e)[:100]}")

c.execute("""
CREATE OR REPLACE TRIGGER TRG_PROCESS_CDC_INBOX
AFTER INSERT ON CDC_INBOX
FOR EACH ROW
DECLARE
    v_sp VARCHAR2(300);
BEGIN
    BEGIN
        SELECT SP_NAME INTO v_sp FROM CDC_INBOX_MODULE_CONFIG
        WHERE AGGREGATE_TYPE = :NEW.AGGREGATE_TYPE AND ACTIVE = 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN RETURN; END;
    -- invoca el SP que tiene autonomous transaction para evitar mutating table
    EXECUTE IMMEDIATE 'BEGIN '||v_sp||'(:1, :2, :3, :4); END;'
        USING :NEW.ID, :NEW.AGGREGATE_TYPE, :NEW.EVENT_TYPE, :NEW.PAYLOAD;
END;
""")
print("  ok")

# 3) Reprocesar los ya existentes
print("[3] Reprocesar existentes")
c.execute("SELECT ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD FROM CDC_INBOX WHERE PROCESSED=0 AND AGGREGATE_TYPE='actualizacionAfiliadoType'")
rows = c.fetchall()
for r in rows:
    p = r[3].read() if hasattr(r[3],'read') else r[3]
    c.execute("BEGIN USP_INBOX_PARTICIPE(:1, :2, :3, :4); END;", [r[0], r[1], r[2], p])
orcl.commit()
c.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE"); print(f"  ACTUALIZACION_AFILIADO_TYPE: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM CDC_INBOX WHERE PROCESSED=1"); print(f"  CDC_INBOX processed=1: {c.fetchone()[0]}")

# 4) Test con UPDATE fresco
print("[4] Disparar UPDATE y ver flujo automatico")
f = pyodbc.connect("DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=dbFC;UID=sa;PWD=YourPassword123", autocommit=True).cursor()
cc = pyodbc.connect("DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_canonicos;UID=sa;PWD=YourPassword123", autocommit=True).cursor()
cc.execute("SELECT COUNT(*), MAX(id) FROM dbo.cdc_outbox"); b = cc.fetchone()
c.execute("SELECT COUNT(*), MAX(ID) FROM CDC_INBOX"); oi = c.fetchone()
print(f"  antes: canonicos={b}  oracle={oi}")
f.execute("UPDATE dbo.fctbafil_actu SET ci_cedu=ci_cedu WHERE ci_cedu=(SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu)")
for i in range(6):
    time.sleep(10)
    cc.execute("SELECT COUNT(*), MAX(id) FROM dbo.cdc_outbox"); ac = cc.fetchone()
    c.execute("SELECT COUNT(*), MAX(ID) FROM CDC_INBOX"); ao = c.fetchone()
    c.execute("SELECT COUNT(*) FROM ACTUALIZACION_AFILIADO_TYPE"); at = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM CDC_INBOX_ERRORS"); ae = c.fetchone()[0]
    print(f"  t={(i+1)*10}s  canonicos={ac}  oracle CDC_INBOX={ao}  ACTUALIZACION_AFILIADO_TYPE={at}  errors={ae}")
    if ao[0] > oi[0]: break
