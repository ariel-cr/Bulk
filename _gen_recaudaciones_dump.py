"""Genera recaudaciones_triggers_dump.sql con los DDL F1+F2 a partir de los SPECS
de _deploy_recaudaciones_flow1.py y _deploy_recaudaciones_flow2.py (mismo formato
que nomina_triggers_dump.sql).
"""

SPECS_F1 = [
    {'agg':'aplicacionRecaudacion_type','ltbl':'rctbapli_reca','lkey':['sc_apli_reca','ci_cedu','ti_reca','co_rol'],
     'pcols':['sc_apli_reca','ci_cedu','ti_reca','co_rol','co_inst','co_prov','mo_reca','mo_rubr']},
    {'agg':'caucionRecaudacion_type','ltbl':'rctbcaut','lkey':['co_empr','ti_reca','ti_dsto'],
     'pcols':['co_empr','ti_reca','ti_dsto','co_cnta_auto_noid','co_cnta_auto_papl']},
    {'agg':'devolucionRendicion_type','ltbl':'rctbdevo_rind','lkey':['sc_devo'],
     'pcols':['sc_devo','ci_cedu','ti_reca','ti_dsto','nu_cpbt','co_cnta','mo_devo','fe_depo']},
    {'agg':'estadoCuenta_type','ltbl':'rctbesta_cnta','lkey':['co_empr','sc_cpbt'],
     'pcols':['co_empr','sc_cpbt','co_cnta','nu_cpbt','fe_depo','mo_depo']},
    {'agg':'estadoRecaudacion_type','ltbl':'rctbesta_reca','lkey':['st_reca'],
     'pcols':['st_reca','ds_estado','no_corto','ci_tipo','ti_esta']},
    {'agg':'recaudacion_type','ltbl':'rctbreca','lkey':['sc_reca'],
     'pcols':['ti_reca','st_reca','mo_reca','sc_reca','fe_ingr','fe_autr']},
    {'agg':'rendicion_type','ltbl':'rctbrind','lkey':['co_empr','sc_reca'],
     'pcols':['co_empr','sc_reca','ci_cedula','co_rol','co_inst','co_prov','ti_reca','nu_cpbt']},
    {'agg':'saldoRecaudacion_type','ltbl':'rctbcsal_reca','lkey':['co_empr','co_fond','fe_cort','sc_rol','co_rol','ti_reca'],
     'pcols':['co_empr','co_fond','fe_cort','sc_rol','co_rol','ti_reca','mo_gene']},
    {'agg':'sciRecaudacion_type','ltbl':'rctbcsci_bce','lkey':['sc_scib'],
     'pcols':['sc_scib','ci_rol','fe_gene','ho_gene','nu_envi','nu_regi_tota','mo_gene_tota']},
    {'agg':'tipoDescuento_type','ltbl':'rctbtipo_desc','lkey':['ti_desc'],
     'pcols':['ti_desc','no_tipo_desc','ds_tipo_desc','st_regi']},
    {'agg':'transaccionRecaudacion_type','ltbl':'rctbtrec','lkey':['ti_reca'],
     'pcols':['ti_reca','no_reca','co_fond','ti_proc']},
]

SPECS_F2 = [
    {'agg':'aplicacionRecaudacion_type','dest':'APLICACIONRECAUDACION_TYPE','trg':'TRG_OUTBOX_APLICACIONRECAUD',
     'opk':'ID','okeys':['ID','SECUENCIAAPLIRECA','CODIGOCEDU','TIPORECA','CODIGOROL']},
    {'agg':'caucionRecaudacion_type','dest':'CAUCIONRECAUDACION_TYPE','trg':'TRG_OUTBOX_CAUCIONRECAUD',
     'opk':'ID','okeys':['ID','TIPORECA','CODIGOEMPRESA','TIPODSTO']},
    {'agg':'devolucionRendicion_type','dest':'DEVOLUCIONRENDICION_TYPE','trg':'TRG_OUTBOX_DEVOLUCIONRENDICION',
     'opk':'ID','okeys':['ID','SECUENCIADEVO','CODIGOCEDU','TIPORECA']},
    {'agg':'estadoCuenta_type','dest':'ESTADOCUENTA_TYPE','trg':'TRG_OUTBOX_ESTADOCUENTA',
     'opk':'ID','okeys':['ID','SECUENCIACPBT','CODIGOCNTA','NUMEROCPBT']},
    {'agg':'estadoRecaudacion_type','dest':'ESTADORECAUDACION_TYPE','trg':'TRG_OUTBOX_ESTADORECAUDACION',
     'opk':'ID','okeys':['ID','ESTADORECA','DESCRIPCIONESTADO','NOMBRECORTO']},
    {'agg':'recaudacion_type','dest':'RECAUDACION_TYPE','trg':'TRG_OUTBOX_RECAUDACION',
     'opk':'ID','okeys':['ID','SECUENCIA_RECA','TIPO_RECA','ESTADO_RECA','MONTO_RECA']},
    {'agg':'rendicion_type','dest':'RENDICION_TYPE','trg':'TRG_OUTBOX_RENDICION',
     'opk':'ID','okeys':['ID','CODIGO_CEDULA','CODIGO_ROL','TIPO_RECA']},
    {'agg':'saldoRecaudacion_type','dest':'SALDORECAUDACION_TYPE','trg':'TRG_OUTBOX_SALDORECAUDACION',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOFONDO','SECUENCIAROL','CODIGOROL','TIPORECA']},
    {'agg':'sciRecaudacion_type','dest':'SCIRECAUDACION_TYPE','trg':'TRG_OUTBOX_SCIRECAUDACION',
     'opk':'ID','okeys':['ID','SECUENCIASCIB','CODIGOROL']},
    {'agg':'tipoDescuento_type','dest':'TIPODESCUENTO_TYPE','trg':'TRG_OUTBOX_TIPODESCUENTO',
     'opk':'ID','okeys':['ID','TIPODESC','NOMBRETIPODESC','DESCRIPCIONTIPODESC']},
    {'agg':'transaccionRecaudacion_type','dest':'TRANSACCIONRECAUDACION_TYPE','trg':'TRG_OUTBOX_TRANSACCIONRECAUD',
     'opk':'ID','okeys':['ID','TIPORECA','NOMBRERECA','CODIGOFONDO','TIPOPROC']},
]

def f1_trigger(s):
    trg = f"trg_outbox_{s['ltbl']}"
    if len(s['lkey']) > 1:
        agg_id_i = "CONCAT_WS('|'," + ",".join(f"CONVERT(NVARCHAR(200), i.[{k}])" for k in s['lkey']) + ")"
        agg_id_d = agg_id_i.replace("i.[", "d.[")
    else:
        agg_id_i = f"CONVERT(NVARCHAR(200), i.[{s['lkey'][0]}])"
        agg_id_d = f"CONVERT(NVARCHAR(200), d.[{s['lkey'][0]}])"
    pkmatch_i = " AND ".join(f"x.[{k}]=i.[{k}]" for k in s['lkey'])
    pkmatch_d = " AND ".join(f"x.[{k}]=d.[{k}]" for k in s['lkey'])
    pcols_q = ",".join(f"x.[{c}]" for c in s['pcols'])
    return f"""/* --- {trg}  ON dbo.{s['ltbl']}  disabled=False --- */
IF OBJECT_ID(N'dbo.{trg}', N'TR') IS NOT NULL DROP TRIGGER dbo.{trg};
GO
CREATE TRIGGER dbo.{trg}
ON dbo.[{s['ltbl']}]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;
    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;
    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_i}, N'{s['agg']}', @op,
            (SELECT {pcols_q} FROM inserted x WHERE {pkmatch_i} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_d}, N'{s['agg']}', N'DELETE',
            (SELECT {pcols_q} FROM deleted x WHERE {pkmatch_d} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM deleted d;
END
GO
"""

def f2_trigger(s):
    json_kvs_new = ", ".join(f"'{k}' VALUE :NEW.{k}" for k in s['okeys'])
    json_kvs_old = ", ".join(f"'{k}' VALUE :OLD.{k}" for k in s['okeys'])
    return f"""/* --- {s['trg']}  ON FCME_USER.{s['dest']}  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.{s['trg']}
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.{s['dest']}
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.{s['opk']});
        v_payload := JSON_OBJECT({json_kvs_new});
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.{s['opk']});
        v_payload := JSON_OBJECT({json_kvs_new});
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.{s['opk']});
        v_payload := JSON_OBJECT({json_kvs_old});
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('{s['agg']}', v_pk, v_event, v_payload, 'FCME_USER.{s['dest']}', SYSTIMESTAMP);
END;
/
"""

out = []
out.append("""/* ============================================================
   DUMP TRIGGERS RECAUDACIONES (F1 + F2) - DDL completo
   Snapshot generado a partir de _deploy_recaudaciones_flow1.py / _deploy_recaudaciones_flow2.py
   ============================================================ */

/* ############################################################
   FLUJO 1 - Recaudaciones dbRC -> fcme_canonicos.cdc_outbox
   Filtro: triggers cuya definicion publica un aggregate_type de Recaudaciones
   ############################################################ */

USE [dbRC];
GO
""")
out.append(f"/* TOTAL F1 (dbRC) Recaudaciones: {len(SPECS_F1)} triggers */\n")
for s in SPECS_F1:
    out.append(f1_trigger(s))

out.append("""
/* ############################################################
   FLUJO 2 - Recaudaciones FCME_USER -> FCME_USER.CDC_OUTBOX
   ############################################################ */
""")
out.append(f"/* TOTAL F2 (FCME_USER) Recaudaciones: {len(SPECS_F2)} triggers */\n")
for s in SPECS_F2:
    out.append(f2_trigger(s))

content = "\n".join(out)
with open("recaudaciones_triggers_dump.sql", "w", encoding="utf-8") as f:
    f.write(content)
print(f"OK - F1={len(SPECS_F1)} F2={len(SPECS_F2)} bytes={len(content)}")
