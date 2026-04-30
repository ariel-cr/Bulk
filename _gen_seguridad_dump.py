"""Genera seguridad_triggers_dump.sql con los DDL F1+F2 a partir de los SPECS
de _deploy_seguridad_flow1.py y _deploy_seguridad_flow2.py (mismo formato que
nomina_triggers_dump.sql).
"""

SPECS_F1 = [
    {'agg':'aplicacionFuncion_type','ltbl':'sgtbapli','lkey':['co_apli','ti_loca'],
     'pcols':['co_apli','ti_loca','no_apli','st_apli']},
    {'agg':'auditoriaFlujo_type','ltbl':'sgtbtran','lkey':['co_apli','co_func','nu_tran','ti_loca'],
     'pcols':['co_apli','co_func','nu_tran','ti_loca','no_tran']},
    {'agg':'cuentaNostroType','ltbl':'sgtbcnts','lkey':['co_cnts'],
     'pcols':['co_cnts','nu_iden','no_cnts','st_cnts','ds_mail']},
    {'agg':'empresa_type','ltbl':'sgtbempr','lkey':['co_empr'],
     'pcols':['co_empr','no_empr','st_empr','nu_ruc','no_desc']},
    {'agg':'firmaSeguridad_type','ltbl':'sgtbfirm','lkey':['co_empr','co_usua','fe_firm','ho_firm'],
     'pcols':['co_empr','co_usua','no_maqu','no_usua_nt','fe_firm','ho_firm']},
    {'agg':'fondoSeguridad_type','ltbl':'sgtbfond','lkey':['co_fond'],
     'pcols':['co_fond','no_fond','st_fond','in_part','co_rubr_prim']},
    {'agg':'localidad_type','ltbl':'sgtbloca','lkey':['co_empr','co_loca'],
     'pcols':['co_empr','co_loca','no_loca','co_prov','ti_loca','ci_repr','no_repr']},
    {'agg':'parametroSeguridad_type','ltbl':'sgtbpara','lkey':['co_empr','co_para'],
     'pcols':['co_empr','co_para','no_para','va_para','in_prov']},
    {'agg':'passwordSeguridad_type','ltbl':'sgtbpass','lkey':['co_empr','co_usua','sc_pass'],
     'pcols':['co_empr','co_usua','sc_pass','ds_pass','fe_ingr','st_pass','ti_pass']},
    {'agg':'usuarioSeguridad_type','ltbl':'sgtbusua','lkey':['co_empr','co_usua'],
     'pcols':['co_empr','co_usua','no_usua','fe_ingr','fe_expi','ds_pass','nu_cedu']},
    {'agg':'usuarioServicio_type','ltbl':'sgtbconf_serv_apli','lkey':['sc_serv'],
     'pcols':['sc_serv','co_serv_apli','no_serv_apli','sc_tipo','no_usua','ds_pass','st_regi']},
]

SPECS_F2 = [
    {'agg':'aplicacionFuncion_type','dest':'APLICACIONFUNCION_TYPE','trg':'TRG_OUTBOX_APLICACIONFUNCION',
     'opk':'ID','okeys':['ID','CODIGOAPLICACION','TIPOLOCALIDAD','NOMBREAPLICACION','ESTADOAPLICACION']},
    {'agg':'auditoriaFlujo_type','dest':'AUDITORIAFLUJO_TYPE','trg':'TRG_OUTBOX_AUDITORIAFLUJO',
     'opk':'ID','okeys':['ID','CODIGOPROCESO','CODIGOSUBPROCESO']},
    {'agg':'cuentaNostroType','dest':'CUENTANOSTRO_TYPE','trg':'TRG_OUTBOX_CUENTANOSTRO',
     'opk':'ID','okeys':['ID','CODIGO','NOMBRE','ESTADO']},
    {'agg':'empresa_type','dest':'EMPRESA_TYPE','trg':'TRG_OUTBOX_EMPRESA',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','NOMBREEMPRESA','ESTADOEMPRESA']},
    {'agg':'firmaSeguridad_type','dest':'FIRMASEGURIDAD_TYPE','trg':'TRG_OUTBOX_FIRMASEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOUSUARIO','FECHAFIRMAENTRADASALIDA','HORAFIRMAENTRADASALIDA']},
    {'agg':'fondoSeguridad_type','dest':'FONDOSEGURIDAD_TYPE','trg':'TRG_OUTBOX_FONDOSEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOFONDO','NOMBREFONDO','ESTADOFONDO']},
    {'agg':'localidad_type','dest':'LOCALIDAD_TYPE','trg':'TRG_OUTBOX_LOCALIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOLOCALIDAD','CODIGOPROVINCIA','CEDULAREPRESENTANTE']},
    {'agg':'parametroSeguridad_type','dest':'PARAMETROSEGURIDAD_TYPE','trg':'TRG_OUTBOX_PARAMETROSEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOPARAMETRO','NOMBREPARAMETRO','VALORPARAMETRO']},
    {'agg':'passwordSeguridad_type','dest':'PASSWORDSEGURIDAD_TYPE','trg':'TRG_OUTBOX_PASSWORDSEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOUSUARIO','SECUENCIACAMBIOCONTRASENIA']},
    {'agg':'usuarioSeguridad_type','dest':'USUARIOSEGURIDAD_TYPE','trg':'TRG_OUTBOX_USUARIOSEGURIDAD',
     'opk':'ID','okeys':['ID','CODIGOEMPRESA','CODIGOUSUARIO']},
    {'agg':'usuarioServicio_type','dest':'USUARIOSERVICIO_TYPE','trg':'TRG_OUTBOX_USUARIOSERVICIO',
     'opk':'ID','okeys':['ID','CODIGOUSUARIO','CONTRASENIA']},
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
            N'dbSG.dbo.{s['ltbl']}', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT {agg_id_d}, N'{s['agg']}', N'DELETE',
            (SELECT {pcols_q} FROM deleted x WHERE {pkmatch_d} FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.{s['ltbl']}', SYSUTCDATETIME()
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
   DUMP TRIGGERS SEGURIDAD (F1 + F2) - DDL completo
   Snapshot generado a partir de _deploy_seguridad_flow1.py / _deploy_seguridad_flow2.py
   ============================================================ */

/* ############################################################
   FLUJO 1 - Seguridad dbSG -> fcme_canonicos.cdc_outbox
   Filtro: triggers cuya definicion publica un aggregate_type de Seguridad
   ############################################################ */

USE [dbSG];
GO
""")
out.append(f"/* TOTAL F1 (dbSG) Seguridad: {len(SPECS_F1)} triggers */\n")
for s in SPECS_F1:
    out.append(f1_trigger(s))

out.append("""
/* ############################################################
   FLUJO 2 - Seguridad FCME_USER -> FCME_USER.CDC_OUTBOX
   ############################################################ */
""")
out.append(f"/* TOTAL F2 (FCME_USER) Seguridad: {len(SPECS_F2)} triggers */\n")
for s in SPECS_F2:
    out.append(f2_trigger(s))

content = "\n".join(out)
with open("seguridad_triggers_dump.sql", "w", encoding="utf-8") as f:
    f.write(content)
print(f"OK - F1={len(SPECS_F1)} F2={len(SPECS_F2)} bytes={len(content)}")
