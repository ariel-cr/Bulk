CREATE OR REPLACE PROCEDURE USP_INBOX_PARTICIPE(
    p_id             IN NUMBER,
    p_aggregate_type IN VARCHAR2,
    p_event_type     IN VARCHAR2,
    p_payload        IN CLOB
) AS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

    IF p_aggregate_type = 'fctbafil_actu' THEN

        -- target: ACTUALIZACION_AFILIADO_TYPE
        BEGIN
            IF p_event_type IN ('DELETE','DELETED') THEN
                NULL; -- DELETE handler per-table, no implementado aun
            ELSE
                INSERT INTO ACTUALIZACION_AFILIADO_TYPE (CODIGO_CEDU, CODIGO_PROV, CODIGO_CANT, CODIGO_PARR, DESCRIPCION_CALL_PRIM, NUMERO_CALL_PRIM, DESCRIPCION_CALL_SECU, NUMERO_CALL_SECU, NUMERO_MANZ, NUMERO_VILL, DESCRIPCION_CDLA, TEXTO_TELF_CONVENIO, TEXTO_TELF_CELU, TIPO_OPERACION, DESCRIPCION_REFERENCIA_VIVI, TEXTO_MAIL, NOMBRE_CONTABLE_ADIC, TEXTO_TELF_CON1, TEXTO_TELF_CON2, TIPO_RELA, CODIGO_PROV_INST, CODIGO_TIPO, CODIGO_INST, CODIGO_CARG, CODIGO_NIVE, CODIGO_CATE, TIPO_CONTABLE, TIPO_JORN, CODIGO_PROV_OBSQ, CODIGO_ZONA_OBSQ, INDICADOR_RENO_CREDITO, INDICADOR_ACCI, FECHA_INGRESO, FECHA_MODIFICACION, FECHA_ULTM_ENVI, INDICADOR_IMPR_DOCUMENTO, FECHA_IMPR_DOCUMENTO, INDICADOR_COBRANZA_PRES, INDICADOR_VALD_CELU, INDICADOR_VALD_MAIL, CODIGO_AMI, ESTADO_ENTR_OBSQ, FECHA_ENTR_OBSQ, FECHA_VERI_DATO, INDICADOR_IMPR_DOCUMENTO_CRED, FECHA_IMPR_DOCUMENTO_CREDITO, NOMBRE_INST, CODIGO_CANT_INST, CODIGO_PARR_INST)
                VALUES (JSON_VALUE(p_payload,'$.ci_cedu'), JSON_VALUE(p_payload,'$.co_prov'), JSON_VALUE(p_payload,'$.co_cant'), JSON_VALUE(p_payload,'$.co_parr'), JSON_VALUE(p_payload,'$.ds_call_prim'), JSON_VALUE(p_payload,'$.nu_call_prim'), JSON_VALUE(p_payload,'$.ds_call_secu'), JSON_VALUE(p_payload,'$.nu_call_secu'), JSON_VALUE(p_payload,'$.nu_manz'), JSON_VALUE(p_payload,'$.nu_vill'), JSON_VALUE(p_payload,'$.ds_cdla'), NULL, JSON_VALUE(p_payload,'$.tx_telf_celu'), NULL, NULL, JSON_VALUE(p_payload,'$.tx_mail'), NULL, JSON_VALUE(p_payload,'$.tx_telf_con1'), JSON_VALUE(p_payload,'$.tx_telf_con2'), JSON_VALUE(p_payload,'$.ti_rela'), JSON_VALUE(p_payload,'$.co_prov_inst'), JSON_VALUE(p_payload,'$.ci_tipo'), JSON_VALUE(p_payload,'$.co_inst'), JSON_VALUE(p_payload,'$.co_carg'), JSON_VALUE(p_payload,'$.co_nive'), JSON_VALUE(p_payload,'$.co_cate'), NULL, JSON_VALUE(p_payload,'$.ti_jorn'), JSON_VALUE(p_payload,'$.co_prov_obsq'), JSON_VALUE(p_payload,'$.co_zona_obsq'), NULL, JSON_VALUE(p_payload,'$.in_acci'), NULL, NULL, JSON_VALUE(p_payload,'$.fe_ultm_envi'), NULL, NULL, NULL, JSON_VALUE(p_payload,'$.in_vald_celu'), JSON_VALUE(p_payload,'$.in_vald_mail'), JSON_VALUE(p_payload,'$.co_ami'), JSON_VALUE(p_payload,'$.st_entr_obsq'), JSON_VALUE(p_payload,'$.fe_entr_obsq'), JSON_VALUE(p_payload,'$.fe_veri_dato'), NULL, NULL, JSON_VALUE(p_payload,'$.no_inst'), JSON_VALUE(p_payload,'$.co_cant_inst'), JSON_VALUE(p_payload,'$.co_parr_inst'));
            END IF;
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'ACTUALIZACION_AFILIADO_TYPE: ' || SUBSTR(SQLERRM,1,3900));
        END;
    END IF;

    IF p_aggregate_type = 'fctbafil_info_actu_docs' THEN

        -- target: ACTUALIZACION_DOCUMENTOS_TYPE
        BEGIN
            IF p_event_type IN ('DELETE','DELETED') THEN
                NULL; -- DELETE handler per-table, no implementado aun
            ELSE
                INSERT INTO ACTUALIZACION_DOCUMENTOS_TYPE (SECUENCIA_ACTU_DOCS, CODIGO_EMPRESA, CODIGO_CEDU, SECUENCIA_ACTV_SUJE_CRED, SECUENCIA_ORGN_INGR, CODIGO_PERS_POLI_EXPU, DESCRIPCION_CIUD_NACI, INDICADOR_COMI_SERV, DESCRIPCION_COMI_SERV, FECHA_INGR, USUARIO_INGRESA)
                VALUES (JSON_VALUE(p_payload,'$.sc_actu_docs'), NULL, JSON_VALUE(p_payload,'$.co_cedu'), JSON_VALUE(p_payload,'$.sc_actv_suje_cred'), JSON_VALUE(p_payload,'$.sc_orgn_ingr'), JSON_VALUE(p_payload,'$.co_pers_poli_expu'), JSON_VALUE(p_payload,'$.ds_ciud_naci'), JSON_VALUE(p_payload,'$.in_comi_serv'), JSON_VALUE(p_payload,'$.ds_comi_serv'), JSON_VALUE(p_payload,'$.fx_ingr'), NULL);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'ACTUALIZACION_DOCUMENTOS_TYPE: ' || SUBSTR(SQLERRM,1,3900));
        END;
    END IF;

    UPDATE CDC_INBOX SET PROCESSED=1, PROCESSED_AT=SYSTIMESTAMP WHERE ID=p_id;
    COMMIT;
EXCEPTION WHEN OTHERS THEN
    INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, SUBSTR(SQLERRM,1,4000));
    COMMIT;
END;