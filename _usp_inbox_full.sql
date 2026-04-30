CREATE OR REPLACE PROCEDURE USP_INBOX_PARTICIPES(
    p_id             IN NUMBER,
    p_aggregate_type IN VARCHAR2,
    p_source_table   IN VARCHAR2,
    p_event_type     IN VARCHAR2,
    p_payload        IN CLOB
) AS
    v_err VARCHAR2(4000);
    v_pk  VARCHAR2(200);
BEGIN


    IF p_source_table = 'fctbafil_actu' AND p_aggregate_type = 'actualizacionAfiliadoType' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload,'$.ci_cedu');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM ACTUALIZACION_AFILIADO_TYPE WHERE CODIGO_CEDU = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO ACTUALIZACION_AFILIADO_TYPE t
                USING (SELECT v_pk AS k FROM dual) s
                ON (t.CODIGO_CEDU = s.k)
                WHEN MATCHED THEN UPDATE SET CODIGO_PROV = JSON_VALUE(p_payload,'$.co_prov'), CODIGO_CANT = JSON_VALUE(p_payload,'$.co_cant'), CODIGO_PARR = JSON_VALUE(p_payload,'$.co_parr'), DESCRIPCION_CALL_PRIM = JSON_VALUE(p_payload,'$.ds_call_prim'), NUMERO_CALL_PRIM = JSON_VALUE(p_payload,'$.nu_call_prim'), DESCRIPCION_CALL_SECU = JSON_VALUE(p_payload,'$.ds_call_secu'), NUMERO_CALL_SECU = JSON_VALUE(p_payload,'$.nu_call_secu'), NUMERO_MANZ = JSON_VALUE(p_payload,'$.nu_manz'), NUMERO_VILL = JSON_VALUE(p_payload,'$.nu_vill'), DESCRIPCION_CDLA = JSON_VALUE(p_payload,'$.ds_cdla'), TEXTO_TELF_CONVENIO = NULL, TEXTO_TELF_CELU = JSON_VALUE(p_payload,'$.tx_telf_celu'), TIPO_OPERACION = NULL, DESCRIPCION_REFERENCIA_VIVI = NULL, TEXTO_MAIL = JSON_VALUE(p_payload,'$.tx_mail'), NOMBRE_CONTABLE_ADIC = NULL, TEXTO_TELF_CON1 = JSON_VALUE(p_payload,'$.tx_telf_con1'), TEXTO_TELF_CON2 = JSON_VALUE(p_payload,'$.tx_telf_con2'), TIPO_RELA = JSON_VALUE(p_payload,'$.ti_rela'), CODIGO_PROV_INST = JSON_VALUE(p_payload,'$.co_prov_inst'), CODIGO_TIPO = JSON_VALUE(p_payload,'$.ci_tipo'), CODIGO_INST = JSON_VALUE(p_payload,'$.co_inst'), CODIGO_CARG = JSON_VALUE(p_payload,'$.co_carg'), CODIGO_NIVE = JSON_VALUE(p_payload,'$.co_nive'), CODIGO_CATE = JSON_VALUE(p_payload,'$.co_cate'), TIPO_CONTABLE = NULL, TIPO_JORN = JSON_VALUE(p_payload,'$.ti_jorn'), CODIGO_PROV_OBSQ = JSON_VALUE(p_payload,'$.co_prov_obsq'), CODIGO_ZONA_OBSQ = JSON_VALUE(p_payload,'$.co_zona_obsq'), INDICADOR_RENO_CREDITO = NULL, INDICADOR_ACCI = JSON_VALUE(p_payload,'$.in_acci'), FECHA_INGRESO = NULL, FECHA_MODIFICACION = NULL, FECHA_ULTM_ENVI = JSON_VALUE(p_payload,'$.fe_ultm_envi'), INDICADOR_IMPR_DOCUMENTO = NULL, FECHA_IMPR_DOCUMENTO = NULL, INDICADOR_COBRANZA_PRES = NULL, INDICADOR_VALD_CELU = JSON_VALUE(p_payload,'$.in_vald_celu'), INDICADOR_VALD_MAIL = JSON_VALUE(p_payload,'$.in_vald_mail'), CODIGO_AMI = JSON_VALUE(p_payload,'$.co_ami'), ESTADO_ENTR_OBSQ = JSON_VALUE(p_payload,'$.st_entr_obsq'), FECHA_ENTR_OBSQ = JSON_VALUE(p_payload,'$.fe_entr_obsq'), FECHA_VERI_DATO = JSON_VALUE(p_payload,'$.fe_veri_dato'), INDICADOR_IMPR_DOCUMENTO_CRED = NULL, FECHA_IMPR_DOCUMENTO_CREDITO = NULL, NOMBRE_INST = JSON_VALUE(p_payload,'$.no_inst'), CODIGO_CANT_INST = JSON_VALUE(p_payload,'$.co_cant_inst'), CODIGO_PARR_INST = JSON_VALUE(p_payload,'$.co_parr_inst')
                WHEN NOT MATCHED THEN INSERT (CODIGO_CEDU, CODIGO_PROV, CODIGO_CANT, CODIGO_PARR, DESCRIPCION_CALL_PRIM, NUMERO_CALL_PRIM, DESCRIPCION_CALL_SECU, NUMERO_CALL_SECU, NUMERO_MANZ, NUMERO_VILL, DESCRIPCION_CDLA, TEXTO_TELF_CONVENIO, TEXTO_TELF_CELU, TIPO_OPERACION, DESCRIPCION_REFERENCIA_VIVI, TEXTO_MAIL, NOMBRE_CONTABLE_ADIC, TEXTO_TELF_CON1, TEXTO_TELF_CON2, TIPO_RELA, CODIGO_PROV_INST, CODIGO_TIPO, CODIGO_INST, CODIGO_CARG, CODIGO_NIVE, CODIGO_CATE, TIPO_CONTABLE, TIPO_JORN, CODIGO_PROV_OBSQ, CODIGO_ZONA_OBSQ, INDICADOR_RENO_CREDITO, INDICADOR_ACCI, FECHA_INGRESO, FECHA_MODIFICACION, FECHA_ULTM_ENVI, INDICADOR_IMPR_DOCUMENTO, FECHA_IMPR_DOCUMENTO, INDICADOR_COBRANZA_PRES, INDICADOR_VALD_CELU, INDICADOR_VALD_MAIL, CODIGO_AMI, ESTADO_ENTR_OBSQ, FECHA_ENTR_OBSQ, FECHA_VERI_DATO, INDICADOR_IMPR_DOCUMENTO_CRED, FECHA_IMPR_DOCUMENTO_CREDITO, NOMBRE_INST, CODIGO_CANT_INST, CODIGO_PARR_INST) VALUES (JSON_VALUE(p_payload,'$.ci_cedu'), JSON_VALUE(p_payload,'$.co_prov'), JSON_VALUE(p_payload,'$.co_cant'), JSON_VALUE(p_payload,'$.co_parr'), JSON_VALUE(p_payload,'$.ds_call_prim'), JSON_VALUE(p_payload,'$.nu_call_prim'), JSON_VALUE(p_payload,'$.ds_call_secu'), JSON_VALUE(p_payload,'$.nu_call_secu'), JSON_VALUE(p_payload,'$.nu_manz'), JSON_VALUE(p_payload,'$.nu_vill'), JSON_VALUE(p_payload,'$.ds_cdla'), NULL, JSON_VALUE(p_payload,'$.tx_telf_celu'), NULL, NULL, JSON_VALUE(p_payload,'$.tx_mail'), NULL, JSON_VALUE(p_payload,'$.tx_telf_con1'), JSON_VALUE(p_payload,'$.tx_telf_con2'), JSON_VALUE(p_payload,'$.ti_rela'), JSON_VALUE(p_payload,'$.co_prov_inst'), JSON_VALUE(p_payload,'$.ci_tipo'), JSON_VALUE(p_payload,'$.co_inst'), JSON_VALUE(p_payload,'$.co_carg'), JSON_VALUE(p_payload,'$.co_nive'), JSON_VALUE(p_payload,'$.co_cate'), NULL, JSON_VALUE(p_payload,'$.ti_jorn'), JSON_VALUE(p_payload,'$.co_prov_obsq'), JSON_VALUE(p_payload,'$.co_zona_obsq'), NULL, JSON_VALUE(p_payload,'$.in_acci'), NULL, NULL, JSON_VALUE(p_payload,'$.fe_ultm_envi'), NULL, NULL, NULL, JSON_VALUE(p_payload,'$.in_vald_celu'), JSON_VALUE(p_payload,'$.in_vald_mail'), JSON_VALUE(p_payload,'$.co_ami'), JSON_VALUE(p_payload,'$.st_entr_obsq'), JSON_VALUE(p_payload,'$.fe_entr_obsq'), JSON_VALUE(p_payload,'$.fe_veri_dato'), NULL, NULL, JSON_VALUE(p_payload,'$.no_inst'), JSON_VALUE(p_payload,'$.co_cant_inst'), JSON_VALUE(p_payload,'$.co_parr_inst'));
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'ACTUALIZACION_AFILIADO_TYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;

    IF p_source_table = 'fctbafil_info_actu_docs' AND p_aggregate_type = 'actualizacionDocumentosType' THEN
        BEGIN
            v_pk := JSON_VALUE(p_payload,'$.co_cedu');
            IF p_event_type IN ('DELETE','DELETED') THEN
                IF v_pk IS NOT NULL THEN
                    DELETE FROM ACTUALIZACION_DOCUMENTOS_TYPE WHERE CODIGO_CEDU = v_pk;
                END IF;
            ELSIF v_pk IS NOT NULL THEN
                MERGE INTO ACTUALIZACION_DOCUMENTOS_TYPE t
                USING (SELECT v_pk AS k FROM dual) s
                ON (t.CODIGO_CEDU = s.k)
                WHEN MATCHED THEN UPDATE SET SECUENCIA_ACTU_DOCS = JSON_VALUE(p_payload,'$.sc_actu_docs'), CODIGO_EMPRESA = NULL, SECUENCIA_ACTV_SUJE_CRED = JSON_VALUE(p_payload,'$.sc_actv_suje_cred'), SECUENCIA_ORGN_INGR = JSON_VALUE(p_payload,'$.sc_orgn_ingr'), CODIGO_PERS_POLI_EXPU = JSON_VALUE(p_payload,'$.co_pers_poli_expu'), DESCRIPCION_CIUD_NACI = JSON_VALUE(p_payload,'$.ds_ciud_naci'), INDICADOR_COMI_SERV = JSON_VALUE(p_payload,'$.in_comi_serv'), DESCRIPCION_COMI_SERV = JSON_VALUE(p_payload,'$.ds_comi_serv'), FECHA_INGR = JSON_VALUE(p_payload,'$.fx_ingr'), USUARIO_INGRESA = NULL
                WHEN NOT MATCHED THEN INSERT (SECUENCIA_ACTU_DOCS, CODIGO_EMPRESA, CODIGO_CEDU, SECUENCIA_ACTV_SUJE_CRED, SECUENCIA_ORGN_INGR, CODIGO_PERS_POLI_EXPU, DESCRIPCION_CIUD_NACI, INDICADOR_COMI_SERV, DESCRIPCION_COMI_SERV, FECHA_INGR, USUARIO_INGRESA) VALUES (JSON_VALUE(p_payload,'$.sc_actu_docs'), NULL, JSON_VALUE(p_payload,'$.co_cedu'), JSON_VALUE(p_payload,'$.sc_actv_suje_cred'), JSON_VALUE(p_payload,'$.sc_orgn_ingr'), JSON_VALUE(p_payload,'$.co_pers_poli_expu'), JSON_VALUE(p_payload,'$.ds_ciud_naci'), JSON_VALUE(p_payload,'$.in_comi_serv'), JSON_VALUE(p_payload,'$.ds_comi_serv'), JSON_VALUE(p_payload,'$.fx_ingr'), NULL);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_err := SQLERRM;
            INSERT INTO CDC_INBOX_ERRORS (INBOX_ID, AGGREGATE_TYPE, EVENT_TYPE, ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'ACTUALIZACION_DOCUMENTOS_TYPE: ' || SUBSTR(v_err,1,3900));
        END;
    END IF;
END;