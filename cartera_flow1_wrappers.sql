/* CARTERA Flow1 wrappers - 91 USP_INBOX_<X> Oracle */

/* abonoExtraordinario_type -> USP_INBOX_ABONOEXTRAORDINARIO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_ABONOEXTRAORDINARIO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_abno VARCHAR2(4000);
    v_co_proc VARCHAR2(4000);
    v_mo_abno_extr VARCHAR2(4000);
    v_ds_refe VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_fe_autr VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_co_usua_conf VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_abno := JSON_VALUE(p_payload, '$.sc_abno');
    v_co_proc := JSON_VALUE(p_payload, '$.co_proc');
    v_mo_abno_extr := JSON_VALUE(p_payload, '$.mo_abno_extr');
    v_ds_refe := JSON_VALUE(p_payload, '$.ds_refe');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_fe_autr := JSON_VALUE(p_payload, '$.fe_autr');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_co_usua_conf := JSON_VALUE(p_payload, '$.co_usua_conf');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.ABONOEXTRAORDINARIO_TYPE WHERE SECUENCIAABONOPROCESO=v_sc_abno;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.ABONOEXTRAORDINARIO_TYPE (SECUENCIAABONOPROCESO,CODIGOPROCESO,MONTOABONOEXTRAORDINARIO,DESCRIPCIONREFERENCIA,ESTADOREGISTRO,FECHAAUTORIZACION,FECHAVERIFICACION,FECHACREACION,FECHAELIMINACION,CODIGOUSUARIOCONFIRMA,ANIOCREDITO,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_sc_abno,v_co_proc,v_mo_abno_extr,v_ds_refe,v_st_regi,v_fe_autr,v_fe_elim,v_fe_elim,v_fe_elim,v_co_usua_conf,v_aa_cred,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.ABONOEXTRAORDINARIO_TYPE SET CODIGOPROCESO=v_co_proc, MONTOABONOEXTRAORDINARIO=v_mo_abno_extr, DESCRIPCIONREFERENCIA=v_ds_refe, ESTADOREGISTRO=v_st_regi, FECHAAUTORIZACION=v_fe_autr, FECHAVERIFICACION=v_fe_elim, FECHACREACION=v_fe_elim, FECHAELIMINACION=v_fe_elim, CODIGOUSUARIOCONFIRMA=v_co_usua_conf, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIAABONOPROCESO=v_sc_abno;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper abonoExtraordinario_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper abonoExtraordinario_type: ' || v_err);
END;
/

/* autorizacionCreditoDetalle_type -> USP_INBOX_AUTRCREDDETA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_AUTRCREDDETA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_empr VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_fe_ingr VARCHAR2(4000);
    v_fe_modi VARCHAR2(4000);
    v_sc_autr_deta VARCHAR2(4000);
    v_sc_rngo_usua VARCHAR2(4000);
    v_sc_cred_autr VARCHAR2(4000);
    v_fe_autr_deta VARCHAR2(4000);
    v_st_autr_deta VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_fe_ingr := JSON_VALUE(p_payload, '$.fe_ingr');
    v_fe_modi := JSON_VALUE(p_payload, '$.fe_modi');
    v_sc_autr_deta := JSON_VALUE(p_payload, '$.sc_autr_deta');
    v_sc_rngo_usua := JSON_VALUE(p_payload, '$.sc_rngo_usua');
    v_sc_cred_autr := JSON_VALUE(p_payload, '$.sc_cred_autr');
    v_fe_autr_deta := JSON_VALUE(p_payload, '$.fe_autr_deta');
    v_st_autr_deta := JSON_VALUE(p_payload, '$.st_autr_deta');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.AUTORIZACIONCREDITODETALLE_TYPE WHERE CODIGOEMPRESA=v_co_empr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.AUTORIZACIONCREDITODETALLE_TYPE (CODIGOEMPRESA,SECUENCIACREDITOAUTORIZACION,TIPOCREDITO,ANIOCREDITO,SECUENCIACREDITO,FECHAINGRESO,FECHAMODIFICACION,CODIGOEMPRESACREDAUTR,SECUENCIAAUTORIZACIONDETALLE,SECUENCIACREDITOAUTORIZACIONCREDAUTR,SECUENCIARNGOUSUARIO,TIPOCREDITOCREDAUTR,ANIOCREDITOCREDAUTR,SECUENCIACREDITOCREDAUTR,FECHAAUTORIZACIONDETALLE,FECHAINGRESOCREDAUTR,FECHAMODIFICACIONCREDAUTR,ESTADOAUTORIZACIONDETALLE)
            VALUES (v_co_empr,v_qs_cred,v_ti_cred,v_aa_cred,v_qs_cred,v_fe_ingr,v_fe_modi,v_co_empr,v_sc_autr_deta,v_qs_cred,v_sc_rngo_usua,v_ti_cred,v_aa_cred,v_sc_cred_autr,v_fe_autr_deta,v_fe_ingr,v_fe_modi,v_st_autr_deta);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.AUTORIZACIONCREDITODETALLE_TYPE SET SECUENCIACREDITOAUTORIZACION=v_qs_cred, TIPOCREDITO=v_ti_cred, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, FECHAINGRESO=v_fe_ingr, FECHAMODIFICACION=v_fe_modi, CODIGOEMPRESACREDAUTR=v_co_empr, SECUENCIAAUTORIZACIONDETALLE=v_sc_autr_deta, SECUENCIACREDITOAUTORIZACIONCREDAUTR=v_qs_cred, SECUENCIARNGOUSUARIO=v_sc_rngo_usua, TIPOCREDITOCREDAUTR=v_ti_cred, ANIOCREDITOCREDAUTR=v_aa_cred, SECUENCIACREDITOCREDAUTR=v_sc_cred_autr, FECHAAUTORIZACIONDETALLE=v_fe_autr_deta, FECHAINGRESOCREDAUTR=v_fe_ingr, FECHAMODIFICACIONCREDAUTR=v_fe_modi, ESTADOAUTORIZACIONDETALLE=v_st_autr_deta WHERE CODIGOEMPRESA=v_co_empr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper autorizacionCreditoDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper autorizacionCreditoDetalle_type: ' || v_err);
END;
/

/* autorizacionCredito_type -> USP_INBOX_AUTORIZACIONCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_AUTORIZACIONCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_cobr VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_co_usua_ingr VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_fe_sald_cred VARCHAR2(4000);
    v_fe_modi VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_co_usua_elim VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_co_etap VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_cobr := JSON_VALUE(p_payload, '$.sc_cobr');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_co_usua_ingr := JSON_VALUE(p_payload, '$.co_usua_ingr');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_fe_sald_cred := JSON_VALUE(p_payload, '$.fe_sald_cred');
    v_fe_modi := JSON_VALUE(p_payload, '$.fe_modi');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_co_usua_elim := JSON_VALUE(p_payload, '$.co_usua_elim');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_co_etap := JSON_VALUE(p_payload, '$.co_etap');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.AUTORIZACIONCREDITO_TYPE WHERE SECUENCIAABONO=v_sc_cobr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.AUTORIZACIONCREDITO_TYPE (SECUENCIAABONO,FECHAAUTORIZACION,FECHAVERIFICACION,CODIGOUSUARIORECEPTA,SECUENCIAAUTORIZACIONCREDITO,FECHAAUTORIZACREDITO,SECUENCIARANGOUSUARIOS,FECHACREACION,FECHAMODIFICACION,ANIOCREDITO,CODIGOUSUARIOTRANSMICION,SECUENCIACREDITO,TIPOCREDITO,CODIGOEMPRESA,CODIGOAUTORIZACION,CODIGOCUENTA,TIPOREGISTRO,CODIGOUSUARIOTRANSMISION)
            VALUES (v_sc_cobr,v_fe_elim,v_fe_elim,v_co_usua_ingr,v_qs_cred,v_fe_sald_cred,v_sc_cobr,v_fe_elim,v_fe_modi,v_aa_cred,v_co_usua_elim,v_qs_cred,v_ti_cred,v_co_empr,v_co_usua_elim,v_co_etap,v_ti_cred,v_co_usua_elim);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.AUTORIZACIONCREDITO_TYPE SET FECHAAUTORIZACION=v_fe_elim, FECHAVERIFICACION=v_fe_elim, CODIGOUSUARIORECEPTA=v_co_usua_ingr, SECUENCIAAUTORIZACIONCREDITO=v_qs_cred, FECHAAUTORIZACREDITO=v_fe_sald_cred, SECUENCIARANGOUSUARIOS=v_sc_cobr, FECHACREACION=v_fe_elim, FECHAMODIFICACION=v_fe_modi, ANIOCREDITO=v_aa_cred, CODIGOUSUARIOTRANSMICION=v_co_usua_elim, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred, CODIGOEMPRESA=v_co_empr, CODIGOAUTORIZACION=v_co_usua_elim, CODIGOCUENTA=v_co_etap, TIPOREGISTRO=v_ti_cred, CODIGOUSUARIOTRANSMISION=v_co_usua_elim WHERE SECUENCIAABONO=v_sc_cobr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper autorizacionCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper autorizacionCredito_type: ' || v_err);
END;
/

/* auxDatosCobrosAdicionalesType -> USP_INBOX_AUXDATOSCOB (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_AUXDATOSCOB(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_usua VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_usua := JSON_VALUE(p_payload, '$.co_usua');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.AUXDATOSCOBROSADICIONALESTYPE WHERE CODIGOCUENTA=v_co_usua;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.AUXDATOSCOBROSADICIONALESTYPE (CODIGOCUENTA)
            VALUES (v_co_usua);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.AUXDATOSCOBROSADICIONALESTYPE SET CODIGOCUENTA=v_co_usua WHERE CODIGOCUENTA=v_co_usua;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper auxDatosCobrosAdicionalesType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper auxDatosCobrosAdicionalesType: ' || v_err);
END;
/

/* calificacionCarteraDetalle_type -> USP_INBOX_CALFCARTDETA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CALFCARTDETA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_cred VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_mo_reve VARCHAR2(4000);
    v_mo_dvgo VARCHAR2(4000);
    v_nu_dcto VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_mo_reve := JSON_VALUE(p_payload, '$.mo_reve');
    v_mo_dvgo := JSON_VALUE(p_payload, '$.mo_dvgo');
    v_nu_dcto := JSON_VALUE(p_payload, '$.nu_dcto');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CALIFICACIONCARTERADETALLE_TYPE WHERE TIPOCREDITO=v_ti_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CALIFICACIONCARTERADETALLE_TYPE (TIPOCREDITO,ANIOCREDITO,SECUENCIACREDITO,FECHACORT,MONTOXVEN,MONTOVCDO,MONTOPROV,MONTOREVEPROV,NUMERODIASVCDO,NUMERODCTO)
            VALUES (v_ti_cred,v_aa_cred,v_qs_cred,v_fe_cort,v_mo_reve,v_mo_dvgo,v_mo_reve,v_mo_reve,v_nu_dcto,v_nu_dcto);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CALIFICACIONCARTERADETALLE_TYPE SET ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, FECHACORT=v_fe_cort, MONTOXVEN=v_mo_reve, MONTOVCDO=v_mo_dvgo, MONTOPROV=v_mo_reve, MONTOREVEPROV=v_mo_reve, NUMERODIASVCDO=v_nu_dcto, NUMERODCTO=v_nu_dcto WHERE TIPOCREDITO=v_ti_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper calificacionCarteraDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper calificacionCarteraDetalle_type: ' || v_err);
END;
/

/* calificacionCartera_type -> USP_INBOX_CALIFICACIONCARTERA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CALIFICACIONCARTERA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_cred VARCHAR2(4000);
    v_ti_calf VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_nu_dcto VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_ti_calf := JSON_VALUE(p_payload, '$.ti_calf');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_nu_dcto := JSON_VALUE(p_payload, '$.nu_dcto');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CALIFICACIONCARTERA_TYPE WHERE TIPOCREDITO=v_ti_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CALIFICACIONCARTERA_TYPE (TIPOCREDITO,TIPOCALIFICACION,TIPOCALIFICACIONHOMOLOGADO,FECHACORTE,NUMERODIASVENCIDO,ANIOCREDITO,SECUENCIACREDITO)
            VALUES (v_ti_cred,v_ti_calf,v_ti_calf,v_fe_cort,v_nu_dcto,v_aa_cred,v_qs_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CALIFICACIONCARTERA_TYPE SET TIPOCALIFICACION=v_ti_calf, TIPOCALIFICACIONHOMOLOGADO=v_ti_calf, FECHACORTE=v_fe_cort, NUMERODIASVENCIDO=v_nu_dcto, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred WHERE TIPOCREDITO=v_ti_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper calificacionCartera_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper calificacionCartera_type: ' || v_err);
END;
/

/* cancelacionCredito_type -> USP_INBOX_CANCELACIONCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CANCELACIONCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred_ante VARCHAR2(4000);
    v_mo_intr VARCHAR2(4000);
    v_mo_gast_judi VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred_ante := JSON_VALUE(p_payload, '$.ti_cred_ante');
    v_mo_intr := JSON_VALUE(p_payload, '$.mo_intr');
    v_mo_gast_judi := JSON_VALUE(p_payload, '$.mo_gast_judi');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CANCELACIONCREDITO_TYPE WHERE ANIOCREDITOACANCELAR=v_aa_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CANCELACIONCREDITO_TYPE (ANIOCREDITOACANCELAR,SECUENCIACREDITOACANCELAR,MONTOCREDITOACANCELAR,MONTOINTERES,MONTOGASTOSJUDICIALES,ANIOCREDITO,SECUENCIACREDITO,TIPOCREDITO,TIPOCREDITOAACANCELAR)
            VALUES (v_aa_cred,v_qs_cred,v_ti_cred_ante,v_mo_intr,v_mo_gast_judi,v_aa_cred,v_qs_cred,v_ti_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CANCELACIONCREDITO_TYPE SET SECUENCIACREDITOACANCELAR=v_qs_cred, MONTOCREDITOACANCELAR=v_ti_cred_ante, MONTOINTERES=v_mo_intr, MONTOGASTOSJUDICIALES=v_mo_gast_judi, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred, TIPOCREDITOAACANCELAR=v_ti_cred WHERE ANIOCREDITOACANCELAR=v_aa_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cancelacionCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cancelacionCredito_type: ' || v_err);
END;
/

/* caucionCredito_type -> USP_INBOX_CAUCIONCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CAUCIONCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_cred VARCHAR2(4000);
    v_co_medi VARCHAR2(4000);
    v_co_rubr VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_co_medi := JSON_VALUE(p_payload, '$.co_medi');
    v_co_rubr := JSON_VALUE(p_payload, '$.co_rubr');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CAUCIONCREDITO_TYPE WHERE TIPOCREDITO=v_ti_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CAUCIONCREDITO_TYPE (TIPOCREDITO,CODIGOFONDO,TIPORUBRO,CODIGORUBRO,CODIGOEMPRESA)
            VALUES (v_ti_cred,v_co_medi,v_co_rubr,v_co_rubr,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CAUCIONCREDITO_TYPE SET CODIGOFONDO=v_co_medi, TIPORUBRO=v_co_rubr, CODIGORUBRO=v_co_rubr, CODIGOEMPRESA=v_co_empr WHERE TIPOCREDITO=v_ti_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper caucionCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper caucionCredito_type: ' || v_err);
END;
/

/* cobranzaJudicialDetalle_type -> USP_INBOX_COBJUDDETA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_COBJUDDETA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_cobr_judi VARCHAR2(4000);
    v_ti_rubr_pagd VARCHAR2(4000);
    v_mo_mvto VARCHAR2(4000);
    v_mo_aplic VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_po_desc VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_cobr_judi := JSON_VALUE(p_payload, '$.sc_cobr_judi');
    v_ti_rubr_pagd := JSON_VALUE(p_payload, '$.ti_rubr_pagd');
    v_mo_mvto := JSON_VALUE(p_payload, '$.mo_mvto');
    v_mo_aplic := JSON_VALUE(p_payload, '$.mo_aplic');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_po_desc := JSON_VALUE(p_payload, '$.po_desc');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.COBRANZAJUDICIALDETALLE_TYPE WHERE SECUENCIACOBROJUDICIAL=v_sc_cobr_judi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.COBRANZAJUDICIALDETALLE_TYPE (SECUENCIACOBROJUDICIAL,TIPORUBROPAGADO,MONTOMOVIMIENTO,MONTOAPLICADO,ANIOCREDITO,PORCENTAJEDESC,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_sc_cobr_judi,v_ti_rubr_pagd,v_mo_mvto,v_mo_aplic,v_aa_cred,v_po_desc,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.COBRANZAJUDICIALDETALLE_TYPE SET TIPORUBROPAGADO=v_ti_rubr_pagd, MONTOMOVIMIENTO=v_mo_mvto, MONTOAPLICADO=v_mo_aplic, ANIOCREDITO=v_aa_cred, PORCENTAJEDESC=v_po_desc, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIACOBROJUDICIAL=v_sc_cobr_judi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cobranzaJudicialDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cobranzaJudicialDetalle_type: ' || v_err);
END;
/

/* cobranzaJudicialDistribucion_type -> USP_INBOX_COBJUDDIST (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_COBJUDDIST(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_cobr_judi VARCHAR2(4000);
    v_ti_cobr VARCHAR2(4000);
    v_ti_abno VARCHAR2(4000);
    v_ti_proc VARCHAR2(4000);
    v_co_rol VARCHAR2(4000);
    v_mo_carg VARCHAR2(4000);
    v_nu_cpbt VARCHAR2(4000);
    v_ds_url VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_fe_carg VARCHAR2(4000);
    v_fe_depo VARCHAR2(4000);
    v_fe_modi VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_fe_liqu_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_cobr_judi := JSON_VALUE(p_payload, '$.sc_cobr_judi');
    v_ti_cobr := JSON_VALUE(p_payload, '$.ti_cobr');
    v_ti_abno := JSON_VALUE(p_payload, '$.ti_abno');
    v_ti_proc := JSON_VALUE(p_payload, '$.ti_proc');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    v_mo_carg := JSON_VALUE(p_payload, '$.mo_carg');
    v_nu_cpbt := JSON_VALUE(p_payload, '$.nu_cpbt');
    v_ds_url := JSON_VALUE(p_payload, '$.ds_url');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_fe_carg := JSON_VALUE(p_payload, '$.fe_carg');
    v_fe_depo := JSON_VALUE(p_payload, '$.fe_depo');
    v_fe_modi := JSON_VALUE(p_payload, '$.fe_modi');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_fe_liqu_cred := JSON_VALUE(p_payload, '$.fe_liqu_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.COBRANZAJUDICIALDISTRIBUCION_TYPE WHERE SECUENCIACOBROJUDICIAL=v_sc_cobr_judi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.COBRANZAJUDICIALDISTRIBUCION_TYPE (SECUENCIACOBROJUDICIAL,TIPOCOBRANZA,TIPOABONO,TIPOPROCESO,CODIGOROL,MONTOCARGA,NUMEROCOMPROBANTE,URL,ESTADOREGISTRO,FECHACARGA,FECHADEPOSITO,FECHAMODIFICACION,FECHAELIMINACION,ANIOCREDITO,CODIGOEMPRESA,FECHALIQUIDACIONCREDITP,SECUENCIACREDITO,TIPOCREDITO,CODIGOCOBRANZAJUDICIALDIST)
            VALUES (v_sc_cobr_judi,v_ti_cobr,v_ti_abno,v_ti_proc,v_co_rol,v_mo_carg,v_nu_cpbt,v_ds_url,v_st_regi,v_fe_carg,v_fe_depo,v_fe_modi,v_fe_elim,v_aa_cred,v_co_empr,v_fe_liqu_cred,v_qs_cred,v_ti_cred,v_sc_cobr_judi);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.COBRANZAJUDICIALDISTRIBUCION_TYPE SET TIPOCOBRANZA=v_ti_cobr, TIPOABONO=v_ti_abno, TIPOPROCESO=v_ti_proc, CODIGOROL=v_co_rol, MONTOCARGA=v_mo_carg, NUMEROCOMPROBANTE=v_nu_cpbt, URL=v_ds_url, ESTADOREGISTRO=v_st_regi, FECHACARGA=v_fe_carg, FECHADEPOSITO=v_fe_depo, FECHAMODIFICACION=v_fe_modi, FECHAELIMINACION=v_fe_elim, ANIOCREDITO=v_aa_cred, CODIGOEMPRESA=v_co_empr, FECHALIQUIDACIONCREDITP=v_fe_liqu_cred, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred, CODIGOCOBRANZAJUDICIALDIST=v_sc_cobr_judi WHERE SECUENCIACOBROJUDICIAL=v_sc_cobr_judi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cobranzaJudicialDistribucion_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cobranzaJudicialDistribucion_type: ' || v_err);
END;
/

/* cobranzaJudicial_type -> USP_INBOX_COBRANZAJUDICIAL (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_COBRANZAJUDICIAL(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_qs_cred VARCHAR2(4000);
    v_ti_cobr VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_co_rol VARCHAR2(4000);
    v_mo_carg VARCHAR2(4000);
    v_fe_depo VARCHAR2(4000);
    v_fe_liqu_cred VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_fe_modi VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cobr := JSON_VALUE(p_payload, '$.ti_cobr');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    v_mo_carg := JSON_VALUE(p_payload, '$.mo_carg');
    v_fe_depo := JSON_VALUE(p_payload, '$.fe_depo');
    v_fe_liqu_cred := JSON_VALUE(p_payload, '$.fe_liqu_cred');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_fe_modi := JSON_VALUE(p_payload, '$.fe_modi');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.COBRANZAJUDICIAL_TYPE WHERE SECUENCIACREDITOJUDICIAL=v_qs_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.COBRANZAJUDICIAL_TYPE (SECUENCIACREDITOJUDICIAL,TIPOCOBRANZA,CODIGOETAPA,CODIGORUBRO,MONTOCOBROS,MONTOCOBRARGASTOS,FECHAGESTION,FECHASALDOSCREDITOS,FECHACREACION,FECHAMODIFICACION,FECHAELIMINACION,ANIOCREDITO,CODIGOEMPRESA,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_qs_cred,v_ti_cobr,v_co_empr,v_co_rol,v_mo_carg,v_mo_carg,v_fe_depo,v_fe_liqu_cred,v_fe_elim,v_fe_modi,v_fe_elim,v_aa_cred,v_co_empr,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.COBRANZAJUDICIAL_TYPE SET TIPOCOBRANZA=v_ti_cobr, CODIGOETAPA=v_co_empr, CODIGORUBRO=v_co_rol, MONTOCOBROS=v_mo_carg, MONTOCOBRARGASTOS=v_mo_carg, FECHAGESTION=v_fe_depo, FECHASALDOSCREDITOS=v_fe_liqu_cred, FECHACREACION=v_fe_elim, FECHAMODIFICACION=v_fe_modi, FECHAELIMINACION=v_fe_elim, ANIOCREDITO=v_aa_cred, CODIGOEMPRESA=v_co_empr, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIACREDITOJUDICIAL=v_qs_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cobranzaJudicial_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cobranzaJudicial_type: ' || v_err);
END;
/

/* conceptoGastoJudicialType -> USP_INBOX_CONCEPGSTOJUD (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CONCEPGSTOJUD(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_empr VARCHAR2(4000);
    v_co_rubr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_co_rubr := JSON_VALUE(p_payload, '$.co_rubr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CONCEPTOGASTOJUDICIALTYPE WHERE CODIGO=v_co_empr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CONCEPTOGASTOJUDICIALTYPE (CODIGO,RUBRO)
            VALUES (v_co_empr,v_co_rubr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CONCEPTOGASTOJUDICIALTYPE SET RUBRO=v_co_rubr WHERE CODIGO=v_co_empr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper conceptoGastoJudicialType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper conceptoGastoJudicialType: ' || v_err);
END;
/

/* contabilizacionCredito_type -> USP_INBOX_CONTABCRED (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CONTABCRED(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_regi VARCHAR2(4000);
    v_ds_asien_cnta VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_co_usua_elim VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_regi := JSON_VALUE(p_payload, '$.sc_regi');
    v_ds_asien_cnta := JSON_VALUE(p_payload, '$.ds_asien_cnta');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_co_usua_elim := JSON_VALUE(p_payload, '$.co_usua_elim');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CONTABILIZACIONCREDITO_TYPE WHERE SECUENCIAREGISTRO=v_sc_regi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CONTABILIZACIONCREDITO_TYPE (SECUENCIAREGISTRO,DESCRIPCIONASIENTOCONTABLE,ESTADOREGISTRO,FECHACORTE,FECHACONTABILIZACION,FECHACREACION,FECHAELIMINACION,CODIGOUSUARIOCONTABILIZACION,CODIGOEMPRESA)
            VALUES (v_sc_regi,v_ds_asien_cnta,v_st_regi,v_fe_cort,v_fe_elim,v_fe_elim,v_fe_elim,v_co_usua_elim,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CONTABILIZACIONCREDITO_TYPE SET DESCRIPCIONASIENTOCONTABLE=v_ds_asien_cnta, ESTADOREGISTRO=v_st_regi, FECHACORTE=v_fe_cort, FECHACONTABILIZACION=v_fe_elim, FECHACREACION=v_fe_elim, FECHAELIMINACION=v_fe_elim, CODIGOUSUARIOCONTABILIZACION=v_co_usua_elim, CODIGOEMPRESA=v_co_empr WHERE SECUENCIAREGISTRO=v_sc_regi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper contabilizacionCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper contabilizacionCredito_type: ' || v_err);
END;
/

/* convenioPagoCredito_type -> USP_INBOX_CONVENIOPAGOCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CONVENIOPAGOCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_conv VARCHAR2(4000);
    v_co_proc VARCHAR2(4000);
    v_mo_capi_venc VARCHAR2(4000);
    v_mo_intr_venc VARCHAR2(4000);
    v_mo_intr_mora VARCHAR2(4000);
    v_mo_cuot_inic VARCHAR2(4000);
    v_mo_cobr_gast VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_ce_esta_civi VARCHAR2(4000);
    v_st_apli_gara VARCHAR2(4000);
    v_ds_obsr VARCHAR2(4000);
    v_ds_refe VARCHAR2(4000);
    v_fe_perd_conv VARCHAR2(4000);
    v_fe_fall_afil VARCHAR2(4000);
    v_fe_autr VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_co_usua_conf VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_ce_esta_afil VARCHAR2(4000);
    v_fe_ingr_calc VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_conv := JSON_VALUE(p_payload, '$.sc_conv');
    v_co_proc := JSON_VALUE(p_payload, '$.co_proc');
    v_mo_capi_venc := JSON_VALUE(p_payload, '$.mo_capi_venc');
    v_mo_intr_venc := JSON_VALUE(p_payload, '$.mo_intr_venc');
    v_mo_intr_mora := JSON_VALUE(p_payload, '$.mo_intr_mora');
    v_mo_cuot_inic := JSON_VALUE(p_payload, '$.mo_cuot_inic');
    v_mo_cobr_gast := JSON_VALUE(p_payload, '$.mo_cobr_gast');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_ce_esta_civi := JSON_VALUE(p_payload, '$.ce_esta_civi');
    v_st_apli_gara := JSON_VALUE(p_payload, '$.st_apli_gara');
    v_ds_obsr := JSON_VALUE(p_payload, '$.ds_obsr');
    v_ds_refe := JSON_VALUE(p_payload, '$.ds_refe');
    v_fe_perd_conv := JSON_VALUE(p_payload, '$.fe_perd_conv');
    v_fe_fall_afil := JSON_VALUE(p_payload, '$.fe_fall_afil');
    v_fe_autr := JSON_VALUE(p_payload, '$.fe_autr');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_co_usua_conf := JSON_VALUE(p_payload, '$.co_usua_conf');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_ce_esta_afil := JSON_VALUE(p_payload, '$.ce_esta_afil');
    v_fe_ingr_calc := JSON_VALUE(p_payload, '$.fe_ingr_calc');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CONVENIOPAGOCREDITO_TYPE WHERE SECUENCIACONVENIO=v_sc_conv;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CONVENIOPAGOCREDITO_TYPE (SECUENCIACONVENIO,NUMERODOCUMENTOCONVENIO,CODIGOPROCESO,MONTOCAPITALVENCIDOALAFECHA,MONTOINTERESVENCIDO,MONTOINTERESMORA,MONTOCUOTAINICIAL,MONTOCOBRARGASTOS,MONTOINTERESCONVENIO,ESTADOREGISTRO,ESTADOCIVIL,INDICADORAPLICAGARANTE,DESCRIPCIONOBSERVACIONES,DESCRIPCIONREFERENCIA,FECHACONVENIOPAGO,FECHAFALLECIMIENTOAFILIADO,FECHAAUTORIZACION,FECHAVERIFICACION,FECHACREACION,FECHAELIMINACION,CODIGOUSUARIOCONFIRMA,ANIOCREDITO,CODIGOEMPRESA,ESSTADOAFILIADO,FECHAINGRCALC,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_sc_conv,v_sc_conv,v_co_proc,v_mo_capi_venc,v_mo_intr_venc,v_mo_intr_mora,v_mo_cuot_inic,v_mo_cobr_gast,v_mo_intr_venc,v_st_regi,v_ce_esta_civi,v_st_apli_gara,v_ds_obsr,v_ds_refe,v_fe_perd_conv,v_fe_fall_afil,v_fe_autr,v_fe_elim,v_fe_elim,v_fe_elim,v_co_usua_conf,v_aa_cred,v_co_empr,v_ce_esta_afil,v_fe_ingr_calc,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CONVENIOPAGOCREDITO_TYPE SET NUMERODOCUMENTOCONVENIO=v_sc_conv, CODIGOPROCESO=v_co_proc, MONTOCAPITALVENCIDOALAFECHA=v_mo_capi_venc, MONTOINTERESVENCIDO=v_mo_intr_venc, MONTOINTERESMORA=v_mo_intr_mora, MONTOCUOTAINICIAL=v_mo_cuot_inic, MONTOCOBRARGASTOS=v_mo_cobr_gast, MONTOINTERESCONVENIO=v_mo_intr_venc, ESTADOREGISTRO=v_st_regi, ESTADOCIVIL=v_ce_esta_civi, INDICADORAPLICAGARANTE=v_st_apli_gara, DESCRIPCIONOBSERVACIONES=v_ds_obsr, DESCRIPCIONREFERENCIA=v_ds_refe, FECHACONVENIOPAGO=v_fe_perd_conv, FECHAFALLECIMIENTOAFILIADO=v_fe_fall_afil, FECHAAUTORIZACION=v_fe_autr, FECHAVERIFICACION=v_fe_elim, FECHACREACION=v_fe_elim, FECHAELIMINACION=v_fe_elim, CODIGOUSUARIOCONFIRMA=v_co_usua_conf, ANIOCREDITO=v_aa_cred, CODIGOEMPRESA=v_co_empr, ESSTADOAFILIADO=v_ce_esta_afil, FECHAINGRCALC=v_fe_ingr_calc, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIACONVENIO=v_sc_conv;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper convenioPagoCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper convenioPagoCredito_type: ' || v_err);
END;
/

/* costoFinancieroCredito_type -> USP_INBOX_COSTOFINCRED (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_COSTOFINCRED(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_prea VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_mo_cred VARCHAR2(4000);
    v_ti_cred_cncd VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_prea := JSON_VALUE(p_payload, '$.sc_prea');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_mo_cred := JSON_VALUE(p_payload, '$.mo_cred');
    v_ti_cred_cncd := JSON_VALUE(p_payload, '$.ti_cred_cncd');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.COSTOFINANCIEROCREDITO_TYPE WHERE SECUENCIAABONOPROCESO=v_sc_prea;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.COSTOFINANCIEROCREDITO_TYPE (SECUENCIAABONOPROCESO,ESTADOREGISTRO,ANIOCREDITO,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_sc_prea,v_st_regi,v_mo_cred,v_sc_prea,v_ti_cred_cncd);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.COSTOFINANCIEROCREDITO_TYPE SET ESTADOREGISTRO=v_st_regi, ANIOCREDITO=v_mo_cred, SECUENCIACREDITO=v_sc_prea, TIPOCREDITO=v_ti_cred_cncd WHERE SECUENCIAABONOPROCESO=v_sc_prea;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper costoFinancieroCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper costoFinancieroCredito_type: ' || v_err);
END;
/

/* creditoType -> USP_INBOX_CREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_calf VARCHAR2(4000);
    v_co_rol VARCHAR2(4000);
    v_fe_aprb VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_mo_suel_liqd VARCHAR2(4000);
    v_ds_oper VARCHAR2(4000);
    v_co_usua_elim VARCHAR2(4000);
    v_co_usua_ingr VARCHAR2(4000);
    v_mo_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_calf := JSON_VALUE(p_payload, '$.ti_calf');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    v_fe_aprb := JSON_VALUE(p_payload, '$.fe_aprb');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_mo_suel_liqd := JSON_VALUE(p_payload, '$.mo_suel_liqd');
    v_ds_oper := JSON_VALUE(p_payload, '$.ds_oper');
    v_co_usua_elim := JSON_VALUE(p_payload, '$.co_usua_elim');
    v_co_usua_ingr := JSON_VALUE(p_payload, '$.co_usua_ingr');
    v_mo_cred := JSON_VALUE(p_payload, '$.mo_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CREDITOTYPE WHERE TIPOIDENTIFICACION=v_ti_calf;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CREDITOTYPE (TIPOIDENTIFICACION,IDENTIFICACION,CODIGOSUCURSAL,CODIGOPRODUCTO,FECHAAPERTURA,FECHACANCELACION,FECHAVENCIMIENTO,MONTO,FECHAEMISION,CODIGOCALIFICACIONCREDITO,FECHACALIFICACION,PORCENTAJECALIFICACION,CODIGOTIPOOPERACION,CODIGOPAISINVERSION,CODIGOSUCURSALINGRESO,CODIGOOFICINAINGRESO,CODIGOUSUARIOINGRESO,CODIGOSEGMENTOCREDITO)
            VALUES (v_ti_calf,v_ti_calf,v_co_rol,v_co_rol,v_fe_aprb,v_fe_elim,v_fe_elim,v_mo_suel_liqd,v_fe_elim,v_ti_calf,v_fe_elim,v_ti_calf,v_ds_oper,v_co_usua_elim,v_co_usua_ingr,v_co_usua_ingr,v_co_usua_ingr,v_mo_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CREDITOTYPE SET IDENTIFICACION=v_ti_calf, CODIGOSUCURSAL=v_co_rol, CODIGOPRODUCTO=v_co_rol, FECHAAPERTURA=v_fe_aprb, FECHACANCELACION=v_fe_elim, FECHAVENCIMIENTO=v_fe_elim, MONTO=v_mo_suel_liqd, FECHAEMISION=v_fe_elim, CODIGOCALIFICACIONCREDITO=v_ti_calf, FECHACALIFICACION=v_fe_elim, PORCENTAJECALIFICACION=v_ti_calf, CODIGOTIPOOPERACION=v_ds_oper, CODIGOPAISINVERSION=v_co_usua_elim, CODIGOSUCURSALINGRESO=v_co_usua_ingr, CODIGOOFICINAINGRESO=v_co_usua_ingr, CODIGOUSUARIOINGRESO=v_co_usua_ingr, CODIGOSEGMENTOCREDITO=v_mo_cred WHERE TIPOIDENTIFICACION=v_ti_calf;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper creditoType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper creditoType: ' || v_err);
END;
/

/* cuentaAutomaticaDetalle_type -> USP_INBOX_CTAAUTODETA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CTAAUTODETA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_empr VARCHAR2(4000);
    v_co_fond VARCHAR2(4000);
    v_co_cnta_auto VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_co_fond := JSON_VALUE(p_payload, '$.co_fond');
    v_co_cnta_auto := JSON_VALUE(p_payload, '$.co_cnta_auto');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUENTAAUTOMATICADETALLE_TYPE WHERE CODIGOEMPRESA=v_co_empr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUENTAAUTOMATICADETALLE_TYPE (CODIGOEMPRESA,CODIGOFONDO,CODIGOCUENTAAUTOXVEN,CODIGOCUENTAAUTOVENC,CODIGOEMPRESACAUTTRUB)
            VALUES (v_co_empr,v_co_fond,v_co_cnta_auto,v_co_cnta_auto,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUENTAAUTOMATICADETALLE_TYPE SET CODIGOFONDO=v_co_fond, CODIGOCUENTAAUTOXVEN=v_co_cnta_auto, CODIGOCUENTAAUTOVENC=v_co_cnta_auto, CODIGOEMPRESACAUTTRUB=v_co_empr WHERE CODIGOEMPRESA=v_co_empr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuentaAutomaticaDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuentaAutomaticaDetalle_type: ' || v_err);
END;
/

/* cuentaAutomatica_type -> USP_INBOX_CUENTAAUTOMATICA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CUENTAAUTOMATICA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_fond VARCHAR2(4000);
    v_co_prod VARCHAR2(4000);
    v_co_cnta_auto VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_fond := JSON_VALUE(p_payload, '$.co_fond');
    v_co_prod := JSON_VALUE(p_payload, '$.co_prod');
    v_co_cnta_auto := JSON_VALUE(p_payload, '$.co_cnta_auto');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUENTAAUTOMATICA_TYPE WHERE CODIGOFONDO=v_co_fond;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUENTAAUTOMATICA_TYPE (CODIGOFONDO,CODIGODOCUMENTO,CODIGOCUENTA,CODIGOCUENTAAUTOMATICAVENCIDA,VARIABLECUENTAAUTOMATICA,CODIGOEMPRESA)
            VALUES (v_co_fond,v_co_prod,v_co_cnta_auto,v_co_cnta_auto,v_co_cnta_auto,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUENTAAUTOMATICA_TYPE SET CODIGODOCUMENTO=v_co_prod, CODIGOCUENTA=v_co_cnta_auto, CODIGOCUENTAAUTOMATICAVENCIDA=v_co_cnta_auto, VARIABLECUENTAAUTOMATICA=v_co_cnta_auto, CODIGOEMPRESA=v_co_empr WHERE CODIGOFONDO=v_co_fond;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuentaAutomatica_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuentaAutomatica_type: ' || v_err);
END;
/

/* cuentaCuotasType -> USP_INBOX_CUENTACUOTAS (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CUENTACUOTAS(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_nu_anio VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_fe_proc VARCHAR2(4000);
    v_fe_conf VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_nu_anio := JSON_VALUE(p_payload, '$.nu_anio');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_fe_proc := JSON_VALUE(p_payload, '$.fe_proc');
    v_fe_conf := JSON_VALUE(p_payload, '$.fe_conf');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUENTACUOTASTYPE WHERE NUMERODIASCALENDARIO=v_nu_anio;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUENTACUOTASTYPE (NUMERODIASCALENDARIO,FECHAVENCIMIENTO,FECHAPAGO,FECHAABONO,FECHAINICIO)
            VALUES (v_nu_anio,v_fe_elim,v_fe_proc,v_fe_conf,v_fe_elim);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUENTACUOTASTYPE SET FECHAVENCIMIENTO=v_fe_elim, FECHAPAGO=v_fe_proc, FECHAABONO=v_fe_conf, FECHAINICIO=v_fe_elim WHERE NUMERODIASCALENDARIO=v_nu_anio;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuentaCuotasType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuentaCuotasType: ' || v_err);
END;
/

/* cuentaCxPCxCType -> USP_INBOX_CUENTACXPCXC (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CUENTACXPCXC(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_empr VARCHAR2(4000);
    v_co_usua_veri VARCHAR2(4000);
    v_co_usua_ingr VARCHAR2(4000);
    v_co_usua_elim VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_co_usua_veri := JSON_VALUE(p_payload, '$.co_usua_veri');
    v_co_usua_ingr := JSON_VALUE(p_payload, '$.co_usua_ingr');
    v_co_usua_elim := JSON_VALUE(p_payload, '$.co_usua_elim');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUENTACXPCXCTYPE WHERE CODIGOSUCURSAL=v_co_empr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUENTACXPCXCTYPE (CODIGOSUCURSAL,CODIGOUSUARIOOFICIAL,CODIGOMONEDA,CODIGOUSUARIOINGRESO,CODIGOUSUARIOMODIFICACION)
            VALUES (v_co_empr,v_co_usua_veri,v_co_empr,v_co_usua_ingr,v_co_usua_elim);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUENTACXPCXCTYPE SET CODIGOUSUARIOOFICIAL=v_co_usua_veri, CODIGOMONEDA=v_co_empr, CODIGOUSUARIOINGRESO=v_co_usua_ingr, CODIGOUSUARIOMODIFICACION=v_co_usua_elim WHERE CODIGOSUCURSAL=v_co_empr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuentaCxPCxCType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuentaCxPCxCType: ' || v_err);
END;
/

/* cuentaPersonasType -> USP_INBOX_CUENTAPERSONAS (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CUENTAPERSONAS(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_grup_cnta VARCHAR2(4000);
    v_co_tipo_tran VARCHAR2(4000);
    v_nu_prio VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_grup_cnta := JSON_VALUE(p_payload, '$.co_grup_cnta');
    v_co_tipo_tran := JSON_VALUE(p_payload, '$.co_tipo_tran');
    v_nu_prio := JSON_VALUE(p_payload, '$.nu_prio');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUENTAPERSONASTYPE WHERE CODIGOCUENTA=v_co_grup_cnta;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUENTAPERSONASTYPE (CODIGOCUENTA,CODIGOTIPOIDENTIFICACION,NUMERODIRECCION)
            VALUES (v_co_grup_cnta,v_co_tipo_tran,v_nu_prio);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUENTAPERSONASTYPE SET CODIGOTIPOIDENTIFICACION=v_co_tipo_tran, NUMERODIRECCION=v_nu_prio WHERE CODIGOCUENTA=v_co_grup_cnta;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuentaPersonasType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuentaPersonasType: ' || v_err);
END;
/

/* cuentaPorCobrarType -> USP_INBOX_CUENTAPORCOBRAR (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CUENTAPORCOBRAR(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_cnta_cble VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_cnta_cble := JSON_VALUE(p_payload, '$.co_cnta_cble');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUENTAPORCOBRARTYPE WHERE CUENTA=v_co_cnta_cble;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUENTAPORCOBRARTYPE (CUENTA)
            VALUES (v_co_cnta_cble);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUENTAPORCOBRARTYPE SET CUENTA=v_co_cnta_cble WHERE CUENTA=v_co_cnta_cble;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuentaPorCobrarType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuentaPorCobrarType: ' || v_err);
END;
/

/* cuentaType -> USP_INBOX_CUENTA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CUENTA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_empr VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_co_usua_elim VARCHAR2(4000);
    v_co_usua_ingr VARCHAR2(4000);
    v_fe_proc VARCHAR2(4000);
    v_co_usua_veri VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_co_usua_elim := JSON_VALUE(p_payload, '$.co_usua_elim');
    v_co_usua_ingr := JSON_VALUE(p_payload, '$.co_usua_ingr');
    v_fe_proc := JSON_VALUE(p_payload, '$.fe_proc');
    v_co_usua_veri := JSON_VALUE(p_payload, '$.co_usua_veri');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUENTATYPE WHERE CODIGOSUCURSAL=v_co_empr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUENTATYPE (CODIGOSUCURSAL,FECHACANCELACION,CODIGOCALIFICACIONCREDITO,CODIGOUSUARIOINGRESO,FECHAVENCIMIENTO,CODIGOFRECUENCIAINTERES,FECHACASTIGO,CODIGOPAISINVERSION,FECHACALIFICACION)
            VALUES (v_co_empr,v_fe_elim,v_co_usua_elim,v_co_usua_ingr,v_fe_elim,v_co_usua_ingr,v_fe_proc,v_co_usua_veri,v_fe_elim);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUENTATYPE SET FECHACANCELACION=v_fe_elim, CODIGOCALIFICACIONCREDITO=v_co_usua_elim, CODIGOUSUARIOINGRESO=v_co_usua_ingr, FECHAVENCIMIENTO=v_fe_elim, CODIGOFRECUENCIAINTERES=v_co_usua_ingr, FECHACASTIGO=v_fe_proc, CODIGOPAISINVERSION=v_co_usua_veri, FECHACALIFICACION=v_fe_elim WHERE CODIGOSUCURSAL=v_co_empr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuentaType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuentaType: ' || v_err);
END;
/

/* cuentasEnLegalType -> USP_INBOX_CUENTASENLEGAL (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CUENTASENLEGAL(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_mo_desc VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_mo_desc := JSON_VALUE(p_payload, '$.mo_desc');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUENTASENLEGALTYPE WHERE MONTODEMANDA=v_mo_desc;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUENTASENLEGALTYPE (MONTODEMANDA)
            VALUES (v_mo_desc);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUENTASENLEGALTYPE SET MONTODEMANDA=v_mo_desc WHERE MONTODEMANDA=v_mo_desc;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuentasEnLegalType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuentasEnLegalType: ' || v_err);
END;
/

/* cuotaConvenio_type -> USP_INBOX_CUOTACONVENIO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CUOTACONVENIO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_conv VARCHAR2(4000);
    v_sc_dcto VARCHAR2(4000);
    v_sc_rol VARCHAR2(4000);
    v_st_cred VARCHAR2(4000);
    v_st_vcto VARCHAR2(4000);
    v_fe_intr VARCHAR2(4000);
    v_fe_inic_venc VARCHAR2(4000);
    v_fe_vcto VARCHAR2(4000);
    v_fe_ultm_envi VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_mo_cuot VARCHAR2(4000);
    v_mo_capi VARCHAR2(4000);
    v_mo_intr VARCHAR2(4000);
    v_mo_dvdo VARCHAR2(4000);
    v_mo_segu VARCHAR2(4000);
    v_mo_incd VARCHAR2(4000);
    v_mo_comi VARCHAR2(4000);
    v_mo_gast_judi VARCHAR2(4000);
    v_mo_inte_pmes VARCHAR2(4000);
    v_mo_abno_mora VARCHAR2(4000);
    v_mo_dvgd_intr VARCHAR2(4000);
    v_mo_dvgd_mora VARCHAR2(4000);
    v_mo_dvgo_acum VARCHAR2(4000);
    v_in_reve_dvgo VARCHAR2(4000);
    v_nu_anos VARCHAR2(4000);
    v_nu_dias VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_mo_dvgo_diar VARCHAR2(4000);
    v_mo_abno_capi VARCHAR2(4000);
    v_mo_abno_intr VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_conv := JSON_VALUE(p_payload, '$.sc_conv');
    v_sc_dcto := JSON_VALUE(p_payload, '$.sc_dcto');
    v_sc_rol := JSON_VALUE(p_payload, '$.sc_rol');
    v_st_cred := JSON_VALUE(p_payload, '$.st_cred');
    v_st_vcto := JSON_VALUE(p_payload, '$.st_vcto');
    v_fe_intr := JSON_VALUE(p_payload, '$.fe_intr');
    v_fe_inic_venc := JSON_VALUE(p_payload, '$.fe_inic_venc');
    v_fe_vcto := JSON_VALUE(p_payload, '$.fe_vcto');
    v_fe_ultm_envi := JSON_VALUE(p_payload, '$.fe_ultm_envi');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_mo_cuot := JSON_VALUE(p_payload, '$.mo_cuot');
    v_mo_capi := JSON_VALUE(p_payload, '$.mo_capi');
    v_mo_intr := JSON_VALUE(p_payload, '$.mo_intr');
    v_mo_dvdo := JSON_VALUE(p_payload, '$.mo_dvdo');
    v_mo_segu := JSON_VALUE(p_payload, '$.mo_segu');
    v_mo_incd := JSON_VALUE(p_payload, '$.mo_incd');
    v_mo_comi := JSON_VALUE(p_payload, '$.mo_comi');
    v_mo_gast_judi := JSON_VALUE(p_payload, '$.mo_gast_judi');
    v_mo_inte_pmes := JSON_VALUE(p_payload, '$.mo_inte_pmes');
    v_mo_abno_mora := JSON_VALUE(p_payload, '$.mo_abno_mora');
    v_mo_dvgd_intr := JSON_VALUE(p_payload, '$.mo_dvgd_intr');
    v_mo_dvgd_mora := JSON_VALUE(p_payload, '$.mo_dvgd_mora');
    v_mo_dvgo_acum := JSON_VALUE(p_payload, '$.mo_dvgo_acum');
    v_in_reve_dvgo := JSON_VALUE(p_payload, '$.in_reve_dvgo');
    v_nu_anos := JSON_VALUE(p_payload, '$.nu_anos');
    v_nu_dias := JSON_VALUE(p_payload, '$.nu_dias');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_mo_dvgo_diar := JSON_VALUE(p_payload, '$.mo_dvgo_diar');
    v_mo_abno_capi := JSON_VALUE(p_payload, '$.mo_abno_capi');
    v_mo_abno_intr := JSON_VALUE(p_payload, '$.mo_abno_intr');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUOTACONVENIO_TYPE WHERE SECUENCIACONVENIO=v_sc_conv;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUOTACONVENIO_TYPE (SECUENCIACONVENIO,SECUENCIADOCUMENTO,NUMERODOCUMENTOCONVENIO,SECUENCIAROL,ESTADOCREDITO,ESTADOVENCIMIENTO,FECHAINICIOINTERES,FECHAINICVENC,FECHAVENCIMIENTO,FECHAULTIMOENVIO,FECHAELIMINACION,MONTOCUOTA,MONTOCAPITAL,MONTOINTERES,MONTODIVIDENDO,MONTOSEGURO,MONTOSEGUROVEHICULO,MONTOCOBROINCENDIO,MONTOCOMISION,MONTOCOSTOSEMISION,MONTOGASTOSJUDICIALES,MONTOINTERESPRIMERMES,MONTOABONOMORA,MONTODEVENGADOINTERES,MONTODEVENGADOMORA,MONTODEVENGOACUMULADO,INDICADORREVERSODEVENGO,NUMEROANIOS,NUMERODIAS,ANIOCREDITO,MOABONOMORASOLCA,MODEVENGODIARIO,MONTOABONOCAPITALCAPITAL,MONTOABONOINTERESCAPITAL,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_sc_conv,v_sc_dcto,v_sc_conv,v_sc_rol,v_st_cred,v_st_vcto,v_fe_intr,v_fe_inic_venc,v_fe_vcto,v_fe_ultm_envi,v_fe_elim,v_mo_cuot,v_mo_capi,v_mo_intr,v_mo_dvdo,v_mo_segu,v_mo_segu,v_mo_incd,v_mo_comi,v_mo_comi,v_mo_gast_judi,v_mo_inte_pmes,v_mo_abno_mora,v_mo_dvgd_intr,v_mo_dvgd_mora,v_mo_dvgo_acum,v_in_reve_dvgo,v_nu_anos,v_nu_dias,v_aa_cred,v_mo_abno_mora,v_mo_dvgo_diar,v_mo_abno_capi,v_mo_abno_intr,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUOTACONVENIO_TYPE SET SECUENCIADOCUMENTO=v_sc_dcto, NUMERODOCUMENTOCONVENIO=v_sc_conv, SECUENCIAROL=v_sc_rol, ESTADOCREDITO=v_st_cred, ESTADOVENCIMIENTO=v_st_vcto, FECHAINICIOINTERES=v_fe_intr, FECHAINICVENC=v_fe_inic_venc, FECHAVENCIMIENTO=v_fe_vcto, FECHAULTIMOENVIO=v_fe_ultm_envi, FECHAELIMINACION=v_fe_elim, MONTOCUOTA=v_mo_cuot, MONTOCAPITAL=v_mo_capi, MONTOINTERES=v_mo_intr, MONTODIVIDENDO=v_mo_dvdo, MONTOSEGURO=v_mo_segu, MONTOSEGUROVEHICULO=v_mo_segu, MONTOCOBROINCENDIO=v_mo_incd, MONTOCOMISION=v_mo_comi, MONTOCOSTOSEMISION=v_mo_comi, MONTOGASTOSJUDICIALES=v_mo_gast_judi, MONTOINTERESPRIMERMES=v_mo_inte_pmes, MONTOABONOMORA=v_mo_abno_mora, MONTODEVENGADOINTERES=v_mo_dvgd_intr, MONTODEVENGADOMORA=v_mo_dvgd_mora, MONTODEVENGOACUMULADO=v_mo_dvgo_acum, INDICADORREVERSODEVENGO=v_in_reve_dvgo, NUMEROANIOS=v_nu_anos, NUMERODIAS=v_nu_dias, ANIOCREDITO=v_aa_cred, MOABONOMORASOLCA=v_mo_abno_mora, MODEVENGODIARIO=v_mo_dvgo_diar, MONTOABONOCAPITALCAPITAL=v_mo_abno_capi, MONTOABONOINTERESCAPITAL=v_mo_abno_intr, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIACONVENIO=v_sc_conv;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuotaConvenio_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuotaConvenio_type: ' || v_err);
END;
/

/* cuotaCreditoType -> USP_INBOX_CUOTACREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_CUOTACREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_usua VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_fe_ingr VARCHAR2(4000);
    v_co_grup VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_usua := JSON_VALUE(p_payload, '$.co_usua');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_fe_ingr := JSON_VALUE(p_payload, '$.fe_ingr');
    v_co_grup := JSON_VALUE(p_payload, '$.co_grup');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.CUOTACREDITOTYPE WHERE CODIGOCUOTA=v_co_usua;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.CUOTACREDITOTYPE (CODIGOCUOTA,CODIGOCUENTA,FECHAINICIO,FECHAVENCIMIENTO,FECHAPAGO,CODIGOGRUPOBALANCEDESGRAVAMEN,CODIGOGRUPOBALANCEINCENDIO,CODIGOGRUPOBALANCETASAMORA)
            VALUES (v_co_usua,v_co_usua,v_fe_elim,v_fe_elim,v_fe_ingr,v_co_grup,v_co_grup,v_co_grup);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.CUOTACREDITOTYPE SET CODIGOCUENTA=v_co_usua, FECHAINICIO=v_fe_elim, FECHAVENCIMIENTO=v_fe_elim, FECHAPAGO=v_fe_ingr, CODIGOGRUPOBALANCEDESGRAVAMEN=v_co_grup, CODIGOGRUPOBALANCEINCENDIO=v_co_grup, CODIGOGRUPOBALANCETASAMORA=v_co_grup WHERE CODIGOCUOTA=v_co_usua;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper cuotaCreditoType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper cuotaCreditoType: ' || v_err);
END;
/

/* desembolsoCredito_type -> USP_INBOX_DESEMBOLSOCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_DESEMBOLSOCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_comb VARCHAR2(4000);
    v_co_rol VARCHAR2(4000);
    v_nu_plzo VARCHAR2(4000);
    v_fe_ingr VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_co_usua_aprb VARCHAR2(4000);
    v_mo_cred VARCHAR2(4000);
    v_ds_mail VARCHAR2(4000);
    v_sc_prea VARCHAR2(4000);
    v_ti_cred_cncd VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_comb := JSON_VALUE(p_payload, '$.co_comb');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    v_nu_plzo := JSON_VALUE(p_payload, '$.nu_plzo');
    v_fe_ingr := JSON_VALUE(p_payload, '$.fe_ingr');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_co_usua_aprb := JSON_VALUE(p_payload, '$.co_usua_aprb');
    v_mo_cred := JSON_VALUE(p_payload, '$.mo_cred');
    v_ds_mail := JSON_VALUE(p_payload, '$.ds_mail');
    v_sc_prea := JSON_VALUE(p_payload, '$.sc_prea');
    v_ti_cred_cncd := JSON_VALUE(p_payload, '$.ti_cred_cncd');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.DESEMBOLSOCREDITO_TYPE WHERE CODIGOBANCO=v_co_comb;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.DESEMBOLSOCREDITO_TYPE (CODIGOBANCO,CODIGOPROVEEDOR,NUMEROORDEN,FECHAPAGO,FECHACREACION,FECHAELIMINACION,CODIGOUSUARIO,ANIOCREDITO,DESCRIPCIONPAGODESEMBOLSO,SECUENCIACREDITO,TIPOCREDITO,DESCRIPCIONPAGO)
            VALUES (v_co_comb,v_co_rol,v_nu_plzo,v_fe_ingr,v_fe_elim,v_fe_elim,v_co_usua_aprb,v_mo_cred,v_ds_mail,v_sc_prea,v_ti_cred_cncd,v_ds_mail);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.DESEMBOLSOCREDITO_TYPE SET CODIGOPROVEEDOR=v_co_rol, NUMEROORDEN=v_nu_plzo, FECHAPAGO=v_fe_ingr, FECHACREACION=v_fe_elim, FECHAELIMINACION=v_fe_elim, CODIGOUSUARIO=v_co_usua_aprb, ANIOCREDITO=v_mo_cred, DESCRIPCIONPAGODESEMBOLSO=v_ds_mail, SECUENCIACREDITO=v_sc_prea, TIPOCREDITO=v_ti_cred_cncd, DESCRIPCIONPAGO=v_ds_mail WHERE CODIGOBANCO=v_co_comb;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper desembolsoCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper desembolsoCredito_type: ' || v_err);
END;
/

/* desembolsoDevolucion_type -> USP_INBOX_DESEMBOLSODEVOLUCION (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_DESEMBOLSODEVOLUCION(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_devo VARCHAR2(4000);
    v_aa_devo VARCHAR2(4000);
    v_qs_devo VARCHAR2(4000);
    v_qs_dbso VARCHAR2(4000);
    v_co_tord VARCHAR2(4000);
    v_co_bnco VARCHAR2(4000);
    v_co_bnco_acre VARCHAR2(4000);
    v_ti_cnta VARCHAR2(4000);
    v_nu_cnta VARCHAR2(4000);
    v_co_bene VARCHAR2(4000);
    v_no_bene VARCHAR2(4000);
    v_st_dbso VARCHAR2(4000);
    v_mo_dbso VARCHAR2(4000);
    v_nu_orde VARCHAR2(4000);
    v_fe_pago VARCHAR2(4000);
    v_co_usua VARCHAR2(4000);
    v_ds_pago VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_devo := JSON_VALUE(p_payload, '$.ti_devo');
    v_aa_devo := JSON_VALUE(p_payload, '$.aa_devo');
    v_qs_devo := JSON_VALUE(p_payload, '$.qs_devo');
    v_qs_dbso := JSON_VALUE(p_payload, '$.qs_dbso');
    v_co_tord := JSON_VALUE(p_payload, '$.co_tord');
    v_co_bnco := JSON_VALUE(p_payload, '$.co_bnco');
    v_co_bnco_acre := JSON_VALUE(p_payload, '$.co_bnco_acre');
    v_ti_cnta := JSON_VALUE(p_payload, '$.ti_cnta');
    v_nu_cnta := JSON_VALUE(p_payload, '$.nu_cnta');
    v_co_bene := JSON_VALUE(p_payload, '$.co_bene');
    v_no_bene := JSON_VALUE(p_payload, '$.no_bene');
    v_st_dbso := JSON_VALUE(p_payload, '$.st_dbso');
    v_mo_dbso := JSON_VALUE(p_payload, '$.mo_dbso');
    v_nu_orde := JSON_VALUE(p_payload, '$.nu_orde');
    v_fe_pago := JSON_VALUE(p_payload, '$.fe_pago');
    v_co_usua := JSON_VALUE(p_payload, '$.co_usua');
    v_ds_pago := JSON_VALUE(p_payload, '$.ds_pago');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.DESEMBOLSODEVOLUCION_TYPE WHERE TIPODEVOLUCION=v_ti_devo;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.DESEMBOLSODEVOLUCION_TYPE (TIPODEVOLUCION,ANIODEVOLUCION,SECUENCIADEVOLUCION,SECUENCIADESEMBOLSO,CODIGOTORD,CODIGOBNCO,CODIGOBNCOACRE,TIPOCUENTA,NUMEROCUENTA,CODIGOBENE,NOMBREBENE,ESTADODESEMBOLSO,MONTODESEMBOLSO,NUMEROORDE,FECHAPAGO,CODIGOUSUARIO,DESCRIPCIONPAGO,TIPODEVOLUCIONDDEVO,ANIODEVOLUCIONDDEVO,SECUENCIADEVOLUCIONDDEVO,CODIGOROL,SECUENCIAROL,SECUENCIASOBRANTE)
            VALUES (v_ti_devo,v_aa_devo,v_qs_devo,v_qs_dbso,v_co_tord,v_co_bnco,v_co_bnco_acre,v_ti_cnta,v_nu_cnta,v_co_bene,v_no_bene,v_st_dbso,v_mo_dbso,v_nu_orde,v_fe_pago,v_co_usua,v_ds_pago,v_ti_devo,v_aa_devo,v_qs_devo,v_co_tord,v_qs_dbso,v_qs_dbso);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.DESEMBOLSODEVOLUCION_TYPE SET ANIODEVOLUCION=v_aa_devo, SECUENCIADEVOLUCION=v_qs_devo, SECUENCIADESEMBOLSO=v_qs_dbso, CODIGOTORD=v_co_tord, CODIGOBNCO=v_co_bnco, CODIGOBNCOACRE=v_co_bnco_acre, TIPOCUENTA=v_ti_cnta, NUMEROCUENTA=v_nu_cnta, CODIGOBENE=v_co_bene, NOMBREBENE=v_no_bene, ESTADODESEMBOLSO=v_st_dbso, MONTODESEMBOLSO=v_mo_dbso, NUMEROORDE=v_nu_orde, FECHAPAGO=v_fe_pago, CODIGOUSUARIO=v_co_usua, DESCRIPCIONPAGO=v_ds_pago, TIPODEVOLUCIONDDEVO=v_ti_devo, ANIODEVOLUCIONDDEVO=v_aa_devo, SECUENCIADEVOLUCIONDDEVO=v_qs_devo, CODIGOROL=v_co_tord, SECUENCIAROL=v_qs_dbso, SECUENCIASOBRANTE=v_qs_dbso WHERE TIPODEVOLUCION=v_ti_devo;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper desembolsoDevolucion_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper desembolsoDevolucion_type: ' || v_err);
END;
/

/* detalleRecuperacion_type -> USP_INBOX_DETALLERECUPERACION (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_DETALLERECUPERACION(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_qs_abno VARCHAR2(4000);
    v_mo_mvto VARCHAR2(4000);
    v_st_mvto VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_qs_abno := JSON_VALUE(p_payload, '$.qs_abno');
    v_mo_mvto := JSON_VALUE(p_payload, '$.mo_mvto');
    v_st_mvto := JSON_VALUE(p_payload, '$.st_mvto');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.DETALLERECUPERACION_TYPE WHERE SECUENCIAABONO=v_qs_abno;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.DETALLERECUPERACION_TYPE (SECUENCIAABONO,MONTOMOVIMIENTO,ESTADOVENCIMIENTO,ANIOCREDITO,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_qs_abno,v_mo_mvto,v_st_mvto,v_aa_cred,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.DETALLERECUPERACION_TYPE SET MONTOMOVIMIENTO=v_mo_mvto, ESTADOVENCIMIENTO=v_st_mvto, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIAABONO=v_qs_abno;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper detalleRecuperacion_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper detalleRecuperacion_type: ' || v_err);
END;
/

/* devengamientoCarteraDetalle_type -> USP_INBOX_DVGOCARTDETA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_DVGOCARTDETA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_dvgo_deta VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_nu_dcto VARCHAR2(4000);
    v_mo_sald_capi VARCHAR2(4000);
    v_fe_ultm_cort VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_mo_dvgo_xven VARCHAR2(4000);
    v_mo_dvgo_venc VARCHAR2(4000);
    v_mo_reve VARCHAR2(4000);
    v_mo_ajus VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_dvgo_deta := JSON_VALUE(p_payload, '$.co_dvgo_deta');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_nu_dcto := JSON_VALUE(p_payload, '$.nu_dcto');
    v_mo_sald_capi := JSON_VALUE(p_payload, '$.mo_sald_capi');
    v_fe_ultm_cort := JSON_VALUE(p_payload, '$.fe_ultm_cort');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_mo_dvgo_xven := JSON_VALUE(p_payload, '$.mo_dvgo_xven');
    v_mo_dvgo_venc := JSON_VALUE(p_payload, '$.mo_dvgo_venc');
    v_mo_reve := JSON_VALUE(p_payload, '$.mo_reve');
    v_mo_ajus := JSON_VALUE(p_payload, '$.mo_ajus');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.DEVENGAMIENTOCARTERADETALLE_TYPE WHERE CODIGODVGODETALLE=v_co_dvgo_deta;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.DEVENGAMIENTOCARTERADETALLE_TYPE (CODIGODVGODETALLE,TIPOCREDITO,ANIOCREDITO,SECUENCIACREDITO,NUMERODCTO,MONTOSALDOCAPI,FECHAULTMCORT,FECHACORT,MONTODVGOXVEN,MONTODVGOVENC,MONTOREVE,MONTOAJUS,CODIGOEMPRESA)
            VALUES (v_co_dvgo_deta,v_ti_cred,v_aa_cred,v_qs_cred,v_nu_dcto,v_mo_sald_capi,v_fe_ultm_cort,v_fe_cort,v_mo_dvgo_xven,v_mo_dvgo_venc,v_mo_reve,v_mo_ajus,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.DEVENGAMIENTOCARTERADETALLE_TYPE SET TIPOCREDITO=v_ti_cred, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, NUMERODCTO=v_nu_dcto, MONTOSALDOCAPI=v_mo_sald_capi, FECHAULTMCORT=v_fe_ultm_cort, FECHACORT=v_fe_cort, MONTODVGOXVEN=v_mo_dvgo_xven, MONTODVGOVENC=v_mo_dvgo_venc, MONTOREVE=v_mo_reve, MONTOAJUS=v_mo_ajus, CODIGOEMPRESA=v_co_empr WHERE CODIGODVGODETALLE=v_co_dvgo_deta;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper devengamientoCarteraDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper devengamientoCarteraDetalle_type: ' || v_err);
END;
/

/* devengamientoCartera_type -> USP_INBOX_DEVENGAMIENTOCARTERA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_DEVENGAMIENTOCARTERA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_cred VARCHAR2(4000);
    v_co_dvgo_deta VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_fe_ultm_cort VARCHAR2(4000);
    v_mo_dvgo_venc VARCHAR2(4000);
    v_mo_ajus VARCHAR2(4000);
    v_mo_reve VARCHAR2(4000);
    v_mo_sald_capi VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_co_dvgo_deta := JSON_VALUE(p_payload, '$.co_dvgo_deta');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_fe_ultm_cort := JSON_VALUE(p_payload, '$.fe_ultm_cort');
    v_mo_dvgo_venc := JSON_VALUE(p_payload, '$.mo_dvgo_venc');
    v_mo_ajus := JSON_VALUE(p_payload, '$.mo_ajus');
    v_mo_reve := JSON_VALUE(p_payload, '$.mo_reve');
    v_mo_sald_capi := JSON_VALUE(p_payload, '$.mo_sald_capi');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.DEVENGAMIENTOCARTERA_TYPE WHERE TIPOCREDITO=v_ti_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.DEVENGAMIENTOCARTERA_TYPE (TIPOCREDITO,CODIGODEVENGODETALLE,FECHACORTE,FECHAULTIMOCORTE,MONTODEVENGAMIENTOPORVENCER,MONTODEVENGAMIENTOVENCIDO,MONTOAJUSTEDEVENGOS,MONTOREVERSODEVENGOS,MONTOSALDOCAPITAL,CODIGOEMPRESA,ANIOCREDITO,SECUENCIACREDITO)
            VALUES (v_ti_cred,v_co_dvgo_deta,v_fe_cort,v_fe_ultm_cort,v_mo_dvgo_venc,v_mo_dvgo_venc,v_mo_ajus,v_mo_reve,v_mo_sald_capi,v_co_empr,v_aa_cred,v_qs_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.DEVENGAMIENTOCARTERA_TYPE SET CODIGODEVENGODETALLE=v_co_dvgo_deta, FECHACORTE=v_fe_cort, FECHAULTIMOCORTE=v_fe_ultm_cort, MONTODEVENGAMIENTOPORVENCER=v_mo_dvgo_venc, MONTODEVENGAMIENTOVENCIDO=v_mo_dvgo_venc, MONTOAJUSTEDEVENGOS=v_mo_ajus, MONTOREVERSODEVENGOS=v_mo_reve, MONTOSALDOCAPITAL=v_mo_sald_capi, CODIGOEMPRESA=v_co_empr, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred WHERE TIPOCREDITO=v_ti_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper devengamientoCartera_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper devengamientoCartera_type: ' || v_err);
END;
/

/* devolucionCredito_type -> USP_INBOX_DEVOLUCIONCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_DEVOLUCIONCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_qs_devo VARCHAR2(4000);
    v_aa_devo VARCHAR2(4000);
    v_ti_devo VARCHAR2(4000);
    v_co_tord VARCHAR2(4000);
    v_co_usua VARCHAR2(4000);
    v_qs_dbso VARCHAR2(4000);
    v_co_bene VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_qs_devo := JSON_VALUE(p_payload, '$.qs_devo');
    v_aa_devo := JSON_VALUE(p_payload, '$.aa_devo');
    v_ti_devo := JSON_VALUE(p_payload, '$.ti_devo');
    v_co_tord := JSON_VALUE(p_payload, '$.co_tord');
    v_co_usua := JSON_VALUE(p_payload, '$.co_usua');
    v_qs_dbso := JSON_VALUE(p_payload, '$.qs_dbso');
    v_co_bene := JSON_VALUE(p_payload, '$.co_bene');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.DEVOLUCIONCREDITO_TYPE WHERE SECUENCIADEVOLUCION=v_qs_devo;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.DEVOLUCIONCREDITO_TYPE (SECUENCIADEVOLUCION,ANIODEVOLUCION,TIPODEVOLUCION,CODIGOFONDO,MONTODEVOLUCION,CODIGOUSUARIOTRANSMISION,CODIGOROL,SECUENCIAROL,SECUENCIASOBRANTES,CODIGOEMPRESA)
            VALUES (v_qs_devo,v_aa_devo,v_ti_devo,v_co_tord,v_ti_devo,v_co_usua,v_co_tord,v_qs_dbso,v_qs_dbso,v_co_bene);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.DEVOLUCIONCREDITO_TYPE SET ANIODEVOLUCION=v_aa_devo, TIPODEVOLUCION=v_ti_devo, CODIGOFONDO=v_co_tord, MONTODEVOLUCION=v_ti_devo, CODIGOUSUARIOTRANSMISION=v_co_usua, CODIGOROL=v_co_tord, SECUENCIAROL=v_qs_dbso, SECUENCIASOBRANTES=v_qs_dbso, CODIGOEMPRESA=v_co_bene WHERE SECUENCIADEVOLUCION=v_qs_devo;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper devolucionCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper devolucionCredito_type: ' || v_err);
END;
/

/* devolucionMasivaDetalle_type -> USP_INBOX_DEVOMASDETA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_DEVOMASDETA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_empr VARCHAR2(4000);
    v_sc_devo_deta VARCHAR2(4000);
    v_sc_devo_masi VARCHAR2(4000);
    v_sc_sobr VARCHAR2(4000);
    v_mo_disp VARCHAR2(4000);
    v_sc_mvto VARCHAR2(4000);
    v_co_liqd_rubr VARCHAR2(4000);
    v_mo_mvto VARCHAR2(4000);
    v_st_devo_deta VARCHAR2(4000);
    v_co_rubr_rol VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_sc_devo_deta := JSON_VALUE(p_payload, '$.sc_devo_deta');
    v_sc_devo_masi := JSON_VALUE(p_payload, '$.sc_devo_masi');
    v_sc_sobr := JSON_VALUE(p_payload, '$.sc_sobr');
    v_mo_disp := JSON_VALUE(p_payload, '$.mo_disp');
    v_sc_mvto := JSON_VALUE(p_payload, '$.sc_mvto');
    v_co_liqd_rubr := JSON_VALUE(p_payload, '$.co_liqd_rubr');
    v_mo_mvto := JSON_VALUE(p_payload, '$.mo_mvto');
    v_st_devo_deta := JSON_VALUE(p_payload, '$.st_devo_deta');
    v_co_rubr_rol := JSON_VALUE(p_payload, '$.co_rubr_rol');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.DEVOLUCIONMASIVADETALLE_TYPE WHERE CODIGOEMPRESA=v_co_empr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.DEVOLUCIONMASIVADETALLE_TYPE (CODIGOEMPRESA,SECUENCIADEVOLUCIONDETALLE,SECUENCIADEVOLUCIONMASIVA,SECUENCIASOBRANTE,MONTODISP,SECUENCIAMOVIMIENTO,CODIGOLIQUIDACIONRUBRO,MONTOMOVIMIENTO,ESTADODEVOLUCIONDETALLE,CODIGORUBROROL)
            VALUES (v_co_empr,v_sc_devo_deta,v_sc_devo_masi,v_sc_sobr,v_mo_disp,v_sc_mvto,v_co_liqd_rubr,v_mo_mvto,v_st_devo_deta,v_co_rubr_rol);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.DEVOLUCIONMASIVADETALLE_TYPE SET SECUENCIADEVOLUCIONDETALLE=v_sc_devo_deta, SECUENCIADEVOLUCIONMASIVA=v_sc_devo_masi, SECUENCIASOBRANTE=v_sc_sobr, MONTODISP=v_mo_disp, SECUENCIAMOVIMIENTO=v_sc_mvto, CODIGOLIQUIDACIONRUBRO=v_co_liqd_rubr, MONTOMOVIMIENTO=v_mo_mvto, ESTADODEVOLUCIONDETALLE=v_st_devo_deta, CODIGORUBROROL=v_co_rubr_rol WHERE CODIGOEMPRESA=v_co_empr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper devolucionMasivaDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper devolucionMasivaDetalle_type: ' || v_err);
END;
/

/* devolucionMasiva_type -> USP_INBOX_DEVOLUCIONMASIVA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_DEVOLUCIONMASIVA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_devo_masi VARCHAR2(4000);
    v_st_devo_deta VARCHAR2(4000);
    v_sc_devo_deta VARCHAR2(4000);
    v_co_liqd_rubr VARCHAR2(4000);
    v_sc_mvto VARCHAR2(4000);
    v_sc_sobr VARCHAR2(4000);
    v_mo_disp VARCHAR2(4000);
    v_mo_mvto VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_co_rubr_rol VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_devo_masi := JSON_VALUE(p_payload, '$.sc_devo_masi');
    v_st_devo_deta := JSON_VALUE(p_payload, '$.st_devo_deta');
    v_sc_devo_deta := JSON_VALUE(p_payload, '$.sc_devo_deta');
    v_co_liqd_rubr := JSON_VALUE(p_payload, '$.co_liqd_rubr');
    v_sc_mvto := JSON_VALUE(p_payload, '$.sc_mvto');
    v_sc_sobr := JSON_VALUE(p_payload, '$.sc_sobr');
    v_mo_disp := JSON_VALUE(p_payload, '$.mo_disp');
    v_mo_mvto := JSON_VALUE(p_payload, '$.mo_mvto');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_co_rubr_rol := JSON_VALUE(p_payload, '$.co_rubr_rol');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.DEVOLUCIONMASIVA_TYPE WHERE SECUENCIADEVOLUCIONESMASIVAS=v_sc_devo_masi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.DEVOLUCIONMASIVA_TYPE (SECUENCIADEVOLUCIONESMASIVAS,TIPODEVOLUCIONMASIVA,ESTADODEVOLUCIONESMASIVAS,SECUENCIADEVOLUCIONESMASIVASDETALLE,CODIGOLIQUIDACIONRUBRO,SECUENCIAMOVIMIENTO,SECUENCIASOBRANTES,MONTODISPONIBLESOBRANTE,MONTOMOVIMIENTO,CODIGOEMPRESA,COIGORUBROROL)
            VALUES (v_sc_devo_masi,v_sc_devo_masi,v_st_devo_deta,v_sc_devo_deta,v_co_liqd_rubr,v_sc_mvto,v_sc_sobr,v_mo_disp,v_mo_mvto,v_co_empr,v_co_rubr_rol);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.DEVOLUCIONMASIVA_TYPE SET TIPODEVOLUCIONMASIVA=v_sc_devo_masi, ESTADODEVOLUCIONESMASIVAS=v_st_devo_deta, SECUENCIADEVOLUCIONESMASIVASDETALLE=v_sc_devo_deta, CODIGOLIQUIDACIONRUBRO=v_co_liqd_rubr, SECUENCIAMOVIMIENTO=v_sc_mvto, SECUENCIASOBRANTES=v_sc_sobr, MONTODISPONIBLESOBRANTE=v_mo_disp, MONTOMOVIMIENTO=v_mo_mvto, CODIGOEMPRESA=v_co_empr, COIGORUBROROL=v_co_rubr_rol WHERE SECUENCIADEVOLUCIONESMASIVAS=v_sc_devo_masi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper devolucionMasiva_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper devolucionMasiva_type: ' || v_err);
END;
/

/* documentoCredito_type -> USP_INBOX_DOCUMENTOCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_DOCUMENTOCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_docu VARCHAR2(4000);
    v_ds_docu VARCHAR2(4000);
    v_st_docu VARCHAR2(4000);
    v_ti_docu VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_docu := JSON_VALUE(p_payload, '$.co_docu');
    v_ds_docu := JSON_VALUE(p_payload, '$.ds_docu');
    v_st_docu := JSON_VALUE(p_payload, '$.st_docu');
    v_ti_docu := JSON_VALUE(p_payload, '$.ti_docu');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.DOCUMENTOCREDITO_TYPE WHERE CODIGODOCUMENTO=v_co_docu;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.DOCUMENTOCREDITO_TYPE (CODIGODOCUMENTO,DESCRIPCIONDOCUMENTO,ESTADODOCUMENTO,TIPODOCUMENTO)
            VALUES (v_co_docu,v_ds_docu,v_st_docu,v_ti_docu);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.DOCUMENTOCREDITO_TYPE SET DESCRIPCIONDOCUMENTO=v_ds_docu, ESTADODOCUMENTO=v_st_docu, TIPODOCUMENTO=v_ti_docu WHERE CODIGODOCUMENTO=v_co_docu;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper documentoCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper documentoCredito_type: ' || v_err);
END;
/

/* estadoConvenioCredito_type -> USP_INBOX_ESTCONVCRED (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_ESTCONVCRED(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_st_regi_conv VARCHAR2(4000);
    v_ds_esta VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_st_regi_conv := JSON_VALUE(p_payload, '$.st_regi_conv');
    v_ds_esta := JSON_VALUE(p_payload, '$.ds_esta');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.ESTADOCONVENIOCREDITO_TYPE WHERE ESTADOREGISTROCONVENIO=v_st_regi_conv;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.ESTADOCONVENIOCREDITO_TYPE (ESTADOREGISTROCONVENIO,DESCRIPCIONESTADO,ESTADOREGISTRO)
            VALUES (v_st_regi_conv,v_ds_esta,v_st_regi);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.ESTADOCONVENIOCREDITO_TYPE SET DESCRIPCIONESTADO=v_ds_esta, ESTADOREGISTRO=v_st_regi WHERE ESTADOREGISTROCONVENIO=v_st_regi_conv;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper estadoConvenioCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper estadoConvenioCredito_type: ' || v_err);
END;
/

/* estadoLegalType -> USP_INBOX_ESTADOLEGAL (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_ESTADOLEGAL(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_nu_oper VARCHAR2(4000);
    v_mo_reca_inve VARCHAR2(4000);
    v_ds_esta_docu_inve VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_nu_oper := JSON_VALUE(p_payload, '$.nu_oper');
    v_mo_reca_inve := JSON_VALUE(p_payload, '$.mo_reca_inve');
    v_ds_esta_docu_inve := JSON_VALUE(p_payload, '$.ds_esta_docu_inve');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.ESTADOLEGALTYPE WHERE NUMEROJUICIO=v_nu_oper;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.ESTADOLEGALTYPE (NUMEROJUICIO,MONTO,ESTADO)
            VALUES (v_nu_oper,v_mo_reca_inve,v_ds_esta_docu_inve);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.ESTADOLEGALTYPE SET MONTO=v_mo_reca_inve, ESTADO=v_ds_esta_docu_inve WHERE NUMEROJUICIO=v_nu_oper;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper estadoLegalType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper estadoLegalType: ' || v_err);
END;
/

/* etapaJudicialCredito_type -> USP_INBOX_ETAPAJUDICIALCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_ETAPAJUDICIALCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_empr VARCHAR2(4000);
    v_co_etap VARCHAR2(4000);
    v_co_medi VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_co_etap := JSON_VALUE(p_payload, '$.co_etap');
    v_co_medi := JSON_VALUE(p_payload, '$.co_medi');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.ETAPAJUDICIALCREDITO_TYPE WHERE CODIGOEMPRESA=v_co_empr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.ETAPAJUDICIALCREDITO_TYPE (CODIGOEMPRESA,CODIGOETAP,CODIGOEMPRESAMEDICOBR,CODIGOMEDI)
            VALUES (v_co_empr,v_co_etap,v_co_empr,v_co_medi);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.ETAPAJUDICIALCREDITO_TYPE SET CODIGOETAP=v_co_etap, CODIGOEMPRESAMEDICOBR=v_co_empr, CODIGOMEDI=v_co_medi WHERE CODIGOEMPRESA=v_co_empr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper etapaJudicialCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper etapaJudicialCredito_type: ' || v_err);
END;
/

/* fechasProcesoType -> USP_INBOX_FECHASPROCESO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_FECHASPROCESO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_noti VARCHAR2(4000);
    v_co_usua_crea VARCHAR2(4000);
    v_co_proc VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_noti := JSON_VALUE(p_payload, '$.co_noti');
    v_co_usua_crea := JSON_VALUE(p_payload, '$.co_usua_crea');
    v_co_proc := JSON_VALUE(p_payload, '$.co_proc');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.FECHASPROCESOTYPE WHERE CODIGOCUENTA=v_co_noti;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.FECHASPROCESOTYPE (CODIGOCUENTA,CODIGOSUBSISTEMA,CODIGOGRUPOPRODUCTO,CODIGOPRODUCTO,CODIGOSUCURSAL)
            VALUES (v_co_noti,v_co_usua_crea,v_co_proc,v_co_proc,v_co_usua_crea);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.FECHASPROCESOTYPE SET CODIGOSUBSISTEMA=v_co_usua_crea, CODIGOGRUPOPRODUCTO=v_co_proc, CODIGOPRODUCTO=v_co_proc, CODIGOSUCURSAL=v_co_usua_crea WHERE CODIGOCUENTA=v_co_noti;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper fechasProcesoType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper fechasProcesoType: ' || v_err);
END;
/

/* flujoTrabajoCredito_type -> USP_INBOX_FLUJOTRABAJOCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_FLUJOTRABAJOCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_usua_modi VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_usua_modi := JSON_VALUE(p_payload, '$.co_usua_modi');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.FLUJOTRABAJOCREDITO_TYPE WHERE CODIGOUSUARIO=v_co_usua_modi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.FLUJOTRABAJOCREDITO_TYPE (CODIGOUSUARIO,FECHAGENERACION,ANIOCREDITO,SECUENCIACREDITO,SECUENCIASEGUMIENTO,TIPOCREDITO)
            VALUES (v_co_usua_modi,v_fe_elim,v_aa_cred,v_qs_cred,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.FLUJOTRABAJOCREDITO_TYPE SET FECHAGENERACION=v_fe_elim, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, SECUENCIASEGUMIENTO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE CODIGOUSUARIO=v_co_usua_modi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper flujoTrabajoCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper flujoTrabajoCredito_type: ' || v_err);
END;
/

/* garantiaCredito_type -> USP_INBOX_GARANTIACREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_GARANTIACREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_gara_hipo VARCHAR2(4000);
    v_co_prov VARCHAR2(4000);
    v_nu_vill VARCHAR2(4000);
    v_nu_manz VARCHAR2(4000);
    v_nu_bloq VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_fe_ingr VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_co_usua_conf VARCHAR2(4000);
    v_co_ciud VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_gara_hipo := JSON_VALUE(p_payload, '$.sc_gara_hipo');
    v_co_prov := JSON_VALUE(p_payload, '$.co_prov');
    v_nu_vill := JSON_VALUE(p_payload, '$.nu_vill');
    v_nu_manz := JSON_VALUE(p_payload, '$.nu_manz');
    v_nu_bloq := JSON_VALUE(p_payload, '$.nu_bloq');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_fe_ingr := JSON_VALUE(p_payload, '$.fe_ingr');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_co_usua_conf := JSON_VALUE(p_payload, '$.co_usua_conf');
    v_co_ciud := JSON_VALUE(p_payload, '$.co_ciud');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.GARANTIACREDITO_TYPE WHERE SECUENCIAGARANTIA=v_sc_gara_hipo;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.GARANTIACREDITO_TYPE (SECUENCIAGARANTIA,CODIGOPROGRAMAVIVIENDA,CODIGOTIPOVIVIENDA,NUMEROVIVIENDA,NUMEROMANZANA,NUMEROBLOQUE,SECUENCIAAVALUO,ESTADOREGISTRO,FECHAENTREGA,FECHAVERIFICACION,FECHAELIMINACION,FECHACREACION,CODIGOUSUARIOVERIFICA,CODIGOGARANTIA,CODIGOCUENTA)
            VALUES (v_sc_gara_hipo,v_co_prov,v_co_prov,v_nu_vill,v_nu_manz,v_nu_bloq,v_sc_gara_hipo,v_st_regi,v_fe_ingr,v_fe_elim,v_fe_elim,v_fe_elim,v_co_usua_conf,v_co_prov,v_co_ciud);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.GARANTIACREDITO_TYPE SET CODIGOPROGRAMAVIVIENDA=v_co_prov, CODIGOTIPOVIVIENDA=v_co_prov, NUMEROVIVIENDA=v_nu_vill, NUMEROMANZANA=v_nu_manz, NUMEROBLOQUE=v_nu_bloq, SECUENCIAAVALUO=v_sc_gara_hipo, ESTADOREGISTRO=v_st_regi, FECHAENTREGA=v_fe_ingr, FECHAVERIFICACION=v_fe_elim, FECHAELIMINACION=v_fe_elim, FECHACREACION=v_fe_elim, CODIGOUSUARIOVERIFICA=v_co_usua_conf, CODIGOGARANTIA=v_co_prov, CODIGOCUENTA=v_co_ciud WHERE SECUENCIAGARANTIA=v_sc_gara_hipo;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper garantiaCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper garantiaCredito_type: ' || v_err);
END;
/

/* gestionCobranzaAsignacion_type -> USP_INBOX_GESTCOBRASIG (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_GESTCOBRASIG(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_qs_cred VARCHAR2(4000);
    v_co_usua_gest VARCHAR2(4000);
    v_co_usua_ante VARCHAR2(4000);
    v_ti_calf_homo VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_co_gest_cart_asig VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_co_usua_gest := JSON_VALUE(p_payload, '$.co_usua_gest');
    v_co_usua_ante := JSON_VALUE(p_payload, '$.co_usua_ante');
    v_ti_calf_homo := JSON_VALUE(p_payload, '$.ti_calf_homo');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_co_gest_cart_asig := JSON_VALUE(p_payload, '$.co_gest_cart_asig');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.GESTIONCOBRANZAASIGNACION_TYPE WHERE SECUENCIAREGISTRO=v_qs_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.GESTIONCOBRANZAASIGNACION_TYPE (SECUENCIAREGISTRO,CODIGOUSUARIOASIGNADO,CODIGOUSUARIOASIGNADOANTERIORMENTE,TIPOCALIFICACIONHOMOLOGADO,FECHACORTE,ANIOCREDITO,SECUENCIACREDITO,TIPOCREDITO,CODIGOASIGNACION)
            VALUES (v_qs_cred,v_co_usua_gest,v_co_usua_ante,v_ti_calf_homo,v_fe_cort,v_aa_cred,v_qs_cred,v_ti_cred,v_co_gest_cart_asig);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.GESTIONCOBRANZAASIGNACION_TYPE SET CODIGOUSUARIOASIGNADO=v_co_usua_gest, CODIGOUSUARIOASIGNADOANTERIORMENTE=v_co_usua_ante, TIPOCALIFICACIONHOMOLOGADO=v_ti_calf_homo, FECHACORTE=v_fe_cort, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred, CODIGOASIGNACION=v_co_gest_cart_asig WHERE SECUENCIAREGISTRO=v_qs_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper gestionCobranzaAsignacion_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper gestionCobranzaAsignacion_type: ' || v_err);
END;
/

/* gestionComunicacionCredito_type -> USP_INBOX_GESTCOMUCRED (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_GESTCOMUCRED(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_fe_pago VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_st_gest VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_sc_pago VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_fe_pago := JSON_VALUE(p_payload, '$.fe_pago');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_st_gest := JSON_VALUE(p_payload, '$.st_gest');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_sc_pago := JSON_VALUE(p_payload, '$.sc_pago');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.GESTIONCOMUNICACIONCREDITO_TYPE WHERE FECHACARGA=v_fe_pago;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.GESTIONCOMUNICACIONCREDITO_TYPE (FECHACARGA,FECHAGUIA,ANIOCREDITO,ESTADOGESTIONTLLAMADA,RESULTADOGESTIONLLAMADA,SECUENCIACREDITO,TIPOCREDITO,ESTADOGESTIONMAIL,ESTADOGESTIONSMS,SECUENCIAGESTIONSMS)
            VALUES (v_fe_pago,v_fe_pago,v_aa_cred,v_st_gest,v_st_gest,v_qs_cred,v_ti_cred,v_st_gest,v_st_gest,v_sc_pago);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.GESTIONCOMUNICACIONCREDITO_TYPE SET FECHAGUIA=v_fe_pago, ANIOCREDITO=v_aa_cred, ESTADOGESTIONTLLAMADA=v_st_gest, RESULTADOGESTIONLLAMADA=v_st_gest, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred, ESTADOGESTIONMAIL=v_st_gest, ESTADOGESTIONSMS=v_st_gest, SECUENCIAGESTIONSMS=v_sc_pago WHERE FECHACARGA=v_fe_pago;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper gestionComunicacionCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper gestionComunicacionCredito_type: ' || v_err);
END;
/

/* grupoCreditoDetalle_type -> USP_INBOX_GRUPOCREDITODETALLE (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_GRUPOCREDITODETALLE(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_rubr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_rubr := JSON_VALUE(p_payload, '$.co_rubr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.GRUPOCREDITODETALLE_TYPE WHERE CODIGOGRUPO=v_co_rubr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.GRUPOCREDITODETALLE_TYPE (CODIGOGRUPO)
            VALUES (v_co_rubr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.GRUPOCREDITODETALLE_TYPE SET CODIGOGRUPO=v_co_rubr WHERE CODIGOGRUPO=v_co_rubr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper grupoCreditoDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper grupoCreditoDetalle_type: ' || v_err);
END;
/

/* grupoCreditoDocumento_type -> USP_INBOX_GRUPOCREDDOCUMENTO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_GRUPOCREDDOCUMENTO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_docu VARCHAR2(4000);
    v_co_docu VARCHAR2(4000);
    v_st_docu VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_docu := JSON_VALUE(p_payload, '$.ti_docu');
    v_co_docu := JSON_VALUE(p_payload, '$.co_docu');
    v_st_docu := JSON_VALUE(p_payload, '$.st_docu');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.GRUPOCREDITODOCUMENTO_TYPE WHERE TIPOCREDITO=v_ti_docu;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.GRUPOCREDITODOCUMENTO_TYPE (TIPOCREDITO,CODIGODOCUMENTO,ESTADOCREDITO)
            VALUES (v_ti_docu,v_co_docu,v_st_docu);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.GRUPOCREDITODOCUMENTO_TYPE SET CODIGODOCUMENTO=v_co_docu, ESTADOCREDITO=v_st_docu WHERE TIPOCREDITO=v_ti_docu;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper grupoCreditoDocumento_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper grupoCreditoDocumento_type: ' || v_err);
END;
/

/* informacionLegal_type -> USP_INBOX_INFORMACIONLEGAL (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_INFORMACIONLEGAL(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_aa_cred VARCHAR2(4000);
    v_co_usua_recp VARCHAR2(4000);
    v_ds_refe VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_fe_modi VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_co_usua_recp := JSON_VALUE(p_payload, '$.co_usua_recp');
    v_ds_refe := JSON_VALUE(p_payload, '$.ds_refe');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_fe_modi := JSON_VALUE(p_payload, '$.fe_modi');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.INFORMACIONLEGAL_TYPE WHERE ANIOCREDITO=v_aa_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.INFORMACIONLEGAL_TYPE (ANIOCREDITO,CODIGOUSUARIORECEPTA,DESCRIPCIONREFERENCIA,ESTADOREGISTRO,FECHAELIMINACION,FECHAMODIFICACION,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_aa_cred,v_co_usua_recp,v_ds_refe,v_st_regi,v_fe_elim,v_fe_modi,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.INFORMACIONLEGAL_TYPE SET CODIGOUSUARIORECEPTA=v_co_usua_recp, DESCRIPCIONREFERENCIA=v_ds_refe, ESTADOREGISTRO=v_st_regi, FECHAELIMINACION=v_fe_elim, FECHAMODIFICACION=v_fe_modi, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE ANIOCREDITO=v_aa_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper informacionLegal_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper informacionLegal_type: ' || v_err);
END;
/

/* liquidacionDiariaCredito_type -> USP_INBOX_LIQDIARIACRED (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_LIQDIARIACRED(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_liqd VARCHAR2(4000);
    v_mo_rubr VARCHAR2(4000);
    v_st_cred VARCHAR2(4000);
    v_st_liqd_diar VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_liqd := JSON_VALUE(p_payload, '$.sc_liqd');
    v_mo_rubr := JSON_VALUE(p_payload, '$.mo_rubr');
    v_st_cred := JSON_VALUE(p_payload, '$.st_cred');
    v_st_liqd_diar := JSON_VALUE(p_payload, '$.st_liqd_diar');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.LIQUIDACIONDIARIACREDITO_TYPE WHERE SECUENCIALIQUIDACION=v_sc_liqd;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.LIQUIDACIONDIARIACREDITO_TYPE (SECUENCIALIQUIDACION,CODIGORUBRO,MONTORUBRO,ESTADOCREDITO,ESTADOLIQUIDACIONDIARIA,FECHACORTE,ANIOCREDITO,CODIGOEMPRESA,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_sc_liqd,v_mo_rubr,v_mo_rubr,v_st_cred,v_st_liqd_diar,v_fe_cort,v_aa_cred,v_co_empr,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.LIQUIDACIONDIARIACREDITO_TYPE SET CODIGORUBRO=v_mo_rubr, MONTORUBRO=v_mo_rubr, ESTADOCREDITO=v_st_cred, ESTADOLIQUIDACIONDIARIA=v_st_liqd_diar, FECHACORTE=v_fe_cort, ANIOCREDITO=v_aa_cred, CODIGOEMPRESA=v_co_empr, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIALIQUIDACION=v_sc_liqd;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper liquidacionDiariaCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper liquidacionDiariaCredito_type: ' || v_err);
END;
/

/* medidaJudicialType -> USP_INBOX_MEDIDAJUDICIAL (STUB) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_MEDIDAJUDICIAL(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_err VARCHAR2(500);
BEGIN
    -- STUB: sin column mapping disponible para medidaJudicialType
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper medidaJudicialType: STUB sin column mapping');
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

/* movimientoContableCredito_type -> USP_INBOX_MOVCONTACRED (STUB-IDONLY) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_MOVCONTACRED(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
BEGIN
    -- STUB: dest_match solo mapea a ID (GENERATED ALWAYS) tras filtro. Sin cols insertables.
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper movimientoContableCredito_type: STUB - solo mapea a ID');
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

/* obligacionRol_type -> USP_INBOX_OBLIGACIONROL (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_OBLIGACIONROL(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_rol VARCHAR2(4000);
    v_ti_desc VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    v_ti_desc := JSON_VALUE(p_payload, '$.ti_desc');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.OBLIGACIONROL_TYPE WHERE CODIGOROL=v_co_rol;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.OBLIGACIONROL_TYPE (CODIGOROL,TIPODESCUENTO,ESTADOREGISTRO,ANIOCREDITO,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_co_rol,v_ti_desc,v_st_regi,v_aa_cred,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.OBLIGACIONROL_TYPE SET TIPODESCUENTO=v_ti_desc, ESTADOREGISTRO=v_st_regi, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE CODIGOROL=v_co_rol;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper obligacionRol_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper obligacionRol_type: ' || v_err);
END;
/

/* operacionConyugal_type -> USP_INBOX_OPERACIONCONYUGAL (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_OPERACIONCONYUGAL(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_cred VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_co_tipo_deud VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_co_tipo_deud := JSON_VALUE(p_payload, '$.co_tipo_deud');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.OPERACIONCONYUGAL_TYPE WHERE TIPOCREDITO=v_ti_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.OPERACIONCONYUGAL_TYPE (TIPOCREDITO,ANIOCREDITO,SECUENCIACREDITO,CODIGOTIPODEUD)
            VALUES (v_ti_cred,v_aa_cred,v_qs_cred,v_co_tipo_deud);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.OPERACIONCONYUGAL_TYPE SET ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, CODIGOTIPODEUD=v_co_tipo_deud WHERE TIPOCREDITO=v_ti_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper operacionConyugal_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper operacionConyugal_type: ' || v_err);
END;
/

/* pagoCredito_type -> USP_INBOX_PAGOCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PAGOCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_aa_cred VARCHAR2(4000);
    v_ci_rol VARCHAR2(4000);
    v_co_fond VARCHAR2(4000);
    v_ti_inst VARCHAR2(4000);
    v_co_paga VARCHAR2(4000);
    v_co_prov VARCHAR2(4000);
    v_ce_regi VARCHAR2(4000);
    v_fx_pago VARCHAR2(4000);
    v_fx_proc VARCHAR2(4000);
    v_qs_abno VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_sc_reca VARCHAR2(4000);
    v_sc_rol VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_ti_pago VARCHAR2(4000);
    v_va_abno VARCHAR2(4000);
    v_va_pagr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_ci_rol := JSON_VALUE(p_payload, '$.ci_rol');
    v_co_fond := JSON_VALUE(p_payload, '$.co_fond');
    v_ti_inst := JSON_VALUE(p_payload, '$.ti_inst');
    v_co_paga := JSON_VALUE(p_payload, '$.co_paga');
    v_co_prov := JSON_VALUE(p_payload, '$.co_prov');
    v_ce_regi := JSON_VALUE(p_payload, '$.ce_regi');
    v_fx_pago := JSON_VALUE(p_payload, '$.fx_pago');
    v_fx_proc := JSON_VALUE(p_payload, '$.fx_proc');
    v_qs_abno := JSON_VALUE(p_payload, '$.qs_abno');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_sc_reca := JSON_VALUE(p_payload, '$.sc_reca');
    v_sc_rol := JSON_VALUE(p_payload, '$.sc_rol');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_ti_pago := JSON_VALUE(p_payload, '$.ti_pago');
    v_va_abno := JSON_VALUE(p_payload, '$.va_abno');
    v_va_pagr := JSON_VALUE(p_payload, '$.va_pagr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PAGOCREDITO_TYPE WHERE ANIOCREDITO=v_aa_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PAGOCREDITO_TYPE (ANIOCREDITO,CEDULAPROVEEDOR,CODIGOFONDO,CODIGOINSTICION,CODIGOPAGO,CODIGOPROVINCIA,CODIGOROL,ESTADOREGISTRO,FECHAPAGO,FECHAPROCESO,SECUENCIAABONO,SECUENCIACREDITO,SECUENCIARECAUDACION,SECUENCIAROL,TIPOCREDITO,TIPOINSTITUCION,TIPOPAGO,VALORABONO,VALORPAGADO)
            VALUES (v_aa_cred,v_ci_rol,v_co_fond,v_ti_inst,v_co_paga,v_co_prov,v_co_fond,v_ce_regi,v_fx_pago,v_fx_proc,v_qs_abno,v_qs_cred,v_sc_reca,v_sc_rol,v_ti_cred,v_ti_inst,v_ti_pago,v_va_abno,v_va_pagr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PAGOCREDITO_TYPE SET CEDULAPROVEEDOR=v_ci_rol, CODIGOFONDO=v_co_fond, CODIGOINSTICION=v_ti_inst, CODIGOPAGO=v_co_paga, CODIGOPROVINCIA=v_co_prov, CODIGOROL=v_co_fond, ESTADOREGISTRO=v_ce_regi, FECHAPAGO=v_fx_pago, FECHAPROCESO=v_fx_proc, SECUENCIAABONO=v_qs_abno, SECUENCIACREDITO=v_qs_cred, SECUENCIARECAUDACION=v_sc_reca, SECUENCIAROL=v_sc_rol, TIPOCREDITO=v_ti_cred, TIPOINSTITUCION=v_ti_inst, TIPOPAGO=v_ti_pago, VALORABONO=v_va_abno, VALORPAGADO=v_va_pagr WHERE ANIOCREDITO=v_aa_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper pagoCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper pagoCredito_type: ' || v_err);
END;
/

/* pagosCreditoType -> USP_INBOX_PAGOSCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PAGOSCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_comb VARCHAR2(4000);
    v_fe_aprb VARCHAR2(4000);
    v_fe_ingr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_comb := JSON_VALUE(p_payload, '$.co_comb');
    v_fe_aprb := JSON_VALUE(p_payload, '$.fe_aprb');
    v_fe_ingr := JSON_VALUE(p_payload, '$.fe_ingr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PAGOSCREDITOTYPE WHERE CODIGOCUOTA=v_co_comb;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PAGOSCREDITOTYPE (CODIGOCUOTA,FECHAHORA,CODIGOCONCEPTO,FECHAPAGO)
            VALUES (v_co_comb,v_fe_aprb,v_co_comb,v_fe_ingr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PAGOSCREDITOTYPE SET FECHAHORA=v_fe_aprb, CODIGOCONCEPTO=v_co_comb, FECHAPAGO=v_fe_ingr WHERE CODIGOCUOTA=v_co_comb;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper pagosCreditoType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper pagosCreditoType: ' || v_err);
END;
/

/* personaCreditoType -> USP_INBOX_PERSONACREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PERSONACREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_iden VARCHAR2(4000);
    v_nu_iden VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_iden := JSON_VALUE(p_payload, '$.ti_iden');
    v_nu_iden := JSON_VALUE(p_payload, '$.nu_iden');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PERSONACREDITOTYPE WHERE NUMERODIRECCION=v_nu_iden;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PERSONACREDITOTYPE (TIPOIDENTIFICACION,NUMERODIRECCION)
            VALUES (v_ti_iden,v_nu_iden);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PERSONACREDITOTYPE SET TIPOIDENTIFICACION=v_ti_iden WHERE NUMERODIRECCION=v_nu_iden;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper personaCreditoType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper personaCreditoType: ' || v_err);
END;
/

/* personaCxPCxCType -> USP_INBOX_PERSONACXPCXC (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PERSONACXPCXC(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_iden VARCHAR2(4000);
    v_ti_calf VARCHAR2(4000);
    v_co_tamo VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_iden := JSON_VALUE(p_payload, '$.ti_iden');
    v_ti_calf := JSON_VALUE(p_payload, '$.ti_calf');
    v_co_tamo := JSON_VALUE(p_payload, '$.co_tamo');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PERSONACXPCXCTYPE WHERE IDENTIFICACION=v_ti_calf;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PERSONACXPCXCTYPE (CODIGOTIPOIDENTIFICACION,IDENTIFICACION,CODIGOCUENTA)
            VALUES (v_ti_iden,v_ti_calf,v_co_tamo);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PERSONACXPCXCTYPE SET CODIGOTIPOIDENTIFICACION=v_ti_iden, CODIGOCUENTA=v_co_tamo WHERE IDENTIFICACION=v_ti_calf;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper personaCxPCxCType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper personaCxPCxCType: ' || v_err);
END;
/

/* planPagoAjuste_type -> USP_INBOX_PLANPAGOAJUSTE (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PLANPAGOAJUSTE(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_abno VARCHAR2(4000);
    v_fe_vcto VARCHAR2(4000);
    v_mo_cuot VARCHAR2(4000);
    v_mo_capi VARCHAR2(4000);
    v_mo_intr VARCHAR2(4000);
    v_mo_dvdo VARCHAR2(4000);
    v_mo_segu VARCHAR2(4000);
    v_mo_incd VARCHAR2(4000);
    v_mo_comi VARCHAR2(4000);
    v_mo_inte_pmes VARCHAR2(4000);
    v_pl_dias VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_abno := JSON_VALUE(p_payload, '$.sc_abno');
    v_fe_vcto := JSON_VALUE(p_payload, '$.fe_vcto');
    v_mo_cuot := JSON_VALUE(p_payload, '$.mo_cuot');
    v_mo_capi := JSON_VALUE(p_payload, '$.mo_capi');
    v_mo_intr := JSON_VALUE(p_payload, '$.mo_intr');
    v_mo_dvdo := JSON_VALUE(p_payload, '$.mo_dvdo');
    v_mo_segu := JSON_VALUE(p_payload, '$.mo_segu');
    v_mo_incd := JSON_VALUE(p_payload, '$.mo_incd');
    v_mo_comi := JSON_VALUE(p_payload, '$.mo_comi');
    v_mo_inte_pmes := JSON_VALUE(p_payload, '$.mo_inte_pmes');
    v_pl_dias := JSON_VALUE(p_payload, '$.pl_dias');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PLANPAGOAJUSTE_TYPE WHERE SECUENCIAABONOPROCESO=v_sc_abno;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PLANPAGOAJUSTE_TYPE (SECUENCIAABONOPROCESO,FECHAVENCIMIENTO,MONTOCUOTA,MONTOCAPITAL,MONTOINTERES,MONTODIVIDENDO,MONTOSEGURO,MONTOSEGUROVEHICULO,MONTOCOBROINCENDIO,MONTOCOMISION,MONTOCOSTOSEMISION,MONTOINTERESPRIMERMES,PLAZODIAS,ANIOCREDITO,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_sc_abno,v_fe_vcto,v_mo_cuot,v_mo_capi,v_mo_intr,v_mo_dvdo,v_mo_segu,v_mo_segu,v_mo_incd,v_mo_comi,v_mo_comi,v_mo_inte_pmes,v_pl_dias,v_aa_cred,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PLANPAGOAJUSTE_TYPE SET FECHAVENCIMIENTO=v_fe_vcto, MONTOCUOTA=v_mo_cuot, MONTOCAPITAL=v_mo_capi, MONTOINTERES=v_mo_intr, MONTODIVIDENDO=v_mo_dvdo, MONTOSEGURO=v_mo_segu, MONTOSEGUROVEHICULO=v_mo_segu, MONTOCOBROINCENDIO=v_mo_incd, MONTOCOMISION=v_mo_comi, MONTOCOSTOSEMISION=v_mo_comi, MONTOINTERESPRIMERMES=v_mo_inte_pmes, PLAZODIAS=v_pl_dias, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIAABONOPROCESO=v_sc_abno;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper planPagoAjuste_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper planPagoAjuste_type: ' || v_err);
END;
/

/* planPago_type -> USP_INBOX_PLANPAGO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PLANPAGO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_aa_cred VARCHAR2(4000);
    v_mo_gast_judi VARCHAR2(4000);
    v_st_pago_fcme VARCHAR2(4000);
    v_st_vcto VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_fe_inic_venc VARCHAR2(4000);
    v_fx_pago_fcme VARCHAR2(4000);
    v_fe_ultm_envi VARCHAR2(4000);
    v_in_reve_dvgo VARCHAR2(4000);
    v_mo_abno_mora VARCHAR2(4000);
    v_mo_dvgo_diar VARCHAR2(4000);
    v_mo_abno_capi VARCHAR2(4000);
    v_mo_abno_intr VARCHAR2(4000);
    v_mo_capi VARCHAR2(4000);
    v_mo_incd VARCHAR2(4000);
    v_mo_comi VARCHAR2(4000);
    v_mo_cuot VARCHAR2(4000);
    v_mo_dvgd_intr VARCHAR2(4000);
    v_mo_dvgd_mora VARCHAR2(4000);
    v_mo_dvgo_acum VARCHAR2(4000);
    v_mo_dvdo VARCHAR2(4000);
    v_mo_intr VARCHAR2(4000);
    v_mo_inte_pmes VARCHAR2(4000);
    v_mo_rast VARCHAR2(4000);
    v_mo_segu VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_sc_dcto VARCHAR2(4000);
    v_sc_rol VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_mo_gast_judi := JSON_VALUE(p_payload, '$.mo_gast_judi');
    v_st_pago_fcme := JSON_VALUE(p_payload, '$.st_pago_fcme');
    v_st_vcto := JSON_VALUE(p_payload, '$.st_vcto');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_fe_inic_venc := JSON_VALUE(p_payload, '$.fe_inic_venc');
    v_fx_pago_fcme := JSON_VALUE(p_payload, '$.fx_pago_fcme');
    v_fe_ultm_envi := JSON_VALUE(p_payload, '$.fe_ultm_envi');
    v_in_reve_dvgo := JSON_VALUE(p_payload, '$.in_reve_dvgo');
    v_mo_abno_mora := JSON_VALUE(p_payload, '$.mo_abno_mora');
    v_mo_dvgo_diar := JSON_VALUE(p_payload, '$.mo_dvgo_diar');
    v_mo_abno_capi := JSON_VALUE(p_payload, '$.mo_abno_capi');
    v_mo_abno_intr := JSON_VALUE(p_payload, '$.mo_abno_intr');
    v_mo_capi := JSON_VALUE(p_payload, '$.mo_capi');
    v_mo_incd := JSON_VALUE(p_payload, '$.mo_incd');
    v_mo_comi := JSON_VALUE(p_payload, '$.mo_comi');
    v_mo_cuot := JSON_VALUE(p_payload, '$.mo_cuot');
    v_mo_dvgd_intr := JSON_VALUE(p_payload, '$.mo_dvgd_intr');
    v_mo_dvgd_mora := JSON_VALUE(p_payload, '$.mo_dvgd_mora');
    v_mo_dvgo_acum := JSON_VALUE(p_payload, '$.mo_dvgo_acum');
    v_mo_dvdo := JSON_VALUE(p_payload, '$.mo_dvdo');
    v_mo_intr := JSON_VALUE(p_payload, '$.mo_intr');
    v_mo_inte_pmes := JSON_VALUE(p_payload, '$.mo_inte_pmes');
    v_mo_rast := JSON_VALUE(p_payload, '$.mo_rast');
    v_mo_segu := JSON_VALUE(p_payload, '$.mo_segu');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_sc_dcto := JSON_VALUE(p_payload, '$.sc_dcto');
    v_sc_rol := JSON_VALUE(p_payload, '$.sc_rol');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PLANPAGO_TYPE WHERE ANIOCREDITO=v_aa_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PLANPAGO_TYPE (ANIOCREDITO,ESTADOGASTOJUDICIAL,ESTADOPAGOFCME,ESTADOVENCIMIENTO,FECHAELIMINACION,FECHAINICVENC,FECHAPAGOFCME,FECHAULTIMOENVIO,FECHAVENCIMIENTO,INDICADORREVERSODEVENGO,MOABONOMORASOLCA,MODEVENGODIARIO,MONTOABONOCAPITALCAPITAL,MONTOABONOINTERESCAPITAL,MONTOABONOMORA,MONTOCAPITAL,MONTOCOBROINCENDIO,MONTOCOMISION,MONTOCOSTOSEMISION,MONTOCUOTA,MONTODEVENGADOINTERES,MONTODEVENGADOMORA,MONTODEVENGOACUMULADO,MONTODIVIDENDO,MONTOGASTOSJUDICIALES,MONTOINTERES,MONTOINTERESPRIMERMES,MONTORASTREOSATELITAL,MONTOSEGURO,MONTOSEGUROVEHICULO,SECUENCIACREDITO,SECUENCIADOCUMENTO,SECUENCIAROL,TIPOCREDITO)
            VALUES (v_aa_cred,v_mo_gast_judi,v_st_pago_fcme,v_st_vcto,v_fe_elim,v_fe_inic_venc,v_fx_pago_fcme,v_fe_ultm_envi,v_fe_elim,v_in_reve_dvgo,v_mo_abno_mora,v_mo_dvgo_diar,v_mo_abno_capi,v_mo_abno_intr,v_mo_abno_mora,v_mo_capi,v_mo_incd,v_mo_comi,v_mo_comi,v_mo_cuot,v_mo_dvgd_intr,v_mo_dvgd_mora,v_mo_dvgo_acum,v_mo_dvdo,v_mo_gast_judi,v_mo_intr,v_mo_inte_pmes,v_mo_rast,v_mo_segu,v_mo_segu,v_qs_cred,v_sc_dcto,v_sc_rol,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PLANPAGO_TYPE SET ESTADOGASTOJUDICIAL=v_mo_gast_judi, ESTADOPAGOFCME=v_st_pago_fcme, ESTADOVENCIMIENTO=v_st_vcto, FECHAELIMINACION=v_fe_elim, FECHAINICVENC=v_fe_inic_venc, FECHAPAGOFCME=v_fx_pago_fcme, FECHAULTIMOENVIO=v_fe_ultm_envi, FECHAVENCIMIENTO=v_fe_elim, INDICADORREVERSODEVENGO=v_in_reve_dvgo, MOABONOMORASOLCA=v_mo_abno_mora, MODEVENGODIARIO=v_mo_dvgo_diar, MONTOABONOCAPITALCAPITAL=v_mo_abno_capi, MONTOABONOINTERESCAPITAL=v_mo_abno_intr, MONTOABONOMORA=v_mo_abno_mora, MONTOCAPITAL=v_mo_capi, MONTOCOBROINCENDIO=v_mo_incd, MONTOCOMISION=v_mo_comi, MONTOCOSTOSEMISION=v_mo_comi, MONTOCUOTA=v_mo_cuot, MONTODEVENGADOINTERES=v_mo_dvgd_intr, MONTODEVENGADOMORA=v_mo_dvgd_mora, MONTODEVENGOACUMULADO=v_mo_dvgo_acum, MONTODIVIDENDO=v_mo_dvdo, MONTOGASTOSJUDICIALES=v_mo_gast_judi, MONTOINTERES=v_mo_intr, MONTOINTERESPRIMERMES=v_mo_inte_pmes, MONTORASTREOSATELITAL=v_mo_rast, MONTOSEGURO=v_mo_segu, MONTOSEGUROVEHICULO=v_mo_segu, SECUENCIACREDITO=v_qs_cred, SECUENCIADOCUMENTO=v_sc_dcto, SECUENCIAROL=v_sc_rol, TIPOCREDITO=v_ti_cred WHERE ANIOCREDITO=v_aa_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper planPago_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper planPago_type: ' || v_err);
END;
/

/* plazoVencido_type -> USP_INBOX_PLAZOVENCIDO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PLAZOVENCIDO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_cred_plzo VARCHAR2(4000);
    v_st_cred_plzo VARCHAR2(4000);
    v_fe_carg VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_fe_modi VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_mo_abno_capi VARCHAR2(4000);
    v_mo_abno_intr VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_cred_plzo := JSON_VALUE(p_payload, '$.sc_cred_plzo');
    v_st_cred_plzo := JSON_VALUE(p_payload, '$.st_cred_plzo');
    v_fe_carg := JSON_VALUE(p_payload, '$.fe_carg');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_fe_modi := JSON_VALUE(p_payload, '$.fe_modi');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_mo_abno_capi := JSON_VALUE(p_payload, '$.mo_abno_capi');
    v_mo_abno_intr := JSON_VALUE(p_payload, '$.mo_abno_intr');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PLAZOVENCIDO_TYPE WHERE SECUENCIA=v_sc_cred_plzo;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PLAZOVENCIDO_TYPE (SECUENCIA,ESTADO,FECHACARGA,FECHACREACION,FECHAMODIFICACION,FECHAELIMINACION,ANIOCREDITO,CODIGOEMPRESA,MONTOABONOCAPITALCAPITAL,MONTOABONOINTERESCAPITAL,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_sc_cred_plzo,v_st_cred_plzo,v_fe_carg,v_fe_elim,v_fe_modi,v_fe_elim,v_aa_cred,v_co_empr,v_mo_abno_capi,v_mo_abno_intr,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PLAZOVENCIDO_TYPE SET ESTADO=v_st_cred_plzo, FECHACARGA=v_fe_carg, FECHACREACION=v_fe_elim, FECHAMODIFICACION=v_fe_modi, FECHAELIMINACION=v_fe_elim, ANIOCREDITO=v_aa_cred, CODIGOEMPRESA=v_co_empr, MONTOABONOCAPITALCAPITAL=v_mo_abno_capi, MONTOABONOINTERESCAPITAL=v_mo_abno_intr, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIA=v_sc_cred_plzo;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper plazoVencido_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper plazoVencido_type: ' || v_err);
END;
/

/* precalificacionCredito_type -> USP_INBOX_PRECALIFCRED (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PRECALIFCRED(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_co_medi VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_mo_sald_cred VARCHAR2(4000);
    v_mo_sald_venc VARCHAR2(4000);
    v_mo_otro VARCHAR2(4000);
    v_ti_cobr VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_co_usua_elim VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_co_medi := JSON_VALUE(p_payload, '$.co_medi');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_mo_sald_cred := JSON_VALUE(p_payload, '$.mo_sald_cred');
    v_mo_sald_venc := JSON_VALUE(p_payload, '$.mo_sald_venc');
    v_mo_otro := JSON_VALUE(p_payload, '$.mo_otro');
    v_ti_cobr := JSON_VALUE(p_payload, '$.ti_cobr');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_co_usua_elim := JSON_VALUE(p_payload, '$.co_usua_elim');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PRECALIFICACIONCREDITO_TYPE WHERE SECUENCIAPRECALIFICACION=v_qs_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PRECALIFICACIONCREDITO_TYPE (SECUENCIAPRECALIFICACION,TIPOCREDITO,CODIGOFONDO,ANIO,MONTOCREDITO,MONTODIVIDENDO,MONTOOTROSGASTOS,TIPOGARANTIA,FECHACREACION,FECHAELIMINACION,CODIGOEMPRESA,CODIGOPRECALIFICACION)
            VALUES (v_qs_cred,v_ti_cred,v_co_medi,v_aa_cred,v_mo_sald_cred,v_mo_sald_venc,v_mo_otro,v_ti_cobr,v_fe_elim,v_fe_elim,v_co_empr,v_co_usua_elim);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PRECALIFICACIONCREDITO_TYPE SET TIPOCREDITO=v_ti_cred, CODIGOFONDO=v_co_medi, ANIO=v_aa_cred, MONTOCREDITO=v_mo_sald_cred, MONTODIVIDENDO=v_mo_sald_venc, MONTOOTROSGASTOS=v_mo_otro, TIPOGARANTIA=v_ti_cobr, FECHACREACION=v_fe_elim, FECHAELIMINACION=v_fe_elim, CODIGOEMPRESA=v_co_empr, CODIGOPRECALIFICACION=v_co_usua_elim WHERE SECUENCIAPRECALIFICACION=v_qs_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper precalificacionCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper precalificacionCredito_type: ' || v_err);
END;
/

/* procesoAccionType -> USP_INBOX_PROCESOACCION (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_PROCESOACCION(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_proc_obse VARCHAR2(4000);
    v_nu_tran VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_proc_obse := JSON_VALUE(p_payload, '$.co_proc_obse');
    v_nu_tran := JSON_VALUE(p_payload, '$.nu_tran');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.PROCESOACCIONTYPE WHERE PROCESO=v_co_proc_obse;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.PROCESOACCIONTYPE (PROCESO,ACCION)
            VALUES (v_co_proc_obse,v_nu_tran);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.PROCESOACCIONTYPE SET ACCION=v_nu_tran WHERE PROCESO=v_co_proc_obse;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper procesoAccionType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper procesoAccionType: ' || v_err);
END;
/

/* recuperacionConvenio_type -> USP_INBOX_RECUPERACIONCONVENIO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RECUPERACIONCONVENIO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_qs_abno VARCHAR2(4000);
    v_ti_recp VARCHAR2(4000);
    v_mo_mvto VARCHAR2(4000);
    v_co_rol VARCHAR2(4000);
    v_nu_cpbt_cble VARCHAR2(4000);
    v_st_mvto VARCHAR2(4000);
    v_st_autr VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_ti_revz VARCHAR2(4000);
    v_fe_abno VARCHAR2(4000);
    v_fe_mvto VARCHAR2(4000);
    v_fe_revz VARCHAR2(4000);
    v_co_usua_liqd VARCHAR2(4000);
    v_co_usua_conf VARCHAR2(4000);
    v_co_usua_revz VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_ds_liqd VARCHAR2(4000);
    v_nu_dias_atra VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_qs_abno := JSON_VALUE(p_payload, '$.qs_abno');
    v_ti_recp := JSON_VALUE(p_payload, '$.ti_recp');
    v_mo_mvto := JSON_VALUE(p_payload, '$.mo_mvto');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    v_nu_cpbt_cble := JSON_VALUE(p_payload, '$.nu_cpbt_cble');
    v_st_mvto := JSON_VALUE(p_payload, '$.st_mvto');
    v_st_autr := JSON_VALUE(p_payload, '$.st_autr');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_ti_revz := JSON_VALUE(p_payload, '$.ti_revz');
    v_fe_abno := JSON_VALUE(p_payload, '$.fe_abno');
    v_fe_mvto := JSON_VALUE(p_payload, '$.fe_mvto');
    v_fe_revz := JSON_VALUE(p_payload, '$.fe_revz');
    v_co_usua_liqd := JSON_VALUE(p_payload, '$.co_usua_liqd');
    v_co_usua_conf := JSON_VALUE(p_payload, '$.co_usua_conf');
    v_co_usua_revz := JSON_VALUE(p_payload, '$.co_usua_revz');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_ds_liqd := JSON_VALUE(p_payload, '$.ds_liqd');
    v_nu_dias_atra := JSON_VALUE(p_payload, '$.nu_dias_atra');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.RECUPERACIONCONVENIO_TYPE WHERE SECUENCIAABONO=v_qs_abno;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.RECUPERACIONCONVENIO_TYPE (SECUENCIAABONO,TIPORECUPERACION,MONTOMOVIMIENTO,CODIGOROL,NUMEROCOMPROBANTECONTABLE,ESTADOMOVIMIENTO,ESTADOAUTORIZACION,ESTADOREGISTRO,TIPOREVERSO,FECHAABONO,FECHAMOVIMIENTO,FECHAREVERSO,CODIGOUSUARIOLIQUIDA,CODIGOUSUARIOCONFIRMA,CODIGOUSUARIOREVERSA,ESTADOVENCIMIENTO,ANIOCREDITO,DESCRIPCIONLIQD,NUMERODIASATRAZO,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_qs_abno,v_ti_recp,v_mo_mvto,v_co_rol,v_nu_cpbt_cble,v_st_mvto,v_st_autr,v_st_regi,v_ti_revz,v_fe_abno,v_fe_mvto,v_fe_revz,v_co_usua_liqd,v_co_usua_conf,v_co_usua_revz,v_st_mvto,v_aa_cred,v_ds_liqd,v_nu_dias_atra,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.RECUPERACIONCONVENIO_TYPE SET TIPORECUPERACION=v_ti_recp, MONTOMOVIMIENTO=v_mo_mvto, CODIGOROL=v_co_rol, NUMEROCOMPROBANTECONTABLE=v_nu_cpbt_cble, ESTADOMOVIMIENTO=v_st_mvto, ESTADOAUTORIZACION=v_st_autr, ESTADOREGISTRO=v_st_regi, TIPOREVERSO=v_ti_revz, FECHAABONO=v_fe_abno, FECHAMOVIMIENTO=v_fe_mvto, FECHAREVERSO=v_fe_revz, CODIGOUSUARIOLIQUIDA=v_co_usua_liqd, CODIGOUSUARIOCONFIRMA=v_co_usua_conf, CODIGOUSUARIOREVERSA=v_co_usua_revz, ESTADOVENCIMIENTO=v_st_mvto, ANIOCREDITO=v_aa_cred, DESCRIPCIONLIQD=v_ds_liqd, NUMERODIASATRAZO=v_nu_dias_atra, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIAABONO=v_qs_abno;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper recuperacionConvenio_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper recuperacionConvenio_type: ' || v_err);
END;
/

/* recuperacionCredito_type -> USP_INBOX_RECUPERACIONCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RECUPERACIONCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_aa_cred VARCHAR2(4000);
    v_co_rol VARCHAR2(4000);
    v_co_usua_liqd VARCHAR2(4000);
    v_co_usua_revz VARCHAR2(4000);
    v_ds_liqd VARCHAR2(4000);
    v_st_mvto VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_fe_abno VARCHAR2(4000);
    v_fe_cble VARCHAR2(4000);
    v_fe_mvto VARCHAR2(4000);
    v_fe_revz VARCHAR2(4000);
    v_mo_mvto VARCHAR2(4000);
    v_nu_cpbt_cble VARCHAR2(4000);
    v_nu_dias_atra VARCHAR2(4000);
    v_qs_abno VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_ti_recp VARCHAR2(4000);
    v_ti_revz VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    v_co_usua_liqd := JSON_VALUE(p_payload, '$.co_usua_liqd');
    v_co_usua_revz := JSON_VALUE(p_payload, '$.co_usua_revz');
    v_ds_liqd := JSON_VALUE(p_payload, '$.ds_liqd');
    v_st_mvto := JSON_VALUE(p_payload, '$.st_mvto');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_fe_abno := JSON_VALUE(p_payload, '$.fe_abno');
    v_fe_cble := JSON_VALUE(p_payload, '$.fe_cble');
    v_fe_mvto := JSON_VALUE(p_payload, '$.fe_mvto');
    v_fe_revz := JSON_VALUE(p_payload, '$.fe_revz');
    v_mo_mvto := JSON_VALUE(p_payload, '$.mo_mvto');
    v_nu_cpbt_cble := JSON_VALUE(p_payload, '$.nu_cpbt_cble');
    v_nu_dias_atra := JSON_VALUE(p_payload, '$.nu_dias_atra');
    v_qs_abno := JSON_VALUE(p_payload, '$.qs_abno');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_ti_recp := JSON_VALUE(p_payload, '$.ti_recp');
    v_ti_revz := JSON_VALUE(p_payload, '$.ti_revz');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.RECUPERACIONCREDITO_TYPE WHERE ANIOCREDITO=v_aa_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.RECUPERACIONCREDITO_TYPE (ANIOCREDITO,CODIGOROL,CODIGOUSUARIOLIQUIDA,CODIGOUSUARIOREVERSA,DESCRIPCIONLIQUIDACION,ESTADOMOVIMIENTO,ESTADOREGISTRO,FECHAABONO,FECHACONTABLE,FECHAMOVIMIENTO,FECHAREVERSION,MONTOMOVIMIENTO,NUMEROCOMPROBANTECONTABLE,NUMERODIASATRASO,SECUENCIAABONO,SECUENCIACREDITO,TIPOCREDITO,TIPORECUPERACION,TIPOREVERSO)
            VALUES (v_aa_cred,v_co_rol,v_co_usua_liqd,v_co_usua_revz,v_ds_liqd,v_st_mvto,v_st_regi,v_fe_abno,v_fe_cble,v_fe_mvto,v_fe_revz,v_mo_mvto,v_nu_cpbt_cble,v_nu_dias_atra,v_qs_abno,v_qs_cred,v_ti_cred,v_ti_recp,v_ti_revz);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.RECUPERACIONCREDITO_TYPE SET CODIGOROL=v_co_rol, CODIGOUSUARIOLIQUIDA=v_co_usua_liqd, CODIGOUSUARIOREVERSA=v_co_usua_revz, DESCRIPCIONLIQUIDACION=v_ds_liqd, ESTADOMOVIMIENTO=v_st_mvto, ESTADOREGISTRO=v_st_regi, FECHAABONO=v_fe_abno, FECHACONTABLE=v_fe_cble, FECHAMOVIMIENTO=v_fe_mvto, FECHAREVERSION=v_fe_revz, MONTOMOVIMIENTO=v_mo_mvto, NUMEROCOMPROBANTECONTABLE=v_nu_cpbt_cble, NUMERODIASATRASO=v_nu_dias_atra, SECUENCIAABONO=v_qs_abno, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred, TIPORECUPERACION=v_ti_recp, TIPOREVERSO=v_ti_revz WHERE ANIOCREDITO=v_aa_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper recuperacionCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper recuperacionCredito_type: ' || v_err);
END;
/

/* referenciaCliente_type -> USP_INBOX_REFERENCIACLIENTE (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_REFERENCIACLIENTE(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_refe VARCHAR2(4000);
    v_co_tref VARCHAR2(4000);
    v_fe_ingr VARCHAR2(4000);
    v_st_cart VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_refe := JSON_VALUE(p_payload, '$.sc_refe');
    v_co_tref := JSON_VALUE(p_payload, '$.co_tref');
    v_fe_ingr := JSON_VALUE(p_payload, '$.fe_ingr');
    v_st_cart := JSON_VALUE(p_payload, '$.st_cart');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REFERENCIACLIENTE_TYPE WHERE SECUENCIACREDITO=v_sc_refe;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REFERENCIACLIENTE_TYPE (SECUENCIACREDITO,CODIGOCEDU,CODIGOTIPODEUD,FECHAINGRESO,ESTADOREGISTRO,CODIGORFAM)
            VALUES (v_sc_refe,v_co_tref,v_co_tref,v_fe_ingr,v_st_cart,v_co_tref);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REFERENCIACLIENTE_TYPE SET CODIGOCEDU=v_co_tref, CODIGOTIPODEUD=v_co_tref, FECHAINGRESO=v_fe_ingr, ESTADOREGISTRO=v_st_cart, CODIGORFAM=v_co_tref WHERE SECUENCIACREDITO=v_sc_refe;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper referenciaCliente_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper referenciaCliente_type: ' || v_err);
END;
/

/* referenciaDeudor_type -> USP_INBOX_REFERENCIADEUDOR (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_REFERENCIADEUDOR(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_aa_cred VARCHAR2(4000);
    v_co_tipo_deud VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_fe_crea_deud VARCHAR2(4000);
    v_fe_elim_deud VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_co_tipo_deud := JSON_VALUE(p_payload, '$.co_tipo_deud');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_fe_crea_deud := JSON_VALUE(p_payload, '$.fe_crea_deud');
    v_fe_elim_deud := JSON_VALUE(p_payload, '$.fe_elim_deud');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REFERENCIADEUDOR_TYPE WHERE ANIOCREDITO=v_aa_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REFERENCIADEUDOR_TYPE (ANIOCREDITO,CODIGOTIPODEUDOR,ESTADOREGISTRO,FECHACREACION,FECHAELIMINACION,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_aa_cred,v_co_tipo_deud,v_st_regi,v_fe_crea_deud,v_fe_elim_deud,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REFERENCIADEUDOR_TYPE SET CODIGOTIPODEUDOR=v_co_tipo_deud, ESTADOREGISTRO=v_st_regi, FECHACREACION=v_fe_crea_deud, FECHAELIMINACION=v_fe_elim_deud, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE ANIOCREDITO=v_aa_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper referenciaDeudor_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper referenciaDeudor_type: ' || v_err);
END;
/

/* refinanciamientoCreditoType -> USP_INBOX_REFICRED (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_REFICRED(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_mo_suel_liqd VARCHAR2(4000);
    v_fe_elim VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_mo_suel_liqd := JSON_VALUE(p_payload, '$.mo_suel_liqd');
    v_fe_elim := JSON_VALUE(p_payload, '$.fe_elim');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REFINANCIAMIENTOCREDITOTYPE WHERE MONTO=v_mo_suel_liqd;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REFINANCIAMIENTOCREDITOTYPE (MONTO,FECHAREPROGRAMACION)
            VALUES (v_mo_suel_liqd,v_fe_elim);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REFINANCIAMIENTOCREDITOTYPE SET FECHAREPROGRAMACION=v_fe_elim WHERE MONTO=v_mo_suel_liqd;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper refinanciamientoCreditoType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper refinanciamientoCreditoType: ' || v_err);
END;
/

/* reporteSBSCabecera_type -> USP_INBOX_REPORTESBSCABECERA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_REPORTESBSCABECERA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_usua VARCHAR2(4000);
    v_co_afil VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_fe_devo VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_usua := JSON_VALUE(p_payload, '$.co_usua');
    v_co_afil := JSON_VALUE(p_payload, '$.co_afil');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_fe_devo := JSON_VALUE(p_payload, '$.fe_devo');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REPORTESBSCABECERA_TYPE WHERE CODIGOESTRUCTURA=v_co_usua;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REPORTESBSCABECERA_TYPE (CODIGOESTRUCTURA,CODIGOENTIDAD,FECHACORTE,FECHAGENERACION,FECHAELIMINACION,CODIGOUSUARIOGENERACION,CODIGOUSUARIOCONFIRMA)
            VALUES (v_co_usua,v_co_afil,v_fe_cort,v_fe_devo,v_fe_devo,v_co_usua,v_co_usua);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REPORTESBSCABECERA_TYPE SET CODIGOENTIDAD=v_co_afil, FECHACORTE=v_fe_cort, FECHAGENERACION=v_fe_devo, FECHAELIMINACION=v_fe_devo, CODIGOUSUARIOGENERACION=v_co_usua, CODIGOUSUARIOCONFIRMA=v_co_usua WHERE CODIGOESTRUCTURA=v_co_usua;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper reporteSBSCabecera_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper reporteSBSCabecera_type: ' || v_err);
END;
/

/* reporteSBSDetalle_type -> USP_INBOX_REPORTESBSDETALLE (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_REPORTESBSDETALLE(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_rol VARCHAR2(4000);
    v_co_inst VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_co_usua VARCHAR2(4000);
    v_fe_devo VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    v_co_inst := JSON_VALUE(p_payload, '$.co_inst');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_co_usua := JSON_VALUE(p_payload, '$.co_usua');
    v_fe_devo := JSON_VALUE(p_payload, '$.fe_devo');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REPORTESBSDETALLE_TYPE WHERE CODIGOESTR=v_co_rol;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REPORTESBSDETALLE_TYPE (CODIGOESTR,CODIGOENTI,FECHACORT,CODIGOUSUARIOGENERAL,CODIGOUSUARIOCONF,FECHACONF,FECHAELIMINACION,CODIGOENTICOPECANC,FECHACORTCOPECANC,CODIGOUSUARIOGENERALCOPECANC,CODIGOUSUARIOCONFCOPECANC,CODIGOENTICOPECONC,FECHACORTCOPECONC,CODIGOUSUARIOGENERALCOPECONC,CODIGOUSUARIOCONFCOPECONC,FECHACORTCSALOPER,CODIGOUSUARIOGENERALCSALOPER,CODIGOUSUARIOCONFCSALOPER,FECHACORTCSUJRIES,CODIGOUSUARIOGENERALCSUJRIES,CODIGOUSUARIOCONFCSUJRIES)
            VALUES (v_co_rol,v_co_inst,v_fe_cort,v_co_usua,v_co_usua,v_fe_cort,v_fe_devo,v_co_inst,v_fe_cort,v_co_usua,v_co_usua,v_co_inst,v_fe_cort,v_co_usua,v_co_usua,v_fe_cort,v_co_usua,v_co_usua,v_fe_cort,v_co_usua,v_co_usua);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REPORTESBSDETALLE_TYPE SET CODIGOENTI=v_co_inst, FECHACORT=v_fe_cort, CODIGOUSUARIOGENERAL=v_co_usua, CODIGOUSUARIOCONF=v_co_usua, FECHACONF=v_fe_cort, FECHAELIMINACION=v_fe_devo, CODIGOENTICOPECANC=v_co_inst, FECHACORTCOPECANC=v_fe_cort, CODIGOUSUARIOGENERALCOPECANC=v_co_usua, CODIGOUSUARIOCONFCOPECANC=v_co_usua, CODIGOENTICOPECONC=v_co_inst, FECHACORTCOPECONC=v_fe_cort, CODIGOUSUARIOGENERALCOPECONC=v_co_usua, CODIGOUSUARIOCONFCOPECONC=v_co_usua, FECHACORTCSALOPER=v_fe_cort, CODIGOUSUARIOGENERALCSALOPER=v_co_usua, CODIGOUSUARIOCONFCSALOPER=v_co_usua, FECHACORTCSUJRIES=v_fe_cort, CODIGOUSUARIOGENERALCSUJRIES=v_co_usua, CODIGOUSUARIOCONFCSUJRIES=v_co_usua WHERE CODIGOESTR=v_co_rol;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper reporteSBSDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper reporteSBSDetalle_type: ' || v_err);
END;
/

/* reporteSBSGaranteCodeudor_type -> USP_INBOX_RPTSBSGRCOD (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RPTSBSGRCOD(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_qs_cred VARCHAR2(4000);
    v_nu_plaz VARCHAR2(4000);
    v_ti_calf VARCHAR2(4000);
    v_nu_oper_canc VARCHAR2(4000);
    v_co_inst_gara VARCHAR2(4000);
    v_fe_naci VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_nu_plaz := JSON_VALUE(p_payload, '$.nu_plaz');
    v_ti_calf := JSON_VALUE(p_payload, '$.ti_calf');
    v_nu_oper_canc := JSON_VALUE(p_payload, '$.nu_oper_canc');
    v_co_inst_gara := JSON_VALUE(p_payload, '$.co_inst_gara');
    v_fe_naci := JSON_VALUE(p_payload, '$.fe_naci');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REPORTESBSGARANTECODEUDOR_TYPE WHERE SECUENCIAREGISTRO=v_qs_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REPORTESBSGARANTECODEUDOR_TYPE (SECUENCIAREGISTRO,NUMEROCEDULA,TIPOIDENTIFICACION,NUMEROOPERACIONES,CODIGOCAUSAELIMINACIONGARANTIA,FECHAELIMINACION)
            VALUES (v_qs_cred,v_nu_plaz,v_ti_calf,v_nu_oper_canc,v_co_inst_gara,v_fe_naci);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REPORTESBSGARANTECODEUDOR_TYPE SET NUMEROCEDULA=v_nu_plaz, TIPOIDENTIFICACION=v_ti_calf, NUMEROOPERACIONES=v_nu_oper_canc, CODIGOCAUSAELIMINACIONGARANTIA=v_co_inst_gara, FECHAELIMINACION=v_fe_naci WHERE SECUENCIAREGISTRO=v_qs_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper reporteSBSGaranteCodeudor_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper reporteSBSGaranteCodeudor_type: ' || v_err);
END;
/

/* reporteSBSGarantiaReal_type -> USP_INBOX_RPTSBSGARR (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RPTSBSGARR(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_qs_cred VARCHAR2(4000);
    v_nu_gara_oper VARCHAR2(4000);
    v_ti_gara VARCHAR2(4000);
    v_nu_regi VARCHAR2(4000);
    v_ds_gara VARCHAR2(4000);
    v_st_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_nu_gara_oper := JSON_VALUE(p_payload, '$.nu_gara_oper');
    v_ti_gara := JSON_VALUE(p_payload, '$.ti_gara');
    v_nu_regi := JSON_VALUE(p_payload, '$.nu_regi');
    v_ds_gara := JSON_VALUE(p_payload, '$.ds_gara');
    v_st_cred := JSON_VALUE(p_payload, '$.st_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REPORTESBSGARANTIAREAL_TYPE WHERE SECUENCIAREGISTRO=v_qs_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REPORTESBSGARANTIAREAL_TYPE (SECUENCIAREGISTRO,NUMEROIDENTIFICACION,NUMEROOPERACIONES,TIPOGARANTIA,NUMEROGARANTIA,NUMEROREGISTROGARANTIA,DESCRIPCIONGARANTIA,ESTADOREGISTRO)
            VALUES (v_qs_cred,v_nu_gara_oper,v_nu_gara_oper,v_ti_gara,v_nu_gara_oper,v_nu_regi,v_ds_gara,v_st_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REPORTESBSGARANTIAREAL_TYPE SET NUMEROIDENTIFICACION=v_nu_gara_oper, NUMEROOPERACIONES=v_nu_gara_oper, TIPOGARANTIA=v_ti_gara, NUMEROGARANTIA=v_nu_gara_oper, NUMEROREGISTROGARANTIA=v_nu_regi, DESCRIPCIONGARANTIA=v_ds_gara, ESTADOREGISTRO=v_st_cred WHERE SECUENCIAREGISTRO=v_qs_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper reporteSBSGarantiaReal_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper reporteSBSGarantiaReal_type: ' || v_err);
END;
/

/* reporteSBSOperacionAnterior_type -> USP_INBOX_RPTSBSOPANT (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RPTSBSOPANT(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_regi VARCHAR2(4000);
    v_nu_oper VARCHAR2(4000);
    v_nu_oper_ante VARCHAR2(4000);
    v_fe_ingr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_regi := JSON_VALUE(p_payload, '$.sc_regi');
    v_nu_oper := JSON_VALUE(p_payload, '$.nu_oper');
    v_nu_oper_ante := JSON_VALUE(p_payload, '$.nu_oper_ante');
    v_fe_ingr := JSON_VALUE(p_payload, '$.fe_ingr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REPORTESBSOPERACIONANTERIOR_TYPE WHERE SECUENCIAREGISTRO=v_sc_regi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REPORTESBSOPERACIONANTERIOR_TYPE (SECUENCIAREGISTRO,NUMEROOPERACIONES,NUMEROOPERACIONANTERIOR,FECHACONCESION)
            VALUES (v_sc_regi,v_nu_oper,v_nu_oper_ante,v_fe_ingr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REPORTESBSOPERACIONANTERIOR_TYPE SET NUMEROOPERACIONES=v_nu_oper, NUMEROOPERACIONANTERIOR=v_nu_oper_ante, FECHACONCESION=v_fe_ingr WHERE SECUENCIAREGISTRO=v_sc_regi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper reporteSBSOperacionAnterior_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper reporteSBSOperacionAnterior_type: ' || v_err);
END;
/

/* reporteSBSOperacionCancelada_type -> USP_INBOX_RPTSBSOPCANC (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RPTSBSOPCANC(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_regi VARCHAR2(4000);
    v_ti_calf VARCHAR2(4000);
    v_ti_iden VARCHAR2(4000);
    v_nu_oper VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_regi := JSON_VALUE(p_payload, '$.sc_regi');
    v_ti_calf := JSON_VALUE(p_payload, '$.ti_calf');
    v_ti_iden := JSON_VALUE(p_payload, '$.ti_iden');
    v_nu_oper := JSON_VALUE(p_payload, '$.nu_oper');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REPORTESBSOPERACIONCANCELADA_TYPE WHERE SECUENCIAREGISTRO=v_sc_regi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REPORTESBSOPERACIONCANCELADA_TYPE (SECUENCIAREGISTRO,IDENTIFICACION,TIPOIDENTIFICACION,NUMEROOPERACIONES)
            VALUES (v_sc_regi,v_ti_calf,v_ti_iden,v_nu_oper);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REPORTESBSOPERACIONCANCELADA_TYPE SET IDENTIFICACION=v_ti_calf, TIPOIDENTIFICACION=v_ti_iden, NUMEROOPERACIONES=v_nu_oper WHERE SECUENCIAREGISTRO=v_sc_regi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper reporteSBSOperacionCancelada_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper reporteSBSOperacionCancelada_type: ' || v_err);
END;
/

/* reporteSBSOperacionConcedida_type -> USP_INBOX_RPTSBSOPCONC (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RPTSBSOPCONC(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_regi VARCHAR2(4000);
    v_ti_iden VARCHAR2(4000);
    v_nu_oper VARCHAR2(4000);
    v_mo_prov_requ VARCHAR2(4000);
    v_co_tamo VARCHAR2(4000);
    v_mo_capi_cred VARCHAR2(4000);
    v_mo_cuot VARCHAR2(4000);
    v_pr_inte VARCHAR2(4000);
    v_fe_docu VARCHAR2(4000);
    v_co_tipo_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_regi := JSON_VALUE(p_payload, '$.sc_regi');
    v_ti_iden := JSON_VALUE(p_payload, '$.ti_iden');
    v_nu_oper := JSON_VALUE(p_payload, '$.nu_oper');
    v_mo_prov_requ := JSON_VALUE(p_payload, '$.mo_prov_requ');
    v_co_tamo := JSON_VALUE(p_payload, '$.co_tamo');
    v_mo_capi_cred := JSON_VALUE(p_payload, '$.mo_capi_cred');
    v_mo_cuot := JSON_VALUE(p_payload, '$.mo_cuot');
    v_pr_inte := JSON_VALUE(p_payload, '$.pr_inte');
    v_fe_docu := JSON_VALUE(p_payload, '$.fe_docu');
    v_co_tipo_cred := JSON_VALUE(p_payload, '$.co_tipo_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REPORTESBSOPERACIONCONCEDIDA_TYPE WHERE SECUENCIAREGISTRO=v_sc_regi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REPORTESBSOPERACIONCONCEDIDA_TYPE (SECUENCIAREGISTRO,TIPOIDENTIFICACION,NUMEROOPERACIONES,CODIGOPROVINCIA,CODIGOCANTON,MONTOCREDITO,MONTOTEA,TASAINTERES,FECHAVENCIMIENTO,COTIPOCRED)
            VALUES (v_sc_regi,v_ti_iden,v_nu_oper,v_mo_prov_requ,v_co_tamo,v_mo_capi_cred,v_mo_cuot,v_pr_inte,v_fe_docu,v_co_tipo_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REPORTESBSOPERACIONCONCEDIDA_TYPE SET TIPOIDENTIFICACION=v_ti_iden, NUMEROOPERACIONES=v_nu_oper, CODIGOPROVINCIA=v_mo_prov_requ, CODIGOCANTON=v_co_tamo, MONTOCREDITO=v_mo_capi_cred, MONTOTEA=v_mo_cuot, TASAINTERES=v_pr_inte, FECHAVENCIMIENTO=v_fe_docu, COTIPOCRED=v_co_tipo_cred WHERE SECUENCIAREGISTRO=v_sc_regi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper reporteSBSOperacionConcedida_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper reporteSBSOperacionConcedida_type: ' || v_err);
END;
/

/* reporteSBSSaldoOperacion_type -> USP_INBOX_RPTSBSSALOP (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RPTSBSSALOP(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_regi VARCHAR2(4000);
    v_ti_iden VARCHAR2(4000);
    v_nu_oper VARCHAR2(4000);
    v_ti_calf VARCHAR2(4000);
    v_pr_inte VARCHAR2(4000);
    v_mo_venc VARCHAR2(4000);
    v_mo_ndev_inte VARCHAR2(4000);
    v_mo_capi_cred VARCHAR2(4000);
    v_mo_cuot VARCHAR2(4000);
    v_pr_inte_mora VARCHAR2(4000);
    v_mo_dema_judi VARCHAR2(4000);
    v_mo_cart_cast VARCHAR2(4000);
    v_mo_prov_cons VARCHAR2(4000);
    v_mo_prov_requ VARCHAR2(4000);
    v_nu_dias_moro VARCHAR2(4000);
    v_co_tamo VARCHAR2(4000);
    v_co_tipo_cred VARCHAR2(4000);
    v_mo_cost_oper VARCHAR2(4000);
    v_mo_cnta_indv VARCHAR2(4000);
    v_mo_suje_prov VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_regi := JSON_VALUE(p_payload, '$.sc_regi');
    v_ti_iden := JSON_VALUE(p_payload, '$.ti_iden');
    v_nu_oper := JSON_VALUE(p_payload, '$.nu_oper');
    v_ti_calf := JSON_VALUE(p_payload, '$.ti_calf');
    v_pr_inte := JSON_VALUE(p_payload, '$.pr_inte');
    v_mo_venc := JSON_VALUE(p_payload, '$.mo_venc');
    v_mo_ndev_inte := JSON_VALUE(p_payload, '$.mo_ndev_inte');
    v_mo_capi_cred := JSON_VALUE(p_payload, '$.mo_capi_cred');
    v_mo_cuot := JSON_VALUE(p_payload, '$.mo_cuot');
    v_pr_inte_mora := JSON_VALUE(p_payload, '$.pr_inte_mora');
    v_mo_dema_judi := JSON_VALUE(p_payload, '$.mo_dema_judi');
    v_mo_cart_cast := JSON_VALUE(p_payload, '$.mo_cart_cast');
    v_mo_prov_cons := JSON_VALUE(p_payload, '$.mo_prov_cons');
    v_mo_prov_requ := JSON_VALUE(p_payload, '$.mo_prov_requ');
    v_nu_dias_moro := JSON_VALUE(p_payload, '$.nu_dias_moro');
    v_co_tamo := JSON_VALUE(p_payload, '$.co_tamo');
    v_co_tipo_cred := JSON_VALUE(p_payload, '$.co_tipo_cred');
    v_mo_cost_oper := JSON_VALUE(p_payload, '$.mo_cost_oper');
    v_mo_cnta_indv := JSON_VALUE(p_payload, '$.mo_cnta_indv');
    v_mo_suje_prov := JSON_VALUE(p_payload, '$.mo_suje_prov');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REPORTESBSSALDOOPERACION_TYPE WHERE SECUENCIAREGISTRO=v_sc_regi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REPORTESBSSALDOOPERACION_TYPE (SECUENCIAREGISTRO,TIPOIDENTIFICACION,NUMEROOPERACIONES,TIPOCALIFICACION,TASAINTERES,MONTOPORVENCER,MONTOVENCIMIENTO,MONTONODEVENGAINTERES,MONTOCAPITALCREDITO,MONTOCUOTA,MONTOINTERESMORA,MONTODEMANDAJUDICIAL,MONTOCARTERACASTIGADA,MONTOPROVISIONCONSTITUIDA,MONTOPROVISIONREQUERIDA,NUMERODIASMOROSIDAD,COTAMO,COTIPOCRED,MONTOCOSTOOPERATIVP,MONTOCUENTAINDIVIDUAL,MONTOSUJETPAPROVISION)
            VALUES (v_sc_regi,v_ti_iden,v_nu_oper,v_ti_calf,v_pr_inte,v_mo_venc,v_mo_venc,v_mo_ndev_inte,v_mo_capi_cred,v_mo_cuot,v_pr_inte_mora,v_mo_dema_judi,v_mo_cart_cast,v_mo_prov_cons,v_mo_prov_requ,v_nu_dias_moro,v_co_tamo,v_co_tipo_cred,v_mo_cost_oper,v_mo_cnta_indv,v_mo_suje_prov);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REPORTESBSSALDOOPERACION_TYPE SET TIPOIDENTIFICACION=v_ti_iden, NUMEROOPERACIONES=v_nu_oper, TIPOCALIFICACION=v_ti_calf, TASAINTERES=v_pr_inte, MONTOPORVENCER=v_mo_venc, MONTOVENCIMIENTO=v_mo_venc, MONTONODEVENGAINTERES=v_mo_ndev_inte, MONTOCAPITALCREDITO=v_mo_capi_cred, MONTOCUOTA=v_mo_cuot, MONTOINTERESMORA=v_pr_inte_mora, MONTODEMANDAJUDICIAL=v_mo_dema_judi, MONTOCARTERACASTIGADA=v_mo_cart_cast, MONTOPROVISIONCONSTITUIDA=v_mo_prov_cons, MONTOPROVISIONREQUERIDA=v_mo_prov_requ, NUMERODIASMOROSIDAD=v_nu_dias_moro, COTAMO=v_co_tamo, COTIPOCRED=v_co_tipo_cred, MONTOCOSTOOPERATIVP=v_mo_cost_oper, MONTOCUENTAINDIVIDUAL=v_mo_cnta_indv, MONTOSUJETPAPROVISION=v_mo_suje_prov WHERE SECUENCIAREGISTRO=v_sc_regi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper reporteSBSSaldoOperacion_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper reporteSBSSaldoOperacion_type: ' || v_err);
END;
/

/* reporteSBSSujetoRiesgo_type -> USP_INBOX_RPTSBSSJTO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RPTSBSSJTO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_qs_cred VARCHAR2(4000);
    v_nu_ctas VARCHAR2(4000);
    v_ti_inst VARCHAR2(4000);
    v_co_prov VARCHAR2(4000);
    v_co_rol VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_Cred');
    v_nu_ctas := JSON_VALUE(p_payload, '$.nu_ctas');
    v_ti_inst := JSON_VALUE(p_payload, '$.ti_inst');
    v_co_prov := JSON_VALUE(p_payload, '$.co_prov');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.REPORTESBSSUJETORIESGO_TYPE WHERE SECUENCIAREGISTRO=v_qs_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.REPORTESBSSUJETORIESGO_TYPE (SECUENCIAREGISTRO,NUMEROCEDULA,TIPOIDENTIFICACION,CODIGOPROFESION,CODIGOPROVINCIA,CODIGOCANTON,CODIGOPARROQUIA,COGNRO)
            VALUES (v_qs_cred,v_nu_ctas,v_ti_inst,v_co_prov,v_co_prov,v_co_rol,v_co_prov,v_co_rol);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.REPORTESBSSUJETORIESGO_TYPE SET NUMEROCEDULA=v_nu_ctas, TIPOIDENTIFICACION=v_ti_inst, CODIGOPROFESION=v_co_prov, CODIGOPROVINCIA=v_co_prov, CODIGOCANTON=v_co_rol, CODIGOPARROQUIA=v_co_prov, COGNRO=v_co_rol WHERE SECUENCIAREGISTRO=v_qs_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper reporteSBSSujetoRiesgo_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper reporteSBSSujetoRiesgo_type: ' || v_err);
END;
/

/* rubroCobranza_type -> USP_INBOX_RUBROCOBRANZA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RUBROCOBRANZA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_cred VARCHAR2(4000);
    v_ti_pago VARCHAR2(4000);
    v_ti_rubr_pago VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_ti_pago := JSON_VALUE(p_payload, '$.ti_pago');
    v_ti_rubr_pago := JSON_VALUE(p_payload, '$.ti_rubr_pago');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.RUBROCOBRANZA_TYPE WHERE TIPOCREDITO=v_ti_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.RUBROCOBRANZA_TYPE (TIPOCREDITO,TIPOPAGO,TIPORUBROPAGO,CODIGOEMPRESA)
            VALUES (v_ti_cred,v_ti_pago,v_ti_rubr_pago,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.RUBROCOBRANZA_TYPE SET TIPOPAGO=v_ti_pago, TIPORUBROPAGO=v_ti_rubr_pago, CODIGOEMPRESA=v_co_empr WHERE TIPOCREDITO=v_ti_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper rubroCobranza_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper rubroCobranza_type: ' || v_err);
END;
/

/* rubrosCobranzaDetalle_type -> USP_INBOX_RUBRCOBRDETA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_RUBRCOBRDETA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_cred VARCHAR2(4000);
    v_ti_pago VARCHAR2(4000);
    v_ti_rubr_pago VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_ti_pago := JSON_VALUE(p_payload, '$.ti_pago');
    v_ti_rubr_pago := JSON_VALUE(p_payload, '$.ti_rubr_pago');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.RUBROSCOBRANZADETALLE_TYPE WHERE TIPOCRED=v_ti_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.RUBROSCOBRANZADETALLE_TYPE (TIPOCRED,TIPOPAGO,TIPORUBRPAGO,CODIGOEMPRESA)
            VALUES (v_ti_cred,v_ti_pago,v_ti_rubr_pago,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.RUBROSCOBRANZADETALLE_TYPE SET TIPOPAGO=v_ti_pago, TIPORUBRPAGO=v_ti_rubr_pago, CODIGOEMPRESA=v_co_empr WHERE TIPOCRED=v_ti_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper rubrosCobranzaDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper rubrosCobranzaDetalle_type: ' || v_err);
END;
/

/* saldoCarteraDetalle_type -> USP_INBOX_SALDOCARTERADETALLE (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SALDOCARTERADETALLE(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_fe_cort VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_nu_dcto VARCHAR2(4000);
    v_mo_sald_capi_vcdo VARCHAR2(4000);
    v_mo_sald_capi_xven VARCHAR2(4000);
    v_mo_inte_dvgo VARCHAR2(4000);
    v_mo_inte_abno VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_nu_dcto := JSON_VALUE(p_payload, '$.nu_dcto');
    v_mo_sald_capi_vcdo := JSON_VALUE(p_payload, '$.mo_sald_capi_vcdo');
    v_mo_sald_capi_xven := JSON_VALUE(p_payload, '$.mo_sald_capi_xven');
    v_mo_inte_dvgo := JSON_VALUE(p_payload, '$.mo_inte_dvgo');
    v_mo_inte_abno := JSON_VALUE(p_payload, '$.mo_inte_abno');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SALDOCARTERADETALLE_TYPE WHERE FECHACORT=v_fe_cort;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SALDOCARTERADETALLE_TYPE (FECHACORT,TIPOCREDITO,ANIOCREDITO,SECUENCIACREDITO,NUMERODCTO,MONTOSALDOCAPIVCDO,MONTOSALDOCAPIXVEN,MONTOINTEDVGO,MONTOINTEABNO,CODIGOEMPRESA)
            VALUES (v_fe_cort,v_ti_cred,v_aa_cred,v_qs_cred,v_nu_dcto,v_mo_sald_capi_vcdo,v_mo_sald_capi_xven,v_mo_inte_dvgo,v_mo_inte_abno,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SALDOCARTERADETALLE_TYPE SET TIPOCREDITO=v_ti_cred, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, NUMERODCTO=v_nu_dcto, MONTOSALDOCAPIVCDO=v_mo_sald_capi_vcdo, MONTOSALDOCAPIXVEN=v_mo_sald_capi_xven, MONTOINTEDVGO=v_mo_inte_dvgo, MONTOINTEABNO=v_mo_inte_abno, CODIGOEMPRESA=v_co_empr WHERE FECHACORT=v_fe_cort;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper saldoCarteraDetalle_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper saldoCarteraDetalle_type: ' || v_err);
END;
/

/* saldoCartera_type -> USP_INBOX_SALDOCARTERA (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SALDOCARTERA(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_cred VARCHAR2(4000);
    v_co_fond VARCHAR2(4000);
    v_fe_sald VARCHAR2(4000);
    v_mo_capi VARCHAR2(4000);
    v_mo_inte VARCHAR2(4000);
    v_mo_sald_capi_xven VARCHAR2(4000);
    v_mo_sald_capi_vcdo VARCHAR2(4000);
    v_mo_abno_capi_xven VARCHAR2(4000);
    v_mo_abno_capi_vcdo VARCHAR2(4000);
    v_mo_abno_inte VARCHAR2(4000);
    v_mo_abno_mora VARCHAR2(4000);
    v_mo_inte_reve_vcdo VARCHAR2(4000);
    v_nu_oper VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_mo_abno_capi VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_co_fond := JSON_VALUE(p_payload, '$.co_fond');
    v_fe_sald := JSON_VALUE(p_payload, '$.fe_sald');
    v_mo_capi := JSON_VALUE(p_payload, '$.mo_capi');
    v_mo_inte := JSON_VALUE(p_payload, '$.mo_inte');
    v_mo_sald_capi_xven := JSON_VALUE(p_payload, '$.mo_sald_capi_xven');
    v_mo_sald_capi_vcdo := JSON_VALUE(p_payload, '$.mo_sald_capi_vcdo');
    v_mo_abno_capi_xven := JSON_VALUE(p_payload, '$.mo_abno_capi_xven');
    v_mo_abno_capi_vcdo := JSON_VALUE(p_payload, '$.mo_abno_capi_vcdo');
    v_mo_abno_inte := JSON_VALUE(p_payload, '$.mo_abno_inte');
    v_mo_abno_mora := JSON_VALUE(p_payload, '$.mo_abno_mora');
    v_mo_inte_reve_vcdo := JSON_VALUE(p_payload, '$.mo_inte_reve_vcdo');
    v_nu_oper := JSON_VALUE(p_payload, '$.nu_oper');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_mo_abno_capi := JSON_VALUE(p_payload, '$.mo_abno_capi');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SALDOCARTERA_TYPE WHERE TIPOCREDITO=v_ti_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SALDOCARTERA_TYPE (TIPOCREDITO,CODIGOFONDO,FECHASALDO,MONTOCAPITAL,MONTOINTERES,MONTOSALDOCAPITALPORVENCER,MONTOSALDOCAPITALVENCIDO,MONTOABONADOALCAPITALPORVENCER,MONTOABONADOALCAPITALVENCIDO,MONTOABONADOALINTERES,MONTOABONOMORA,MONTOINTERESREVERSADO,MONTOINTERESDEVENGADO,MONTOINTERESABONADO,NUMEROOPERACIONES,CODIGOEMPRESA,MONTOABONOCAPITALCAPITAL,ANIOCREDITO)
            VALUES (v_ti_cred,v_co_fond,v_fe_sald,v_mo_capi,v_mo_inte,v_mo_sald_capi_xven,v_mo_sald_capi_vcdo,v_mo_abno_capi_xven,v_mo_abno_capi_vcdo,v_mo_abno_inte,v_mo_abno_mora,v_mo_inte_reve_vcdo,v_mo_inte,v_mo_inte,v_nu_oper,v_co_empr,v_mo_abno_capi,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SALDOCARTERA_TYPE SET CODIGOFONDO=v_co_fond, FECHASALDO=v_fe_sald, MONTOCAPITAL=v_mo_capi, MONTOINTERES=v_mo_inte, MONTOSALDOCAPITALPORVENCER=v_mo_sald_capi_xven, MONTOSALDOCAPITALVENCIDO=v_mo_sald_capi_vcdo, MONTOABONADOALCAPITALPORVENCER=v_mo_abno_capi_xven, MONTOABONADOALCAPITALVENCIDO=v_mo_abno_capi_vcdo, MONTOABONADOALINTERES=v_mo_abno_inte, MONTOABONOMORA=v_mo_abno_mora, MONTOINTERESREVERSADO=v_mo_inte_reve_vcdo, MONTOINTERESDEVENGADO=v_mo_inte, MONTOINTERESABONADO=v_mo_inte, NUMEROOPERACIONES=v_nu_oper, CODIGOEMPRESA=v_co_empr, MONTOABONOCAPITALCAPITAL=v_mo_abno_capi, ANIOCREDITO=v_ti_cred WHERE TIPOCREDITO=v_ti_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper saldoCartera_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper saldoCartera_type: ' || v_err);
END;
/

/* saldoCxPCxCType -> USP_INBOX_SALDOCXPCXC (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SALDOCXPCXC(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_fond VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_fond := JSON_VALUE(p_payload, '$.co_fond');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SALDOCXPCXCTYPE WHERE CODIGOPRODUCTO=v_co_fond;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SALDOCXPCXCTYPE (CODIGOPRODUCTO,CODIGOSUCURSAL,CODIGOOFICINA)
            VALUES (v_co_fond,v_co_empr,v_co_fond);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SALDOCXPCXCTYPE SET CODIGOSUCURSAL=v_co_empr, CODIGOOFICINA=v_co_fond WHERE CODIGOPRODUCTO=v_co_fond;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper saldoCxPCxCType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper saldoCxPCxCType: ' || v_err);
END;
/

/* saldoVinculadoType -> USP_INBOX_SALDOVINCULADO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SALDOVINCULADO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_empr VARCHAR2(4000);
    v_co_fond VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_co_fond := JSON_VALUE(p_payload, '$.co_fond');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SALDOVINCULADOTYPE WHERE CODIGOSUCURSAL=v_co_empr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SALDOVINCULADOTYPE (CODIGOSUCURSAL,CODIGOOFICINA)
            VALUES (v_co_empr,v_co_fond);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SALDOVINCULADOTYPE SET CODIGOOFICINA=v_co_fond WHERE CODIGOSUCURSAL=v_co_empr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper saldoVinculadoType insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper saldoVinculadoType: ' || v_err);
END;
/

/* seguimientoAutorizacion_type -> USP_INBOX_SEGAUTR (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SEGAUTR(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_segi VARCHAR2(4000);
    v_co_moti VARCHAR2(4000);
    v_co_prov VARCHAR2(4000);
    v_st_segi VARCHAR2(4000);
    v_ds_obsr VARCHAR2(4000);
    v_fe_actu VARCHAR2(4000);
    v_co_usua_tran VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_segi := JSON_VALUE(p_payload, '$.sc_segi');
    v_co_moti := JSON_VALUE(p_payload, '$.co_moti');
    v_co_prov := JSON_VALUE(p_payload, '$.co_prov');
    v_st_segi := JSON_VALUE(p_payload, '$.st_segi');
    v_ds_obsr := JSON_VALUE(p_payload, '$.ds_obsr');
    v_fe_actu := JSON_VALUE(p_payload, '$.fe_actu');
    v_co_usua_tran := JSON_VALUE(p_payload, '$.co_usua_tran');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SEGUIMIENTOAUTORIZACION_TYPE WHERE SECUENCIASEGUIMIENTO=v_sc_segi;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SEGUIMIENTOAUTORIZACION_TYPE (SECUENCIASEGUIMIENTO,CODIGOMOTIVORECHAZO,CODIGOPROVINCIA,ESTADOSEGUIMIENTO,DESCRIPCIONOBSERVACIONES,FECHAACTUALIZACION,CODIGOUSUARIOTRANSACCION,ANIOCREDITO,CODIGOEMPRESA,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_sc_segi,v_co_moti,v_co_prov,v_st_segi,v_ds_obsr,v_fe_actu,v_co_usua_tran,v_aa_cred,v_co_empr,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SEGUIMIENTOAUTORIZACION_TYPE SET CODIGOMOTIVORECHAZO=v_co_moti, CODIGOPROVINCIA=v_co_prov, ESTADOSEGUIMIENTO=v_st_segi, DESCRIPCIONOBSERVACIONES=v_ds_obsr, FECHAACTUALIZACION=v_fe_actu, CODIGOUSUARIOTRANSACCION=v_co_usua_tran, ANIOCREDITO=v_aa_cred, CODIGOEMPRESA=v_co_empr, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE SECUENCIASEGUIMIENTO=v_sc_segi;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper seguimientoAutorizacion_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper seguimientoAutorizacion_type: ' || v_err);
END;
/

/* seguroCredito_type -> USP_INBOX_SEGUROCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SEGUROCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_moti VARCHAR2(4000);
    v_sc_segu VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_moti := JSON_VALUE(p_payload, '$.co_moti');
    v_sc_segu := JSON_VALUE(p_payload, '$.sc_segu');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SEGUROCREDITO_TYPE WHERE CODIGOPROVEEDOR=v_co_moti;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SEGUROCREDITO_TYPE (CODIGOPROVEEDOR,SECUENCIACREDITO)
            VALUES (v_co_moti,v_sc_segu);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SEGUROCREDITO_TYPE SET SECUENCIACREDITO=v_sc_segu WHERE CODIGOPROVEEDOR=v_co_moti;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper seguroCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper seguroCredito_type: ' || v_err);
END;
/

/* sobranteCaucion_type -> USP_INBOX_SOBRANTECAUCION (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SOBRANTECAUCION(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_sobr VARCHAR2(4000);
    v_co_fond VARCHAR2(4000);
    v_va_cnta_auto VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_sobr := JSON_VALUE(p_payload, '$.ti_sobr');
    v_co_fond := JSON_VALUE(p_payload, '$.co_fond');
    v_va_cnta_auto := JSON_VALUE(p_payload, '$.va_cnta_auto');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SOBRANTECAUCIÓN_TYPE WHERE TIPOSOBRANTE=v_ti_sobr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SOBRANTECAUCIÓN_TYPE (TIPOSOBRANTE,CODIGOFONDO,VACUENTAAUTO,CODIGOEMPRESA)
            VALUES (v_ti_sobr,v_co_fond,v_va_cnta_auto,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SOBRANTECAUCIÓN_TYPE SET CODIGOFONDO=v_co_fond, VACUENTAAUTO=v_va_cnta_auto, CODIGOEMPRESA=v_co_empr WHERE TIPOSOBRANTE=v_ti_sobr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper sobranteCaucion_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper sobranteCaucion_type: ' || v_err);
END;
/

/* sobranteCredito_type -> USP_INBOX_SOBRANTECREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SOBRANTECREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_sobr VARCHAR2(4000);
    v_ti_sobr VARCHAR2(4000);
    v_ti_apli VARCHAR2(4000);
    v_ti_pago VARCHAR2(4000);
    v_ci_rol VARCHAR2(4000);
    v_co_fond VARCHAR2(4000);
    v_co_prov VARCHAR2(4000);
    v_ti_inst VARCHAR2(4000);
    v_co_paga VARCHAR2(4000);
    v_sc_rol VARCHAR2(4000);
    v_sc_reca VARCHAR2(4000);
    v_mo_sobr VARCHAR2(4000);
    v_mo_disp VARCHAR2(4000);
    v_ds_oper_refe VARCHAR2(4000);
    v_nu_cpbt_cble VARCHAR2(4000);
    v_fe_cort VARCHAR2(4000);
    v_fe_proc VARCHAR2(4000);
    v_co_empr VARCHAR2(4000);
    v_st_devo VARCHAR2(4000);
    v_fe_devo VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_sobr := JSON_VALUE(p_payload, '$.sc_sobr');
    v_ti_sobr := JSON_VALUE(p_payload, '$.ti_sobr');
    v_ti_apli := JSON_VALUE(p_payload, '$.ti_apli');
    v_ti_pago := JSON_VALUE(p_payload, '$.ti_pago');
    v_ci_rol := JSON_VALUE(p_payload, '$.ci_rol');
    v_co_fond := JSON_VALUE(p_payload, '$.co_fond');
    v_co_prov := JSON_VALUE(p_payload, '$.co_prov');
    v_ti_inst := JSON_VALUE(p_payload, '$.ti_inst');
    v_co_paga := JSON_VALUE(p_payload, '$.co_paga');
    v_sc_rol := JSON_VALUE(p_payload, '$.sc_rol');
    v_sc_reca := JSON_VALUE(p_payload, '$.sc_reca');
    v_mo_sobr := JSON_VALUE(p_payload, '$.mo_sobr');
    v_mo_disp := JSON_VALUE(p_payload, '$.mo_disp');
    v_ds_oper_refe := JSON_VALUE(p_payload, '$.ds_oper_refe');
    v_nu_cpbt_cble := JSON_VALUE(p_payload, '$.nu_cpbt_cble');
    v_fe_cort := JSON_VALUE(p_payload, '$.fe_cort');
    v_fe_proc := JSON_VALUE(p_payload, '$.fe_proc');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    v_st_devo := JSON_VALUE(p_payload, '$.st_devo');
    v_fe_devo := JSON_VALUE(p_payload, '$.fe_devo');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SOBRANTECREDITO_TYPE WHERE SECUENCIASOBRANTES=v_sc_sobr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SOBRANTECREDITO_TYPE (SECUENCIASOBRANTES,TIPOSOBRANTE,TIPOAPLICACION,TIPOPAGO,CEDULAPROVEEDOR,CODIGOFONDO,CODIGOPROVINCIA,TIPOINSTITUCION,CODIGOPAGO,CODIGOROL,SECUENCIAROL,SECUENCIARECAUDACION,MONTOSOBRANTE,MONTODISPONIBLESOBRANTE,DESCRIPCIONOPERACIONREFERENCIA,NUMEROCOMPROBANTECONTABLE,FECHACORTE,FECHAPROCESO,CODIGOEMPRESA,CODIGOINSTICION,ESTADODEVOLUCIONESMASIVAS,FECHALADEVOLUCION)
            VALUES (v_sc_sobr,v_ti_sobr,v_ti_apli,v_ti_pago,v_ci_rol,v_co_fond,v_co_prov,v_ti_inst,v_co_paga,v_co_fond,v_sc_rol,v_sc_reca,v_mo_sobr,v_mo_disp,v_ds_oper_refe,v_nu_cpbt_cble,v_fe_cort,v_fe_proc,v_co_empr,v_ti_inst,v_st_devo,v_fe_devo);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SOBRANTECREDITO_TYPE SET TIPOSOBRANTE=v_ti_sobr, TIPOAPLICACION=v_ti_apli, TIPOPAGO=v_ti_pago, CEDULAPROVEEDOR=v_ci_rol, CODIGOFONDO=v_co_fond, CODIGOPROVINCIA=v_co_prov, TIPOINSTITUCION=v_ti_inst, CODIGOPAGO=v_co_paga, CODIGOROL=v_co_fond, SECUENCIAROL=v_sc_rol, SECUENCIARECAUDACION=v_sc_reca, MONTOSOBRANTE=v_mo_sobr, MONTODISPONIBLESOBRANTE=v_mo_disp, DESCRIPCIONOPERACIONREFERENCIA=v_ds_oper_refe, NUMEROCOMPROBANTECONTABLE=v_nu_cpbt_cble, FECHACORTE=v_fe_cort, FECHAPROCESO=v_fe_proc, CODIGOEMPRESA=v_co_empr, CODIGOINSTICION=v_ti_inst, ESTADODEVOLUCIONESMASIVAS=v_st_devo, FECHALADEVOLUCION=v_fe_devo WHERE SECUENCIASOBRANTES=v_sc_sobr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper sobranteCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper sobranteCredito_type: ' || v_err);
END;
/

/* sobranteDistribucion_type -> USP_INBOX_SOBRANTEDISTRIBUCION (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SOBRANTEDISTRIBUCION(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_sc_sobr VARCHAR2(4000);
    v_sc_rol VARCHAR2(4000);
    v_ti_apli VARCHAR2(4000);
    v_ds_obse VARCHAR2(4000);
    v_nu_cpbt_cble VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_sc_sobr := JSON_VALUE(p_payload, '$.sc_sobr');
    v_sc_rol := JSON_VALUE(p_payload, '$.sc_rol');
    v_ti_apli := JSON_VALUE(p_payload, '$.ti_apli');
    v_ds_obse := JSON_VALUE(p_payload, '$.ds_obse');
    v_nu_cpbt_cble := JSON_VALUE(p_payload, '$.nu_cpbt_cble');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SOBRANTEDISTRIBUCION_TYPE WHERE SECUENCIASOBRANTES=v_sc_sobr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SOBRANTEDISTRIBUCION_TYPE (SECUENCIASOBRANTES,SECUENCIAREGISTRO,TIPOAPLICACION,MONTOAPLICADO,DESCRIPCIONREFERENCIA,NUMEROCOMPROBANTECONTABLE,FECHAAPLICACION)
            VALUES (v_sc_sobr,v_sc_rol,v_ti_apli,v_ti_apli,v_ds_obse,v_nu_cpbt_cble,v_ti_apli);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SOBRANTEDISTRIBUCION_TYPE SET SECUENCIAREGISTRO=v_sc_rol, TIPOAPLICACION=v_ti_apli, MONTOAPLICADO=v_ti_apli, DESCRIPCIONREFERENCIA=v_ds_obse, NUMEROCOMPROBANTECONTABLE=v_nu_cpbt_cble, FECHAAPLICACION=v_ti_apli WHERE SECUENCIASOBRANTES=v_sc_sobr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper sobranteDistribucion_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper sobranteDistribucion_type: ' || v_err);
END;
/

/* solidarioCredito_type -> USP_INBOX_SOLIDARIOCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_SOLIDARIOCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_aa_cred VARCHAR2(4000);
    v_in_soli VARCHAR2(4000);
    v_mo_cuot VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_in_soli := JSON_VALUE(p_payload, '$.in_soli');
    v_mo_cuot := JSON_VALUE(p_payload, '$.mo_cuot');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.SOLIDARIOCREDITO_TYPE WHERE ANIOCREDITO=v_aa_cred;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.SOLIDARIOCREDITO_TYPE (ANIOCREDITO,INDICADORSOLIDARIO,MONTOCUOTA,SECUENCIACREDITO,TIPOCREDITO)
            VALUES (v_aa_cred,v_in_soli,v_mo_cuot,v_qs_cred,v_ti_cred);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.SOLIDARIOCREDITO_TYPE SET INDICADORSOLIDARIO=v_in_soli, MONTOCUOTA=v_mo_cuot, SECUENCIACREDITO=v_qs_cred, TIPOCREDITO=v_ti_cred WHERE ANIOCREDITO=v_aa_cred;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper solidarioCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper solidarioCredito_type: ' || v_err);
END;
/

/* tasaInteresCredito_type -> USP_INBOX_TASAINTERESCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_TASAINTERESCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_co_empr VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_co_empr := JSON_VALUE(p_payload, '$.co_empr');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.TASAINTERESCREDITO_TYPE WHERE CODIGOMONEDA=v_co_empr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.TASAINTERESCREDITO_TYPE (CODIGOMONEDA,CODIGOEMPRESA)
            VALUES (v_co_empr,v_co_empr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.TASAINTERESCREDITO_TYPE SET CODIGOEMPRESA=v_co_empr WHERE CODIGOMONEDA=v_co_empr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper tasaInteresCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper tasaInteresCredito_type: ' || v_err);
END;
/

/* tipoCredito_type -> USP_INBOX_TIPOCREDITO (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_TIPOCREDITO(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ds_tcre VARCHAR2(4000);
    v_co_grup_fcme VARCHAR2(4000);
    v_st_tcre VARCHAR2(4000);
    v_co_tcre VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ds_tcre := JSON_VALUE(p_payload, '$.ds_tcre');
    v_co_grup_fcme := JSON_VALUE(p_payload, '$.co_grup_fcme');
    v_st_tcre := JSON_VALUE(p_payload, '$.st_tcre');
    v_co_tcre := JSON_VALUE(p_payload, '$.co_tcre');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.TIPOCREDITO_TYPE WHERE CODIGOGRUPO=v_co_grup_fcme;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.TIPOCREDITO_TYPE (DESCRIPCIONCREDITO,CODIGOGRUPO,ESTADOCREDITO,ESTADOGARANTE,CODIGOEMPRESA)
            VALUES (v_ds_tcre,v_co_grup_fcme,v_st_tcre,v_st_tcre,v_co_tcre);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.TIPOCREDITO_TYPE SET DESCRIPCIONCREDITO=v_ds_tcre, ESTADOCREDITO=v_st_tcre, ESTADOGARANTE=v_st_tcre, CODIGOEMPRESA=v_co_tcre WHERE CODIGOGRUPO=v_co_grup_fcme;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper tipoCredito_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper tipoCredito_type: ' || v_err);
END;
/

/* tipoSobrante_type -> USP_INBOX_TIPOSOBRANTE (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_TIPOSOBRANTE(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_sobr VARCHAR2(4000);
    v_ds_obse VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_sobr := JSON_VALUE(p_payload, '$.ti_sobr');
    v_ds_obse := JSON_VALUE(p_payload, '$.ds_obse');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.TIPOSOBRANTE_TYPE WHERE TIPOSOBRANTE=v_ti_sobr;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.TIPOSOBRANTE_TYPE (TIPOSOBRANTE,DESCRIPCIONSOBRANTE,ESTADOSOBRANTE)
            VALUES (v_ti_sobr,v_ds_obse,v_ti_sobr);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.TIPOSOBRANTE_TYPE SET DESCRIPCIONSOBRANTE=v_ds_obse, ESTADOSOBRANTE=v_ti_sobr WHERE TIPOSOBRANTE=v_ti_sobr;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper tipoSobrante_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper tipoSobrante_type: ' || v_err);
END;
/

/* transaccionRecuperacion_type -> USP_INBOX_TRANSRECUP (OK) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_TRANSRECUP(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_ti_recp VARCHAR2(4000);
    v_st_autr VARCHAR2(4000);
    v_in_cble_revz VARCHAR2(4000);
    v_ti_cred VARCHAR2(4000);
    v_aa_cred VARCHAR2(4000);
    v_qs_cred VARCHAR2(4000);
    v_qs_abno VARCHAR2(4000);
    v_st_regi VARCHAR2(4000);
    v_fe_mvto VARCHAR2(4000);
    v_fe_abno VARCHAR2(4000);
    v_fe_cble VARCHAR2(4000);
    v_fe_revz VARCHAR2(4000);
    v_mo_mvto VARCHAR2(4000);
    v_nu_cpbt_cble VARCHAR2(4000);
    v_in_conf_fond VARCHAR2(4000);
    v_co_usua_liqd VARCHAR2(4000);
    v_ds_liqd VARCHAR2(4000);
    v_co_usua_conf VARCHAR2(4000);
    v_nu_dias_atra VARCHAR2(4000);
    v_ti_revz VARCHAR2(4000);
    v_co_rol VARCHAR2(4000);
    v_co_usua_revz VARCHAR2(4000);
    v_ti_diar VARCHAR2(4000);
    v_err VARCHAR2(500);
BEGIN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('is_replicating');
    v_ti_recp := JSON_VALUE(p_payload, '$.ti_recp');
    v_st_autr := JSON_VALUE(p_payload, '$.st_autr');
    v_in_cble_revz := JSON_VALUE(p_payload, '$.in_cble_revz');
    v_ti_cred := JSON_VALUE(p_payload, '$.ti_cred');
    v_aa_cred := JSON_VALUE(p_payload, '$.aa_cred');
    v_qs_cred := JSON_VALUE(p_payload, '$.qs_cred');
    v_qs_abno := JSON_VALUE(p_payload, '$.qs_abno');
    v_st_regi := JSON_VALUE(p_payload, '$.st_regi');
    v_fe_mvto := JSON_VALUE(p_payload, '$.fe_mvto');
    v_fe_abno := JSON_VALUE(p_payload, '$.fe_abno');
    v_fe_cble := JSON_VALUE(p_payload, '$.fe_cble');
    v_fe_revz := JSON_VALUE(p_payload, '$.fe_revz');
    v_mo_mvto := JSON_VALUE(p_payload, '$.mo_mvto');
    v_nu_cpbt_cble := JSON_VALUE(p_payload, '$.nu_cpbt_cble');
    v_in_conf_fond := JSON_VALUE(p_payload, '$.in_conf_fond');
    v_co_usua_liqd := JSON_VALUE(p_payload, '$.co_usua_liqd');
    v_ds_liqd := JSON_VALUE(p_payload, '$.ds_liqd');
    v_co_usua_conf := JSON_VALUE(p_payload, '$.co_usua_conf');
    v_nu_dias_atra := JSON_VALUE(p_payload, '$.nu_dias_atra');
    v_ti_revz := JSON_VALUE(p_payload, '$.ti_revz');
    v_co_rol := JSON_VALUE(p_payload, '$.co_rol');
    v_co_usua_revz := JSON_VALUE(p_payload, '$.co_usua_revz');
    v_ti_diar := JSON_VALUE(p_payload, '$.ti_diar');
    IF p_event_type IN ('DELETE','DELETED') THEN
        DELETE FROM FCME_USER.TRANSACCIONRECUPERACION_TYPE WHERE TIPORECP=v_ti_recp;
    ELSE
        BEGIN
            INSERT INTO FCME_USER.TRANSACCIONRECUPERACION_TYPE (TIPORECP,ESTADORECP,INDICADORRECA,TIPOCREDITO,ANIOCREDITO,SECUENCIACREDITO,SECUENCIAABNO,TIPORECPRECUP,CODIGOREGISTRO,FECHAMOVIMIENTO,FECHAABNO,FECHACONTABLE,FECHAREVZ,MONTOMOVIMIENTO,NUMEROCOMPROBANTECONTABLE,INDICADORCONFFOND,CODIGOAUTORIZACION,TEXTOLIQUIDACION,CODIGOUSUARIOCONF,CODIGOUSUARIOLIQUIDACION,NUMERODIASATRA,TIPOREVZ,CODIGOROL,CODIGOUSUARIOREVZ,TIPODIARIO,INDICADORCONTABLEREVZ)
            VALUES (v_ti_recp,v_st_autr,v_in_cble_revz,v_ti_cred,v_aa_cred,v_qs_cred,v_qs_abno,v_ti_recp,v_st_regi,v_fe_mvto,v_fe_abno,v_fe_cble,v_fe_revz,v_mo_mvto,v_nu_cpbt_cble,v_in_conf_fond,v_co_usua_liqd,v_ds_liqd,v_co_usua_conf,v_co_usua_liqd,v_nu_dias_atra,v_ti_revz,v_co_rol,v_co_usua_revz,v_ti_diar,v_in_cble_revz);
        EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
            UPDATE FCME_USER.TRANSACCIONRECUPERACION_TYPE SET ESTADORECP=v_st_autr, INDICADORRECA=v_in_cble_revz, TIPOCREDITO=v_ti_cred, ANIOCREDITO=v_aa_cred, SECUENCIACREDITO=v_qs_cred, SECUENCIAABNO=v_qs_abno, TIPORECPRECUP=v_ti_recp, CODIGOREGISTRO=v_st_regi, FECHAMOVIMIENTO=v_fe_mvto, FECHAABNO=v_fe_abno, FECHACONTABLE=v_fe_cble, FECHAREVZ=v_fe_revz, MONTOMOVIMIENTO=v_mo_mvto, NUMEROCOMPROBANTECONTABLE=v_nu_cpbt_cble, INDICADORCONFFOND=v_in_conf_fond, CODIGOAUTORIZACION=v_co_usua_liqd, TEXTOLIQUIDACION=v_ds_liqd, CODIGOUSUARIOCONF=v_co_usua_conf, CODIGOUSUARIOLIQUIDACION=v_co_usua_liqd, NUMERODIASATRA=v_nu_dias_atra, TIPOREVZ=v_ti_revz, CODIGOROL=v_co_rol, CODIGOUSUARIOREVZ=v_co_usua_revz, TIPODIARIO=v_ti_diar, INDICADORCONTABLEREVZ=v_in_cble_revz WHERE TIPORECP=v_ti_recp;
        WHEN OTHERS THEN
            v_err := SUBSTR(SQLERRM, 1, 400);
            INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
            VALUES (p_id, p_aggregate_type, p_event_type,
                    'wrapper transaccionRecuperacion_type insert: ' || v_err);
        END;
    END IF;
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
EXCEPTION WHEN OTHERS THEN
    SYS.DBMS_APPLICATION_INFO.SET_CLIENT_INFO('');
    v_err := SUBSTR(SQLERRM, 1, 500);
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper transaccionRecuperacion_type: ' || v_err);
END;
/

/* unidadJudicialType -> USP_INBOX_UNIDADJUDICIAL (STUB) */
CREATE OR REPLACE PROCEDURE FCME_USER.USP_INBOX_UNIDADJUDICIAL(
    p_id IN NUMBER, p_aggregate_type IN VARCHAR2, p_source_table IN VARCHAR2,
    p_event_type IN VARCHAR2, p_payload IN CLOB
) AS
    v_err VARCHAR2(500);
BEGIN
    -- STUB: sin column mapping disponible para unidadJudicialType
    INSERT INTO FCME_USER.CDC_INBOX_ERRORS(INBOX_ID,AGGREGATE_TYPE,EVENT_TYPE,ERROR_MESSAGE)
    VALUES (p_id, p_aggregate_type, p_event_type, 'wrapper unidadJudicialType: STUB sin column mapping');
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

