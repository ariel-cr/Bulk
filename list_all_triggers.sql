-- ============================================================
-- LISTADO COMPLETO DE TRIGGERS OUTBOX (modulo PARTICIPE)
-- Snapshot generado del estado actual de la BD
-- ============================================================

-- ============================================================
-- FLUJO 1: triggers en BDs legacy (publican a fcme_canonicos.cdc_outbox)
-- ============================================================
-- formato: USE [db]; GO; trg_name on tabla

-- [dbIM] 2 triggers
USE [dbIM];
GO
-- trg_outbox_imtbbene_firm                                ON dbo.imtbbene_firm  
-- trg_outbox_imtbmiem_cony                                ON dbo.imtbmiem_cony  

-- [dbFC] 60 triggers
USE [dbFC];
GO
-- trg_outbox_fctbactv_suje_cred                           ON dbo.fctbactv_suje_cred  
-- trg_outbox_fctbafil_actu                                ON dbo.fctbafil_actu  
-- trg_outbox_fctbafil_ahor_refe                           ON dbo.fctbafil_ahor_refe  
-- trg_outbox_fctbafil_auto_docs                           ON dbo.fctbafil_auto_docs  
-- trg_outbox_fctbafil_dcap                                ON dbo.fctbafil_dcap  
-- trg_outbox_fctbafil_gast_pers                           ON dbo.fctbafil_gast_pers  
-- trg_outbox_fctbafil_info_actu_docs                      ON dbo.fctbafil_info_actu_docs  
-- trg_outbox_fctbafil_info_adic                           ON dbo.fctbafil_info_adic  
-- trg_outbox_fctbafil_unif                                ON dbo.fctbafil_unif  
-- trg_outbox_fctbagen_mail                                ON dbo.fctbagen_mail  
-- trg_outbox_fctbagen_telf_part                           ON dbo.fctbagen_telf_part  
-- trg_outbox_fctbagru_moti_repo                           ON dbo.fctbagru_moti_repo  
-- trg_outbox_fctbarea_lbrl                                ON dbo.fctbarea_lbrl  
-- trg_outbox_fctbaudi_actu_afil                           ON dbo.fctbaudi_actu_afil  
-- trg_outbox_fctbaudi_movi                                ON dbo.fctbaudi_movi  
-- trg_outbox_fctbcart_rpag                                ON dbo.fctbcart_rpag  
-- trg_outbox_fctbcinf_part_sibs                           ON dbo.fctbcinf_part_sibs  
-- trg_outbox_fctbcser_adic                                ON dbo.fctbcser_adic  
-- trg_outbox_fctbdinf_liqd_cnta_sibs                      ON dbo.fctbdinf_liqd_cnta_sibs  
-- trg_outbox_fctbdinf_part_sibs                           ON dbo.fctbdinf_part_sibs  
-- trg_outbox_fctbesta_civi                                ON dbo.fctbesta_civi  
-- trg_outbox_fctbfcha_afil                                ON dbo.fctbfcha_afil  
-- trg_outbox_fctbfcha_afil_dcto                           ON dbo.fctbfcha_afil_dcto  
-- trg_outbox_fctbgene_sibs                                ON dbo.fctbgene_sibs  
-- trg_outbox_fctbinst_info_adic                           ON dbo.fctbinst_info_adic  
-- trg_outbox_fctbjuri_inst                                ON dbo.fctbjuri_inst  
-- trg_outbox_fctbotro_ingr_afil                           ON dbo.fctbotro_ingr_afil  
-- trg_outbox_fctbotro_ingr_cony                           ON dbo.fctbotro_ingr_cony  
-- trg_outbox_fctbpara_serv_adic                           ON dbo.fctbpara_serv_adic  
-- trg_outbox_fctbpart_foto                                ON dbo.fctbpart_foto  
-- trg_outbox_fctbrubr_rent                                ON dbo.fctbrubr_rent  
-- trg_outbox_fctbrvol_esta_afil                           ON dbo.fctbrvol_esta_afil  
-- trg_outbox_fctbsald_diar_afil_rubr                      ON dbo.fctbsald_diar_afil_rubr  
-- trg_outbox_fctbsald_diar_rubr                           ON dbo.fctbsald_diar_rubr  
-- trg_outbox_sfct_afiliado                                ON dbo.sfct_afiliado  
-- trg_outbox_sfct_afiliado_auditor                        ON dbo.sfct_afiliado_auditor  
-- trg_outbox_sfct_afiliado_fondos                         ON dbo.sfct_afiliado_fondos  
-- trg_outbox_sfct_afiliado_otros                          ON dbo.sfct_afiliado_otros  
-- trg_outbox_sfct_afiliado_referencias                    ON dbo.sfct_afiliado_referencias  
-- trg_outbox_sfct_afiliado_rubro                          ON dbo.sfct_afiliado_rubro  
-- trg_outbox_sfct_banco                                   ON dbo.sfct_banco  
-- trg_outbox_sfct_beneficiario                            ON dbo.sfct_beneficiario  
-- trg_outbox_sfct_beneficiario_retiro                     ON dbo.sfct_beneficiario_retiro  
-- trg_outbox_sfct_cabecera_rol                            ON dbo.sfct_cabecera_rol  
-- trg_outbox_sfct_ciudad                                  ON dbo.sfct_ciudad  
-- trg_outbox_sfct_conyuge                                 ON dbo.sfct_conyuge  
-- trg_outbox_sfct_detalle_rol                             ON dbo.sfct_detalle_rol  
-- trg_outbox_sfct_firmante                                ON dbo.sfct_firmante  
-- trg_outbox_sfct_grupo_fami                              ON dbo.sfct_grupo_fami  
-- trg_outbox_sfct_institucion                             ON dbo.sfct_institucion  
-- trg_outbox_sfct_motivo                                  ON dbo.sfct_motivo  
-- trg_outbox_sfct_motivo_cnta_cble                        ON dbo.sfct_motivo_cnta_cble  
-- trg_outbox_sfct_motivo_mant_afiliados                   ON dbo.sfct_motivo_mant_afiliados  
-- trg_outbox_sfct_movimiento                              ON dbo.sfct_movimiento  
-- trg_outbox_sfct_movimiento_temp                         ON dbo.sfct_movimiento_temp  
-- trg_outbox_sfct_padbs                                   ON dbo.sfct_padbs  
-- trg_outbox_sfct_referencias                             ON dbo.sfct_referencias  
-- trg_outbox_sfct_retiro                                  ON dbo.sfct_retiro  
-- trg_outbox_sfct_rubro_rol                               ON dbo.sfct_rubro_rol  
-- trg_outbox_sfct_saldos_diarios_afiliados                ON dbo.sfct_saldos_diarios_afiliados  

-- [dbCR] 2 triggers
USE [dbCR];
GO
-- trg_outbox_crtboper_cony                                ON dbo.crtboper_cony  
-- trg_outbox_crtoblig                                     ON dbo.crtoblig  

-- [dbCG] 1 triggers
USE [dbCG];
GO
-- trg_outbox_cgtbprvd                                     ON dbo.cgtbprvd  

-- [dbCT] 4 triggers
USE [dbCT];
GO
-- trg_outbox_cttbafil_audi                                ON dbo.cttbafil_audi  
-- trg_outbox_cttbcomi_cred                                ON dbo.cttbcomi_cred  
-- trg_outbox_cttbmatr_dist_afil                           ON dbo.cttbmatr_dist_afil  
-- trg_outbox_cttbtabl_afil                                ON dbo.cttbtabl_afil  

-- [dbNO] 2 triggers
USE [dbNO];
GO
-- trg_outbox_notbcgfm                                     ON dbo.notbcgfm  
-- trg_outbox_notbempl                                     ON dbo.notbempl  

-- [dbSV] 9 triggers
USE [dbSV];
GO
-- trg_outbox_svtbcaus                                     ON dbo.svtbcaus  
-- trg_outbox_svtbdisc                                     ON dbo.svtbdisc  
-- trg_outbox_svtbefec                                     ON dbo.svtbefec  
-- trg_outbox_svtbfmpg                                     ON dbo.svtbfmpg  
-- trg_outbox_svtbstro                                     ON dbo.svtbstro  
-- trg_outbox_svtbstro_bene                                ON dbo.svtbstro_bene  
-- trg_outbox_svtbstro_cred                                ON dbo.svtbstro_cred  
-- trg_outbox_svtbstro_deta                                ON dbo.svtbstro_deta  
-- trg_outbox_svtbstro_exte                                ON dbo.svtbstro_exte  

-- TOTAL FLUJO 1 = 80 triggers

-- ============================================================
-- FLUJO 2: triggers en Oracle FCME_USER (publican a FCME_USER.CDC_OUTBOX)
-- ============================================================

-- TOTAL FLUJO 2 = 42 triggers

-- FCME_USER.TRG_OUTBOX_ACTUALIZACION_AFILI           ON FCME_USER.ACTUALIZACION_AFILIADO_TYPE         (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_ACTUALIZACION_AFILIADO_TY     ON FCME_USER.ACTUALIZACION_AFILIADO_TYPE         (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_ACTUALIZACION_DOCUM           ON FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE       (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_ACTUALIZACION_DOCUMENTOS_     ON FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE       (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_AGENDAMAILAFILIADO_TYPE       ON FCME_USER.AGENDAMAILAFILIADO_TYPE             (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_AREALABORALPARTICIP           ON FCME_USER.AREALABORALPARTICIPE_TYPE           (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_AUDITORIAAFILIADO_TYPE        ON FCME_USER.AUDITORIAAFILIADO_TYPE              (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_BENEFICIARIOPARTICIPE_TYP     ON FCME_USER.BENEFICIARIOPARTICIPE_TYPE          (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_COMISIONPARTICIPE_T           ON FCME_USER.COMISIONPARTICIPE_TYPE              (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_CUENTABANCARIAAFILIADO_TY     ON FCME_USER.CUENTABANCARIAAFILIADO_TYPE         (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_DISTRIBUCIONAFILIADO_TYPE     ON FCME_USER.DISTRIBUCIONAFILIADO_TYPE           (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_DOCUMENTACIONAFILIADO_TYP     ON FCME_USER.DOCUMENTACIONAFILIADO_TYPE          (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_FIRMANTEPARTICIPE_TYPE        ON FCME_USER.FIRMANTEPARTICIPE_TYPE              (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_GRUPOFAMILIAR_TYPE            ON FCME_USER.GRUPOFAMILIAR_TYPE                  (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_IMAGENESTYPE                  ON FCME_USER.IMAGENESTYPE                        (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_INFORMACIONADICIONALAFILI     ON FCME_USER.INFORMACIONADICIONALAFILIADO_TYPE   (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_INSTITUCION_TYPE              ON FCME_USER.INSTITUCION_TYPE                    (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_JURIDICOINFORMACION           ON FCME_USER.JURIDICOINFORMACIONBASICATYPE       (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_MOTIVOCONTABLE_TYPE           ON FCME_USER.MOTIVOCONTABLE_TYPE                 (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_MOVIMIENTOCUENTA_TYPE         ON FCME_USER.MOVIMIENTOCUENTA_TYPE               (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_MOVIMIENTOTEMPORAL_TYPE       ON FCME_USER.MOVIMIENTOTEMPORAL_TYPE             (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_NATURALINFORMACIONADICION     ON FCME_USER.NATURALINFORMACIONADICIONALTYPE     (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_NATURALINFORMACIONB           ON FCME_USER.NATURALINFORMACIONBASICATYPE        (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_NATURALINGRESOSEGRESOSTYP     ON FCME_USER.NATURALINGRESOSEGRESOSTYPE          (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_NATURALTRABAJOTYPE            ON FCME_USER.NATURALTRABAJOTYPE                  (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_OTROSINGRESOSAFILIA           ON FCME_USER.OTROSINGRESOSAFILIADO_TYPE          (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_PERSONADIRECCIONEST           ON FCME_USER.PERSONADIRECCIONESTYPE              (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_PERSONAFIRMASTYPE             ON FCME_USER.PERSONAFIRMASTYPE                   (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_PERSONAREFERENCIASBANCARI     ON FCME_USER.PERSONAREFERENCIASBANCARIASTYPE     (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_PERSONAREFERENCIASPERSONA     ON FCME_USER.PERSONAREFERENCIASPERSONALESTYPE    (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_PERSONATELEFONOSTYPE          ON FCME_USER.PERSONATELEFONOSTYPE                (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_PERSONATYPE                   ON FCME_USER.PERSONATYPE                         (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_PERSONAVINCULACIONESTYPE      ON FCME_USER.PERSONAVINCULACIONESTYPE            (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_REFERENCIAPARTICIPE           ON FCME_USER.REFERENCIAPARTICIPE_TYPE            (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_REPORTESIBSPARTICIPE_TYPE     ON FCME_USER.REPORTESIBSPARTICIPE_TYPE           (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_RETIROLIQUIDACION_TYPE        ON FCME_USER.RETIROLIQUIDACION_TYPE              (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_RETIROVOLUNTARIOESTADO_TY     ON FCME_USER.RETIROVOLUNTARIOESTADO_TYPE         (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_ROLNOMINA_TYPE                ON FCME_USER.ROLNOMINA_TYPE                      (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_SALDODIARIORUBRO_TYPE         ON FCME_USER.SALDODIARIORUBRO_TYPE               (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_SALDODIARIO_TYPE              ON FCME_USER.SALDODIARIO_TYPE                    (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_SEGUROVIDAPARTICIPE_TYPE      ON FCME_USER.SEGUROVIDAPARTICIPE_TYPE            (INSERT OR UPDATE OR DELETE) [ENABLED]
-- FCME_USER.TRG_OUTBOX_SERVICIOADICIONAL_TYPE        ON FCME_USER.SERVICIOADICIONAL_TYPE              (INSERT OR UPDATE OR DELETE) [ENABLED]

-- ============================================================
-- DUPLICADOS (mismo table_name con >1 trigger outbox)
-- ============================================================
-- ACTUALIZACION_AFILIADO_TYPE         -> 2 triggers
-- ACTUALIZACION_DOCUMENTOS_TYPE       -> 2 triggers