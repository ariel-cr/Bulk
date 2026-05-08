/* =====================================================================
   CDC OUTBOX TRIGGERS - Modulo PARTICIPE
   Generado automaticamente. Patron: AFTER INSERT/UPDATE/DELETE
   Anti-loop via SESSION_CONTEXT('cdc_origin').
   Target: fcme_canonicos.dbo.cdc_outbox
   ===================================================================== */


/* ------------------------------------------------------------------
   BD ORIGINAL: dbCG
   ------------------------------------------------------------------ */
USE [dbCG];
GO
IF OBJECT_ID(N'dbo.trg_outbox_cgtbprvd', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cgtbprvd;
GO
-- Types canonicos que dependen de esta tabla: sp_personaDireccionesType,sp_personaReferenciasBancariasType,sp_personaType
CREATE TRIGGER dbo.trg_outbox_cgtbprvd
ON dbo.[cgtbprvd]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_empr], x.[co_prvd], x.[no_prvd], x.[ds_dire], x.[nu_tel1], x.[nu_tel2], x.[di_emai], x.[ti_iden], x.[nu_iden], x.[ti_pers], x.[st_prvd], x.[in_dpro], x.[ci_bnco], x.[ti_cnta], x.[nu_cnta], x.[co_loca], x.[co_dpto], x.[co_iden], x.[fe_ingr], x.[fe_modi], x.[fe_elim], x.[co_usua_ingr], x.[co_usua_modi], x.[co_usua_elim], x.[ti_prvd], x.[no_gere], x.[in_cont], x.[in_segu], x.[in_segu_cred], x.[in_conv_fcme], x.[nu_edad], x.[in_pago_bene], x.[co_iden_sri], x.[no_bene], x.[co_iden_bene], x.[ci_bnco_bene], x.[ti_cnta_bene], x.[nu_cnta_bene] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'cgtbprvd',
            @op,
            (SELECT x.[co_empr], x.[co_prvd], x.[no_prvd], x.[ds_dire], x.[nu_tel1], x.[nu_tel2], x.[di_emai], x.[ti_iden], x.[nu_iden], x.[ti_pers], x.[st_prvd], x.[in_dpro], x.[ci_bnco], x.[ti_cnta], x.[nu_cnta], x.[co_loca], x.[co_dpto], x.[co_iden], x.[fe_ingr], x.[fe_modi], x.[fe_elim], x.[co_usua_ingr], x.[co_usua_modi], x.[co_usua_elim], x.[ti_prvd], x.[no_gere], x.[in_cont], x.[in_segu], x.[in_segu_cred], x.[in_conv_fcme], x.[nu_edad], x.[in_pago_bene], x.[co_iden_sri], x.[no_bene], x.[co_iden_bene], x.[ci_bnco_bene], x.[ti_cnta_bene], x.[nu_cnta_bene] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbprvd',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_empr], x.[co_prvd], x.[no_prvd], x.[ds_dire], x.[nu_tel1], x.[nu_tel2], x.[di_emai], x.[ti_iden], x.[nu_iden], x.[ti_pers], x.[st_prvd], x.[in_dpro], x.[ci_bnco], x.[ti_cnta], x.[nu_cnta], x.[co_loca], x.[co_dpto], x.[co_iden], x.[fe_ingr], x.[fe_modi], x.[fe_elim], x.[co_usua_ingr], x.[co_usua_modi], x.[co_usua_elim], x.[ti_prvd], x.[no_gere], x.[in_cont], x.[in_segu], x.[in_segu_cred], x.[in_conv_fcme], x.[nu_edad], x.[in_pago_bene], x.[co_iden_sri], x.[no_bene], x.[co_iden_bene], x.[ci_bnco_bene], x.[ti_cnta_bene], x.[nu_cnta_bene] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'cgtbprvd',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_prvd], x.[no_prvd], x.[ds_dire], x.[nu_tel1], x.[nu_tel2], x.[di_emai], x.[ti_iden], x.[nu_iden], x.[ti_pers], x.[st_prvd], x.[in_dpro], x.[ci_bnco], x.[ti_cnta], x.[nu_cnta], x.[co_loca], x.[co_dpto], x.[co_iden], x.[fe_ingr], x.[fe_modi], x.[fe_elim], x.[co_usua_ingr], x.[co_usua_modi], x.[co_usua_elim], x.[ti_prvd], x.[no_gere], x.[in_cont], x.[in_segu], x.[in_segu_cred], x.[in_conv_fcme], x.[nu_edad], x.[in_pago_bene], x.[co_iden_sri], x.[no_bene], x.[co_iden_bene], x.[ci_bnco_bene], x.[ti_cnta_bene], x.[nu_cnta_bene] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbprvd',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO


/* ------------------------------------------------------------------
   BD ORIGINAL: dbCR
   ------------------------------------------------------------------ */
USE [dbCR];
GO
IF OBJECT_ID(N'dbo.trg_outbox_crtboper_cony', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtboper_cony;
GO
-- Types canonicos que dependen de esta tabla: sp_personaVinculacionesType
CREATE TRIGGER dbo.trg_outbox_crtboper_cony
ON dbo.[crtboper_cony]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[ti_cred]), CONVERT(NVARCHAR(200), i.[aa_cred]), CONVERT(NVARCHAR(200), i.[qs_cred]), CONVERT(NVARCHAR(200), i.[ci_cedu_cony])),
            N'crtboper_cony',
            @op,
            (SELECT x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[co_tipo_deud], x.[ci_cedu_cony] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtboper_cony',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[ti_cred]), CONVERT(NVARCHAR(200), d.[aa_cred]), CONVERT(NVARCHAR(200), d.[qs_cred]), CONVERT(NVARCHAR(200), d.[ci_cedu_cony])),
            N'crtboper_cony',
            N'DELETE',
            (SELECT x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[co_tipo_deud], x.[ci_cedu_cony] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtboper_cony',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbCR];
GO
IF OBJECT_ID(N'dbo.trg_outbox_crtoblig', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtoblig;
GO
-- Types canonicos que dependen de esta tabla: sp_personaVinculacionesType
CREATE TRIGGER dbo.trg_outbox_crtoblig
ON dbo.[crtoblig]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[ti_cred]), CONVERT(NVARCHAR(200), i.[aa_cred]), CONVERT(NVARCHAR(200), i.[qs_cred])),
            N'crtoblig',
            @op,
            (SELECT x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[ce_cred], x.[ci_clie], x.[fx_ppta], x.[fx_autr], x.[fx_cncd], x.[fx_vcto], x.[fx_canc], x.[fx_inst], x.[ci_mnda], x.[mo_cred], x.[mo_intr], x.[mo_abno_capi], x.[mo_abno_intr], x.[pr_cobr_impt], x.[fr_cobr_capi], x.[fr_cobr_intr], x.[ci_pais], x.[ci_prov], x.[pr_intr], x.[du_anos], x.[du_dias], x.[nu_dcto], x.[in_anci], x.[nu_peri_grac], x.[ti_peri_grac], x.[ti_cuot], x.[pr_marg_intr], x.[ti_marg_reaj], x.[fr_reaj], x.[ti_tasa_reaj], x.[ti_oprc], x.[in_gara], x.[in_grte], x.[ti_tasa_intr], x.[in_base_calc], x.[in_cred_dudo], x.[fx_dudo], x.[fx_ultm_actu], x.[ci_usua_actu], x.[qs_abno], x.[fx_dolr], x.[ci_mnda_antr], x.[pr_tasa_inic], x.[ci_ejec], x.[co_prog], x.[sc_casa], x.[co_afil_refe], x.[co_usua_ingr], x.[co_usua_inst], x.[nu_oper_cc], x.[ti_oper_cc], x.[nu_oper_ante], x.[pr_comi], x.[in_soli], x.[co_fond], x.[fe_inic_dvgo], x.[sc_cupo], x.[co_zona], x.[co_loca], x.[nu_rol_hist], x.[in_cobr_segu], x.[mo_pagd_segu], x.[fx_ultm_dvgo], x.[st_calf], x.[fe_ultm_calf], x.[co_empr], x.[co_tamo], x.[ti_diar], x.[nu_cpbt_cble], x.[in_cble_anul], x.[in_gara_cnta], x.[pr_gara_cnta], x.[mo_gara_cind], x.[co_tipo_segu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtoblig',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[ti_cred]), CONVERT(NVARCHAR(200), d.[aa_cred]), CONVERT(NVARCHAR(200), d.[qs_cred])),
            N'crtoblig',
            N'DELETE',
            (SELECT x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[ce_cred], x.[ci_clie], x.[fx_ppta], x.[fx_autr], x.[fx_cncd], x.[fx_vcto], x.[fx_canc], x.[fx_inst], x.[ci_mnda], x.[mo_cred], x.[mo_intr], x.[mo_abno_capi], x.[mo_abno_intr], x.[pr_cobr_impt], x.[fr_cobr_capi], x.[fr_cobr_intr], x.[ci_pais], x.[ci_prov], x.[pr_intr], x.[du_anos], x.[du_dias], x.[nu_dcto], x.[in_anci], x.[nu_peri_grac], x.[ti_peri_grac], x.[ti_cuot], x.[pr_marg_intr], x.[ti_marg_reaj], x.[fr_reaj], x.[ti_tasa_reaj], x.[ti_oprc], x.[in_gara], x.[in_grte], x.[ti_tasa_intr], x.[in_base_calc], x.[in_cred_dudo], x.[fx_dudo], x.[fx_ultm_actu], x.[ci_usua_actu], x.[qs_abno], x.[fx_dolr], x.[ci_mnda_antr], x.[pr_tasa_inic], x.[ci_ejec], x.[co_prog], x.[sc_casa], x.[co_afil_refe], x.[co_usua_ingr], x.[co_usua_inst], x.[nu_oper_cc], x.[ti_oper_cc], x.[nu_oper_ante], x.[pr_comi], x.[in_soli], x.[co_fond], x.[fe_inic_dvgo], x.[sc_cupo], x.[co_zona], x.[co_loca], x.[nu_rol_hist], x.[in_cobr_segu], x.[mo_pagd_segu], x.[fx_ultm_dvgo], x.[st_calf], x.[fe_ultm_calf], x.[co_empr], x.[co_tamo], x.[ti_diar], x.[nu_cpbt_cble], x.[in_cble_anul], x.[in_gara_cnta], x.[pr_gara_cnta], x.[mo_gara_cind], x.[co_tipo_segu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtoblig',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO


/* ------------------------------------------------------------------
   BD ORIGINAL: dbCT
   ------------------------------------------------------------------ */
USE [dbCT];
GO
IF OBJECT_ID(N'dbo.trg_outbox_cttbafil_audi', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cttbafil_audi;
GO
-- Types canonicos que dependen de esta tabla: sp_auditoriaAfiliado_type
CREATE TRIGGER dbo.trg_outbox_cttbafil_audi
ON dbo.[cttbafil_audi]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[ci_cedula]), CONVERT(NVARCHAR(200), i.[fe_crea]), CONVERT(NVARCHAR(200), i.[ho_crea]), CONVERT(NVARCHAR(200), i.[ci_camp])),
            N'cttbafil_audi',
            @op,
            (SELECT x.[ci_cedula], x.[fe_crea], x.[ho_crea], x.[co_usua], x.[ci_camp], x.[ds_audi], x.[ci_moti] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbafil_audi',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[ci_cedula]), CONVERT(NVARCHAR(200), d.[fe_crea]), CONVERT(NVARCHAR(200), d.[ho_crea]), CONVERT(NVARCHAR(200), d.[ci_camp])),
            N'cttbafil_audi',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[fe_crea], x.[ho_crea], x.[co_usua], x.[ci_camp], x.[ds_audi], x.[ci_moti] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbafil_audi',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbCT];
GO
IF OBJECT_ID(N'dbo.trg_outbox_cttbmatr_dist_afil', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cttbmatr_dist_afil;
GO
-- Types canonicos que dependen de esta tabla: sp_distribucionAfiliado_type
CREATE TRIGGER dbo.trg_outbox_cttbmatr_dist_afil
ON dbo.[cttbmatr_dist_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[Zona], x.[Provincia], x.[Distrito], x.[Ciudad], x.[Circuito], x.[Parroquia], x.[Estado], x.[trabajo], x.[NumeroAfiliado], x.[NumeroAfiliadoActualizado], x.[NumeroCAP], x.[NumeroEjecutivoFinanciero], x.[NumeroCreditoVigente], x.[NumeroCADB], x.[NumeroInstituciones], x.[NumeroPresidenteProvinciales2008], x.[NumeroPresidenteProvinciales2010], x.[NumeroPresidenteEjecutivo2008], x.[NumeroPresidenteEjecutivo2010], x.[NumeroDirectivoNacional2008], x.[NumeroDirectivoNacional2010], x.[NumeroDirectivoProvincial2008], x.[NumeroDirectivoProvincial2010], x.[NumeroDelegadoConvencion2008], x.[NumeroDelegadoConvencion2010], x.[mo_cred_vige], x.[mo_cuen_unic], x.[nu_lide_opin], x.[nu_solo_cam] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'cttbmatr_dist_afil',
            @op,
            (SELECT x.[Zona], x.[Provincia], x.[Distrito], x.[Ciudad], x.[Circuito], x.[Parroquia], x.[Estado], x.[trabajo], x.[NumeroAfiliado], x.[NumeroAfiliadoActualizado], x.[NumeroCAP], x.[NumeroEjecutivoFinanciero], x.[NumeroCreditoVigente], x.[NumeroCADB], x.[NumeroInstituciones], x.[NumeroPresidenteProvinciales2008], x.[NumeroPresidenteProvinciales2010], x.[NumeroPresidenteEjecutivo2008], x.[NumeroPresidenteEjecutivo2010], x.[NumeroDirectivoNacional2008], x.[NumeroDirectivoNacional2010], x.[NumeroDirectivoProvincial2008], x.[NumeroDirectivoProvincial2010], x.[NumeroDelegadoConvencion2008], x.[NumeroDelegadoConvencion2010], x.[mo_cred_vige], x.[mo_cuen_unic], x.[nu_lide_opin], x.[nu_solo_cam] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbmatr_dist_afil',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[Zona], x.[Provincia], x.[Distrito], x.[Ciudad], x.[Circuito], x.[Parroquia], x.[Estado], x.[trabajo], x.[NumeroAfiliado], x.[NumeroAfiliadoActualizado], x.[NumeroCAP], x.[NumeroEjecutivoFinanciero], x.[NumeroCreditoVigente], x.[NumeroCADB], x.[NumeroInstituciones], x.[NumeroPresidenteProvinciales2008], x.[NumeroPresidenteProvinciales2010], x.[NumeroPresidenteEjecutivo2008], x.[NumeroPresidenteEjecutivo2010], x.[NumeroDirectivoNacional2008], x.[NumeroDirectivoNacional2010], x.[NumeroDirectivoProvincial2008], x.[NumeroDirectivoProvincial2010], x.[NumeroDelegadoConvencion2008], x.[NumeroDelegadoConvencion2010], x.[mo_cred_vige], x.[mo_cuen_unic], x.[nu_lide_opin], x.[nu_solo_cam] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'cttbmatr_dist_afil',
            N'DELETE',
            (SELECT x.[Zona], x.[Provincia], x.[Distrito], x.[Ciudad], x.[Circuito], x.[Parroquia], x.[Estado], x.[trabajo], x.[NumeroAfiliado], x.[NumeroAfiliadoActualizado], x.[NumeroCAP], x.[NumeroEjecutivoFinanciero], x.[NumeroCreditoVigente], x.[NumeroCADB], x.[NumeroInstituciones], x.[NumeroPresidenteProvinciales2008], x.[NumeroPresidenteProvinciales2010], x.[NumeroPresidenteEjecutivo2008], x.[NumeroPresidenteEjecutivo2010], x.[NumeroDirectivoNacional2008], x.[NumeroDirectivoNacional2010], x.[NumeroDirectivoProvincial2008], x.[NumeroDirectivoProvincial2010], x.[NumeroDelegadoConvencion2008], x.[NumeroDelegadoConvencion2010], x.[mo_cred_vige], x.[mo_cuen_unic], x.[nu_lide_opin], x.[nu_solo_cam] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbmatr_dist_afil',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbCT];
GO
IF OBJECT_ID(N'dbo.trg_outbox_cttbtabl_afil', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cttbtabl_afil;
GO
-- Types canonicos que dependen de esta tabla: sp_auditoriaAfiliado_type
CREATE TRIGGER dbo.trg_outbox_cttbtabl_afil
ON dbo.[cttbtabl_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_camp]),
            N'cttbtabl_afil',
            @op,
            (SELECT x.[ci_camp], x.[no_camp], x.[ds_camp] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbtabl_afil',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_camp]),
            N'cttbtabl_afil',
            N'DELETE',
            (SELECT x.[ci_camp], x.[no_camp], x.[ds_camp] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbtabl_afil',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO


/* ------------------------------------------------------------------
   BD ORIGINAL: dbFC
   ------------------------------------------------------------------ */
USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbactv_suje_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbactv_suje_cred;
GO
-- Types canonicos que dependen de esta tabla: sp_informacionAdicionalAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbactv_suje_cred
ON dbo.[fctbactv_suje_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_actv_suje_cred]),
            N'fctbactv_suje_cred',
            @op,
            (SELECT x.[sc_actv_suje_cred], x.[co_actv_suje_cred], x.[ds_actv_suje_cred], x.[st_actv_suje_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbactv_suje_cred',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_actv_suje_cred]),
            N'fctbactv_suje_cred',
            N'DELETE',
            (SELECT x.[sc_actv_suje_cred], x.[co_actv_suje_cred], x.[ds_actv_suje_cred], x.[st_actv_suje_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbactv_suje_cred',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_actu', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbafil_actu;
GO
-- Types canonicos que dependen de esta tabla: sp_actualizacionAfiliado_type,sp_naturalTrabajoType,sp_personaTelefonosType
CREATE TRIGGER dbo.trg_outbox_fctbafil_actu
ON dbo.[fctbafil_actu]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedu]),
            N'fctbafil_actu',
            @op,
            (SELECT x.[ci_cedu], x.[co_prov], x.[co_cant], x.[co_parr], x.[ds_call_prim], x.[nu_call_prim], x.[ds_call_secu], x.[nu_call_secu], x.[nu_manz], x.[nu_vill], x.[ds_cdla], x.[tx_telf_conv], x.[tx_telf_celu], x.[ti_oper], x.[ds_refe_vivi], x.[tx_mail], x.[no_cont_adic], x.[tx_telf_con1], x.[tx_telf_con2], x.[ti_rela], x.[co_prov_inst], x.[ci_tipo], x.[co_inst], x.[co_carg], x.[co_nive], x.[co_cate], x.[ti_cont], x.[ti_jorn], x.[co_prov_obsq], x.[co_zona_obsq], x.[in_reno_cred], x.[in_acci], x.[fe_ingr], x.[fe_modi], x.[fe_ultm_envi], x.[in_impr_docu], x.[fe_impr_docu], x.[in_cobr_pres], x.[in_vald_celu], x.[in_vald_mail], x.[co_ami], x.[st_entr_obsq], x.[fe_entr_obsq], x.[fe_veri_dato], x.[in_impr_docu_cred], x.[fe_impr_docu_cred], x.[no_inst], x.[co_cant_inst], x.[co_parr_inst] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_actu',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedu]),
            N'fctbafil_actu',
            N'DELETE',
            (SELECT x.[ci_cedu], x.[co_prov], x.[co_cant], x.[co_parr], x.[ds_call_prim], x.[nu_call_prim], x.[ds_call_secu], x.[nu_call_secu], x.[nu_manz], x.[nu_vill], x.[ds_cdla], x.[tx_telf_conv], x.[tx_telf_celu], x.[ti_oper], x.[ds_refe_vivi], x.[tx_mail], x.[no_cont_adic], x.[tx_telf_con1], x.[tx_telf_con2], x.[ti_rela], x.[co_prov_inst], x.[ci_tipo], x.[co_inst], x.[co_carg], x.[co_nive], x.[co_cate], x.[ti_cont], x.[ti_jorn], x.[co_prov_obsq], x.[co_zona_obsq], x.[in_reno_cred], x.[in_acci], x.[fe_ingr], x.[fe_modi], x.[fe_ultm_envi], x.[in_impr_docu], x.[fe_impr_docu], x.[in_cobr_pres], x.[in_vald_celu], x.[in_vald_mail], x.[co_ami], x.[st_entr_obsq], x.[fe_entr_obsq], x.[fe_veri_dato], x.[in_impr_docu_cred], x.[fe_impr_docu_cred], x.[no_inst], x.[co_cant_inst], x.[co_parr_inst] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_actu',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_ahor_refe', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbafil_ahor_refe;
GO
-- Types canonicos que dependen de esta tabla: sp_personaReferenciasPersonalesType
CREATE TRIGGER dbo.trg_outbox_fctbafil_ahor_refe
ON dbo.[fctbafil_ahor_refe]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[ci_cedu]), CONVERT(NVARCHAR(200), i.[ci_Cedu_refe])),
            N'fctbafil_ahor_refe',
            @op,
            (SELECT x.[ci_cedu], x.[ci_Cedu_refe], x.[no_nomb], x.[no_apel], x.[no_pare], x.[nu_telf] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_ahor_refe',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[ci_cedu]), CONVERT(NVARCHAR(200), d.[ci_Cedu_refe])),
            N'fctbafil_ahor_refe',
            N'DELETE',
            (SELECT x.[ci_cedu], x.[ci_Cedu_refe], x.[no_nomb], x.[no_apel], x.[no_pare], x.[nu_telf] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_ahor_refe',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_auto_docs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbafil_auto_docs;
GO
-- Types canonicos que dependen de esta tabla: sp_documentacionAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbafil_auto_docs
ON dbo.[fctbafil_auto_docs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[co_cedu]), CONVERT(NVARCHAR(200), i.[sc_regi])),
            N'fctbafil_auto_docs',
            @op,
            (SELECT x.[co_empr], x.[co_cedu], x.[sc_regi], x.[co_docu], x.[fe_crea], x.[fe_elim], x.[co_auto], x.[st_regi], x.[co_usua_crea], x.[co_usua_elim], x.[fe_firm_dcto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_auto_docs',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[co_cedu]), CONVERT(NVARCHAR(200), d.[sc_regi])),
            N'fctbafil_auto_docs',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_cedu], x.[sc_regi], x.[co_docu], x.[fe_crea], x.[fe_elim], x.[co_auto], x.[st_regi], x.[co_usua_crea], x.[co_usua_elim], x.[fe_firm_dcto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_auto_docs',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_dcap', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbafil_dcap;
GO
-- Types canonicos que dependen de esta tabla: sp_informacionAdicionalAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbafil_dcap
ON dbo.[fctbafil_dcap]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_dsto_cap]),
            N'fctbafil_dcap',
            @op,
            (SELECT x.[ci_cedula], x.[ti_dsto], x.[ci_rold], x.[ci_rolh], x.[va_dsto], x.[st_dsto], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[sc_dsto_cap], x.[co_prod] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_dcap',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_dsto_cap]),
            N'fctbafil_dcap',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ti_dsto], x.[ci_rold], x.[ci_rolh], x.[va_dsto], x.[st_dsto], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[sc_dsto_cap], x.[co_prod] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_dcap',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_gast_pers', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbafil_gast_pers;
GO
-- Types canonicos que dependen de esta tabla: sp_informacionAdicionalAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbafil_gast_pers
ON dbo.[fctbafil_gast_pers]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[co_cedu]), CONVERT(NVARCHAR(200), i.[co_elem])),
            N'fctbafil_gast_pers',
            @op,
            (SELECT x.[co_empr], x.[co_cedu], x.[co_elem], x.[mo_mnto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_gast_pers',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[co_cedu]), CONVERT(NVARCHAR(200), d.[co_elem])),
            N'fctbafil_gast_pers',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_cedu], x.[co_elem], x.[mo_mnto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_gast_pers',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_info_actu_docs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbafil_info_actu_docs;
GO
-- Types canonicos que dependen de esta tabla: sp_actualizacionDocumentos_type,sp_naturalInformacionAdicionalType,sp_naturalInformacionBasicaType,sp_personaDireccionesType,sp_personaType
CREATE TRIGGER dbo.trg_outbox_fctbafil_info_actu_docs
ON dbo.[fctbafil_info_actu_docs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[sc_actu_docs]), CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[co_cedu])),
            N'fctbafil_info_actu_docs',
            @op,
            (SELECT x.[sc_actu_docs], x.[co_empr], x.[co_cedu], x.[sc_actv_suje_cred], x.[sc_orgn_ingr], x.[co_pers_poli_expu], x.[ds_ciud_naci], x.[in_comi_serv], x.[ds_comi_serv], x.[fx_ingr], x.[co_usua_ingr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_info_actu_docs',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[sc_actu_docs]), CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[co_cedu])),
            N'fctbafil_info_actu_docs',
            N'DELETE',
            (SELECT x.[sc_actu_docs], x.[co_empr], x.[co_cedu], x.[sc_actv_suje_cred], x.[sc_orgn_ingr], x.[co_pers_poli_expu], x.[ds_ciud_naci], x.[in_comi_serv], x.[ds_comi_serv], x.[fx_ingr], x.[co_usua_ingr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_info_actu_docs',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_info_adic', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbafil_info_adic;
GO
-- Types canonicos que dependen de esta tabla: sp_informacionAdicionalAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbafil_info_adic
ON dbo.[fctbafil_info_adic]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedu]),
            N'fctbafil_info_adic',
            @op,
            (SELECT x.[ci_cedu], x.[ds_calle_prim], x.[nu_calle_prim], x.[ds_calle_secu], x.[nu_calle_secu], x.[nu_manz], x.[nu_villa], x.[ds_refe_vivi], x.[ti_oper], x.[no_cont_adic], x.[tx_telf_con1], x.[tx_telf_con2], x.[ti_rela], x.[ti_jorn], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_dist_amie], x.[co_dist_mins], x.[in_corr_cedu], x.[co_pais_naci], x.[co_area_lbrl] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_info_adic',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedu]),
            N'fctbafil_info_adic',
            N'DELETE',
            (SELECT x.[ci_cedu], x.[ds_calle_prim], x.[nu_calle_prim], x.[ds_calle_secu], x.[nu_calle_secu], x.[nu_manz], x.[nu_villa], x.[ds_refe_vivi], x.[ti_oper], x.[no_cont_adic], x.[tx_telf_con1], x.[tx_telf_con2], x.[ti_rela], x.[ti_jorn], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_dist_amie], x.[co_dist_mins], x.[in_corr_cedu], x.[co_pais_naci], x.[co_area_lbrl] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_info_adic',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_unif', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbafil_unif;
GO
-- Types canonicos que dependen de esta tabla: sp_documentacionAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbafil_unif
ON dbo.[fctbafil_unif]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[ci_cedu]), CONVERT(NVARCHAR(200), i.[sc_reac])),
            N'fctbafil_unif',
            @op,
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_reac], x.[in_veri], x.[fe_ingr], x.[co_usua_ingr], x.[fe_proc], x.[co_usua_proc], x.[fe_elim], x.[co_usua_elim], x.[ci_cedu_ejec], x.[sc_gene], x.[ti_proc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_unif',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[ci_cedu]), CONVERT(NVARCHAR(200), d.[sc_reac])),
            N'fctbafil_unif',
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_reac], x.[in_veri], x.[fe_ingr], x.[co_usua_ingr], x.[fe_proc], x.[co_usua_proc], x.[fe_elim], x.[co_usua_elim], x.[ci_cedu_ejec], x.[sc_gene], x.[ti_proc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_unif',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbagen_mail', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbagen_mail;
GO
-- Types canonicos que dependen de esta tabla: sp_agendaMailAfiliado_type,sp_personaDireccionesType
CREATE TRIGGER dbo.trg_outbox_fctbagen_mail
ON dbo.[fctbagen_mail]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[ci_cedu]), CONVERT(NVARCHAR(200), i.[sc_regi])),
            N'fctbagen_mail',
            @op,
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_regi], x.[ds_mail], x.[in_prin], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usu_elim], x.[fe_elim], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagen_mail',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[ci_cedu]), CONVERT(NVARCHAR(200), d.[sc_regi])),
            N'fctbagen_mail',
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_regi], x.[ds_mail], x.[in_prin], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usu_elim], x.[fe_elim], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagen_mail',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbagen_telf_part', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbagen_telf_part;
GO
-- Types canonicos que dependen de esta tabla: sp_personaTelefonosType
CREATE TRIGGER dbo.trg_outbox_fctbagen_telf_part
ON dbo.[fctbagen_telf_part]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[ci_cedu]), CONVERT(NVARCHAR(200), i.[sc_regi])),
            N'fctbagen_telf_part',
            @op,
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_regi], x.[nu_telf], x.[ti_telf], x.[co_oper], x.[in_prin], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagen_telf_part',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[ci_cedu]), CONVERT(NVARCHAR(200), d.[sc_regi])),
            N'fctbagen_telf_part',
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_regi], x.[nu_telf], x.[ti_telf], x.[co_oper], x.[in_prin], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagen_telf_part',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbagru_moti_repo', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbagru_moti_repo;
GO
-- Types canonicos que dependen de esta tabla: sp_movimientoCuenta_type
CREATE TRIGGER dbo.trg_outbox_fctbagru_moti_repo
ON dbo.[fctbagru_moti_repo]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[co_agru_moti])),
            N'fctbagru_moti_repo',
            @op,
            (SELECT x.[co_empr], x.[co_agru_moti], x.[ds_agru_moti], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagru_moti_repo',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[co_agru_moti])),
            N'fctbagru_moti_repo',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_agru_moti], x.[ds_agru_moti], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagru_moti_repo',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbarea_lbrl', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbarea_lbrl;
GO
-- Types canonicos que dependen de esta tabla: sp_areaLaboralParticipe_type
CREATE TRIGGER dbo.trg_outbox_fctbarea_lbrl
ON dbo.[fctbarea_lbrl]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_area_lbrl]),
            N'fctbarea_lbrl',
            @op,
            (SELECT x.[co_area_lbrl], x.[ds_area_lbrl], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbarea_lbrl',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_area_lbrl]),
            N'fctbarea_lbrl',
            N'DELETE',
            (SELECT x.[co_area_lbrl], x.[ds_area_lbrl], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbarea_lbrl',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbaudi_actu_afil', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbaudi_actu_afil;
GO
-- Types canonicos que dependen de esta tabla: sp_auditoriaAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbaudi_actu_afil
ON dbo.[fctbaudi_actu_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[sc_actu])),
            N'fctbaudi_actu_afil',
            @op,
            (SELECT x.[co_empr], x.[sc_actu], x.[ci_cedu], x.[ds_mail], x.[nu_telf_conv], x.[nu_telf_celu], x.[ds_dire], x.[ds_inst_afil], x.[ti_orig], x.[co_usua_ingr], x.[fe_ingr], x.[ho_ingr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbaudi_actu_afil',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[sc_actu])),
            N'fctbaudi_actu_afil',
            N'DELETE',
            (SELECT x.[co_empr], x.[sc_actu], x.[ci_cedu], x.[ds_mail], x.[nu_telf_conv], x.[nu_telf_celu], x.[ds_dire], x.[ds_inst_afil], x.[ti_orig], x.[co_usua_ingr], x.[fe_ingr], x.[ho_ingr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbaudi_actu_afil',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbaudi_movi', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbaudi_movi;
GO
-- Types canonicos que dependen de esta tabla: sp_auditoriaAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbaudi_movi
ON dbo.[fctbaudi_movi]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[ci_transaccion]), CONVERT(NVARCHAR(200), i.[ci_cedula])),
            N'fctbaudi_movi',
            @op,
            (SELECT x.[ci_transaccion], x.[ci_cedula], x.[co_usua], x.[co_tran], x.[fx_crea], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbaudi_movi',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[ci_transaccion]), CONVERT(NVARCHAR(200), d.[ci_cedula])),
            N'fctbaudi_movi',
            N'DELETE',
            (SELECT x.[ci_transaccion], x.[ci_cedula], x.[co_usua], x.[co_tran], x.[fx_crea], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbaudi_movi',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbcart_rpag', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbcart_rpag;
GO
-- Types canonicos que dependen de esta tabla: sp_documentacionAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbcart_rpag
ON dbo.[fctbcart_rpag]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[co_fond]), CONVERT(NVARCHAR(200), i.[ci_cedu]), CONVERT(NVARCHAR(200), i.[co_proc])),
            N'fctbcart_rpag',
            @op,
            (SELECT x.[co_empr], x.[co_fond], x.[ci_cedu], x.[co_proc], x.[co_form_dsto], x.[co_tseg], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_ejec], x.[st_regi], x.[in_cart_afil] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcart_rpag',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[co_fond]), CONVERT(NVARCHAR(200), d.[ci_cedu]), CONVERT(NVARCHAR(200), d.[co_proc])),
            N'fctbcart_rpag',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_fond], x.[ci_cedu], x.[co_proc], x.[co_form_dsto], x.[co_tseg], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_ejec], x.[st_regi], x.[in_cart_afil] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcart_rpag',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbcinf_part_sibs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbcinf_part_sibs;
GO
-- Types canonicos que dependen de esta tabla: sp_reporteSIBSParticipe_type
CREATE TRIGGER dbo.trg_outbox_fctbcinf_part_sibs
ON dbo.[fctbcinf_part_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_regi], x.[co_estr_sibs], x.[co_enti_sibs], x.[fe_cort], x.[nu_regi], x.[co_usua_gene], x.[fe_usua_gene], x.[co_usua_conf], x.[fe_usua_conf], x.[co_usua_elim], x.[fe_usua_elim], x.[st_estr_sibs] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'fctbcinf_part_sibs',
            @op,
            (SELECT x.[sc_regi], x.[co_estr_sibs], x.[co_enti_sibs], x.[fe_cort], x.[nu_regi], x.[co_usua_gene], x.[fe_usua_gene], x.[co_usua_conf], x.[fe_usua_conf], x.[co_usua_elim], x.[fe_usua_elim], x.[st_estr_sibs] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcinf_part_sibs',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_regi], x.[co_estr_sibs], x.[co_enti_sibs], x.[fe_cort], x.[nu_regi], x.[co_usua_gene], x.[fe_usua_gene], x.[co_usua_conf], x.[fe_usua_conf], x.[co_usua_elim], x.[fe_usua_elim], x.[st_estr_sibs] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'fctbcinf_part_sibs',
            N'DELETE',
            (SELECT x.[sc_regi], x.[co_estr_sibs], x.[co_enti_sibs], x.[fe_cort], x.[nu_regi], x.[co_usua_gene], x.[fe_usua_gene], x.[co_usua_conf], x.[fe_usua_conf], x.[co_usua_elim], x.[fe_usua_elim], x.[st_estr_sibs] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcinf_part_sibs',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbcser_adic', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbcser_adic;
GO
-- Types canonicos que dependen de esta tabla: sp_servicioAdicional_type
CREATE TRIGGER dbo.trg_outbox_fctbcser_adic
ON dbo.[fctbcser_adic]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[sc_secu])),
            N'fctbcser_adic',
            @op,
            (SELECT x.[co_empr], x.[sc_secu], x.[ci_cedu], x.[co_serv], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_autr], x.[fe_autr], x.[co_usua_modi], x.[fe_modi], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcser_adic',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[sc_secu])),
            N'fctbcser_adic',
            N'DELETE',
            (SELECT x.[co_empr], x.[sc_secu], x.[ci_cedu], x.[co_serv], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_autr], x.[fe_autr], x.[co_usua_modi], x.[fe_modi], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcser_adic',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbdinf_liqd_cnta_sibs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbdinf_liqd_cnta_sibs;
GO
-- Types canonicos que dependen de esta tabla: sp_reporteSIBSParticipe_type
CREATE TRIGGER dbo.trg_outbox_fctbdinf_liqd_cnta_sibs
ON dbo.[fctbdinf_liqd_cnta_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[sc_regi]), CONVERT(NVARCHAR(200), i.[ci_cedu])),
            N'fctbdinf_liqd_cnta_sibs',
            @op,
            (SELECT x.[sc_regi], x.[ci_cedu], x.[ti_iden], x.[fe_term_rela], x.[nu_impo_pers], x.[nu_impo_patr], x.[fe_liqd], x.[mo_cnta_indi], x.[mo_desc], x.[mo_tota_paga] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_liqd_cnta_sibs',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[sc_regi]), CONVERT(NVARCHAR(200), d.[ci_cedu])),
            N'fctbdinf_liqd_cnta_sibs',
            N'DELETE',
            (SELECT x.[sc_regi], x.[ci_cedu], x.[ti_iden], x.[fe_term_rela], x.[nu_impo_pers], x.[nu_impo_patr], x.[fe_liqd], x.[mo_cnta_indi], x.[mo_desc], x.[mo_tota_paga] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_liqd_cnta_sibs',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbdinf_part_sibs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbdinf_part_sibs;
GO
-- Types canonicos que dependen de esta tabla: sp_reporteSIBSParticipe_type
CREATE TRIGGER dbo.trg_outbox_fctbdinf_part_sibs
ON dbo.[fctbdinf_part_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_regi], x.[ti_iden], x.[nu_iden], x.[in_gene_sibs], x.[co_esta_civi], x.[fe_naci], x.[fe_ingr_part], x.[co_esta_part_sbis], x.[co_tsis_sibs], x.[co_base_cal_sibs], x.[co_rela_lab_sibs], x.[co_esta_regi], x.[fe_actu_regi] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'fctbdinf_part_sibs',
            @op,
            (SELECT x.[sc_regi], x.[ti_iden], x.[nu_iden], x.[in_gene_sibs], x.[co_esta_civi], x.[fe_naci], x.[fe_ingr_part], x.[co_esta_part_sbis], x.[co_tsis_sibs], x.[co_base_cal_sibs], x.[co_rela_lab_sibs], x.[co_esta_regi], x.[fe_actu_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_part_sibs',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_regi], x.[ti_iden], x.[nu_iden], x.[in_gene_sibs], x.[co_esta_civi], x.[fe_naci], x.[fe_ingr_part], x.[co_esta_part_sbis], x.[co_tsis_sibs], x.[co_base_cal_sibs], x.[co_rela_lab_sibs], x.[co_esta_regi], x.[fe_actu_regi] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'fctbdinf_part_sibs',
            N'DELETE',
            (SELECT x.[sc_regi], x.[ti_iden], x.[nu_iden], x.[in_gene_sibs], x.[co_esta_civi], x.[fe_naci], x.[fe_ingr_part], x.[co_esta_part_sbis], x.[co_tsis_sibs], x.[co_base_cal_sibs], x.[co_rela_lab_sibs], x.[co_esta_regi], x.[fe_actu_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_part_sibs',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbesta_civi', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbesta_civi;
GO
-- Types canonicos que dependen de esta tabla: sp_servicioAdicional_type
CREATE TRIGGER dbo.trg_outbox_fctbesta_civi
ON dbo.[fctbesta_civi]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_esta_civi]),
            N'fctbesta_civi',
            @op,
            (SELECT x.[co_esta_civi], x.[ds_esta_civi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbesta_civi',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_esta_civi]),
            N'fctbesta_civi',
            N'DELETE',
            (SELECT x.[co_esta_civi], x.[ds_esta_civi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbesta_civi',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbfcha_afil', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbfcha_afil;
GO
-- Types canonicos que dependen de esta tabla: sp_documentacionAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbfcha_afil
ON dbo.[fctbfcha_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[sc_fcha_afil])),
            N'fctbfcha_afil',
            @op,
            (SELECT x.[co_empr], x.[ci_cedu], x.[no_apel_prim], x.[no_apel_secu], x.[no_nomb], x.[co_gene], x.[co_pais_naci], x.[co_esta_civi], x.[fe_naci], x.[nu_carg], x.[co_prov_resi], x.[co_cant_resi], x.[co_parr_resi], x.[in_rura_resi], x.[ds_call_prin_resi], x.[ds_call_secu_resi], x.[ds_cdla_resi], x.[nu_manz_resi], x.[nu_vill_resi], x.[ds_refe_ubic_resi], x.[co_carg_actu], x.[co_titu_prof], x.[co_catg], x.[co_tipo_cont], x.[co_tipo_doce], x.[fe_ingr_magi], x.[co_inst], x.[no_inst], x.[ds_amie], x.[co_nive], x.[co_sost], x.[co_jorn], x.[co_zona], x.[co_dist], x.[ci_tipo_inst], x.[co_prov_inst], x.[co_cant_inst], x.[co_parr_inst], x.[in_rura_inst], x.[ds_call_prin_inst], x.[ds_call_secu_inst], x.[ds_cdla_inst], x.[ds_manz_inst], x.[ds_vill_inst], x.[ds_refe_ubic_inst], x.[co_sect_regi], x.[co_vivi], x.[nu_anio_auto], x.[co_marc_auto], x.[ds_mode_auto], x.[ds_otro_bien], x.[nu_telf_inst], x.[ds_hora_cnto], x.[mo_apor], x.[in_afil], x.[fe_ingr], x.[ho_ingr], x.[fe_aprb], x.[co_usua_aprb], x.[fe_elim], x.[co_usua_elim], x.[ti_tran], x.[st_regi], x.[co_area_lbrl], x.[nu_call_prin], x.[co_ejec], x.[fe_modi], x.[co_banc], x.[nu_cnta], x.[ti_cnta], x.[mo_apor_adic], x.[sc_fcha_afil], x.[FE_VERI], x.[CO_USUA_VERI] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbfcha_afil',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[sc_fcha_afil])),
            N'fctbfcha_afil',
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_cedu], x.[no_apel_prim], x.[no_apel_secu], x.[no_nomb], x.[co_gene], x.[co_pais_naci], x.[co_esta_civi], x.[fe_naci], x.[nu_carg], x.[co_prov_resi], x.[co_cant_resi], x.[co_parr_resi], x.[in_rura_resi], x.[ds_call_prin_resi], x.[ds_call_secu_resi], x.[ds_cdla_resi], x.[nu_manz_resi], x.[nu_vill_resi], x.[ds_refe_ubic_resi], x.[co_carg_actu], x.[co_titu_prof], x.[co_catg], x.[co_tipo_cont], x.[co_tipo_doce], x.[fe_ingr_magi], x.[co_inst], x.[no_inst], x.[ds_amie], x.[co_nive], x.[co_sost], x.[co_jorn], x.[co_zona], x.[co_dist], x.[ci_tipo_inst], x.[co_prov_inst], x.[co_cant_inst], x.[co_parr_inst], x.[in_rura_inst], x.[ds_call_prin_inst], x.[ds_call_secu_inst], x.[ds_cdla_inst], x.[ds_manz_inst], x.[ds_vill_inst], x.[ds_refe_ubic_inst], x.[co_sect_regi], x.[co_vivi], x.[nu_anio_auto], x.[co_marc_auto], x.[ds_mode_auto], x.[ds_otro_bien], x.[nu_telf_inst], x.[ds_hora_cnto], x.[mo_apor], x.[in_afil], x.[fe_ingr], x.[ho_ingr], x.[fe_aprb], x.[co_usua_aprb], x.[fe_elim], x.[co_usua_elim], x.[ti_tran], x.[st_regi], x.[co_area_lbrl], x.[nu_call_prin], x.[co_ejec], x.[fe_modi], x.[co_banc], x.[nu_cnta], x.[ti_cnta], x.[mo_apor_adic], x.[sc_fcha_afil], x.[FE_VERI], x.[CO_USUA_VERI] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbfcha_afil',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbfcha_afil_dcto', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbfcha_afil_dcto;
GO
-- Types canonicos que dependen de esta tabla: sp_documentacionAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbfcha_afil_dcto
ON dbo.[fctbfcha_afil_dcto]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[sc_fcha_afil]), CONVERT(NVARCHAR(200), i.[co_dcto])),
            N'fctbfcha_afil_dcto',
            @op,
            (SELECT x.[co_empr], x.[sc_fcha_afil], x.[co_dcto], x.[fe_firm_dcto], x.[fe_modi], x.[co_usua_modi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbfcha_afil_dcto',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[sc_fcha_afil]), CONVERT(NVARCHAR(200), d.[co_dcto])),
            N'fctbfcha_afil_dcto',
            N'DELETE',
            (SELECT x.[co_empr], x.[sc_fcha_afil], x.[co_dcto], x.[fe_firm_dcto], x.[fe_modi], x.[co_usua_modi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbfcha_afil_dcto',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbgene_sibs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbgene_sibs;
GO
-- Types canonicos que dependen de esta tabla: sp_servicioAdicional_type
CREATE TRIGGER dbo.trg_outbox_fctbgene_sibs
ON dbo.[fctbgene_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_gene], x.[ds_gene] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'fctbgene_sibs',
            @op,
            (SELECT x.[co_gene], x.[ds_gene] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbgene_sibs',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_gene], x.[ds_gene] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'fctbgene_sibs',
            N'DELETE',
            (SELECT x.[co_gene], x.[ds_gene] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbgene_sibs',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbinst_info_adic', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbinst_info_adic;
GO
-- Types canonicos que dependen de esta tabla: sp_institucion_type
CREATE TRIGGER dbo.trg_outbox_fctbinst_info_adic
ON dbo.[fctbinst_info_adic]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[ci_tipo]), CONVERT(NVARCHAR(200), i.[ci_inst])),
            N'fctbinst_info_adic',
            @op,
            (SELECT x.[ci_tipo], x.[ci_inst], x.[tx_tefl_conv_inst], x.[ci_cedu_repr], x.[no_nomb_repr], x.[tx_mail_repr], x.[tx_telf_repr], x.[ti_acce], x.[nu_doce], x.[nu_boni], x.[nu_admi], x.[nu_alum], x.[co_circ_mned], x.[co_dist_mned], x.[co_moda], x.[co_etni], x.[co_naci_inst], x.[ti_educ_mned], x.[co_zona_mned], x.[in_unid_admi], x.[co_moti_modi], x.[fe_modi], x.[co_empr], x.[co_regi_esco] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbinst_info_adic',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[ci_tipo]), CONVERT(NVARCHAR(200), d.[ci_inst])),
            N'fctbinst_info_adic',
            N'DELETE',
            (SELECT x.[ci_tipo], x.[ci_inst], x.[tx_tefl_conv_inst], x.[ci_cedu_repr], x.[no_nomb_repr], x.[tx_mail_repr], x.[tx_telf_repr], x.[ti_acce], x.[nu_doce], x.[nu_boni], x.[nu_admi], x.[nu_alum], x.[co_circ_mned], x.[co_dist_mned], x.[co_moda], x.[co_etni], x.[co_naci_inst], x.[ti_educ_mned], x.[co_zona_mned], x.[in_unid_admi], x.[co_moti_modi], x.[fe_modi], x.[co_empr], x.[co_regi_esco] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbinst_info_adic',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbotro_ingr_afil', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbotro_ingr_afil;
GO
-- Types canonicos que dependen de esta tabla: sp_otrosIngresosAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbotro_ingr_afil
ON dbo.[fctbotro_ingr_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_rol]), CONVERT(NVARCHAR(200), i.[ci_cedu]), CONVERT(NVARCHAR(200), i.[co_otro_ingr_rubr])),
            N'fctbotro_ingr_afil',
            @op,
            (SELECT x.[co_rol], x.[ci_cedu], x.[co_otro_ingr_rubr], x.[mo_rubr], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[ds_adic], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbotro_ingr_afil',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_rol]), CONVERT(NVARCHAR(200), d.[ci_cedu]), CONVERT(NVARCHAR(200), d.[co_otro_ingr_rubr])),
            N'fctbotro_ingr_afil',
            N'DELETE',
            (SELECT x.[co_rol], x.[ci_cedu], x.[co_otro_ingr_rubr], x.[mo_rubr], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[ds_adic], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbotro_ingr_afil',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbotro_ingr_cony', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbotro_ingr_cony;
GO
-- Types canonicos que dependen de esta tabla: sp_otrosIngresosAfiliado_type
CREATE TRIGGER dbo.trg_outbox_fctbotro_ingr_cony
ON dbo.[fctbotro_ingr_cony]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_rol]), CONVERT(NVARCHAR(200), i.[ci_cedu]), CONVERT(NVARCHAR(200), i.[ci_cedu_cony]), CONVERT(NVARCHAR(200), i.[co_otro_ingr_rubr])),
            N'fctbotro_ingr_cony',
            @op,
            (SELECT x.[co_rol], x.[ci_cedu], x.[ci_cedu_cony], x.[co_otro_ingr_rubr], x.[mo_rubr], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[ds_adic], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbotro_ingr_cony',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_rol]), CONVERT(NVARCHAR(200), d.[ci_cedu]), CONVERT(NVARCHAR(200), d.[ci_cedu_cony]), CONVERT(NVARCHAR(200), d.[co_otro_ingr_rubr])),
            N'fctbotro_ingr_cony',
            N'DELETE',
            (SELECT x.[co_rol], x.[ci_cedu], x.[ci_cedu_cony], x.[co_otro_ingr_rubr], x.[mo_rubr], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[ds_adic], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbotro_ingr_cony',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbpara_serv_adic', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbpara_serv_adic;
GO
-- Types canonicos que dependen de esta tabla: sp_servicioAdicional_type
CREATE TRIGGER dbo.trg_outbox_fctbpara_serv_adic
ON dbo.[fctbpara_serv_adic]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[co_serv])),
            N'fctbpara_serv_adic',
            @op,
            (SELECT x.[co_empr], x.[co_serv], x.[mo_serv], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbpara_serv_adic',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[co_serv])),
            N'fctbpara_serv_adic',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_serv], x.[mo_serv], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbpara_serv_adic',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbrubr_rent', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbrubr_rent;
GO
-- Types canonicos que dependen de esta tabla: sp_movimientoCuenta_type,sp_saldoDiario_type
CREATE TRIGGER dbo.trg_outbox_fctbrubr_rent
ON dbo.[fctbrubr_rent]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[co_rubr]), CONVERT(NVARCHAR(200), i.[co_rubr_rent])),
            N'fctbrubr_rent',
            @op,
            (SELECT x.[co_empr], x.[co_rubr], x.[co_rubr_rent], x.[st_regi], x.[co_fond], x.[co_rubr_prin] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbrubr_rent',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[co_rubr]), CONVERT(NVARCHAR(200), d.[co_rubr_rent])),
            N'fctbrubr_rent',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_rubr], x.[co_rubr_rent], x.[st_regi], x.[co_fond], x.[co_rubr_prin] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbrubr_rent',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbrvol_esta_afil', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbrvol_esta_afil;
GO
-- Types canonicos que dependen de esta tabla: sp_retiroVoluntarioEstado_type
CREATE TRIGGER dbo.trg_outbox_fctbrvol_esta_afil
ON dbo.[fctbrvol_esta_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[co_fond]), CONVERT(NVARCHAR(200), i.[co_tret_volu]), CONVERT(NVARCHAR(200), i.[nu_anio]), CONVERT(NVARCHAR(200), i.[sc_deta])),
            N'fctbrvol_esta_afil',
            @op,
            (SELECT x.[co_empr], x.[co_fond], x.[co_tret_volu], x.[nu_anio], x.[sc_deta], x.[co_esta_afil], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbrvol_esta_afil',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[co_fond]), CONVERT(NVARCHAR(200), d.[co_tret_volu]), CONVERT(NVARCHAR(200), d.[nu_anio]), CONVERT(NVARCHAR(200), d.[sc_deta])),
            N'fctbrvol_esta_afil',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_fond], x.[co_tret_volu], x.[nu_anio], x.[sc_deta], x.[co_esta_afil], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbrvol_esta_afil',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbsald_diar_afil_rubr', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbsald_diar_afil_rubr;
GO
-- Types canonicos que dependen de esta tabla: sp_saldoDiario_type
CREATE TRIGGER dbo.trg_outbox_fctbsald_diar_afil_rubr
ON dbo.[fctbsald_diar_afil_rubr]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[fx_saldo]), CONVERT(NVARCHAR(200), i.[ci_cedula]), CONVERT(NVARCHAR(200), i.[ci_rubro_rol]), CONVERT(NVARCHAR(200), i.[co_empr])),
            N'fctbsald_diar_afil_rubr',
            @op,
            (SELECT x.[fx_saldo], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[ci_pagador], x.[ci_cedula], x.[ce_estado], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbsald_diar_afil_rubr',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[fx_saldo]), CONVERT(NVARCHAR(200), d.[ci_cedula]), CONVERT(NVARCHAR(200), d.[ci_rubro_rol]), CONVERT(NVARCHAR(200), d.[co_empr])),
            N'fctbsald_diar_afil_rubr',
            N'DELETE',
            (SELECT x.[fx_saldo], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[ci_pagador], x.[ci_cedula], x.[ce_estado], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbsald_diar_afil_rubr',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_fctbsald_diar_rubr', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbsald_diar_rubr;
GO
-- Types canonicos que dependen de esta tabla: sp_saldoDiarioRubro_type
CREATE TRIGGER dbo.trg_outbox_fctbsald_diar_rubr
ON dbo.[fctbsald_diar_rubr]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[fx_saldo], x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'fctbsald_diar_rubr',
            @op,
            (SELECT x.[fx_saldo], x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbsald_diar_rubr',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[fx_saldo], x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'fctbsald_diar_rubr',
            N'DELETE',
            (SELECT x.[fx_saldo], x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbsald_diar_rubr',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_afiliado;
GO
-- Types canonicos que dependen de esta tabla: sp_movimientoCuenta_type,sp_naturalInformacionAdicionalType,sp_naturalInformacionBasicaType,sp_naturalTrabajoType,sp_personaDireccionesType,sp_personaReferenciasBancariasType,sp_personaTelefonosType,sp_personaType,sp_personaVinculacionesType,sp_servicioAdicional_type
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado
ON dbo.[sfct_afiliado]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[ci_cedula])),
            N'sfct_afiliado',
            @op,
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[no_nombre], x.[no_apellido], x.[no_direccion], x.[tx_telefono], x.[ci_titulo], x.[ci_provincia], x.[ci_categoria], x.[ci_nivel], x.[ci_regimen], x.[ci_pagador], x.[tx_iess], x.[tx_contrato], x.[fx_nacimiento], x.[fx_ingreso], x.[fx_fondo], x.[fx_reingreso], x.[fx_retiro], x.[va_sueldo], x.[va_liquido], x.[va_funcional], x.[va_adicional], x.[va_antiguedad], x.[pr_cam], x.[nu_anios_antiguedad], x.[fx_creacion], x.[ce_estado], x.[ci_ciudad], x.[ce_estadocivil], x.[tx_telefono2], x.[co_escuela], x.[ti_sector], x.[ci_nivelaporte], x.[ci_cedula_numerica], x.[fx_modificacion], x.[ci_provincia_residencia], x.[in_sexo], x.[va_hipotecario], x.[ci_usuario_ingr], x.[tx_email], x.[ci_usuario_modi], x.[tx_barrio], x.[ci_parroquia], x.[ci_rold_dsto_hipo], x.[ci_rolh_dsto_hipo], x.[tx_telefono3], x.[tx_telefono4], x.[ds_observaciones], x.[ci_cargo], x.[nu_carga], x.[ci_cedula_correccion], x.[pr_funcional], x.[fe_reti_parc], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[ci_cedula])),
            N'sfct_afiliado',
            N'DELETE',
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[no_nombre], x.[no_apellido], x.[no_direccion], x.[tx_telefono], x.[ci_titulo], x.[ci_provincia], x.[ci_categoria], x.[ci_nivel], x.[ci_regimen], x.[ci_pagador], x.[tx_iess], x.[tx_contrato], x.[fx_nacimiento], x.[fx_ingreso], x.[fx_fondo], x.[fx_reingreso], x.[fx_retiro], x.[va_sueldo], x.[va_liquido], x.[va_funcional], x.[va_adicional], x.[va_antiguedad], x.[pr_cam], x.[nu_anios_antiguedad], x.[fx_creacion], x.[ce_estado], x.[ci_ciudad], x.[ce_estadocivil], x.[tx_telefono2], x.[co_escuela], x.[ti_sector], x.[ci_nivelaporte], x.[ci_cedula_numerica], x.[fx_modificacion], x.[ci_provincia_residencia], x.[in_sexo], x.[va_hipotecario], x.[ci_usuario_ingr], x.[tx_email], x.[ci_usuario_modi], x.[tx_barrio], x.[ci_parroquia], x.[ci_rold_dsto_hipo], x.[ci_rolh_dsto_hipo], x.[tx_telefono3], x.[tx_telefono4], x.[ds_observaciones], x.[ci_cargo], x.[nu_carga], x.[ci_cedula_correccion], x.[pr_funcional], x.[fe_reti_parc], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_auditor', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_afiliado_auditor;
GO
-- Types canonicos que dependen de esta tabla: sp_auditoriaAfiliado_type
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_auditor
ON dbo.[sfct_afiliado_auditor]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[fe_generacion], x.[ho_generacion], x.[ce_estado], x.[tx_contrato], x.[ci_nivelaporte], x.[va_hipotecario], x.[ci_categoria], x.[ci_usuario], x.[ci_motivo_mant], x.[ci_cedula_coord], x.[pr_cam], x.[ci_cargo], x.[ds_fondos], x.[pr_funcional], x.[co_empr] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_auditor',
            @op,
            (SELECT x.[ci_cedula], x.[fe_generacion], x.[ho_generacion], x.[ce_estado], x.[tx_contrato], x.[ci_nivelaporte], x.[va_hipotecario], x.[ci_categoria], x.[ci_usuario], x.[ci_motivo_mant], x.[ci_cedula_coord], x.[pr_cam], x.[ci_cargo], x.[ds_fondos], x.[pr_funcional], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_auditor',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[fe_generacion], x.[ho_generacion], x.[ce_estado], x.[tx_contrato], x.[ci_nivelaporte], x.[va_hipotecario], x.[ci_categoria], x.[ci_usuario], x.[ci_motivo_mant], x.[ci_cedula_coord], x.[pr_cam], x.[ci_cargo], x.[ds_fondos], x.[pr_funcional], x.[co_empr] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_auditor',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[fe_generacion], x.[ho_generacion], x.[ce_estado], x.[tx_contrato], x.[ci_nivelaporte], x.[va_hipotecario], x.[ci_categoria], x.[ci_usuario], x.[ci_motivo_mant], x.[ci_cedula_coord], x.[pr_cam], x.[ci_cargo], x.[ds_fondos], x.[pr_funcional], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_auditor',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_fondos', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_afiliado_fondos;
GO
-- Types canonicos que dependen de esta tabla: sp_informacionAdicionalAfiliado_type
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_fondos
ON dbo.[sfct_afiliado_fondos]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[co_fond], x.[ce_estado], x.[fx_ultima_impr], x.[va_historico], x.[va_traspaso], x.[fx_ingreso], x.[fx_retiro], x.[fx_reingreso], x.[in_pres], x.[co_empr] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_fondos',
            @op,
            (SELECT x.[ci_cedula], x.[co_fond], x.[ce_estado], x.[fx_ultima_impr], x.[va_historico], x.[va_traspaso], x.[fx_ingreso], x.[fx_retiro], x.[fx_reingreso], x.[in_pres], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_fondos',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[co_fond], x.[ce_estado], x.[fx_ultima_impr], x.[va_historico], x.[va_traspaso], x.[fx_ingreso], x.[fx_retiro], x.[fx_reingreso], x.[in_pres], x.[co_empr] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_fondos',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[co_fond], x.[ce_estado], x.[fx_ultima_impr], x.[va_historico], x.[va_traspaso], x.[fx_ingreso], x.[fx_retiro], x.[fx_reingreso], x.[in_pres], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_fondos',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_otros', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_afiliado_otros;
GO
-- Types canonicos que dependen de esta tabla: sp_naturalIngresosEgresosType
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_otros
ON dbo.[sfct_afiliado_otros]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[ci_otros], x.[va_otros], x.[ci_drol_dsto], x.[ci_hrol_dsto], x.[ce_estado], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[co_empr] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_otros',
            @op,
            (SELECT x.[ci_cedula], x.[ci_otros], x.[va_otros], x.[ci_drol_dsto], x.[ci_hrol_dsto], x.[ce_estado], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_otros',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[ci_otros], x.[va_otros], x.[ci_drol_dsto], x.[ci_hrol_dsto], x.[ce_estado], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[co_empr] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_otros',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_otros], x.[va_otros], x.[ci_drol_dsto], x.[ci_hrol_dsto], x.[ce_estado], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_otros',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_referencias', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_afiliado_referencias;
GO
-- Types canonicos que dependen de esta tabla: sp_movimientoCuenta_type,sp_personaReferenciasBancariasType,sp_retiroLiquidacion_type
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_referencias
ON dbo.[sfct_afiliado_referencias]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[co_tref], x.[sc_refe], x.[ds_ref1], x.[ds_ref2], x.[ds_ref3], x.[ds_ref4], x.[ds_ref5], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_cart], x.[fe_actu_cart], x.[ci_cedu_ejec], x.[co_venc] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_referencias',
            @op,
            (SELECT x.[ci_cedula], x.[co_tref], x.[sc_refe], x.[ds_ref1], x.[ds_ref2], x.[ds_ref3], x.[ds_ref4], x.[ds_ref5], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_cart], x.[fe_actu_cart], x.[ci_cedu_ejec], x.[co_venc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_referencias',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[co_tref], x.[sc_refe], x.[ds_ref1], x.[ds_ref2], x.[ds_ref3], x.[ds_ref4], x.[ds_ref5], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_cart], x.[fe_actu_cart], x.[ci_cedu_ejec], x.[co_venc] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_referencias',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[co_tref], x.[sc_refe], x.[ds_ref1], x.[ds_ref2], x.[ds_ref3], x.[ds_ref4], x.[ds_ref5], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_cart], x.[fe_actu_cart], x.[ci_cedu_ejec], x.[co_venc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_referencias',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_rubro', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_afiliado_rubro;
GO
-- Types canonicos que dependen de esta tabla: sp_naturalIngresosEgresosType
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_rubro
ON dbo.[sfct_afiliado_rubro]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[ci_rubro_rol], x.[fx_prim_apor], x.[fx_ultm_apor], x.[rol_prim_apor], x.[rol_ultm_apor], x.[nu_aportaciones], x.[va_ultm_apor], x.[va_nc], x.[va_nd], x.[co_empr] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_rubro',
            @op,
            (SELECT x.[ci_cedula], x.[ci_rubro_rol], x.[fx_prim_apor], x.[fx_ultm_apor], x.[rol_prim_apor], x.[rol_ultm_apor], x.[nu_aportaciones], x.[va_ultm_apor], x.[va_nc], x.[va_nd], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_rubro',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[ci_rubro_rol], x.[fx_prim_apor], x.[fx_ultm_apor], x.[rol_prim_apor], x.[rol_ultm_apor], x.[nu_aportaciones], x.[va_ultm_apor], x.[va_nc], x.[va_nd], x.[co_empr] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_afiliado_rubro',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_rubro_rol], x.[fx_prim_apor], x.[fx_ultm_apor], x.[rol_prim_apor], x.[rol_ultm_apor], x.[nu_aportaciones], x.[va_ultm_apor], x.[va_nc], x.[va_nd], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_rubro',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_banco', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_banco;
GO
-- Types canonicos que dependen de esta tabla: sp_movimientoCuenta_type,sp_naturalInformacionAdicionalType,sp_personaDireccionesType,sp_personaReferenciasBancariasType,sp_personaType
CREATE TRIGGER dbo.trg_outbox_sfct_banco
ON dbo.[sfct_banco]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_banco], x.[no_banco], x.[in_reca], x.[fx_creacion], x.[ce_estado], x.[nu_ruc], x.[no_cont], x.[ds_dire], x.[nu_tele], x.[co_spi], x.[ti_cnta_spi], x.[in_firm_conv], x.[co_bnco_asoc] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_banco',
            @op,
            (SELECT x.[ci_banco], x.[no_banco], x.[in_reca], x.[fx_creacion], x.[ce_estado], x.[nu_ruc], x.[no_cont], x.[ds_dire], x.[nu_tele], x.[co_spi], x.[ti_cnta_spi], x.[in_firm_conv], x.[co_bnco_asoc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_banco',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_banco], x.[no_banco], x.[in_reca], x.[fx_creacion], x.[ce_estado], x.[nu_ruc], x.[no_cont], x.[ds_dire], x.[nu_tele], x.[co_spi], x.[ti_cnta_spi], x.[in_firm_conv], x.[co_bnco_asoc] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_banco',
            N'DELETE',
            (SELECT x.[ci_banco], x.[no_banco], x.[in_reca], x.[fx_creacion], x.[ce_estado], x.[nu_ruc], x.[no_cont], x.[ds_dire], x.[nu_tele], x.[co_spi], x.[ti_cnta_spi], x.[in_firm_conv], x.[co_bnco_asoc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_banco',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_beneficiario', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_beneficiario;
GO
-- Types canonicos que dependen de esta tabla: sp_beneficiarioParticipe_type,sp_naturalInformacionAdicionalType,sp_naturalInformacionBasicaType,sp_personaType,sp_personaVinculacionesType
CREATE TRIGGER dbo.trg_outbox_sfct_beneficiario
ON dbo.[sfct_beneficiario]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[ci_cedula]), CONVERT(NVARCHAR(200), i.[sc_bene])),
            N'sfct_beneficiario',
            @op,
            (SELECT x.[ci_cedula], x.[ci_cedula_beneficiario], x.[no_nombre], x.[no_apellido], x.[pr_porcentaje], x.[fx_creacion], x.[ce_beneficiario], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[sc_bene], x.[co_bnco_pago], x.[ti_cnta_pago], x.[nu_cnta_pago], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_beneficiario',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[ci_cedula]), CONVERT(NVARCHAR(200), d.[sc_bene])),
            N'sfct_beneficiario',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_cedula_beneficiario], x.[no_nombre], x.[no_apellido], x.[pr_porcentaje], x.[fx_creacion], x.[ce_beneficiario], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[sc_bene], x.[co_bnco_pago], x.[ti_cnta_pago], x.[nu_cnta_pago], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_beneficiario',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_beneficiario_retiro', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_beneficiario_retiro;
GO
-- Types canonicos que dependen de esta tabla: sp_beneficiarioParticipe_type
CREATE TRIGGER dbo.trg_outbox_sfct_beneficiario_retiro
ON dbo.[sfct_beneficiario_retiro]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[ci_retiro]), CONVERT(NVARCHAR(200), i.[sc_dbso])),
            N'sfct_beneficiario_retiro',
            @op,
            (SELECT x.[co_empr], x.[ci_retiro], x.[sc_dbso], x.[ci_cedula_beneficiario], x.[no_beneficiario], x.[pr_porcentaje], x.[va_desembolso], x.[ti_desembolso], x.[ci_banco], x.[nu_cuenta], x.[ti_cuenta], x.[fe_dbso], x.[co_bnco_dbso], x.[st_regi], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_beneficiario_retiro',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[ci_retiro]), CONVERT(NVARCHAR(200), d.[sc_dbso])),
            N'sfct_beneficiario_retiro',
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_retiro], x.[sc_dbso], x.[ci_cedula_beneficiario], x.[no_beneficiario], x.[pr_porcentaje], x.[va_desembolso], x.[ti_desembolso], x.[ci_banco], x.[nu_cuenta], x.[ti_cuenta], x.[fe_dbso], x.[co_bnco_dbso], x.[st_regi], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_beneficiario_retiro',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_cabecera_rol', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_cabecera_rol;
GO
-- Types canonicos que dependen de esta tabla: sp_rolNomina_type
CREATE TRIGGER dbo.trg_outbox_sfct_cabecera_rol
ON dbo.[sfct_cabecera_rol]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_rol]),
            N'sfct_cabecera_rol',
            @op,
            (SELECT x.[ci_rol], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[fx_creacion], x.[sc_rol], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_cabecera_rol',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_rol]),
            N'sfct_cabecera_rol',
            N'DELETE',
            (SELECT x.[ci_rol], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[fx_creacion], x.[sc_rol], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_cabecera_rol',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_ciudad', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_ciudad;
GO
-- Types canonicos que dependen de esta tabla: sp_naturalInformacionAdicionalType,sp_naturalInformacionBasicaType,sp_personaDireccionesType,sp_personaType
CREATE TRIGGER dbo.trg_outbox_sfct_ciudad
ON dbo.[sfct_ciudad]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_ciudad], x.[no_nombre], x.[ci_provincia], x.[fx_creacion], x.[ce_estado], x.[co_cant_sine], x.[co_banc], x.[in_cred_cnta], x.[co_zona], x.[co_dist] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_ciudad',
            @op,
            (SELECT x.[ci_ciudad], x.[no_nombre], x.[ci_provincia], x.[fx_creacion], x.[ce_estado], x.[co_cant_sine], x.[co_banc], x.[in_cred_cnta], x.[co_zona], x.[co_dist] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_ciudad',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_ciudad], x.[no_nombre], x.[ci_provincia], x.[fx_creacion], x.[ce_estado], x.[co_cant_sine], x.[co_banc], x.[in_cred_cnta], x.[co_zona], x.[co_dist] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_ciudad',
            N'DELETE',
            (SELECT x.[ci_ciudad], x.[no_nombre], x.[ci_provincia], x.[fx_creacion], x.[ce_estado], x.[co_cant_sine], x.[co_banc], x.[in_cred_cnta], x.[co_zona], x.[co_dist] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_ciudad',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_conyuge', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_conyuge;
GO
-- Types canonicos que dependen de esta tabla: sp_personaVinculacionesType
CREATE TRIGGER dbo.trg_outbox_sfct_conyuge
ON dbo.[sfct_conyuge]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedula]),
            N'sfct_conyuge',
            @op,
            (SELECT x.[ci_cedula], x.[ci_cedula_conyuge], x.[no_nombre_conyuge], x.[no_apellido_conyuge], x.[tx_direccion_conyuge], x.[ci_ciudad], x.[ci_provincia], x.[co_pais] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_conyuge',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedula]),
            N'sfct_conyuge',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_cedula_conyuge], x.[no_nombre_conyuge], x.[no_apellido_conyuge], x.[tx_direccion_conyuge], x.[ci_ciudad], x.[ci_provincia], x.[co_pais] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_conyuge',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_detalle_rol', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_detalle_rol;
GO
-- Types canonicos que dependen de esta tabla: sp_rolNomina_type
CREATE TRIGGER dbo.trg_outbox_sfct_detalle_rol
ON dbo.[sfct_detalle_rol]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_rol], x.[ci_cedula], x.[ci_rubro_rol], x.[va_generado], x.[ce_cobrar], x.[ci_categoria], x.[in_cble] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_detalle_rol',
            @op,
            (SELECT x.[sc_rol], x.[ci_cedula], x.[ci_rubro_rol], x.[va_generado], x.[ce_cobrar], x.[ci_categoria], x.[in_cble] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_detalle_rol',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_rol], x.[ci_cedula], x.[ci_rubro_rol], x.[va_generado], x.[ce_cobrar], x.[ci_categoria], x.[in_cble] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_detalle_rol',
            N'DELETE',
            (SELECT x.[sc_rol], x.[ci_cedula], x.[ci_rubro_rol], x.[va_generado], x.[ce_cobrar], x.[ci_categoria], x.[in_cble] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_detalle_rol',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_firmante', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_firmante;
GO
-- Types canonicos que dependen de esta tabla: sp_firmanteParticipe_type
CREATE TRIGGER dbo.trg_outbox_sfct_firmante
ON dbo.[sfct_firmante]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_tipo], x.[ci_institucion], x.[sc_firmante], x.[ci_cedula], x.[no_firmante], x.[co_empr] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_firmante',
            @op,
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[sc_firmante], x.[ci_cedula], x.[no_firmante], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_firmante',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_tipo], x.[ci_institucion], x.[sc_firmante], x.[ci_cedula], x.[no_firmante], x.[co_empr] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_firmante',
            N'DELETE',
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[sc_firmante], x.[ci_cedula], x.[no_firmante], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_firmante',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_grupo_fami', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_grupo_fami;
GO
-- Types canonicos que dependen de esta tabla: sp_grupoFamiliar_type
CREATE TRIGGER dbo.trg_outbox_sfct_grupo_fami
ON dbo.[sfct_grupo_fami]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[ci_cedula_familiar], x.[no_nombre], x.[no_apellido], x.[fx_nacimiento], x.[ti_relacion], x.[fx_creacion], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[ce_familiar], x.[in_discapacidad] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_grupo_fami',
            @op,
            (SELECT x.[ci_cedula], x.[ci_cedula_familiar], x.[no_nombre], x.[no_apellido], x.[fx_nacimiento], x.[ti_relacion], x.[fx_creacion], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[ce_familiar], x.[in_discapacidad] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_grupo_fami',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_cedula], x.[ci_cedula_familiar], x.[no_nombre], x.[no_apellido], x.[fx_nacimiento], x.[ti_relacion], x.[fx_creacion], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[ce_familiar], x.[in_discapacidad] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_grupo_fami',
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_cedula_familiar], x.[no_nombre], x.[no_apellido], x.[fx_nacimiento], x.[ti_relacion], x.[fx_creacion], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[ce_familiar], x.[in_discapacidad] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_grupo_fami',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_institucion', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_institucion;
GO
-- Types canonicos que dependen de esta tabla: sp_institucion_type,sp_naturalInformacionAdicionalType,sp_personaDireccionesType,sp_personaType
CREATE TRIGGER dbo.trg_outbox_sfct_institucion
ON dbo.[sfct_institucion]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[ci_tipo]), CONVERT(NVARCHAR(200), i.[ci_institucion])),
            N'sfct_institucion',
            @op,
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[no_institucion], x.[ci_provincia], x.[ci_ciudad], x.[ci_parroquia], x.[fx_creacion], x.[no_direccion], x.[nu_telefono], x.[nu_telefono2], x.[no_colector], x.[no_direccion_colector], x.[ci_provincia_colector], x.[ci_ciudad_colector], x.[ci_parroquia_colector], x.[nu_telefono_colector], x.[nu_telefono_colector2], x.[in_entrega], x.[ce_estado], x.[in_jornada], x.[in_recepcion], x.[in_municipales], x.[in_impresio_esta_cnta], x.[ci_patronal], x.[co_plan_sine], x.[no_rector], x.[co_usua_modi], x.[ci_cedula_colec], x.[ds_email], x.[pr_cam], x.[ti_direccion], x.[nu_cuenta_bc], x.[in_contrato_CAM], x.[nu_ruc], x.[ti_nivel], x.[ds_email_inst], x.[ti_sostenimiento], x.[nu_ute], x.[nu_zona], x.[co_usua_ingr], x.[fx_modificacion], x.[in_contrato_BCE], x.[fe_firma_BCE], x.[in_confirmacion_BCE], x.[ci_rol_ultm_actu], x.[fe_ultm_actu], x.[ho_ultm_actu], x.[co_pres_mefz], x.[nu_cnta_inst], x.[ti_cnta_inst], x.[co_bnco_inst], x.[in_dsto_bce], x.[co_amie], x.[co_sect], x.[co_dist], x.[co_circ], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_institucion',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[ci_tipo]), CONVERT(NVARCHAR(200), d.[ci_institucion])),
            N'sfct_institucion',
            N'DELETE',
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[no_institucion], x.[ci_provincia], x.[ci_ciudad], x.[ci_parroquia], x.[fx_creacion], x.[no_direccion], x.[nu_telefono], x.[nu_telefono2], x.[no_colector], x.[no_direccion_colector], x.[ci_provincia_colector], x.[ci_ciudad_colector], x.[ci_parroquia_colector], x.[nu_telefono_colector], x.[nu_telefono_colector2], x.[in_entrega], x.[ce_estado], x.[in_jornada], x.[in_recepcion], x.[in_municipales], x.[in_impresio_esta_cnta], x.[ci_patronal], x.[co_plan_sine], x.[no_rector], x.[co_usua_modi], x.[ci_cedula_colec], x.[ds_email], x.[pr_cam], x.[ti_direccion], x.[nu_cuenta_bc], x.[in_contrato_CAM], x.[nu_ruc], x.[ti_nivel], x.[ds_email_inst], x.[ti_sostenimiento], x.[nu_ute], x.[nu_zona], x.[co_usua_ingr], x.[fx_modificacion], x.[in_contrato_BCE], x.[fe_firma_BCE], x.[in_confirmacion_BCE], x.[ci_rol_ultm_actu], x.[fe_ultm_actu], x.[ho_ultm_actu], x.[co_pres_mefz], x.[nu_cnta_inst], x.[ti_cnta_inst], x.[co_bnco_inst], x.[in_dsto_bce], x.[co_amie], x.[co_sect], x.[co_dist], x.[co_circ], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_institucion',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_motivo', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_motivo;
GO
-- Types canonicos que dependen de esta tabla: sp_movimientoCuenta_type
CREATE TRIGGER dbo.trg_outbox_sfct_motivo
ON dbo.[sfct_motivo]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[ci_tipo_transaccion]), CONVERT(NVARCHAR(200), i.[ci_motivo])),
            N'sfct_motivo',
            @op,
            (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[no_motivo], x.[fx_creacion], x.[ce_estado], x.[in_usua_auto], x.[in_mvto_manu], x.[ci_motivo_contr], x.[in_moti_contr], x.[in_moti_ccup], x.[co_moti_ccup], x.[co_empr], x.[co_agru_moti] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[ci_tipo_transaccion]), CONVERT(NVARCHAR(200), d.[ci_motivo])),
            N'sfct_motivo',
            N'DELETE',
            (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[no_motivo], x.[fx_creacion], x.[ce_estado], x.[in_usua_auto], x.[in_mvto_manu], x.[ci_motivo_contr], x.[in_moti_contr], x.[in_moti_ccup], x.[co_moti_ccup], x.[co_empr], x.[co_agru_moti] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_motivo_cnta_cble', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_motivo_cnta_cble;
GO
-- Types canonicos que dependen de esta tabla: sp_motivoContable_type
CREATE TRIGGER dbo.trg_outbox_sfct_motivo_cnta_cble
ON dbo.[sfct_motivo_cnta_cble]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[co_cnta_auto_debe], x.[co_cnta_auto_habe], x.[co_empr], x.[co_fond] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_motivo_cnta_cble',
            @op,
            (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[co_cnta_auto_debe], x.[co_cnta_auto_habe], x.[co_empr], x.[co_fond] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo_cnta_cble',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[co_cnta_auto_debe], x.[co_cnta_auto_habe], x.[co_empr], x.[co_fond] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_motivo_cnta_cble',
            N'DELETE',
            (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[co_cnta_auto_debe], x.[co_cnta_auto_habe], x.[co_empr], x.[co_fond] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo_cnta_cble',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_motivo_mant_afiliados', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_motivo_mant_afiliados;
GO
-- Types canonicos que dependen de esta tabla: sp_auditoriaAfiliado_type
CREATE TRIGGER dbo.trg_outbox_sfct_motivo_mant_afiliados
ON dbo.[sfct_motivo_mant_afiliados]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_motivo_mant], x.[no_motivo_mant], x.[st_motivo_mant] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_motivo_mant_afiliados',
            @op,
            (SELECT x.[ci_motivo_mant], x.[no_motivo_mant], x.[st_motivo_mant] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo_mant_afiliados',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_motivo_mant], x.[no_motivo_mant], x.[st_motivo_mant] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_motivo_mant_afiliados',
            N'DELETE',
            (SELECT x.[ci_motivo_mant], x.[no_motivo_mant], x.[st_motivo_mant] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo_mant_afiliados',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_movimiento', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_movimiento;
GO
-- Types canonicos que dependen de esta tabla: sp_movimientoCuenta_type
CREATE TRIGGER dbo.trg_outbox_sfct_movimiento
ON dbo.[sfct_movimiento]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_empr] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_movimiento',
            @op,
            (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_movimiento',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_empr] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_movimiento',
            N'DELETE',
            (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_movimiento',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_movimiento_temp', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_movimiento_temp;
GO
-- Types canonicos que dependen de esta tabla: sp_movimientoTemporal_type
CREATE TRIGGER dbo.trg_outbox_sfct_movimiento_temp
ON dbo.[sfct_movimiento_temp]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_usua_ingr], x.[co_usua_conf], x.[st_mvto], x.[ci_tran_autr], x.[ds_movi], x.[in_proc], x.[fx_autr], x.[co_empr], x.[co_usua_veri], x.[fe_veri], x.[sc_carg_mvto] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_movimiento_temp',
            @op,
            (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_usua_ingr], x.[co_usua_conf], x.[st_mvto], x.[ci_tran_autr], x.[ds_movi], x.[in_proc], x.[fx_autr], x.[co_empr], x.[co_usua_veri], x.[fe_veri], x.[sc_carg_mvto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_movimiento_temp',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_usua_ingr], x.[co_usua_conf], x.[st_mvto], x.[ci_tran_autr], x.[ds_movi], x.[in_proc], x.[fx_autr], x.[co_empr], x.[co_usua_veri], x.[fe_veri], x.[sc_carg_mvto] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_movimiento_temp',
            N'DELETE',
            (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_usua_ingr], x.[co_usua_conf], x.[st_mvto], x.[ci_tran_autr], x.[ds_movi], x.[in_proc], x.[fx_autr], x.[co_empr], x.[co_usua_veri], x.[fe_veri], x.[sc_carg_mvto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_movimiento_temp',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_padbs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_padbs;
GO
-- Types canonicos que dependen de esta tabla: sp_cuentaBancariaAfiliado_type
CREATE TRIGGER dbo.trg_outbox_sfct_padbs
ON dbo.[sfct_padbs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[qs_liqd_hipo]), CONVERT(NVARCHAR(200), i.[qs_pago_dbso])),
            N'sfct_padbs',
            @op,
            (SELECT x.[co_empr], x.[qs_liqd_hipo], x.[qs_pago_dbso], x.[ti_pago], x.[mo_mvto], x.[nu_cta], x.[no_bcos], x.[co_bene], x.[ci_bnco], x.[ti_cnta], x.[ci_bnco_acre], x.[st_regi], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_padbs',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[qs_liqd_hipo]), CONVERT(NVARCHAR(200), d.[qs_pago_dbso])),
            N'sfct_padbs',
            N'DELETE',
            (SELECT x.[co_empr], x.[qs_liqd_hipo], x.[qs_pago_dbso], x.[ti_pago], x.[mo_mvto], x.[nu_cta], x.[no_bcos], x.[co_bene], x.[ci_bnco], x.[ti_cnta], x.[ci_bnco_acre], x.[st_regi], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_padbs',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_referencias', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_referencias;
GO
-- Types canonicos que dependen de esta tabla: sp_referenciaParticipe_type
CREATE TRIGGER dbo.trg_outbox_sfct_referencias
ON dbo.[sfct_referencias]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_tref], x.[ds_tref] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_referencias',
            @op,
            (SELECT x.[co_tref], x.[ds_tref] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_referencias',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_tref], x.[ds_tref] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'sfct_referencias',
            N'DELETE',
            (SELECT x.[co_tref], x.[ds_tref] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_referencias',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_retiro', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_retiro;
GO
-- Types canonicos que dependen de esta tabla: sp_retiroLiquidacion_type
CREATE TRIGGER dbo.trg_outbox_sfct_retiro
ON dbo.[sfct_retiro]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[ci_retiro])),
            N'sfct_retiro',
            @op,
            (SELECT x.[co_empr], x.[ci_retiro], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_provincia], x.[ci_pagador], x.[va_aporte], x.[va_fas], x.[va_acciones], x.[va_saldo_inicial], x.[fx_retiro], x.[ci_motivo_retiro], x.[fx_ingreso], x.[st_reti], x.[fx_cncd], x.[va_credito], x.[va_interes_ci], x.[va_interes_acci], x.[fx_proceso], x.[ci_rol], x.[ci_motivo], x.[fe_conf], x.[fe_autr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_ingr], x.[co_usua_elim], x.[fe_elim], x.[co_usua_autr_prov], x.[fe_autr_prov], x.[ce_estado_anterior], x.[co_fond], x.[va_rese_fas], x.[va_adic], x.[va_rete], x.[va_ccrd], x.[va_sobr], x.[va_gast], x.[va_cred_fond], x.[va_pago_fond], x.[va_cred_ccrd], x.[mo_cup], x.[mo_apor_cup], x.[mo_inve_hidr], x.[mo_capi_cdp], x.[mo_rent_cdp], x.[mo_cred_grte], x.[mo_gara_cup], x.[co_tasa_cup], x.[co_plaz_cup], x.[co_tipo_capi], x.[mo_rent_cup], x.[ci_cedu_hidr], x.[ci_cedu_cup], x.[co_orig], x.[in_cbro_pres], x.[co_proc], x.[in_proc_msvo] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_retiro',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[ci_retiro])),
            N'sfct_retiro',
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_retiro], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_provincia], x.[ci_pagador], x.[va_aporte], x.[va_fas], x.[va_acciones], x.[va_saldo_inicial], x.[fx_retiro], x.[ci_motivo_retiro], x.[fx_ingreso], x.[st_reti], x.[fx_cncd], x.[va_credito], x.[va_interes_ci], x.[va_interes_acci], x.[fx_proceso], x.[ci_rol], x.[ci_motivo], x.[fe_conf], x.[fe_autr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_ingr], x.[co_usua_elim], x.[fe_elim], x.[co_usua_autr_prov], x.[fe_autr_prov], x.[ce_estado_anterior], x.[co_fond], x.[va_rese_fas], x.[va_adic], x.[va_rete], x.[va_ccrd], x.[va_sobr], x.[va_gast], x.[va_cred_fond], x.[va_pago_fond], x.[va_cred_ccrd], x.[mo_cup], x.[mo_apor_cup], x.[mo_inve_hidr], x.[mo_capi_cdp], x.[mo_rent_cdp], x.[mo_cred_grte], x.[mo_gara_cup], x.[co_tasa_cup], x.[co_plaz_cup], x.[co_tipo_capi], x.[mo_rent_cup], x.[ci_cedu_hidr], x.[ci_cedu_cup], x.[co_orig], x.[in_cbro_pres], x.[co_proc], x.[in_proc_msvo] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_retiro',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_rubro_rol', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_rubro_rol;
GO
-- Types canonicos que dependen de esta tabla: sp_rolNomina_type
CREATE TRIGGER dbo.trg_outbox_sfct_rubro_rol
ON dbo.[sfct_rubro_rol]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[ci_rubro_rol])),
            N'sfct_rubro_rol',
            @op,
            (SELECT x.[ci_rubro_rol], x.[no_rubro_rol], x.[no_rubro_abreviado], x.[nu_prio], x.[ci_rol_inicial], x.[nu_debitos], x.[co_cnta_auto_mvto], x.[co_cnta_auto_tran], x.[tx_total], x.[qs_impresion], x.[ci_rubro_acum], x.[ti_reca], x.[in_mvto], x.[in_intr], x.[in_unif], x.[co_empr], x.[ti_dato_dsto], x.[ds_esta_afil] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_rubro_rol',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[ci_rubro_rol])),
            N'sfct_rubro_rol',
            N'DELETE',
            (SELECT x.[ci_rubro_rol], x.[no_rubro_rol], x.[no_rubro_abreviado], x.[nu_prio], x.[ci_rol_inicial], x.[nu_debitos], x.[co_cnta_auto_mvto], x.[co_cnta_auto_tran], x.[tx_total], x.[qs_impresion], x.[ci_rubro_acum], x.[ti_reca], x.[in_mvto], x.[in_intr], x.[in_unif], x.[co_empr], x.[ti_dato_dsto], x.[ds_esta_afil] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_rubro_rol',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbFC];
GO
IF OBJECT_ID(N'dbo.trg_outbox_sfct_saldos_diarios_afiliados', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_saldos_diarios_afiliados;
GO
-- Types canonicos que dependen de esta tabla: sp_saldoDiario_type
CREATE TRIGGER dbo.trg_outbox_sfct_saldos_diarios_afiliados
ON dbo.[sfct_saldos_diarios_afiliados]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), i.[fx_saldo]), CONVERT(NVARCHAR(200), i.[co_empr]), CONVERT(NVARCHAR(200), i.[co_fond]), CONVERT(NVARCHAR(200), i.[ci_cedula])),
            N'sfct_saldos_diarios_afiliados',
            @op,
            (SELECT x.[fx_saldo], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[ci_pagador], x.[ci_cedula], x.[ce_estado], x.[co_fond], x.[va_saldo], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_saldos_diarios_afiliados',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|', CONVERT(NVARCHAR(200), d.[fx_saldo]), CONVERT(NVARCHAR(200), d.[co_empr]), CONVERT(NVARCHAR(200), d.[co_fond]), CONVERT(NVARCHAR(200), d.[ci_cedula])),
            N'sfct_saldos_diarios_afiliados',
            N'DELETE',
            (SELECT x.[fx_saldo], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[ci_pagador], x.[ci_cedula], x.[ce_estado], x.[co_fond], x.[va_saldo], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_saldos_diarios_afiliados',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO


/* ------------------------------------------------------------------
   BD ORIGINAL: dbIM
   ------------------------------------------------------------------ */
USE [dbIM];
GO
IF OBJECT_ID(N'dbo.trg_outbox_imtbmiem_cony', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_imtbmiem_cony;
GO
-- Types canonicos que dependen de esta tabla: sp_personaVinculacionesType
CREATE TRIGGER dbo.trg_outbox_imtbmiem_cony
ON dbo.[imtbmiem_cony]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_miem], x.[co_cony], x.[no_apel], x.[no_nomb], x.[ds_dire], x.[co_prov], x.[co_ciud] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'imtbmiem_cony',
            @op,
            (SELECT x.[co_miem], x.[co_cony], x.[no_apel], x.[no_nomb], x.[ds_dire], x.[co_prov], x.[co_ciud] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbIM.dbo.imtbmiem_cony',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_miem], x.[co_cony], x.[no_apel], x.[no_nomb], x.[ds_dire], x.[co_prov], x.[co_ciud] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'imtbmiem_cony',
            N'DELETE',
            (SELECT x.[co_miem], x.[co_cony], x.[no_apel], x.[no_nomb], x.[ds_dire], x.[co_prov], x.[co_ciud] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbIM.dbo.imtbmiem_cony',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO


/* ------------------------------------------------------------------
   BD ORIGINAL: dbNO
   ------------------------------------------------------------------ */
USE [dbNO];
GO
IF OBJECT_ID(N'dbo.trg_outbox_notbcgfm', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_notbcgfm;
GO
-- Types canonicos que dependen de esta tabla: sp_naturalInformacionAdicionalType,sp_naturalInformacionBasicaType,sp_personaType
CREATE TRIGGER dbo.trg_outbox_notbcgfm
ON dbo.[notbcgfm]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_empr], x.[co_empl], x.[sc_cgfm], x.[ti_rela], x.[no_nomb], x.[fe_naci], x.[ti_iden], x.[nu_iden], x.[in_minu], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[st_regi] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'notbcgfm',
            @op,
            (SELECT x.[co_empr], x.[co_empl], x.[sc_cgfm], x.[ti_rela], x.[no_nomb], x.[fe_naci], x.[ti_iden], x.[nu_iden], x.[in_minu], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcgfm',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_empr], x.[co_empl], x.[sc_cgfm], x.[ti_rela], x.[no_nomb], x.[fe_naci], x.[ti_iden], x.[nu_iden], x.[in_minu], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[st_regi] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'notbcgfm',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_empl], x.[sc_cgfm], x.[ti_rela], x.[no_nomb], x.[fe_naci], x.[ti_iden], x.[nu_iden], x.[in_minu], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcgfm',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbNO];
GO
IF OBJECT_ID(N'dbo.trg_outbox_notbempl', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_notbempl;
GO
-- Types canonicos que dependen de esta tabla: sp_naturalInformacionAdicionalType,sp_naturalInformacionBasicaType,sp_personaDireccionesType,sp_personaType,sp_personaVinculacionesType
CREATE TRIGGER dbo.trg_outbox_notbempl
ON dbo.[notbempl]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_empr], x.[co_empl], x.[no_empl], x.[no_dire], x.[co_carg], x.[nu_telf], x.[co_loca], x.[co_dpto], x.[co_empl_jefe], x.[co_prof], x.[in_iess], x.[nu_patr], x.[nu_iess], x.[fe_afil_iess], x.[in_sexo], x.[in_esta_civil], x.[fe_naci], x.[in_pago_cnta], x.[nu_cnta_pago], x.[ti_cnta_pago], x.[fe_ingr], x.[fe_sali], x.[ds_debe_oblg], x.[ti_iden], x.[nu_iden], x.[co_hora], x.[ds_clav], x.[mo_suel], x.[mo_suel_neto], x.[in_firm_entr_sali], x.[co_clas], x.[fe_vcto_cont], x.[st_empl], x.[co_usua_mvto_ingr], x.[fe_mvto_ingr], x.[co_usua_mvto_modi], x.[fe_mvto_modi], x.[co_cnta_anti], x.[co_banc_pago], x.[in_pago_cnta_fond_rese], x.[co_banc_pago_fond_rese], x.[nu_cnta_pago_fond_rese], x.[ti_cnta_pago_fond_rese], x.[in_peri_grac], x.[fe_grac_inic], x.[fe_grac_fina], x.[ds_mail], x.[no_usua], x.[nu_exte_tele], x.[co_prov_naci], x.[co_ciud_naci], x.[co_parr], x.[no_sect], x.[ds_refe_vivi], x.[nu_telf_celu], x.[no_cont_emrg], x.[nu_telf_emrg], x.[ds_sut] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'notbempl',
            @op,
            (SELECT x.[co_empr], x.[co_empl], x.[no_empl], x.[no_dire], x.[co_carg], x.[nu_telf], x.[co_loca], x.[co_dpto], x.[co_empl_jefe], x.[co_prof], x.[in_iess], x.[nu_patr], x.[nu_iess], x.[fe_afil_iess], x.[in_sexo], x.[in_esta_civil], x.[fe_naci], x.[in_pago_cnta], x.[nu_cnta_pago], x.[ti_cnta_pago], x.[fe_ingr], x.[fe_sali], x.[ds_debe_oblg], x.[ti_iden], x.[nu_iden], x.[co_hora], x.[ds_clav], x.[mo_suel], x.[mo_suel_neto], x.[in_firm_entr_sali], x.[co_clas], x.[fe_vcto_cont], x.[st_empl], x.[co_usua_mvto_ingr], x.[fe_mvto_ingr], x.[co_usua_mvto_modi], x.[fe_mvto_modi], x.[co_cnta_anti], x.[co_banc_pago], x.[in_pago_cnta_fond_rese], x.[co_banc_pago_fond_rese], x.[nu_cnta_pago_fond_rese], x.[ti_cnta_pago_fond_rese], x.[in_peri_grac], x.[fe_grac_inic], x.[fe_grac_fina], x.[ds_mail], x.[no_usua], x.[nu_exte_tele], x.[co_prov_naci], x.[co_ciud_naci], x.[co_parr], x.[no_sect], x.[ds_refe_vivi], x.[nu_telf_celu], x.[no_cont_emrg], x.[nu_telf_emrg], x.[ds_sut] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_empr], x.[co_empl], x.[no_empl], x.[no_dire], x.[co_carg], x.[nu_telf], x.[co_loca], x.[co_dpto], x.[co_empl_jefe], x.[co_prof], x.[in_iess], x.[nu_patr], x.[nu_iess], x.[fe_afil_iess], x.[in_sexo], x.[in_esta_civil], x.[fe_naci], x.[in_pago_cnta], x.[nu_cnta_pago], x.[ti_cnta_pago], x.[fe_ingr], x.[fe_sali], x.[ds_debe_oblg], x.[ti_iden], x.[nu_iden], x.[co_hora], x.[ds_clav], x.[mo_suel], x.[mo_suel_neto], x.[in_firm_entr_sali], x.[co_clas], x.[fe_vcto_cont], x.[st_empl], x.[co_usua_mvto_ingr], x.[fe_mvto_ingr], x.[co_usua_mvto_modi], x.[fe_mvto_modi], x.[co_cnta_anti], x.[co_banc_pago], x.[in_pago_cnta_fond_rese], x.[co_banc_pago_fond_rese], x.[nu_cnta_pago_fond_rese], x.[ti_cnta_pago_fond_rese], x.[in_peri_grac], x.[fe_grac_inic], x.[fe_grac_fina], x.[ds_mail], x.[no_usua], x.[nu_exte_tele], x.[co_prov_naci], x.[co_ciud_naci], x.[co_parr], x.[no_sect], x.[ds_refe_vivi], x.[nu_telf_celu], x.[no_cont_emrg], x.[nu_telf_emrg], x.[ds_sut] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'notbempl',
            N'DELETE',
            (SELECT x.[co_empr], x.[co_empl], x.[no_empl], x.[no_dire], x.[co_carg], x.[nu_telf], x.[co_loca], x.[co_dpto], x.[co_empl_jefe], x.[co_prof], x.[in_iess], x.[nu_patr], x.[nu_iess], x.[fe_afil_iess], x.[in_sexo], x.[in_esta_civil], x.[fe_naci], x.[in_pago_cnta], x.[nu_cnta_pago], x.[ti_cnta_pago], x.[fe_ingr], x.[fe_sali], x.[ds_debe_oblg], x.[ti_iden], x.[nu_iden], x.[co_hora], x.[ds_clav], x.[mo_suel], x.[mo_suel_neto], x.[in_firm_entr_sali], x.[co_clas], x.[fe_vcto_cont], x.[st_empl], x.[co_usua_mvto_ingr], x.[fe_mvto_ingr], x.[co_usua_mvto_modi], x.[fe_mvto_modi], x.[co_cnta_anti], x.[co_banc_pago], x.[in_pago_cnta_fond_rese], x.[co_banc_pago_fond_rese], x.[nu_cnta_pago_fond_rese], x.[ti_cnta_pago_fond_rese], x.[in_peri_grac], x.[fe_grac_inic], x.[fe_grac_fina], x.[ds_mail], x.[no_usua], x.[nu_exte_tele], x.[co_prov_naci], x.[co_ciud_naci], x.[co_parr], x.[no_sect], x.[ds_refe_vivi], x.[nu_telf_celu], x.[no_cont_emrg], x.[nu_telf_emrg], x.[ds_sut] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO


/* ------------------------------------------------------------------
   BD ORIGINAL: dbSV
   ------------------------------------------------------------------ */
USE [dbSV];
GO
IF OBJECT_ID(N'dbo.trg_outbox_svtbcaus', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_svtbcaus;
GO
-- Types canonicos que dependen de esta tabla: sp_seguroVidaParticipe_type
CREATE TRIGGER dbo.trg_outbox_svtbcaus
ON dbo.[svtbcaus]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_caus], x.[no_caus], x.[st_caus], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbcaus',
            @op,
            (SELECT x.[co_caus], x.[no_caus], x.[st_caus], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbcaus',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_caus], x.[no_caus], x.[st_caus], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbcaus',
            N'DELETE',
            (SELECT x.[co_caus], x.[no_caus], x.[st_caus], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbcaus',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbSV];
GO
IF OBJECT_ID(N'dbo.trg_outbox_svtbdisc', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_svtbdisc;
GO
-- Types canonicos que dependen de esta tabla: sp_seguroVidaParticipe_type
CREATE TRIGGER dbo.trg_outbox_svtbdisc
ON dbo.[svtbdisc]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_disc], x.[no_disc], x.[st_disc], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbdisc',
            @op,
            (SELECT x.[co_disc], x.[no_disc], x.[st_disc], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbdisc',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_disc], x.[no_disc], x.[st_disc], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbdisc',
            N'DELETE',
            (SELECT x.[co_disc], x.[no_disc], x.[st_disc], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbdisc',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbSV];
GO
IF OBJECT_ID(N'dbo.trg_outbox_svtbefec', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_svtbefec;
GO
-- Types canonicos que dependen de esta tabla: sp_seguroVidaParticipe_type
CREATE TRIGGER dbo.trg_outbox_svtbefec
ON dbo.[svtbefec]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_efec], x.[no_efec], x.[ti_efec], x.[st_efec], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbefec',
            @op,
            (SELECT x.[co_efec], x.[no_efec], x.[ti_efec], x.[st_efec], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbefec',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_efec], x.[no_efec], x.[ti_efec], x.[st_efec], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbefec',
            N'DELETE',
            (SELECT x.[co_efec], x.[no_efec], x.[ti_efec], x.[st_efec], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbefec',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbSV];
GO
IF OBJECT_ID(N'dbo.trg_outbox_svtbfmpg', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_svtbfmpg;
GO
-- Types canonicos que dependen de esta tabla: sp_seguroVidaParticipe_type
CREATE TRIGGER dbo.trg_outbox_svtbfmpg
ON dbo.[svtbfmpg]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ti_fmpg], x.[ds_fmpg], x.[st_fmpg] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbfmpg',
            @op,
            (SELECT x.[ti_fmpg], x.[ds_fmpg], x.[st_fmpg] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbfmpg',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[ti_fmpg], x.[ds_fmpg], x.[st_fmpg] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbfmpg',
            N'DELETE',
            (SELECT x.[ti_fmpg], x.[ds_fmpg], x.[st_fmpg] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbfmpg',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbSV];
GO
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_svtbstro;
GO
-- Types canonicos que dependen de esta tabla: sp_seguroVidaParticipe_type
CREATE TRIGGER dbo.trg_outbox_svtbstro
ON dbo.[svtbstro]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_sine], x.[co_prov], x.[ti_sine], x.[co_afil], x.[co_sine], x.[co_caus], x.[co_efec], x.[co_disc], x.[fe_ingr], x.[fe_fall], x.[fe_pres], x.[fe_noti], x.[fe_autr], x.[fe_conf], x.[fe_elim], x.[ti_fmpg], x.[co_banco], x.[co_tfam], x.[st_sine], x.[co_usua_ingr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_elim], x.[ce_esta_ante], x.[co_empr] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro',
            @op,
            (SELECT x.[sc_sine], x.[co_prov], x.[ti_sine], x.[co_afil], x.[co_sine], x.[co_caus], x.[co_efec], x.[co_disc], x.[fe_ingr], x.[fe_fall], x.[fe_pres], x.[fe_noti], x.[fe_autr], x.[fe_conf], x.[fe_elim], x.[ti_fmpg], x.[co_banco], x.[co_tfam], x.[st_sine], x.[co_usua_ingr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_elim], x.[ce_esta_ante], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_sine], x.[co_prov], x.[ti_sine], x.[co_afil], x.[co_sine], x.[co_caus], x.[co_efec], x.[co_disc], x.[fe_ingr], x.[fe_fall], x.[fe_pres], x.[fe_noti], x.[fe_autr], x.[fe_conf], x.[fe_elim], x.[ti_fmpg], x.[co_banco], x.[co_tfam], x.[st_sine], x.[co_usua_ingr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_elim], x.[ce_esta_ante], x.[co_empr] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro',
            N'DELETE',
            (SELECT x.[sc_sine], x.[co_prov], x.[ti_sine], x.[co_afil], x.[co_sine], x.[co_caus], x.[co_efec], x.[co_disc], x.[fe_ingr], x.[fe_fall], x.[fe_pres], x.[fe_noti], x.[fe_autr], x.[fe_conf], x.[fe_elim], x.[ti_fmpg], x.[co_banco], x.[co_tfam], x.[st_sine], x.[co_usua_ingr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_elim], x.[ce_esta_ante], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbSV];
GO
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro_bene', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_svtbstro_bene;
GO
-- Types canonicos que dependen de esta tabla: sp_seguroVidaParticipe_type
CREATE TRIGGER dbo.trg_outbox_svtbstro_bene
ON dbo.[svtbstro_bene]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_sine], x.[co_fond], x.[co_bene], x.[pr_dist], x.[mo_dist], x.[co_tpar] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro_bene',
            @op,
            (SELECT x.[sc_sine], x.[co_fond], x.[co_bene], x.[pr_dist], x.[mo_dist], x.[co_tpar] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_bene',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_sine], x.[co_fond], x.[co_bene], x.[pr_dist], x.[mo_dist], x.[co_tpar] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro_bene',
            N'DELETE',
            (SELECT x.[sc_sine], x.[co_fond], x.[co_bene], x.[pr_dist], x.[mo_dist], x.[co_tpar] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_bene',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbSV];
GO
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_svtbstro_cred;
GO
-- Types canonicos que dependen de esta tabla: sp_seguroVidaParticipe_type
CREATE TRIGGER dbo.trg_outbox_svtbstro_cred
ON dbo.[svtbstro_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_sine], x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[mo_abno], x.[va_pago_fond], x.[va_desg], x.[va_desg_cont], x.[mo_cobe], x.[in_pago_desg] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro_cred',
            @op,
            (SELECT x.[sc_sine], x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[mo_abno], x.[va_pago_fond], x.[va_desg], x.[va_desg_cont], x.[mo_cobe], x.[in_pago_desg] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_cred',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_sine], x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[mo_abno], x.[va_pago_fond], x.[va_desg], x.[va_desg_cont], x.[mo_cobe], x.[in_pago_desg] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro_cred',
            N'DELETE',
            (SELECT x.[sc_sine], x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[mo_abno], x.[va_pago_fond], x.[va_desg], x.[va_desg_cont], x.[mo_cobe], x.[in_pago_desg] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_cred',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbSV];
GO
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro_deta', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_svtbstro_deta;
GO
-- Types canonicos que dependen de esta tabla: sp_seguroVidaParticipe_type
CREATE TRIGGER dbo.trg_outbox_svtbstro_deta
ON dbo.[svtbstro_deta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_sine], x.[co_fond], x.[mo_cobe], x.[mo_desg], x.[mo_ncub], x.[mo_ci], x.[mo_tota], x.[va_pago_fond] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro_deta',
            @op,
            (SELECT x.[sc_sine], x.[co_fond], x.[mo_cobe], x.[mo_desg], x.[mo_ncub], x.[mo_ci], x.[mo_tota], x.[va_pago_fond] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_deta',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[sc_sine], x.[co_fond], x.[mo_cobe], x.[mo_desg], x.[mo_ncub], x.[mo_ci], x.[mo_tota], x.[va_pago_fond] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro_deta',
            N'DELETE',
            (SELECT x.[sc_sine], x.[co_fond], x.[mo_cobe], x.[mo_desg], x.[mo_ncub], x.[mo_ci], x.[mo_tota], x.[va_pago_fond] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_deta',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO

USE [dbSV];
GO
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro_exte', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_svtbstro_exte;
GO
-- Types canonicos que dependen de esta tabla: sp_seguroVidaParticipe_type
CREATE TRIGGER dbo.trg_outbox_svtbstro_exte
ON dbo.[svtbstro_exte]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_afil], x.[co_sine], x.[fe_ingr_exte], x.[st_sine_exte], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro_exte',
            @op,
            (SELECT x.[co_afil], x.[co_sine], x.[fe_ingr_exte], x.[st_sine_exte], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_exte',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), HASHBYTES('SHA1', (SELECT x.[co_afil], x.[co_sine], x.[fe_ingr_exte], x.[st_sine_exte], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2),
            N'svtbstro_exte',
            N'DELETE',
            (SELECT x.[co_afil], x.[co_sine], x.[fe_ingr_exte], x.[st_sine_exte], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_exte',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO
