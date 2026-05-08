/* ============================================================
   DUMP COMPLETO DE TRIGGERS OUTBOX - modulo PARTICIPE
   Snapshot generado del estado actual de las BDs
   ============================================================ */

/* ############################################################
   FLUJO 1 - SQL Server legacy (publica a fcme_canonicos.cdc_outbox)
   ############################################################ */

/* ----- BD: dbIM  (2 triggers) ----- */
USE [dbIM];
GO

/* --- trg_outbox_imtbbene_firm  ON dbo.imtbbene_firm  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_imtbbene_firm', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_imtbbene_firm;
GO
CREATE TRIGGER dbo.trg_outbox_imtbbene_firm
ON dbo.[imtbbene_firm]
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
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_bene]),CONVERT(NVARCHAR(200), i.[sc_vivi])),
            N'personaFirmasType',
            @op,
            (SELECT x.[co_prog],x.[co_bene],x.[sc_vivi],x.[fe_firm],x.[ds_obse] FROM inserted x WHERE x.[co_bene]=i.[co_bene] AND x.[sc_vivi]=i.[sc_vivi] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbIM.dbo.imtbbene_firm',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_bene]),CONVERT(NVARCHAR(200), d.[sc_vivi])),
            N'personaFirmasType',
            N'DELETE',
            (SELECT x.[co_prog],x.[co_bene],x.[sc_vivi],x.[fe_firm],x.[ds_obse] FROM deleted x WHERE x.[co_bene]=d.[co_bene] AND x.[sc_vivi]=d.[sc_vivi] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbIM.dbo.imtbbene_firm',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_imtbmiem_cony  ON dbo.imtbmiem_cony  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_imtbmiem_cony', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_imtbmiem_cony;
GO
CREATE TRIGGER dbo.trg_outbox_imtbmiem_cony
ON dbo.[imtbmiem_cony]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'personaVinculacionesType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_miem]),
            tt.t,
            @op,
            (SELECT x.[co_miem], x.[co_cony], x.[no_apel], x.[no_nomb], x.[ds_dire], x.[co_prov], x.[co_ciud] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbIM.dbo.imtbmiem_cony',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_miem]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_miem], x.[co_cony], x.[no_apel], x.[no_nomb], x.[ds_dire], x.[co_prov], x.[co_ciud] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbIM.dbo.imtbmiem_cony',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* ----- BD: dbFC  (60 triggers) ----- */
USE [dbFC];
GO

/* --- trg_outbox_fctbactv_suje_cred  ON dbo.fctbactv_suje_cred  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbactv_suje_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbactv_suje_cred;
GO
CREATE TRIGGER dbo.trg_outbox_fctbactv_suje_cred
ON dbo.[fctbactv_suje_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'informacionAdicionalAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_actv_suje_cred]),
            tt.t,
            @op,
            (SELECT x.[sc_actv_suje_cred], x.[co_actv_suje_cred], x.[ds_actv_suje_cred], x.[st_actv_suje_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbactv_suje_cred',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_actv_suje_cred]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_actv_suje_cred], x.[co_actv_suje_cred], x.[ds_actv_suje_cred], x.[st_actv_suje_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbactv_suje_cred',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbafil_actu  ON dbo.fctbafil_actu  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_actu', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbafil_actu;
GO
CREATE TRIGGER dbo.trg_outbox_fctbafil_actu
ON dbo.[fctbafil_actu]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'actualizacionAfiliadoType'), (N'naturalTrabajoType'), (N'personaTelefonosType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedu]),
            tt.t,
            @op,
            (SELECT x.[ci_cedu], x.[co_prov], x.[co_cant], x.[co_parr], x.[ds_call_prim], x.[nu_call_prim], x.[ds_call_secu], x.[nu_call_secu], x.[nu_manz], x.[nu_vill], x.[ds_cdla], x.[tx_telf_conv], x.[tx_telf_celu], x.[ti_oper], x.[ds_refe_vivi], x.[tx_mail], x.[no_cont_adic], x.[tx_telf_con1], x.[tx_telf_con2], x.[ti_rela], x.[co_prov_inst], x.[ci_tipo], x.[co_inst], x.[co_carg], x.[co_nive], x.[co_cate], x.[ti_cont], x.[ti_jorn], x.[co_prov_obsq], x.[co_zona_obsq], x.[in_reno_cred], x.[in_acci], x.[fe_ingr], x.[fe_modi], x.[fe_ultm_envi], x.[in_impr_docu], x.[fe_impr_docu], x.[in_cobr_pres], x.[in_vald_celu], x.[in_vald_mail], x.[co_ami], x.[st_entr_obsq], x.[fe_entr_obsq], x.[fe_veri_dato], x.[in_impr_docu_cred], x.[fe_impr_docu_cred], x.[no_inst], x.[co_cant_inst], x.[co_parr_inst] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_actu',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedu]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedu], x.[co_prov], x.[co_cant], x.[co_parr], x.[ds_call_prim], x.[nu_call_prim], x.[ds_call_secu], x.[nu_call_secu], x.[nu_manz], x.[nu_vill], x.[ds_cdla], x.[tx_telf_conv], x.[tx_telf_celu], x.[ti_oper], x.[ds_refe_vivi], x.[tx_mail], x.[no_cont_adic], x.[tx_telf_con1], x.[tx_telf_con2], x.[ti_rela], x.[co_prov_inst], x.[ci_tipo], x.[co_inst], x.[co_carg], x.[co_nive], x.[co_cate], x.[ti_cont], x.[ti_jorn], x.[co_prov_obsq], x.[co_zona_obsq], x.[in_reno_cred], x.[in_acci], x.[fe_ingr], x.[fe_modi], x.[fe_ultm_envi], x.[in_impr_docu], x.[fe_impr_docu], x.[in_cobr_pres], x.[in_vald_celu], x.[in_vald_mail], x.[co_ami], x.[st_entr_obsq], x.[fe_entr_obsq], x.[fe_veri_dato], x.[in_impr_docu_cred], x.[fe_impr_docu_cred], x.[no_inst], x.[co_cant_inst], x.[co_parr_inst] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_actu',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbafil_ahor_refe  ON dbo.fctbafil_ahor_refe  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_ahor_refe', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbafil_ahor_refe;
GO
CREATE TRIGGER dbo.trg_outbox_fctbafil_ahor_refe
ON dbo.[fctbafil_ahor_refe]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'personaReferenciasPersonalesType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_Cedu_refe]),
            tt.t,
            @op,
            (SELECT x.[ci_cedu], x.[ci_Cedu_refe], x.[no_nomb], x.[no_apel], x.[no_pare], x.[nu_telf] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_ahor_refe',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_Cedu_refe]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedu], x.[ci_Cedu_refe], x.[no_nomb], x.[no_apel], x.[no_pare], x.[nu_telf] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_ahor_refe',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbafil_auto_docs  ON dbo.fctbafil_auto_docs  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_auto_docs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbafil_auto_docs;
GO
CREATE TRIGGER dbo.trg_outbox_fctbafil_auto_docs
ON dbo.[fctbafil_auto_docs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'documentacionAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_cedu]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_regi]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_cedu], x.[sc_regi], x.[co_docu], x.[fe_crea], x.[fe_elim], x.[co_auto], x.[st_regi], x.[co_usua_crea], x.[co_usua_elim], x.[fe_firm_dcto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_auto_docs',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_cedu]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_regi]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_cedu], x.[sc_regi], x.[co_docu], x.[fe_crea], x.[fe_elim], x.[co_auto], x.[st_regi], x.[co_usua_crea], x.[co_usua_elim], x.[fe_firm_dcto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_auto_docs',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbafil_dcap  ON dbo.fctbafil_dcap  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_dcap', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbafil_dcap;
GO
CREATE TRIGGER dbo.trg_outbox_fctbafil_dcap
ON dbo.[fctbafil_dcap]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'informacionAdicionalAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_dsto_cap]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[ti_dsto], x.[ci_rold], x.[ci_rolh], x.[va_dsto], x.[st_dsto], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[sc_dsto_cap], x.[co_prod] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_dcap',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_dsto_cap]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ti_dsto], x.[ci_rold], x.[ci_rolh], x.[va_dsto], x.[st_dsto], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[sc_dsto_cap], x.[co_prod] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_dcap',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbafil_gast_pers  ON dbo.fctbafil_gast_pers  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_gast_pers', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbafil_gast_pers;
GO
CREATE TRIGGER dbo.trg_outbox_fctbafil_gast_pers
ON dbo.[fctbafil_gast_pers]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'informacionAdicionalAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_cedu]) + N'|' + CONVERT(NVARCHAR(100), i.[co_elem]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_cedu], x.[co_elem], x.[mo_mnto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_gast_pers',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_cedu]) + N'|' + CONVERT(NVARCHAR(100), d.[co_elem]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_cedu], x.[co_elem], x.[mo_mnto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_gast_pers',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbafil_info_actu_docs  ON dbo.fctbafil_info_actu_docs  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_info_actu_docs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbafil_info_actu_docs;
GO
CREATE TRIGGER dbo.trg_outbox_fctbafil_info_actu_docs
ON dbo.[fctbafil_info_actu_docs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'actualizacionDocumentosType'), (N'naturalInformacionAdicionalType'), (N'naturalInformacionBasicaType'), (N'personaDireccionesType'), (N'personaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[sc_actu_docs]) + N'|' + CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_cedu]),
            tt.t,
            @op,
            (SELECT x.[sc_actu_docs], x.[co_empr], x.[co_cedu], x.[sc_actv_suje_cred], x.[sc_orgn_ingr], x.[co_pers_poli_expu], x.[ds_ciud_naci], x.[in_comi_serv], x.[ds_comi_serv], x.[fx_ingr], x.[co_usua_ingr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_info_actu_docs',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[sc_actu_docs]) + N'|' + CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_cedu]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_actu_docs], x.[co_empr], x.[co_cedu], x.[sc_actv_suje_cred], x.[sc_orgn_ingr], x.[co_pers_poli_expu], x.[ds_ciud_naci], x.[in_comi_serv], x.[ds_comi_serv], x.[fx_ingr], x.[co_usua_ingr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_info_actu_docs',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbafil_info_adic  ON dbo.fctbafil_info_adic  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_info_adic', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbafil_info_adic;
GO
CREATE TRIGGER dbo.trg_outbox_fctbafil_info_adic
ON dbo.[fctbafil_info_adic]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'informacionAdicionalAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedu]),
            tt.t,
            @op,
            (SELECT x.[ci_cedu], x.[ds_calle_prim], x.[nu_calle_prim], x.[ds_calle_secu], x.[nu_calle_secu], x.[nu_manz], x.[nu_villa], x.[ds_refe_vivi], x.[ti_oper], x.[no_cont_adic], x.[tx_telf_con1], x.[tx_telf_con2], x.[ti_rela], x.[ti_jorn], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_dist_amie], x.[co_dist_mins], x.[in_corr_cedu], x.[co_pais_naci], x.[co_area_lbrl] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_info_adic',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedu]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedu], x.[ds_calle_prim], x.[nu_calle_prim], x.[ds_calle_secu], x.[nu_calle_secu], x.[nu_manz], x.[nu_villa], x.[ds_refe_vivi], x.[ti_oper], x.[no_cont_adic], x.[tx_telf_con1], x.[tx_telf_con2], x.[ti_rela], x.[ti_jorn], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_dist_amie], x.[co_dist_mins], x.[in_corr_cedu], x.[co_pais_naci], x.[co_area_lbrl] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_info_adic',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbafil_unif  ON dbo.fctbafil_unif  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_unif', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbafil_unif;
GO
CREATE TRIGGER dbo.trg_outbox_fctbafil_unif
ON dbo.[fctbafil_unif]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'documentacionAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_reac]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_reac], x.[in_veri], x.[fe_ingr], x.[co_usua_ingr], x.[fe_proc], x.[co_usua_proc], x.[fe_elim], x.[co_usua_elim], x.[ci_cedu_ejec], x.[sc_gene], x.[ti_proc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_unif',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_reac]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_reac], x.[in_veri], x.[fe_ingr], x.[co_usua_ingr], x.[fe_proc], x.[co_usua_proc], x.[fe_elim], x.[co_usua_elim], x.[ci_cedu_ejec], x.[sc_gene], x.[ti_proc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_unif',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbagen_mail  ON dbo.fctbagen_mail  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbagen_mail', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbagen_mail;
GO
CREATE TRIGGER dbo.trg_outbox_fctbagen_mail
ON dbo.[fctbagen_mail]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'agendaMailAfiliadoType'), (N'personaDireccionesType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_regi]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_regi], x.[ds_mail], x.[in_prin], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usu_elim], x.[fe_elim], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagen_mail',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_regi]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_regi], x.[ds_mail], x.[in_prin], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usu_elim], x.[fe_elim], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagen_mail',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbagen_telf_part  ON dbo.fctbagen_telf_part  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbagen_telf_part', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbagen_telf_part;
GO
CREATE TRIGGER dbo.trg_outbox_fctbagen_telf_part
ON dbo.[fctbagen_telf_part]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'personaTelefonosType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_regi]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_regi], x.[nu_telf], x.[ti_telf], x.[co_oper], x.[in_prin], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagen_telf_part',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_regi]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_cedu], x.[sc_regi], x.[nu_telf], x.[ti_telf], x.[co_oper], x.[in_prin], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagen_telf_part',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbagru_moti_repo  ON dbo.fctbagru_moti_repo  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbagru_moti_repo', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbagru_moti_repo;
GO
CREATE TRIGGER dbo.trg_outbox_fctbagru_moti_repo
ON dbo.[fctbagru_moti_repo]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'movimientoCuentaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_agru_moti]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_agru_moti], x.[ds_agru_moti], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagru_moti_repo',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_agru_moti]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_agru_moti], x.[ds_agru_moti], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbagru_moti_repo',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbarea_lbrl  ON dbo.fctbarea_lbrl  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbarea_lbrl', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbarea_lbrl;
GO
CREATE TRIGGER dbo.trg_outbox_fctbarea_lbrl
ON dbo.[fctbarea_lbrl]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'areaLaboralParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_area_lbrl]),
            tt.t,
            @op,
            (SELECT x.[co_area_lbrl], x.[ds_area_lbrl], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbarea_lbrl',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_area_lbrl]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_area_lbrl], x.[ds_area_lbrl], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbarea_lbrl',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbaudi_actu_afil  ON dbo.fctbaudi_actu_afil  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbaudi_actu_afil', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbaudi_actu_afil;
GO
CREATE TRIGGER dbo.trg_outbox_fctbaudi_actu_afil
ON dbo.[fctbaudi_actu_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'auditoriaAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_actu]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[sc_actu], x.[ci_cedu], x.[ds_mail], x.[nu_telf_conv], x.[nu_telf_celu], x.[ds_dire], x.[ds_inst_afil], x.[ti_orig], x.[co_usua_ingr], x.[fe_ingr], x.[ho_ingr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbaudi_actu_afil',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_actu]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[sc_actu], x.[ci_cedu], x.[ds_mail], x.[nu_telf_conv], x.[nu_telf_celu], x.[ds_dire], x.[ds_inst_afil], x.[ti_orig], x.[co_usua_ingr], x.[fe_ingr], x.[ho_ingr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbaudi_actu_afil',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbaudi_movi  ON dbo.fctbaudi_movi  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbaudi_movi', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbaudi_movi;
GO
CREATE TRIGGER dbo.trg_outbox_fctbaudi_movi
ON dbo.[fctbaudi_movi]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'auditoriaAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[ci_transaccion]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[ci_transaccion], x.[ci_cedula], x.[co_usua], x.[co_tran], x.[fx_crea], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbaudi_movi',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[ci_transaccion]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_transaccion], x.[ci_cedula], x.[co_usua], x.[co_tran], x.[fx_crea], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbaudi_movi',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbcart_rpag  ON dbo.fctbcart_rpag  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbcart_rpag', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbcart_rpag;
GO
CREATE TRIGGER dbo.trg_outbox_fctbcart_rpag
ON dbo.[fctbcart_rpag]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'documentacionAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_fond]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), i.[co_proc]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_fond], x.[ci_cedu], x.[co_proc], x.[co_form_dsto], x.[co_tseg], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_ejec], x.[st_regi], x.[in_cart_afil] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcart_rpag',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_fond]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), d.[co_proc]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_fond], x.[ci_cedu], x.[co_proc], x.[co_form_dsto], x.[co_tseg], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_ejec], x.[st_regi], x.[in_cart_afil] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcart_rpag',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbcinf_part_sibs  ON dbo.fctbcinf_part_sibs  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbcinf_part_sibs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbcinf_part_sibs;
GO
CREATE TRIGGER dbo.trg_outbox_fctbcinf_part_sibs
ON dbo.[fctbcinf_part_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'reporteSIBSParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_regi]),
            tt.t,
            @op,
            (SELECT x.[sc_regi], x.[co_estr_sibs], x.[co_enti_sibs], x.[fe_cort], x.[nu_regi], x.[co_usua_gene], x.[fe_usua_gene], x.[co_usua_conf], x.[fe_usua_conf], x.[co_usua_elim], x.[fe_usua_elim], x.[st_estr_sibs] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcinf_part_sibs',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_regi]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_regi], x.[co_estr_sibs], x.[co_enti_sibs], x.[fe_cort], x.[nu_regi], x.[co_usua_gene], x.[fe_usua_gene], x.[co_usua_conf], x.[fe_usua_conf], x.[co_usua_elim], x.[fe_usua_elim], x.[st_estr_sibs] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcinf_part_sibs',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbcser_adic  ON dbo.fctbcser_adic  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbcser_adic', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbcser_adic;
GO
CREATE TRIGGER dbo.trg_outbox_fctbcser_adic
ON dbo.[fctbcser_adic]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'servicioAdicionalType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_secu]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[sc_secu], x.[ci_cedu], x.[co_serv], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_autr], x.[fe_autr], x.[co_usua_modi], x.[fe_modi], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcser_adic',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_secu]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[sc_secu], x.[ci_cedu], x.[co_serv], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_autr], x.[fe_autr], x.[co_usua_modi], x.[fe_modi], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbcser_adic',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbdinf_liqd_cnta_sibs  ON dbo.fctbdinf_liqd_cnta_sibs  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbdinf_liqd_cnta_sibs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbdinf_liqd_cnta_sibs;
GO
CREATE TRIGGER dbo.trg_outbox_fctbdinf_liqd_cnta_sibs
ON dbo.[fctbdinf_liqd_cnta_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'reporteSIBSParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[sc_regi]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedu]),
            tt.t,
            @op,
            (SELECT x.[sc_regi], x.[ci_cedu], x.[ti_iden], x.[fe_term_rela], x.[nu_impo_pers], x.[nu_impo_patr], x.[fe_liqd], x.[mo_cnta_indi], x.[mo_desc], x.[mo_tota_paga] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_liqd_cnta_sibs',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[sc_regi]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedu]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_regi], x.[ci_cedu], x.[ti_iden], x.[fe_term_rela], x.[nu_impo_pers], x.[nu_impo_patr], x.[fe_liqd], x.[mo_cnta_indi], x.[mo_desc], x.[mo_tota_paga] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_liqd_cnta_sibs',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbdinf_part_sibs  ON dbo.fctbdinf_part_sibs  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbdinf_part_sibs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbdinf_part_sibs;
GO
CREATE TRIGGER dbo.trg_outbox_fctbdinf_part_sibs
ON dbo.[fctbdinf_part_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'reporteSIBSParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_regi]),
            tt.t,
            @op,
            (SELECT x.[sc_regi], x.[ti_iden], x.[nu_iden], x.[in_gene_sibs], x.[co_esta_civi], x.[fe_naci], x.[fe_ingr_part], x.[co_esta_part_sbis], x.[co_tsis_sibs], x.[co_base_cal_sibs], x.[co_rela_lab_sibs], x.[co_esta_regi], x.[fe_actu_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_part_sibs',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_regi]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_regi], x.[ti_iden], x.[nu_iden], x.[in_gene_sibs], x.[co_esta_civi], x.[fe_naci], x.[fe_ingr_part], x.[co_esta_part_sbis], x.[co_tsis_sibs], x.[co_base_cal_sibs], x.[co_rela_lab_sibs], x.[co_esta_regi], x.[fe_actu_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_part_sibs',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbesta_civi  ON dbo.fctbesta_civi  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbesta_civi', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbesta_civi;
GO
CREATE TRIGGER dbo.trg_outbox_fctbesta_civi
ON dbo.[fctbesta_civi]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'servicioAdicionalType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_esta_civi]),
            tt.t,
            @op,
            (SELECT x.[co_esta_civi], x.[ds_esta_civi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbesta_civi',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_esta_civi]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_esta_civi], x.[ds_esta_civi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbesta_civi',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbfcha_afil  ON dbo.fctbfcha_afil  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbfcha_afil', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbfcha_afil;
GO
CREATE TRIGGER dbo.trg_outbox_fctbfcha_afil
ON dbo.[fctbfcha_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'documentacionAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_fcha_afil]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[ci_cedu], x.[no_apel_prim], x.[no_apel_secu], x.[no_nomb], x.[co_gene], x.[co_pais_naci], x.[co_esta_civi], x.[fe_naci], x.[nu_carg], x.[co_prov_resi], x.[co_cant_resi], x.[co_parr_resi], x.[in_rura_resi], x.[ds_call_prin_resi], x.[ds_call_secu_resi], x.[ds_cdla_resi], x.[nu_manz_resi], x.[nu_vill_resi], x.[ds_refe_ubic_resi], x.[co_carg_actu], x.[co_titu_prof], x.[co_catg], x.[co_tipo_cont], x.[co_tipo_doce], x.[fe_ingr_magi], x.[co_inst], x.[no_inst], x.[ds_amie], x.[co_nive], x.[co_sost], x.[co_jorn], x.[co_zona], x.[co_dist], x.[ci_tipo_inst], x.[co_prov_inst], x.[co_cant_inst], x.[co_parr_inst], x.[in_rura_inst], x.[ds_call_prin_inst], x.[ds_call_secu_inst], x.[ds_cdla_inst], x.[ds_manz_inst], x.[ds_vill_inst], x.[ds_refe_ubic_inst], x.[co_sect_regi], x.[co_vivi], x.[nu_anio_auto], x.[co_marc_auto], x.[ds_mode_auto], x.[ds_otro_bien], x.[nu_telf_inst], x.[ds_hora_cnto], x.[mo_apor], x.[in_afil], x.[fe_ingr], x.[ho_ingr], x.[fe_aprb], x.[co_usua_aprb], x.[fe_elim], x.[co_usua_elim], x.[ti_tran], x.[st_regi], x.[co_area_lbrl], x.[nu_call_prin], x.[co_ejec], x.[fe_modi], x.[co_banc], x.[nu_cnta], x.[ti_cnta], x.[mo_apor_adic], x.[sc_fcha_afil], x.[FE_VERI], x.[CO_USUA_VERI] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbfcha_afil',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_fcha_afil]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_cedu], x.[no_apel_prim], x.[no_apel_secu], x.[no_nomb], x.[co_gene], x.[co_pais_naci], x.[co_esta_civi], x.[fe_naci], x.[nu_carg], x.[co_prov_resi], x.[co_cant_resi], x.[co_parr_resi], x.[in_rura_resi], x.[ds_call_prin_resi], x.[ds_call_secu_resi], x.[ds_cdla_resi], x.[nu_manz_resi], x.[nu_vill_resi], x.[ds_refe_ubic_resi], x.[co_carg_actu], x.[co_titu_prof], x.[co_catg], x.[co_tipo_cont], x.[co_tipo_doce], x.[fe_ingr_magi], x.[co_inst], x.[no_inst], x.[ds_amie], x.[co_nive], x.[co_sost], x.[co_jorn], x.[co_zona], x.[co_dist], x.[ci_tipo_inst], x.[co_prov_inst], x.[co_cant_inst], x.[co_parr_inst], x.[in_rura_inst], x.[ds_call_prin_inst], x.[ds_call_secu_inst], x.[ds_cdla_inst], x.[ds_manz_inst], x.[ds_vill_inst], x.[ds_refe_ubic_inst], x.[co_sect_regi], x.[co_vivi], x.[nu_anio_auto], x.[co_marc_auto], x.[ds_mode_auto], x.[ds_otro_bien], x.[nu_telf_inst], x.[ds_hora_cnto], x.[mo_apor], x.[in_afil], x.[fe_ingr], x.[ho_ingr], x.[fe_aprb], x.[co_usua_aprb], x.[fe_elim], x.[co_usua_elim], x.[ti_tran], x.[st_regi], x.[co_area_lbrl], x.[nu_call_prin], x.[co_ejec], x.[fe_modi], x.[co_banc], x.[nu_cnta], x.[ti_cnta], x.[mo_apor_adic], x.[sc_fcha_afil], x.[FE_VERI], x.[CO_USUA_VERI] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbfcha_afil',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbfcha_afil_dcto  ON dbo.fctbfcha_afil_dcto  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbfcha_afil_dcto', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbfcha_afil_dcto;
GO
CREATE TRIGGER dbo.trg_outbox_fctbfcha_afil_dcto
ON dbo.[fctbfcha_afil_dcto]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'documentacionAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_fcha_afil]) + N'|' + CONVERT(NVARCHAR(100), i.[co_dcto]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[sc_fcha_afil], x.[co_dcto], x.[fe_firm_dcto], x.[fe_modi], x.[co_usua_modi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbfcha_afil_dcto',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_fcha_afil]) + N'|' + CONVERT(NVARCHAR(100), d.[co_dcto]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[sc_fcha_afil], x.[co_dcto], x.[fe_firm_dcto], x.[fe_modi], x.[co_usua_modi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbfcha_afil_dcto',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbgene_sibs  ON dbo.fctbgene_sibs  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbgene_sibs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbgene_sibs;
GO
CREATE TRIGGER dbo.trg_outbox_fctbgene_sibs
ON dbo.[fctbgene_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'servicioAdicionalType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_gene]),
            tt.t,
            @op,
            (SELECT x.[co_gene], x.[ds_gene] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbgene_sibs',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_gene]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_gene], x.[ds_gene] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbgene_sibs',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbinst_info_adic  ON dbo.fctbinst_info_adic  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbinst_info_adic', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbinst_info_adic;
GO
CREATE TRIGGER dbo.trg_outbox_fctbinst_info_adic
ON dbo.[fctbinst_info_adic]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'institucionType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[ci_tipo]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_inst]),
            tt.t,
            @op,
            (SELECT x.[ci_tipo], x.[ci_inst], x.[tx_tefl_conv_inst], x.[ci_cedu_repr], x.[no_nomb_repr], x.[tx_mail_repr], x.[tx_telf_repr], x.[ti_acce], x.[nu_doce], x.[nu_boni], x.[nu_admi], x.[nu_alum], x.[co_circ_mned], x.[co_dist_mned], x.[co_moda], x.[co_etni], x.[co_naci_inst], x.[ti_educ_mned], x.[co_zona_mned], x.[in_unid_admi], x.[co_moti_modi], x.[fe_modi], x.[co_empr], x.[co_regi_esco] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbinst_info_adic',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[ci_tipo]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_inst]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_tipo], x.[ci_inst], x.[tx_tefl_conv_inst], x.[ci_cedu_repr], x.[no_nomb_repr], x.[tx_mail_repr], x.[tx_telf_repr], x.[ti_acce], x.[nu_doce], x.[nu_boni], x.[nu_admi], x.[nu_alum], x.[co_circ_mned], x.[co_dist_mned], x.[co_moda], x.[co_etni], x.[co_naci_inst], x.[ti_educ_mned], x.[co_zona_mned], x.[in_unid_admi], x.[co_moti_modi], x.[fe_modi], x.[co_empr], x.[co_regi_esco] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbinst_info_adic',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbjuri_inst  ON dbo.fctbjuri_inst  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbjuri_inst', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbjuri_inst;
GO
CREATE TRIGGER dbo.trg_outbox_fctbjuri_inst
ON dbo.[fctbjuri_inst]
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
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_juri])),
            N'juridicoInformacionBasicaType',
            @op,
            (SELECT x.[co_empr],x.[co_juri],x.[ds_juri],x.[st_regi] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_juri]=i.[co_juri] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbjuri_inst',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_juri])),
            N'juridicoInformacionBasicaType',
            N'DELETE',
            (SELECT x.[co_empr],x.[co_juri],x.[ds_juri],x.[st_regi] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_juri]=d.[co_juri] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbjuri_inst',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_fctbotro_ingr_afil  ON dbo.fctbotro_ingr_afil  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbotro_ingr_afil', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbotro_ingr_afil;
GO
CREATE TRIGGER dbo.trg_outbox_fctbotro_ingr_afil
ON dbo.[fctbotro_ingr_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'otrosIngresosAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_rol]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), i.[co_otro_ingr_rubr]),
            tt.t,
            @op,
            (SELECT x.[co_rol], x.[ci_cedu], x.[co_otro_ingr_rubr], x.[mo_rubr], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[ds_adic], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbotro_ingr_afil',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_rol]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), d.[co_otro_ingr_rubr]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_rol], x.[ci_cedu], x.[co_otro_ingr_rubr], x.[mo_rubr], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[ds_adic], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbotro_ingr_afil',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbotro_ingr_cony  ON dbo.fctbotro_ingr_cony  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbotro_ingr_cony', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbotro_ingr_cony;
GO
CREATE TRIGGER dbo.trg_outbox_fctbotro_ingr_cony
ON dbo.[fctbotro_ingr_cony]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'otrosIngresosAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_rol]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedu_cony]) + N'|' + CONVERT(NVARCHAR(100), i.[co_otro_ingr_rubr]),
            tt.t,
            @op,
            (SELECT x.[co_rol], x.[ci_cedu], x.[ci_cedu_cony], x.[co_otro_ingr_rubr], x.[mo_rubr], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[ds_adic], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbotro_ingr_cony',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_rol]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedu]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedu_cony]) + N'|' + CONVERT(NVARCHAR(100), d.[co_otro_ingr_rubr]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_rol], x.[ci_cedu], x.[ci_cedu_cony], x.[co_otro_ingr_rubr], x.[mo_rubr], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[fe_elim], x.[co_usua_elim], x.[ds_adic], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbotro_ingr_cony',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbpara_serv_adic  ON dbo.fctbpara_serv_adic  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbpara_serv_adic', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbpara_serv_adic;
GO
CREATE TRIGGER dbo.trg_outbox_fctbpara_serv_adic
ON dbo.[fctbpara_serv_adic]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'servicioAdicionalType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_serv]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_serv], x.[mo_serv], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbpara_serv_adic',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_serv]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_serv], x.[mo_serv], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbpara_serv_adic',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbpart_foto  ON dbo.fctbpart_foto  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbpart_foto', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbpart_foto;
GO
CREATE TRIGGER dbo.trg_outbox_fctbpart_foto
ON dbo.[fctbpart_foto]
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
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ci_cedu])),
            N'imagenesType',
            @op,
            (SELECT x.[co_empr],x.[ci_cedu],x.[ds_ruta],x.[no_arch],x.[fe_ingr_foto] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[ci_cedu]=i.[ci_cedu] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbpart_foto',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ci_cedu])),
            N'imagenesType',
            N'DELETE',
            (SELECT x.[co_empr],x.[ci_cedu],x.[ds_ruta],x.[no_arch],x.[fe_ingr_foto] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[ci_cedu]=d.[ci_cedu] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbpart_foto',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_fctbrubr_rent  ON dbo.fctbrubr_rent  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbrubr_rent', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbrubr_rent;
GO
CREATE TRIGGER dbo.trg_outbox_fctbrubr_rent
ON dbo.[fctbrubr_rent]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'movimientoCuentaType'), (N'saldoDiarioType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_rubr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_rubr_rent]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_rubr], x.[co_rubr_rent], x.[st_regi], x.[co_fond], x.[co_rubr_prin] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbrubr_rent',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_rubr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_rubr_rent]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_rubr], x.[co_rubr_rent], x.[st_regi], x.[co_fond], x.[co_rubr_prin] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbrubr_rent',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbrvol_esta_afil  ON dbo.fctbrvol_esta_afil  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbrvol_esta_afil', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbrvol_esta_afil;
GO
CREATE TRIGGER dbo.trg_outbox_fctbrvol_esta_afil
ON dbo.[fctbrvol_esta_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'retiroVoluntarioEstadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_fond]) + N'|' + CONVERT(NVARCHAR(100), i.[co_tret_volu]) + N'|' + CONVERT(NVARCHAR(100), i.[nu_anio]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_deta]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_fond], x.[co_tret_volu], x.[nu_anio], x.[sc_deta], x.[co_esta_afil], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbrvol_esta_afil',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_fond]) + N'|' + CONVERT(NVARCHAR(100), d.[co_tret_volu]) + N'|' + CONVERT(NVARCHAR(100), d.[nu_anio]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_deta]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_fond], x.[co_tret_volu], x.[nu_anio], x.[sc_deta], x.[co_esta_afil], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbrvol_esta_afil',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbsald_diar_afil_rubr  ON dbo.fctbsald_diar_afil_rubr  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbsald_diar_afil_rubr', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbsald_diar_afil_rubr;
GO
CREATE TRIGGER dbo.trg_outbox_fctbsald_diar_afil_rubr
ON dbo.[fctbsald_diar_afil_rubr]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'saldoDiarioType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[fx_saldo]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedula]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_rubro_rol]) + N'|' + CONVERT(NVARCHAR(100), i.[co_empr]),
            tt.t,
            @op,
            (SELECT x.[fx_saldo], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[ci_pagador], x.[ci_cedula], x.[ce_estado], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbsald_diar_afil_rubr',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[fx_saldo]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedula]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_rubro_rol]) + N'|' + CONVERT(NVARCHAR(100), d.[co_empr]),
            tt.t,
            N'DELETE',
            (SELECT x.[fx_saldo], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[ci_pagador], x.[ci_cedula], x.[ce_estado], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbsald_diar_afil_rubr',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_fctbsald_diar_rubr  ON dbo.fctbsald_diar_rubr  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_fctbsald_diar_rubr', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbsald_diar_rubr;
GO
CREATE TRIGGER dbo.trg_outbox_fctbsald_diar_rubr
ON dbo.[fctbsald_diar_rubr]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'saldoDiarioRubroType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[fx_saldo]),
            tt.t,
            @op,
            (SELECT x.[fx_saldo], x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbsald_diar_rubr',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[fx_saldo]),
            tt.t,
            N'DELETE',
            (SELECT x.[fx_saldo], x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[va_saldo], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbsald_diar_rubr',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_afiliado  ON dbo.sfct_afiliado  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_afiliado;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado
ON dbo.[sfct_afiliado]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'movimientoCuentaType'), (N'naturalInformacionAdicionalType'), (N'naturalInformacionBasicaType'), (N'naturalTrabajoType'), (N'personaDireccionesType'), (N'personaReferenciasBancariasType'), (N'personaTelefonosType'), (N'personaType'), (N'personaVinculacionesType'), (N'servicioAdicionalType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[no_nombre], x.[no_apellido], x.[no_direccion], x.[tx_telefono], x.[ci_titulo], x.[ci_provincia], x.[ci_categoria], x.[ci_nivel], x.[ci_regimen], x.[ci_pagador], x.[tx_iess], x.[tx_contrato], x.[fx_nacimiento], x.[fx_ingreso], x.[fx_fondo], x.[fx_reingreso], x.[fx_retiro], x.[va_sueldo], x.[va_liquido], x.[va_funcional], x.[va_adicional], x.[va_antiguedad], x.[pr_cam], x.[nu_anios_antiguedad], x.[fx_creacion], x.[ce_estado], x.[ci_ciudad], x.[ce_estadocivil], x.[tx_telefono2], x.[co_escuela], x.[ti_sector], x.[ci_nivelaporte], x.[ci_cedula_numerica], x.[fx_modificacion], x.[ci_provincia_residencia], x.[in_sexo], x.[va_hipotecario], x.[ci_usuario_ingr], x.[tx_email], x.[ci_usuario_modi], x.[tx_barrio], x.[ci_parroquia], x.[ci_rold_dsto_hipo], x.[ci_rolh_dsto_hipo], x.[tx_telefono3], x.[tx_telefono4], x.[ds_observaciones], x.[ci_cargo], x.[nu_carga], x.[ci_cedula_correccion], x.[pr_funcional], x.[fe_reti_parc], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[no_nombre], x.[no_apellido], x.[no_direccion], x.[tx_telefono], x.[ci_titulo], x.[ci_provincia], x.[ci_categoria], x.[ci_nivel], x.[ci_regimen], x.[ci_pagador], x.[tx_iess], x.[tx_contrato], x.[fx_nacimiento], x.[fx_ingreso], x.[fx_fondo], x.[fx_reingreso], x.[fx_retiro], x.[va_sueldo], x.[va_liquido], x.[va_funcional], x.[va_adicional], x.[va_antiguedad], x.[pr_cam], x.[nu_anios_antiguedad], x.[fx_creacion], x.[ce_estado], x.[ci_ciudad], x.[ce_estadocivil], x.[tx_telefono2], x.[co_escuela], x.[ti_sector], x.[ci_nivelaporte], x.[ci_cedula_numerica], x.[fx_modificacion], x.[ci_provincia_residencia], x.[in_sexo], x.[va_hipotecario], x.[ci_usuario_ingr], x.[tx_email], x.[ci_usuario_modi], x.[tx_barrio], x.[ci_parroquia], x.[ci_rold_dsto_hipo], x.[ci_rolh_dsto_hipo], x.[tx_telefono3], x.[tx_telefono4], x.[ds_observaciones], x.[ci_cargo], x.[nu_carga], x.[ci_cedula_correccion], x.[pr_funcional], x.[fe_reti_parc], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_afiliado_auditor  ON dbo.sfct_afiliado_auditor  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_auditor', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_afiliado_auditor;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_auditor
ON dbo.[sfct_afiliado_auditor]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'auditoriaAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[fe_generacion], x.[ho_generacion], x.[ce_estado], x.[tx_contrato], x.[ci_nivelaporte], x.[va_hipotecario], x.[ci_categoria], x.[ci_usuario], x.[ci_motivo_mant], x.[ci_cedula_coord], x.[pr_cam], x.[ci_cargo], x.[ds_fondos], x.[pr_funcional], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_auditor',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[fe_generacion], x.[ho_generacion], x.[ce_estado], x.[tx_contrato], x.[ci_nivelaporte], x.[va_hipotecario], x.[ci_categoria], x.[ci_usuario], x.[ci_motivo_mant], x.[ci_cedula_coord], x.[pr_cam], x.[ci_cargo], x.[ds_fondos], x.[pr_funcional], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_auditor',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_afiliado_fondos  ON dbo.sfct_afiliado_fondos  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_fondos', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_afiliado_fondos;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_fondos
ON dbo.[sfct_afiliado_fondos]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'informacionAdicionalAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[co_fond], x.[ce_estado], x.[fx_ultima_impr], x.[va_historico], x.[va_traspaso], x.[fx_ingreso], x.[fx_retiro], x.[fx_reingreso], x.[in_pres], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_fondos',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[co_fond], x.[ce_estado], x.[fx_ultima_impr], x.[va_historico], x.[va_traspaso], x.[fx_ingreso], x.[fx_retiro], x.[fx_reingreso], x.[in_pres], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_fondos',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_afiliado_otros  ON dbo.sfct_afiliado_otros  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_otros', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_afiliado_otros;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_otros
ON dbo.[sfct_afiliado_otros]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'naturalIngresosEgresosType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[ci_otros], x.[va_otros], x.[ci_drol_dsto], x.[ci_hrol_dsto], x.[ce_estado], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_otros',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_otros], x.[va_otros], x.[ci_drol_dsto], x.[ci_hrol_dsto], x.[ce_estado], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_otros',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_afiliado_referencias  ON dbo.sfct_afiliado_referencias  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_referencias', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_afiliado_referencias;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_referencias
ON dbo.[sfct_afiliado_referencias]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'movimientoCuentaType'), (N'personaReferenciasBancariasType'), (N'retiroLiquidacionType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[co_tref], x.[sc_refe], x.[ds_ref1], x.[ds_ref2], x.[ds_ref3], x.[ds_ref4], x.[ds_ref5], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_cart], x.[fe_actu_cart], x.[ci_cedu_ejec], x.[co_venc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_referencias',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[co_tref], x.[sc_refe], x.[ds_ref1], x.[ds_ref2], x.[ds_ref3], x.[ds_ref4], x.[ds_ref5], x.[fe_ingr], x.[co_usua_ingr], x.[fe_modi], x.[co_usua_modi], x.[st_cart], x.[fe_actu_cart], x.[ci_cedu_ejec], x.[co_venc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_referencias',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_afiliado_rubro  ON dbo.sfct_afiliado_rubro  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_rubro', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_afiliado_rubro;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_rubro
ON dbo.[sfct_afiliado_rubro]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'naturalIngresosEgresosType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[ci_rubro_rol], x.[fx_prim_apor], x.[fx_ultm_apor], x.[rol_prim_apor], x.[rol_ultm_apor], x.[nu_aportaciones], x.[va_ultm_apor], x.[va_nc], x.[va_nd], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_rubro',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_rubro_rol], x.[fx_prim_apor], x.[fx_ultm_apor], x.[rol_prim_apor], x.[rol_ultm_apor], x.[nu_aportaciones], x.[va_ultm_apor], x.[va_nc], x.[va_nd], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_rubro',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_banco  ON dbo.sfct_banco  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_banco', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_banco;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_banco
ON dbo.[sfct_banco]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'movimientoCuentaType'), (N'naturalInformacionAdicionalType'), (N'personaDireccionesType'), (N'personaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_banco]),
            tt.t,
            @op,
            (SELECT x.[ci_banco], x.[no_banco], x.[in_reca], x.[fx_creacion], x.[ce_estado], x.[nu_ruc], x.[no_cont], x.[ds_dire], x.[nu_tele], x.[co_spi], x.[ti_cnta_spi], x.[in_firm_conv], x.[co_bnco_asoc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_banco',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_banco]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_banco], x.[no_banco], x.[in_reca], x.[fx_creacion], x.[ce_estado], x.[nu_ruc], x.[no_cont], x.[ds_dire], x.[nu_tele], x.[co_spi], x.[ti_cnta_spi], x.[in_firm_conv], x.[co_bnco_asoc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_banco',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_beneficiario  ON dbo.sfct_beneficiario  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_beneficiario', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_beneficiario;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_beneficiario
ON dbo.[sfct_beneficiario]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'beneficiarioParticipeType'), (N'naturalInformacionAdicionalType'), (N'naturalInformacionBasicaType'), (N'personaType'), (N'personaVinculacionesType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedula]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_bene]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[ci_cedula_beneficiario], x.[no_nombre], x.[no_apellido], x.[pr_porcentaje], x.[fx_creacion], x.[ce_beneficiario], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[sc_bene], x.[co_bnco_pago], x.[ti_cnta_pago], x.[nu_cnta_pago], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_beneficiario',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedula]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_bene]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_cedula_beneficiario], x.[no_nombre], x.[no_apellido], x.[pr_porcentaje], x.[fx_creacion], x.[ce_beneficiario], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[sc_bene], x.[co_bnco_pago], x.[ti_cnta_pago], x.[nu_cnta_pago], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_beneficiario',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_beneficiario_retiro  ON dbo.sfct_beneficiario_retiro  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_beneficiario_retiro', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_beneficiario_retiro;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_beneficiario_retiro
ON dbo.[sfct_beneficiario_retiro]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'beneficiarioParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_retiro]) + N'|' + CONVERT(NVARCHAR(100), i.[sc_dbso]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[ci_retiro], x.[sc_dbso], x.[ci_cedula_beneficiario], x.[no_beneficiario], x.[pr_porcentaje], x.[va_desembolso], x.[ti_desembolso], x.[ci_banco], x.[nu_cuenta], x.[ti_cuenta], x.[fe_dbso], x.[co_bnco_dbso], x.[st_regi], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_beneficiario_retiro',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_retiro]) + N'|' + CONVERT(NVARCHAR(100), d.[sc_dbso]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_retiro], x.[sc_dbso], x.[ci_cedula_beneficiario], x.[no_beneficiario], x.[pr_porcentaje], x.[va_desembolso], x.[ti_desembolso], x.[ci_banco], x.[nu_cuenta], x.[ti_cuenta], x.[fe_dbso], x.[co_bnco_dbso], x.[st_regi], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_beneficiario_retiro',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_cabecera_rol  ON dbo.sfct_cabecera_rol  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_cabecera_rol', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_cabecera_rol;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_cabecera_rol
ON dbo.[sfct_cabecera_rol]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'rolNominaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_rol]),
            tt.t,
            @op,
            (SELECT x.[ci_rol], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[fx_creacion], x.[sc_rol], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_cabecera_rol',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_rol]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_rol], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[fx_creacion], x.[sc_rol], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_cabecera_rol',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_ciudad  ON dbo.sfct_ciudad  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_ciudad', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_ciudad;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_ciudad
ON dbo.[sfct_ciudad]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'naturalInformacionAdicionalType'), (N'naturalInformacionBasicaType'), (N'personaDireccionesType'), (N'personaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_ciudad]),
            tt.t,
            @op,
            (SELECT x.[ci_ciudad], x.[no_nombre], x.[ci_provincia], x.[fx_creacion], x.[ce_estado], x.[co_cant_sine], x.[co_banc], x.[in_cred_cnta], x.[co_zona], x.[co_dist] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_ciudad',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_ciudad]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_ciudad], x.[no_nombre], x.[ci_provincia], x.[fx_creacion], x.[ce_estado], x.[co_cant_sine], x.[co_banc], x.[in_cred_cnta], x.[co_zona], x.[co_dist] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_ciudad',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_conyuge  ON dbo.sfct_conyuge  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_conyuge', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_conyuge;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_conyuge
ON dbo.[sfct_conyuge]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'personaVinculacionesType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[ci_cedula_conyuge], x.[no_nombre_conyuge], x.[no_apellido_conyuge], x.[tx_direccion_conyuge], x.[ci_ciudad], x.[ci_provincia], x.[co_pais] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_conyuge',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_cedula_conyuge], x.[no_nombre_conyuge], x.[no_apellido_conyuge], x.[tx_direccion_conyuge], x.[ci_ciudad], x.[ci_provincia], x.[co_pais] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_conyuge',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_detalle_rol  ON dbo.sfct_detalle_rol  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_detalle_rol', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_detalle_rol;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_detalle_rol
ON dbo.[sfct_detalle_rol]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'rolNominaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_rol]),
            tt.t,
            @op,
            (SELECT x.[sc_rol], x.[ci_cedula], x.[ci_rubro_rol], x.[va_generado], x.[ce_cobrar], x.[ci_categoria], x.[in_cble] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_detalle_rol',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_rol]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_rol], x.[ci_cedula], x.[ci_rubro_rol], x.[va_generado], x.[ce_cobrar], x.[ci_categoria], x.[in_cble] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_detalle_rol',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_firmante  ON dbo.sfct_firmante  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_firmante', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_firmante;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_firmante
ON dbo.[sfct_firmante]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'firmanteParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_tipo]),
            tt.t,
            @op,
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[sc_firmante], x.[ci_cedula], x.[no_firmante], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_firmante',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_tipo]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[sc_firmante], x.[ci_cedula], x.[no_firmante], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_firmante',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_grupo_fami  ON dbo.sfct_grupo_fami  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_grupo_fami', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_grupo_fami;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_grupo_fami
ON dbo.[sfct_grupo_fami]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'grupoFamiliarType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[ci_cedula_familiar], x.[no_nombre], x.[no_apellido], x.[fx_nacimiento], x.[ti_relacion], x.[fx_creacion], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[ce_familiar], x.[in_discapacidad] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_grupo_fami',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[ci_cedula_familiar], x.[no_nombre], x.[no_apellido], x.[fx_nacimiento], x.[ti_relacion], x.[fx_creacion], x.[fx_modificacion], x.[ci_usuario_ingr], x.[ci_usuario_modi], x.[ce_familiar], x.[in_discapacidad] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_grupo_fami',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_institucion  ON dbo.sfct_institucion  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_institucion', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_institucion;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_institucion
ON dbo.[sfct_institucion]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'institucionType'), (N'naturalInformacionAdicionalType'), (N'personaDireccionesType'), (N'personaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[ci_tipo]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_institucion]),
            tt.t,
            @op,
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[no_institucion], x.[ci_provincia], x.[ci_ciudad], x.[ci_parroquia], x.[fx_creacion], x.[no_direccion], x.[nu_telefono], x.[nu_telefono2], x.[no_colector], x.[no_direccion_colector], x.[ci_provincia_colector], x.[ci_ciudad_colector], x.[ci_parroquia_colector], x.[nu_telefono_colector], x.[nu_telefono_colector2], x.[in_entrega], x.[ce_estado], x.[in_jornada], x.[in_recepcion], x.[in_municipales], x.[in_impresio_esta_cnta], x.[ci_patronal], x.[co_plan_sine], x.[no_rector], x.[co_usua_modi], x.[ci_cedula_colec], x.[ds_email], x.[pr_cam], x.[ti_direccion], x.[nu_cuenta_bc], x.[in_contrato_CAM], x.[nu_ruc], x.[ti_nivel], x.[ds_email_inst], x.[ti_sostenimiento], x.[nu_ute], x.[nu_zona], x.[co_usua_ingr], x.[fx_modificacion], x.[in_contrato_BCE], x.[fe_firma_BCE], x.[in_confirmacion_BCE], x.[ci_rol_ultm_actu], x.[fe_ultm_actu], x.[ho_ultm_actu], x.[co_pres_mefz], x.[nu_cnta_inst], x.[ti_cnta_inst], x.[co_bnco_inst], x.[in_dsto_bce], x.[co_amie], x.[co_sect], x.[co_dist], x.[co_circ], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_institucion',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[ci_tipo]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_institucion]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_tipo], x.[ci_institucion], x.[no_institucion], x.[ci_provincia], x.[ci_ciudad], x.[ci_parroquia], x.[fx_creacion], x.[no_direccion], x.[nu_telefono], x.[nu_telefono2], x.[no_colector], x.[no_direccion_colector], x.[ci_provincia_colector], x.[ci_ciudad_colector], x.[ci_parroquia_colector], x.[nu_telefono_colector], x.[nu_telefono_colector2], x.[in_entrega], x.[ce_estado], x.[in_jornada], x.[in_recepcion], x.[in_municipales], x.[in_impresio_esta_cnta], x.[ci_patronal], x.[co_plan_sine], x.[no_rector], x.[co_usua_modi], x.[ci_cedula_colec], x.[ds_email], x.[pr_cam], x.[ti_direccion], x.[nu_cuenta_bc], x.[in_contrato_CAM], x.[nu_ruc], x.[ti_nivel], x.[ds_email_inst], x.[ti_sostenimiento], x.[nu_ute], x.[nu_zona], x.[co_usua_ingr], x.[fx_modificacion], x.[in_contrato_BCE], x.[fe_firma_BCE], x.[in_confirmacion_BCE], x.[ci_rol_ultm_actu], x.[fe_ultm_actu], x.[ho_ultm_actu], x.[co_pres_mefz], x.[nu_cnta_inst], x.[ti_cnta_inst], x.[co_bnco_inst], x.[in_dsto_bce], x.[co_amie], x.[co_sect], x.[co_dist], x.[co_circ], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_institucion',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_motivo  ON dbo.sfct_motivo  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_motivo', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_motivo;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_motivo
ON dbo.[sfct_motivo]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'movimientoCuentaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_tipo_transaccion]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_motivo]),
            tt.t,
            @op,
            (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[no_motivo], x.[fx_creacion], x.[ce_estado], x.[in_usua_auto], x.[in_mvto_manu], x.[ci_motivo_contr], x.[in_moti_contr], x.[in_moti_ccup], x.[co_moti_ccup], x.[co_empr], x.[co_agru_moti] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_tipo_transaccion]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_motivo]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[no_motivo], x.[fx_creacion], x.[ce_estado], x.[in_usua_auto], x.[in_mvto_manu], x.[ci_motivo_contr], x.[in_moti_contr], x.[in_moti_ccup], x.[co_moti_ccup], x.[co_empr], x.[co_agru_moti] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_motivo_cnta_cble  ON dbo.sfct_motivo_cnta_cble  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_motivo_cnta_cble', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_motivo_cnta_cble;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_motivo_cnta_cble
ON dbo.[sfct_motivo_cnta_cble]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'motivoContableType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_tipo_transaccion]),
            tt.t,
            @op,
            (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[co_cnta_auto_debe], x.[co_cnta_auto_habe], x.[co_empr], x.[co_fond] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo_cnta_cble',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_tipo_transaccion]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_tipo_transaccion], x.[ci_motivo], x.[ci_rubro_rol], x.[co_cnta_auto_debe], x.[co_cnta_auto_habe], x.[co_empr], x.[co_fond] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo_cnta_cble',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_motivo_mant_afiliados  ON dbo.sfct_motivo_mant_afiliados  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_motivo_mant_afiliados', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_motivo_mant_afiliados;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_motivo_mant_afiliados
ON dbo.[sfct_motivo_mant_afiliados]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'auditoriaAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_motivo_mant]),
            tt.t,
            @op,
            (SELECT x.[ci_motivo_mant], x.[no_motivo_mant], x.[st_motivo_mant] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo_mant_afiliados',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_motivo_mant]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_motivo_mant], x.[no_motivo_mant], x.[st_motivo_mant] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_motivo_mant_afiliados',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_movimiento  ON dbo.sfct_movimiento  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_movimiento', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_movimiento;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_movimiento
ON dbo.[sfct_movimiento]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'movimientoCuentaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_tipo_transaccion]),
            tt.t,
            @op,
            (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_movimiento',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_tipo_transaccion]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_movimiento',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_movimiento_temp  ON dbo.sfct_movimiento_temp  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_movimiento_temp', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_movimiento_temp;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_movimiento_temp
ON dbo.[sfct_movimiento_temp]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'movimientoTemporalType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_tipo_transaccion]),
            tt.t,
            @op,
            (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_usua_ingr], x.[co_usua_conf], x.[st_mvto], x.[ci_tran_autr], x.[ds_movi], x.[in_proc], x.[fx_autr], x.[co_empr], x.[co_usua_veri], x.[fe_veri], x.[sc_carg_mvto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_movimiento_temp',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_tipo_transaccion]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_tipo_transaccion], x.[ci_transaccion], x.[ci_motivo], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_rol], x.[ci_rubro_rol], x.[ci_motivo_retiro], x.[fx_ajuste], x.[fx_final_ajuste], x.[fx_proceso], x.[fx_retiro], x.[pr_porcentaje], x.[va_transaccion], x.[va_saldo_anterior], x.[fx_creacion], x.[ce_capitalizado], x.[ce_estado], x.[qs_hora], x.[ti_comprobante], x.[nu_comprobante], x.[ci_provincia], x.[ci_pagador], x.[in_impresion], x.[sc_rol], x.[co_usua_ingr], x.[co_usua_conf], x.[st_mvto], x.[ci_tran_autr], x.[ds_movi], x.[in_proc], x.[fx_autr], x.[co_empr], x.[co_usua_veri], x.[fe_veri], x.[sc_carg_mvto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_movimiento_temp',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_padbs  ON dbo.sfct_padbs  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_padbs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_padbs;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_padbs
ON dbo.[sfct_padbs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cuentaBancariaAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[qs_liqd_hipo]) + N'|' + CONVERT(NVARCHAR(100), i.[qs_pago_dbso]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[qs_liqd_hipo], x.[qs_pago_dbso], x.[ti_pago], x.[mo_mvto], x.[nu_cta], x.[no_bcos], x.[co_bene], x.[ci_bnco], x.[ti_cnta], x.[ci_bnco_acre], x.[st_regi], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_padbs',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[qs_liqd_hipo]) + N'|' + CONVERT(NVARCHAR(100), d.[qs_pago_dbso]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[qs_liqd_hipo], x.[qs_pago_dbso], x.[ti_pago], x.[mo_mvto], x.[nu_cta], x.[no_bcos], x.[co_bene], x.[ci_bnco], x.[ti_cnta], x.[ci_bnco_acre], x.[st_regi], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_padbs',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_referencias  ON dbo.sfct_referencias  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_referencias', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_referencias;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_referencias
ON dbo.[sfct_referencias]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'referenciaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_tref]),
            tt.t,
            @op,
            (SELECT x.[co_tref], x.[ds_tref] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_referencias',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_tref]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_tref], x.[ds_tref] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_referencias',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_retiro  ON dbo.sfct_retiro  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_retiro', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_retiro;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_retiro
ON dbo.[sfct_retiro]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'retiroLiquidacionType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_retiro]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[ci_retiro], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_provincia], x.[ci_pagador], x.[va_aporte], x.[va_fas], x.[va_acciones], x.[va_saldo_inicial], x.[fx_retiro], x.[ci_motivo_retiro], x.[fx_ingreso], x.[st_reti], x.[fx_cncd], x.[va_credito], x.[va_interes_ci], x.[va_interes_acci], x.[fx_proceso], x.[ci_rol], x.[ci_motivo], x.[fe_conf], x.[fe_autr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_ingr], x.[co_usua_elim], x.[fe_elim], x.[co_usua_autr_prov], x.[fe_autr_prov], x.[ce_estado_anterior], x.[co_fond], x.[va_rese_fas], x.[va_adic], x.[va_rete], x.[va_ccrd], x.[va_sobr], x.[va_gast], x.[va_cred_fond], x.[va_pago_fond], x.[va_cred_ccrd], x.[mo_cup], x.[mo_apor_cup], x.[mo_inve_hidr], x.[mo_capi_cdp], x.[mo_rent_cdp], x.[mo_cred_grte], x.[mo_gara_cup], x.[co_tasa_cup], x.[co_plaz_cup], x.[co_tipo_capi], x.[mo_rent_cup], x.[ci_cedu_hidr], x.[ci_cedu_cup], x.[co_orig], x.[in_cbro_pres], x.[co_proc], x.[in_proc_msvo] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_retiro',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_retiro]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[ci_retiro], x.[ci_tipo], x.[ci_institucion], x.[ci_cedula], x.[ci_provincia], x.[ci_pagador], x.[va_aporte], x.[va_fas], x.[va_acciones], x.[va_saldo_inicial], x.[fx_retiro], x.[ci_motivo_retiro], x.[fx_ingreso], x.[st_reti], x.[fx_cncd], x.[va_credito], x.[va_interes_ci], x.[va_interes_acci], x.[fx_proceso], x.[ci_rol], x.[ci_motivo], x.[fe_conf], x.[fe_autr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_ingr], x.[co_usua_elim], x.[fe_elim], x.[co_usua_autr_prov], x.[fe_autr_prov], x.[ce_estado_anterior], x.[co_fond], x.[va_rese_fas], x.[va_adic], x.[va_rete], x.[va_ccrd], x.[va_sobr], x.[va_gast], x.[va_cred_fond], x.[va_pago_fond], x.[va_cred_ccrd], x.[mo_cup], x.[mo_apor_cup], x.[mo_inve_hidr], x.[mo_capi_cdp], x.[mo_rent_cdp], x.[mo_cred_grte], x.[mo_gara_cup], x.[co_tasa_cup], x.[co_plaz_cup], x.[co_tipo_capi], x.[mo_rent_cup], x.[ci_cedu_hidr], x.[ci_cedu_cup], x.[co_orig], x.[in_cbro_pres], x.[co_proc], x.[in_proc_msvo] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_retiro',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_rubro_rol  ON dbo.sfct_rubro_rol  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_rubro_rol', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_rubro_rol;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_rubro_rol
ON dbo.[sfct_rubro_rol]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'rolNominaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_rubro_rol]),
            tt.t,
            @op,
            (SELECT x.[ci_rubro_rol], x.[no_rubro_rol], x.[no_rubro_abreviado], x.[nu_prio], x.[ci_rol_inicial], x.[nu_debitos], x.[co_cnta_auto_mvto], x.[co_cnta_auto_tran], x.[tx_total], x.[qs_impresion], x.[ci_rubro_acum], x.[ti_reca], x.[in_mvto], x.[in_intr], x.[in_unif], x.[co_empr], x.[ti_dato_dsto], x.[ds_esta_afil] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_rubro_rol',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_rubro_rol]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_rubro_rol], x.[no_rubro_rol], x.[no_rubro_abreviado], x.[nu_prio], x.[ci_rol_inicial], x.[nu_debitos], x.[co_cnta_auto_mvto], x.[co_cnta_auto_tran], x.[tx_total], x.[qs_impresion], x.[ci_rubro_acum], x.[ti_reca], x.[in_mvto], x.[in_intr], x.[in_unif], x.[co_empr], x.[ti_dato_dsto], x.[ds_esta_afil] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_rubro_rol',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_sfct_saldos_diarios_afiliados  ON dbo.sfct_saldos_diarios_afiliados  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sfct_saldos_diarios_afiliados', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_saldos_diarios_afiliados;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_saldos_diarios_afiliados
ON dbo.[sfct_saldos_diarios_afiliados]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'saldoDiarioType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[fx_saldo]) + N'|' + CONVERT(NVARCHAR(100), i.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), i.[co_fond]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedula]),
            tt.t,
            @op,
            (SELECT x.[fx_saldo], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[ci_pagador], x.[ci_cedula], x.[ce_estado], x.[co_fond], x.[va_saldo], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_saldos_diarios_afiliados',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[fx_saldo]) + N'|' + CONVERT(NVARCHAR(100), d.[co_empr]) + N'|' + CONVERT(NVARCHAR(100), d.[co_fond]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedula]),
            tt.t,
            N'DELETE',
            (SELECT x.[fx_saldo], x.[ci_tipo], x.[ci_institucion], x.[ci_provincia], x.[ci_pagador], x.[ci_cedula], x.[ce_estado], x.[co_fond], x.[va_saldo], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_saldos_diarios_afiliados',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* ----- BD: dbCR  (2 triggers) ----- */
USE [dbCR];
GO

/* --- trg_outbox_crtboper_cony  ON dbo.crtboper_cony  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_crtboper_cony', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtboper_cony;
GO
CREATE TRIGGER dbo.trg_outbox_crtboper_cony
ON dbo.[crtboper_cony]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'personaVinculacionesType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[ti_cred]) + N'|' + CONVERT(NVARCHAR(100), i.[aa_cred]) + N'|' + CONVERT(NVARCHAR(100), i.[qs_cred]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_cedu_cony]),
            tt.t,
            @op,
            (SELECT x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[co_tipo_deud], x.[ci_cedu_cony] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtboper_cony',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[ti_cred]) + N'|' + CONVERT(NVARCHAR(100), d.[aa_cred]) + N'|' + CONVERT(NVARCHAR(100), d.[qs_cred]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_cedu_cony]),
            tt.t,
            N'DELETE',
            (SELECT x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[co_tipo_deud], x.[ci_cedu_cony] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtboper_cony',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_crtoblig  ON dbo.crtoblig  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_crtoblig', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtoblig;
GO
CREATE TRIGGER dbo.trg_outbox_crtoblig
ON dbo.[crtoblig]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'personaVinculacionesType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[ti_cred]) + N'|' + CONVERT(NVARCHAR(100), i.[aa_cred]) + N'|' + CONVERT(NVARCHAR(100), i.[qs_cred]),
            tt.t,
            @op,
            (SELECT x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[ce_cred], x.[ci_clie], x.[fx_ppta], x.[fx_autr], x.[fx_cncd], x.[fx_vcto], x.[fx_canc], x.[fx_inst], x.[ci_mnda], x.[mo_cred], x.[mo_intr], x.[mo_abno_capi], x.[mo_abno_intr], x.[pr_cobr_impt], x.[fr_cobr_capi], x.[fr_cobr_intr], x.[ci_pais], x.[ci_prov], x.[pr_intr], x.[du_anos], x.[du_dias], x.[nu_dcto], x.[in_anci], x.[nu_peri_grac], x.[ti_peri_grac], x.[ti_cuot], x.[pr_marg_intr], x.[ti_marg_reaj], x.[fr_reaj], x.[ti_tasa_reaj], x.[ti_oprc], x.[in_gara], x.[in_grte], x.[ti_tasa_intr], x.[in_base_calc], x.[in_cred_dudo], x.[fx_dudo], x.[fx_ultm_actu], x.[ci_usua_actu], x.[qs_abno], x.[fx_dolr], x.[ci_mnda_antr], x.[pr_tasa_inic], x.[ci_ejec], x.[co_prog], x.[sc_casa], x.[co_afil_refe], x.[co_usua_ingr], x.[co_usua_inst], x.[nu_oper_cc], x.[ti_oper_cc], x.[nu_oper_ante], x.[pr_comi], x.[in_soli], x.[co_fond], x.[fe_inic_dvgo], x.[sc_cupo], x.[co_zona], x.[co_loca], x.[nu_rol_hist], x.[in_cobr_segu], x.[mo_pagd_segu], x.[fx_ultm_dvgo], x.[st_calf], x.[fe_ultm_calf], x.[co_empr], x.[co_tamo], x.[ti_diar], x.[nu_cpbt_cble], x.[in_cble_anul], x.[in_gara_cnta], x.[pr_gara_cnta], x.[mo_gara_cind], x.[co_tipo_segu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtoblig',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[ti_cred]) + N'|' + CONVERT(NVARCHAR(100), d.[aa_cred]) + N'|' + CONVERT(NVARCHAR(100), d.[qs_cred]),
            tt.t,
            N'DELETE',
            (SELECT x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[ce_cred], x.[ci_clie], x.[fx_ppta], x.[fx_autr], x.[fx_cncd], x.[fx_vcto], x.[fx_canc], x.[fx_inst], x.[ci_mnda], x.[mo_cred], x.[mo_intr], x.[mo_abno_capi], x.[mo_abno_intr], x.[pr_cobr_impt], x.[fr_cobr_capi], x.[fr_cobr_intr], x.[ci_pais], x.[ci_prov], x.[pr_intr], x.[du_anos], x.[du_dias], x.[nu_dcto], x.[in_anci], x.[nu_peri_grac], x.[ti_peri_grac], x.[ti_cuot], x.[pr_marg_intr], x.[ti_marg_reaj], x.[fr_reaj], x.[ti_tasa_reaj], x.[ti_oprc], x.[in_gara], x.[in_grte], x.[ti_tasa_intr], x.[in_base_calc], x.[in_cred_dudo], x.[fx_dudo], x.[fx_ultm_actu], x.[ci_usua_actu], x.[qs_abno], x.[fx_dolr], x.[ci_mnda_antr], x.[pr_tasa_inic], x.[ci_ejec], x.[co_prog], x.[sc_casa], x.[co_afil_refe], x.[co_usua_ingr], x.[co_usua_inst], x.[nu_oper_cc], x.[ti_oper_cc], x.[nu_oper_ante], x.[pr_comi], x.[in_soli], x.[co_fond], x.[fe_inic_dvgo], x.[sc_cupo], x.[co_zona], x.[co_loca], x.[nu_rol_hist], x.[in_cobr_segu], x.[mo_pagd_segu], x.[fx_ultm_dvgo], x.[st_calf], x.[fe_ultm_calf], x.[co_empr], x.[co_tamo], x.[ti_diar], x.[nu_cpbt_cble], x.[in_cble_anul], x.[in_gara_cnta], x.[pr_gara_cnta], x.[mo_gara_cind], x.[co_tipo_segu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtoblig',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* ----- BD: dbCG  (1 triggers) ----- */
USE [dbCG];
GO

/* --- trg_outbox_cgtbprvd  ON dbo.cgtbprvd  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_cgtbprvd', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cgtbprvd;
GO
CREATE TRIGGER dbo.trg_outbox_cgtbprvd
ON dbo.[cgtbprvd]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'personaDireccionesType'), (N'personaReferenciasBancariasType'), (N'personaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_empr]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_prvd], x.[no_prvd], x.[ds_dire], x.[nu_tel1], x.[nu_tel2], x.[di_emai], x.[ti_iden], x.[nu_iden], x.[ti_pers], x.[st_prvd], x.[in_dpro], x.[ci_bnco], x.[ti_cnta], x.[nu_cnta], x.[co_loca], x.[co_dpto], x.[co_iden], x.[fe_ingr], x.[fe_modi], x.[fe_elim], x.[co_usua_ingr], x.[co_usua_modi], x.[co_usua_elim], x.[ti_prvd], x.[no_gere], x.[in_cont], x.[in_segu], x.[in_segu_cred], x.[in_conv_fcme], x.[nu_edad], x.[in_pago_bene], x.[co_iden_sri], x.[no_bene], x.[co_iden_bene], x.[ci_bnco_bene], x.[ti_cnta_bene], x.[nu_cnta_bene] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbprvd',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_empr]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_prvd], x.[no_prvd], x.[ds_dire], x.[nu_tel1], x.[nu_tel2], x.[di_emai], x.[ti_iden], x.[nu_iden], x.[ti_pers], x.[st_prvd], x.[in_dpro], x.[ci_bnco], x.[ti_cnta], x.[nu_cnta], x.[co_loca], x.[co_dpto], x.[co_iden], x.[fe_ingr], x.[fe_modi], x.[fe_elim], x.[co_usua_ingr], x.[co_usua_modi], x.[co_usua_elim], x.[ti_prvd], x.[no_gere], x.[in_cont], x.[in_segu], x.[in_segu_cred], x.[in_conv_fcme], x.[nu_edad], x.[in_pago_bene], x.[co_iden_sri], x.[no_bene], x.[co_iden_bene], x.[ci_bnco_bene], x.[ti_cnta_bene], x.[nu_cnta_bene] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbprvd',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* ----- BD: dbCT  (4 triggers) ----- */
USE [dbCT];
GO

/* --- trg_outbox_cttbafil_audi  ON dbo.cttbafil_audi  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_cttbafil_audi', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cttbafil_audi;
GO
CREATE TRIGGER dbo.trg_outbox_cttbafil_audi
ON dbo.[cttbafil_audi]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'auditoriaAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), i.[ci_cedula]) + N'|' + CONVERT(NVARCHAR(100), i.[fe_crea]) + N'|' + CONVERT(NVARCHAR(100), i.[ho_crea]) + N'|' + CONVERT(NVARCHAR(100), i.[ci_camp]),
            tt.t,
            @op,
            (SELECT x.[ci_cedula], x.[fe_crea], x.[ho_crea], x.[co_usua], x.[ci_camp], x.[ds_audi], x.[ci_moti] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbafil_audi',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(100), d.[ci_cedula]) + N'|' + CONVERT(NVARCHAR(100), d.[fe_crea]) + N'|' + CONVERT(NVARCHAR(100), d.[ho_crea]) + N'|' + CONVERT(NVARCHAR(100), d.[ci_camp]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_cedula], x.[fe_crea], x.[ho_crea], x.[co_usua], x.[ci_camp], x.[ds_audi], x.[ci_moti] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbafil_audi',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_cttbcomi_cred  ON dbo.cttbcomi_cred  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_cttbcomi_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cttbcomi_cred;
GO
CREATE TRIGGER dbo.trg_outbox_cttbcomi_cred
ON dbo.[cttbcomi_cred]
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
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ci_ejec])),
            N'comisionParticipe_type',
            @op,
            (SELECT x.[ti_cred],x.[aa_cred],x.[qs_cred],x.[ci_ejec],x.[st_comi] FROM inserted x WHERE x.[ti_cred]=i.[ti_cred] AND x.[aa_cred]=i.[aa_cred] AND x.[qs_cred]=i.[qs_cred] AND x.[ci_ejec]=i.[ci_ejec] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbcomi_cred',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ci_ejec])),
            N'comisionParticipe_type',
            N'DELETE',
            (SELECT x.[ti_cred],x.[aa_cred],x.[qs_cred],x.[ci_ejec],x.[st_comi] FROM deleted x WHERE x.[ti_cred]=d.[ti_cred] AND x.[aa_cred]=d.[aa_cred] AND x.[qs_cred]=d.[qs_cred] AND x.[ci_ejec]=d.[ci_ejec] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbcomi_cred',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_cttbmatr_dist_afil  ON dbo.cttbmatr_dist_afil  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_cttbmatr_dist_afil', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cttbmatr_dist_afil;
GO
CREATE TRIGGER dbo.trg_outbox_cttbmatr_dist_afil
ON dbo.[cttbmatr_dist_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'distribucionAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[Zona]),
            tt.t,
            @op,
            (SELECT x.[Zona], x.[Provincia], x.[Distrito], x.[Ciudad], x.[Circuito], x.[Parroquia], x.[Estado], x.[trabajo], x.[NumeroAfiliado], x.[NumeroAfiliadoActualizado], x.[NumeroCAP], x.[NumeroEjecutivoFinanciero], x.[NumeroCreditoVigente], x.[NumeroCADB], x.[NumeroInstituciones], x.[NumeroPresidenteProvinciales2008], x.[NumeroPresidenteProvinciales2010], x.[NumeroPresidenteEjecutivo2008], x.[NumeroPresidenteEjecutivo2010], x.[NumeroDirectivoNacional2008], x.[NumeroDirectivoNacional2010], x.[NumeroDirectivoProvincial2008], x.[NumeroDirectivoProvincial2010], x.[NumeroDelegadoConvencion2008], x.[NumeroDelegadoConvencion2010], x.[mo_cred_vige], x.[mo_cuen_unic], x.[nu_lide_opin], x.[nu_solo_cam] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbmatr_dist_afil',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[Zona]),
            tt.t,
            N'DELETE',
            (SELECT x.[Zona], x.[Provincia], x.[Distrito], x.[Ciudad], x.[Circuito], x.[Parroquia], x.[Estado], x.[trabajo], x.[NumeroAfiliado], x.[NumeroAfiliadoActualizado], x.[NumeroCAP], x.[NumeroEjecutivoFinanciero], x.[NumeroCreditoVigente], x.[NumeroCADB], x.[NumeroInstituciones], x.[NumeroPresidenteProvinciales2008], x.[NumeroPresidenteProvinciales2010], x.[NumeroPresidenteEjecutivo2008], x.[NumeroPresidenteEjecutivo2010], x.[NumeroDirectivoNacional2008], x.[NumeroDirectivoNacional2010], x.[NumeroDirectivoProvincial2008], x.[NumeroDirectivoProvincial2010], x.[NumeroDelegadoConvencion2008], x.[NumeroDelegadoConvencion2010], x.[mo_cred_vige], x.[mo_cuen_unic], x.[nu_lide_opin], x.[nu_solo_cam] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbmatr_dist_afil',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_cttbtabl_afil  ON dbo.cttbtabl_afil  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_cttbtabl_afil', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cttbtabl_afil;
GO
CREATE TRIGGER dbo.trg_outbox_cttbtabl_afil
ON dbo.[cttbtabl_afil]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'auditoriaAfiliadoType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_camp]),
            tt.t,
            @op,
            (SELECT x.[ci_camp], x.[no_camp], x.[ds_camp] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbtabl_afil',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_camp]),
            tt.t,
            N'DELETE',
            (SELECT x.[ci_camp], x.[no_camp], x.[ds_camp] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbtabl_afil',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* ----- BD: dbNO  (2 triggers) ----- */
USE [dbNO];
GO

/* --- trg_outbox_notbcgfm  ON dbo.notbcgfm  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbcgfm', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbcgfm;
GO
CREATE TRIGGER dbo.trg_outbox_notbcgfm
ON dbo.[notbcgfm]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'naturalInformacionAdicionalType'), (N'naturalInformacionBasicaType'), (N'personaType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_empr]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_empl], x.[sc_cgfm], x.[ti_rela], x.[no_nomb], x.[fe_naci], x.[ti_iden], x.[nu_iden], x.[in_minu], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[st_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcgfm',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_empr]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_empl], x.[sc_cgfm], x.[ti_rela], x.[no_nomb], x.[fe_naci], x.[ti_iden], x.[nu_iden], x.[in_minu], x.[co_usua_ingr], x.[fe_ingr], x.[co_usua_modi], x.[fe_modi], x.[co_usua_elim], x.[fe_elim], x.[st_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcgfm',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_notbempl  ON dbo.notbempl  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbempl', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbempl;
GO
CREATE TRIGGER dbo.trg_outbox_notbempl
ON dbo.[notbempl]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'naturalInformacionAdicionalType'), (N'naturalInformacionBasicaType'), (N'personaDireccionesType'), (N'personaType'), (N'personaVinculacionesType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_empr]),
            tt.t,
            @op,
            (SELECT x.[co_empr], x.[co_empl], x.[no_empl], x.[no_dire], x.[co_carg], x.[nu_telf], x.[co_loca], x.[co_dpto], x.[co_empl_jefe], x.[co_prof], x.[in_iess], x.[nu_patr], x.[nu_iess], x.[fe_afil_iess], x.[in_sexo], x.[in_esta_civil], x.[fe_naci], x.[in_pago_cnta], x.[nu_cnta_pago], x.[ti_cnta_pago], x.[fe_ingr], x.[fe_sali], x.[ds_debe_oblg], x.[ti_iden], x.[nu_iden], x.[co_hora], x.[ds_clav], x.[mo_suel], x.[mo_suel_neto], x.[in_firm_entr_sali], x.[co_clas], x.[fe_vcto_cont], x.[st_empl], x.[co_usua_mvto_ingr], x.[fe_mvto_ingr], x.[co_usua_mvto_modi], x.[fe_mvto_modi], x.[co_cnta_anti], x.[co_banc_pago], x.[in_pago_cnta_fond_rese], x.[co_banc_pago_fond_rese], x.[nu_cnta_pago_fond_rese], x.[ti_cnta_pago_fond_rese], x.[in_peri_grac], x.[fe_grac_inic], x.[fe_grac_fina], x.[ds_mail], x.[no_usua], x.[nu_exte_tele], x.[co_prov_naci], x.[co_ciud_naci], x.[co_parr], x.[no_sect], x.[ds_refe_vivi], x.[nu_telf_celu], x.[no_cont_emrg], x.[nu_telf_emrg], x.[ds_sut] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_empr]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_empr], x.[co_empl], x.[no_empl], x.[no_dire], x.[co_carg], x.[nu_telf], x.[co_loca], x.[co_dpto], x.[co_empl_jefe], x.[co_prof], x.[in_iess], x.[nu_patr], x.[nu_iess], x.[fe_afil_iess], x.[in_sexo], x.[in_esta_civil], x.[fe_naci], x.[in_pago_cnta], x.[nu_cnta_pago], x.[ti_cnta_pago], x.[fe_ingr], x.[fe_sali], x.[ds_debe_oblg], x.[ti_iden], x.[nu_iden], x.[co_hora], x.[ds_clav], x.[mo_suel], x.[mo_suel_neto], x.[in_firm_entr_sali], x.[co_clas], x.[fe_vcto_cont], x.[st_empl], x.[co_usua_mvto_ingr], x.[fe_mvto_ingr], x.[co_usua_mvto_modi], x.[fe_mvto_modi], x.[co_cnta_anti], x.[co_banc_pago], x.[in_pago_cnta_fond_rese], x.[co_banc_pago_fond_rese], x.[nu_cnta_pago_fond_rese], x.[ti_cnta_pago_fond_rese], x.[in_peri_grac], x.[fe_grac_inic], x.[fe_grac_fina], x.[ds_mail], x.[no_usua], x.[nu_exte_tele], x.[co_prov_naci], x.[co_ciud_naci], x.[co_parr], x.[no_sect], x.[ds_refe_vivi], x.[nu_telf_celu], x.[no_cont_emrg], x.[nu_telf_emrg], x.[ds_sut] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* ----- BD: dbSV  (9 triggers) ----- */
USE [dbSV];
GO

/* --- trg_outbox_svtbcaus  ON dbo.svtbcaus  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_svtbcaus', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_svtbcaus;
GO
CREATE TRIGGER dbo.trg_outbox_svtbcaus
ON dbo.[svtbcaus]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroVidaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_caus]),
            tt.t,
            @op,
            (SELECT x.[co_caus], x.[no_caus], x.[st_caus], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbcaus',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_caus]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_caus], x.[no_caus], x.[st_caus], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbcaus',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_svtbdisc  ON dbo.svtbdisc  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_svtbdisc', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_svtbdisc;
GO
CREATE TRIGGER dbo.trg_outbox_svtbdisc
ON dbo.[svtbdisc]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroVidaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_disc]),
            tt.t,
            @op,
            (SELECT x.[co_disc], x.[no_disc], x.[st_disc], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbdisc',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_disc]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_disc], x.[no_disc], x.[st_disc], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbdisc',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_svtbefec  ON dbo.svtbefec  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_svtbefec', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_svtbefec;
GO
CREATE TRIGGER dbo.trg_outbox_svtbefec
ON dbo.[svtbefec]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroVidaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_efec]),
            tt.t,
            @op,
            (SELECT x.[co_efec], x.[no_efec], x.[ti_efec], x.[st_efec], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbefec',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_efec]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_efec], x.[no_efec], x.[ti_efec], x.[st_efec], x.[fe_ingr], x.[fe_modi], x.[co_usua_ingr], x.[co_usua_modi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbefec',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_svtbfmpg  ON dbo.svtbfmpg  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_svtbfmpg', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_svtbfmpg;
GO
CREATE TRIGGER dbo.trg_outbox_svtbfmpg
ON dbo.[svtbfmpg]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroVidaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ti_fmpg]),
            tt.t,
            @op,
            (SELECT x.[ti_fmpg], x.[ds_fmpg], x.[st_fmpg] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbfmpg',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ti_fmpg]),
            tt.t,
            N'DELETE',
            (SELECT x.[ti_fmpg], x.[ds_fmpg], x.[st_fmpg] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbfmpg',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_svtbstro  ON dbo.svtbstro  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_svtbstro;
GO
CREATE TRIGGER dbo.trg_outbox_svtbstro
ON dbo.[svtbstro]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroVidaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_sine]),
            tt.t,
            @op,
            (SELECT x.[sc_sine], x.[co_prov], x.[ti_sine], x.[co_afil], x.[co_sine], x.[co_caus], x.[co_efec], x.[co_disc], x.[fe_ingr], x.[fe_fall], x.[fe_pres], x.[fe_noti], x.[fe_autr], x.[fe_conf], x.[fe_elim], x.[ti_fmpg], x.[co_banco], x.[co_tfam], x.[st_sine], x.[co_usua_ingr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_elim], x.[ce_esta_ante], x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_sine]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_sine], x.[co_prov], x.[ti_sine], x.[co_afil], x.[co_sine], x.[co_caus], x.[co_efec], x.[co_disc], x.[fe_ingr], x.[fe_fall], x.[fe_pres], x.[fe_noti], x.[fe_autr], x.[fe_conf], x.[fe_elim], x.[ti_fmpg], x.[co_banco], x.[co_tfam], x.[st_sine], x.[co_usua_ingr], x.[co_usua_autr], x.[co_usua_conf], x.[co_usua_elim], x.[ce_esta_ante], x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_svtbstro_bene  ON dbo.svtbstro_bene  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro_bene', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_svtbstro_bene;
GO
CREATE TRIGGER dbo.trg_outbox_svtbstro_bene
ON dbo.[svtbstro_bene]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroVidaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_sine]),
            tt.t,
            @op,
            (SELECT x.[sc_sine], x.[co_fond], x.[co_bene], x.[pr_dist], x.[mo_dist], x.[co_tpar] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_bene',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_sine]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_sine], x.[co_fond], x.[co_bene], x.[pr_dist], x.[mo_dist], x.[co_tpar] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_bene',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_svtbstro_cred  ON dbo.svtbstro_cred  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_svtbstro_cred;
GO
CREATE TRIGGER dbo.trg_outbox_svtbstro_cred
ON dbo.[svtbstro_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroVidaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_sine]),
            tt.t,
            @op,
            (SELECT x.[sc_sine], x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[mo_abno], x.[va_pago_fond], x.[va_desg], x.[va_desg_cont], x.[mo_cobe], x.[in_pago_desg] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_cred',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_sine]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_sine], x.[ti_cred], x.[aa_cred], x.[qs_cred], x.[mo_abno], x.[va_pago_fond], x.[va_desg], x.[va_desg_cont], x.[mo_cobe], x.[in_pago_desg] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_cred',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_svtbstro_deta  ON dbo.svtbstro_deta  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro_deta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_svtbstro_deta;
GO
CREATE TRIGGER dbo.trg_outbox_svtbstro_deta
ON dbo.[svtbstro_deta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroVidaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_sine]),
            tt.t,
            @op,
            (SELECT x.[sc_sine], x.[co_fond], x.[mo_cobe], x.[mo_desg], x.[mo_ncub], x.[mo_ci], x.[mo_tota], x.[va_pago_fond] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_deta',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_sine]),
            tt.t,
            N'DELETE',
            (SELECT x.[sc_sine], x.[co_fond], x.[mo_cobe], x.[mo_desg], x.[mo_ncub], x.[mo_ci], x.[mo_tota], x.[va_pago_fond] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_deta',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* --- trg_outbox_svtbstro_exte  ON dbo.svtbstro_exte  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_svtbstro_exte', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_svtbstro_exte;
GO
CREATE TRIGGER dbo.trg_outbox_svtbstro_exte
ON dbo.[svtbstro_exte]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroVidaParticipeType');

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_afil]),
            tt.t,
            @op,
            (SELECT x.[co_afil], x.[co_sine], x.[fe_ingr_exte], x.[st_sine_exte], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_exte',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_afil]),
            tt.t,
            N'DELETE',
            (SELECT x.[co_afil], x.[co_sine], x.[fe_ingr_exte], x.[st_sine_exte], x.[fe_ingr], x.[co_usua_ingr], x.[fe_elim], x.[co_usua_elim] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSV.dbo.svtbstro_exte',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
GO

/* TOTAL FLUJO 1 = 80 triggers */

/* ############################################################
   FLUJO 2 - Oracle FCME_USER (publica a FCME_USER.CDC_OUTBOX)
   ############################################################ */

/* --- TRG_OUTBOX_ACTUALIZACION_AFILI  ON FCME_USER.ACTUALIZACION_AFILIADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ACTUALIZACION_AFILI
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.ACTUALIZACION_AFILIADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO_CEDU' VALUE :NEW.CODIGO_CEDU);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO_CEDU' VALUE :NEW.CODIGO_CEDU);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGO_CEDU' VALUE :OLD.CODIGO_CEDU);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('actualizacionAfiliadoType', v_pk, v_event, v_payload, 'FCME_USER.ACTUALIZACION_AFILIADO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_ACTUALIZACION_AFILIADO_TY  ON FCME_USER.ACTUALIZACION_AFILIADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ACTUALIZACION_AFILIADO_TY
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.ACTUALIZACION_AFILIADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.CODIGO_CEDU);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO_CEDU' VALUE :NEW.CODIGO_CEDU, 'CODIGO_PROV' VALUE :NEW.CODIGO_PROV, 'CODIGO_CANT' VALUE :NEW.CODIGO_CANT, 'CODIGO_PARR' VALUE :NEW.CODIGO_PARR, 'DESCRIPCION_CALL_PRIM' VALUE :NEW.DESCRIPCION_CALL_PRIM, 'NUMERO_CALL_PRIM' VALUE :NEW.NUMERO_CALL_PRIM, 'DESCRIPCION_CALL_SECU' VALUE :NEW.DESCRIPCION_CALL_SECU, 'NUMERO_CALL_SECU' VALUE :NEW.NUMERO_CALL_SECU, 'NUMERO_MANZ' VALUE :NEW.NUMERO_MANZ, 'NUMERO_VILL' VALUE :NEW.NUMERO_VILL, 'DESCRIPCION_CDLA' VALUE :NEW.DESCRIPCION_CDLA, 'TEXTO_TELF_CONVENIO' VALUE :NEW.TEXTO_TELF_CONVENIO, 'TEXTO_TELF_CELU' VALUE :NEW.TEXTO_TELF_CELU, 'TIPO_OPERACION' VALUE :NEW.TIPO_OPERACION, 'DESCRIPCION_REFERENCIA_VIVI' VALUE :NEW.DESCRIPCION_REFERENCIA_VIVI, 'TEXTO_MAIL' VALUE :NEW.TEXTO_MAIL, 'NOMBRE_CONTABLE_ADIC' VALUE :NEW.NOMBRE_CONTABLE_ADIC, 'TEXTO_TELF_CON1' VALUE :NEW.TEXTO_TELF_CON1, 'TEXTO_TELF_CON2' VALUE :NEW.TEXTO_TELF_CON2, 'TIPO_RELA' VALUE :NEW.TIPO_RELA, 'CODIGO_PROV_INST' VALUE :NEW.CODIGO_PROV_INST, 'CODIGO_TIPO' VALUE :NEW.CODIGO_TIPO, 'CODIGO_INST' VALUE :NEW.CODIGO_INST, 'CODIGO_CARG' VALUE :NEW.CODIGO_CARG, 'CODIGO_NIVE' VALUE :NEW.CODIGO_NIVE, 'CODIGO_CATE' VALUE :NEW.CODIGO_CATE, 'TIPO_CONTABLE' VALUE :NEW.TIPO_CONTABLE, 'TIPO_JORN' VALUE :NEW.TIPO_JORN, 'CODIGO_PROV_OBSQ' VALUE :NEW.CODIGO_PROV_OBSQ, 'CODIGO_ZONA_OBSQ' VALUE :NEW.CODIGO_ZONA_OBSQ, 'INDICADOR_RENO_CREDITO' VALUE :NEW.INDICADOR_RENO_CREDITO, 'INDICADOR_ACCI' VALUE :NEW.INDICADOR_ACCI, 'FECHA_INGRESO' VALUE :NEW.FECHA_INGRESO, 'FECHA_MODIFICACION' VALUE :NEW.FECHA_MODIFICACION, 'FECHA_ULTM_ENVI' VALUE :NEW.FECHA_ULTM_ENVI, 'INDICADOR_IMPR_DOCUMENTO' VALUE :NEW.INDICADOR_IMPR_DOCUMENTO, 'FECHA_IMPR_DOCUMENTO' VALUE :NEW.FECHA_IMPR_DOCUMENTO, 'INDICADOR_COBRANZA_PRES' VALUE :NEW.INDICADOR_COBRANZA_PRES, 'INDICADOR_VALD_CELU' VALUE :NEW.INDICADOR_VALD_CELU, 'INDICADOR_VALD_MAIL' VALUE :NEW.INDICADOR_VALD_MAIL, 'CODIGO_AMI' VALUE :NEW.CODIGO_AMI, 'ESTADO_ENTR_OBSQ' VALUE :NEW.ESTADO_ENTR_OBSQ, 'FECHA_ENTR_OBSQ' VALUE :NEW.FECHA_ENTR_OBSQ, 'FECHA_VERI_DATO' VALUE :NEW.FECHA_VERI_DATO, 'INDICADOR_IMPR_DOCUMENTO_CRED' VALUE :NEW.INDICADOR_IMPR_DOCUMENTO_CRED, 'FECHA_IMPR_DOCUMENTO_CREDITO' VALUE :NEW.FECHA_IMPR_DOCUMENTO_CREDITO, 'NOMBRE_INST' VALUE :NEW.NOMBRE_INST, 'CODIGO_CANT_INST' VALUE :NEW.CODIGO_CANT_INST, 'CODIGO_PARR_INST' VALUE :NEW.CODIGO_PARR_INST);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.CODIGO_CEDU);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO_CEDU' VALUE :NEW.CODIGO_CEDU, 'CODIGO_PROV' VALUE :NEW.CODIGO_PROV, 'CODIGO_CANT' VALUE :NEW.CODIGO_CANT, 'CODIGO_PARR' VALUE :NEW.CODIGO_PARR, 'DESCRIPCION_CALL_PRIM' VALUE :NEW.DESCRIPCION_CALL_PRIM, 'NUMERO_CALL_PRIM' VALUE :NEW.NUMERO_CALL_PRIM, 'DESCRIPCION_CALL_SECU' VALUE :NEW.DESCRIPCION_CALL_SECU, 'NUMERO_CALL_SECU' VALUE :NEW.NUMERO_CALL_SECU, 'NUMERO_MANZ' VALUE :NEW.NUMERO_MANZ, 'NUMERO_VILL' VALUE :NEW.NUMERO_VILL, 'DESCRIPCION_CDLA' VALUE :NEW.DESCRIPCION_CDLA, 'TEXTO_TELF_CONVENIO' VALUE :NEW.TEXTO_TELF_CONVENIO, 'TEXTO_TELF_CELU' VALUE :NEW.TEXTO_TELF_CELU, 'TIPO_OPERACION' VALUE :NEW.TIPO_OPERACION, 'DESCRIPCION_REFERENCIA_VIVI' VALUE :NEW.DESCRIPCION_REFERENCIA_VIVI, 'TEXTO_MAIL' VALUE :NEW.TEXTO_MAIL, 'NOMBRE_CONTABLE_ADIC' VALUE :NEW.NOMBRE_CONTABLE_ADIC, 'TEXTO_TELF_CON1' VALUE :NEW.TEXTO_TELF_CON1, 'TEXTO_TELF_CON2' VALUE :NEW.TEXTO_TELF_CON2, 'TIPO_RELA' VALUE :NEW.TIPO_RELA, 'CODIGO_PROV_INST' VALUE :NEW.CODIGO_PROV_INST, 'CODIGO_TIPO' VALUE :NEW.CODIGO_TIPO, 'CODIGO_INST' VALUE :NEW.CODIGO_INST, 'CODIGO_CARG' VALUE :NEW.CODIGO_CARG, 'CODIGO_NIVE' VALUE :NEW.CODIGO_NIVE, 'CODIGO_CATE' VALUE :NEW.CODIGO_CATE, 'TIPO_CONTABLE' VALUE :NEW.TIPO_CONTABLE, 'TIPO_JORN' VALUE :NEW.TIPO_JORN, 'CODIGO_PROV_OBSQ' VALUE :NEW.CODIGO_PROV_OBSQ, 'CODIGO_ZONA_OBSQ' VALUE :NEW.CODIGO_ZONA_OBSQ, 'INDICADOR_RENO_CREDITO' VALUE :NEW.INDICADOR_RENO_CREDITO, 'INDICADOR_ACCI' VALUE :NEW.INDICADOR_ACCI, 'FECHA_INGRESO' VALUE :NEW.FECHA_INGRESO, 'FECHA_MODIFICACION' VALUE :NEW.FECHA_MODIFICACION, 'FECHA_ULTM_ENVI' VALUE :NEW.FECHA_ULTM_ENVI, 'INDICADOR_IMPR_DOCUMENTO' VALUE :NEW.INDICADOR_IMPR_DOCUMENTO, 'FECHA_IMPR_DOCUMENTO' VALUE :NEW.FECHA_IMPR_DOCUMENTO, 'INDICADOR_COBRANZA_PRES' VALUE :NEW.INDICADOR_COBRANZA_PRES, 'INDICADOR_VALD_CELU' VALUE :NEW.INDICADOR_VALD_CELU, 'INDICADOR_VALD_MAIL' VALUE :NEW.INDICADOR_VALD_MAIL, 'CODIGO_AMI' VALUE :NEW.CODIGO_AMI, 'ESTADO_ENTR_OBSQ' VALUE :NEW.ESTADO_ENTR_OBSQ, 'FECHA_ENTR_OBSQ' VALUE :NEW.FECHA_ENTR_OBSQ, 'FECHA_VERI_DATO' VALUE :NEW.FECHA_VERI_DATO, 'INDICADOR_IMPR_DOCUMENTO_CRED' VALUE :NEW.INDICADOR_IMPR_DOCUMENTO_CRED, 'FECHA_IMPR_DOCUMENTO_CREDITO' VALUE :NEW.FECHA_IMPR_DOCUMENTO_CREDITO, 'NOMBRE_INST' VALUE :NEW.NOMBRE_INST, 'CODIGO_CANT_INST' VALUE :NEW.CODIGO_CANT_INST, 'CODIGO_PARR_INST' VALUE :NEW.CODIGO_PARR_INST);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.CODIGO_CEDU);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGO_CEDU' VALUE :OLD.CODIGO_CEDU, 'CODIGO_PROV' VALUE :OLD.CODIGO_PROV, 'CODIGO_CANT' VALUE :OLD.CODIGO_CANT, 'CODIGO_PARR' VALUE :OLD.CODIGO_PARR, 'DESCRIPCION_CALL_PRIM' VALUE :OLD.DESCRIPCION_CALL_PRIM, 'NUMERO_CALL_PRIM' VALUE :OLD.NUMERO_CALL_PRIM, 'DESCRIPCION_CALL_SECU' VALUE :OLD.DESCRIPCION_CALL_SECU, 'NUMERO_CALL_SECU' VALUE :OLD.NUMERO_CALL_SECU, 'NUMERO_MANZ' VALUE :OLD.NUMERO_MANZ, 'NUMERO_VILL' VALUE :OLD.NUMERO_VILL, 'DESCRIPCION_CDLA' VALUE :OLD.DESCRIPCION_CDLA, 'TEXTO_TELF_CONVENIO' VALUE :OLD.TEXTO_TELF_CONVENIO, 'TEXTO_TELF_CELU' VALUE :OLD.TEXTO_TELF_CELU, 'TIPO_OPERACION' VALUE :OLD.TIPO_OPERACION, 'DESCRIPCION_REFERENCIA_VIVI' VALUE :OLD.DESCRIPCION_REFERENCIA_VIVI, 'TEXTO_MAIL' VALUE :OLD.TEXTO_MAIL, 'NOMBRE_CONTABLE_ADIC' VALUE :OLD.NOMBRE_CONTABLE_ADIC, 'TEXTO_TELF_CON1' VALUE :OLD.TEXTO_TELF_CON1, 'TEXTO_TELF_CON2' VALUE :OLD.TEXTO_TELF_CON2, 'TIPO_RELA' VALUE :OLD.TIPO_RELA, 'CODIGO_PROV_INST' VALUE :OLD.CODIGO_PROV_INST, 'CODIGO_TIPO' VALUE :OLD.CODIGO_TIPO, 'CODIGO_INST' VALUE :OLD.CODIGO_INST, 'CODIGO_CARG' VALUE :OLD.CODIGO_CARG, 'CODIGO_NIVE' VALUE :OLD.CODIGO_NIVE, 'CODIGO_CATE' VALUE :OLD.CODIGO_CATE, 'TIPO_CONTABLE' VALUE :OLD.TIPO_CONTABLE, 'TIPO_JORN' VALUE :OLD.TIPO_JORN, 'CODIGO_PROV_OBSQ' VALUE :OLD.CODIGO_PROV_OBSQ, 'CODIGO_ZONA_OBSQ' VALUE :OLD.CODIGO_ZONA_OBSQ, 'INDICADOR_RENO_CREDITO' VALUE :OLD.INDICADOR_RENO_CREDITO, 'INDICADOR_ACCI' VALUE :OLD.INDICADOR_ACCI, 'FECHA_INGRESO' VALUE :OLD.FECHA_INGRESO, 'FECHA_MODIFICACION' VALUE :OLD.FECHA_MODIFICACION, 'FECHA_ULTM_ENVI' VALUE :OLD.FECHA_ULTM_ENVI, 'INDICADOR_IMPR_DOCUMENTO' VALUE :OLD.INDICADOR_IMPR_DOCUMENTO, 'FECHA_IMPR_DOCUMENTO' VALUE :OLD.FECHA_IMPR_DOCUMENTO, 'INDICADOR_COBRANZA_PRES' VALUE :OLD.INDICADOR_COBRANZA_PRES, 'INDICADOR_VALD_CELU' VALUE :OLD.INDICADOR_VALD_CELU, 'INDICADOR_VALD_MAIL' VALUE :OLD.INDICADOR_VALD_MAIL, 'CODIGO_AMI' VALUE :OLD.CODIGO_AMI, 'ESTADO_ENTR_OBSQ' VALUE :OLD.ESTADO_ENTR_OBSQ, 'FECHA_ENTR_OBSQ' VALUE :OLD.FECHA_ENTR_OBSQ, 'FECHA_VERI_DATO' VALUE :OLD.FECHA_VERI_DATO, 'INDICADOR_IMPR_DOCUMENTO_CRED' VALUE :OLD.INDICADOR_IMPR_DOCUMENTO_CRED, 'FECHA_IMPR_DOCUMENTO_CREDITO' VALUE :OLD.FECHA_IMPR_DOCUMENTO_CREDITO, 'NOMBRE_INST' VALUE :OLD.NOMBRE_INST, 'CODIGO_CANT_INST' VALUE :OLD.CODIGO_CANT_INST, 'CODIGO_PARR_INST' VALUE :OLD.CODIGO_PARR_INST);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'actualizacionAfiliadoType', v_event, v_payload, 'FCME_USER.ACTUALIZACION_AFILIADO_TYPE');
END;
/

/* --- TRG_OUTBOX_ACTUALIZACION_DOCUM  ON FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ACTUALIZACION_DOCUM
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIA_ACTU_DOCS' VALUE :NEW.SECUENCIA_ACTU_DOCS, 'CODIGO_EMPRESA' VALUE :NEW.CODIGO_EMPRESA, 'CODIGO_CEDU' VALUE :NEW.CODIGO_CEDU);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIA_ACTU_DOCS' VALUE :NEW.SECUENCIA_ACTU_DOCS, 'CODIGO_EMPRESA' VALUE :NEW.CODIGO_EMPRESA, 'CODIGO_CEDU' VALUE :NEW.CODIGO_CEDU);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIA_ACTU_DOCS' VALUE :OLD.SECUENCIA_ACTU_DOCS, 'CODIGO_EMPRESA' VALUE :OLD.CODIGO_EMPRESA, 'CODIGO_CEDU' VALUE :OLD.CODIGO_CEDU);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('actualizacionDocumentosType', v_pk, v_event, v_payload, 'FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_ACTUALIZACION_DOCUMENTOS_  ON FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ACTUALIZACION_DOCUMENTOS_
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.CODIGO_CEDU);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIA_ACTU_DOCS' VALUE :NEW.SECUENCIA_ACTU_DOCS, 'CODIGO_EMPRESA' VALUE :NEW.CODIGO_EMPRESA, 'CODIGO_CEDU' VALUE :NEW.CODIGO_CEDU, 'SECUENCIA_ACTV_SUJE_CRED' VALUE :NEW.SECUENCIA_ACTV_SUJE_CRED, 'SECUENCIA_ORGN_INGR' VALUE :NEW.SECUENCIA_ORGN_INGR, 'CODIGO_PERS_POLI_EXPU' VALUE :NEW.CODIGO_PERS_POLI_EXPU, 'DESCRIPCION_CIUD_NACI' VALUE :NEW.DESCRIPCION_CIUD_NACI, 'INDICADOR_COMI_SERV' VALUE :NEW.INDICADOR_COMI_SERV, 'DESCRIPCION_COMI_SERV' VALUE :NEW.DESCRIPCION_COMI_SERV, 'FECHA_INGR' VALUE :NEW.FECHA_INGR, 'USUARIO_INGRESA' VALUE :NEW.USUARIO_INGRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.CODIGO_CEDU);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIA_ACTU_DOCS' VALUE :NEW.SECUENCIA_ACTU_DOCS, 'CODIGO_EMPRESA' VALUE :NEW.CODIGO_EMPRESA, 'CODIGO_CEDU' VALUE :NEW.CODIGO_CEDU, 'SECUENCIA_ACTV_SUJE_CRED' VALUE :NEW.SECUENCIA_ACTV_SUJE_CRED, 'SECUENCIA_ORGN_INGR' VALUE :NEW.SECUENCIA_ORGN_INGR, 'CODIGO_PERS_POLI_EXPU' VALUE :NEW.CODIGO_PERS_POLI_EXPU, 'DESCRIPCION_CIUD_NACI' VALUE :NEW.DESCRIPCION_CIUD_NACI, 'INDICADOR_COMI_SERV' VALUE :NEW.INDICADOR_COMI_SERV, 'DESCRIPCION_COMI_SERV' VALUE :NEW.DESCRIPCION_COMI_SERV, 'FECHA_INGR' VALUE :NEW.FECHA_INGR, 'USUARIO_INGRESA' VALUE :NEW.USUARIO_INGRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.CODIGO_CEDU);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIA_ACTU_DOCS' VALUE :OLD.SECUENCIA_ACTU_DOCS, 'CODIGO_EMPRESA' VALUE :OLD.CODIGO_EMPRESA, 'CODIGO_CEDU' VALUE :OLD.CODIGO_CEDU, 'SECUENCIA_ACTV_SUJE_CRED' VALUE :OLD.SECUENCIA_ACTV_SUJE_CRED, 'SECUENCIA_ORGN_INGR' VALUE :OLD.SECUENCIA_ORGN_INGR, 'CODIGO_PERS_POLI_EXPU' VALUE :OLD.CODIGO_PERS_POLI_EXPU, 'DESCRIPCION_CIUD_NACI' VALUE :OLD.DESCRIPCION_CIUD_NACI, 'INDICADOR_COMI_SERV' VALUE :OLD.INDICADOR_COMI_SERV, 'DESCRIPCION_COMI_SERV' VALUE :OLD.DESCRIPCION_COMI_SERV, 'FECHA_INGR' VALUE :OLD.FECHA_INGR, 'USUARIO_INGRESA' VALUE :OLD.USUARIO_INGRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'actualizacionDocumentosType', v_event, v_payload, 'FCME_USER.ACTUALIZACION_DOCUMENTOS_TYPE');
END;
/

/* --- TRG_OUTBOX_AGENDAMAILAFILIADO_TYPE  ON FCME_USER.AGENDAMAILAFILIADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_AGENDAMAILAFILIADO_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.AGENDAMAILAFILIADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'DESCRIPCIONMAIL' VALUE :NEW.DESCRIPCIONMAIL, 'INDICADORPRIN' VALUE :NEW.INDICADORPRIN, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'CODIGOUSUELIM' VALUE :NEW.CODIGOUSUELIM, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'DESCRIPCIONMAIL' VALUE :NEW.DESCRIPCIONMAIL, 'INDICADORPRIN' VALUE :NEW.INDICADORPRIN, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'CODIGOUSUELIM' VALUE :NEW.CODIGOUSUELIM, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOCEDU' VALUE :OLD.CODIGOCEDU, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'DESCRIPCIONMAIL' VALUE :OLD.DESCRIPCIONMAIL, 'INDICADORPRIN' VALUE :OLD.INDICADORPRIN, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'FECHAINGRESO' VALUE :OLD.FECHAINGRESO, 'USUARIOMODIFICA' VALUE :OLD.USUARIOMODIFICA, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'CODIGOUSUELIM' VALUE :OLD.CODIGOUSUELIM, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'agendaMailAfiliadoType', v_event, v_payload, 'FCME_USER.AGENDAMAILAFILIADO_TYPE');
END;
/

/* --- TRG_OUTBOX_AREALABORALPARTICIP  ON FCME_USER.AREALABORALPARTICIPE_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_AREALABORALPARTICIP
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.AREALABORALPARTICIPE_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOAREALABORAL' VALUE :NEW.CODIGOAREALABORAL, 'DESCRIPCIONAREALABORAL' VALUE :NEW.DESCRIPCIONAREALABORAL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOAREALABORAL' VALUE :NEW.CODIGOAREALABORAL, 'DESCRIPCIONAREALABORAL' VALUE :NEW.DESCRIPCIONAREALABORAL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOAREALABORAL' VALUE :OLD.CODIGOAREALABORAL, 'DESCRIPCIONAREALABORAL' VALUE :OLD.DESCRIPCIONAREALABORAL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('areaLaboralParticipeType', v_pk, v_event, v_payload, 'FCME_USER.AREALABORALPARTICIPE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_AUDITORIAAFILIADO_TYPE  ON FCME_USER.AUDITORIAAFILIADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_AUDITORIAAFILIADO_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.AUDITORIAAFILIADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAAUDITORIA' VALUE :NEW.SECUENCIAAUDITORIA, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'CAMPOMODIFICADO' VALUE :NEW.CAMPOMODIFICADO, 'VALORANTERIOR' VALUE :NEW.VALORANTERIOR, 'VALORNUEVO' VALUE :NEW.VALORNUEVO, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'TIPOOPERACION' VALUE :NEW.TIPOOPERACION, 'ORIGENMODIFICACION' VALUE :NEW.ORIGENMODIFICACION, 'ESTADO' VALUE :NEW.ESTADO, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'RELACIONPRODUCTO' VALUE :NEW.RELACIONPRODUCTO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIAACTUALIZACION' VALUE :NEW.SECUENCIAACTUALIZACION, 'NUMEROCEDULARECIBECOREO' VALUE :NEW.NUMEROCEDULARECIBECOREO, 'NUMEROTELEFONOCONVENCIONAL' VALUE :NEW.NUMEROTELEFONOCONVENCIONAL, 'NUMEROTELEFONOCELULAR' VALUE :NEW.NUMEROTELEFONOCELULAR, 'DIRECCIONPATRONO' VALUE :NEW.DIRECCIONPATRONO, 'DESCRIPCIONINSTITUCION' VALUE :NEW.DESCRIPCIONINSTITUCION, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'HORAINGRESO' VALUE :NEW.HORAINGRESO, 'CODIGOCAMPOAMODIFICAR' VALUE :NEW.CODIGOCAMPOAMODIFICAR, 'NOMBRECAMPOAMODIFICAR' VALUE :NEW.NOMBRECAMPOAMODIFICAR, 'DESCRIPCIONCAMPOAMODIFICAR' VALUE :NEW.DESCRIPCIONCAMPOAMODIFICAR, 'HORACREACION' VALUE :NEW.HORACREACION, 'DESCRIPCIONADICIONAL' VALUE :NEW.DESCRIPCIONADICIONAL, 'CODIGOMOTIVOMANTENIMIENTO' VALUE :NEW.CODIGOMOTIVOMANTENIMIENTO, 'NUMEROTRANSACCION' VALUE :NEW.NUMEROTRANSACCION, 'CODIGOTRANSACCIONUTILIZADA' VALUE :NEW.CODIGOTRANSACCIONUTILIZADA, 'HORAGENERACIONREGISTRO' VALUE :NEW.HORAGENERACIONREGISTRO, 'INDICADORCONTRATOCESANTIA' VALUE :NEW.INDICADORCONTRATOCESANTIA, 'NIVELAPORTE' VALUE :NEW.NIVELAPORTE, 'VALORDESCUENTOHIPOTECARIO' VALUE :NEW.VALORDESCUENTOHIPOTECARIO, 'CODIGOCATEGORIA' VALUE :NEW.CODIGOCATEGORIA, 'CEDULACOORDINADOR' VALUE :NEW.CEDULACOORDINADOR, 'PORCENTAJECAM' VALUE :NEW.PORCENTAJECAM, 'PORCENTAJEFUNCIONAL' VALUE :NEW.PORCENTAJEFUNCIONAL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAAUDITORIA' VALUE :NEW.SECUENCIAAUDITORIA, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'CAMPOMODIFICADO' VALUE :NEW.CAMPOMODIFICADO, 'VALORANTERIOR' VALUE :NEW.VALORANTERIOR, 'VALORNUEVO' VALUE :NEW.VALORNUEVO, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'TIPOOPERACION' VALUE :NEW.TIPOOPERACION, 'ORIGENMODIFICACION' VALUE :NEW.ORIGENMODIFICACION, 'ESTADO' VALUE :NEW.ESTADO, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'RELACIONPRODUCTO' VALUE :NEW.RELACIONPRODUCTO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIAACTUALIZACION' VALUE :NEW.SECUENCIAACTUALIZACION, 'NUMEROCEDULARECIBECOREO' VALUE :NEW.NUMEROCEDULARECIBECOREO, 'NUMEROTELEFONOCONVENCIONAL' VALUE :NEW.NUMEROTELEFONOCONVENCIONAL, 'NUMEROTELEFONOCELULAR' VALUE :NEW.NUMEROTELEFONOCELULAR, 'DIRECCIONPATRONO' VALUE :NEW.DIRECCIONPATRONO, 'DESCRIPCIONINSTITUCION' VALUE :NEW.DESCRIPCIONINSTITUCION, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'HORAINGRESO' VALUE :NEW.HORAINGRESO, 'CODIGOCAMPOAMODIFICAR' VALUE :NEW.CODIGOCAMPOAMODIFICAR, 'NOMBRECAMPOAMODIFICAR' VALUE :NEW.NOMBRECAMPOAMODIFICAR, 'DESCRIPCIONCAMPOAMODIFICAR' VALUE :NEW.DESCRIPCIONCAMPOAMODIFICAR, 'HORACREACION' VALUE :NEW.HORACREACION, 'DESCRIPCIONADICIONAL' VALUE :NEW.DESCRIPCIONADICIONAL, 'CODIGOMOTIVOMANTENIMIENTO' VALUE :NEW.CODIGOMOTIVOMANTENIMIENTO, 'NUMEROTRANSACCION' VALUE :NEW.NUMEROTRANSACCION, 'CODIGOTRANSACCIONUTILIZADA' VALUE :NEW.CODIGOTRANSACCIONUTILIZADA, 'HORAGENERACIONREGISTRO' VALUE :NEW.HORAGENERACIONREGISTRO, 'INDICADORCONTRATOCESANTIA' VALUE :NEW.INDICADORCONTRATOCESANTIA, 'NIVELAPORTE' VALUE :NEW.NIVELAPORTE, 'VALORDESCUENTOHIPOTECARIO' VALUE :NEW.VALORDESCUENTOHIPOTECARIO, 'CODIGOCATEGORIA' VALUE :NEW.CODIGOCATEGORIA, 'CEDULACOORDINADOR' VALUE :NEW.CEDULACOORDINADOR, 'PORCENTAJECAM' VALUE :NEW.PORCENTAJECAM, 'PORCENTAJEFUNCIONAL' VALUE :NEW.PORCENTAJEFUNCIONAL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIAAUDITORIA' VALUE :OLD.SECUENCIAAUDITORIA, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'CAMPOMODIFICADO' VALUE :OLD.CAMPOMODIFICADO, 'VALORANTERIOR' VALUE :OLD.VALORANTERIOR, 'VALORNUEVO' VALUE :OLD.VALORNUEVO, 'CODIGOUSUARIO' VALUE :OLD.CODIGOUSUARIO, 'TIPOOPERACION' VALUE :OLD.TIPOOPERACION, 'ORIGENMODIFICACION' VALUE :OLD.ORIGENMODIFICACION, 'ESTADO' VALUE :OLD.ESTADO, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA, 'TIPOIDENTIFICACION' VALUE :OLD.TIPOIDENTIFICACION, 'RELACIONPRODUCTO' VALUE :OLD.RELACIONPRODUCTO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'SECUENCIAACTUALIZACION' VALUE :OLD.SECUENCIAACTUALIZACION, 'NUMEROCEDULARECIBECOREO' VALUE :OLD.NUMEROCEDULARECIBECOREO, 'NUMEROTELEFONOCONVENCIONAL' VALUE :OLD.NUMEROTELEFONOCONVENCIONAL, 'NUMEROTELEFONOCELULAR' VALUE :OLD.NUMEROTELEFONOCELULAR, 'DIRECCIONPATRONO' VALUE :OLD.DIRECCIONPATRONO, 'DESCRIPCIONINSTITUCION' VALUE :OLD.DESCRIPCIONINSTITUCION, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'HORAINGRESO' VALUE :OLD.HORAINGRESO, 'CODIGOCAMPOAMODIFICAR' VALUE :OLD.CODIGOCAMPOAMODIFICAR, 'NOMBRECAMPOAMODIFICAR' VALUE :OLD.NOMBRECAMPOAMODIFICAR, 'DESCRIPCIONCAMPOAMODIFICAR' VALUE :OLD.DESCRIPCIONCAMPOAMODIFICAR, 'HORACREACION' VALUE :OLD.HORACREACION, 'DESCRIPCIONADICIONAL' VALUE :OLD.DESCRIPCIONADICIONAL, 'CODIGOMOTIVOMANTENIMIENTO' VALUE :OLD.CODIGOMOTIVOMANTENIMIENTO, 'NUMEROTRANSACCION' VALUE :OLD.NUMEROTRANSACCION, 'CODIGOTRANSACCIONUTILIZADA' VALUE :OLD.CODIGOTRANSACCIONUTILIZADA, 'HORAGENERACIONREGISTRO' VALUE :OLD.HORAGENERACIONREGISTRO, 'INDICADORCONTRATOCESANTIA' VALUE :OLD.INDICADORCONTRATOCESANTIA, 'NIVELAPORTE' VALUE :OLD.NIVELAPORTE, 'VALORDESCUENTOHIPOTECARIO' VALUE :OLD.VALORDESCUENTOHIPOTECARIO, 'CODIGOCATEGORIA' VALUE :OLD.CODIGOCATEGORIA, 'CEDULACOORDINADOR' VALUE :OLD.CEDULACOORDINADOR, 'PORCENTAJECAM' VALUE :OLD.PORCENTAJECAM, 'PORCENTAJEFUNCIONAL' VALUE :OLD.PORCENTAJEFUNCIONAL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'auditoriaAfiliadoType', v_event, v_payload, 'FCME_USER.AUDITORIAAFILIADO_TYPE');
END;
/

/* --- TRG_OUTBOX_BENEFICIARIOPARTICIPE_TYP  ON FCME_USER.BENEFICIARIOPARTICIPE_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_BENEFICIARIOPARTICIPE_TYP
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.BENEFICIARIOPARTICIPE_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'APELLIDOSBENEFICIARIOS' VALUE :NEW.APELLIDOSBENEFICIARIOS, 'CODIGOBANCOPAGO' VALUE :NEW.CODIGOBANCOPAGO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOUSUARIOINGRESOREGISTRO' VALUE :NEW.CODIGOUSUARIOINGRESOREGISTRO, 'CODIGOUSUARIOMODIFICOREGISTRO' VALUE :NEW.CODIGOUSUARIOMODIFICOREGISTRO, 'ESTATUSDELBENEFICIARIO' VALUE :NEW.ESTATUSDELBENEFICIARIO, 'FECHACREACIONREGISTRO' VALUE :NEW.FECHACREACIONREGISTRO, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'NOMBRESBENEFICIARIO' VALUE :NEW.NOMBRESBENEFICIARIO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'NUMEROCEDULABENEFICIARIO' VALUE :NEW.NUMEROCEDULABENEFICIARIO, 'NUMEROCUENTA' VALUE :NEW.NUMEROCUENTA, 'PORCENTAJEDISTRIBUCIONVALORES' VALUE :NEW.PORCENTAJEDISTRIBUCIONVALORES, 'SECUENCIABENEFICIARIO' VALUE :NEW.SECUENCIABENEFICIARIO, 'TIPOCUENTAPAGO' VALUE :NEW.TIPOCUENTAPAGO, 'CODIGOBANCO' VALUE :NEW.CODIGOBANCO, 'CODIGOBANCODESEMBOLSO' VALUE :NEW.CODIGOBANCODESEMBOLSO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHADESEMBOLSOLIQUIDACION' VALUE :NEW.FECHADESEMBOLSOLIQUIDACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'MONTOARECIBIR' VALUE :NEW.MONTOARECIBIR, 'NOMBREBENEFICIARIO' VALUE :NEW.NOMBREBENEFICIARIO, 'SECUENCIADESEMBOLSOPORLIQUIDACION' VALUE :NEW.SECUENCIADESEMBOLSOPORLIQUIDACION, 'SECUENCIARETIRO' VALUE :NEW.SECUENCIARETIRO, 'TIPOCUENTA' VALUE :NEW.TIPOCUENTA, 'TIPODESEMBOLSO' VALUE :NEW.TIPODESEMBOLSO, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'APELLIDOSBENEFICIARIOS' VALUE :NEW.APELLIDOSBENEFICIARIOS, 'CODIGOBANCOPAGO' VALUE :NEW.CODIGOBANCOPAGO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOUSUARIOINGRESOREGISTRO' VALUE :NEW.CODIGOUSUARIOINGRESOREGISTRO, 'CODIGOUSUARIOMODIFICOREGISTRO' VALUE :NEW.CODIGOUSUARIOMODIFICOREGISTRO, 'ESTATUSDELBENEFICIARIO' VALUE :NEW.ESTATUSDELBENEFICIARIO, 'FECHACREACIONREGISTRO' VALUE :NEW.FECHACREACIONREGISTRO, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'NOMBRESBENEFICIARIO' VALUE :NEW.NOMBRESBENEFICIARIO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'NUMEROCEDULABENEFICIARIO' VALUE :NEW.NUMEROCEDULABENEFICIARIO, 'NUMEROCUENTA' VALUE :NEW.NUMEROCUENTA, 'PORCENTAJEDISTRIBUCIONVALORES' VALUE :NEW.PORCENTAJEDISTRIBUCIONVALORES, 'SECUENCIABENEFICIARIO' VALUE :NEW.SECUENCIABENEFICIARIO, 'TIPOCUENTAPAGO' VALUE :NEW.TIPOCUENTAPAGO, 'CODIGOBANCO' VALUE :NEW.CODIGOBANCO, 'CODIGOBANCODESEMBOLSO' VALUE :NEW.CODIGOBANCODESEMBOLSO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHADESEMBOLSOLIQUIDACION' VALUE :NEW.FECHADESEMBOLSOLIQUIDACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'MONTOARECIBIR' VALUE :NEW.MONTOARECIBIR, 'NOMBREBENEFICIARIO' VALUE :NEW.NOMBREBENEFICIARIO, 'SECUENCIADESEMBOLSOPORLIQUIDACION' VALUE :NEW.SECUENCIADESEMBOLSOPORLIQUIDACION, 'SECUENCIARETIRO' VALUE :NEW.SECUENCIARETIRO, 'TIPOCUENTA' VALUE :NEW.TIPOCUENTA, 'TIPODESEMBOLSO' VALUE :NEW.TIPODESEMBOLSO, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'APELLIDOSBENEFICIARIOS' VALUE :OLD.APELLIDOSBENEFICIARIOS, 'CODIGOBANCOPAGO' VALUE :OLD.CODIGOBANCOPAGO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOUSUARIOINGRESOREGISTRO' VALUE :OLD.CODIGOUSUARIOINGRESOREGISTRO, 'CODIGOUSUARIOMODIFICOREGISTRO' VALUE :OLD.CODIGOUSUARIOMODIFICOREGISTRO, 'ESTATUSDELBENEFICIARIO' VALUE :OLD.ESTATUSDELBENEFICIARIO, 'FECHACREACIONREGISTRO' VALUE :OLD.FECHACREACIONREGISTRO, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'NOMBRESBENEFICIARIO' VALUE :OLD.NOMBRESBENEFICIARIO, 'NUMEROCEDULA' VALUE :OLD.NUMEROCEDULA, 'NUMEROCEDULABENEFICIARIO' VALUE :OLD.NUMEROCEDULABENEFICIARIO, 'NUMEROCUENTA' VALUE :OLD.NUMEROCUENTA, 'PORCENTAJEDISTRIBUCIONVALORES' VALUE :OLD.PORCENTAJEDISTRIBUCIONVALORES, 'SECUENCIABENEFICIARIO' VALUE :OLD.SECUENCIABENEFICIARIO, 'TIPOCUENTAPAGO' VALUE :OLD.TIPOCUENTAPAGO, 'CODIGOBANCO' VALUE :OLD.CODIGOBANCO, 'CODIGOBANCODESEMBOLSO' VALUE :OLD.CODIGOBANCODESEMBOLSO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHADESEMBOLSOLIQUIDACION' VALUE :OLD.FECHADESEMBOLSOLIQUIDACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'MONTOARECIBIR' VALUE :OLD.MONTOARECIBIR, 'NOMBREBENEFICIARIO' VALUE :OLD.NOMBREBENEFICIARIO, 'SECUENCIADESEMBOLSOPORLIQUIDACION' VALUE :OLD.SECUENCIADESEMBOLSOPORLIQUIDACION, 'SECUENCIARETIRO' VALUE :OLD.SECUENCIARETIRO, 'TIPOCUENTA' VALUE :OLD.TIPOCUENTA, 'TIPODESEMBOLSO' VALUE :OLD.TIPODESEMBOLSO, 'USUARIOELIMINA' VALUE :OLD.USUARIOELIMINA, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'beneficiarioParticipeType', v_event, v_payload, 'FCME_USER.BENEFICIARIOPARTICIPE_TYPE');
END;
/

/* --- TRG_OUTBOX_COMISIONPARTICIPE_T  ON FCME_USER.COMISIONPARTICIPE_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_COMISIONPARTICIPE_T
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.COMISIONPARTICIPE_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSECUENCIACOMISION' VALUE :NEW.CODIGOSECUENCIACOMISION, 'CEDULAPROMOTOR' VALUE :NEW.CEDULAPROMOTOR);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSECUENCIACOMISION' VALUE :NEW.CODIGOSECUENCIACOMISION, 'CEDULAPROMOTOR' VALUE :NEW.CEDULAPROMOTOR);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOSECUENCIACOMISION' VALUE :OLD.CODIGOSECUENCIACOMISION, 'CEDULAPROMOTOR' VALUE :OLD.CEDULAPROMOTOR);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('comisionParticipe_type', v_pk, v_event, v_payload, 'FCME_USER.COMISIONPARTICIPE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUENTABANCARIAAFILIADO_TY  ON FCME_USER.CUENTABANCARIAAFILIADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUENTABANCARIAAFILIADO_TY
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.CUENTABANCARIAAFILIADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CEDULABENEFICIARIO' VALUE :NEW.CEDULABENEFICIARIO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CUENTABANCARIA' VALUE :NEW.CUENTABANCARIA, 'CUENTABANCODESTINO' VALUE :NEW.CUENTABANCODESTINO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'NOMBREBENEFICIARIOPAGO' VALUE :NEW.NOMBREBENEFICIARIOPAGO, 'NUMEROCUENTA' VALUE :NEW.NUMEROCUENTA, 'SECUENCIALIQUIDACION' VALUE :NEW.SECUENCIALIQUIDACION, 'SECUENCIAPAGO' VALUE :NEW.SECUENCIAPAGO, 'TIPOCUENTA' VALUE :NEW.TIPOCUENTA, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'CODIGOBANCO' VALUE :NEW.CODIGOBANCO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CEDULABENEFICIARIO' VALUE :NEW.CEDULABENEFICIARIO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CUENTABANCARIA' VALUE :NEW.CUENTABANCARIA, 'CUENTABANCODESTINO' VALUE :NEW.CUENTABANCODESTINO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'NOMBREBENEFICIARIOPAGO' VALUE :NEW.NOMBREBENEFICIARIOPAGO, 'NUMEROCUENTA' VALUE :NEW.NUMEROCUENTA, 'SECUENCIALIQUIDACION' VALUE :NEW.SECUENCIALIQUIDACION, 'SECUENCIAPAGO' VALUE :NEW.SECUENCIAPAGO, 'TIPOCUENTA' VALUE :NEW.TIPOCUENTA, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'CODIGOBANCO' VALUE :NEW.CODIGOBANCO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CEDULABENEFICIARIO' VALUE :OLD.CEDULABENEFICIARIO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CUENTABANCARIA' VALUE :OLD.CUENTABANCARIA, 'CUENTABANCODESTINO' VALUE :OLD.CUENTABANCODESTINO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'MONTOMOVIMIENTO' VALUE :OLD.MONTOMOVIMIENTO, 'NOMBREBENEFICIARIOPAGO' VALUE :OLD.NOMBREBENEFICIARIOPAGO, 'NUMEROCUENTA' VALUE :OLD.NUMEROCUENTA, 'SECUENCIALIQUIDACION' VALUE :OLD.SECUENCIALIQUIDACION, 'SECUENCIAPAGO' VALUE :OLD.SECUENCIAPAGO, 'TIPOCUENTA' VALUE :OLD.TIPOCUENTA, 'TIPOPAGO' VALUE :OLD.TIPOPAGO, 'USUARIOELIMINA' VALUE :OLD.USUARIOELIMINA, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'CODIGOBANCO' VALUE :OLD.CODIGOBANCO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'cuentaBancariaAfiliadoType', v_event, v_payload, 'FCME_USER.CUENTABANCARIAAFILIADO_TYPE');
END;
/

/* --- TRG_OUTBOX_DISTRIBUCIONAFILIADO_TYPE  ON FCME_USER.DISTRIBUCIONAFILIADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DISTRIBUCIONAFILIADO_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.DISTRIBUCIONAFILIADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CIRCUITO' VALUE :NEW.CIRCUITO, 'CIUDAD' VALUE :NEW.CIUDAD, 'DISTRITO' VALUE :NEW.DISTRITO, 'ESTADODISTRIBUCIONAFILIADO' VALUE :NEW.ESTADODISTRIBUCIONAFILIADO, 'MONTOCREDITOVIGENTE' VALUE :NEW.MONTOCREDITOVIGENTE, 'MONTOCUENTAUNICA' VALUE :NEW.MONTOCUENTAUNICA, 'NOMBREPROVINCIA' VALUE :NEW.NOMBREPROVINCIA, 'NUMEROAFILIADO' VALUE :NEW.NUMEROAFILIADO, 'NUMEROAFILIADOACTUALIZADO' VALUE :NEW.NUMEROAFILIADOACTUALIZADO, 'NUMEROCADB' VALUE :NEW.NUMEROCADB, 'NUMEROCAP' VALUE :NEW.NUMEROCAP, 'NUMEROCREDITOVIGENTE' VALUE :NEW.NUMEROCREDITOVIGENTE, 'NUMERODIRECTIVONACIONAL2008' VALUE :NEW.NUMERODIRECTIVONACIONAL2008, 'NUMERODIRECTIVONACIONAL2010' VALUE :NEW.NUMERODIRECTIVONACIONAL2010, 'NUMERODIRECTIVOPROVINCIAL2008' VALUE :NEW.NUMERODIRECTIVOPROVINCIAL2008, 'NUMERODIRECTIVOPROVINCIAL2010' VALUE :NEW.NUMERODIRECTIVOPROVINCIAL2010, 'NUMEROEJECUTIVOFINANCIERO' VALUE :NEW.NUMEROEJECUTIVOFINANCIERO, 'NUMEROINSTITUCIONES' VALUE :NEW.NUMEROINSTITUCIONES, 'NUMEROLEGADOCONVENCION2008' VALUE :NEW.NUMEROLEGADOCONVENCION2008, 'NUMEROLEGADOCONVENCION2010' VALUE :NEW.NUMEROLEGADOCONVENCION2010, 'NUMEROLIDEROPINION' VALUE :NEW.NUMEROLIDEROPINION, 'NUMEROPRESIDENTEEJECUTIVO2008' VALUE :NEW.NUMEROPRESIDENTEEJECUTIVO2008, 'NUMEROPRESIDENTEEJECUTIVO2010' VALUE :NEW.NUMEROPRESIDENTEEJECUTIVO2010, 'NUMEROPRESIDENTESPROVINCIALES2008' VALUE :NEW.NUMEROPRESIDENTESPROVINCIALES2008, 'NUMEROPRESIDENTESPROVINCIALES2010' VALUE :NEW.NUMEROPRESIDENTESPROVINCIALES2010, 'NUMEROSOLOCAM' VALUE :NEW.NUMEROSOLOCAM, 'DESCRIPCIONPARROQUIA' VALUE :NEW.DESCRIPCIONPARROQUIA, 'TRABAJO' VALUE :NEW.TRABAJO, 'ZONA' VALUE :NEW.ZONA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CIRCUITO' VALUE :NEW.CIRCUITO, 'CIUDAD' VALUE :NEW.CIUDAD, 'DISTRITO' VALUE :NEW.DISTRITO, 'ESTADODISTRIBUCIONAFILIADO' VALUE :NEW.ESTADODISTRIBUCIONAFILIADO, 'MONTOCREDITOVIGENTE' VALUE :NEW.MONTOCREDITOVIGENTE, 'MONTOCUENTAUNICA' VALUE :NEW.MONTOCUENTAUNICA, 'NOMBREPROVINCIA' VALUE :NEW.NOMBREPROVINCIA, 'NUMEROAFILIADO' VALUE :NEW.NUMEROAFILIADO, 'NUMEROAFILIADOACTUALIZADO' VALUE :NEW.NUMEROAFILIADOACTUALIZADO, 'NUMEROCADB' VALUE :NEW.NUMEROCADB, 'NUMEROCAP' VALUE :NEW.NUMEROCAP, 'NUMEROCREDITOVIGENTE' VALUE :NEW.NUMEROCREDITOVIGENTE, 'NUMERODIRECTIVONACIONAL2008' VALUE :NEW.NUMERODIRECTIVONACIONAL2008, 'NUMERODIRECTIVONACIONAL2010' VALUE :NEW.NUMERODIRECTIVONACIONAL2010, 'NUMERODIRECTIVOPROVINCIAL2008' VALUE :NEW.NUMERODIRECTIVOPROVINCIAL2008, 'NUMERODIRECTIVOPROVINCIAL2010' VALUE :NEW.NUMERODIRECTIVOPROVINCIAL2010, 'NUMEROEJECUTIVOFINANCIERO' VALUE :NEW.NUMEROEJECUTIVOFINANCIERO, 'NUMEROINSTITUCIONES' VALUE :NEW.NUMEROINSTITUCIONES, 'NUMEROLEGADOCONVENCION2008' VALUE :NEW.NUMEROLEGADOCONVENCION2008, 'NUMEROLEGADOCONVENCION2010' VALUE :NEW.NUMEROLEGADOCONVENCION2010, 'NUMEROLIDEROPINION' VALUE :NEW.NUMEROLIDEROPINION, 'NUMEROPRESIDENTEEJECUTIVO2008' VALUE :NEW.NUMEROPRESIDENTEEJECUTIVO2008, 'NUMEROPRESIDENTEEJECUTIVO2010' VALUE :NEW.NUMEROPRESIDENTEEJECUTIVO2010, 'NUMEROPRESIDENTESPROVINCIALES2008' VALUE :NEW.NUMEROPRESIDENTESPROVINCIALES2008, 'NUMEROPRESIDENTESPROVINCIALES2010' VALUE :NEW.NUMEROPRESIDENTESPROVINCIALES2010, 'NUMEROSOLOCAM' VALUE :NEW.NUMEROSOLOCAM, 'DESCRIPCIONPARROQUIA' VALUE :NEW.DESCRIPCIONPARROQUIA, 'TRABAJO' VALUE :NEW.TRABAJO, 'ZONA' VALUE :NEW.ZONA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CIRCUITO' VALUE :OLD.CIRCUITO, 'CIUDAD' VALUE :OLD.CIUDAD, 'DISTRITO' VALUE :OLD.DISTRITO, 'ESTADODISTRIBUCIONAFILIADO' VALUE :OLD.ESTADODISTRIBUCIONAFILIADO, 'MONTOCREDITOVIGENTE' VALUE :OLD.MONTOCREDITOVIGENTE, 'MONTOCUENTAUNICA' VALUE :OLD.MONTOCUENTAUNICA, 'NOMBREPROVINCIA' VALUE :OLD.NOMBREPROVINCIA, 'NUMEROAFILIADO' VALUE :OLD.NUMEROAFILIADO, 'NUMEROAFILIADOACTUALIZADO' VALUE :OLD.NUMEROAFILIADOACTUALIZADO, 'NUMEROCADB' VALUE :OLD.NUMEROCADB, 'NUMEROCAP' VALUE :OLD.NUMEROCAP, 'NUMEROCREDITOVIGENTE' VALUE :OLD.NUMEROCREDITOVIGENTE, 'NUMERODIRECTIVONACIONAL2008' VALUE :OLD.NUMERODIRECTIVONACIONAL2008, 'NUMERODIRECTIVONACIONAL2010' VALUE :OLD.NUMERODIRECTIVONACIONAL2010, 'NUMERODIRECTIVOPROVINCIAL2008' VALUE :OLD.NUMERODIRECTIVOPROVINCIAL2008, 'NUMERODIRECTIVOPROVINCIAL2010' VALUE :OLD.NUMERODIRECTIVOPROVINCIAL2010, 'NUMEROEJECUTIVOFINANCIERO' VALUE :OLD.NUMEROEJECUTIVOFINANCIERO, 'NUMEROINSTITUCIONES' VALUE :OLD.NUMEROINSTITUCIONES, 'NUMEROLEGADOCONVENCION2008' VALUE :OLD.NUMEROLEGADOCONVENCION2008, 'NUMEROLEGADOCONVENCION2010' VALUE :OLD.NUMEROLEGADOCONVENCION2010, 'NUMEROLIDEROPINION' VALUE :OLD.NUMEROLIDEROPINION, 'NUMEROPRESIDENTEEJECUTIVO2008' VALUE :OLD.NUMEROPRESIDENTEEJECUTIVO2008, 'NUMEROPRESIDENTEEJECUTIVO2010' VALUE :OLD.NUMEROPRESIDENTEEJECUTIVO2010, 'NUMEROPRESIDENTESPROVINCIALES2008' VALUE :OLD.NUMEROPRESIDENTESPROVINCIALES2008, 'NUMEROPRESIDENTESPROVINCIALES2010' VALUE :OLD.NUMEROPRESIDENTESPROVINCIALES2010, 'NUMEROSOLOCAM' VALUE :OLD.NUMEROSOLOCAM, 'DESCRIPCIONPARROQUIA' VALUE :OLD.DESCRIPCIONPARROQUIA, 'TRABAJO' VALUE :OLD.TRABAJO, 'ZONA' VALUE :OLD.ZONA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'distribucionAfiliadoType', v_event, v_payload, 'FCME_USER.DISTRIBUCIONAFILIADO_TYPE');
END;
/

/* --- TRG_OUTBOX_DOCUMENTACIONAFILIADO_TYP  ON FCME_USER.DOCUMENTACIONAFILIADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DOCUMENTACIONAFILIADO_TYP
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.DOCUMENTACIONAFILIADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIADOCUMENTO' VALUE :NEW.SECUENCIADOCUMENTO, 'CODIGODOCUMENTO' VALUE :NEW.CODIGODOCUMENTO, 'FECHAFIRMADOCUMENTO' VALUE :NEW.FECHAFIRMADOCUMENTO, 'CEDULAUNIFICADA' VALUE :NEW.CEDULAUNIFICADA, 'FECHAUNIFICACION' VALUE :NEW.FECHAUNIFICACION, 'TIPOUNIFICACION' VALUE :NEW.TIPOUNIFICACION, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'FECHAFIRMACARTA' VALUE :NEW.FECHAFIRMACARTA, 'INDICADORDESCUENTOROL' VALUE :NEW.INDICADORDESCUENTOROL, 'MONTODESCUENTO' VALUE :NEW.MONTODESCUENTO, 'CODIGOTIPODOCUMENTO' VALUE :NEW.CODIGOTIPODOCUMENTO, 'FECHADOCUMENTO' VALUE :NEW.FECHADOCUMENTO, 'ESTADO' VALUE :NEW.ESTADO, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'INGRESOEGRESO' VALUE :NEW.INGRESOEGRESO, 'CODIGOTIPOINGRESOEGRESO' VALUE :NEW.CODIGOTIPOINGRESOEGRESO, 'SECUENCIAINGRESOEGRESO' VALUE :NEW.SECUENCIAINGRESOEGRESO, 'MONTOMENSUAL' VALUE :NEW.MONTOMENSUAL, 'FIJO' VALUE :NEW.FIJO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'VIAAUTORIZACIONTRATAMIENTODATOS' VALUE :NEW.VIAAUTORIZACIONTRATAMIENTODATOS, 'CODIGOUSUARIOCREA' VALUE :NEW.CODIGOUSUARIOCREA, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'NUMEROCEDULARECIBECOREO' VALUE :NEW.NUMEROCEDULARECIBECOREO, 'SECUENCIAREACTIVACIONPARTICIPE' VALUE :NEW.SECUENCIAREACTIVACIONPARTICIPE, 'INDICADORVERIFICACION' VALUE :NEW.INDICADORVERIFICACION, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'CODIGOUSUARIOPROCESO' VALUE :NEW.CODIGOUSUARIOPROCESO, 'CEDULAPROMOTORPORPROCESO' VALUE :NEW.CEDULAPROMOTORPORPROCESO, 'TIPOPROCESO' VALUE :NEW.TIPOPROCESO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'CODIGOFORMADESCUENTO' VALUE :NEW.CODIGOFORMADESCUENTO, 'CODIGOTIPOSEGUIMIENTO' VALUE :NEW.CODIGOTIPOSEGUIMIENTO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'CEDULAEJECUTIVOS' VALUE :NEW.CEDULAEJECUTIVOS, 'POSEECARTAANTIGUAAFILIACION' VALUE :NEW.POSEECARTAANTIGUAAFILIACION, 'SECUENCIAFICHAAFILIACION' VALUE :NEW.SECUENCIAFICHAAFILIACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIADOCUMENTO' VALUE :NEW.SECUENCIADOCUMENTO, 'CODIGODOCUMENTO' VALUE :NEW.CODIGODOCUMENTO, 'FECHAFIRMADOCUMENTO' VALUE :NEW.FECHAFIRMADOCUMENTO, 'CEDULAUNIFICADA' VALUE :NEW.CEDULAUNIFICADA, 'FECHAUNIFICACION' VALUE :NEW.FECHAUNIFICACION, 'TIPOUNIFICACION' VALUE :NEW.TIPOUNIFICACION, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'FECHAFIRMACARTA' VALUE :NEW.FECHAFIRMACARTA, 'INDICADORDESCUENTOROL' VALUE :NEW.INDICADORDESCUENTOROL, 'MONTODESCUENTO' VALUE :NEW.MONTODESCUENTO, 'CODIGOTIPODOCUMENTO' VALUE :NEW.CODIGOTIPODOCUMENTO, 'FECHADOCUMENTO' VALUE :NEW.FECHADOCUMENTO, 'ESTADO' VALUE :NEW.ESTADO, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'INGRESOEGRESO' VALUE :NEW.INGRESOEGRESO, 'CODIGOTIPOINGRESOEGRESO' VALUE :NEW.CODIGOTIPOINGRESOEGRESO, 'SECUENCIAINGRESOEGRESO' VALUE :NEW.SECUENCIAINGRESOEGRESO, 'MONTOMENSUAL' VALUE :NEW.MONTOMENSUAL, 'FIJO' VALUE :NEW.FIJO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'VIAAUTORIZACIONTRATAMIENTODATOS' VALUE :NEW.VIAAUTORIZACIONTRATAMIENTODATOS, 'CODIGOUSUARIOCREA' VALUE :NEW.CODIGOUSUARIOCREA, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'NUMEROCEDULARECIBECOREO' VALUE :NEW.NUMEROCEDULARECIBECOREO, 'SECUENCIAREACTIVACIONPARTICIPE' VALUE :NEW.SECUENCIAREACTIVACIONPARTICIPE, 'INDICADORVERIFICACION' VALUE :NEW.INDICADORVERIFICACION, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'CODIGOUSUARIOPROCESO' VALUE :NEW.CODIGOUSUARIOPROCESO, 'CEDULAPROMOTORPORPROCESO' VALUE :NEW.CEDULAPROMOTORPORPROCESO, 'TIPOPROCESO' VALUE :NEW.TIPOPROCESO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'CODIGOFORMADESCUENTO' VALUE :NEW.CODIGOFORMADESCUENTO, 'CODIGOTIPOSEGUIMIENTO' VALUE :NEW.CODIGOTIPOSEGUIMIENTO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'CEDULAEJECUTIVOS' VALUE :NEW.CEDULAEJECUTIVOS, 'POSEECARTAANTIGUAAFILIACION' VALUE :NEW.POSEECARTAANTIGUAAFILIACION, 'SECUENCIAFICHAAFILIACION' VALUE :NEW.SECUENCIAFICHAAFILIACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIADOCUMENTO' VALUE :OLD.SECUENCIADOCUMENTO, 'CODIGODOCUMENTO' VALUE :OLD.CODIGODOCUMENTO, 'FECHAFIRMADOCUMENTO' VALUE :OLD.FECHAFIRMADOCUMENTO, 'CEDULAUNIFICADA' VALUE :OLD.CEDULAUNIFICADA, 'FECHAUNIFICACION' VALUE :OLD.FECHAUNIFICACION, 'TIPOUNIFICACION' VALUE :OLD.TIPOUNIFICACION, 'CODIGOINSTITUCION' VALUE :OLD.CODIGOINSTITUCION, 'FECHAFIRMACARTA' VALUE :OLD.FECHAFIRMACARTA, 'INDICADORDESCUENTOROL' VALUE :OLD.INDICADORDESCUENTOROL, 'MONTODESCUENTO' VALUE :OLD.MONTODESCUENTO, 'CODIGOTIPODOCUMENTO' VALUE :OLD.CODIGOTIPODOCUMENTO, 'FECHADOCUMENTO' VALUE :OLD.FECHADOCUMENTO, 'ESTADO' VALUE :OLD.ESTADO, 'FECHAINGRESO' VALUE :OLD.FECHAINGRESO, 'INGRESOEGRESO' VALUE :OLD.INGRESOEGRESO, 'CODIGOTIPOINGRESOEGRESO' VALUE :OLD.CODIGOTIPOINGRESOEGRESO, 'SECUENCIAINGRESOEGRESO' VALUE :OLD.SECUENCIAINGRESOEGRESO, 'MONTOMENSUAL' VALUE :OLD.MONTOMENSUAL, 'FIJO' VALUE :OLD.FIJO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'VIAAUTORIZACIONTRATAMIENTODATOS' VALUE :OLD.VIAAUTORIZACIONTRATAMIENTODATOS, 'CODIGOUSUARIOCREA' VALUE :OLD.CODIGOUSUARIOCREA, 'USUARIOELIMINA' VALUE :OLD.USUARIOELIMINA, 'NUMEROCEDULARECIBECOREO' VALUE :OLD.NUMEROCEDULARECIBECOREO, 'SECUENCIAREACTIVACIONPARTICIPE' VALUE :OLD.SECUENCIAREACTIVACIONPARTICIPE, 'INDICADORVERIFICACION' VALUE :OLD.INDICADORVERIFICACION, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'CODIGOUSUARIOPROCESO' VALUE :OLD.CODIGOUSUARIOPROCESO, 'CEDULAPROMOTORPORPROCESO' VALUE :OLD.CEDULAPROMOTORPORPROCESO, 'TIPOPROCESO' VALUE :OLD.TIPOPROCESO, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'CODIGOPROCESO' VALUE :OLD.CODIGOPROCESO, 'CODIGOFORMADESCUENTO' VALUE :OLD.CODIGOFORMADESCUENTO, 'CODIGOTIPOSEGUIMIENTO' VALUE :OLD.CODIGOTIPOSEGUIMIENTO, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'USUARIOMODIFICA' VALUE :OLD.USUARIOMODIFICA, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'CEDULAEJECUTIVOS' VALUE :OLD.CEDULAEJECUTIVOS, 'POSEECARTAANTIGUAAFILIACION' VALUE :OLD.POSEECARTAANTIGUAAFILIACION, 'SECUENCIAFICHAAFILIACION' VALUE :OLD.SECUENCIAFICHAAFILIACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'documentacionAfiliadoType', v_event, v_payload, 'FCME_USER.DOCUMENTACIONAFILIADO_TYPE');
END;
/

/* --- TRG_OUTBOX_FIRMANTEPARTICIPE_TYPE  ON FCME_USER.FIRMANTEPARTICIPE_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_FIRMANTEPARTICIPE_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.FIRMANTEPARTICIPE_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'NOFIRMANTE' VALUE :NEW.NOFIRMANTE, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'SECUENCIAFIRMANTE' VALUE :NEW.SECUENCIAFIRMANTE, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'NOFIRMANTE' VALUE :NEW.NOFIRMANTE, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'SECUENCIAFIRMANTE' VALUE :NEW.SECUENCIAFIRMANTE, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOINSTITUCION' VALUE :OLD.CODIGOINSTITUCION, 'NOFIRMANTE' VALUE :OLD.NOFIRMANTE, 'NUMEROCEDULA' VALUE :OLD.NUMEROCEDULA, 'SECUENCIAFIRMANTE' VALUE :OLD.SECUENCIAFIRMANTE, 'TIPOINSTITUCION' VALUE :OLD.TIPOINSTITUCION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'firmanteParticipeType', v_event, v_payload, 'FCME_USER.FIRMANTEPARTICIPE_TYPE');
END;
/

/* --- TRG_OUTBOX_GRUPOFAMILIAR_TYPE  ON FCME_USER.GRUPOFAMILIAR_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_GRUPOFAMILIAR_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.GRUPOFAMILIAR_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'APELLIDOSGRUPOFAMILIAR' VALUE :NEW.APELLIDOSGRUPOFAMILIAR, 'CEDULAFAMILIAR' VALUE :NEW.CEDULAFAMILIAR, 'CODIGOUSUARIOINGRESOREGISTRO' VALUE :NEW.CODIGOUSUARIOINGRESOREGISTRO, 'CODIGOUSUARIOMODIFICOREGISTRO' VALUE :NEW.CODIGOUSUARIOMODIFICOREGISTRO, 'ESTADOFAMILIAR' VALUE :NEW.ESTADOFAMILIAR, 'FECHACREACIONREGISTRO' VALUE :NEW.FECHACREACIONREGISTRO, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'FECHANACIMIENTO' VALUE :NEW.FECHANACIMIENTO, 'INDICADORDISCAPACIDAD' VALUE :NEW.INDICADORDISCAPACIDAD, 'NOMBRESGRUPOFAMILIAR' VALUE :NEW.NOMBRESGRUPOFAMILIAR, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOREALCIONFAMILIAR' VALUE :NEW.TIPOREALCIONFAMILIAR, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'APELLIDOSGRUPOFAMILIAR' VALUE :NEW.APELLIDOSGRUPOFAMILIAR, 'CEDULAFAMILIAR' VALUE :NEW.CEDULAFAMILIAR, 'CODIGOUSUARIOINGRESOREGISTRO' VALUE :NEW.CODIGOUSUARIOINGRESOREGISTRO, 'CODIGOUSUARIOMODIFICOREGISTRO' VALUE :NEW.CODIGOUSUARIOMODIFICOREGISTRO, 'ESTADOFAMILIAR' VALUE :NEW.ESTADOFAMILIAR, 'FECHACREACIONREGISTRO' VALUE :NEW.FECHACREACIONREGISTRO, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'FECHANACIMIENTO' VALUE :NEW.FECHANACIMIENTO, 'INDICADORDISCAPACIDAD' VALUE :NEW.INDICADORDISCAPACIDAD, 'NOMBRESGRUPOFAMILIAR' VALUE :NEW.NOMBRESGRUPOFAMILIAR, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOREALCIONFAMILIAR' VALUE :NEW.TIPOREALCIONFAMILIAR, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'APELLIDOSGRUPOFAMILIAR' VALUE :OLD.APELLIDOSGRUPOFAMILIAR, 'CEDULAFAMILIAR' VALUE :OLD.CEDULAFAMILIAR, 'CODIGOUSUARIOINGRESOREGISTRO' VALUE :OLD.CODIGOUSUARIOINGRESOREGISTRO, 'CODIGOUSUARIOMODIFICOREGISTRO' VALUE :OLD.CODIGOUSUARIOMODIFICOREGISTRO, 'ESTADOFAMILIAR' VALUE :OLD.ESTADOFAMILIAR, 'FECHACREACIONREGISTRO' VALUE :OLD.FECHACREACIONREGISTRO, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'FECHANACIMIENTO' VALUE :OLD.FECHANACIMIENTO, 'INDICADORDISCAPACIDAD' VALUE :OLD.INDICADORDISCAPACIDAD, 'NOMBRESGRUPOFAMILIAR' VALUE :OLD.NOMBRESGRUPOFAMILIAR, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'TIPOREALCIONFAMILIAR' VALUE :OLD.TIPOREALCIONFAMILIAR, 'NUMEROCEDULA' VALUE :OLD.NUMEROCEDULA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'grupoFamiliarType', v_event, v_payload, 'FCME_USER.GRUPOFAMILIAR_TYPE');
END;
/

/* --- TRG_OUTBOX_IMAGENESTYPE  ON FCME_USER.IMAGENESTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_IMAGENESTYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.IMAGENESTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOIMAGEN' VALUE :NEW.CODIGOIMAGEN, 'NOMBREARCHIVO' VALUE :NEW.NOMBREARCHIVO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOIMAGEN' VALUE :NEW.CODIGOIMAGEN, 'NOMBREARCHIVO' VALUE :NEW.NOMBREARCHIVO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOIMAGEN' VALUE :OLD.CODIGOIMAGEN, 'NOMBREARCHIVO' VALUE :OLD.NOMBREARCHIVO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('imagenesType', v_pk, v_event, v_payload, 'FCME_USER.IMAGENESTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_INFORMACIONADICIONALAFILI  ON FCME_USER.INFORMACIONADICIONALAFILIADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_INFORMACIONADICIONALAFILI
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.INFORMACIONADICIONALAFILIADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOGENERO' VALUE :NEW.CODIGOGENERO, 'DESCRIPCIONGENERO' VALUE :NEW.DESCRIPCIONGENERO, 'ESTADOGENERO' VALUE :NEW.ESTADOGENERO, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'CODIGOPRODUCTO' VALUE :NEW.CODIGOPRODUCTO, 'ESTADODESCUENTO' VALUE :NEW.ESTADODESCUENTO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'ROLFINDESCUENTO' VALUE :NEW.ROLFINDESCUENTO, 'ROLINICIODESCUENTO' VALUE :NEW.ROLINICIODESCUENTO, 'SECUENCIADESCUENTOCAP' VALUE :NEW.SECUENCIADESCUENTOCAP, 'TIPODESCUENTOCAP' VALUE :NEW.TIPODESCUENTOCAP, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'VALORDESCUENTO' VALUE :NEW.VALORDESCUENTO, 'CODIGOELEMENTOFINANCIERO' VALUE :NEW.CODIGOELEMENTOFINANCIERO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'MONTOMONETARIOCUENTAENMENCION' VALUE :NEW.MONTOMONETARIOCUENTAENMENCION, 'CODIGOAREALABORAL' VALUE :NEW.CODIGOAREALABORAL, 'CODIGODISTRITOAMIE' VALUE :NEW.CODIGODISTRITOAMIE, 'CODIGODISTRITOMINS' VALUE :NEW.CODIGODISTRITOMINS, 'CODIGOPAISNACIONALIDAD' VALUE :NEW.CODIGOPAISNACIONALIDAD, 'DESCRIPCIONCALLEPRINCIPAL' VALUE :NEW.DESCRIPCIONCALLEPRINCIPAL, 'DESCRIPCIONCALLESECUNDARIA' VALUE :NEW.DESCRIPCIONCALLESECUNDARIA, 'DESCRIPCIONVIVIENDA' VALUE :NEW.DESCRIPCIONVIVIENDA, 'INDICADORCORRECCIONCEDULA' VALUE :NEW.INDICADORCORRECCIONCEDULA, 'NOMBRECONTACTOADICIONAL' VALUE :NEW.NOMBRECONTACTOADICIONAL, 'NUMEROCALLEPRINCIPAL' VALUE :NEW.NUMEROCALLEPRINCIPAL, 'NUMEROCALLESECUNDARIA' VALUE :NEW.NUMEROCALLESECUNDARIA, 'NUMEROMANZANA' VALUE :NEW.NUMEROMANZANA, 'NUMEROVILLA' VALUE :NEW.NUMEROVILLA, 'TELEFONOCONTACTO1' VALUE :NEW.TELEFONOCONTACTO1, 'TELEFONOCONTACTO2' VALUE :NEW.TELEFONOCONTACTO2, 'TIPOJORNADA' VALUE :NEW.TIPOJORNADA, 'TIPOOPERADORACELULAR' VALUE :NEW.TIPOOPERADORACELULAR, 'TIPORELACION' VALUE :NEW.TIPORELACION, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'ESTADOAFILIADO' VALUE :NEW.ESTADOAFILIADO, 'FECHAINGRESOMAGISTERIO' VALUE :NEW.FECHAINGRESOMAGISTERIO, 'FECHAREINGRESOFCME' VALUE :NEW.FECHAREINGRESOFCME, 'FECHARETIROFCME' VALUE :NEW.FECHARETIROFCME, 'FECHAULTIMAIMPRESIONESTADOCUENTA' VALUE :NEW.FECHAULTIMAIMPRESIONESTADOCUENTA, 'INDICADORCOBROPRESTACION' VALUE :NEW.INDICADORCOBROPRESTACION, 'SALDOQUEPASOALHISTORICO' VALUE :NEW.SALDOQUEPASOALHISTORICO, 'SALDOTRANSFERENRCIAFONDO' VALUE :NEW.SALDOTRANSFERENRCIAFONDO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOGENERO' VALUE :NEW.CODIGOGENERO, 'DESCRIPCIONGENERO' VALUE :NEW.DESCRIPCIONGENERO, 'ESTADOGENERO' VALUE :NEW.ESTADOGENERO, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'CODIGOPRODUCTO' VALUE :NEW.CODIGOPRODUCTO, 'ESTADODESCUENTO' VALUE :NEW.ESTADODESCUENTO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'ROLFINDESCUENTO' VALUE :NEW.ROLFINDESCUENTO, 'ROLINICIODESCUENTO' VALUE :NEW.ROLINICIODESCUENTO, 'SECUENCIADESCUENTOCAP' VALUE :NEW.SECUENCIADESCUENTOCAP, 'TIPODESCUENTOCAP' VALUE :NEW.TIPODESCUENTOCAP, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'VALORDESCUENTO' VALUE :NEW.VALORDESCUENTO, 'CODIGOELEMENTOFINANCIERO' VALUE :NEW.CODIGOELEMENTOFINANCIERO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'MONTOMONETARIOCUENTAENMENCION' VALUE :NEW.MONTOMONETARIOCUENTAENMENCION, 'CODIGOAREALABORAL' VALUE :NEW.CODIGOAREALABORAL, 'CODIGODISTRITOAMIE' VALUE :NEW.CODIGODISTRITOAMIE, 'CODIGODISTRITOMINS' VALUE :NEW.CODIGODISTRITOMINS, 'CODIGOPAISNACIONALIDAD' VALUE :NEW.CODIGOPAISNACIONALIDAD, 'DESCRIPCIONCALLEPRINCIPAL' VALUE :NEW.DESCRIPCIONCALLEPRINCIPAL, 'DESCRIPCIONCALLESECUNDARIA' VALUE :NEW.DESCRIPCIONCALLESECUNDARIA, 'DESCRIPCIONVIVIENDA' VALUE :NEW.DESCRIPCIONVIVIENDA, 'INDICADORCORRECCIONCEDULA' VALUE :NEW.INDICADORCORRECCIONCEDULA, 'NOMBRECONTACTOADICIONAL' VALUE :NEW.NOMBRECONTACTOADICIONAL, 'NUMEROCALLEPRINCIPAL' VALUE :NEW.NUMEROCALLEPRINCIPAL, 'NUMEROCALLESECUNDARIA' VALUE :NEW.NUMEROCALLESECUNDARIA, 'NUMEROMANZANA' VALUE :NEW.NUMEROMANZANA, 'NUMEROVILLA' VALUE :NEW.NUMEROVILLA, 'TELEFONOCONTACTO1' VALUE :NEW.TELEFONOCONTACTO1, 'TELEFONOCONTACTO2' VALUE :NEW.TELEFONOCONTACTO2, 'TIPOJORNADA' VALUE :NEW.TIPOJORNADA, 'TIPOOPERADORACELULAR' VALUE :NEW.TIPOOPERADORACELULAR, 'TIPORELACION' VALUE :NEW.TIPORELACION, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'ESTADOAFILIADO' VALUE :NEW.ESTADOAFILIADO, 'FECHAINGRESOMAGISTERIO' VALUE :NEW.FECHAINGRESOMAGISTERIO, 'FECHAREINGRESOFCME' VALUE :NEW.FECHAREINGRESOFCME, 'FECHARETIROFCME' VALUE :NEW.FECHARETIROFCME, 'FECHAULTIMAIMPRESIONESTADOCUENTA' VALUE :NEW.FECHAULTIMAIMPRESIONESTADOCUENTA, 'INDICADORCOBROPRESTACION' VALUE :NEW.INDICADORCOBROPRESTACION, 'SALDOQUEPASOALHISTORICO' VALUE :NEW.SALDOQUEPASOALHISTORICO, 'SALDOTRANSFERENRCIAFONDO' VALUE :NEW.SALDOTRANSFERENRCIAFONDO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOGENERO' VALUE :OLD.CODIGOGENERO, 'DESCRIPCIONGENERO' VALUE :OLD.DESCRIPCIONGENERO, 'ESTADOGENERO' VALUE :OLD.ESTADOGENERO, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'CODIGOPRODUCTO' VALUE :OLD.CODIGOPRODUCTO, 'ESTADODESCUENTO' VALUE :OLD.ESTADODESCUENTO, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'NUMEROCEDULA' VALUE :OLD.NUMEROCEDULA, 'ROLFINDESCUENTO' VALUE :OLD.ROLFINDESCUENTO, 'ROLINICIODESCUENTO' VALUE :OLD.ROLINICIODESCUENTO, 'SECUENCIADESCUENTOCAP' VALUE :OLD.SECUENCIADESCUENTOCAP, 'TIPODESCUENTOCAP' VALUE :OLD.TIPODESCUENTOCAP, 'USUARIOELIMINA' VALUE :OLD.USUARIOELIMINA, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'USUARIOMODIFICA' VALUE :OLD.USUARIOMODIFICA, 'VALORDESCUENTO' VALUE :OLD.VALORDESCUENTO, 'CODIGOELEMENTOFINANCIERO' VALUE :OLD.CODIGOELEMENTOFINANCIERO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'MONTOMONETARIOCUENTAENMENCION' VALUE :OLD.MONTOMONETARIOCUENTAENMENCION, 'CODIGOAREALABORAL' VALUE :OLD.CODIGOAREALABORAL, 'CODIGODISTRITOAMIE' VALUE :OLD.CODIGODISTRITOAMIE, 'CODIGODISTRITOMINS' VALUE :OLD.CODIGODISTRITOMINS, 'CODIGOPAISNACIONALIDAD' VALUE :OLD.CODIGOPAISNACIONALIDAD, 'DESCRIPCIONCALLEPRINCIPAL' VALUE :OLD.DESCRIPCIONCALLEPRINCIPAL, 'DESCRIPCIONCALLESECUNDARIA' VALUE :OLD.DESCRIPCIONCALLESECUNDARIA, 'DESCRIPCIONVIVIENDA' VALUE :OLD.DESCRIPCIONVIVIENDA, 'INDICADORCORRECCIONCEDULA' VALUE :OLD.INDICADORCORRECCIONCEDULA, 'NOMBRECONTACTOADICIONAL' VALUE :OLD.NOMBRECONTACTOADICIONAL, 'NUMEROCALLEPRINCIPAL' VALUE :OLD.NUMEROCALLEPRINCIPAL, 'NUMEROCALLESECUNDARIA' VALUE :OLD.NUMEROCALLESECUNDARIA, 'NUMEROMANZANA' VALUE :OLD.NUMEROMANZANA, 'NUMEROVILLA' VALUE :OLD.NUMEROVILLA, 'TELEFONOCONTACTO1' VALUE :OLD.TELEFONOCONTACTO1, 'TELEFONOCONTACTO2' VALUE :OLD.TELEFONOCONTACTO2, 'TIPOJORNADA' VALUE :OLD.TIPOJORNADA, 'TIPOOPERADORACELULAR' VALUE :OLD.TIPOOPERADORACELULAR, 'TIPORELACION' VALUE :OLD.TIPORELACION, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'ESTADOAFILIADO' VALUE :OLD.ESTADOAFILIADO, 'FECHAINGRESOMAGISTERIO' VALUE :OLD.FECHAINGRESOMAGISTERIO, 'FECHAREINGRESOFCME' VALUE :OLD.FECHAREINGRESOFCME, 'FECHARETIROFCME' VALUE :OLD.FECHARETIROFCME, 'FECHAULTIMAIMPRESIONESTADOCUENTA' VALUE :OLD.FECHAULTIMAIMPRESIONESTADOCUENTA, 'INDICADORCOBROPRESTACION' VALUE :OLD.INDICADORCOBROPRESTACION, 'SALDOQUEPASOALHISTORICO' VALUE :OLD.SALDOQUEPASOALHISTORICO, 'SALDOTRANSFERENRCIAFONDO' VALUE :OLD.SALDOTRANSFERENRCIAFONDO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'informacionAdicionalAfiliadoType', v_event, v_payload, 'FCME_USER.INFORMACIONADICIONALAFILIADO_TYPE');
END;
/

/* --- TRG_OUTBOX_INSTITUCION_TYPE  ON FCME_USER.INSTITUCION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_INSTITUCION_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.INSTITUCION_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'NOMBREINSTITUCION' VALUE :NEW.NOMBREINSTITUCION, 'RUCINSTITUCION' VALUE :NEW.RUCINSTITUCION, 'CODIGOTIPOINSTITUCION' VALUE :NEW.CODIGOTIPOINSTITUCION, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOCIUDAD' VALUE :NEW.CODIGOCIUDAD, 'DIRECCION' VALUE :NEW.DIRECCION, 'TELEFONO' VALUE :NEW.TELEFONO, 'REPRESENTANTELEGAL' VALUE :NEW.REPRESENTANTELEGAL, 'CORREOELECTRONICO' VALUE :NEW.CORREOELECTRONICO, 'ESTADO' VALUE :NEW.ESTADO, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'CODIGOCONVENIO' VALUE :NEW.CODIGOCONVENIO, 'INDICADORDESCUENTOROL' VALUE :NEW.INDICADORDESCUENTOROL, 'CODIGOPARROQUIA' VALUE :NEW.CODIGOPARROQUIA, 'CODIGOAMIE' VALUE :NEW.CODIGOAMIE, 'CODIGODISTRITO' VALUE :NEW.CODIGODISTRITO, 'CODIGOCIRCUITO' VALUE :NEW.CODIGOCIRCUITO, 'CODIGOSECTOR' VALUE :NEW.CODIGOSECTOR, 'TIPOSOSTENIMIENTO' VALUE :NEW.TIPOSOSTENIMIENTO, 'NIVEL' VALUE :NEW.NIVEL, 'JORNADACLASES' VALUE :NEW.JORNADACLASES, 'NUMEROPATRONAL' VALUE :NEW.NUMEROPATRONAL, 'NUMERORUC' VALUE :NEW.NUMERORUC, 'MAILINSTITUCION' VALUE :NEW.MAILINSTITUCION, 'NUMEROCUENTABANCOCENTRAL' VALUE :NEW.NUMEROCUENTABANCOCENTRAL, 'INDICADORDESCUENTOBCE' VALUE :NEW.INDICADORDESCUENTOBCE, 'INDICADORINSTITUCIONMUNICIPAL' VALUE :NEW.INDICADORINSTITUCIONMUNICIPAL, 'NOMBRECOLECTOR' VALUE :NEW.NOMBRECOLECTOR, 'CEDULACOLECTOR' VALUE :NEW.CEDULACOLECTOR, 'TELEFONOCOLECTOR' VALUE :NEW.TELEFONOCOLECTOR, 'DIRECCIONCOLECTOR' VALUE :NEW.DIRECCIONCOLECTOR, 'PORCENTAJECAM' VALUE :NEW.PORCENTAJECAM, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'NUMEROTELEFONO' VALUE :NEW.NUMEROTELEFONO, 'CODIGOPROVINCIACOLECTOR' VALUE :NEW.CODIGOPROVINCIACOLECTOR, 'CIUDADCOLECTOR' VALUE :NEW.CIUDADCOLECTOR, 'CODIGOPARROQUIACOLECTOR' VALUE :NEW.CODIGOPARROQUIACOLECTOR, 'NUMEROTELEFONOCOLECTOR' VALUE :NEW.NUMEROTELEFONOCOLECTOR, 'TIPODIRECCIONENTREGALISTADOS' VALUE :NEW.TIPODIRECCIONENTREGALISTADOS, 'TIPODIRECCIONPAGOS' VALUE :NEW.TIPODIRECCIONPAGOS, 'INDICADORIMPRESIONESTADOCUENTA' VALUE :NEW.INDICADORIMPRESIONESTADOCUENTA, 'CODIGOSEGUNELSINEC' VALUE :NEW.CODIGOSEGUNELSINEC, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'EMAILCOLECTOR' VALUE :NEW.EMAILCOLECTOR, 'DIRECCIONPROVINCIALQUECORRESPONDE' VALUE :NEW.DIRECCIONPROVINCIALQUECORRESPONDE, 'NUMEROUTE' VALUE :NEW.NUMEROUTE, 'NUMEROZONA' VALUE :NEW.NUMEROZONA, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'TIENECONTRATOBCEPARARECAUDACION' VALUE :NEW.TIENECONTRATOBCEPARARECAUDACION, 'FECHADIRMACONTRATO' VALUE :NEW.FECHADIRMACONTRATO, 'INDICADORCONFIRMACIONBANCOCENTRAL' VALUE :NEW.INDICADORCONFIRMACIONBANCOCENTRAL, 'ROLACTUALIZACIONINSTITUCION' VALUE :NEW.ROLACTUALIZACIONINSTITUCION, 'FECHAULTIMAACTUALIZACION' VALUE :NEW.FECHAULTIMAACTUALIZACION, 'HORAULTIMAACTUALIZACION' VALUE :NEW.HORAULTIMAACTUALIZACION, 'CODIGOUNIDADEJECUTORA' VALUE :NEW.CODIGOUNIDADEJECUTORA, 'NUMEROCUENTAROTATIVAINGRESO' VALUE :NEW.NUMEROCUENTAROTATIVAINGRESO, 'TIPOCUENTAROTATIVAINGRESO' VALUE :NEW.TIPOCUENTAROTATIVAINGRESO, 'CODIGOBANCOCUENTAROTATIVAINGRESO' VALUE :NEW.CODIGOBANCOCUENTAROTATIVAINGRESO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION, 'TELEFONOCONVENCIONALINSTITUCION' VALUE :NEW.TELEFONOCONVENCIONALINSTITUCION, 'CEDULAREPRESENTANTE' VALUE :NEW.CEDULAREPRESENTANTE, 'NOMBREREPRESENTANTE' VALUE :NEW.NOMBREREPRESENTANTE, 'MAILREPRESENTANTE' VALUE :NEW.MAILREPRESENTANTE, 'TELEFONOREPRESENTANTE' VALUE :NEW.TELEFONOREPRESENTANTE, 'TIPOACCESO' VALUE :NEW.TIPOACCESO, 'NUMERODOCENTE' VALUE :NEW.NUMERODOCENTE, 'NUMEROBONIFICACION' VALUE :NEW.NUMEROBONIFICACION, 'NUMEROADMINISTRADOR' VALUE :NEW.NUMEROADMINISTRADOR, 'NUMEROALUMNOS' VALUE :NEW.NUMEROALUMNOS, 'CODIGOCIRCUITOMINISTERIOEDUCACION' VALUE :NEW.CODIGOCIRCUITOMINISTERIOEDUCACION, 'CODIGODISTRITOMINISTERIOEDUCACION' VALUE :NEW.CODIGODISTRITOMINISTERIOEDUCACION, 'CODIGOMODALIDAD' VALUE :NEW.CODIGOMODALIDAD, 'CODIGOETNIA' VALUE :NEW.CODIGOETNIA, 'CODIGONACIONALIDAD' VALUE :NEW.CODIGONACIONALIDAD, 'TIPOEDUCACIONMINISTERIOEDUCACION' VALUE :NEW.TIPOEDUCACIONMINISTERIOEDUCACION, 'CODIGOZONAMINISTERIOEDUCACION' VALUE :NEW.CODIGOZONAMINISTERIOEDUCACION, 'UNIDADADMINISTRATIVACIRCUITAL' VALUE :NEW.UNIDADADMINISTRATIVACIRCUITAL, 'CODIGOMOTIVOMODIFICACIONINSTITUCION' VALUE :NEW.CODIGOMOTIVOMODIFICACIONINSTITUCION, 'CODIGOREGISTROESCOLAR' VALUE :NEW.CODIGOREGISTROESCOLAR);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'NOMBREINSTITUCION' VALUE :NEW.NOMBREINSTITUCION, 'RUCINSTITUCION' VALUE :NEW.RUCINSTITUCION, 'CODIGOTIPOINSTITUCION' VALUE :NEW.CODIGOTIPOINSTITUCION, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOCIUDAD' VALUE :NEW.CODIGOCIUDAD, 'DIRECCION' VALUE :NEW.DIRECCION, 'TELEFONO' VALUE :NEW.TELEFONO, 'REPRESENTANTELEGAL' VALUE :NEW.REPRESENTANTELEGAL, 'CORREOELECTRONICO' VALUE :NEW.CORREOELECTRONICO, 'ESTADO' VALUE :NEW.ESTADO, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'CODIGOCONVENIO' VALUE :NEW.CODIGOCONVENIO, 'INDICADORDESCUENTOROL' VALUE :NEW.INDICADORDESCUENTOROL, 'CODIGOPARROQUIA' VALUE :NEW.CODIGOPARROQUIA, 'CODIGOAMIE' VALUE :NEW.CODIGOAMIE, 'CODIGODISTRITO' VALUE :NEW.CODIGODISTRITO, 'CODIGOCIRCUITO' VALUE :NEW.CODIGOCIRCUITO, 'CODIGOSECTOR' VALUE :NEW.CODIGOSECTOR, 'TIPOSOSTENIMIENTO' VALUE :NEW.TIPOSOSTENIMIENTO, 'NIVEL' VALUE :NEW.NIVEL, 'JORNADACLASES' VALUE :NEW.JORNADACLASES, 'NUMEROPATRONAL' VALUE :NEW.NUMEROPATRONAL, 'NUMERORUC' VALUE :NEW.NUMERORUC, 'MAILINSTITUCION' VALUE :NEW.MAILINSTITUCION, 'NUMEROCUENTABANCOCENTRAL' VALUE :NEW.NUMEROCUENTABANCOCENTRAL, 'INDICADORDESCUENTOBCE' VALUE :NEW.INDICADORDESCUENTOBCE, 'INDICADORINSTITUCIONMUNICIPAL' VALUE :NEW.INDICADORINSTITUCIONMUNICIPAL, 'NOMBRECOLECTOR' VALUE :NEW.NOMBRECOLECTOR, 'CEDULACOLECTOR' VALUE :NEW.CEDULACOLECTOR, 'TELEFONOCOLECTOR' VALUE :NEW.TELEFONOCOLECTOR, 'DIRECCIONCOLECTOR' VALUE :NEW.DIRECCIONCOLECTOR, 'PORCENTAJECAM' VALUE :NEW.PORCENTAJECAM, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'NUMEROTELEFONO' VALUE :NEW.NUMEROTELEFONO, 'CODIGOPROVINCIACOLECTOR' VALUE :NEW.CODIGOPROVINCIACOLECTOR, 'CIUDADCOLECTOR' VALUE :NEW.CIUDADCOLECTOR, 'CODIGOPARROQUIACOLECTOR' VALUE :NEW.CODIGOPARROQUIACOLECTOR, 'NUMEROTELEFONOCOLECTOR' VALUE :NEW.NUMEROTELEFONOCOLECTOR, 'TIPODIRECCIONENTREGALISTADOS' VALUE :NEW.TIPODIRECCIONENTREGALISTADOS, 'TIPODIRECCIONPAGOS' VALUE :NEW.TIPODIRECCIONPAGOS, 'INDICADORIMPRESIONESTADOCUENTA' VALUE :NEW.INDICADORIMPRESIONESTADOCUENTA, 'CODIGOSEGUNELSINEC' VALUE :NEW.CODIGOSEGUNELSINEC, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'EMAILCOLECTOR' VALUE :NEW.EMAILCOLECTOR, 'DIRECCIONPROVINCIALQUECORRESPONDE' VALUE :NEW.DIRECCIONPROVINCIALQUECORRESPONDE, 'NUMEROUTE' VALUE :NEW.NUMEROUTE, 'NUMEROZONA' VALUE :NEW.NUMEROZONA, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'TIENECONTRATOBCEPARARECAUDACION' VALUE :NEW.TIENECONTRATOBCEPARARECAUDACION, 'FECHADIRMACONTRATO' VALUE :NEW.FECHADIRMACONTRATO, 'INDICADORCONFIRMACIONBANCOCENTRAL' VALUE :NEW.INDICADORCONFIRMACIONBANCOCENTRAL, 'ROLACTUALIZACIONINSTITUCION' VALUE :NEW.ROLACTUALIZACIONINSTITUCION, 'FECHAULTIMAACTUALIZACION' VALUE :NEW.FECHAULTIMAACTUALIZACION, 'HORAULTIMAACTUALIZACION' VALUE :NEW.HORAULTIMAACTUALIZACION, 'CODIGOUNIDADEJECUTORA' VALUE :NEW.CODIGOUNIDADEJECUTORA, 'NUMEROCUENTAROTATIVAINGRESO' VALUE :NEW.NUMEROCUENTAROTATIVAINGRESO, 'TIPOCUENTAROTATIVAINGRESO' VALUE :NEW.TIPOCUENTAROTATIVAINGRESO, 'CODIGOBANCOCUENTAROTATIVAINGRESO' VALUE :NEW.CODIGOBANCOCUENTAROTATIVAINGRESO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION, 'TELEFONOCONVENCIONALINSTITUCION' VALUE :NEW.TELEFONOCONVENCIONALINSTITUCION, 'CEDULAREPRESENTANTE' VALUE :NEW.CEDULAREPRESENTANTE, 'NOMBREREPRESENTANTE' VALUE :NEW.NOMBREREPRESENTANTE, 'MAILREPRESENTANTE' VALUE :NEW.MAILREPRESENTANTE, 'TELEFONOREPRESENTANTE' VALUE :NEW.TELEFONOREPRESENTANTE, 'TIPOACCESO' VALUE :NEW.TIPOACCESO, 'NUMERODOCENTE' VALUE :NEW.NUMERODOCENTE, 'NUMEROBONIFICACION' VALUE :NEW.NUMEROBONIFICACION, 'NUMEROADMINISTRADOR' VALUE :NEW.NUMEROADMINISTRADOR, 'NUMEROALUMNOS' VALUE :NEW.NUMEROALUMNOS, 'CODIGOCIRCUITOMINISTERIOEDUCACION' VALUE :NEW.CODIGOCIRCUITOMINISTERIOEDUCACION, 'CODIGODISTRITOMINISTERIOEDUCACION' VALUE :NEW.CODIGODISTRITOMINISTERIOEDUCACION, 'CODIGOMODALIDAD' VALUE :NEW.CODIGOMODALIDAD, 'CODIGOETNIA' VALUE :NEW.CODIGOETNIA, 'CODIGONACIONALIDAD' VALUE :NEW.CODIGONACIONALIDAD, 'TIPOEDUCACIONMINISTERIOEDUCACION' VALUE :NEW.TIPOEDUCACIONMINISTERIOEDUCACION, 'CODIGOZONAMINISTERIOEDUCACION' VALUE :NEW.CODIGOZONAMINISTERIOEDUCACION, 'UNIDADADMINISTRATIVACIRCUITAL' VALUE :NEW.UNIDADADMINISTRATIVACIRCUITAL, 'CODIGOMOTIVOMODIFICACIONINSTITUCION' VALUE :NEW.CODIGOMOTIVOMODIFICACIONINSTITUCION, 'CODIGOREGISTROESCOLAR' VALUE :NEW.CODIGOREGISTROESCOLAR);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOINSTITUCION' VALUE :OLD.CODIGOINSTITUCION, 'NOMBREINSTITUCION' VALUE :OLD.NOMBREINSTITUCION, 'RUCINSTITUCION' VALUE :OLD.RUCINSTITUCION, 'CODIGOTIPOINSTITUCION' VALUE :OLD.CODIGOTIPOINSTITUCION, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'CODIGOCIUDAD' VALUE :OLD.CODIGOCIUDAD, 'DIRECCION' VALUE :OLD.DIRECCION, 'TELEFONO' VALUE :OLD.TELEFONO, 'REPRESENTANTELEGAL' VALUE :OLD.REPRESENTANTELEGAL, 'CORREOELECTRONICO' VALUE :OLD.CORREOELECTRONICO, 'ESTADO' VALUE :OLD.ESTADO, 'FECHAINGRESO' VALUE :OLD.FECHAINGRESO, 'CODIGOCONVENIO' VALUE :OLD.CODIGOCONVENIO, 'INDICADORDESCUENTOROL' VALUE :OLD.INDICADORDESCUENTOROL, 'CODIGOPARROQUIA' VALUE :OLD.CODIGOPARROQUIA, 'CODIGOAMIE' VALUE :OLD.CODIGOAMIE, 'CODIGODISTRITO' VALUE :OLD.CODIGODISTRITO, 'CODIGOCIRCUITO' VALUE :OLD.CODIGOCIRCUITO, 'CODIGOSECTOR' VALUE :OLD.CODIGOSECTOR, 'TIPOSOSTENIMIENTO' VALUE :OLD.TIPOSOSTENIMIENTO, 'NIVEL' VALUE :OLD.NIVEL, 'JORNADACLASES' VALUE :OLD.JORNADACLASES, 'NUMEROPATRONAL' VALUE :OLD.NUMEROPATRONAL, 'NUMERORUC' VALUE :OLD.NUMERORUC, 'MAILINSTITUCION' VALUE :OLD.MAILINSTITUCION, 'NUMEROCUENTABANCOCENTRAL' VALUE :OLD.NUMEROCUENTABANCOCENTRAL, 'INDICADORDESCUENTOBCE' VALUE :OLD.INDICADORDESCUENTOBCE, 'INDICADORINSTITUCIONMUNICIPAL' VALUE :OLD.INDICADORINSTITUCIONMUNICIPAL, 'NOMBRECOLECTOR' VALUE :OLD.NOMBRECOLECTOR, 'CEDULACOLECTOR' VALUE :OLD.CEDULACOLECTOR, 'TELEFONOCOLECTOR' VALUE :OLD.TELEFONOCOLECTOR, 'DIRECCIONCOLECTOR' VALUE :OLD.DIRECCIONCOLECTOR, 'PORCENTAJECAM' VALUE :OLD.PORCENTAJECAM, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'NUMEROTELEFONO' VALUE :OLD.NUMEROTELEFONO, 'CODIGOPROVINCIACOLECTOR' VALUE :OLD.CODIGOPROVINCIACOLECTOR, 'CIUDADCOLECTOR' VALUE :OLD.CIUDADCOLECTOR, 'CODIGOPARROQUIACOLECTOR' VALUE :OLD.CODIGOPARROQUIACOLECTOR, 'NUMEROTELEFONOCOLECTOR' VALUE :OLD.NUMEROTELEFONOCOLECTOR, 'TIPODIRECCIONENTREGALISTADOS' VALUE :OLD.TIPODIRECCIONENTREGALISTADOS, 'TIPODIRECCIONPAGOS' VALUE :OLD.TIPODIRECCIONPAGOS, 'INDICADORIMPRESIONESTADOCUENTA' VALUE :OLD.INDICADORIMPRESIONESTADOCUENTA, 'CODIGOSEGUNELSINEC' VALUE :OLD.CODIGOSEGUNELSINEC, 'USUARIOMODIFICA' VALUE :OLD.USUARIOMODIFICA, 'EMAILCOLECTOR' VALUE :OLD.EMAILCOLECTOR, 'DIRECCIONPROVINCIALQUECORRESPONDE' VALUE :OLD.DIRECCIONPROVINCIALQUECORRESPONDE, 'NUMEROUTE' VALUE :OLD.NUMEROUTE, 'NUMEROZONA' VALUE :OLD.NUMEROZONA, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'TIENECONTRATOBCEPARARECAUDACION' VALUE :OLD.TIENECONTRATOBCEPARARECAUDACION, 'FECHADIRMACONTRATO' VALUE :OLD.FECHADIRMACONTRATO, 'INDICADORCONFIRMACIONBANCOCENTRAL' VALUE :OLD.INDICADORCONFIRMACIONBANCOCENTRAL, 'ROLACTUALIZACIONINSTITUCION' VALUE :OLD.ROLACTUALIZACIONINSTITUCION, 'FECHAULTIMAACTUALIZACION' VALUE :OLD.FECHAULTIMAACTUALIZACION, 'HORAULTIMAACTUALIZACION' VALUE :OLD.HORAULTIMAACTUALIZACION, 'CODIGOUNIDADEJECUTORA' VALUE :OLD.CODIGOUNIDADEJECUTORA, 'NUMEROCUENTAROTATIVAINGRESO' VALUE :OLD.NUMEROCUENTAROTATIVAINGRESO, 'TIPOCUENTAROTATIVAINGRESO' VALUE :OLD.TIPOCUENTAROTATIVAINGRESO, 'CODIGOBANCOCUENTAROTATIVAINGRESO' VALUE :OLD.CODIGOBANCOCUENTAROTATIVAINGRESO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'TIPOINSTITUCION' VALUE :OLD.TIPOINSTITUCION, 'TELEFONOCONVENCIONALINSTITUCION' VALUE :OLD.TELEFONOCONVENCIONALINSTITUCION, 'CEDULAREPRESENTANTE' VALUE :OLD.CEDULAREPRESENTANTE, 'NOMBREREPRESENTANTE' VALUE :OLD.NOMBREREPRESENTANTE, 'MAILREPRESENTANTE' VALUE :OLD.MAILREPRESENTANTE, 'TELEFONOREPRESENTANTE' VALUE :OLD.TELEFONOREPRESENTANTE, 'TIPOACCESO' VALUE :OLD.TIPOACCESO, 'NUMERODOCENTE' VALUE :OLD.NUMERODOCENTE, 'NUMEROBONIFICACION' VALUE :OLD.NUMEROBONIFICACION, 'NUMEROADMINISTRADOR' VALUE :OLD.NUMEROADMINISTRADOR, 'NUMEROALUMNOS' VALUE :OLD.NUMEROALUMNOS, 'CODIGOCIRCUITOMINISTERIOEDUCACION' VALUE :OLD.CODIGOCIRCUITOMINISTERIOEDUCACION, 'CODIGODISTRITOMINISTERIOEDUCACION' VALUE :OLD.CODIGODISTRITOMINISTERIOEDUCACION, 'CODIGOMODALIDAD' VALUE :OLD.CODIGOMODALIDAD, 'CODIGOETNIA' VALUE :OLD.CODIGOETNIA, 'CODIGONACIONALIDAD' VALUE :OLD.CODIGONACIONALIDAD, 'TIPOEDUCACIONMINISTERIOEDUCACION' VALUE :OLD.TIPOEDUCACIONMINISTERIOEDUCACION, 'CODIGOZONAMINISTERIOEDUCACION' VALUE :OLD.CODIGOZONAMINISTERIOEDUCACION, 'UNIDADADMINISTRATIVACIRCUITAL' VALUE :OLD.UNIDADADMINISTRATIVACIRCUITAL, 'CODIGOMOTIVOMODIFICACIONINSTITUCION' VALUE :OLD.CODIGOMOTIVOMODIFICACIONINSTITUCION, 'CODIGOREGISTROESCOLAR' VALUE :OLD.CODIGOREGISTROESCOLAR);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'institucionType', v_event, v_payload, 'FCME_USER.INSTITUCION_TYPE');
END;
/

/* --- TRG_OUTBOX_JURIDICOINFORMACION  ON FCME_USER.JURIDICOINFORMACIONBASICATYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_JURIDICOINFORMACION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.JURIDICOINFORMACIONBASICATYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('juridicoInformacionBasicaType', v_pk, v_event, v_payload, 'FCME_USER.JURIDICOINFORMACIONBASICATYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_MOTIVOCONTABLE_TYPE  ON FCME_USER.MOTIVOCONTABLE_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_MOTIVOCONTABLE_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.MOTIVOCONTABLE_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CUENTAAUTOMATICADEBE' VALUE :NEW.CUENTAAUTOMATICADEBE, 'CUENTAAUTOMATICAHABER' VALUE :NEW.CUENTAAUTOMATICAHABER, 'MOTIVO' VALUE :NEW.MOTIVO, 'RUBROROL' VALUE :NEW.RUBROROL, 'TIPOTRANSACCION' VALUE :NEW.TIPOTRANSACCION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CUENTAAUTOMATICADEBE' VALUE :NEW.CUENTAAUTOMATICADEBE, 'CUENTAAUTOMATICAHABER' VALUE :NEW.CUENTAAUTOMATICAHABER, 'MOTIVO' VALUE :NEW.MOTIVO, 'RUBROROL' VALUE :NEW.RUBROROL, 'TIPOTRANSACCION' VALUE :NEW.TIPOTRANSACCION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'CUENTAAUTOMATICADEBE' VALUE :OLD.CUENTAAUTOMATICADEBE, 'CUENTAAUTOMATICAHABER' VALUE :OLD.CUENTAAUTOMATICAHABER, 'MOTIVO' VALUE :OLD.MOTIVO, 'RUBROROL' VALUE :OLD.RUBROROL, 'TIPOTRANSACCION' VALUE :OLD.TIPOTRANSACCION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'motivoContableType', v_event, v_payload, 'FCME_USER.MOTIVOCONTABLE_TYPE');
END;
/

/* --- TRG_OUTBOX_MOVIMIENTOCUENTA_TYPE  ON FCME_USER.MOVIMIENTOCUENTA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_MOVIMIENTOCUENTA_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.MOVIMIENTOCUENTA_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAMOVIMIENTO' VALUE :NEW.SECUENCIAMOVIMIENTO, 'CODIGOTIPOMOVIMIENTO' VALUE :NEW.CODIGOTIPOMOVIMIENTO, 'CODIGOMOTIVO' VALUE :NEW.CODIGOMOTIVO, 'FECHAMOVIMIENTO' VALUE :NEW.FECHAMOVIMIENTO, 'FECHAPROCESO' VALUE :NEW.FECHAPROCESO, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOCUENTACONTABLE' VALUE :NEW.CODIGOCUENTACONTABLE, 'DESCRIPCION' VALUE :NEW.DESCRIPCION, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'ESTADO' VALUE :NEW.ESTADO, 'CUENTABANCARIA' VALUE :NEW.CUENTABANCARIA, 'CUENTABANCODESTINO' VALUE :NEW.CUENTABANCODESTINO, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'NOMBREBENEFICIARIOPAGO' VALUE :NEW.NOMBREBENEFICIARIOPAGO, 'TIPOCUENTA' VALUE :NEW.TIPOCUENTA, 'TIPOTRANSACCION' VALUE :NEW.TIPOTRANSACCION, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOMOTIVORETIRO' VALUE :NEW.CODIGOMOTIVORETIRO, 'FECHAINICIOAJUSTE' VALUE :NEW.FECHAINICIOAJUSTE, 'FECHAFINAJUSTE' VALUE :NEW.FECHAFINAJUSTE, 'FECHARETIROFCME' VALUE :NEW.FECHARETIROFCME, 'PORCENTAJEDISTRIBUCIONVALORES' VALUE :NEW.PORCENTAJEDISTRIBUCIONVALORES, 'SALDOANTERIOR' VALUE :NEW.SALDOANTERIOR, 'FECHACREACIONREGISTRO' VALUE :NEW.FECHACREACIONREGISTRO, 'INDICADORMOVIMIENTOCAPITALIZADO' VALUE :NEW.INDICADORMOVIMIENTOCAPITALIZADO, 'HORAGENERACIONMOVIMIENTO' VALUE :NEW.HORAGENERACIONMOVIMIENTO, 'TIPOCOMPROBANTECONTABLE' VALUE :NEW.TIPOCOMPROBANTECONTABLE, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOPAGADOR' VALUE :NEW.CODIGOPAGADOR, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAMOVIMIENTO' VALUE :NEW.SECUENCIAMOVIMIENTO, 'CODIGOTIPOMOVIMIENTO' VALUE :NEW.CODIGOTIPOMOVIMIENTO, 'CODIGOMOTIVO' VALUE :NEW.CODIGOMOTIVO, 'FECHAMOVIMIENTO' VALUE :NEW.FECHAMOVIMIENTO, 'FECHAPROCESO' VALUE :NEW.FECHAPROCESO, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOCUENTACONTABLE' VALUE :NEW.CODIGOCUENTACONTABLE, 'DESCRIPCION' VALUE :NEW.DESCRIPCION, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'ESTADO' VALUE :NEW.ESTADO, 'CUENTABANCARIA' VALUE :NEW.CUENTABANCARIA, 'CUENTABANCODESTINO' VALUE :NEW.CUENTABANCODESTINO, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'NOMBREBENEFICIARIOPAGO' VALUE :NEW.NOMBREBENEFICIARIOPAGO, 'TIPOCUENTA' VALUE :NEW.TIPOCUENTA, 'TIPOTRANSACCION' VALUE :NEW.TIPOTRANSACCION, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOMOTIVORETIRO' VALUE :NEW.CODIGOMOTIVORETIRO, 'FECHAINICIOAJUSTE' VALUE :NEW.FECHAINICIOAJUSTE, 'FECHAFINAJUSTE' VALUE :NEW.FECHAFINAJUSTE, 'FECHARETIROFCME' VALUE :NEW.FECHARETIROFCME, 'PORCENTAJEDISTRIBUCIONVALORES' VALUE :NEW.PORCENTAJEDISTRIBUCIONVALORES, 'SALDOANTERIOR' VALUE :NEW.SALDOANTERIOR, 'FECHACREACIONREGISTRO' VALUE :NEW.FECHACREACIONREGISTRO, 'INDICADORMOVIMIENTOCAPITALIZADO' VALUE :NEW.INDICADORMOVIMIENTOCAPITALIZADO, 'HORAGENERACIONMOVIMIENTO' VALUE :NEW.HORAGENERACIONMOVIMIENTO, 'TIPOCOMPROBANTECONTABLE' VALUE :NEW.TIPOCOMPROBANTECONTABLE, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOPAGADOR' VALUE :NEW.CODIGOPAGADOR, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIAMOVIMIENTO' VALUE :OLD.SECUENCIAMOVIMIENTO, 'CODIGOTIPOMOVIMIENTO' VALUE :OLD.CODIGOTIPOMOVIMIENTO, 'CODIGOMOTIVO' VALUE :OLD.CODIGOMOTIVO, 'FECHAMOVIMIENTO' VALUE :OLD.FECHAMOVIMIENTO, 'FECHAPROCESO' VALUE :OLD.FECHAPROCESO, 'MONTOMOVIMIENTO' VALUE :OLD.MONTOMOVIMIENTO, 'CODIGORUBRO' VALUE :OLD.CODIGORUBRO, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'CODIGOCUENTACONTABLE' VALUE :OLD.CODIGOCUENTACONTABLE, 'DESCRIPCION' VALUE :OLD.DESCRIPCION, 'CODIGOUSUARIO' VALUE :OLD.CODIGOUSUARIO, 'ESTADO' VALUE :OLD.ESTADO, 'CUENTABANCARIA' VALUE :OLD.CUENTABANCARIA, 'CUENTABANCODESTINO' VALUE :OLD.CUENTABANCODESTINO, 'TIPOPAGO' VALUE :OLD.TIPOPAGO, 'NOMBREBENEFICIARIOPAGO' VALUE :OLD.NOMBREBENEFICIARIOPAGO, 'TIPOCUENTA' VALUE :OLD.TIPOCUENTA, 'TIPOTRANSACCION' VALUE :OLD.TIPOTRANSACCION, 'CODIGOINSTITUCION' VALUE :OLD.CODIGOINSTITUCION, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'CODIGOMOTIVORETIRO' VALUE :OLD.CODIGOMOTIVORETIRO, 'FECHAINICIOAJUSTE' VALUE :OLD.FECHAINICIOAJUSTE, 'FECHAFINAJUSTE' VALUE :OLD.FECHAFINAJUSTE, 'FECHARETIROFCME' VALUE :OLD.FECHARETIROFCME, 'PORCENTAJEDISTRIBUCIONVALORES' VALUE :OLD.PORCENTAJEDISTRIBUCIONVALORES, 'SALDOANTERIOR' VALUE :OLD.SALDOANTERIOR, 'FECHACREACIONREGISTRO' VALUE :OLD.FECHACREACIONREGISTRO, 'INDICADORMOVIMIENTOCAPITALIZADO' VALUE :OLD.INDICADORMOVIMIENTOCAPITALIZADO, 'HORAGENERACIONMOVIMIENTO' VALUE :OLD.HORAGENERACIONMOVIMIENTO, 'TIPOCOMPROBANTECONTABLE' VALUE :OLD.TIPOCOMPROBANTECONTABLE, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'CODIGOPAGADOR' VALUE :OLD.CODIGOPAGADOR, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'movimientoCuentaType', v_event, v_payload, 'FCME_USER.MOVIMIENTOCUENTA_TYPE');
END;
/

/* --- TRG_OUTBOX_MOVIMIENTOTEMPORAL_TYPE  ON FCME_USER.MOVIMIENTOTEMPORAL_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_MOVIMIENTOTEMPORAL_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.MOVIMIENTOTEMPORAL_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'CODIGOMOTIVORETIRO' VALUE :NEW.CODIGOMOTIVORETIRO, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'CODIGOUSUARIOVERIFICA' VALUE :NEW.CODIGOUSUARIOVERIFICA, 'DESCRIPCIONMOVIMIENTO' VALUE :NEW.DESCRIPCIONMOVIMIENTO, 'ESTADOMOVIMIENTO' VALUE :NEW.ESTADOMOVIMIENTO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHACREACIONREGISTRO' VALUE :NEW.FECHACREACIONREGISTRO, 'FECHAFINAJUSTE' VALUE :NEW.FECHAFINAJUSTE, 'FECHAINICIOAJUSTE' VALUE :NEW.FECHAINICIOAJUSTE, 'FECHAPROCESO' VALUE :NEW.FECHAPROCESO, 'FECHARETIROFCME' VALUE :NEW.FECHARETIROFCME, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'HORAGENERACIONMOVIMIENTO' VALUE :NEW.HORAGENERACIONMOVIMIENTO, 'INDICADORMOVIMIENTOCAPITALIZADO' VALUE :NEW.INDICADORMOVIMIENTOCAPITALIZADO, 'INDICADORMOVIMIENTOIMPRESO' VALUE :NEW.INDICADORMOVIMIENTOIMPRESO, 'INDICADORTIPOPROCESO' VALUE :NEW.INDICADORTIPOPROCESO, 'MOTIVO' VALUE :NEW.MOTIVO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'NUMEROTRANSACCION' VALUE :NEW.NUMEROTRANSACCION, 'CODIGOPAGADOR' VALUE :NEW.CODIGOPAGADOR, 'PORCENTAJEDISTRIBUCIONVALORES' VALUE :NEW.PORCENTAJEDISTRIBUCIONVALORES, 'RUBROROL' VALUE :NEW.RUBROROL, 'SALDOANTERIOR' VALUE :NEW.SALDOANTERIOR, 'SECUENCIACARGAMOVIMIENTOSMASIVOS' VALUE :NEW.SECUENCIACARGAMOVIMIENTOSMASIVOS, 'SECUENCIAMOVIMIENTOND52' VALUE :NEW.SECUENCIAMOVIMIENTOND52, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'TIPOCOMPROBANTECONTABLE' VALUE :NEW.TIPOCOMPROBANTECONTABLE, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION, 'TIPOTRANSACCION' VALUE :NEW.TIPOTRANSACCION, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'VALORMOVIMIENTO' VALUE :NEW.VALORMOVIMIENTO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'CODIGOMOTIVORETIRO' VALUE :NEW.CODIGOMOTIVORETIRO, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'CODIGOUSUARIOVERIFICA' VALUE :NEW.CODIGOUSUARIOVERIFICA, 'DESCRIPCIONMOVIMIENTO' VALUE :NEW.DESCRIPCIONMOVIMIENTO, 'ESTADOMOVIMIENTO' VALUE :NEW.ESTADOMOVIMIENTO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHACREACIONREGISTRO' VALUE :NEW.FECHACREACIONREGISTRO, 'FECHAFINAJUSTE' VALUE :NEW.FECHAFINAJUSTE, 'FECHAINICIOAJUSTE' VALUE :NEW.FECHAINICIOAJUSTE, 'FECHAPROCESO' VALUE :NEW.FECHAPROCESO, 'FECHARETIROFCME' VALUE :NEW.FECHARETIROFCME, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'HORAGENERACIONMOVIMIENTO' VALUE :NEW.HORAGENERACIONMOVIMIENTO, 'INDICADORMOVIMIENTOCAPITALIZADO' VALUE :NEW.INDICADORMOVIMIENTOCAPITALIZADO, 'INDICADORMOVIMIENTOIMPRESO' VALUE :NEW.INDICADORMOVIMIENTOIMPRESO, 'INDICADORTIPOPROCESO' VALUE :NEW.INDICADORTIPOPROCESO, 'MOTIVO' VALUE :NEW.MOTIVO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'NUMEROTRANSACCION' VALUE :NEW.NUMEROTRANSACCION, 'CODIGOPAGADOR' VALUE :NEW.CODIGOPAGADOR, 'PORCENTAJEDISTRIBUCIONVALORES' VALUE :NEW.PORCENTAJEDISTRIBUCIONVALORES, 'RUBROROL' VALUE :NEW.RUBROROL, 'SALDOANTERIOR' VALUE :NEW.SALDOANTERIOR, 'SECUENCIACARGAMOVIMIENTOSMASIVOS' VALUE :NEW.SECUENCIACARGAMOVIMIENTOSMASIVOS, 'SECUENCIAMOVIMIENTOND52' VALUE :NEW.SECUENCIAMOVIMIENTOND52, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'TIPOCOMPROBANTECONTABLE' VALUE :NEW.TIPOCOMPROBANTECONTABLE, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION, 'TIPOTRANSACCION' VALUE :NEW.TIPOTRANSACCION, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'VALORMOVIMIENTO' VALUE :NEW.VALORMOVIMIENTO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOINSTITUCION' VALUE :OLD.CODIGOINSTITUCION, 'CODIGOMOTIVORETIRO' VALUE :OLD.CODIGOMOTIVORETIRO, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'CODIGOUSUARIOCONFIRMA' VALUE :OLD.CODIGOUSUARIOCONFIRMA, 'CODIGOUSUARIOVERIFICA' VALUE :OLD.CODIGOUSUARIOVERIFICA, 'DESCRIPCIONMOVIMIENTO' VALUE :OLD.DESCRIPCIONMOVIMIENTO, 'ESTADOMOVIMIENTO' VALUE :OLD.ESTADOMOVIMIENTO, 'FECHAAUTORIZACION' VALUE :OLD.FECHAAUTORIZACION, 'FECHACREACIONREGISTRO' VALUE :OLD.FECHACREACIONREGISTRO, 'FECHAFINAJUSTE' VALUE :OLD.FECHAFINAJUSTE, 'FECHAINICIOAJUSTE' VALUE :OLD.FECHAINICIOAJUSTE, 'FECHAPROCESO' VALUE :OLD.FECHAPROCESO, 'FECHARETIROFCME' VALUE :OLD.FECHARETIROFCME, 'FECHAVERIFICACION' VALUE :OLD.FECHAVERIFICACION, 'HORAGENERACIONMOVIMIENTO' VALUE :OLD.HORAGENERACIONMOVIMIENTO, 'INDICADORMOVIMIENTOCAPITALIZADO' VALUE :OLD.INDICADORMOVIMIENTOCAPITALIZADO, 'INDICADORMOVIMIENTOIMPRESO' VALUE :OLD.INDICADORMOVIMIENTOIMPRESO, 'INDICADORTIPOPROCESO' VALUE :OLD.INDICADORTIPOPROCESO, 'MOTIVO' VALUE :OLD.MOTIVO, 'NUMEROCEDULA' VALUE :OLD.NUMEROCEDULA, 'NUMEROCOMPROBANTECONTABLE' VALUE :OLD.NUMEROCOMPROBANTECONTABLE, 'NUMEROTRANSACCION' VALUE :OLD.NUMEROTRANSACCION, 'CODIGOPAGADOR' VALUE :OLD.CODIGOPAGADOR, 'PORCENTAJEDISTRIBUCIONVALORES' VALUE :OLD.PORCENTAJEDISTRIBUCIONVALORES, 'RUBROROL' VALUE :OLD.RUBROROL, 'SALDOANTERIOR' VALUE :OLD.SALDOANTERIOR, 'SECUENCIACARGAMOVIMIENTOSMASIVOS' VALUE :OLD.SECUENCIACARGAMOVIMIENTOSMASIVOS, 'SECUENCIAMOVIMIENTOND52' VALUE :OLD.SECUENCIAMOVIMIENTOND52, 'SECUENCIAROL' VALUE :OLD.SECUENCIAROL, 'TIPOCOMPROBANTECONTABLE' VALUE :OLD.TIPOCOMPROBANTECONTABLE, 'TIPOINSTITUCION' VALUE :OLD.TIPOINSTITUCION, 'TIPOTRANSACCION' VALUE :OLD.TIPOTRANSACCION, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'VALORMOVIMIENTO' VALUE :OLD.VALORMOVIMIENTO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'movimientoTemporalType', v_event, v_payload, 'FCME_USER.MOVIMIENTOTEMPORAL_TYPE');
END;
/

/* --- TRG_OUTBOX_NATURALINFORMACIONADICION  ON FCME_USER.NATURALINFORMACIONADICIONALTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_NATURALINFORMACIONADICION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.NATURALINFORMACIONADICIONALTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'LICENCIACONDUCIR' VALUE :NEW.LICENCIACONDUCIR, 'CODIGOTIPOIDENTIFICACIONADICIONAL' VALUE :NEW.CODIGOTIPOIDENTIFICACIONADICIONAL, 'IDENTIFICACIONADICIONAL' VALUE :NEW.IDENTIFICACIONADICIONAL, 'CODIGOPAISNACIMIENTO' VALUE :NEW.CODIGOPAISNACIMIENTO, 'CODIGOPROVINCIANACIMIENTO' VALUE :NEW.CODIGOPROVINCIANACIMIENTO, 'CODIGOCIUDADNACIMIENTO' VALUE :NEW.CODIGOCIUDADNACIMIENTO, 'LUGARTRABAJO' VALUE :NEW.LUGARTRABAJO, 'NUMEROCARGAS' VALUE :NEW.NUMEROCARGAS, 'CODIGOPROFESION' VALUE :NEW.CODIGOPROFESION, 'CODIGONIVELEDUCACION' VALUE :NEW.CODIGONIVELEDUCACION, 'CODIGOFUENTEINGRESO' VALUE :NEW.CODIGOFUENTEINGRESO, 'MONTOVENTASESPERADO' VALUE :NEW.MONTOVENTASESPERADO, 'CANTIDADEMPLEADOS' VALUE :NEW.CANTIDADEMPLEADOS, 'NEGOCIOPROPIO' VALUE :NEW.NEGOCIOPROPIO, 'CODIGOBARRIONACIMIENTO' VALUE :NEW.CODIGOBARRIONACIMIENTO, 'OCUPACARGOPUBLICO' VALUE :NEW.OCUPACARGOPUBLICO, 'RELACIONSECTORPUBLICO' VALUE :NEW.RELACIONSECTORPUBLICO, 'OBLIGADOCONTABILIDAD' VALUE :NEW.OBLIGADOCONTABILIDAD, 'FECHAULTIMADECLARACION' VALUE :NEW.FECHAULTIMADECLARACION, 'FECHAINICIONEGOCIO' VALUE :NEW.FECHAINICIONEGOCIO, 'NUMEROCARGASESCOLARES' VALUE :NEW.NUMEROCARGASESCOLARES, 'DISCAPACITADO' VALUE :NEW.DISCAPACITADO, 'PORCENTAJEDISCAPACIDAD' VALUE :NEW.PORCENTAJEDISCAPACIDAD, 'OBSERVACIONES' VALUE :NEW.OBSERVACIONES, 'SEGUNDANACIONALIDAD' VALUE :NEW.SEGUNDANACIONALIDAD, 'CODIGOIMAGENFOTO' VALUE :NEW.CODIGOIMAGENFOTO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'LICENCIACONDUCIR' VALUE :NEW.LICENCIACONDUCIR, 'CODIGOTIPOIDENTIFICACIONADICIONAL' VALUE :NEW.CODIGOTIPOIDENTIFICACIONADICIONAL, 'IDENTIFICACIONADICIONAL' VALUE :NEW.IDENTIFICACIONADICIONAL, 'CODIGOPAISNACIMIENTO' VALUE :NEW.CODIGOPAISNACIMIENTO, 'CODIGOPROVINCIANACIMIENTO' VALUE :NEW.CODIGOPROVINCIANACIMIENTO, 'CODIGOCIUDADNACIMIENTO' VALUE :NEW.CODIGOCIUDADNACIMIENTO, 'LUGARTRABAJO' VALUE :NEW.LUGARTRABAJO, 'NUMEROCARGAS' VALUE :NEW.NUMEROCARGAS, 'CODIGOPROFESION' VALUE :NEW.CODIGOPROFESION, 'CODIGONIVELEDUCACION' VALUE :NEW.CODIGONIVELEDUCACION, 'CODIGOFUENTEINGRESO' VALUE :NEW.CODIGOFUENTEINGRESO, 'MONTOVENTASESPERADO' VALUE :NEW.MONTOVENTASESPERADO, 'CANTIDADEMPLEADOS' VALUE :NEW.CANTIDADEMPLEADOS, 'NEGOCIOPROPIO' VALUE :NEW.NEGOCIOPROPIO, 'CODIGOBARRIONACIMIENTO' VALUE :NEW.CODIGOBARRIONACIMIENTO, 'OCUPACARGOPUBLICO' VALUE :NEW.OCUPACARGOPUBLICO, 'RELACIONSECTORPUBLICO' VALUE :NEW.RELACIONSECTORPUBLICO, 'OBLIGADOCONTABILIDAD' VALUE :NEW.OBLIGADOCONTABILIDAD, 'FECHAULTIMADECLARACION' VALUE :NEW.FECHAULTIMADECLARACION, 'FECHAINICIONEGOCIO' VALUE :NEW.FECHAINICIONEGOCIO, 'NUMEROCARGASESCOLARES' VALUE :NEW.NUMEROCARGASESCOLARES, 'DISCAPACITADO' VALUE :NEW.DISCAPACITADO, 'PORCENTAJEDISCAPACIDAD' VALUE :NEW.PORCENTAJEDISCAPACIDAD, 'OBSERVACIONES' VALUE :NEW.OBSERVACIONES, 'SEGUNDANACIONALIDAD' VALUE :NEW.SEGUNDANACIONALIDAD, 'CODIGOIMAGENFOTO' VALUE :NEW.CODIGOIMAGENFOTO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'LICENCIACONDUCIR' VALUE :OLD.LICENCIACONDUCIR, 'CODIGOTIPOIDENTIFICACIONADICIONAL' VALUE :OLD.CODIGOTIPOIDENTIFICACIONADICIONAL, 'IDENTIFICACIONADICIONAL' VALUE :OLD.IDENTIFICACIONADICIONAL, 'CODIGOPAISNACIMIENTO' VALUE :OLD.CODIGOPAISNACIMIENTO, 'CODIGOPROVINCIANACIMIENTO' VALUE :OLD.CODIGOPROVINCIANACIMIENTO, 'CODIGOCIUDADNACIMIENTO' VALUE :OLD.CODIGOCIUDADNACIMIENTO, 'LUGARTRABAJO' VALUE :OLD.LUGARTRABAJO, 'NUMEROCARGAS' VALUE :OLD.NUMEROCARGAS, 'CODIGOPROFESION' VALUE :OLD.CODIGOPROFESION, 'CODIGONIVELEDUCACION' VALUE :OLD.CODIGONIVELEDUCACION, 'CODIGOFUENTEINGRESO' VALUE :OLD.CODIGOFUENTEINGRESO, 'MONTOVENTASESPERADO' VALUE :OLD.MONTOVENTASESPERADO, 'CANTIDADEMPLEADOS' VALUE :OLD.CANTIDADEMPLEADOS, 'NEGOCIOPROPIO' VALUE :OLD.NEGOCIOPROPIO, 'CODIGOBARRIONACIMIENTO' VALUE :OLD.CODIGOBARRIONACIMIENTO, 'OCUPACARGOPUBLICO' VALUE :OLD.OCUPACARGOPUBLICO, 'RELACIONSECTORPUBLICO' VALUE :OLD.RELACIONSECTORPUBLICO, 'OBLIGADOCONTABILIDAD' VALUE :OLD.OBLIGADOCONTABILIDAD, 'FECHAULTIMADECLARACION' VALUE :OLD.FECHAULTIMADECLARACION, 'FECHAINICIONEGOCIO' VALUE :OLD.FECHAINICIONEGOCIO, 'NUMEROCARGASESCOLARES' VALUE :OLD.NUMEROCARGASESCOLARES, 'DISCAPACITADO' VALUE :OLD.DISCAPACITADO, 'PORCENTAJEDISCAPACIDAD' VALUE :OLD.PORCENTAJEDISCAPACIDAD, 'OBSERVACIONES' VALUE :OLD.OBSERVACIONES, 'SEGUNDANACIONALIDAD' VALUE :OLD.SEGUNDANACIONALIDAD, 'CODIGOIMAGENFOTO' VALUE :OLD.CODIGOIMAGENFOTO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'naturalInformacionAdicionalType', v_event, v_payload, 'FCME_USER.NATURALINFORMACIONADICIONALTYPE');
END;
/

/* --- TRG_OUTBOX_NATURALINFORMACIONB  ON FCME_USER.NATURALINFORMACIONBASICATYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_NATURALINFORMACIONB
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.NATURALINFORMACIONBASICATYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('naturalInformacionBasicaType', v_pk, v_event, v_payload, 'FCME_USER.NATURALINFORMACIONBASICATYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_NATURALINGRESOSEGRESOSTYP  ON FCME_USER.NATURALINGRESOSEGRESOSTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_NATURALINGRESOSEGRESOSTYP
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.NATURALINGRESOSEGRESOSTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'INGRESOEGRESO' VALUE :NEW.INGRESOEGRESO, 'CODIGOTIPOINGRESOEGRESO' VALUE :NEW.CODIGOTIPOINGRESOEGRESO, 'SECUENCIAINGRESOEGRESO' VALUE :NEW.SECUENCIAINGRESOEGRESO, 'MONTOMENSUAL' VALUE :NEW.MONTOMENSUAL, 'FIJO' VALUE :NEW.FIJO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'INGRESOEGRESO' VALUE :NEW.INGRESOEGRESO, 'CODIGOTIPOINGRESOEGRESO' VALUE :NEW.CODIGOTIPOINGRESOEGRESO, 'SECUENCIAINGRESOEGRESO' VALUE :NEW.SECUENCIAINGRESOEGRESO, 'MONTOMENSUAL' VALUE :NEW.MONTOMENSUAL, 'FIJO' VALUE :NEW.FIJO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'INGRESOEGRESO' VALUE :OLD.INGRESOEGRESO, 'CODIGOTIPOINGRESOEGRESO' VALUE :OLD.CODIGOTIPOINGRESOEGRESO, 'SECUENCIAINGRESOEGRESO' VALUE :OLD.SECUENCIAINGRESOEGRESO, 'MONTOMENSUAL' VALUE :OLD.MONTOMENSUAL, 'FIJO' VALUE :OLD.FIJO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'naturalIngresosEgresosType', v_event, v_payload, 'FCME_USER.NATURALINGRESOSEGRESOSTYPE');
END;
/

/* --- TRG_OUTBOX_NATURALTRABAJOTYPE  ON FCME_USER.NATURALTRABAJOTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_NATURALTRABAJOTYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.NATURALTRABAJOTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIATRABAJO' VALUE :NEW.SECUENCIATRABAJO, 'CODIGOCARGOPERSONA' VALUE :NEW.CODIGOCARGOPERSONA, 'CODIGOCODIGOCARGO' VALUE :NEW.CODIGOCODIGOCARGO, 'FECHAINGRESOTRABAJO' VALUE :NEW.FECHAINGRESOTRABAJO, 'FECHASALIDA' VALUE :NEW.FECHASALIDA, 'NOMBREEMPLEADOR' VALUE :NEW.NOMBREEMPLEADOR, 'PROPIETARIO' VALUE :NEW.PROPIETARIO, 'TIPOCONTRATO' VALUE :NEW.TIPOCONTRATO, 'CARGOPUBLICO' VALUE :NEW.CARGOPUBLICO, 'SUELDO' VALUE :NEW.SUELDO, 'CANTIDADEMPLEADOS' VALUE :NEW.CANTIDADEMPLEADOS, 'CODIGOCOCUPACION' VALUE :NEW.CODIGOCOCUPACION, 'TIEMPOPARCIAL' VALUE :NEW.TIEMPOPARCIAL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIATRABAJO' VALUE :NEW.SECUENCIATRABAJO, 'CODIGOCARGOPERSONA' VALUE :NEW.CODIGOCARGOPERSONA, 'CODIGOCODIGOCARGO' VALUE :NEW.CODIGOCODIGOCARGO, 'FECHAINGRESOTRABAJO' VALUE :NEW.FECHAINGRESOTRABAJO, 'FECHASALIDA' VALUE :NEW.FECHASALIDA, 'NOMBREEMPLEADOR' VALUE :NEW.NOMBREEMPLEADOR, 'PROPIETARIO' VALUE :NEW.PROPIETARIO, 'TIPOCONTRATO' VALUE :NEW.TIPOCONTRATO, 'CARGOPUBLICO' VALUE :NEW.CARGOPUBLICO, 'SUELDO' VALUE :NEW.SUELDO, 'CANTIDADEMPLEADOS' VALUE :NEW.CANTIDADEMPLEADOS, 'CODIGOCOCUPACION' VALUE :NEW.CODIGOCOCUPACION, 'TIEMPOPARCIAL' VALUE :NEW.TIEMPOPARCIAL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIATRABAJO' VALUE :OLD.SECUENCIATRABAJO, 'CODIGOCARGOPERSONA' VALUE :OLD.CODIGOCARGOPERSONA, 'CODIGOCODIGOCARGO' VALUE :OLD.CODIGOCODIGOCARGO, 'FECHAINGRESOTRABAJO' VALUE :OLD.FECHAINGRESOTRABAJO, 'FECHASALIDA' VALUE :OLD.FECHASALIDA, 'NOMBREEMPLEADOR' VALUE :OLD.NOMBREEMPLEADOR, 'PROPIETARIO' VALUE :OLD.PROPIETARIO, 'TIPOCONTRATO' VALUE :OLD.TIPOCONTRATO, 'CARGOPUBLICO' VALUE :OLD.CARGOPUBLICO, 'SUELDO' VALUE :OLD.SUELDO, 'CANTIDADEMPLEADOS' VALUE :OLD.CANTIDADEMPLEADOS, 'CODIGOCOCUPACION' VALUE :OLD.CODIGOCOCUPACION, 'TIEMPOPARCIAL' VALUE :OLD.TIEMPOPARCIAL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'naturalTrabajoType', v_event, v_payload, 'FCME_USER.NATURALTRABAJOTYPE');
END;
/

/* --- TRG_OUTBOX_OTROSINGRESOSAFILIA  ON FCME_USER.OTROSINGRESOSAFILIADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_OTROSINGRESOSAFILIA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.OTROSINGRESOSAFILIADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'CODIGOOTROINGRRUBR' VALUE :NEW.CODIGOOTROINGRRUBR);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'CODIGOOTROINGRRUBR' VALUE :NEW.CODIGOOTROINGRRUBR);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'CODIGOCEDU' VALUE :OLD.CODIGOCEDU, 'CODIGOOTROINGRRUBR' VALUE :OLD.CODIGOOTROINGRRUBR);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('otrosIngresosAfiliadoType', v_pk, v_event, v_payload, 'FCME_USER.OTROSINGRESOSAFILIADO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PERSONADIRECCIONEST  ON FCME_USER.PERSONADIRECCIONESTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PERSONADIRECCIONEST
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PERSONADIRECCIONESTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('personaDireccionesType', v_pk, v_event, v_payload, 'FCME_USER.PERSONADIRECCIONESTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PERSONAFIRMASTYPE  ON FCME_USER.PERSONAFIRMASTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PERSONAFIRMASTYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PERSONAFIRMASTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAPERSONAFIRMA' VALUE :NEW.SECUENCIAPERSONAFIRMA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAPERSONAFIRMA' VALUE :NEW.SECUENCIAPERSONAFIRMA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIAPERSONAFIRMA' VALUE :OLD.SECUENCIAPERSONAFIRMA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('personaFirmasType', v_pk, v_event, v_payload, 'FCME_USER.PERSONAFIRMASTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PERSONAREFERENCIASBANCARI  ON FCME_USER.PERSONAREFERENCIASBANCARIASTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PERSONAREFERENCIASBANCARI
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PERSONAREFERENCIASBANCARIASTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAREFERENCIABANCARIA' VALUE :NEW.SECUENCIAREFERENCIABANCARIA, 'CODIGOTIPOCUENTAREFERENCIA' VALUE :NEW.CODIGOTIPOCUENTAREFERENCIA, 'TIPOIDENTIFICACIONIFINANCIERA' VALUE :NEW.TIPOIDENTIFICACIONIFINANCIERA, 'IDENTIFICACIONIFINANCIERA' VALUE :NEW.IDENTIFICACIONIFINANCIERA, 'NUCEMPRESABANCARIA' VALUE :NEW.NUCEMPRESABANCARIA, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'NOMBRETITULAR' VALUE :NEW.NOMBRETITULAR, 'NUMEROCIFRAS' VALUE :NEW.NUMEROCIFRAS, 'CODIGOCIFRASALDO' VALUE :NEW.CODIGOCIFRASALDO, 'FECHAAPERTURA' VALUE :NEW.FECHAAPERTURA, 'NUMEROPROTESTOS' VALUE :NEW.NUMEROPROTESTOS, 'CERRADA' VALUE :NEW.CERRADA, 'NOMBREINSTITUCIONPARACACEL' VALUE :NEW.NOMBREINSTITUCIONPARACACEL, 'OBSERVACIONESSOLOPARACACEL' VALUE :NEW.OBSERVACIONESSOLOPARACACEL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAREFERENCIABANCARIA' VALUE :NEW.SECUENCIAREFERENCIABANCARIA, 'CODIGOTIPOCUENTAREFERENCIA' VALUE :NEW.CODIGOTIPOCUENTAREFERENCIA, 'TIPOIDENTIFICACIONIFINANCIERA' VALUE :NEW.TIPOIDENTIFICACIONIFINANCIERA, 'IDENTIFICACIONIFINANCIERA' VALUE :NEW.IDENTIFICACIONIFINANCIERA, 'NUCEMPRESABANCARIA' VALUE :NEW.NUCEMPRESABANCARIA, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'NOMBRETITULAR' VALUE :NEW.NOMBRETITULAR, 'NUMEROCIFRAS' VALUE :NEW.NUMEROCIFRAS, 'CODIGOCIFRASALDO' VALUE :NEW.CODIGOCIFRASALDO, 'FECHAAPERTURA' VALUE :NEW.FECHAAPERTURA, 'NUMEROPROTESTOS' VALUE :NEW.NUMEROPROTESTOS, 'CERRADA' VALUE :NEW.CERRADA, 'NOMBREINSTITUCIONPARACACEL' VALUE :NEW.NOMBREINSTITUCIONPARACACEL, 'OBSERVACIONESSOLOPARACACEL' VALUE :NEW.OBSERVACIONESSOLOPARACACEL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIAREFERENCIABANCARIA' VALUE :OLD.SECUENCIAREFERENCIABANCARIA, 'CODIGOTIPOCUENTAREFERENCIA' VALUE :OLD.CODIGOTIPOCUENTAREFERENCIA, 'TIPOIDENTIFICACIONIFINANCIERA' VALUE :OLD.TIPOIDENTIFICACIONIFINANCIERA, 'IDENTIFICACIONIFINANCIERA' VALUE :OLD.IDENTIFICACIONIFINANCIERA, 'NUCEMPRESABANCARIA' VALUE :OLD.NUCEMPRESABANCARIA, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA, 'NOMBRETITULAR' VALUE :OLD.NOMBRETITULAR, 'NUMEROCIFRAS' VALUE :OLD.NUMEROCIFRAS, 'CODIGOCIFRASALDO' VALUE :OLD.CODIGOCIFRASALDO, 'FECHAAPERTURA' VALUE :OLD.FECHAAPERTURA, 'NUMEROPROTESTOS' VALUE :OLD.NUMEROPROTESTOS, 'CERRADA' VALUE :OLD.CERRADA, 'NOMBREINSTITUCIONPARACACEL' VALUE :OLD.NOMBREINSTITUCIONPARACACEL, 'OBSERVACIONESSOLOPARACACEL' VALUE :OLD.OBSERVACIONESSOLOPARACACEL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'personaReferenciasBancariasType', v_event, v_payload, 'FCME_USER.PERSONAREFERENCIASBANCARIASTYPE');
END;
/

/* --- TRG_OUTBOX_PERSONAREFERENCIASPERSONA  ON FCME_USER.PERSONAREFERENCIASPERSONALESTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PERSONAREFERENCIASPERSONA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PERSONAREFERENCIASPERSONALESTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAREFERENCIAPERSONAL' VALUE :NEW.SECUENCIAREFERENCIAPERSONAL, 'NOMBRESPERSONA' VALUE :NEW.NOMBRESPERSONA, 'APELLIDOPATERNO' VALUE :NEW.APELLIDOPATERNO, 'APELLIDOMATERNO' VALUE :NEW.APELLIDOMATERNO, 'DIRECCION' VALUE :NEW.DIRECCION, 'TELEFONO' VALUE :NEW.TELEFONO, 'CODIGOTIPOVINCULACION' VALUE :NEW.CODIGOTIPOVINCULACION, 'IDENTIFICACIONREFERENCIA' VALUE :NEW.IDENTIFICACIONREFERENCIA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIAREFERENCIAPERSONAL' VALUE :NEW.SECUENCIAREFERENCIAPERSONAL, 'NOMBRESPERSONA' VALUE :NEW.NOMBRESPERSONA, 'APELLIDOPATERNO' VALUE :NEW.APELLIDOPATERNO, 'APELLIDOMATERNO' VALUE :NEW.APELLIDOMATERNO, 'DIRECCION' VALUE :NEW.DIRECCION, 'TELEFONO' VALUE :NEW.TELEFONO, 'CODIGOTIPOVINCULACION' VALUE :NEW.CODIGOTIPOVINCULACION, 'IDENTIFICACIONREFERENCIA' VALUE :NEW.IDENTIFICACIONREFERENCIA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIAREFERENCIAPERSONAL' VALUE :OLD.SECUENCIAREFERENCIAPERSONAL, 'NOMBRESPERSONA' VALUE :OLD.NOMBRESPERSONA, 'APELLIDOPATERNO' VALUE :OLD.APELLIDOPATERNO, 'APELLIDOMATERNO' VALUE :OLD.APELLIDOMATERNO, 'DIRECCION' VALUE :OLD.DIRECCION, 'TELEFONO' VALUE :OLD.TELEFONO, 'CODIGOTIPOVINCULACION' VALUE :OLD.CODIGOTIPOVINCULACION, 'IDENTIFICACIONREFERENCIA' VALUE :OLD.IDENTIFICACIONREFERENCIA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'personaReferenciasPersonalesType', v_event, v_payload, 'FCME_USER.PERSONAREFERENCIASPERSONALESTYPE');
END;
/

/* --- TRG_OUTBOX_PERSONATELEFONOSTYPE  ON FCME_USER.PERSONATELEFONOSTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PERSONATELEFONOSTYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PERSONATELEFONOSTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIATELEFONO' VALUE :NEW.SECUENCIATELEFONO, 'CODIGOTIPOTELEFONO' VALUE :NEW.CODIGOTIPOTELEFONO, 'NUMEROTELEFONO' VALUE :NEW.NUMEROTELEFONO, 'EXTENSION' VALUE :NEW.EXTENSION, 'CODIGOTIPOUBICACION' VALUE :NEW.CODIGOTIPOUBICACION, 'NUMERODIRECCION' VALUE :NEW.NUMERODIRECCION, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'EMPRESAOPERADORA' VALUE :NEW.EMPRESAOPERADORA, 'CODIGOAREA' VALUE :NEW.CODIGOAREA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIATELEFONO' VALUE :NEW.SECUENCIATELEFONO, 'CODIGOTIPOTELEFONO' VALUE :NEW.CODIGOTIPOTELEFONO, 'NUMEROTELEFONO' VALUE :NEW.NUMEROTELEFONO, 'EXTENSION' VALUE :NEW.EXTENSION, 'CODIGOTIPOUBICACION' VALUE :NEW.CODIGOTIPOUBICACION, 'NUMERODIRECCION' VALUE :NEW.NUMERODIRECCION, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'EMPRESAOPERADORA' VALUE :NEW.EMPRESAOPERADORA, 'CODIGOAREA' VALUE :NEW.CODIGOAREA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIATELEFONO' VALUE :OLD.SECUENCIATELEFONO, 'CODIGOTIPOTELEFONO' VALUE :OLD.CODIGOTIPOTELEFONO, 'NUMEROTELEFONO' VALUE :OLD.NUMEROTELEFONO, 'EXTENSION' VALUE :OLD.EXTENSION, 'CODIGOTIPOUBICACION' VALUE :OLD.CODIGOTIPOUBICACION, 'NUMERODIRECCION' VALUE :OLD.NUMERODIRECCION, 'FECHAINGRESO' VALUE :OLD.FECHAINGRESO, 'EMPRESAOPERADORA' VALUE :OLD.EMPRESAOPERADORA, 'CODIGOAREA' VALUE :OLD.CODIGOAREA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'personaTelefonosType', v_event, v_payload, 'FCME_USER.PERSONATELEFONOSTYPE');
END;
/

/* --- TRG_OUTBOX_PERSONATYPE  ON FCME_USER.PERSONATYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PERSONATYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PERSONATYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('personaType', v_pk, v_event, v_payload, 'FCME_USER.PERSONATYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PERSONAVINCULACIONESTYPE  ON FCME_USER.PERSONAVINCULACIONESTYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PERSONAVINCULACIONESTYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PERSONAVINCULACIONESTYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA' VALUE :NEW.CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA, 'IDENTIFICACIONPERSONAVINCULADA' VALUE :NEW.IDENTIFICACIONPERSONAVINCULADA, 'CODIGOTIPOVINCULACION' VALUE :NEW.CODIGOTIPOVINCULACION, 'SECUENCIAPERSONAVINCULACION' VALUE :NEW.SECUENCIAPERSONAVINCULACION, 'FECHAVINCULACION' VALUE :NEW.FECHAVINCULACION, 'FECHASEPARACION' VALUE :NEW.FECHASEPARACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA' VALUE :NEW.CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA, 'IDENTIFICACIONPERSONAVINCULADA' VALUE :NEW.IDENTIFICACIONPERSONAVINCULADA, 'CODIGOTIPOVINCULACION' VALUE :NEW.CODIGOTIPOVINCULACION, 'SECUENCIAPERSONAVINCULACION' VALUE :NEW.SECUENCIAPERSONAVINCULACION, 'FECHAVINCULACION' VALUE :NEW.FECHAVINCULACION, 'FECHASEPARACION' VALUE :NEW.FECHASEPARACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA' VALUE :OLD.CODIGOTIPOIDENTIFICACIONPERSONAVINCULADA, 'IDENTIFICACIONPERSONAVINCULADA' VALUE :OLD.IDENTIFICACIONPERSONAVINCULADA, 'CODIGOTIPOVINCULACION' VALUE :OLD.CODIGOTIPOVINCULACION, 'SECUENCIAPERSONAVINCULACION' VALUE :OLD.SECUENCIAPERSONAVINCULACION, 'FECHAVINCULACION' VALUE :OLD.FECHAVINCULACION, 'FECHASEPARACION' VALUE :OLD.FECHASEPARACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'personaVinculacionesType', v_event, v_payload, 'FCME_USER.PERSONAVINCULACIONESTYPE');
END;
/

/* --- TRG_OUTBOX_REFERENCIAPARTICIPE  ON FCME_USER.REFERENCIAPARTICIPE_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_REFERENCIAPARTICIPE
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
END;
/

/* --- TRG_OUTBOX_REPORTESIBSPARTICIPE_TYPE  ON FCME_USER.REPORTESIBSPARTICIPE_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_REPORTESIBSPARTICIPE_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.REPORTESIBSPARTICIPE_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOENTIDAD' VALUE :NEW.CODIGOENTIDAD, 'CODIGOESTRUCTURA' VALUE :NEW.CODIGOESTRUCTURA, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHAGENERACION' VALUE :NEW.FECHAGENERACION, 'NUMEROREGISTRO' VALUE :NEW.NUMEROREGISTRO, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'CODIGOTIPOAPORTANTE' VALUE :NEW.CODIGOTIPOAPORTANTE, 'MONTOAPORTE' VALUE :NEW.MONTOAPORTE, 'SALDODISPONIBLE' VALUE :NEW.SALDODISPONIBLE, 'SALDOBLOQUEADO' VALUE :NEW.SALDOBLOQUEADO, 'MONTOLIQUIDACION' VALUE :NEW.MONTOLIQUIDACION, 'FECHALIQUIDACION' VALUE :NEW.FECHALIQUIDACION, 'ESTADO' VALUE :NEW.ESTADO, 'CODIGOUSUARIOGENERACION' VALUE :NEW.CODIGOUSUARIOGENERACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOENTIDAD' VALUE :NEW.CODIGOENTIDAD, 'CODIGOESTRUCTURA' VALUE :NEW.CODIGOESTRUCTURA, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHAGENERACION' VALUE :NEW.FECHAGENERACION, 'NUMEROREGISTRO' VALUE :NEW.NUMEROREGISTRO, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'CODIGOTIPOAPORTANTE' VALUE :NEW.CODIGOTIPOAPORTANTE, 'MONTOAPORTE' VALUE :NEW.MONTOAPORTE, 'SALDODISPONIBLE' VALUE :NEW.SALDODISPONIBLE, 'SALDOBLOQUEADO' VALUE :NEW.SALDOBLOQUEADO, 'MONTOLIQUIDACION' VALUE :NEW.MONTOLIQUIDACION, 'FECHALIQUIDACION' VALUE :NEW.FECHALIQUIDACION, 'ESTADO' VALUE :NEW.ESTADO, 'CODIGOUSUARIOGENERACION' VALUE :NEW.CODIGOUSUARIOGENERACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOENTIDAD' VALUE :OLD.CODIGOENTIDAD, 'CODIGOESTRUCTURA' VALUE :OLD.CODIGOESTRUCTURA, 'FECHACORTE' VALUE :OLD.FECHACORTE, 'FECHAGENERACION' VALUE :OLD.FECHAGENERACION, 'NUMEROREGISTRO' VALUE :OLD.NUMEROREGISTRO, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'NUMEROCEDULA' VALUE :OLD.NUMEROCEDULA, 'TIPOIDENTIFICACION' VALUE :OLD.TIPOIDENTIFICACION, 'CODIGOTIPOAPORTANTE' VALUE :OLD.CODIGOTIPOAPORTANTE, 'MONTOAPORTE' VALUE :OLD.MONTOAPORTE, 'SALDODISPONIBLE' VALUE :OLD.SALDODISPONIBLE, 'SALDOBLOQUEADO' VALUE :OLD.SALDOBLOQUEADO, 'MONTOLIQUIDACION' VALUE :OLD.MONTOLIQUIDACION, 'FECHALIQUIDACION' VALUE :OLD.FECHALIQUIDACION, 'ESTADO' VALUE :OLD.ESTADO, 'CODIGOUSUARIOGENERACION' VALUE :OLD.CODIGOUSUARIOGENERACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'reporteSIBSParticipeType', v_event, v_payload, 'FCME_USER.REPORTESIBSPARTICIPE_TYPE');
END;
/

/* --- TRG_OUTBOX_RETIROLIQUIDACION_TYPE  ON FCME_USER.RETIROLIQUIDACION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RETIROLIQUIDACION_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.RETIROLIQUIDACION_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIARETIRO' VALUE :NEW.SECUENCIARETIRO, 'CODIGOTIPORETIRO' VALUE :NEW.CODIGOTIPORETIRO, 'CODIGOMOTIVORETIRO' VALUE :NEW.CODIGOMOTIVORETIRO, 'FECHASOLICITUD' VALUE :NEW.FECHASOLICITUD, 'FECHAAPROBACION' VALUE :NEW.FECHAAPROBACION, 'FECHALIQUIDACION' VALUE :NEW.FECHALIQUIDACION, 'MONTOSOLICITADO' VALUE :NEW.MONTOSOLICITADO, 'MONTOAPROBADO' VALUE :NEW.MONTOAPROBADO, 'MONTOLIQUIDADO' VALUE :NEW.MONTOLIQUIDADO, 'CODIGOESTADO' VALUE :NEW.CODIGOESTADO, 'CODIGOCUENTADESTINO' VALUE :NEW.CODIGOCUENTADESTINO, 'CODIGOBANCODESTINO' VALUE :NEW.CODIGOBANCODESTINO, 'OBSERVACIONES' VALUE :NEW.OBSERVACIONES, 'CODIGOUSUARIOAPRUEBA' VALUE :NEW.CODIGOUSUARIOAPRUEBA, 'CODIGOUSUARIOLIQUIDA' VALUE :NEW.CODIGOUSUARIOLIQUIDA, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'MONTOSALDO' VALUE :NEW.MONTOSALDO, 'MONTOINTERESGENERADO' VALUE :NEW.MONTOINTERESGENERADO, 'PORCENTAJETASAINTERES' VALUE :NEW.PORCENTAJETASAINTERES, 'TIPOLIQUIDACION' VALUE :NEW.TIPOLIQUIDACION, 'SECUENCIALIQUIDACIONHIPO' VALUE :NEW.SECUENCIALIQUIDACIONHIPO, 'TIPOPROCESO' VALUE :NEW.TIPOPROCESO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOPAGADOR' VALUE :NEW.CODIGOPAGADOR, 'MONTOFAS' VALUE :NEW.MONTOFAS, 'MONTOCAPITALINSTITUCIONAL' VALUE :NEW.MONTOCAPITALINSTITUCIONAL, 'VALORSALDOINCIAL' VALUE :NEW.VALORSALDOINCIAL, 'FECHARETIROFCME' VALUE :NEW.FECHARETIROFCME, 'FECHACONCESION' VALUE :NEW.FECHACONCESION, 'VALORCREDITO' VALUE :NEW.VALORCREDITO, 'VALORINTERESCAPITALINICIAL' VALUE :NEW.VALORINTERESCAPITALINICIAL, 'VALORINTERESACCIONES' VALUE :NEW.VALORINTERESACCIONES, 'MOTIVO' VALUE :NEW.MOTIVO, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOAUTORIZAPROVISION' VALUE :NEW.CODIGOUSUARIOAUTORIZAPROVISION, 'FECHAAUTORIZAPROVISION' VALUE :NEW.FECHAAUTORIZAPROVISION, 'ESTADOANTERIOR' VALUE :NEW.ESTADOANTERIOR, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'VALORRESEVAS' VALUE :NEW.VALORRESEVAS, 'VALORADICIONAL' VALUE :NEW.VALORADICIONAL, 'VALORRETENCION' VALUE :NEW.VALORRETENCION, 'VALORCONSULCREDITO' VALUE :NEW.VALORCONSULCREDITO, 'SOBRANTEQUESELIQUIDA' VALUE :NEW.SOBRANTEQUESELIQUIDA, 'MONTODESCUENTOGASTOSJUBILACION' VALUE :NEW.MONTODESCUENTOGASTOSJUBILACION, 'MONTOPAGOCREDITOOTROFONDO' VALUE :NEW.MONTOPAGOCREDITOOTROFONDO, 'MONTOPAGOSOBREGIROOTROFONDO' VALUE :NEW.MONTOPAGOSOBREGIROOTROFONDO, 'VALORCREDITOSCONSULCREDITO' VALUE :NEW.VALORCREDITOSCONSULCREDITO, 'MONTOAPERTURACUP' VALUE :NEW.MONTOAPERTURACUP, 'APORTEROLPARAAPERTURACUP' VALUE :NEW.APORTEROLPARAAPERTURACUP, 'MONTOINVERSIONHIDROELECTRICA' VALUE :NEW.MONTOINVERSIONHIDROELECTRICA, 'MONTOCAPITALIZACIONCDP' VALUE :NEW.MONTOCAPITALIZACIONCDP, 'MONTORENTABILIDADCDP' VALUE :NEW.MONTORENTABILIDADCDP, 'MONTOGARANTIAPORCREDITO' VALUE :NEW.MONTOGARANTIAPORCREDITO, 'MONTOGARANTIAPORCUP' VALUE :NEW.MONTOGARANTIAPORCUP, 'CODIGOTASACUP' VALUE :NEW.CODIGOTASACUP, 'CODIGOPLAZACUP' VALUE :NEW.CODIGOPLAZACUP, 'CODIGOTIPOCAPITALIZACION' VALUE :NEW.CODIGOTIPOCAPITALIZACION, 'MONTORENTABILIDADCUP' VALUE :NEW.MONTORENTABILIDADCUP, 'CEDULAUSUARIOHIDROELECTRICA' VALUE :NEW.CEDULAUSUARIOHIDROELECTRICA, 'CEDULAUSUARIOCAPTACUP' VALUE :NEW.CEDULAUSUARIOCAPTACUP, 'CODIGOAPLICACIONORIGEN' VALUE :NEW.CODIGOAPLICACIONORIGEN, 'COBROPRESTANO' VALUE :NEW.COBROPRESTANO, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'INDICADORPROCESO' VALUE :NEW.INDICADORPROCESO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIARETIRO' VALUE :NEW.SECUENCIARETIRO, 'CODIGOTIPORETIRO' VALUE :NEW.CODIGOTIPORETIRO, 'CODIGOMOTIVORETIRO' VALUE :NEW.CODIGOMOTIVORETIRO, 'FECHASOLICITUD' VALUE :NEW.FECHASOLICITUD, 'FECHAAPROBACION' VALUE :NEW.FECHAAPROBACION, 'FECHALIQUIDACION' VALUE :NEW.FECHALIQUIDACION, 'MONTOSOLICITADO' VALUE :NEW.MONTOSOLICITADO, 'MONTOAPROBADO' VALUE :NEW.MONTOAPROBADO, 'MONTOLIQUIDADO' VALUE :NEW.MONTOLIQUIDADO, 'CODIGOESTADO' VALUE :NEW.CODIGOESTADO, 'CODIGOCUENTADESTINO' VALUE :NEW.CODIGOCUENTADESTINO, 'CODIGOBANCODESTINO' VALUE :NEW.CODIGOBANCODESTINO, 'OBSERVACIONES' VALUE :NEW.OBSERVACIONES, 'CODIGOUSUARIOAPRUEBA' VALUE :NEW.CODIGOUSUARIOAPRUEBA, 'CODIGOUSUARIOLIQUIDA' VALUE :NEW.CODIGOUSUARIOLIQUIDA, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'MONTOSALDO' VALUE :NEW.MONTOSALDO, 'MONTOINTERESGENERADO' VALUE :NEW.MONTOINTERESGENERADO, 'PORCENTAJETASAINTERES' VALUE :NEW.PORCENTAJETASAINTERES, 'TIPOLIQUIDACION' VALUE :NEW.TIPOLIQUIDACION, 'SECUENCIALIQUIDACIONHIPO' VALUE :NEW.SECUENCIALIQUIDACIONHIPO, 'TIPOPROCESO' VALUE :NEW.TIPOPROCESO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOPAGADOR' VALUE :NEW.CODIGOPAGADOR, 'MONTOFAS' VALUE :NEW.MONTOFAS, 'MONTOCAPITALINSTITUCIONAL' VALUE :NEW.MONTOCAPITALINSTITUCIONAL, 'VALORSALDOINCIAL' VALUE :NEW.VALORSALDOINCIAL, 'FECHARETIROFCME' VALUE :NEW.FECHARETIROFCME, 'FECHACONCESION' VALUE :NEW.FECHACONCESION, 'VALORCREDITO' VALUE :NEW.VALORCREDITO, 'VALORINTERESCAPITALINICIAL' VALUE :NEW.VALORINTERESCAPITALINICIAL, 'VALORINTERESACCIONES' VALUE :NEW.VALORINTERESACCIONES, 'MOTIVO' VALUE :NEW.MOTIVO, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOAUTORIZAPROVISION' VALUE :NEW.CODIGOUSUARIOAUTORIZAPROVISION, 'FECHAAUTORIZAPROVISION' VALUE :NEW.FECHAAUTORIZAPROVISION, 'ESTADOANTERIOR' VALUE :NEW.ESTADOANTERIOR, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'VALORRESEVAS' VALUE :NEW.VALORRESEVAS, 'VALORADICIONAL' VALUE :NEW.VALORADICIONAL, 'VALORRETENCION' VALUE :NEW.VALORRETENCION, 'VALORCONSULCREDITO' VALUE :NEW.VALORCONSULCREDITO, 'SOBRANTEQUESELIQUIDA' VALUE :NEW.SOBRANTEQUESELIQUIDA, 'MONTODESCUENTOGASTOSJUBILACION' VALUE :NEW.MONTODESCUENTOGASTOSJUBILACION, 'MONTOPAGOCREDITOOTROFONDO' VALUE :NEW.MONTOPAGOCREDITOOTROFONDO, 'MONTOPAGOSOBREGIROOTROFONDO' VALUE :NEW.MONTOPAGOSOBREGIROOTROFONDO, 'VALORCREDITOSCONSULCREDITO' VALUE :NEW.VALORCREDITOSCONSULCREDITO, 'MONTOAPERTURACUP' VALUE :NEW.MONTOAPERTURACUP, 'APORTEROLPARAAPERTURACUP' VALUE :NEW.APORTEROLPARAAPERTURACUP, 'MONTOINVERSIONHIDROELECTRICA' VALUE :NEW.MONTOINVERSIONHIDROELECTRICA, 'MONTOCAPITALIZACIONCDP' VALUE :NEW.MONTOCAPITALIZACIONCDP, 'MONTORENTABILIDADCDP' VALUE :NEW.MONTORENTABILIDADCDP, 'MONTOGARANTIAPORCREDITO' VALUE :NEW.MONTOGARANTIAPORCREDITO, 'MONTOGARANTIAPORCUP' VALUE :NEW.MONTOGARANTIAPORCUP, 'CODIGOTASACUP' VALUE :NEW.CODIGOTASACUP, 'CODIGOPLAZACUP' VALUE :NEW.CODIGOPLAZACUP, 'CODIGOTIPOCAPITALIZACION' VALUE :NEW.CODIGOTIPOCAPITALIZACION, 'MONTORENTABILIDADCUP' VALUE :NEW.MONTORENTABILIDADCUP, 'CEDULAUSUARIOHIDROELECTRICA' VALUE :NEW.CEDULAUSUARIOHIDROELECTRICA, 'CEDULAUSUARIOCAPTACUP' VALUE :NEW.CEDULAUSUARIOCAPTACUP, 'CODIGOAPLICACIONORIGEN' VALUE :NEW.CODIGOAPLICACIONORIGEN, 'COBROPRESTANO' VALUE :NEW.COBROPRESTANO, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'INDICADORPROCESO' VALUE :NEW.INDICADORPROCESO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIARETIRO' VALUE :OLD.SECUENCIARETIRO, 'CODIGOTIPORETIRO' VALUE :OLD.CODIGOTIPORETIRO, 'CODIGOMOTIVORETIRO' VALUE :OLD.CODIGOMOTIVORETIRO, 'FECHASOLICITUD' VALUE :OLD.FECHASOLICITUD, 'FECHAAPROBACION' VALUE :OLD.FECHAAPROBACION, 'FECHALIQUIDACION' VALUE :OLD.FECHALIQUIDACION, 'MONTOSOLICITADO' VALUE :OLD.MONTOSOLICITADO, 'MONTOAPROBADO' VALUE :OLD.MONTOAPROBADO, 'MONTOLIQUIDADO' VALUE :OLD.MONTOLIQUIDADO, 'CODIGOESTADO' VALUE :OLD.CODIGOESTADO, 'CODIGOCUENTADESTINO' VALUE :OLD.CODIGOCUENTADESTINO, 'CODIGOBANCODESTINO' VALUE :OLD.CODIGOBANCODESTINO, 'OBSERVACIONES' VALUE :OLD.OBSERVACIONES, 'CODIGOUSUARIOAPRUEBA' VALUE :OLD.CODIGOUSUARIOAPRUEBA, 'CODIGOUSUARIOLIQUIDA' VALUE :OLD.CODIGOUSUARIOLIQUIDA, 'FECHAINGRESO' VALUE :OLD.FECHAINGRESO, 'MONTOSALDO' VALUE :OLD.MONTOSALDO, 'MONTOINTERESGENERADO' VALUE :OLD.MONTOINTERESGENERADO, 'PORCENTAJETASAINTERES' VALUE :OLD.PORCENTAJETASAINTERES, 'TIPOLIQUIDACION' VALUE :OLD.TIPOLIQUIDACION, 'SECUENCIALIQUIDACIONHIPO' VALUE :OLD.SECUENCIALIQUIDACIONHIPO, 'TIPOPROCESO' VALUE :OLD.TIPOPROCESO, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOINSTITUCION' VALUE :OLD.CODIGOINSTITUCION, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'CODIGOPAGADOR' VALUE :OLD.CODIGOPAGADOR, 'MONTOFAS' VALUE :OLD.MONTOFAS, 'MONTOCAPITALINSTITUCIONAL' VALUE :OLD.MONTOCAPITALINSTITUCIONAL, 'VALORSALDOINCIAL' VALUE :OLD.VALORSALDOINCIAL, 'FECHARETIROFCME' VALUE :OLD.FECHARETIROFCME, 'FECHACONCESION' VALUE :OLD.FECHACONCESION, 'VALORCREDITO' VALUE :OLD.VALORCREDITO, 'VALORINTERESCAPITALINICIAL' VALUE :OLD.VALORINTERESCAPITALINICIAL, 'VALORINTERESACCIONES' VALUE :OLD.VALORINTERESACCIONES, 'MOTIVO' VALUE :OLD.MOTIVO, 'FECHAVERIFICACION' VALUE :OLD.FECHAVERIFICACION, 'CODIGOUSUARIOCONFIRMA' VALUE :OLD.CODIGOUSUARIOCONFIRMA, 'USUARIOELIMINA' VALUE :OLD.USUARIOELIMINA, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'CODIGOUSUARIOAUTORIZAPROVISION' VALUE :OLD.CODIGOUSUARIOAUTORIZAPROVISION, 'FECHAAUTORIZAPROVISION' VALUE :OLD.FECHAAUTORIZAPROVISION, 'ESTADOANTERIOR' VALUE :OLD.ESTADOANTERIOR, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'VALORRESEVAS' VALUE :OLD.VALORRESEVAS, 'VALORADICIONAL' VALUE :OLD.VALORADICIONAL, 'VALORRETENCION' VALUE :OLD.VALORRETENCION, 'VALORCONSULCREDITO' VALUE :OLD.VALORCONSULCREDITO, 'SOBRANTEQUESELIQUIDA' VALUE :OLD.SOBRANTEQUESELIQUIDA, 'MONTODESCUENTOGASTOSJUBILACION' VALUE :OLD.MONTODESCUENTOGASTOSJUBILACION, 'MONTOPAGOCREDITOOTROFONDO' VALUE :OLD.MONTOPAGOCREDITOOTROFONDO, 'MONTOPAGOSOBREGIROOTROFONDO' VALUE :OLD.MONTOPAGOSOBREGIROOTROFONDO, 'VALORCREDITOSCONSULCREDITO' VALUE :OLD.VALORCREDITOSCONSULCREDITO, 'MONTOAPERTURACUP' VALUE :OLD.MONTOAPERTURACUP, 'APORTEROLPARAAPERTURACUP' VALUE :OLD.APORTEROLPARAAPERTURACUP, 'MONTOINVERSIONHIDROELECTRICA' VALUE :OLD.MONTOINVERSIONHIDROELECTRICA, 'MONTOCAPITALIZACIONCDP' VALUE :OLD.MONTOCAPITALIZACIONCDP, 'MONTORENTABILIDADCDP' VALUE :OLD.MONTORENTABILIDADCDP, 'MONTOGARANTIAPORCREDITO' VALUE :OLD.MONTOGARANTIAPORCREDITO, 'MONTOGARANTIAPORCUP' VALUE :OLD.MONTOGARANTIAPORCUP, 'CODIGOTASACUP' VALUE :OLD.CODIGOTASACUP, 'CODIGOPLAZACUP' VALUE :OLD.CODIGOPLAZACUP, 'CODIGOTIPOCAPITALIZACION' VALUE :OLD.CODIGOTIPOCAPITALIZACION, 'MONTORENTABILIDADCUP' VALUE :OLD.MONTORENTABILIDADCUP, 'CEDULAUSUARIOHIDROELECTRICA' VALUE :OLD.CEDULAUSUARIOHIDROELECTRICA, 'CEDULAUSUARIOCAPTACUP' VALUE :OLD.CEDULAUSUARIOCAPTACUP, 'CODIGOAPLICACIONORIGEN' VALUE :OLD.CODIGOAPLICACIONORIGEN, 'COBROPRESTANO' VALUE :OLD.COBROPRESTANO, 'CODIGOPROCESO' VALUE :OLD.CODIGOPROCESO, 'INDICADORPROCESO' VALUE :OLD.INDICADORPROCESO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'retiroLiquidacionType', v_event, v_payload, 'FCME_USER.RETIROLIQUIDACION_TYPE');
END;
/

/* --- TRG_OUTBOX_RETIROVOLUNTARIOESTADO_TY  ON FCME_USER.RETIROVOLUNTARIOESTADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RETIROVOLUNTARIOESTADO_TY
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.RETIROVOLUNTARIOESTADO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIO' VALUE :NEW.ANIO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'ESTADOAFILIADO' VALUE :NEW.ESTADOAFILIADO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'SECUENCIADETALLE' VALUE :NEW.SECUENCIADETALLE, 'TIPORETIROVOLUNTARIO' VALUE :NEW.TIPORETIROVOLUNTARIO, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIO' VALUE :NEW.ANIO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'ESTADOAFILIADO' VALUE :NEW.ESTADOAFILIADO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'SECUENCIADETALLE' VALUE :NEW.SECUENCIADETALLE, 'TIPORETIROVOLUNTARIO' VALUE :NEW.TIPORETIROVOLUNTARIO, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ANIO' VALUE :OLD.ANIO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'ESTADOAFILIADO' VALUE :OLD.ESTADOAFILIADO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'SECUENCIADETALLE' VALUE :OLD.SECUENCIADETALLE, 'TIPORETIROVOLUNTARIO' VALUE :OLD.TIPORETIROVOLUNTARIO, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'USUARIOMODIFICA' VALUE :OLD.USUARIOMODIFICA, 'CODIGOINSTITUCION' VALUE :OLD.CODIGOINSTITUCION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'retiroVoluntarioEstadoType', v_event, v_payload, 'FCME_USER.RETIROVOLUNTARIOESTADO_TYPE');
END;
/

/* --- TRG_OUTBOX_ROLNOMINA_TYPE  ON FCME_USER.ROLNOMINA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ROLNOMINA_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.ROLNOMINA_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'DESCRIPCIONRUBRO' VALUE :NEW.DESCRIPCIONRUBRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'DESCRIPCIONRUBRO' VALUE :NEW.DESCRIPCIONRUBRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGORUBRO' VALUE :OLD.CODIGORUBRO, 'DESCRIPCIONRUBRO' VALUE :OLD.DESCRIPCIONRUBRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('rolNominaType', v_pk, v_event, v_payload, 'FCME_USER.ROLNOMINA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SALDODIARIORUBRO_TYPE  ON FCME_USER.SALDODIARIORUBRO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SALDODIARIORUBRO_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.SALDODIARIORUBRO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'FECHASALDO' VALUE :NEW.FECHASALDO, 'CODIGOTIPOTRANSACCION' VALUE :NEW.CODIGOTIPOTRANSACCION, 'CODIGOMOTIVO' VALUE :NEW.CODIGOMOTIVO, 'CODIGORUBROROL' VALUE :NEW.CODIGORUBROROL, 'VASALDO' VALUE :NEW.VASALDO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'FECHASALDO' VALUE :NEW.FECHASALDO, 'CODIGOTIPOTRANSACCION' VALUE :NEW.CODIGOTIPOTRANSACCION, 'CODIGOMOTIVO' VALUE :NEW.CODIGOMOTIVO, 'CODIGORUBROROL' VALUE :NEW.CODIGORUBROROL, 'VASALDO' VALUE :NEW.VASALDO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'FECHASALDO' VALUE :OLD.FECHASALDO, 'CODIGOTIPOTRANSACCION' VALUE :OLD.CODIGOTIPOTRANSACCION, 'CODIGOMOTIVO' VALUE :OLD.CODIGOMOTIVO, 'CODIGORUBROROL' VALUE :OLD.CODIGORUBROROL, 'VASALDO' VALUE :OLD.VASALDO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'saldoDiarioRubroType', v_event, v_payload, 'FCME_USER.SALDODIARIORUBRO_TYPE');
END;
/

/* --- TRG_OUTBOX_SALDODIARIO_TYPE  ON FCME_USER.SALDODIARIO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SALDODIARIO_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.SALDODIARIO_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'FECHASALDO' VALUE :NEW.FECHASALDO, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'SALDOANTERIOR' VALUE :NEW.SALDOANTERIOR, 'MONTODEBITO' VALUE :NEW.MONTODEBITO, 'MONTOCREDITO' VALUE :NEW.MONTOCREDITO, 'SALDOACTUAL' VALUE :NEW.SALDOACTUAL, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'FECHASALDO' VALUE :NEW.FECHASALDO, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'SALDOANTERIOR' VALUE :NEW.SALDOANTERIOR, 'MONTODEBITO' VALUE :NEW.MONTODEBITO, 'MONTOCREDITO' VALUE :NEW.MONTOCREDITO, 'SALDOACTUAL' VALUE :NEW.SALDOACTUAL, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'FECHASALDO' VALUE :OLD.FECHASALDO, 'CODIGORUBRO' VALUE :OLD.CODIGORUBRO, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'SALDOANTERIOR' VALUE :OLD.SALDOANTERIOR, 'MONTODEBITO' VALUE :OLD.MONTODEBITO, 'MONTOCREDITO' VALUE :OLD.MONTOCREDITO, 'SALDOACTUAL' VALUE :OLD.SALDOACTUAL, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'saldoDiarioType', v_event, v_payload, 'FCME_USER.SALDODIARIO_TYPE');
END;
/

/* --- TRG_OUTBOX_SEGUROVIDAPARTICIPE_TYPE  ON FCME_USER.SEGUROVIDAPARTICIPE_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SEGUROVIDAPARTICIPE_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.SEGUROVIDAPARTICIPE_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSECUENCIACAUSAFALLECIMIENTO' VALUE :NEW.CODIGOSECUENCIACAUSAFALLECIMIENTO, 'DESCRIPCIONCAUSAFALLECIMIENTO' VALUE :NEW.DESCRIPCIONCAUSAFALLECIMIENTO, 'ESTADOCAUSA' VALUE :NEW.ESTADOCAUSA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOTIPOFAMILIAR' VALUE :NEW.CODIGOTIPOFAMILIAR, 'CODIGOTIPOPLAN' VALUE :NEW.CODIGOTIPOPLAN, 'EDADPROVEEDOR' VALUE :NEW.EDADPROVEEDOR, 'ESTADOCOBERTURA' VALUE :NEW.ESTADOCOBERTURA, 'FECHAAFILIACIONCAM' VALUE :NEW.FECHAAFILIACIONCAM, 'FECHAFINALVIGENCIATASA' VALUE :NEW.FECHAFINALVIGENCIATASA, 'FECHAINICIO' VALUE :NEW.FECHAINICIO, 'MONTOCOBERTURA' VALUE :NEW.MONTOCOBERTURA, 'NUMEROANIOSAFILIACIONFCME' VALUE :NEW.NUMEROANIOSAFILIACIONFCME, 'TIPOCOBERTURA' VALUE :NEW.TIPOCOBERTURA, 'CODIGODISCAPACIDADFAMILIARES' VALUE :NEW.CODIGODISCAPACIDADFAMILIARES, 'DISCAPACIDADFAMILIARESAFILIADO' VALUE :NEW.DISCAPACIDADFAMILIARESAFILIADO, 'ESTADODISCAPACIDAD' VALUE :NEW.ESTADODISCAPACIDAD, 'CODIGOEFECTO' VALUE :NEW.CODIGOEFECTO, 'DESCRIPCIONEFECTO' VALUE :NEW.DESCRIPCIONEFECTO, 'ESTADOEFECTO' VALUE :NEW.ESTADOEFECTO, 'TIPOEFECTO' VALUE :NEW.TIPOEFECTO, 'DESCRIPCIONFORMAPAGO' VALUE :NEW.DESCRIPCIONFORMAPAGO, 'ESTADOFORMAPAGO' VALUE :NEW.ESTADOFORMAPAGO, 'TIPOFORMAPAGO' VALUE :NEW.TIPOFORMAPAGO, 'CEDULAIDENTIDADAFILIADO' VALUE :NEW.CEDULAIDENTIDADAFILIADO, 'CEDULAIDENTIDADSINIESTRADO' VALUE :NEW.CEDULAIDENTIDADSINIESTRADO, 'CODIGOBANCOELCUALREALIZAPAGO' VALUE :NEW.CODIGOBANCOELCUALREALIZAPAGO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'ESTADOANTERIORSINIESTRADO' VALUE :NEW.ESTADOANTERIORSINIESTRADO, 'ESTADOSINESTRO' VALUE :NEW.ESTADOSINESTRO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHAFALLECIMIENTO' VALUE :NEW.FECHAFALLECIMIENTO, 'FECHANOTIFICACIONSINIESTRO' VALUE :NEW.FECHANOTIFICACIONSINIESTRO, 'FECHAPRESENTACIONPAPELESSINIESTRADO' VALUE :NEW.FECHAPRESENTACIONPAPELESSINIESTRADO, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'SECUENCIASINIESTRO' VALUE :NEW.SECUENCIASINIESTRO, 'TIPOSINIESTRO' VALUE :NEW.TIPOSINIESTRO, 'USUARIOAUTORIZACION' VALUE :NEW.USUARIOAUTORIZACION, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'CEDULABENEFICIARIO' VALUE :NEW.CEDULABENEFICIARIO, 'CODIGOTIPOFAMILIARSINIESTRADO' VALUE :NEW.CODIGOTIPOFAMILIARSINIESTRADO, 'MONTOQUERECIBIOBENEFICIARIO' VALUE :NEW.MONTOQUERECIBIOBENEFICIARIO, 'PORCENTAJEDISTRUBUCION' VALUE :NEW.PORCENTAJEDISTRUBUCION, 'ABONOPROPUESTOCREDITODESGRAVAMEN' VALUE :NEW.ABONOPROPUESTOCREDITODESGRAVAMEN, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'INDICADORPAGODESGRAVAMEN' VALUE :NEW.INDICADORPAGODESGRAVAMEN, 'MONTOCREDITOACANCELAR' VALUE :NEW.MONTOCREDITOACANCELAR, 'MONTOPAGARSOBREGIROOTROFONDO' VALUE :NEW.MONTOPAGARSOBREGIROOTROFONDO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'VALORDESGRAVAMEN' VALUE :NEW.VALORDESGRAVAMEN, 'MONTOCUBIERTOPORDESGRAVAMEN' VALUE :NEW.MONTOCUBIERTOPORDESGRAVAMEN, 'MONTONOCUBIERTOCREDITO' VALUE :NEW.MONTONOCUBIERTOCREDITO, 'MONTOPAGOSOBREGIROOTROFONDO' VALUE :NEW.MONTOPAGOSOBREGIROOTROFONDO, 'MONTOPARACUBRIRSALDOCREDITO' VALUE :NEW.MONTOPARACUBRIRSALDOCREDITO, 'MONTOREALAPERCIBIRPORSINIESTRO' VALUE :NEW.MONTOREALAPERCIBIRPORSINIESTRO, 'ESTADOSINIESTROEXTEMPORANEO' VALUE :NEW.ESTADOSINIESTROEXTEMPORANEO, 'FECHASINIESTRO' VALUE :NEW.FECHASINIESTRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSECUENCIACAUSAFALLECIMIENTO' VALUE :NEW.CODIGOSECUENCIACAUSAFALLECIMIENTO, 'DESCRIPCIONCAUSAFALLECIMIENTO' VALUE :NEW.DESCRIPCIONCAUSAFALLECIMIENTO, 'ESTADOCAUSA' VALUE :NEW.ESTADOCAUSA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOTIPOFAMILIAR' VALUE :NEW.CODIGOTIPOFAMILIAR, 'CODIGOTIPOPLAN' VALUE :NEW.CODIGOTIPOPLAN, 'EDADPROVEEDOR' VALUE :NEW.EDADPROVEEDOR, 'ESTADOCOBERTURA' VALUE :NEW.ESTADOCOBERTURA, 'FECHAAFILIACIONCAM' VALUE :NEW.FECHAAFILIACIONCAM, 'FECHAFINALVIGENCIATASA' VALUE :NEW.FECHAFINALVIGENCIATASA, 'FECHAINICIO' VALUE :NEW.FECHAINICIO, 'MONTOCOBERTURA' VALUE :NEW.MONTOCOBERTURA, 'NUMEROANIOSAFILIACIONFCME' VALUE :NEW.NUMEROANIOSAFILIACIONFCME, 'TIPOCOBERTURA' VALUE :NEW.TIPOCOBERTURA, 'CODIGODISCAPACIDADFAMILIARES' VALUE :NEW.CODIGODISCAPACIDADFAMILIARES, 'DISCAPACIDADFAMILIARESAFILIADO' VALUE :NEW.DISCAPACIDADFAMILIARESAFILIADO, 'ESTADODISCAPACIDAD' VALUE :NEW.ESTADODISCAPACIDAD, 'CODIGOEFECTO' VALUE :NEW.CODIGOEFECTO, 'DESCRIPCIONEFECTO' VALUE :NEW.DESCRIPCIONEFECTO, 'ESTADOEFECTO' VALUE :NEW.ESTADOEFECTO, 'TIPOEFECTO' VALUE :NEW.TIPOEFECTO, 'DESCRIPCIONFORMAPAGO' VALUE :NEW.DESCRIPCIONFORMAPAGO, 'ESTADOFORMAPAGO' VALUE :NEW.ESTADOFORMAPAGO, 'TIPOFORMAPAGO' VALUE :NEW.TIPOFORMAPAGO, 'CEDULAIDENTIDADAFILIADO' VALUE :NEW.CEDULAIDENTIDADAFILIADO, 'CEDULAIDENTIDADSINIESTRADO' VALUE :NEW.CEDULAIDENTIDADSINIESTRADO, 'CODIGOBANCOELCUALREALIZAPAGO' VALUE :NEW.CODIGOBANCOELCUALREALIZAPAGO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'ESTADOANTERIORSINIESTRADO' VALUE :NEW.ESTADOANTERIORSINIESTRADO, 'ESTADOSINESTRO' VALUE :NEW.ESTADOSINESTRO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHAFALLECIMIENTO' VALUE :NEW.FECHAFALLECIMIENTO, 'FECHANOTIFICACIONSINIESTRO' VALUE :NEW.FECHANOTIFICACIONSINIESTRO, 'FECHAPRESENTACIONPAPELESSINIESTRADO' VALUE :NEW.FECHAPRESENTACIONPAPELESSINIESTRADO, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'SECUENCIASINIESTRO' VALUE :NEW.SECUENCIASINIESTRO, 'TIPOSINIESTRO' VALUE :NEW.TIPOSINIESTRO, 'USUARIOAUTORIZACION' VALUE :NEW.USUARIOAUTORIZACION, 'USUARIOELIMINA' VALUE :NEW.USUARIOELIMINA, 'CEDULABENEFICIARIO' VALUE :NEW.CEDULABENEFICIARIO, 'CODIGOTIPOFAMILIARSINIESTRADO' VALUE :NEW.CODIGOTIPOFAMILIARSINIESTRADO, 'MONTOQUERECIBIOBENEFICIARIO' VALUE :NEW.MONTOQUERECIBIOBENEFICIARIO, 'PORCENTAJEDISTRUBUCION' VALUE :NEW.PORCENTAJEDISTRUBUCION, 'ABONOPROPUESTOCREDITODESGRAVAMEN' VALUE :NEW.ABONOPROPUESTOCREDITODESGRAVAMEN, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'INDICADORPAGODESGRAVAMEN' VALUE :NEW.INDICADORPAGODESGRAVAMEN, 'MONTOCREDITOACANCELAR' VALUE :NEW.MONTOCREDITOACANCELAR, 'MONTOPAGARSOBREGIROOTROFONDO' VALUE :NEW.MONTOPAGARSOBREGIROOTROFONDO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'VALORDESGRAVAMEN' VALUE :NEW.VALORDESGRAVAMEN, 'MONTOCUBIERTOPORDESGRAVAMEN' VALUE :NEW.MONTOCUBIERTOPORDESGRAVAMEN, 'MONTONOCUBIERTOCREDITO' VALUE :NEW.MONTONOCUBIERTOCREDITO, 'MONTOPAGOSOBREGIROOTROFONDO' VALUE :NEW.MONTOPAGOSOBREGIROOTROFONDO, 'MONTOPARACUBRIRSALDOCREDITO' VALUE :NEW.MONTOPARACUBRIRSALDOCREDITO, 'MONTOREALAPERCIBIRPORSINIESTRO' VALUE :NEW.MONTOREALAPERCIBIRPORSINIESTRO, 'ESTADOSINIESTROEXTEMPORANEO' VALUE :NEW.ESTADOSINIESTROEXTEMPORANEO, 'FECHASINIESTRO' VALUE :NEW.FECHASINIESTRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOSECUENCIACAUSAFALLECIMIENTO' VALUE :OLD.CODIGOSECUENCIACAUSAFALLECIMIENTO, 'DESCRIPCIONCAUSAFALLECIMIENTO' VALUE :OLD.DESCRIPCIONCAUSAFALLECIMIENTO, 'ESTADOCAUSA' VALUE :OLD.ESTADOCAUSA, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'USUARIOMODIFICA' VALUE :OLD.USUARIOMODIFICA, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'CODIGOTIPOFAMILIAR' VALUE :OLD.CODIGOTIPOFAMILIAR, 'CODIGOTIPOPLAN' VALUE :OLD.CODIGOTIPOPLAN, 'EDADPROVEEDOR' VALUE :OLD.EDADPROVEEDOR, 'ESTADOCOBERTURA' VALUE :OLD.ESTADOCOBERTURA, 'FECHAAFILIACIONCAM' VALUE :OLD.FECHAAFILIACIONCAM, 'FECHAFINALVIGENCIATASA' VALUE :OLD.FECHAFINALVIGENCIATASA, 'FECHAINICIO' VALUE :OLD.FECHAINICIO, 'MONTOCOBERTURA' VALUE :OLD.MONTOCOBERTURA, 'NUMEROANIOSAFILIACIONFCME' VALUE :OLD.NUMEROANIOSAFILIACIONFCME, 'TIPOCOBERTURA' VALUE :OLD.TIPOCOBERTURA, 'CODIGODISCAPACIDADFAMILIARES' VALUE :OLD.CODIGODISCAPACIDADFAMILIARES, 'DISCAPACIDADFAMILIARESAFILIADO' VALUE :OLD.DISCAPACIDADFAMILIARESAFILIADO, 'ESTADODISCAPACIDAD' VALUE :OLD.ESTADODISCAPACIDAD, 'CODIGOEFECTO' VALUE :OLD.CODIGOEFECTO, 'DESCRIPCIONEFECTO' VALUE :OLD.DESCRIPCIONEFECTO, 'ESTADOEFECTO' VALUE :OLD.ESTADOEFECTO, 'TIPOEFECTO' VALUE :OLD.TIPOEFECTO, 'DESCRIPCIONFORMAPAGO' VALUE :OLD.DESCRIPCIONFORMAPAGO, 'ESTADOFORMAPAGO' VALUE :OLD.ESTADOFORMAPAGO, 'TIPOFORMAPAGO' VALUE :OLD.TIPOFORMAPAGO, 'CEDULAIDENTIDADAFILIADO' VALUE :OLD.CEDULAIDENTIDADAFILIADO, 'CEDULAIDENTIDADSINIESTRADO' VALUE :OLD.CEDULAIDENTIDADSINIESTRADO, 'CODIGOBANCOELCUALREALIZAPAGO' VALUE :OLD.CODIGOBANCOELCUALREALIZAPAGO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'CODIGOUSUARIOCONFIRMA' VALUE :OLD.CODIGOUSUARIOCONFIRMA, 'ESTADOANTERIORSINIESTRADO' VALUE :OLD.ESTADOANTERIORSINIESTRADO, 'ESTADOSINESTRO' VALUE :OLD.ESTADOSINESTRO, 'FECHAAUTORIZACION' VALUE :OLD.FECHAAUTORIZACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'FECHAFALLECIMIENTO' VALUE :OLD.FECHAFALLECIMIENTO, 'FECHANOTIFICACIONSINIESTRO' VALUE :OLD.FECHANOTIFICACIONSINIESTRO, 'FECHAPRESENTACIONPAPELESSINIESTRADO' VALUE :OLD.FECHAPRESENTACIONPAPELESSINIESTRADO, 'FECHAVERIFICACION' VALUE :OLD.FECHAVERIFICACION, 'SECUENCIASINIESTRO' VALUE :OLD.SECUENCIASINIESTRO, 'TIPOSINIESTRO' VALUE :OLD.TIPOSINIESTRO, 'USUARIOAUTORIZACION' VALUE :OLD.USUARIOAUTORIZACION, 'USUARIOELIMINA' VALUE :OLD.USUARIOELIMINA, 'CEDULABENEFICIARIO' VALUE :OLD.CEDULABENEFICIARIO, 'CODIGOTIPOFAMILIARSINIESTRADO' VALUE :OLD.CODIGOTIPOFAMILIARSINIESTRADO, 'MONTOQUERECIBIOBENEFICIARIO' VALUE :OLD.MONTOQUERECIBIOBENEFICIARIO, 'PORCENTAJEDISTRUBUCION' VALUE :OLD.PORCENTAJEDISTRUBUCION, 'ABONOPROPUESTOCREDITODESGRAVAMEN' VALUE :OLD.ABONOPROPUESTOCREDITODESGRAVAMEN, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'INDICADORPAGODESGRAVAMEN' VALUE :OLD.INDICADORPAGODESGRAVAMEN, 'MONTOCREDITOACANCELAR' VALUE :OLD.MONTOCREDITOACANCELAR, 'MONTOPAGARSOBREGIROOTROFONDO' VALUE :OLD.MONTOPAGARSOBREGIROOTROFONDO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'VALORDESGRAVAMEN' VALUE :OLD.VALORDESGRAVAMEN, 'MONTOCUBIERTOPORDESGRAVAMEN' VALUE :OLD.MONTOCUBIERTOPORDESGRAVAMEN, 'MONTONOCUBIERTOCREDITO' VALUE :OLD.MONTONOCUBIERTOCREDITO, 'MONTOPAGOSOBREGIROOTROFONDO' VALUE :OLD.MONTOPAGOSOBREGIROOTROFONDO, 'MONTOPARACUBRIRSALDOCREDITO' VALUE :OLD.MONTOPARACUBRIRSALDOCREDITO, 'MONTOREALAPERCIBIRPORSINIESTRO' VALUE :OLD.MONTOREALAPERCIBIRPORSINIESTRO, 'ESTADOSINIESTROEXTEMPORANEO' VALUE :OLD.ESTADOSINIESTROEXTEMPORANEO, 'FECHASINIESTRO' VALUE :OLD.FECHASINIESTRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'seguroVidaParticipeType', v_event, v_payload, 'FCME_USER.SEGUROVIDAPARTICIPE_TYPE');
END;
/

/* --- TRG_OUTBOX_SERVICIOADICIONAL_TYPE  ON FCME_USER.SERVICIOADICIONAL_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SERVICIOADICIONAL_TYPE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.SERVICIOADICIONAL_TYPE
FOR EACH ROW
DECLARE
    v_event   VARCHAR2(20);
    v_pk      VARCHAR2(200);
    v_payload CLOB;
BEGIN
    IF SYS_CONTEXT('USERENV','CLIENT_INFO') = 'is_replicating' THEN RETURN; END IF;
    IF INSERTING THEN
        v_event := 'INSERT';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIASERVICIO' VALUE :NEW.SECUENCIASERVICIO, 'CODIGOTIPOSERVICIO' VALUE :NEW.CODIGOTIPOSERVICIO, 'DESCRIPCIONSERVICIO' VALUE :NEW.DESCRIPCIONSERVICIO, 'MONTOSERVICIO' VALUE :NEW.MONTOSERVICIO, 'FECHAINICIO' VALUE :NEW.FECHAINICIO, 'FECHAFIN' VALUE :NEW.FECHAFIN, 'ESTADO' VALUE :NEW.ESTADO, 'CODIGOUSUARIOAUTORIZA' VALUE :NEW.CODIGOUSUARIOAUTORIZA, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIA' VALUE :NEW.SECUENCIA, 'NUMEROCEDULARECIBECOREO' VALUE :NEW.NUMEROCEDULARECIBECOREO, 'TIPOSERVICIO' VALUE :NEW.TIPOSERVICIO, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'USUARIOAUTORIZACION' VALUE :NEW.USUARIOAUTORIZACION, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'SECUENCIASERVICIO' VALUE :NEW.SECUENCIASERVICIO, 'CODIGOTIPOSERVICIO' VALUE :NEW.CODIGOTIPOSERVICIO, 'DESCRIPCIONSERVICIO' VALUE :NEW.DESCRIPCIONSERVICIO, 'MONTOSERVICIO' VALUE :NEW.MONTOSERVICIO, 'FECHAINICIO' VALUE :NEW.FECHAINICIO, 'FECHAFIN' VALUE :NEW.FECHAFIN, 'ESTADO' VALUE :NEW.ESTADO, 'CODIGOUSUARIOAUTORIZA' VALUE :NEW.CODIGOUSUARIOAUTORIZA, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIA' VALUE :NEW.SECUENCIA, 'NUMEROCEDULARECIBECOREO' VALUE :NEW.NUMEROCEDULARECIBECOREO, 'TIPOSERVICIO' VALUE :NEW.TIPOSERVICIO, 'USUARIOINGRESA' VALUE :NEW.USUARIOINGRESA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'USUARIOAUTORIZACION' VALUE :NEW.USUARIOAUTORIZACION, 'USUARIOMODIFICA' VALUE :NEW.USUARIOMODIFICA, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.IDENTIFICACION);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'SECUENCIASERVICIO' VALUE :OLD.SECUENCIASERVICIO, 'CODIGOTIPOSERVICIO' VALUE :OLD.CODIGOTIPOSERVICIO, 'DESCRIPCIONSERVICIO' VALUE :OLD.DESCRIPCIONSERVICIO, 'MONTOSERVICIO' VALUE :OLD.MONTOSERVICIO, 'FECHAINICIO' VALUE :OLD.FECHAINICIO, 'FECHAFIN' VALUE :OLD.FECHAFIN, 'ESTADO' VALUE :OLD.ESTADO, 'CODIGOUSUARIOAUTORIZA' VALUE :OLD.CODIGOUSUARIOAUTORIZA, 'FECHAAUTORIZACION' VALUE :OLD.FECHAAUTORIZACION, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'SECUENCIA' VALUE :OLD.SECUENCIA, 'NUMEROCEDULARECIBECOREO' VALUE :OLD.NUMEROCEDULARECIBECOREO, 'TIPOSERVICIO' VALUE :OLD.TIPOSERVICIO, 'USUARIOINGRESA' VALUE :OLD.USUARIOINGRESA, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'USUARIOAUTORIZACION' VALUE :OLD.USUARIOAUTORIZACION, 'USUARIOMODIFICA' VALUE :OLD.USUARIOMODIFICA, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX
        (AGGREGATE_ID, AGGREGATE_TYPE, EVENT_TYPE, PAYLOAD, SOURCE_TABLE)
    VALUES (v_pk, 'servicioAdicionalType', v_event, v_payload, 'FCME_USER.SERVICIOADICIONAL_TYPE');
END;
/

/* TOTAL FLUJO 2 = 42 triggers */
