/* ==========================================================================
   Triggers de outbox - Modulo TESORERIA  (BD: dbCT)
   Total: 4 triggers
   - trg_outbox_cttbafil_audi       -> auditoriaAfiliadoType
   - trg_outbox_cttbcomi_cred       -> comisionParticipe_type
   - trg_outbox_cttbmatr_dist_afil  -> distribucionAfiliadoType
   - trg_outbox_cttbtabl_afil       -> auditoriaAfiliadoType
   Extraido de all_triggers_dump.sql (lineas 3760-3982)
   ========================================================================== */

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
