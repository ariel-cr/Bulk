/* ============================================================
   DUMP TRIGGERS CARTERA (F1 + F2) - DDL completo
   Snapshot generado del estado actual de las BDs
   ============================================================ */

/* RESUMEN: F1=60 triggers SQL Server, F2=89 triggers Oracle */


/* ############################################################
   FLUJO 1 - Cartera Legacy (dbCR/dbFC/dbCG/dbCT) -> fcme_canonicos.cdc_outbox
   ############################################################ */


/* ----- BD: dbCG (3 triggers) ----- */
USE [dbCG];
GO

/* TOTAL F1 (dbCG) Cartera: 3 triggers */

/* --- trg_outbox_cgtbgara_hipo_cdio  ON dbo.cgtbgara_hipo_cdio (1 type) ---
      - garantiaCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_cgtbgara_hipo_cdio', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cgtbgara_hipo_cdio;
GO
CREATE TRIGGER dbo.trg_outbox_cgtbgara_hipo_cdio
ON dbo.[cgtbgara_hipo_cdio]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'garantiaCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_gara_hipo]),CONVERT(NVARCHAR(200), i.[ci_cedu])), tt.t, @op,
            (SELECT x.[nu_bloq],x.[co_usua_conf],x.[sc_gara_hipo],x.[nu_vill],x.[co_ciud],x.[st_regi],x.[co_prov],x.[nu_manz],x.[fe_elim],x.[fe_ingr],x.[co_empr],x.[ci_cedu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbgara_hipo_cdio', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_gara_hipo]),CONVERT(NVARCHAR(200), d.[ci_cedu])), tt.t, N'DELETE',
            (SELECT x.[nu_bloq],x.[co_usua_conf],x.[sc_gara_hipo],x.[nu_vill],x.[co_ciud],x.[st_regi],x.[co_prov],x.[nu_manz],x.[fe_elim],x.[fe_ingr],x.[co_empr],x.[ci_cedu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbgara_hipo_cdio', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_cgtbprod_cnta_auto  ON dbo.cgtbprod_cnta_auto (2 types) ---
      - cuentaAutomaticaDetalle_type
      - cuentaAutomatica_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_cgtbprod_cnta_auto', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cgtbprod_cnta_auto;
GO
CREATE TRIGGER dbo.trg_outbox_cgtbprod_cnta_auto
ON dbo.[cgtbprod_cnta_auto]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cuentaAutomaticaDetalle_type'),(N'cuentaAutomatica_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_prod]),CONVERT(NVARCHAR(200), i.[co_fond])), tt.t, @op,
            (SELECT x.[co_cnta_auto],x.[co_empr],x.[co_fond],x.[co_prod] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbprod_cnta_auto', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_prod]),CONVERT(NVARCHAR(200), d.[co_fond])), tt.t, N'DELETE',
            (SELECT x.[co_cnta_auto],x.[co_empr],x.[co_fond],x.[co_prod] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbprod_cnta_auto', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_cgtbrepo_anls_cnta  ON dbo.cgtbrepo_anls_cnta (1 type) ---
      - cuentaPorCobrarType
*/
IF OBJECT_ID(N'dbo.trg_outbox_cgtbrepo_anls_cnta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cgtbrepo_anls_cnta;
GO
CREATE TRIGGER dbo.trg_outbox_cgtbrepo_anls_cnta
ON dbo.[cgtbrepo_anls_cnta]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cuentaPorCobrarType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_cnta_cble]),CONVERT(NVARCHAR(200), i.[co_usua])), tt.t, @op,
            (SELECT x.[co_cnta_cble],x.[co_usua] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbrepo_anls_cnta', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_cnta_cble]),CONVERT(NVARCHAR(200), d.[co_usua])), tt.t, N'DELETE',
            (SELECT x.[co_cnta_cble],x.[co_usua] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCG.dbo.cgtbrepo_anls_cnta', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO


/* ----- BD: dbCR (47 triggers) ----- */
USE [dbCR];
GO

/* TOTAL F1 (dbCR) Cartera: 47 triggers */

/* --- trg_outbox_crtbabno_extr  ON dbo.crtbabno_extr (1 type) ---
      - abonoExtraordinario_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbabno_extr', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbabno_extr;
GO
CREATE TRIGGER dbo.trg_outbox_crtbabno_extr
ON dbo.[crtbabno_extr]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'abonoExtraordinario_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_abno])), tt.t, @op,
            (SELECT x.[qs_cred],x.[co_usua_conf],x.[sc_abno],x.[ti_cred],x.[mo_abno_extr],x.[st_regi],x.[fe_elim],x.[co_proc],x.[fe_autr],x.[ds_refe],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbabno_extr', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_abno])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[co_usua_conf],x.[sc_abno],x.[ti_cred],x.[mo_abno_extr],x.[st_regi],x.[fe_elim],x.[co_proc],x.[fe_autr],x.[ds_refe],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbabno_extr', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcart_calf_prov  ON dbo.crtbcart_calf_prov (1 type) ---
      - calificacionCartera_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcart_calf_prov', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcart_calf_prov;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcart_calf_prov
ON dbo.[crtbcart_calf_prov]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'calificacionCartera_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_calf]),CONVERT(NVARCHAR(200), i.[fe_cort])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[nu_dcto],x.[ti_calf],x.[aa_cred],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcart_calf_prov', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_calf]),CONVERT(NVARCHAR(200), d.[fe_cort])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[nu_dcto],x.[ti_calf],x.[aa_cred],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcart_calf_prov', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcaut_cred  ON dbo.crtbcaut_cred (1 type) ---
      - contabilizacionCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcaut_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcaut_cred;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcaut_cred
ON dbo.[crtbcaut_cred]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'contabilizacionCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_regi])), tt.t, @op,
            (SELECT x.[st_regi],x.[fe_elim],x.[ds_asien_cnta],x.[sc_regi],x.[co_usua_elim],x.[co_empr],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcaut_cred', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_regi])), tt.t, N'DELETE',
            (SELECT x.[st_regi],x.[fe_elim],x.[ds_asien_cnta],x.[sc_regi],x.[co_usua_elim],x.[co_empr],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcaut_cred', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbccbr_cred_judi  ON dbo.crtbccbr_cred_judi (7 types) ---
      - autorizacionCredito_type
      - caucionCredito_type
      - conceptoGastoJudicialType
      - etapaJudicialCredito_type
      - medidaJudicialType
      - precalificacionCredito_type
      - unidadJudicialType
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbccbr_cred_judi', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbccbr_cred_judi;
GO
CREATE TRIGGER dbo.trg_outbox_crtbccbr_cred_judi
ON dbo.[crtbccbr_cred_judi]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'autorizacionCredito_type'),(N'caucionCredito_type'),(N'conceptoGastoJudicialType'),(N'etapaJudicialCredito_type'),(N'medidaJudicialType'),(N'precalificacionCredito_type'),(N'unidadJudicialType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_cobr])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_elim],x.[co_usua_ingr],x.[fe_modi],x.[sc_cobr],x.[fe_sald_cred],x.[co_usua_elim],x.[co_empr],x.[aa_cred],x.[co_etap],x.[co_medi],x.[co_rubr],x.[mo_sald_venc],x.[mo_sald_cred],x.[mo_otro],x.[ti_cobr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbccbr_cred_judi', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_cobr])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_elim],x.[co_usua_ingr],x.[fe_modi],x.[sc_cobr],x.[fe_sald_cred],x.[co_usua_elim],x.[co_empr],x.[aa_cred],x.[co_etap],x.[co_medi],x.[co_rubr],x.[mo_sald_venc],x.[mo_sald_cred],x.[mo_otro],x.[ti_cobr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbccbr_cred_judi', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcdeb_cnta  ON dbo.crtbcdeb_cnta (3 types) ---
      - cuentaCuotasType
      - cuentaCxPCxCType
      - cuentaType
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcdeb_cnta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcdeb_cnta;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcdeb_cnta
ON dbo.[crtbcdeb_cnta]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cuentaCuotasType'),(N'cuentaCxPCxCType'),(N'cuentaType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_debi]),CONVERT(NVARCHAR(200), i.[nu_anio])), tt.t, @op,
            (SELECT x.[fe_proc],x.[fe_conf],x.[nu_anio],x.[fe_elim],x.[co_empr],x.[sc_debi],x.[co_usua_elim],x.[co_usua_veri],x.[co_usua_ingr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcdeb_cnta', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_debi]),CONVERT(NVARCHAR(200), d.[nu_anio])), tt.t, N'DELETE',
            (SELECT x.[fe_proc],x.[fe_conf],x.[nu_anio],x.[fe_elim],x.[co_empr],x.[sc_debi],x.[co_usua_elim],x.[co_usua_veri],x.[co_usua_ingr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcdeb_cnta', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcobr_judi_deta  ON dbo.crtbcobr_judi_deta (1 type) ---
      - cobranzaJudicialDetalle_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcobr_judi_deta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcobr_judi_deta;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcobr_judi_deta
ON dbo.[crtbcobr_judi_deta]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cobranzaJudicialDetalle_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_cobr_judi]),CONVERT(NVARCHAR(200), i.[ti_rubr_pagd])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[po_desc],x.[sc_cobr_judi],x.[mo_mvto],x.[mo_aplic],x.[aa_cred],x.[ti_rubr_pagd] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcobr_judi_deta', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_cobr_judi]),CONVERT(NVARCHAR(200), d.[ti_rubr_pagd])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[po_desc],x.[sc_cobr_judi],x.[mo_mvto],x.[mo_aplic],x.[aa_cred],x.[ti_rubr_pagd] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcobr_judi_deta', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcobr_judi_dist  ON dbo.crtbcobr_judi_dist (2 types) ---
      - cobranzaJudicialDistribucion_type
      - cobranzaJudicial_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcobr_judi_dist', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcobr_judi_dist;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcobr_judi_dist
ON dbo.[crtbcobr_judi_dist]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cobranzaJudicialDistribucion_type'),(N'cobranzaJudicial_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_cobr_judi])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[sc_cobr_judi],x.[fe_carg],x.[co_rol],x.[ti_abno],x.[st_regi],x.[aa_cred],x.[fe_elim],x.[ds_url],x.[co_empr],x.[ti_cobr],x.[nu_cpbt],x.[fe_modi],x.[fe_depo],x.[fe_liqu_cred],x.[mo_carg],x.[ti_proc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcobr_judi_dist', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_cobr_judi])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[sc_cobr_judi],x.[fe_carg],x.[co_rol],x.[ti_abno],x.[st_regi],x.[aa_cred],x.[fe_elim],x.[ds_url],x.[co_empr],x.[ti_cobr],x.[nu_cpbt],x.[fe_modi],x.[fe_depo],x.[fe_liqu_cred],x.[mo_carg],x.[ti_proc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcobr_judi_dist', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbconv_pago  ON dbo.crtbconv_pago (1 type) ---
      - convenioPagoCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbconv_pago', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbconv_pago;
GO
CREATE TRIGGER dbo.trg_outbox_crtbconv_pago
ON dbo.[crtbconv_pago]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'convenioPagoCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_conv])), tt.t, @op,
            (SELECT x.[co_usua_conf],x.[ti_cred],x.[st_apli_gara],x.[fe_autr],x.[mo_cobr_gast],x.[mo_intr_venc],x.[fe_ingr_calc],x.[ce_esta_civi],x.[fe_fall_afil],x.[qs_cred],x.[mo_intr_mora],x.[st_regi],x.[fe_elim],x.[ds_obsr],x.[mo_cuot_inic],x.[mo_capi_venc],x.[fe_perd_conv],x.[co_empr],x.[ds_refe],x.[ce_esta_afil],x.[sc_conv],x.[co_proc],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbconv_pago', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_conv])), tt.t, N'DELETE',
            (SELECT x.[co_usua_conf],x.[ti_cred],x.[st_apli_gara],x.[fe_autr],x.[mo_cobr_gast],x.[mo_intr_venc],x.[fe_ingr_calc],x.[ce_esta_civi],x.[fe_fall_afil],x.[qs_cred],x.[mo_intr_mora],x.[st_regi],x.[fe_elim],x.[ds_obsr],x.[mo_cuot_inic],x.[mo_capi_venc],x.[fe_perd_conv],x.[co_empr],x.[ds_refe],x.[ce_esta_afil],x.[sc_conv],x.[co_proc],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbconv_pago', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcred_autr_deta  ON dbo.crtbcred_autr_deta (1 type) ---
      - autorizacionCreditoDetalle_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_autr_deta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcred_autr_deta;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcred_autr_deta
ON dbo.[crtbcred_autr_deta]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'autorizacionCreditoDetalle_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_autr_deta])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_autr_deta],x.[st_autr_deta],x.[sc_autr_deta],x.[sc_rngo_usua],x.[fe_modi],x.[fe_ingr],x.[sc_cred_autr],x.[co_empr],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_autr_deta', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_autr_deta])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_autr_deta],x.[st_autr_deta],x.[sc_autr_deta],x.[sc_rngo_usua],x.[fe_modi],x.[fe_ingr],x.[sc_cred_autr],x.[co_empr],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_autr_deta', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcred_liqd_diar  ON dbo.crtbcred_liqd_diar (2 types) ---
      - liquidacionDiariaCredito_type
      - movimientoContableCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_liqd_diar', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcred_liqd_diar;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcred_liqd_diar
ON dbo.[crtbcred_liqd_diar]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'liquidacionDiariaCredito_type'),(N'movimientoContableCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_liqd])), tt.t, @op,
            (SELECT x.[sc_liqd],x.[qs_cred],x.[mo_rubr],x.[ti_cred],x.[st_cred],x.[st_liqd_diar],x.[co_empr],x.[aa_cred],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_liqd_diar', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_liqd])), tt.t, N'DELETE',
            (SELECT x.[sc_liqd],x.[qs_cred],x.[mo_rubr],x.[ti_cred],x.[st_cred],x.[st_liqd_diar],x.[co_empr],x.[aa_cred],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_liqd_diar', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcred_part  ON dbo.crtbcred_part (2 types) ---
      - flujoTrabajoCredito_type
      - personaCreditoType
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_part', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcred_part;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcred_part
ON dbo.[crtbcred_part]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'flujoTrabajoCredito_type'),(N'personaCreditoType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_elim],x.[nu_iden],x.[co_usua_modi],x.[aa_cred],x.[ti_iden] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_part', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_elim],x.[nu_iden],x.[co_usua_modi],x.[aa_cred],x.[ti_iden] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_part', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcred_plzo_venc  ON dbo.crtbcred_plzo_venc (1 type) ---
      - plazoVencido_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_plzo_venc', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcred_plzo_venc;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcred_plzo_venc
ON dbo.[crtbcred_plzo_venc]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'plazoVencido_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_cred_plzo])), tt.t, @op,
            (SELECT x.[qs_cred],x.[mo_abno_capi],x.[ti_cred],x.[fe_carg],x.[fe_elim],x.[sc_cred_plzo],x.[st_cred_plzo],x.[fe_modi],x.[mo_abno_intr],x.[co_empr],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_plzo_venc', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_cred_plzo])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[mo_abno_capi],x.[ti_cred],x.[fe_carg],x.[fe_elim],x.[sc_cred_plzo],x.[st_cred_plzo],x.[fe_modi],x.[mo_abno_intr],x.[co_empr],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_plzo_venc', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbcred_prea_whts  ON dbo.crtbcred_prea_whts (5 types) ---
      - costoFinancieroCredito_type
      - creditoType
      - desembolsoCredito_type
      - pagosCreditoType
      - refinanciamientoCreditoType
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_prea_whts', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbcred_prea_whts;
GO
CREATE TRIGGER dbo.trg_outbox_crtbcred_prea_whts
ON dbo.[crtbcred_prea_whts]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'costoFinancieroCredito_type'),(N'creditoType'),(N'desembolsoCredito_type'),(N'pagosCreditoType'),(N'refinanciamientoCreditoType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[sc_prea]),CONVERT(NVARCHAR(200), i.[ci_cedu])), tt.t, @op,
            (SELECT x.[st_regi],x.[ti_cred_cncd],x.[mo_cred],x.[mo_suel_liqd],x.[sc_prea],x.[ci_cedu],x.[ds_oper],x.[fe_aprb],x.[co_rol],x.[fe_elim],x.[co_usua_ingr],x.[ti_calf],x.[co_usua_elim],x.[co_usua_aprb],x.[co_comb],x.[fe_ingr],x.[nu_plzo],x.[ds_mail] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_prea_whts', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[sc_prea]),CONVERT(NVARCHAR(200), d.[ci_cedu])), tt.t, N'DELETE',
            (SELECT x.[st_regi],x.[ti_cred_cncd],x.[mo_cred],x.[mo_suel_liqd],x.[sc_prea],x.[ci_cedu],x.[ds_oper],x.[fe_aprb],x.[co_rol],x.[fe_elim],x.[co_usua_ingr],x.[ti_calf],x.[co_usua_elim],x.[co_usua_aprb],x.[co_comb],x.[fe_ingr],x.[nu_plzo],x.[ds_mail] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbcred_prea_whts', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbctrl_oper_ante_sibs  ON dbo.crtbctrl_oper_ante_sibs (1 type) ---
      - reporteSBSOperacionAnterior_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbctrl_oper_ante_sibs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbctrl_oper_ante_sibs;
GO
CREATE TRIGGER dbo.trg_outbox_crtbctrl_oper_ante_sibs
ON dbo.[crtbctrl_oper_ante_sibs]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'reporteSBSOperacionAnterior_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[sc_regi]),CONVERT(NVARCHAR(200), i.[sc_regi_arch]),CONVERT(NVARCHAR(200), i.[ci_cedu]),CONVERT(NVARCHAR(200), i.[nu_oper]),CONVERT(NVARCHAR(200), i.[nu_oper_ante])), tt.t, @op,
            (SELECT x.[sc_regi],x.[fe_ingr],x.[nu_oper_ante],x.[nu_oper],x.[sc_regi_arch],x.[ci_cedu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbctrl_oper_ante_sibs', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[sc_regi]),CONVERT(NVARCHAR(200), d.[sc_regi_arch]),CONVERT(NVARCHAR(200), d.[ci_cedu]),CONVERT(NVARCHAR(200), d.[nu_oper]),CONVERT(NVARCHAR(200), d.[nu_oper_ante])), tt.t, N'DELETE',
            (SELECT x.[sc_regi],x.[fe_ingr],x.[nu_oper_ante],x.[nu_oper],x.[sc_regi_arch],x.[ci_cedu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbctrl_oper_ante_sibs', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbdbso_devo  ON dbo.crtbdbso_devo (2 types) ---
      - desembolsoDevolucion_type
      - devolucionCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbdbso_devo', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbdbso_devo;
GO
CREATE TRIGGER dbo.trg_outbox_crtbdbso_devo
ON dbo.[crtbdbso_devo]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'desembolsoDevolucion_type'),(N'devolucionCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_devo]),CONVERT(NVARCHAR(200), i.[aa_devo]),CONVERT(NVARCHAR(200), i.[qs_devo]),CONVERT(NVARCHAR(200), i.[qs_dbso])), tt.t, @op,
            (SELECT x.[ds_pago],x.[co_usua],x.[qs_dbso],x.[ti_devo],x.[ti_cnta],x.[co_bnco_acre],x.[co_tord],x.[no_bene],x.[nu_cnta],x.[mo_dbso],x.[co_bene],x.[qs_devo],x.[nu_orde],x.[fe_pago],x.[st_dbso],x.[co_bnco],x.[aa_devo] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdbso_devo', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_devo]),CONVERT(NVARCHAR(200), d.[aa_devo]),CONVERT(NVARCHAR(200), d.[qs_devo]),CONVERT(NVARCHAR(200), d.[qs_dbso])), tt.t, N'DELETE',
            (SELECT x.[ds_pago],x.[co_usua],x.[qs_dbso],x.[ti_devo],x.[ti_cnta],x.[co_bnco_acre],x.[co_tord],x.[no_bene],x.[nu_cnta],x.[mo_dbso],x.[co_bene],x.[qs_devo],x.[nu_orde],x.[fe_pago],x.[st_dbso],x.[co_bnco],x.[aa_devo] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdbso_devo', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbdeud_conv  ON dbo.crtbdeud_conv (1 type) ---
      - referenciaDeudor_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbdeud_conv', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbdeud_conv;
GO
CREATE TRIGGER dbo.trg_outbox_crtbdeud_conv
ON dbo.[crtbdeud_conv]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'referenciaDeudor_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_conv]),CONVERT(NVARCHAR(200), i.[co_tipo_deud])), tt.t, @op,
            (SELECT x.[qs_cred],x.[fe_modi_deud],x.[fe_elim_deud],x.[ti_cred],x.[st_regi],x.[fe_crea_deud],x.[co_tipo_deud],x.[aa_cred],x.[co_empr],x.[sc_conv] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdeud_conv', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_conv]),CONVERT(NVARCHAR(200), d.[co_tipo_deud])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[fe_modi_deud],x.[fe_elim_deud],x.[ti_cred],x.[st_regi],x.[fe_crea_deud],x.[co_tipo_deud],x.[aa_cred],x.[co_empr],x.[sc_conv] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdeud_conv', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbdevo_masi_deta  ON dbo.crtbdevo_masi_deta (2 types) ---
      - devolucionMasivaDetalle_type
      - devolucionMasiva_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbdevo_masi_deta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbdevo_masi_deta;
GO
CREATE TRIGGER dbo.trg_outbox_crtbdevo_masi_deta
ON dbo.[crtbdevo_masi_deta]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'devolucionMasivaDetalle_type'),(N'devolucionMasiva_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_devo_deta]),CONVERT(NVARCHAR(200), i.[sc_devo_masi])), tt.t, @op,
            (SELECT x.[sc_mvto],x.[co_rubr_rol],x.[sc_sobr],x.[co_liqd_rubr],x.[mo_disp],x.[sc_devo_deta],x.[st_devo_deta],x.[mo_mvto],x.[sc_devo_masi],x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdevo_masi_deta', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_devo_deta]),CONVERT(NVARCHAR(200), d.[sc_devo_masi])), tt.t, N'DELETE',
            (SELECT x.[sc_mvto],x.[co_rubr_rol],x.[sc_sobr],x.[co_liqd_rubr],x.[mo_disp],x.[sc_devo_deta],x.[st_devo_deta],x.[mo_mvto],x.[sc_devo_masi],x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdevo_masi_deta', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbdocu_cred  ON dbo.crtbdocu_cred (2 types) ---
      - documentoCredito_type
      - grupoCreditoDocumento_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbdocu_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbdocu_cred;
GO
CREATE TRIGGER dbo.trg_outbox_crtbdocu_cred
ON dbo.[crtbdocu_cred]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'documentoCredito_type'),(N'grupoCreditoDocumento_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), i.[co_docu]), tt.t, @op,
            (SELECT x.[st_docu],x.[ds_docu],x.[co_docu],x.[ti_docu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdocu_cred', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[co_docu]), tt.t, N'DELETE',
            (SELECT x.[st_docu],x.[ds_docu],x.[co_docu],x.[ti_docu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdocu_cred', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbdsal_oper  ON dbo.crtbdsal_oper (4 types) ---
      - personaCxPCxCType
      - reporteSBSOperacionCancelada_type
      - reporteSBSOperacionConcedida_type
      - reporteSBSSaldoOperacion_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbdsal_oper', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbdsal_oper;
GO
CREATE TRIGGER dbo.trg_outbox_crtbdsal_oper
ON dbo.[crtbdsal_oper]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'personaCxPCxCType'),(N'reporteSBSOperacionCancelada_type'),(N'reporteSBSOperacionConcedida_type'),(N'reporteSBSSaldoOperacion_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[sc_regi]),CONVERT(NVARCHAR(200), i.[ci_cedu]),CONVERT(NVARCHAR(200), i.[ti_iden]),CONVERT(NVARCHAR(200), i.[nu_oper])), tt.t, @op,
            (SELECT x.[ti_calf],x.[ti_iden],x.[co_tamo],x.[sc_regi],x.[ci_cedu],x.[nu_oper],x.[co_tipo_cred],x.[mo_cuot],x.[fe_docu],x.[pr_inte],x.[mo_prov_requ],x.[mo_capi_cred],x.[pr_inte_mora],x.[mo_cart_cast],x.[mo_suje_prov],x.[mo_venc],x.[mo_prov_cons],x.[mo_cnta_indv],x.[mo_dema_judi],x.[mo_ndev_inte],x.[mo_cost_oper],x.[nu_dias_moro] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdsal_oper', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[sc_regi]),CONVERT(NVARCHAR(200), d.[ci_cedu]),CONVERT(NVARCHAR(200), d.[ti_iden]),CONVERT(NVARCHAR(200), d.[nu_oper])), tt.t, N'DELETE',
            (SELECT x.[ti_calf],x.[ti_iden],x.[co_tamo],x.[sc_regi],x.[ci_cedu],x.[nu_oper],x.[co_tipo_cred],x.[mo_cuot],x.[fe_docu],x.[pr_inte],x.[mo_prov_requ],x.[mo_capi_cred],x.[pr_inte_mora],x.[mo_cart_cast],x.[mo_suje_prov],x.[mo_venc],x.[mo_prov_cons],x.[mo_cnta_indv],x.[mo_dema_judi],x.[mo_ndev_inte],x.[mo_cost_oper],x.[nu_dias_moro] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdsal_oper', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbdvgo_cart_deta  ON dbo.crtbdvgo_cart_deta (2 types) ---
      - devengamientoCarteraDetalle_type
      - devengamientoCartera_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbdvgo_cart_deta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbdvgo_cart_deta;
GO
CREATE TRIGGER dbo.trg_outbox_crtbdvgo_cart_deta
ON dbo.[crtbdvgo_cart_deta]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'devengamientoCarteraDetalle_type'),(N'devengamientoCartera_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_dvgo_deta]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[mo_dvgo_xven],x.[mo_sald_capi],x.[nu_dcto],x.[co_dvgo_deta],x.[fe_ultm_cort],x.[mo_ajus],x.[mo_reve],x.[co_empr],x.[aa_cred],x.[fe_cort],x.[mo_dvgo_venc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdvgo_cart_deta', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_dvgo_deta]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[mo_dvgo_xven],x.[mo_sald_capi],x.[nu_dcto],x.[co_dvgo_deta],x.[fe_ultm_cort],x.[mo_ajus],x.[mo_reve],x.[co_empr],x.[aa_cred],x.[fe_cort],x.[mo_dvgo_venc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdvgo_cart_deta', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbdvgo_cart_deta_diar  ON dbo.crtbdvgo_cart_deta_diar (1 type) ---
      - calificacionCarteraDetalle_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbdvgo_cart_deta_diar', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbdvgo_cart_deta_diar;
GO
CREATE TRIGGER dbo.trg_outbox_crtbdvgo_cart_deta_diar
ON dbo.[crtbdvgo_cart_deta_diar]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'calificacionCarteraDetalle_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[nu_dcto])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[nu_dcto],x.[mo_reve],x.[mo_dvgo],x.[aa_cred],x.[fe_cort],x.[sc_sald] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdvgo_cart_deta_diar', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[nu_dcto])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[nu_dcto],x.[mo_reve],x.[mo_dvgo],x.[aa_cred],x.[fe_cort],x.[sc_sald] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbdvgo_cart_deta_diar', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbesta_conv_cred  ON dbo.crtbesta_conv_cred (1 type) ---
      - estadoConvenioCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbesta_conv_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbesta_conv_cred;
GO
CREATE TRIGGER dbo.trg_outbox_crtbesta_conv_cred
ON dbo.[crtbesta_conv_cred]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'estadoConvenioCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), i.[st_regi_conv]), tt.t, @op,
            (SELECT x.[st_regi],x.[ds_esta],x.[st_regi_conv] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbesta_conv_cred', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[st_regi_conv]), tt.t, N'DELETE',
            (SELECT x.[st_regi],x.[ds_esta],x.[st_regi_conv] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbesta_conv_cred', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbgest_cart_asig  ON dbo.crtbgest_cart_asig (1 type) ---
      - gestionCobranzaAsignacion_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbgest_cart_asig', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbgest_cart_asig;
GO
CREATE TRIGGER dbo.trg_outbox_crtbgest_cart_asig
ON dbo.[crtbgest_cart_asig]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'gestionCobranzaAsignacion_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_gest_cart_asig]),CONVERT(NVARCHAR(200), i.[sc_gene_cart_asig])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[co_usua_ante],x.[ti_calf_homo],x.[co_gest_cart_asig],x.[co_usua_gest],x.[aa_cred],x.[fe_cort],x.[sc_gene_cart_asig] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbgest_cart_asig', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_gest_cart_asig]),CONVERT(NVARCHAR(200), d.[sc_gene_cart_asig])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[co_usua_ante],x.[ti_calf_homo],x.[co_gest_cart_asig],x.[co_usua_gest],x.[aa_cred],x.[fe_cort],x.[sc_gene_cart_asig] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbgest_cart_asig', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbgest_cred  ON dbo.crtbgest_cred (1 type) ---
      - gestionComunicacionCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbgest_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbgest_cred;
GO
CREATE TRIGGER dbo.trg_outbox_crtbgest_cred
ON dbo.[crtbgest_cred]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'gestionComunicacionCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ci_cedu_ejec])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_pago],x.[sc_pago],x.[st_gest],x.[aa_cred],x.[ci_cedu_ejec] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbgest_cred', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ci_cedu_ejec])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_pago],x.[sc_pago],x.[st_gest],x.[aa_cred],x.[ci_cedu_ejec] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbgest_cred', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbinfo_gara_real_sibs  ON dbo.crtbinfo_gara_real_sibs (1 type) ---
      - reporteSBSGarantiaReal_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbinfo_gara_real_sibs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbinfo_gara_real_sibs;
GO
CREATE TRIGGER dbo.trg_outbox_crtbinfo_gara_real_sibs
ON dbo.[crtbinfo_gara_real_sibs]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'reporteSBSGarantiaReal_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[fe_cort])), tt.t, @op,
            (SELECT x.[qs_cred],x.[st_cred],x.[nu_gara_oper],x.[ti_gara],x.[nu_regi],x.[ds_gara],x.[ti_cred],x.[aa_cred],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbinfo_gara_real_sibs', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[fe_cort])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[st_cred],x.[nu_gara_oper],x.[ti_gara],x.[nu_regi],x.[ds_gara],x.[ti_cred],x.[aa_cred],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbinfo_gara_real_sibs', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbinfo_legl  ON dbo.crtbinfo_legl (1 type) ---
      - informacionLegal_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbinfo_legl', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbinfo_legl;
GO
CREATE TRIGGER dbo.trg_outbox_crtbinfo_legl
ON dbo.[crtbinfo_legl]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'informacionLegal_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[co_usua_recp],x.[st_regi],x.[fe_elim],x.[fe_modi],x.[ds_refe],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbinfo_legl', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[co_usua_recp],x.[st_regi],x.[fe_elim],x.[fe_modi],x.[ds_refe],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbinfo_legl', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbobli_rol  ON dbo.crtbobli_rol (1 type) ---
      - obligacionRol_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbobli_rol', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbobli_rol;
GO
CREATE TRIGGER dbo.trg_outbox_crtbobli_rol
ON dbo.[crtbobli_rol]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'obligacionRol_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[ti_desc],x.[co_rol],x.[st_regi],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbobli_rol', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[ti_desc],x.[co_rol],x.[st_regi],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbobli_rol', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtboper_canc  ON dbo.crtboper_canc (1 type) ---
      - cancelacionCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtboper_canc', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtboper_canc;
GO
CREATE TRIGGER dbo.trg_outbox_crtboper_canc
ON dbo.[crtboper_canc]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cancelacionCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_cred_ante]),CONVERT(NVARCHAR(200), i.[aa_cred_ante]),CONVERT(NVARCHAR(200), i.[qs_cred_ante])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[ti_cred_ante],x.[mo_intr],x.[mo_gast_judi],x.[aa_cred],x.[aa_cred_ante],x.[qs_cred_ante] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtboper_canc', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_cred_ante]),CONVERT(NVARCHAR(200), d.[aa_cred_ante]),CONVERT(NVARCHAR(200), d.[qs_cred_ante])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[ti_cred_ante],x.[mo_intr],x.[mo_gast_judi],x.[aa_cred],x.[aa_cred_ante],x.[qs_cred_ante] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtboper_canc', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtboper_dref_liqd  ON dbo.crtboper_dref_liqd (1 type) ---
      - operacionConyugal_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtboper_dref_liqd', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtboper_dref_liqd;
GO
CREATE TRIGGER dbo.trg_outbox_crtboper_dref_liqd
ON dbo.[crtboper_dref_liqd]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'operacionConyugal_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[qs_refe])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[va_liqd],x.[co_tipo_deud],x.[aa_cred],x.[qs_refe] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtboper_dref_liqd', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[qs_refe])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[va_liqd],x.[co_tipo_deud],x.[aa_cred],x.[qs_refe] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtboper_dref_liqd', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbplan_ajus  ON dbo.crtbplan_ajus (1 type) ---
      - planPagoAjuste_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbplan_ajus', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbplan_ajus;
GO
CREATE TRIGGER dbo.trg_outbox_crtbplan_ajus
ON dbo.[crtbplan_ajus]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'planPagoAjuste_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_abno]),CONVERT(NVARCHAR(200), i.[nu_dcto])), tt.t, @op,
            (SELECT x.[qs_cred],x.[sc_abno],x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[pl_dias],x.[mo_capi],x.[mo_incd],x.[mo_comi],x.[mo_intr],x.[fe_vcto],x.[mo_inte_pmes],x.[aa_cred],x.[mo_segu],x.[nu_dcto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbplan_ajus', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_abno]),CONVERT(NVARCHAR(200), d.[nu_dcto])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[sc_abno],x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[pl_dias],x.[mo_capi],x.[mo_incd],x.[mo_comi],x.[mo_intr],x.[fe_vcto],x.[mo_inte_pmes],x.[aa_cred],x.[mo_segu],x.[nu_dcto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbplan_ajus', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbplpg_conv  ON dbo.crtbplpg_conv (1 type) ---
      - cuotaConvenio_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbplpg_conv', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbplpg_conv;
GO
CREATE TRIGGER dbo.trg_outbox_crtbplpg_conv
ON dbo.[crtbplpg_conv]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cuotaConvenio_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[nu_dcto]),CONVERT(NVARCHAR(200), i.[sc_dcto]),CONVERT(NVARCHAR(200), i.[sc_conv])), tt.t, @op,
            (SELECT x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[mo_abno_mora],x.[fe_vcto],x.[mo_segu],x.[mo_incd],x.[mo_dvgo_diar],x.[sc_dcto],x.[sc_rol],x.[fe_intr],x.[qs_cred],x.[mo_dvgd_intr],x.[st_cred],x.[fe_elim],x.[st_vcto],x.[mo_intr],x.[in_reve_dvgo],x.[mo_dvgd_mora],x.[mo_dvgo_acum],x.[fe_ultm_envi],x.[nu_anos],x.[mo_abno_capi],x.[nu_dias],x.[mo_capi],x.[sc_conv],x.[mo_comi],x.[mo_abno_intr],x.[mo_gast_judi],x.[fe_inic_venc],x.[mo_inte_pmes],x.[aa_cred],x.[nu_dcto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbplpg_conv', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[nu_dcto]),CONVERT(NVARCHAR(200), d.[sc_dcto]),CONVERT(NVARCHAR(200), d.[sc_conv])), tt.t, N'DELETE',
            (SELECT x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[mo_abno_mora],x.[fe_vcto],x.[mo_segu],x.[mo_incd],x.[mo_dvgo_diar],x.[sc_dcto],x.[sc_rol],x.[fe_intr],x.[qs_cred],x.[mo_dvgd_intr],x.[st_cred],x.[fe_elim],x.[st_vcto],x.[mo_intr],x.[in_reve_dvgo],x.[mo_dvgd_mora],x.[mo_dvgo_acum],x.[fe_ultm_envi],x.[nu_anos],x.[mo_abno_capi],x.[nu_dias],x.[mo_capi],x.[sc_conv],x.[mo_comi],x.[mo_abno_intr],x.[mo_gast_judi],x.[fe_inic_venc],x.[mo_inte_pmes],x.[aa_cred],x.[nu_dcto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbplpg_conv', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbrecu_conv  ON dbo.crtbrecu_conv (4 types) ---
      - detalleRecuperacion_type
      - recuperacionConvenio_type
      - recuperacionCredito_type
      - transaccionRecuperacion_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbrecu_conv', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbrecu_conv;
GO
CREATE TRIGGER dbo.trg_outbox_crtbrecu_conv
ON dbo.[crtbrecu_conv]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'detalleRecuperacion_type'),(N'recuperacionConvenio_type'),(N'recuperacionCredito_type'),(N'transaccionRecuperacion_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[qs_abno])), tt.t, @op,
            (SELECT x.[qs_abno],x.[qs_cred],x.[ti_cred],x.[st_mvto],x.[mo_mvto],x.[aa_cred],x.[ds_liqd],x.[co_usua_conf],x.[fe_mvto],x.[st_autr],x.[co_usua_revz],x.[ti_recp],x.[fe_revz],x.[co_usua_liqd],x.[st_regi],x.[ti_revz],x.[nu_cpbt_cble],x.[fe_abno],x.[nu_dias_atra],x.[co_rol],x.[fe_cble],x.[ti_diar],x.[in_cble_revz],x.[in_conf_fond] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbrecu_conv', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[qs_abno])), tt.t, N'DELETE',
            (SELECT x.[qs_abno],x.[qs_cred],x.[ti_cred],x.[st_mvto],x.[mo_mvto],x.[aa_cred],x.[ds_liqd],x.[co_usua_conf],x.[fe_mvto],x.[st_autr],x.[co_usua_revz],x.[ti_recp],x.[fe_revz],x.[co_usua_liqd],x.[st_regi],x.[ti_revz],x.[nu_cpbt_cble],x.[fe_abno],x.[nu_dias_atra],x.[co_rol],x.[fe_cble],x.[ti_diar],x.[in_cble_revz],x.[in_conf_fond] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbrecu_conv', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbrepo_sobr  ON dbo.crtbrepo_sobr (1 type) ---
      - reporteSBSSujetoRiesgo_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbrepo_sobr', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbrepo_sobr;
GO
CREATE TRIGGER dbo.trg_outbox_crtbrepo_sobr
ON dbo.[crtbrepo_sobr]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'reporteSBSSujetoRiesgo_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[qs_Cred]),CONVERT(NVARCHAR(200), i.[co_rol]),CONVERT(NVARCHAR(200), i.[co_prov])), tt.t, @op,
            (SELECT x.[qs_Cred],x.[co_rol],x.[co_prov],x.[nu_ctas],x.[ti_inst],x.[co_usua] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbrepo_sobr', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[qs_Cred]),CONVERT(NVARCHAR(200), d.[co_rol]),CONVERT(NVARCHAR(200), d.[co_prov])), tt.t, N'DELETE',
            (SELECT x.[qs_Cred],x.[co_rol],x.[co_prov],x.[nu_ctas],x.[ti_inst],x.[co_usua] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbrepo_sobr', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbrngo_autr_cred  ON dbo.crtbrngo_autr_cred (1 type) ---
      - cuotaCreditoType
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbrngo_autr_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbrngo_autr_cred;
GO
CREATE TRIGGER dbo.trg_outbox_crtbrngo_autr_cred
ON dbo.[crtbrngo_autr_cred]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cuotaCreditoType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_rngo])), tt.t, @op,
            (SELECT x.[co_usua],x.[co_grup],x.[fe_elim],x.[fe_ingr],x.[co_empr],x.[sc_rngo] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbrngo_autr_cred', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_rngo])), tt.t, N'DELETE',
            (SELECT x.[co_usua],x.[co_grup],x.[fe_elim],x.[fe_ingr],x.[co_empr],x.[sc_rngo] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbrngo_autr_cred', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbrngo_intr_cred  ON dbo.crtbrngo_intr_cred (1 type) ---
      - tasaInteresCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbrngo_intr_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbrngo_intr_cred;
GO
CREATE TRIGGER dbo.trg_outbox_crtbrngo_intr_cred
ON dbo.[crtbrngo_intr_cred]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'tasaInteresCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_rngo]),CONVERT(NVARCHAR(200), i.[co_usua]),CONVERT(NVARCHAR(200), i.[co_carg])), tt.t, @op,
            (SELECT x.[co_empr],x.[sc_rngo],x.[co_usua],x.[co_carg] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbrngo_intr_cred', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_rngo]),CONVERT(NVARCHAR(200), d.[co_usua]),CONVERT(NVARCHAR(200), d.[co_carg])), tt.t, N'DELETE',
            (SELECT x.[co_empr],x.[sc_rngo],x.[co_usua],x.[co_carg] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbrngo_intr_cred', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbsald_cart  ON dbo.crtbsald_cart (1 type) ---
      - saldoCartera_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbsald_cart', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbsald_cart;
GO
CREATE TRIGGER dbo.trg_outbox_crtbsald_cart
ON dbo.[crtbsald_cart]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'saldoCartera_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[fe_sald]),CONVERT(NVARCHAR(200), i.[co_fond]),CONVERT(NVARCHAR(200), i.[ti_cred])), tt.t, @op,
            (SELECT x.[mo_sald_capi_xven],x.[mo_abno_capi],x.[ti_cred],x.[mo_abno_capi_xven],x.[mo_abno_capi_vcdo],x.[mo_abno_inte],x.[mo_capi],x.[co_fond],x.[nu_oper],x.[fe_sald],x.[mo_abno_mora],x.[mo_inte],x.[co_empr],x.[mo_sald_capi_vcdo],x.[mo_inte_reve_vcdo] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsald_cart', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[fe_sald]),CONVERT(NVARCHAR(200), d.[co_fond]),CONVERT(NVARCHAR(200), d.[ti_cred])), tt.t, N'DELETE',
            (SELECT x.[mo_sald_capi_xven],x.[mo_abno_capi],x.[ti_cred],x.[mo_abno_capi_xven],x.[mo_abno_capi_vcdo],x.[mo_abno_inte],x.[mo_capi],x.[co_fond],x.[nu_oper],x.[fe_sald],x.[mo_abno_mora],x.[mo_inte],x.[co_empr],x.[mo_sald_capi_vcdo],x.[mo_inte_reve_vcdo] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsald_cart', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbsald_cart_deta  ON dbo.crtbsald_cart_deta (1 type) ---
      - saldoCarteraDetalle_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbsald_cart_deta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbsald_cart_deta;
GO
CREATE TRIGGER dbo.trg_outbox_crtbsald_cart_deta
ON dbo.[crtbsald_cart_deta]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'saldoCarteraDetalle_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[fe_cort])), tt.t, @op,
            (SELECT x.[qs_cred],x.[mo_sald_capi_xven],x.[ti_cred],x.[mo_inte_abno],x.[nu_dcto],x.[mo_sald_capi_vcdo],x.[mo_inte_dvgo],x.[co_empr],x.[aa_cred],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsald_cart_deta', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[fe_cort])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[mo_sald_capi_xven],x.[ti_cred],x.[mo_inte_abno],x.[nu_dcto],x.[mo_sald_capi_vcdo],x.[mo_inte_dvgo],x.[co_empr],x.[aa_cred],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsald_cart_deta', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbsegi_autr_ofic  ON dbo.crtbsegi_autr_ofic (1 type) ---
      - seguimientoAutorizacion_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbsegi_autr_ofic', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbsegi_autr_ofic;
GO
CREATE TRIGGER dbo.trg_outbox_crtbsegi_autr_ofic
ON dbo.[crtbsegi_autr_ofic]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguimientoAutorizacion_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_segi])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_actu],x.[co_prov],x.[co_usua_tran],x.[st_segi],x.[ds_obsr],x.[sc_segi],x.[co_empr],x.[aa_cred],x.[co_moti] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsegi_autr_ofic', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_segi])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_actu],x.[co_prov],x.[co_usua_tran],x.[st_segi],x.[ds_obsr],x.[sc_segi],x.[co_empr],x.[aa_cred],x.[co_moti] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsegi_autr_ofic', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbsegu_cred  ON dbo.crtbsegu_cred (1 type) ---
      - seguroCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbsegu_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbsegu_cred;
GO
CREATE TRIGGER dbo.trg_outbox_crtbsegu_cred
ON dbo.[crtbsegu_cred]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'seguroCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_segu]),CONVERT(NVARCHAR(200), i.[co_moti])), tt.t, @op,
            (SELECT x.[sc_segu],x.[co_moti],x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsegu_cred', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_segu]),CONVERT(NVARCHAR(200), d.[co_moti])), tt.t, N'DELETE',
            (SELECT x.[sc_segu],x.[co_moti],x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsegu_cred', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbsobr_caut  ON dbo.crtbsobr_caut (1 type) ---
      - sobranteCaucion_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbsobr_caut', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbsobr_caut;
GO
CREATE TRIGGER dbo.trg_outbox_crtbsobr_caut
ON dbo.[crtbsobr_caut]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'sobranteCaucion_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_sobr]),CONVERT(NVARCHAR(200), i.[co_fond])), tt.t, @op,
            (SELECT x.[co_empr],x.[ti_sobr],x.[co_fond],x.[va_cnta_auto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsobr_caut', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_sobr]),CONVERT(NVARCHAR(200), d.[co_fond])), tt.t, N'DELETE',
            (SELECT x.[co_empr],x.[ti_sobr],x.[co_fond],x.[va_cnta_auto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbsobr_caut', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtbtipo_cred_sibs  ON dbo.crtbtipo_cred_sibs (1 type) ---
      - tipoCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtbtipo_cred_sibs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtbtipo_cred_sibs;
GO
CREATE TRIGGER dbo.trg_outbox_crtbtipo_cred_sibs
ON dbo.[crtbtipo_cred_sibs]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'tipoCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), i.[sc_tcre]), tt.t, @op,
            (SELECT x.[co_grup_fcme],x.[ds_tcre],x.[st_tcre],x.[co_tcre],x.[sc_tcre] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbtipo_cred_sibs', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[sc_tcre]), tt.t, N'DELETE',
            (SELECT x.[co_grup_fcme],x.[ds_tcre],x.[st_tcre],x.[co_tcre],x.[sc_tcre] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtbtipo_cred_sibs', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtpagos  ON dbo.crtpagos (1 type) ---
      - pagoCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtpagos', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtpagos;
GO
CREATE TRIGGER dbo.trg_outbox_crtpagos
ON dbo.[crtpagos]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'pagoCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[qs_abno]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[sc_reca])), tt.t, @op,
            (SELECT x.[qs_abno],x.[ce_regi],x.[qs_cred],x.[ti_cred],x.[sc_reca],x.[fx_pago],x.[aa_cred],x.[va_pagr],x.[co_prov],x.[co_fond],x.[ti_pago],x.[va_abno],x.[ci_rol],x.[fx_proc],x.[sc_rol],x.[ti_inst],x.[co_paga] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtpagos', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[qs_abno]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[sc_reca])), tt.t, N'DELETE',
            (SELECT x.[qs_abno],x.[ce_regi],x.[qs_cred],x.[ti_cred],x.[sc_reca],x.[fx_pago],x.[aa_cred],x.[va_pagr],x.[co_prov],x.[co_fond],x.[ti_pago],x.[va_abno],x.[ci_rol],x.[fx_proc],x.[sc_rol],x.[ti_inst],x.[co_paga] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtpagos', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtplpag  ON dbo.crtplpag (1 type) ---
      - planPago_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtplpag', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtplpag;
GO
CREATE TRIGGER dbo.trg_outbox_crtplpag
ON dbo.[crtplpag]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'planPago_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[nu_dcto]),CONVERT(NVARCHAR(200), i.[sc_dcto])), tt.t, @op,
            (SELECT x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[mo_abno_mora],x.[mo_segu],x.[mo_dvgo_diar],x.[mo_incd],x.[sc_dcto],x.[sc_rol],x.[mo_rast],x.[qs_cred],x.[mo_dvgd_intr],x.[fe_elim],x.[st_vcto],x.[st_pago_fcme],x.[mo_intr],x.[in_reve_dvgo],x.[mo_dvgd_mora],x.[mo_dvgo_acum],x.[fe_ultm_envi],x.[mo_abno_capi],x.[fx_pago_fcme],x.[mo_capi],x.[mo_comi],x.[mo_abno_intr],x.[mo_gast_judi],x.[fe_inic_venc],x.[mo_inte_pmes],x.[aa_cred],x.[nu_dcto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtplpag', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[nu_dcto]),CONVERT(NVARCHAR(200), d.[sc_dcto])), tt.t, N'DELETE',
            (SELECT x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[mo_abno_mora],x.[mo_segu],x.[mo_dvgo_diar],x.[mo_incd],x.[sc_dcto],x.[sc_rol],x.[mo_rast],x.[qs_cred],x.[mo_dvgd_intr],x.[fe_elim],x.[st_vcto],x.[st_pago_fcme],x.[mo_intr],x.[in_reve_dvgo],x.[mo_dvgd_mora],x.[mo_dvgo_acum],x.[fe_ultm_envi],x.[mo_abno_capi],x.[fx_pago_fcme],x.[mo_capi],x.[mo_comi],x.[mo_abno_intr],x.[mo_gast_judi],x.[fe_inic_venc],x.[mo_inte_pmes],x.[aa_cred],x.[nu_dcto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtplpag', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtrepo_sobr  ON dbo.crtrepo_sobr (3 types) ---
      - reporteSBSCabecera_type
      - reporteSBSDetalle_type
      - tipoSobrante_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtrepo_sobr', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtrepo_sobr;
GO
CREATE TRIGGER dbo.trg_outbox_crtrepo_sobr
ON dbo.[crtrepo_sobr]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'reporteSBSCabecera_type'),(N'reporteSBSDetalle_type'),(N'tipoSobrante_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_sobr]),CONVERT(NVARCHAR(200), i.[co_usua])), tt.t, @op,
            (SELECT x.[co_afil],x.[co_usua],x.[fe_devo],x.[fe_cort],x.[co_rol],x.[co_inst],x.[ti_sobr],x.[ds_obse] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtrepo_sobr', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_sobr]),CONVERT(NVARCHAR(200), d.[co_usua])), tt.t, N'DELETE',
            (SELECT x.[co_afil],x.[co_usua],x.[fe_devo],x.[fe_cort],x.[co_rol],x.[co_inst],x.[ti_sobr],x.[ds_obse] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtrepo_sobr', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtrubros_cobr  ON dbo.crtrubros_cobr (2 types) ---
      - rubroCobranza_type
      - rubrosCobranzaDetalle_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtrubros_cobr', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtrubros_cobr;
GO
CREATE TRIGGER dbo.trg_outbox_crtrubros_cobr
ON dbo.[crtrubros_cobr]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'rubroCobranza_type'),(N'rubrosCobranzaDetalle_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_rubr_pago]),CONVERT(NVARCHAR(200), i.[ti_pago])), tt.t, @op,
            (SELECT x.[ti_cred],x.[co_empr],x.[ti_rubr_pago],x.[ti_pago] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtrubros_cobr', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_rubr_pago]),CONVERT(NVARCHAR(200), d.[ti_pago])), tt.t, N'DELETE',
            (SELECT x.[ti_cred],x.[co_empr],x.[ti_rubr_pago],x.[ti_pago] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtrubros_cobr', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtsobrante  ON dbo.crtsobrante (2 types) ---
      - sobranteCredito_type
      - sobranteDistribucion_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtsobrante', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtsobrante;
GO
CREATE TRIGGER dbo.trg_outbox_crtsobrante
ON dbo.[crtsobrante]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'sobranteCredito_type'),(N'sobranteDistribucion_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), i.[sc_sobr]), tt.t, @op,
            (SELECT x.[ti_apli],x.[co_fond],x.[ti_pago],x.[st_devo],x.[fe_devo],x.[fe_proc],x.[ds_oper_refe],x.[sc_rol],x.[co_paga],x.[ti_sobr],x.[mo_disp],x.[nu_cpbt_cble],x.[ci_rol],x.[co_empr],x.[mo_sobr],x.[ti_inst],x.[sc_reca],x.[sc_sobr],x.[co_prov],x.[fe_cort],x.[ds_obse] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtsobrante', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[sc_sobr]), tt.t, N'DELETE',
            (SELECT x.[ti_apli],x.[co_fond],x.[ti_pago],x.[st_devo],x.[fe_devo],x.[fe_proc],x.[ds_oper_refe],x.[sc_rol],x.[co_paga],x.[ti_sobr],x.[mo_disp],x.[nu_cpbt_cble],x.[ci_rol],x.[co_empr],x.[mo_sobr],x.[ti_inst],x.[sc_reca],x.[sc_sobr],x.[co_prov],x.[fe_cort],x.[ds_obse] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtsobrante', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_crtsolid  ON dbo.crtsolid (1 type) ---
      - solidarioCredito_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_crtsolid', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_crtsolid;
GO
CREATE TRIGGER dbo.trg_outbox_crtsolid
ON dbo.[crtsolid]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'solidarioCredito_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred])), tt.t, @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[mo_cuot],x.[in_soli],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtsolid', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[ti_cred],x.[mo_cuot],x.[in_soli],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCR.dbo.crtsolid', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO


/* ----- BD: dbCT (4 triggers) ----- */
USE [dbCT];
GO

/* TOTAL F1 (dbCT) Cartera: 4 triggers */

/* --- trg_outbox_cttbesta_docu_inve  ON dbo.cttbesta_docu_inve (1 type) ---
      - estadoLegalType
*/
IF OBJECT_ID(N'dbo.trg_outbox_cttbesta_docu_inve', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cttbesta_docu_inve;
GO
CREATE TRIGGER dbo.trg_outbox_cttbesta_docu_inve
ON dbo.[cttbesta_docu_inve]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'estadoLegalType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), i.[nu_oper]), tt.t, @op,
            (SELECT x.[ds_esta_docu_inve],x.[in_revi_docu],x.[mo_reca_inve],x.[nu_oper] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbesta_docu_inve', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[nu_oper]), tt.t, N'DELETE',
            (SELECT x.[ds_esta_docu_inve],x.[in_revi_docu],x.[mo_reca_inve],x.[nu_oper] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbesta_docu_inve', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_cttbproc_obse_tran  ON dbo.cttbproc_obse_tran (1 type) ---
      - procesoAccionType
*/
IF OBJECT_ID(N'dbo.trg_outbox_cttbproc_obse_tran', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cttbproc_obse_tran;
GO
CREATE TRIGGER dbo.trg_outbox_cttbproc_obse_tran
ON dbo.[cttbproc_obse_tran]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'procesoAccionType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_proc_obse]),CONVERT(NVARCHAR(200), i.[co_apli]),CONVERT(NVARCHAR(200), i.[co_func]),CONVERT(NVARCHAR(200), i.[nu_tran]),CONVERT(NVARCHAR(200), i.[ti_loca])), tt.t, @op,
            (SELECT x.[nu_tran],x.[co_proc_obse],x.[co_apli],x.[co_func],x.[ti_loca] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbproc_obse_tran', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_proc_obse]),CONVERT(NVARCHAR(200), d.[co_apli]),CONVERT(NVARCHAR(200), d.[co_func]),CONVERT(NVARCHAR(200), d.[nu_tran]),CONVERT(NVARCHAR(200), d.[ti_loca])), tt.t, N'DELETE',
            (SELECT x.[nu_tran],x.[co_proc_obse],x.[co_apli],x.[co_func],x.[ti_loca] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbproc_obse_tran', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_cttbrepo_gene  ON dbo.cttbrepo_gene (1 type) ---
      - reporteSBSGaranteCodeudor_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_cttbrepo_gene', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cttbrepo_gene;
GO
CREATE TRIGGER dbo.trg_outbox_cttbrepo_gene
ON dbo.[cttbrepo_gene]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'reporteSBSGaranteCodeudor_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])), tt.t, @op,
            (SELECT x.[qs_cred],x.[nu_oper_canc],x.[nu_rol_indi],x.[nu_plaz],x.[co_inst_gara],x.[fe_naci],x.[ti_calf],x.[ti_cred],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbrepo_gene', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])), tt.t, N'DELETE',
            (SELECT x.[qs_cred],x.[nu_oper_canc],x.[nu_rol_indi],x.[nu_plaz],x.[co_inst_gara],x.[fe_naci],x.[ti_calf],x.[ti_cred],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbrepo_gene', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_cttbtran_inve_auxi  ON dbo.cttbtran_inve_auxi (1 type) ---
      - auxDatosCobrosAdicionalesType
*/
IF OBJECT_ID(N'dbo.trg_outbox_cttbtran_inve_auxi', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_cttbtran_inve_auxi;
GO
CREATE TRIGGER dbo.trg_outbox_cttbtran_inve_auxi
ON dbo.[cttbtran_inve_auxi]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'auxDatosCobrosAdicionalesType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ci_cedu]),CONVERT(NVARCHAR(200), i.[sc_auxi])), tt.t, @op,
            (SELECT x.[co_usua],x.[mo_inve_fide],x.[ci_cedu],x.[sc_auxi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbtran_inve_auxi', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ci_cedu]),CONVERT(NVARCHAR(200), d.[sc_auxi])), tt.t, N'DELETE',
            (SELECT x.[co_usua],x.[mo_inve_fide],x.[ci_cedu],x.[sc_auxi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbCT.dbo.cttbtran_inve_auxi', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO


/* ----- BD: dbFC (6 triggers) ----- */
USE [dbFC];
GO

/* TOTAL F1 (dbFC) Cartera: 6 triggers */

/* --- trg_outbox_fctbdeta_liqd_cred  ON dbo.fctbdeta_liqd_cred (1 type) ---
      - grupoCreditoDetalle_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_fctbdeta_liqd_cred', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbdeta_liqd_cred;
GO
CREATE TRIGGER dbo.trg_outbox_fctbdeta_liqd_cred
ON dbo.[fctbdeta_liqd_cred]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'grupoCreditoDetalle_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_liqd]),CONVERT(NVARCHAR(200), i.[sc_deta]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])), tt.t, @op,
            (SELECT x.[sc_liqd],x.[co_rubr],x.[sc_deta],x.[ti_cred],x.[aa_cred],x.[qs_cred],x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdeta_liqd_cred', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_liqd]),CONVERT(NVARCHAR(200), d.[sc_deta]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])), tt.t, N'DELETE',
            (SELECT x.[sc_liqd],x.[co_rubr],x.[sc_deta],x.[ti_cred],x.[aa_cred],x.[qs_cred],x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdeta_liqd_cred', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_fctbdinf_liqd_cnta_sibs  ON dbo.fctbdinf_liqd_cnta_sibs (1 type) ---
      - cuentasEnLegalType
*/
IF OBJECT_ID(N'dbo.trg_outbox_fctbdinf_liqd_cnta_sibs', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbdinf_liqd_cnta_sibs;
GO
CREATE TRIGGER dbo.trg_outbox_fctbdinf_liqd_cnta_sibs
ON dbo.[fctbdinf_liqd_cnta_sibs]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cuentasEnLegalType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[sc_regi]),CONVERT(NVARCHAR(200), i.[ci_cedu])), tt.t, @op,
            (SELECT x.[mo_desc],x.[ti_iden],x.[sc_regi],x.[ci_cedu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_liqd_cnta_sibs', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[sc_regi]),CONVERT(NVARCHAR(200), d.[ci_cedu])), tt.t, N'DELETE',
            (SELECT x.[mo_desc],x.[ti_iden],x.[sc_regi],x.[ci_cedu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbdinf_liqd_cnta_sibs', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_fctbmvto_impr_esta_cnta  ON dbo.fctbmvto_impr_esta_cnta (1 type) ---
      - cuentaPersonasType
*/
IF OBJECT_ID(N'dbo.trg_outbox_fctbmvto_impr_esta_cnta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbmvto_impr_esta_cnta;
GO
CREATE TRIGGER dbo.trg_outbox_fctbmvto_impr_esta_cnta
ON dbo.[fctbmvto_impr_esta_cnta]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'cuentaPersonasType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[ci_cedu]),CONVERT(NVARCHAR(200), i.[co_tran]),CONVERT(NVARCHAR(200), i.[co_grup_cnta]),CONVERT(NVARCHAR(200), i.[co_rol])), tt.t, @op,
            (SELECT x.[co_grup_cnta],x.[nu_prio],x.[co_tipo_tran],x.[ci_cedu],x.[co_tran],x.[co_rol] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbmvto_impr_esta_cnta', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[ci_cedu]),CONVERT(NVARCHAR(200), d.[co_tran]),CONVERT(NVARCHAR(200), d.[co_grup_cnta]),CONVERT(NVARCHAR(200), d.[co_rol])), tt.t, N'DELETE',
            (SELECT x.[co_grup_cnta],x.[nu_prio],x.[co_tipo_tran],x.[ci_cedu],x.[co_tran],x.[co_rol] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbmvto_impr_esta_cnta', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_fctbproc_tseg_noti  ON dbo.fctbproc_tseg_noti (1 type) ---
      - fechasProcesoType
*/
IF OBJECT_ID(N'dbo.trg_outbox_fctbproc_tseg_noti', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbproc_tseg_noti;
GO
CREATE TRIGGER dbo.trg_outbox_fctbproc_tseg_noti
ON dbo.[fctbproc_tseg_noti]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'fechasProcesoType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_proc]),CONVERT(NVARCHAR(200), i.[co_tseg]),CONVERT(NVARCHAR(200), i.[co_noti]),CONVERT(NVARCHAR(200), i.[sc_regi])), tt.t, @op,
            (SELECT x.[co_proc],x.[co_usua_crea],x.[co_mens_telf],x.[co_noti],x.[co_empr],x.[co_tseg],x.[sc_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbproc_tseg_noti', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_proc]),CONVERT(NVARCHAR(200), d.[co_tseg]),CONVERT(NVARCHAR(200), d.[co_noti]),CONVERT(NVARCHAR(200), d.[sc_regi])), tt.t, N'DELETE',
            (SELECT x.[co_proc],x.[co_usua_crea],x.[co_mens_telf],x.[co_noti],x.[co_empr],x.[co_tseg],x.[sc_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbproc_tseg_noti', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_sfct_afiliado_referencias  ON dbo.sfct_afiliado_referencias (1 type) ---
      - referenciaCliente_type
*/
IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_referencias', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_afiliado_referencias;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_referencias
ON dbo.[sfct_afiliado_referencias]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'referenciaCliente_type');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[sc_refe]),CONVERT(NVARCHAR(200), i.[co_tref]),CONVERT(NVARCHAR(200), i.[ci_cedula])), tt.t, @op,
            (SELECT x.[sc_refe],x.[st_cart],x.[co_tref],x.[fe_ingr],x.[ci_cedula] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_referencias', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[sc_refe]),CONVERT(NVARCHAR(200), d.[co_tref]),CONVERT(NVARCHAR(200), d.[ci_cedula])), tt.t, N'DELETE',
            (SELECT x.[sc_refe],x.[st_cart],x.[co_tref],x.[fe_ingr],x.[ci_cedula] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_afiliado_referencias', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO

/* --- trg_outbox_sfct_saldos_diarios_afiliados  ON dbo.sfct_saldos_diarios_afiliados (2 types) ---
      - saldoCxPCxCType
      - saldoVinculadoType
*/
IF OBJECT_ID(N'dbo.trg_outbox_sfct_saldos_diarios_afiliados', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sfct_saldos_diarios_afiliados;
GO
CREATE TRIGGER dbo.trg_outbox_sfct_saldos_diarios_afiliados
ON dbo.[sfct_saldos_diarios_afiliados]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N'saldoCxPCxCType'),(N'saldoVinculadoType');

    IF @op IN (N'INSERT', N'UPDATE')
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[fx_saldo]),CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_fond]),CONVERT(NVARCHAR(200), i.[ci_cedula])), tt.t, @op,
            (SELECT x.[co_empr],x.[co_fond],x.[fx_saldo],x.[ci_cedula] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_saldos_diarios_afiliados', SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[fx_saldo]),CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_fond]),CONVERT(NVARCHAR(200), d.[ci_cedula])), tt.t, N'DELETE',
            (SELECT x.[co_empr],x.[co_fond],x.[fx_saldo],x.[ci_cedula] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.sfct_saldos_diarios_afiliados', SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
END
GO


/* ############################################################
   FLUJO 2 - Cartera FCME_USER -> FCME_USER.CDC_OUTBOX
   ############################################################ */

/* --- TRG_OUTBOX_ABONOEXTRAORDINARIO  ON FCME_USER.ABONOEXTRAORDINARIO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ABONOEXTRAORDINARIO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."ABONOEXTRAORDINARIO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONOPROCESO' VALUE :NEW.SECUENCIAABONOPROCESO, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'MONTOABONOEXTRAORDINARIO' VALUE :NEW.MONTOABONOEXTRAORDINARIO, 'DESCRIPCIONREFERENCIA' VALUE :NEW.DESCRIPCIONREFERENCIA, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONOPROCESO' VALUE :NEW.SECUENCIAABONOPROCESO, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'MONTOABONOEXTRAORDINARIO' VALUE :NEW.MONTOABONOEXTRAORDINARIO, 'DESCRIPCIONREFERENCIA' VALUE :NEW.DESCRIPCIONREFERENCIA, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAABONOPROCESO' VALUE :OLD.SECUENCIAABONOPROCESO, 'CODIGOPROCESO' VALUE :OLD.CODIGOPROCESO, 'MONTOABONOEXTRAORDINARIO' VALUE :OLD.MONTOABONOEXTRAORDINARIO, 'DESCRIPCIONREFERENCIA' VALUE :OLD.DESCRIPCIONREFERENCIA, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHAAUTORIZACION' VALUE :OLD.FECHAAUTORIZACION, 'FECHAVERIFICACION' VALUE :OLD.FECHAVERIFICACION, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'CODIGOUSUARIOCONFIRMA' VALUE :OLD.CODIGOUSUARIOCONFIRMA, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('abonoExtraordinario_type', v_pk, v_event, v_payload, 'FCME_USER.ABONOEXTRAORDINARIO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_AUTRCREDDETA  ON FCME_USER.AUTORIZACIONCREDITODETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_AUTRCREDDETA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."AUTORIZACIONCREDITODETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIACREDITOAUTORIZACION' VALUE :NEW.SECUENCIACREDITOAUTORIZACION, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'CODIGOEMPRESACREDAUTR' VALUE :NEW.CODIGOEMPRESACREDAUTR, 'SECUENCIAAUTORIZACIONDETALLE' VALUE :NEW.SECUENCIAAUTORIZACIONDETALLE, 'SECUENCIACREDITOAUTORIZACIONCREDAUTR' VALUE :NEW.SECUENCIACREDITOAUTORIZACIONCREDAUTR, 'SECUENCIARNGOUSUARIO' VALUE :NEW.SECUENCIARNGOUSUARIO, 'TIPOCREDITOCREDAUTR' VALUE :NEW.TIPOCREDITOCREDAUTR, 'ANIOCREDITOCREDAUTR' VALUE :NEW.ANIOCREDITOCREDAUTR, 'SECUENCIACREDITOCREDAUTR' VALUE :NEW.SECUENCIACREDITOCREDAUTR, 'FECHAAUTORIZACIONDETALLE' VALUE :NEW.FECHAAUTORIZACIONDETALLE, 'FECHAINGRESOCREDAUTR' VALUE :NEW.FECHAINGRESOCREDAUTR, 'FECHAMODIFICACIONCREDAUTR' VALUE :NEW.FECHAMODIFICACIONCREDAUTR, 'ESTADOAUTORIZACIONDETALLE' VALUE :NEW.ESTADOAUTORIZACIONDETALLE);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIACREDITOAUTORIZACION' VALUE :NEW.SECUENCIACREDITOAUTORIZACION, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'CODIGOEMPRESACREDAUTR' VALUE :NEW.CODIGOEMPRESACREDAUTR, 'SECUENCIAAUTORIZACIONDETALLE' VALUE :NEW.SECUENCIAAUTORIZACIONDETALLE, 'SECUENCIACREDITOAUTORIZACIONCREDAUTR' VALUE :NEW.SECUENCIACREDITOAUTORIZACIONCREDAUTR, 'SECUENCIARNGOUSUARIO' VALUE :NEW.SECUENCIARNGOUSUARIO, 'TIPOCREDITOCREDAUTR' VALUE :NEW.TIPOCREDITOCREDAUTR, 'ANIOCREDITOCREDAUTR' VALUE :NEW.ANIOCREDITOCREDAUTR, 'SECUENCIACREDITOCREDAUTR' VALUE :NEW.SECUENCIACREDITOCREDAUTR, 'FECHAAUTORIZACIONDETALLE' VALUE :NEW.FECHAAUTORIZACIONDETALLE, 'FECHAINGRESOCREDAUTR' VALUE :NEW.FECHAINGRESOCREDAUTR, 'FECHAMODIFICACIONCREDAUTR' VALUE :NEW.FECHAMODIFICACIONCREDAUTR, 'ESTADOAUTORIZACIONDETALLE' VALUE :NEW.ESTADOAUTORIZACIONDETALLE);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'SECUENCIACREDITOAUTORIZACION' VALUE :OLD.SECUENCIACREDITOAUTORIZACION, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'FECHAINGRESO' VALUE :OLD.FECHAINGRESO, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'CODIGOEMPRESACREDAUTR' VALUE :OLD.CODIGOEMPRESACREDAUTR, 'SECUENCIAAUTORIZACIONDETALLE' VALUE :OLD.SECUENCIAAUTORIZACIONDETALLE, 'SECUENCIACREDITOAUTORIZACIONCREDAUTR' VALUE :OLD.SECUENCIACREDITOAUTORIZACIONCREDAUTR, 'SECUENCIARNGOUSUARIO' VALUE :OLD.SECUENCIARNGOUSUARIO, 'TIPOCREDITOCREDAUTR' VALUE :OLD.TIPOCREDITOCREDAUTR, 'ANIOCREDITOCREDAUTR' VALUE :OLD.ANIOCREDITOCREDAUTR, 'SECUENCIACREDITOCREDAUTR' VALUE :OLD.SECUENCIACREDITOCREDAUTR, 'FECHAAUTORIZACIONDETALLE' VALUE :OLD.FECHAAUTORIZACIONDETALLE, 'FECHAINGRESOCREDAUTR' VALUE :OLD.FECHAINGRESOCREDAUTR, 'FECHAMODIFICACIONCREDAUTR' VALUE :OLD.FECHAMODIFICACIONCREDAUTR, 'ESTADOAUTORIZACIONDETALLE' VALUE :OLD.ESTADOAUTORIZACIONDETALLE);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('autorizacionCreditoDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.AUTORIZACIONCREDITODETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_AUTORIZACIONCREDITO  ON FCME_USER.AUTORIZACIONCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_AUTORIZACIONCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."AUTORIZACIONCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'CODIGOUSUARIORECEPTA' VALUE :NEW.CODIGOUSUARIORECEPTA, 'SECUENCIAAUTORIZACIONCREDITO' VALUE :NEW.SECUENCIAAUTORIZACIONCREDITO, 'FECHAAUTORIZACREDITO' VALUE :NEW.FECHAAUTORIZACREDITO, 'SECUENCIARANGOUSUARIOS' VALUE :NEW.SECUENCIARANGOUSUARIOS, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOUSUARIOTRANSMICION' VALUE :NEW.CODIGOUSUARIOTRANSMICION, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOAUTORIZACION' VALUE :NEW.CODIGOAUTORIZACION, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'TIPOREGISTRO' VALUE :NEW.TIPOREGISTRO, 'CODIGOUSUARIOTRANSMISION' VALUE :NEW.CODIGOUSUARIOTRANSMISION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'CODIGOUSUARIORECEPTA' VALUE :NEW.CODIGOUSUARIORECEPTA, 'SECUENCIAAUTORIZACIONCREDITO' VALUE :NEW.SECUENCIAAUTORIZACIONCREDITO, 'FECHAAUTORIZACREDITO' VALUE :NEW.FECHAAUTORIZACREDITO, 'SECUENCIARANGOUSUARIOS' VALUE :NEW.SECUENCIARANGOUSUARIOS, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOUSUARIOTRANSMICION' VALUE :NEW.CODIGOUSUARIOTRANSMICION, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOAUTORIZACION' VALUE :NEW.CODIGOAUTORIZACION, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'TIPOREGISTRO' VALUE :NEW.TIPOREGISTRO, 'CODIGOUSUARIOTRANSMISION' VALUE :NEW.CODIGOUSUARIOTRANSMISION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAABONO' VALUE :OLD.SECUENCIAABONO, 'FECHAAUTORIZACION' VALUE :OLD.FECHAAUTORIZACION, 'FECHAVERIFICACION' VALUE :OLD.FECHAVERIFICACION, 'CODIGOUSUARIORECEPTA' VALUE :OLD.CODIGOUSUARIORECEPTA, 'SECUENCIAAUTORIZACIONCREDITO' VALUE :OLD.SECUENCIAAUTORIZACIONCREDITO, 'FECHAAUTORIZACREDITO' VALUE :OLD.FECHAAUTORIZACREDITO, 'SECUENCIARANGOUSUARIOS' VALUE :OLD.SECUENCIARANGOUSUARIOS, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOUSUARIOTRANSMICION' VALUE :OLD.CODIGOUSUARIOTRANSMICION, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOAUTORIZACION' VALUE :OLD.CODIGOAUTORIZACION, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA, 'TIPOREGISTRO' VALUE :OLD.TIPOREGISTRO, 'CODIGOUSUARIOTRANSMISION' VALUE :OLD.CODIGOUSUARIOTRANSMISION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('autorizacionCredito_type', v_pk, v_event, v_payload, 'FCME_USER.AUTORIZACIONCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_AUXDATOSCOB  ON FCME_USER.AUXDATOSCOBROSADICIONALESTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_AUXDATOSCOB
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."AUXDATOSCOBROSADICIONALESTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('auxDatosCobrosAdicionalesType', v_pk, v_event, v_payload, 'FCME_USER.AUXDATOSCOBROSADICIONALESTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CALFCARTDETA  ON FCME_USER.CALIFICACIONCARTERADETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CALFCARTDETA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CALIFICACIONCARTERADETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'FECHACORT' VALUE :NEW.FECHACORT, 'MONTOXVEN' VALUE :NEW.MONTOXVEN, 'MONTOVCDO' VALUE :NEW.MONTOVCDO, 'MONTOPROV' VALUE :NEW.MONTOPROV, 'MONTOREVEPROV' VALUE :NEW.MONTOREVEPROV, 'NUMERODIASVCDO' VALUE :NEW.NUMERODIASVCDO, 'NUMERODCTO' VALUE :NEW.NUMERODCTO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'FECHACORT' VALUE :NEW.FECHACORT, 'MONTOXVEN' VALUE :NEW.MONTOXVEN, 'MONTOVCDO' VALUE :NEW.MONTOVCDO, 'MONTOPROV' VALUE :NEW.MONTOPROV, 'MONTOREVEPROV' VALUE :NEW.MONTOREVEPROV, 'NUMERODIASVCDO' VALUE :NEW.NUMERODIASVCDO, 'NUMERODCTO' VALUE :NEW.NUMERODCTO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'FECHACORT' VALUE :OLD.FECHACORT, 'MONTOXVEN' VALUE :OLD.MONTOXVEN, 'MONTOVCDO' VALUE :OLD.MONTOVCDO, 'MONTOPROV' VALUE :OLD.MONTOPROV, 'MONTOREVEPROV' VALUE :OLD.MONTOREVEPROV, 'NUMERODIASVCDO' VALUE :OLD.NUMERODIASVCDO, 'NUMERODCTO' VALUE :OLD.NUMERODCTO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('calificacionCarteraDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.CALIFICACIONCARTERADETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CALIFICACIONCARTERA  ON FCME_USER.CALIFICACIONCARTERA_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CALIFICACIONCARTERA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CALIFICACIONCARTERA_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOCALIFICACION' VALUE :NEW.TIPOCALIFICACION, 'TIPOCALIFICACIONHOMOLOGADO' VALUE :NEW.TIPOCALIFICACIONHOMOLOGADO, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'NUMERODIASVENCIDO' VALUE :NEW.NUMERODIASVENCIDO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOCALIFICACION' VALUE :NEW.TIPOCALIFICACION, 'TIPOCALIFICACIONHOMOLOGADO' VALUE :NEW.TIPOCALIFICACIONHOMOLOGADO, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'NUMERODIASVENCIDO' VALUE :NEW.NUMERODIASVENCIDO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'TIPOCALIFICACION' VALUE :OLD.TIPOCALIFICACION, 'TIPOCALIFICACIONHOMOLOGADO' VALUE :OLD.TIPOCALIFICACIONHOMOLOGADO, 'FECHACORTE' VALUE :OLD.FECHACORTE, 'NUMERODIASVENCIDO' VALUE :OLD.NUMERODIASVENCIDO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('calificacionCartera_type', v_pk, v_event, v_payload, 'FCME_USER.CALIFICACIONCARTERA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CANCELACIONCREDITO  ON FCME_USER.CANCELACIONCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CANCELACIONCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CANCELACIONCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITOACANCELAR' VALUE :NEW.ANIOCREDITOACANCELAR, 'SECUENCIACREDITOACANCELAR' VALUE :NEW.SECUENCIACREDITOACANCELAR, 'MONTOCREDITOACANCELAR' VALUE :NEW.MONTOCREDITOACANCELAR, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTOGASTOSJUDICIALES' VALUE :NEW.MONTOGASTOSJUDICIALES, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOCREDITOAACANCELAR' VALUE :NEW.TIPOCREDITOAACANCELAR);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITOACANCELAR' VALUE :NEW.ANIOCREDITOACANCELAR, 'SECUENCIACREDITOACANCELAR' VALUE :NEW.SECUENCIACREDITOACANCELAR, 'MONTOCREDITOACANCELAR' VALUE :NEW.MONTOCREDITOACANCELAR, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTOGASTOSJUDICIALES' VALUE :NEW.MONTOGASTOSJUDICIALES, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOCREDITOAACANCELAR' VALUE :NEW.TIPOCREDITOAACANCELAR);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ANIOCREDITOACANCELAR' VALUE :OLD.ANIOCREDITOACANCELAR, 'SECUENCIACREDITOACANCELAR' VALUE :OLD.SECUENCIACREDITOACANCELAR, 'MONTOCREDITOACANCELAR' VALUE :OLD.MONTOCREDITOACANCELAR, 'MONTOINTERES' VALUE :OLD.MONTOINTERES, 'MONTOGASTOSJUDICIALES' VALUE :OLD.MONTOGASTOSJUDICIALES, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'TIPOCREDITOAACANCELAR' VALUE :OLD.TIPOCREDITOAACANCELAR);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cancelacionCredito_type', v_pk, v_event, v_payload, 'FCME_USER.CANCELACIONCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CAUCIONCREDITO  ON FCME_USER.CAUCIONCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CAUCIONCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CAUCIONCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'TIPORUBRO' VALUE :NEW.TIPORUBRO, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'TIPORUBRO' VALUE :NEW.TIPORUBRO, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'TIPORUBRO' VALUE :OLD.TIPORUBRO, 'CODIGORUBRO' VALUE :OLD.CODIGORUBRO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('caucionCredito_type', v_pk, v_event, v_payload, 'FCME_USER.CAUCIONCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_COBJUDDETA  ON FCME_USER.COBRANZAJUDICIALDETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_COBJUDDETA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."COBRANZAJUDICIALDETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACOBROJUDICIAL' VALUE :NEW.SECUENCIACOBROJUDICIAL, 'TIPORUBROPAGADO' VALUE :NEW.TIPORUBROPAGADO, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'MONTOAPLICADO' VALUE :NEW.MONTOAPLICADO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'PORCENTAJEDESC' VALUE :NEW.PORCENTAJEDESC, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACOBROJUDICIAL' VALUE :NEW.SECUENCIACOBROJUDICIAL, 'TIPORUBROPAGADO' VALUE :NEW.TIPORUBROPAGADO, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'MONTOAPLICADO' VALUE :NEW.MONTOAPLICADO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'PORCENTAJEDESC' VALUE :NEW.PORCENTAJEDESC, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIACOBROJUDICIAL' VALUE :OLD.SECUENCIACOBROJUDICIAL, 'TIPORUBROPAGADO' VALUE :OLD.TIPORUBROPAGADO, 'MONTOMOVIMIENTO' VALUE :OLD.MONTOMOVIMIENTO, 'MONTOAPLICADO' VALUE :OLD.MONTOAPLICADO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'PORCENTAJEDESC' VALUE :OLD.PORCENTAJEDESC, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cobranzaJudicialDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.COBRANZAJUDICIALDETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_COBJUDDIST  ON FCME_USER.COBRANZAJUDICIALDISTRIBUCION_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_COBJUDDIST
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."COBRANZAJUDICIALDISTRIBUCION_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACOBROJUDICIAL' VALUE :NEW.SECUENCIACOBROJUDICIAL, 'TIPOCOBRANZA' VALUE :NEW.TIPOCOBRANZA, 'TIPOABONO' VALUE :NEW.TIPOABONO, 'TIPOPROCESO' VALUE :NEW.TIPOPROCESO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'MONTOCARGA' VALUE :NEW.MONTOCARGA, 'NUMEROCOMPROBANTE' VALUE :NEW.NUMEROCOMPROBANTE, 'URL' VALUE :NEW.URL, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACARGA' VALUE :NEW.FECHACARGA, 'FECHADEPOSITO' VALUE :NEW.FECHADEPOSITO, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'FECHALIQUIDACIONCREDITP' VALUE :NEW.FECHALIQUIDACIONCREDITP, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOCOBRANZAJUDICIALDIST' VALUE :NEW.CODIGOCOBRANZAJUDICIALDIST);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACOBROJUDICIAL' VALUE :NEW.SECUENCIACOBROJUDICIAL, 'TIPOCOBRANZA' VALUE :NEW.TIPOCOBRANZA, 'TIPOABONO' VALUE :NEW.TIPOABONO, 'TIPOPROCESO' VALUE :NEW.TIPOPROCESO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'MONTOCARGA' VALUE :NEW.MONTOCARGA, 'NUMEROCOMPROBANTE' VALUE :NEW.NUMEROCOMPROBANTE, 'URL' VALUE :NEW.URL, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACARGA' VALUE :NEW.FECHACARGA, 'FECHADEPOSITO' VALUE :NEW.FECHADEPOSITO, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'FECHALIQUIDACIONCREDITP' VALUE :NEW.FECHALIQUIDACIONCREDITP, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOCOBRANZAJUDICIALDIST' VALUE :NEW.CODIGOCOBRANZAJUDICIALDIST);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIACOBROJUDICIAL' VALUE :OLD.SECUENCIACOBROJUDICIAL, 'TIPOCOBRANZA' VALUE :OLD.TIPOCOBRANZA, 'TIPOABONO' VALUE :OLD.TIPOABONO, 'TIPOPROCESO' VALUE :OLD.TIPOPROCESO, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'MONTOCARGA' VALUE :OLD.MONTOCARGA, 'NUMEROCOMPROBANTE' VALUE :OLD.NUMEROCOMPROBANTE, 'URL' VALUE :OLD.URL, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHACARGA' VALUE :OLD.FECHACARGA, 'FECHADEPOSITO' VALUE :OLD.FECHADEPOSITO, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'FECHALIQUIDACIONCREDITP' VALUE :OLD.FECHALIQUIDACIONCREDITP, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'CODIGOCOBRANZAJUDICIALDIST' VALUE :OLD.CODIGOCOBRANZAJUDICIALDIST);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cobranzaJudicialDistribucion_type', v_pk, v_event, v_payload, 'FCME_USER.COBRANZAJUDICIALDISTRIBUCION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_COBRANZAJUDICIAL  ON FCME_USER.COBRANZAJUDICIAL_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_COBRANZAJUDICIAL
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."COBRANZAJUDICIAL_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACREDITOJUDICIAL' VALUE :NEW.SECUENCIACREDITOJUDICIAL, 'TIPOCOBRANZA' VALUE :NEW.TIPOCOBRANZA, 'CODIGOETAPA' VALUE :NEW.CODIGOETAPA, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'MONTOCOBROS' VALUE :NEW.MONTOCOBROS, 'MONTOCOBRARGASTOS' VALUE :NEW.MONTOCOBRARGASTOS, 'FECHAGESTION' VALUE :NEW.FECHAGESTION, 'FECHASALDOSCREDITOS' VALUE :NEW.FECHASALDOSCREDITOS, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACREDITOJUDICIAL' VALUE :NEW.SECUENCIACREDITOJUDICIAL, 'TIPOCOBRANZA' VALUE :NEW.TIPOCOBRANZA, 'CODIGOETAPA' VALUE :NEW.CODIGOETAPA, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'MONTOCOBROS' VALUE :NEW.MONTOCOBROS, 'MONTOCOBRARGASTOS' VALUE :NEW.MONTOCOBRARGASTOS, 'FECHAGESTION' VALUE :NEW.FECHAGESTION, 'FECHASALDOSCREDITOS' VALUE :NEW.FECHASALDOSCREDITOS, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIACREDITOJUDICIAL' VALUE :OLD.SECUENCIACREDITOJUDICIAL, 'TIPOCOBRANZA' VALUE :OLD.TIPOCOBRANZA, 'CODIGOETAPA' VALUE :OLD.CODIGOETAPA, 'CODIGORUBRO' VALUE :OLD.CODIGORUBRO, 'MONTOCOBROS' VALUE :OLD.MONTOCOBROS, 'MONTOCOBRARGASTOS' VALUE :OLD.MONTOCOBRARGASTOS, 'FECHAGESTION' VALUE :OLD.FECHAGESTION, 'FECHASALDOSCREDITOS' VALUE :OLD.FECHASALDOSCREDITOS, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cobranzaJudicial_type', v_pk, v_event, v_payload, 'FCME_USER.COBRANZAJUDICIAL_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CONCEPGSTOJUD  ON FCME_USER.CONCEPTOGASTOJUDICIALTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CONCEPGSTOJUD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CONCEPTOGASTOJUDICIALTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO' VALUE :NEW.CODIGO, 'RUBRO' VALUE :NEW.RUBRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO' VALUE :NEW.CODIGO, 'RUBRO' VALUE :NEW.RUBRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGO' VALUE :OLD.CODIGO, 'RUBRO' VALUE :OLD.RUBRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('conceptoGastoJudicialType', v_pk, v_event, v_payload, 'FCME_USER.CONCEPTOGASTOJUDICIALTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CONTABCRED  ON FCME_USER.CONTABILIZACIONCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CONTABCRED
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CONTABILIZACIONCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'DESCRIPCIONASIENTOCONTABLE' VALUE :NEW.DESCRIPCIONASIENTOCONTABLE, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHACONTABILIZACION' VALUE :NEW.FECHACONTABILIZACION, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOCONTABILIZACION' VALUE :NEW.CODIGOUSUARIOCONTABILIZACION, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'DESCRIPCIONASIENTOCONTABLE' VALUE :NEW.DESCRIPCIONASIENTOCONTABLE, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHACONTABILIZACION' VALUE :NEW.FECHACONTABILIZACION, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOCONTABILIZACION' VALUE :NEW.CODIGOUSUARIOCONTABILIZACION, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'DESCRIPCIONASIENTOCONTABLE' VALUE :OLD.DESCRIPCIONASIENTOCONTABLE, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHACORTE' VALUE :OLD.FECHACORTE, 'FECHACONTABILIZACION' VALUE :OLD.FECHACONTABILIZACION, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'CODIGOUSUARIOCONTABILIZACION' VALUE :OLD.CODIGOUSUARIOCONTABILIZACION, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('contabilizacionCredito_type', v_pk, v_event, v_payload, 'FCME_USER.CONTABILIZACIONCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CONVENIOPAGOCREDITO  ON FCME_USER.CONVENIOPAGOCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CONVENIOPAGOCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CONVENIOPAGOCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACONVENIO' VALUE :NEW.SECUENCIACONVENIO, 'NUMERODOCUMENTOCONVENIO' VALUE :NEW.NUMERODOCUMENTOCONVENIO, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'MONTOCAPITALVENCIDOALAFECHA' VALUE :NEW.MONTOCAPITALVENCIDOALAFECHA, 'MONTOINTERESVENCIDO' VALUE :NEW.MONTOINTERESVENCIDO, 'MONTOINTERESMORA' VALUE :NEW.MONTOINTERESMORA, 'MONTOCUOTAINICIAL' VALUE :NEW.MONTOCUOTAINICIAL, 'MONTOCOBRARGASTOS' VALUE :NEW.MONTOCOBRARGASTOS, 'MONTOINTERESCONVENIO' VALUE :NEW.MONTOINTERESCONVENIO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'ESTADOCIVIL' VALUE :NEW.ESTADOCIVIL, 'INDICADORAPLICAGARANTE' VALUE :NEW.INDICADORAPLICAGARANTE, 'DESCRIPCIONOBSERVACIONES' VALUE :NEW.DESCRIPCIONOBSERVACIONES, 'DESCRIPCIONREFERENCIA' VALUE :NEW.DESCRIPCIONREFERENCIA, 'FECHACONVENIOPAGO' VALUE :NEW.FECHACONVENIOPAGO, 'FECHAFALLECIMIENTOAFILIADO' VALUE :NEW.FECHAFALLECIMIENTOAFILIADO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'ESSTADOAFILIADO' VALUE :NEW.ESSTADOAFILIADO, 'FECHAINGRCALC' VALUE :NEW.FECHAINGRCALC, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACONVENIO' VALUE :NEW.SECUENCIACONVENIO, 'NUMERODOCUMENTOCONVENIO' VALUE :NEW.NUMERODOCUMENTOCONVENIO, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'MONTOCAPITALVENCIDOALAFECHA' VALUE :NEW.MONTOCAPITALVENCIDOALAFECHA, 'MONTOINTERESVENCIDO' VALUE :NEW.MONTOINTERESVENCIDO, 'MONTOINTERESMORA' VALUE :NEW.MONTOINTERESMORA, 'MONTOCUOTAINICIAL' VALUE :NEW.MONTOCUOTAINICIAL, 'MONTOCOBRARGASTOS' VALUE :NEW.MONTOCOBRARGASTOS, 'MONTOINTERESCONVENIO' VALUE :NEW.MONTOINTERESCONVENIO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'ESTADOCIVIL' VALUE :NEW.ESTADOCIVIL, 'INDICADORAPLICAGARANTE' VALUE :NEW.INDICADORAPLICAGARANTE, 'DESCRIPCIONOBSERVACIONES' VALUE :NEW.DESCRIPCIONOBSERVACIONES, 'DESCRIPCIONREFERENCIA' VALUE :NEW.DESCRIPCIONREFERENCIA, 'FECHACONVENIOPAGO' VALUE :NEW.FECHACONVENIOPAGO, 'FECHAFALLECIMIENTOAFILIADO' VALUE :NEW.FECHAFALLECIMIENTOAFILIADO, 'FECHAAUTORIZACION' VALUE :NEW.FECHAAUTORIZACION, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'ESSTADOAFILIADO' VALUE :NEW.ESSTADOAFILIADO, 'FECHAINGRCALC' VALUE :NEW.FECHAINGRCALC, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIACONVENIO' VALUE :OLD.SECUENCIACONVENIO, 'NUMERODOCUMENTOCONVENIO' VALUE :OLD.NUMERODOCUMENTOCONVENIO, 'CODIGOPROCESO' VALUE :OLD.CODIGOPROCESO, 'MONTOCAPITALVENCIDOALAFECHA' VALUE :OLD.MONTOCAPITALVENCIDOALAFECHA, 'MONTOINTERESVENCIDO' VALUE :OLD.MONTOINTERESVENCIDO, 'MONTOINTERESMORA' VALUE :OLD.MONTOINTERESMORA, 'MONTOCUOTAINICIAL' VALUE :OLD.MONTOCUOTAINICIAL, 'MONTOCOBRARGASTOS' VALUE :OLD.MONTOCOBRARGASTOS, 'MONTOINTERESCONVENIO' VALUE :OLD.MONTOINTERESCONVENIO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'ESTADOCIVIL' VALUE :OLD.ESTADOCIVIL, 'INDICADORAPLICAGARANTE' VALUE :OLD.INDICADORAPLICAGARANTE, 'DESCRIPCIONOBSERVACIONES' VALUE :OLD.DESCRIPCIONOBSERVACIONES, 'DESCRIPCIONREFERENCIA' VALUE :OLD.DESCRIPCIONREFERENCIA, 'FECHACONVENIOPAGO' VALUE :OLD.FECHACONVENIOPAGO, 'FECHAFALLECIMIENTOAFILIADO' VALUE :OLD.FECHAFALLECIMIENTOAFILIADO, 'FECHAAUTORIZACION' VALUE :OLD.FECHAAUTORIZACION, 'FECHAVERIFICACION' VALUE :OLD.FECHAVERIFICACION, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'CODIGOUSUARIOCONFIRMA' VALUE :OLD.CODIGOUSUARIOCONFIRMA, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'ESSTADOAFILIADO' VALUE :OLD.ESSTADOAFILIADO, 'FECHAINGRCALC' VALUE :OLD.FECHAINGRCALC, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('convenioPagoCredito_type', v_pk, v_event, v_payload, 'FCME_USER.CONVENIOPAGOCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_COSTOFINCRED  ON FCME_USER.COSTOFINANCIEROCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_COSTOFINCRED
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."COSTOFINANCIEROCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONOPROCESO' VALUE :NEW.SECUENCIAABONOPROCESO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONOPROCESO' VALUE :NEW.SECUENCIAABONOPROCESO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAABONOPROCESO' VALUE :OLD.SECUENCIAABONOPROCESO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('costoFinancieroCredito_type', v_pk, v_event, v_payload, 'FCME_USER.COSTOFINANCIEROCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CREDITO  ON FCME_USER.CREDITOTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CREDITOTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'CODIGOPRODUCTO' VALUE :NEW.CODIGOPRODUCTO, 'FECHAAPERTURA' VALUE :NEW.FECHAAPERTURA, 'FECHACANCELACION' VALUE :NEW.FECHACANCELACION, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'MONTO' VALUE :NEW.MONTO, 'FECHAEMISION' VALUE :NEW.FECHAEMISION, 'CODIGOCALIFICACIONCREDITO' VALUE :NEW.CODIGOCALIFICACIONCREDITO, 'FECHACALIFICACION' VALUE :NEW.FECHACALIFICACION, 'PORCENTAJECALIFICACION' VALUE :NEW.PORCENTAJECALIFICACION, 'CODIGOTIPOOPERACION' VALUE :NEW.CODIGOTIPOOPERACION, 'CODIGOPAISINVERSION' VALUE :NEW.CODIGOPAISINVERSION, 'CODIGOSUCURSALINGRESO' VALUE :NEW.CODIGOSUCURSALINGRESO, 'CODIGOOFICINAINGRESO' VALUE :NEW.CODIGOOFICINAINGRESO, 'CODIGOUSUARIOINGRESO' VALUE :NEW.CODIGOUSUARIOINGRESO, 'CODIGOSEGMENTOCREDITO' VALUE :NEW.CODIGOSEGMENTOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'CODIGOPRODUCTO' VALUE :NEW.CODIGOPRODUCTO, 'FECHAAPERTURA' VALUE :NEW.FECHAAPERTURA, 'FECHACANCELACION' VALUE :NEW.FECHACANCELACION, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'MONTO' VALUE :NEW.MONTO, 'FECHAEMISION' VALUE :NEW.FECHAEMISION, 'CODIGOCALIFICACIONCREDITO' VALUE :NEW.CODIGOCALIFICACIONCREDITO, 'FECHACALIFICACION' VALUE :NEW.FECHACALIFICACION, 'PORCENTAJECALIFICACION' VALUE :NEW.PORCENTAJECALIFICACION, 'CODIGOTIPOOPERACION' VALUE :NEW.CODIGOTIPOOPERACION, 'CODIGOPAISINVERSION' VALUE :NEW.CODIGOPAISINVERSION, 'CODIGOSUCURSALINGRESO' VALUE :NEW.CODIGOSUCURSALINGRESO, 'CODIGOOFICINAINGRESO' VALUE :NEW.CODIGOOFICINAINGRESO, 'CODIGOUSUARIOINGRESO' VALUE :NEW.CODIGOUSUARIOINGRESO, 'CODIGOSEGMENTOCREDITO' VALUE :NEW.CODIGOSEGMENTOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOIDENTIFICACION' VALUE :OLD.TIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'CODIGOSUCURSAL' VALUE :OLD.CODIGOSUCURSAL, 'CODIGOPRODUCTO' VALUE :OLD.CODIGOPRODUCTO, 'FECHAAPERTURA' VALUE :OLD.FECHAAPERTURA, 'FECHACANCELACION' VALUE :OLD.FECHACANCELACION, 'FECHAVENCIMIENTO' VALUE :OLD.FECHAVENCIMIENTO, 'MONTO' VALUE :OLD.MONTO, 'FECHAEMISION' VALUE :OLD.FECHAEMISION, 'CODIGOCALIFICACIONCREDITO' VALUE :OLD.CODIGOCALIFICACIONCREDITO, 'FECHACALIFICACION' VALUE :OLD.FECHACALIFICACION, 'PORCENTAJECALIFICACION' VALUE :OLD.PORCENTAJECALIFICACION, 'CODIGOTIPOOPERACION' VALUE :OLD.CODIGOTIPOOPERACION, 'CODIGOPAISINVERSION' VALUE :OLD.CODIGOPAISINVERSION, 'CODIGOSUCURSALINGRESO' VALUE :OLD.CODIGOSUCURSALINGRESO, 'CODIGOOFICINAINGRESO' VALUE :OLD.CODIGOOFICINAINGRESO, 'CODIGOUSUARIOINGRESO' VALUE :OLD.CODIGOUSUARIOINGRESO, 'CODIGOSEGMENTOCREDITO' VALUE :OLD.CODIGOSEGMENTOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('creditoType', v_pk, v_event, v_payload, 'FCME_USER.CREDITOTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CTAAUTODETA  ON FCME_USER.CUENTAAUTOMATICADETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CTAAUTODETA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUENTAAUTOMATICADETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOCUENTAAUTOXVEN' VALUE :NEW.CODIGOCUENTAAUTOXVEN, 'CODIGOCUENTAAUTOVENC' VALUE :NEW.CODIGOCUENTAAUTOVENC, 'CODIGOEMPRESACAUTTRUB' VALUE :NEW.CODIGOEMPRESACAUTTRUB);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOCUENTAAUTOXVEN' VALUE :NEW.CODIGOCUENTAAUTOXVEN, 'CODIGOCUENTAAUTOVENC' VALUE :NEW.CODIGOCUENTAAUTOVENC, 'CODIGOEMPRESACAUTTRUB' VALUE :NEW.CODIGOEMPRESACAUTTRUB);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'CODIGOCUENTAAUTOXVEN' VALUE :OLD.CODIGOCUENTAAUTOXVEN, 'CODIGOCUENTAAUTOVENC' VALUE :OLD.CODIGOCUENTAAUTOVENC, 'CODIGOEMPRESACAUTTRUB' VALUE :OLD.CODIGOEMPRESACAUTTRUB);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuentaAutomaticaDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.CUENTAAUTOMATICADETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUENTAAUTOMATICA  ON FCME_USER.CUENTAAUTOMATICA_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUENTAAUTOMATICA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUENTAAUTOMATICA_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGODOCUMENTO' VALUE :NEW.CODIGODOCUMENTO, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'CODIGOCUENTAAUTOMATICAVENCIDA' VALUE :NEW.CODIGOCUENTAAUTOMATICAVENCIDA, 'VARIABLECUENTAAUTOMATICA' VALUE :NEW.VARIABLECUENTAAUTOMATICA, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGODOCUMENTO' VALUE :NEW.CODIGODOCUMENTO, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'CODIGOCUENTAAUTOMATICAVENCIDA' VALUE :NEW.CODIGOCUENTAAUTOMATICAVENCIDA, 'VARIABLECUENTAAUTOMATICA' VALUE :NEW.VARIABLECUENTAAUTOMATICA, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'CODIGODOCUMENTO' VALUE :OLD.CODIGODOCUMENTO, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA, 'CODIGOCUENTAAUTOMATICAVENCIDA' VALUE :OLD.CODIGOCUENTAAUTOMATICAVENCIDA, 'VARIABLECUENTAAUTOMATICA' VALUE :OLD.VARIABLECUENTAAUTOMATICA, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuentaAutomatica_type', v_pk, v_event, v_payload, 'FCME_USER.CUENTAAUTOMATICA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUENTACUOTAS  ON FCME_USER.CUENTACUOTASTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUENTACUOTAS
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUENTACUOTASTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'NUMERODIASCALENDARIO' VALUE :NEW.NUMERODIASCALENDARIO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'FECHAABONO' VALUE :NEW.FECHAABONO, 'FECHAINICIO' VALUE :NEW.FECHAINICIO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'NUMERODIASCALENDARIO' VALUE :NEW.NUMERODIASCALENDARIO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'FECHAABONO' VALUE :NEW.FECHAABONO, 'FECHAINICIO' VALUE :NEW.FECHAINICIO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'NUMERODIASCALENDARIO' VALUE :OLD.NUMERODIASCALENDARIO, 'FECHAVENCIMIENTO' VALUE :OLD.FECHAVENCIMIENTO, 'FECHAPAGO' VALUE :OLD.FECHAPAGO, 'FECHAABONO' VALUE :OLD.FECHAABONO, 'FECHAINICIO' VALUE :OLD.FECHAINICIO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuentaCuotasType', v_pk, v_event, v_payload, 'FCME_USER.CUENTACUOTASTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUENTACXPCXC  ON FCME_USER.CUENTACXPCXCTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUENTACXPCXC
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUENTACXPCXCTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'CODIGOUSUARIOOFICIAL' VALUE :NEW.CODIGOUSUARIOOFICIAL, 'CODIGOMONEDA' VALUE :NEW.CODIGOMONEDA, 'CODIGOUSUARIOINGRESO' VALUE :NEW.CODIGOUSUARIOINGRESO, 'CODIGOUSUARIOMODIFICACION' VALUE :NEW.CODIGOUSUARIOMODIFICACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'CODIGOUSUARIOOFICIAL' VALUE :NEW.CODIGOUSUARIOOFICIAL, 'CODIGOMONEDA' VALUE :NEW.CODIGOMONEDA, 'CODIGOUSUARIOINGRESO' VALUE :NEW.CODIGOUSUARIOINGRESO, 'CODIGOUSUARIOMODIFICACION' VALUE :NEW.CODIGOUSUARIOMODIFICACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOSUCURSAL' VALUE :OLD.CODIGOSUCURSAL, 'CODIGOUSUARIOOFICIAL' VALUE :OLD.CODIGOUSUARIOOFICIAL, 'CODIGOMONEDA' VALUE :OLD.CODIGOMONEDA, 'CODIGOUSUARIOINGRESO' VALUE :OLD.CODIGOUSUARIOINGRESO, 'CODIGOUSUARIOMODIFICACION' VALUE :OLD.CODIGOUSUARIOMODIFICACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuentaCxPCxCType', v_pk, v_event, v_payload, 'FCME_USER.CUENTACXPCXCTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUENTAPERSONAS  ON FCME_USER.CUENTAPERSONASTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUENTAPERSONAS
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUENTAPERSONASTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'NUMERODIRECCION' VALUE :NEW.NUMERODIRECCION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'NUMERODIRECCION' VALUE :NEW.NUMERODIRECCION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'NUMERODIRECCION' VALUE :OLD.NUMERODIRECCION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuentaPersonasType', v_pk, v_event, v_payload, 'FCME_USER.CUENTAPERSONASTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUENTAPORCOBRAR  ON FCME_USER.CUENTAPORCOBRARTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUENTAPORCOBRAR
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUENTAPORCOBRARTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CUENTA' VALUE :NEW.CUENTA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CUENTA' VALUE :NEW.CUENTA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CUENTA' VALUE :OLD.CUENTA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuentaPorCobrarType', v_pk, v_event, v_payload, 'FCME_USER.CUENTAPORCOBRARTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUENTA  ON FCME_USER.CUENTATYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUENTA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUENTATYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'FECHACANCELACION' VALUE :NEW.FECHACANCELACION, 'CODIGOCALIFICACIONCREDITO' VALUE :NEW.CODIGOCALIFICACIONCREDITO, 'CODIGOUSUARIOINGRESO' VALUE :NEW.CODIGOUSUARIOINGRESO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'CODIGOFRECUENCIAINTERES' VALUE :NEW.CODIGOFRECUENCIAINTERES, 'FECHACASTIGO' VALUE :NEW.FECHACASTIGO, 'CODIGOPAISINVERSION' VALUE :NEW.CODIGOPAISINVERSION, 'FECHACALIFICACION' VALUE :NEW.FECHACALIFICACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'FECHACANCELACION' VALUE :NEW.FECHACANCELACION, 'CODIGOCALIFICACIONCREDITO' VALUE :NEW.CODIGOCALIFICACIONCREDITO, 'CODIGOUSUARIOINGRESO' VALUE :NEW.CODIGOUSUARIOINGRESO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'CODIGOFRECUENCIAINTERES' VALUE :NEW.CODIGOFRECUENCIAINTERES, 'FECHACASTIGO' VALUE :NEW.FECHACASTIGO, 'CODIGOPAISINVERSION' VALUE :NEW.CODIGOPAISINVERSION, 'FECHACALIFICACION' VALUE :NEW.FECHACALIFICACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOSUCURSAL' VALUE :OLD.CODIGOSUCURSAL, 'FECHACANCELACION' VALUE :OLD.FECHACANCELACION, 'CODIGOCALIFICACIONCREDITO' VALUE :OLD.CODIGOCALIFICACIONCREDITO, 'CODIGOUSUARIOINGRESO' VALUE :OLD.CODIGOUSUARIOINGRESO, 'FECHAVENCIMIENTO' VALUE :OLD.FECHAVENCIMIENTO, 'CODIGOFRECUENCIAINTERES' VALUE :OLD.CODIGOFRECUENCIAINTERES, 'FECHACASTIGO' VALUE :OLD.FECHACASTIGO, 'CODIGOPAISINVERSION' VALUE :OLD.CODIGOPAISINVERSION, 'FECHACALIFICACION' VALUE :OLD.FECHACALIFICACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuentaType', v_pk, v_event, v_payload, 'FCME_USER.CUENTATYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUENTASENLEGAL  ON FCME_USER.CUENTASENLEGALTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUENTASENLEGAL
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUENTASENLEGALTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'MONTODEMANDA' VALUE :NEW.MONTODEMANDA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'MONTODEMANDA' VALUE :NEW.MONTODEMANDA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'MONTODEMANDA' VALUE :OLD.MONTODEMANDA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuentasEnLegalType', v_pk, v_event, v_payload, 'FCME_USER.CUENTASENLEGALTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUOTACONVENIO  ON FCME_USER.CUOTACONVENIO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUOTACONVENIO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUOTACONVENIO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACONVENIO' VALUE :NEW.SECUENCIACONVENIO, 'SECUENCIADOCUMENTO' VALUE :NEW.SECUENCIADOCUMENTO, 'NUMERODOCUMENTOCONVENIO' VALUE :NEW.NUMERODOCUMENTOCONVENIO, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'ESTADOCREDITO' VALUE :NEW.ESTADOCREDITO, 'ESTADOVENCIMIENTO' VALUE :NEW.ESTADOVENCIMIENTO, 'FECHAINICIOINTERES' VALUE :NEW.FECHAINICIOINTERES, 'FECHAINICVENC' VALUE :NEW.FECHAINICVENC, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'FECHAULTIMOENVIO' VALUE :NEW.FECHAULTIMOENVIO, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'MONTOCAPITAL' VALUE :NEW.MONTOCAPITAL, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTODIVIDENDO' VALUE :NEW.MONTODIVIDENDO, 'MONTOSEGURO' VALUE :NEW.MONTOSEGURO, 'MONTOSEGUROVEHICULO' VALUE :NEW.MONTOSEGUROVEHICULO, 'MONTOCOBROINCENDIO' VALUE :NEW.MONTOCOBROINCENDIO, 'MONTOCOMISION' VALUE :NEW.MONTOCOMISION, 'MONTOCOSTOSEMISION' VALUE :NEW.MONTOCOSTOSEMISION, 'MONTOGASTOSJUDICIALES' VALUE :NEW.MONTOGASTOSJUDICIALES, 'MONTOINTERESPRIMERMES' VALUE :NEW.MONTOINTERESPRIMERMES, 'MONTOABONOMORA' VALUE :NEW.MONTOABONOMORA, 'MONTODEVENGADOINTERES' VALUE :NEW.MONTODEVENGADOINTERES, 'MONTODEVENGADOMORA' VALUE :NEW.MONTODEVENGADOMORA, 'MONTODEVENGOACUMULADO' VALUE :NEW.MONTODEVENGOACUMULADO, 'INDICADORREVERSODEVENGO' VALUE :NEW.INDICADORREVERSODEVENGO, 'NUMEROANIOS' VALUE :NEW.NUMEROANIOS, 'NUMERODIAS' VALUE :NEW.NUMERODIAS, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'MOABONOMORASOLCA' VALUE :NEW.MOABONOMORASOLCA, 'MODEVENGODIARIO' VALUE :NEW.MODEVENGODIARIO, 'MONTOABONOCAPITALCAPITAL' VALUE :NEW.MONTOABONOCAPITALCAPITAL, 'MONTOABONOINTERESCAPITAL' VALUE :NEW.MONTOABONOINTERESCAPITAL, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACONVENIO' VALUE :NEW.SECUENCIACONVENIO, 'SECUENCIADOCUMENTO' VALUE :NEW.SECUENCIADOCUMENTO, 'NUMERODOCUMENTOCONVENIO' VALUE :NEW.NUMERODOCUMENTOCONVENIO, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'ESTADOCREDITO' VALUE :NEW.ESTADOCREDITO, 'ESTADOVENCIMIENTO' VALUE :NEW.ESTADOVENCIMIENTO, 'FECHAINICIOINTERES' VALUE :NEW.FECHAINICIOINTERES, 'FECHAINICVENC' VALUE :NEW.FECHAINICVENC, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'FECHAULTIMOENVIO' VALUE :NEW.FECHAULTIMOENVIO, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'MONTOCAPITAL' VALUE :NEW.MONTOCAPITAL, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTODIVIDENDO' VALUE :NEW.MONTODIVIDENDO, 'MONTOSEGURO' VALUE :NEW.MONTOSEGURO, 'MONTOSEGUROVEHICULO' VALUE :NEW.MONTOSEGUROVEHICULO, 'MONTOCOBROINCENDIO' VALUE :NEW.MONTOCOBROINCENDIO, 'MONTOCOMISION' VALUE :NEW.MONTOCOMISION, 'MONTOCOSTOSEMISION' VALUE :NEW.MONTOCOSTOSEMISION, 'MONTOGASTOSJUDICIALES' VALUE :NEW.MONTOGASTOSJUDICIALES, 'MONTOINTERESPRIMERMES' VALUE :NEW.MONTOINTERESPRIMERMES, 'MONTOABONOMORA' VALUE :NEW.MONTOABONOMORA, 'MONTODEVENGADOINTERES' VALUE :NEW.MONTODEVENGADOINTERES, 'MONTODEVENGADOMORA' VALUE :NEW.MONTODEVENGADOMORA, 'MONTODEVENGOACUMULADO' VALUE :NEW.MONTODEVENGOACUMULADO, 'INDICADORREVERSODEVENGO' VALUE :NEW.INDICADORREVERSODEVENGO, 'NUMEROANIOS' VALUE :NEW.NUMEROANIOS, 'NUMERODIAS' VALUE :NEW.NUMERODIAS, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'MOABONOMORASOLCA' VALUE :NEW.MOABONOMORASOLCA, 'MODEVENGODIARIO' VALUE :NEW.MODEVENGODIARIO, 'MONTOABONOCAPITALCAPITAL' VALUE :NEW.MONTOABONOCAPITALCAPITAL, 'MONTOABONOINTERESCAPITAL' VALUE :NEW.MONTOABONOINTERESCAPITAL, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIACONVENIO' VALUE :OLD.SECUENCIACONVENIO, 'SECUENCIADOCUMENTO' VALUE :OLD.SECUENCIADOCUMENTO, 'NUMERODOCUMENTOCONVENIO' VALUE :OLD.NUMERODOCUMENTOCONVENIO, 'SECUENCIAROL' VALUE :OLD.SECUENCIAROL, 'ESTADOCREDITO' VALUE :OLD.ESTADOCREDITO, 'ESTADOVENCIMIENTO' VALUE :OLD.ESTADOVENCIMIENTO, 'FECHAINICIOINTERES' VALUE :OLD.FECHAINICIOINTERES, 'FECHAINICVENC' VALUE :OLD.FECHAINICVENC, 'FECHAVENCIMIENTO' VALUE :OLD.FECHAVENCIMIENTO, 'FECHAULTIMOENVIO' VALUE :OLD.FECHAULTIMOENVIO, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'MONTOCUOTA' VALUE :OLD.MONTOCUOTA, 'MONTOCAPITAL' VALUE :OLD.MONTOCAPITAL, 'MONTOINTERES' VALUE :OLD.MONTOINTERES, 'MONTODIVIDENDO' VALUE :OLD.MONTODIVIDENDO, 'MONTOSEGURO' VALUE :OLD.MONTOSEGURO, 'MONTOSEGUROVEHICULO' VALUE :OLD.MONTOSEGUROVEHICULO, 'MONTOCOBROINCENDIO' VALUE :OLD.MONTOCOBROINCENDIO, 'MONTOCOMISION' VALUE :OLD.MONTOCOMISION, 'MONTOCOSTOSEMISION' VALUE :OLD.MONTOCOSTOSEMISION, 'MONTOGASTOSJUDICIALES' VALUE :OLD.MONTOGASTOSJUDICIALES, 'MONTOINTERESPRIMERMES' VALUE :OLD.MONTOINTERESPRIMERMES, 'MONTOABONOMORA' VALUE :OLD.MONTOABONOMORA, 'MONTODEVENGADOINTERES' VALUE :OLD.MONTODEVENGADOINTERES, 'MONTODEVENGADOMORA' VALUE :OLD.MONTODEVENGADOMORA, 'MONTODEVENGOACUMULADO' VALUE :OLD.MONTODEVENGOACUMULADO, 'INDICADORREVERSODEVENGO' VALUE :OLD.INDICADORREVERSODEVENGO, 'NUMEROANIOS' VALUE :OLD.NUMEROANIOS, 'NUMERODIAS' VALUE :OLD.NUMERODIAS, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'MOABONOMORASOLCA' VALUE :OLD.MOABONOMORASOLCA, 'MODEVENGODIARIO' VALUE :OLD.MODEVENGODIARIO, 'MONTOABONOCAPITALCAPITAL' VALUE :OLD.MONTOABONOCAPITALCAPITAL, 'MONTOABONOINTERESCAPITAL' VALUE :OLD.MONTOABONOINTERESCAPITAL, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuotaConvenio_type', v_pk, v_event, v_payload, 'FCME_USER.CUOTACONVENIO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUOTACREDITO  ON FCME_USER.CUOTACREDITOTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUOTACREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."CUOTACREDITOTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUOTA' VALUE :NEW.CODIGOCUOTA, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'FECHAINICIO' VALUE :NEW.FECHAINICIO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'CODIGOGRUPOBALANCEDESGRAVAMEN' VALUE :NEW.CODIGOGRUPOBALANCEDESGRAVAMEN, 'CODIGOGRUPOBALANCEINCENDIO' VALUE :NEW.CODIGOGRUPOBALANCEINCENDIO, 'CODIGOGRUPOBALANCETASAMORA' VALUE :NEW.CODIGOGRUPOBALANCETASAMORA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUOTA' VALUE :NEW.CODIGOCUOTA, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'FECHAINICIO' VALUE :NEW.FECHAINICIO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'CODIGOGRUPOBALANCEDESGRAVAMEN' VALUE :NEW.CODIGOGRUPOBALANCEDESGRAVAMEN, 'CODIGOGRUPOBALANCEINCENDIO' VALUE :NEW.CODIGOGRUPOBALANCEINCENDIO, 'CODIGOGRUPOBALANCETASAMORA' VALUE :NEW.CODIGOGRUPOBALANCETASAMORA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOCUOTA' VALUE :OLD.CODIGOCUOTA, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA, 'FECHAINICIO' VALUE :OLD.FECHAINICIO, 'FECHAVENCIMIENTO' VALUE :OLD.FECHAVENCIMIENTO, 'FECHAPAGO' VALUE :OLD.FECHAPAGO, 'CODIGOGRUPOBALANCEDESGRAVAMEN' VALUE :OLD.CODIGOGRUPOBALANCEDESGRAVAMEN, 'CODIGOGRUPOBALANCEINCENDIO' VALUE :OLD.CODIGOGRUPOBALANCEINCENDIO, 'CODIGOGRUPOBALANCETASAMORA' VALUE :OLD.CODIGOGRUPOBALANCETASAMORA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuotaCreditoType', v_pk, v_event, v_payload, 'FCME_USER.CUOTACREDITOTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DESEMBOLSOCREDITO  ON FCME_USER.DESEMBOLSOCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DESEMBOLSOCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."DESEMBOLSOCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOBANCO' VALUE :NEW.CODIGOBANCO, 'CODIGOPROVEEDOR' VALUE :NEW.CODIGOPROVEEDOR, 'NUMEROORDEN' VALUE :NEW.NUMEROORDEN, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'DESCRIPCIONPAGODESEMBOLSO' VALUE :NEW.DESCRIPCIONPAGODESEMBOLSO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'DESCRIPCIONPAGO' VALUE :NEW.DESCRIPCIONPAGO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOBANCO' VALUE :NEW.CODIGOBANCO, 'CODIGOPROVEEDOR' VALUE :NEW.CODIGOPROVEEDOR, 'NUMEROORDEN' VALUE :NEW.NUMEROORDEN, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'DESCRIPCIONPAGODESEMBOLSO' VALUE :NEW.DESCRIPCIONPAGODESEMBOLSO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'DESCRIPCIONPAGO' VALUE :NEW.DESCRIPCIONPAGO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOBANCO' VALUE :OLD.CODIGOBANCO, 'CODIGOPROVEEDOR' VALUE :OLD.CODIGOPROVEEDOR, 'NUMEROORDEN' VALUE :OLD.NUMEROORDEN, 'FECHAPAGO' VALUE :OLD.FECHAPAGO, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'CODIGOUSUARIO' VALUE :OLD.CODIGOUSUARIO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'DESCRIPCIONPAGODESEMBOLSO' VALUE :OLD.DESCRIPCIONPAGODESEMBOLSO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'DESCRIPCIONPAGO' VALUE :OLD.DESCRIPCIONPAGO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('desembolsoCredito_type', v_pk, v_event, v_payload, 'FCME_USER.DESEMBOLSOCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DESEMBDEVO  ON FCME_USER.DESEMBOLSODEVOLUCION_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DESEMBDEVO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."DESEMBOLSODEVOLUCION_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPODEVOLUCION' VALUE :NEW.TIPODEVOLUCION, 'ANIODEVOLUCION' VALUE :NEW.ANIODEVOLUCION, 'SECUENCIADEVOLUCION' VALUE :NEW.SECUENCIADEVOLUCION, 'SECUENCIADESEMBOLSO' VALUE :NEW.SECUENCIADESEMBOLSO, 'CODIGOTORD' VALUE :NEW.CODIGOTORD, 'CODIGOBNCO' VALUE :NEW.CODIGOBNCO, 'CODIGOBNCOACRE' VALUE :NEW.CODIGOBNCOACRE, 'TIPOCUENTA' VALUE :NEW.TIPOCUENTA, 'NUMEROCUENTA' VALUE :NEW.NUMEROCUENTA, 'CODIGOBENE' VALUE :NEW.CODIGOBENE, 'NOMBREBENE' VALUE :NEW.NOMBREBENE, 'ESTADODESEMBOLSO' VALUE :NEW.ESTADODESEMBOLSO, 'MONTODESEMBOLSO' VALUE :NEW.MONTODESEMBOLSO, 'NUMEROORDE' VALUE :NEW.NUMEROORDE, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'DESCRIPCIONPAGO' VALUE :NEW.DESCRIPCIONPAGO, 'TIPODEVOLUCIONDDEVO' VALUE :NEW.TIPODEVOLUCIONDDEVO, 'ANIODEVOLUCIONDDEVO' VALUE :NEW.ANIODEVOLUCIONDDEVO, 'SECUENCIADEVOLUCIONDDEVO' VALUE :NEW.SECUENCIADEVOLUCIONDDEVO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'SECUENCIASOBRANTE' VALUE :NEW.SECUENCIASOBRANTE);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPODEVOLUCION' VALUE :NEW.TIPODEVOLUCION, 'ANIODEVOLUCION' VALUE :NEW.ANIODEVOLUCION, 'SECUENCIADEVOLUCION' VALUE :NEW.SECUENCIADEVOLUCION, 'SECUENCIADESEMBOLSO' VALUE :NEW.SECUENCIADESEMBOLSO, 'CODIGOTORD' VALUE :NEW.CODIGOTORD, 'CODIGOBNCO' VALUE :NEW.CODIGOBNCO, 'CODIGOBNCOACRE' VALUE :NEW.CODIGOBNCOACRE, 'TIPOCUENTA' VALUE :NEW.TIPOCUENTA, 'NUMEROCUENTA' VALUE :NEW.NUMEROCUENTA, 'CODIGOBENE' VALUE :NEW.CODIGOBENE, 'NOMBREBENE' VALUE :NEW.NOMBREBENE, 'ESTADODESEMBOLSO' VALUE :NEW.ESTADODESEMBOLSO, 'MONTODESEMBOLSO' VALUE :NEW.MONTODESEMBOLSO, 'NUMEROORDE' VALUE :NEW.NUMEROORDE, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'DESCRIPCIONPAGO' VALUE :NEW.DESCRIPCIONPAGO, 'TIPODEVOLUCIONDDEVO' VALUE :NEW.TIPODEVOLUCIONDDEVO, 'ANIODEVOLUCIONDDEVO' VALUE :NEW.ANIODEVOLUCIONDDEVO, 'SECUENCIADEVOLUCIONDDEVO' VALUE :NEW.SECUENCIADEVOLUCIONDDEVO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'SECUENCIASOBRANTE' VALUE :NEW.SECUENCIASOBRANTE);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPODEVOLUCION' VALUE :OLD.TIPODEVOLUCION, 'ANIODEVOLUCION' VALUE :OLD.ANIODEVOLUCION, 'SECUENCIADEVOLUCION' VALUE :OLD.SECUENCIADEVOLUCION, 'SECUENCIADESEMBOLSO' VALUE :OLD.SECUENCIADESEMBOLSO, 'CODIGOTORD' VALUE :OLD.CODIGOTORD, 'CODIGOBNCO' VALUE :OLD.CODIGOBNCO, 'CODIGOBNCOACRE' VALUE :OLD.CODIGOBNCOACRE, 'TIPOCUENTA' VALUE :OLD.TIPOCUENTA, 'NUMEROCUENTA' VALUE :OLD.NUMEROCUENTA, 'CODIGOBENE' VALUE :OLD.CODIGOBENE, 'NOMBREBENE' VALUE :OLD.NOMBREBENE, 'ESTADODESEMBOLSO' VALUE :OLD.ESTADODESEMBOLSO, 'MONTODESEMBOLSO' VALUE :OLD.MONTODESEMBOLSO, 'NUMEROORDE' VALUE :OLD.NUMEROORDE, 'FECHAPAGO' VALUE :OLD.FECHAPAGO, 'CODIGOUSUARIO' VALUE :OLD.CODIGOUSUARIO, 'DESCRIPCIONPAGO' VALUE :OLD.DESCRIPCIONPAGO, 'TIPODEVOLUCIONDDEVO' VALUE :OLD.TIPODEVOLUCIONDDEVO, 'ANIODEVOLUCIONDDEVO' VALUE :OLD.ANIODEVOLUCIONDDEVO, 'SECUENCIADEVOLUCIONDDEVO' VALUE :OLD.SECUENCIADEVOLUCIONDDEVO, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'SECUENCIAROL' VALUE :OLD.SECUENCIAROL, 'SECUENCIASOBRANTE' VALUE :OLD.SECUENCIASOBRANTE);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('desembolsoDevolucion_type', v_pk, v_event, v_payload, 'FCME_USER.DESEMBOLSODEVOLUCION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DETALLERECUPERACION  ON FCME_USER.DETALLERECUPERACION_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DETALLERECUPERACION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."DETALLERECUPERACION_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'ESTADOVENCIMIENTO' VALUE :NEW.ESTADOVENCIMIENTO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'ESTADOVENCIMIENTO' VALUE :NEW.ESTADOVENCIMIENTO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAABONO' VALUE :OLD.SECUENCIAABONO, 'MONTOMOVIMIENTO' VALUE :OLD.MONTOMOVIMIENTO, 'ESTADOVENCIMIENTO' VALUE :OLD.ESTADOVENCIMIENTO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('detalleRecuperacion_type', v_pk, v_event, v_payload, 'FCME_USER.DETALLERECUPERACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DVGOCARTDETA  ON FCME_USER.DEVENGAMIENTOCARTERADETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DVGOCARTDETA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."DEVENGAMIENTOCARTERADETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGODVGODETALLE' VALUE :NEW.CODIGODVGODETALLE, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'NUMERODCTO' VALUE :NEW.NUMERODCTO, 'MONTOSALDOCAPI' VALUE :NEW.MONTOSALDOCAPI, 'FECHAULTMCORT' VALUE :NEW.FECHAULTMCORT, 'FECHACORT' VALUE :NEW.FECHACORT, 'MONTODVGOXVEN' VALUE :NEW.MONTODVGOXVEN, 'MONTODVGOVENC' VALUE :NEW.MONTODVGOVENC, 'MONTOREVE' VALUE :NEW.MONTOREVE, 'MONTOAJUS' VALUE :NEW.MONTOAJUS, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGODVGODETALLE' VALUE :NEW.CODIGODVGODETALLE, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'NUMERODCTO' VALUE :NEW.NUMERODCTO, 'MONTOSALDOCAPI' VALUE :NEW.MONTOSALDOCAPI, 'FECHAULTMCORT' VALUE :NEW.FECHAULTMCORT, 'FECHACORT' VALUE :NEW.FECHACORT, 'MONTODVGOXVEN' VALUE :NEW.MONTODVGOXVEN, 'MONTODVGOVENC' VALUE :NEW.MONTODVGOVENC, 'MONTOREVE' VALUE :NEW.MONTOREVE, 'MONTOAJUS' VALUE :NEW.MONTOAJUS, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGODVGODETALLE' VALUE :OLD.CODIGODVGODETALLE, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'NUMERODCTO' VALUE :OLD.NUMERODCTO, 'MONTOSALDOCAPI' VALUE :OLD.MONTOSALDOCAPI, 'FECHAULTMCORT' VALUE :OLD.FECHAULTMCORT, 'FECHACORT' VALUE :OLD.FECHACORT, 'MONTODVGOXVEN' VALUE :OLD.MONTODVGOXVEN, 'MONTODVGOVENC' VALUE :OLD.MONTODVGOVENC, 'MONTOREVE' VALUE :OLD.MONTOREVE, 'MONTOAJUS' VALUE :OLD.MONTOAJUS, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('devengamientoCarteraDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.DEVENGAMIENTOCARTERADETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DVGOCART  ON FCME_USER.DEVENGAMIENTOCARTERA_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DVGOCART
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."DEVENGAMIENTOCARTERA_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGODEVENGODETALLE' VALUE :NEW.CODIGODEVENGODETALLE, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHAULTIMOCORTE' VALUE :NEW.FECHAULTIMOCORTE, 'MONTODEVENGAMIENTOPORVENCER' VALUE :NEW.MONTODEVENGAMIENTOPORVENCER, 'MONTODEVENGAMIENTOVENCIDO' VALUE :NEW.MONTODEVENGAMIENTOVENCIDO, 'MONTOAJUSTEDEVENGOS' VALUE :NEW.MONTOAJUSTEDEVENGOS, 'MONTOREVERSODEVENGOS' VALUE :NEW.MONTOREVERSODEVENGOS, 'MONTOSALDOCAPITAL' VALUE :NEW.MONTOSALDOCAPITAL, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGODEVENGODETALLE' VALUE :NEW.CODIGODEVENGODETALLE, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHAULTIMOCORTE' VALUE :NEW.FECHAULTIMOCORTE, 'MONTODEVENGAMIENTOPORVENCER' VALUE :NEW.MONTODEVENGAMIENTOPORVENCER, 'MONTODEVENGAMIENTOVENCIDO' VALUE :NEW.MONTODEVENGAMIENTOVENCIDO, 'MONTOAJUSTEDEVENGOS' VALUE :NEW.MONTOAJUSTEDEVENGOS, 'MONTOREVERSODEVENGOS' VALUE :NEW.MONTOREVERSODEVENGOS, 'MONTOSALDOCAPITAL' VALUE :NEW.MONTOSALDOCAPITAL, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'CODIGODEVENGODETALLE' VALUE :OLD.CODIGODEVENGODETALLE, 'FECHACORTE' VALUE :OLD.FECHACORTE, 'FECHAULTIMOCORTE' VALUE :OLD.FECHAULTIMOCORTE, 'MONTODEVENGAMIENTOPORVENCER' VALUE :OLD.MONTODEVENGAMIENTOPORVENCER, 'MONTODEVENGAMIENTOVENCIDO' VALUE :OLD.MONTODEVENGAMIENTOVENCIDO, 'MONTOAJUSTEDEVENGOS' VALUE :OLD.MONTOAJUSTEDEVENGOS, 'MONTOREVERSODEVENGOS' VALUE :OLD.MONTOREVERSODEVENGOS, 'MONTOSALDOCAPITAL' VALUE :OLD.MONTOSALDOCAPITAL, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('devengamientoCartera_type', v_pk, v_event, v_payload, 'FCME_USER.DEVENGAMIENTOCARTERA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DEVOLUCIONCREDITO  ON FCME_USER.DEVOLUCIONCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DEVOLUCIONCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."DEVOLUCIONCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIADEVOLUCION' VALUE :NEW.SECUENCIADEVOLUCION, 'ANIODEVOLUCION' VALUE :NEW.ANIODEVOLUCION, 'TIPODEVOLUCION' VALUE :NEW.TIPODEVOLUCION, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'MONTODEVOLUCION' VALUE :NEW.MONTODEVOLUCION, 'CODIGOUSUARIOTRANSMISION' VALUE :NEW.CODIGOUSUARIOTRANSMISION, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'SECUENCIASOBRANTES' VALUE :NEW.SECUENCIASOBRANTES, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIADEVOLUCION' VALUE :NEW.SECUENCIADEVOLUCION, 'ANIODEVOLUCION' VALUE :NEW.ANIODEVOLUCION, 'TIPODEVOLUCION' VALUE :NEW.TIPODEVOLUCION, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'MONTODEVOLUCION' VALUE :NEW.MONTODEVOLUCION, 'CODIGOUSUARIOTRANSMISION' VALUE :NEW.CODIGOUSUARIOTRANSMISION, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'SECUENCIASOBRANTES' VALUE :NEW.SECUENCIASOBRANTES, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIADEVOLUCION' VALUE :OLD.SECUENCIADEVOLUCION, 'ANIODEVOLUCION' VALUE :OLD.ANIODEVOLUCION, 'TIPODEVOLUCION' VALUE :OLD.TIPODEVOLUCION, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'MONTODEVOLUCION' VALUE :OLD.MONTODEVOLUCION, 'CODIGOUSUARIOTRANSMISION' VALUE :OLD.CODIGOUSUARIOTRANSMISION, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'SECUENCIAROL' VALUE :OLD.SECUENCIAROL, 'SECUENCIASOBRANTES' VALUE :OLD.SECUENCIASOBRANTES, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('devolucionCredito_type', v_pk, v_event, v_payload, 'FCME_USER.DEVOLUCIONCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DEVOMASDETA  ON FCME_USER.DEVOLUCIONMASIVADETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DEVOMASDETA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."DEVOLUCIONMASIVADETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIADEVOLUCIONDETALLE' VALUE :NEW.SECUENCIADEVOLUCIONDETALLE, 'SECUENCIADEVOLUCIONMASIVA' VALUE :NEW.SECUENCIADEVOLUCIONMASIVA, 'SECUENCIASOBRANTE' VALUE :NEW.SECUENCIASOBRANTE, 'MONTODISP' VALUE :NEW.MONTODISP, 'SECUENCIAMOVIMIENTO' VALUE :NEW.SECUENCIAMOVIMIENTO, 'CODIGOLIQUIDACIONRUBRO' VALUE :NEW.CODIGOLIQUIDACIONRUBRO, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'ESTADODEVOLUCIONDETALLE' VALUE :NEW.ESTADODEVOLUCIONDETALLE, 'CODIGORUBROROL' VALUE :NEW.CODIGORUBROROL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIADEVOLUCIONDETALLE' VALUE :NEW.SECUENCIADEVOLUCIONDETALLE, 'SECUENCIADEVOLUCIONMASIVA' VALUE :NEW.SECUENCIADEVOLUCIONMASIVA, 'SECUENCIASOBRANTE' VALUE :NEW.SECUENCIASOBRANTE, 'MONTODISP' VALUE :NEW.MONTODISP, 'SECUENCIAMOVIMIENTO' VALUE :NEW.SECUENCIAMOVIMIENTO, 'CODIGOLIQUIDACIONRUBRO' VALUE :NEW.CODIGOLIQUIDACIONRUBRO, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'ESTADODEVOLUCIONDETALLE' VALUE :NEW.ESTADODEVOLUCIONDETALLE, 'CODIGORUBROROL' VALUE :NEW.CODIGORUBROROL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'SECUENCIADEVOLUCIONDETALLE' VALUE :OLD.SECUENCIADEVOLUCIONDETALLE, 'SECUENCIADEVOLUCIONMASIVA' VALUE :OLD.SECUENCIADEVOLUCIONMASIVA, 'SECUENCIASOBRANTE' VALUE :OLD.SECUENCIASOBRANTE, 'MONTODISP' VALUE :OLD.MONTODISP, 'SECUENCIAMOVIMIENTO' VALUE :OLD.SECUENCIAMOVIMIENTO, 'CODIGOLIQUIDACIONRUBRO' VALUE :OLD.CODIGOLIQUIDACIONRUBRO, 'MONTOMOVIMIENTO' VALUE :OLD.MONTOMOVIMIENTO, 'ESTADODEVOLUCIONDETALLE' VALUE :OLD.ESTADODEVOLUCIONDETALLE, 'CODIGORUBROROL' VALUE :OLD.CODIGORUBROROL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('devolucionMasivaDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.DEVOLUCIONMASIVADETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DEVOLUCIONMASIVA  ON FCME_USER.DEVOLUCIONMASIVA_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DEVOLUCIONMASIVA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."DEVOLUCIONMASIVA_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIADEVOLUCIONESMASIVAS' VALUE :NEW.SECUENCIADEVOLUCIONESMASIVAS, 'TIPODEVOLUCIONMASIVA' VALUE :NEW.TIPODEVOLUCIONMASIVA, 'ESTADODEVOLUCIONESMASIVAS' VALUE :NEW.ESTADODEVOLUCIONESMASIVAS, 'SECUENCIADEVOLUCIONESMASIVASDETALLE' VALUE :NEW.SECUENCIADEVOLUCIONESMASIVASDETALLE, 'CODIGOLIQUIDACIONRUBRO' VALUE :NEW.CODIGOLIQUIDACIONRUBRO, 'SECUENCIAMOVIMIENTO' VALUE :NEW.SECUENCIAMOVIMIENTO, 'SECUENCIASOBRANTES' VALUE :NEW.SECUENCIASOBRANTES, 'MONTODISPONIBLESOBRANTE' VALUE :NEW.MONTODISPONIBLESOBRANTE, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'COIGORUBROROL' VALUE :NEW.COIGORUBROROL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIADEVOLUCIONESMASIVAS' VALUE :NEW.SECUENCIADEVOLUCIONESMASIVAS, 'TIPODEVOLUCIONMASIVA' VALUE :NEW.TIPODEVOLUCIONMASIVA, 'ESTADODEVOLUCIONESMASIVAS' VALUE :NEW.ESTADODEVOLUCIONESMASIVAS, 'SECUENCIADEVOLUCIONESMASIVASDETALLE' VALUE :NEW.SECUENCIADEVOLUCIONESMASIVASDETALLE, 'CODIGOLIQUIDACIONRUBRO' VALUE :NEW.CODIGOLIQUIDACIONRUBRO, 'SECUENCIAMOVIMIENTO' VALUE :NEW.SECUENCIAMOVIMIENTO, 'SECUENCIASOBRANTES' VALUE :NEW.SECUENCIASOBRANTES, 'MONTODISPONIBLESOBRANTE' VALUE :NEW.MONTODISPONIBLESOBRANTE, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'COIGORUBROROL' VALUE :NEW.COIGORUBROROL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIADEVOLUCIONESMASIVAS' VALUE :OLD.SECUENCIADEVOLUCIONESMASIVAS, 'TIPODEVOLUCIONMASIVA' VALUE :OLD.TIPODEVOLUCIONMASIVA, 'ESTADODEVOLUCIONESMASIVAS' VALUE :OLD.ESTADODEVOLUCIONESMASIVAS, 'SECUENCIADEVOLUCIONESMASIVASDETALLE' VALUE :OLD.SECUENCIADEVOLUCIONESMASIVASDETALLE, 'CODIGOLIQUIDACIONRUBRO' VALUE :OLD.CODIGOLIQUIDACIONRUBRO, 'SECUENCIAMOVIMIENTO' VALUE :OLD.SECUENCIAMOVIMIENTO, 'SECUENCIASOBRANTES' VALUE :OLD.SECUENCIASOBRANTES, 'MONTODISPONIBLESOBRANTE' VALUE :OLD.MONTODISPONIBLESOBRANTE, 'MONTOMOVIMIENTO' VALUE :OLD.MONTOMOVIMIENTO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'COIGORUBROROL' VALUE :OLD.COIGORUBROROL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('devolucionMasiva_type', v_pk, v_event, v_payload, 'FCME_USER.DEVOLUCIONMASIVA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DOCUMENTOCREDITO  ON FCME_USER.DOCUMENTOCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DOCUMENTOCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."DOCUMENTOCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGODOCUMENTO' VALUE :NEW.CODIGODOCUMENTO, 'DESCRIPCIONDOCUMENTO' VALUE :NEW.DESCRIPCIONDOCUMENTO, 'ESTADODOCUMENTO' VALUE :NEW.ESTADODOCUMENTO, 'TIPODOCUMENTO' VALUE :NEW.TIPODOCUMENTO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGODOCUMENTO' VALUE :NEW.CODIGODOCUMENTO, 'DESCRIPCIONDOCUMENTO' VALUE :NEW.DESCRIPCIONDOCUMENTO, 'ESTADODOCUMENTO' VALUE :NEW.ESTADODOCUMENTO, 'TIPODOCUMENTO' VALUE :NEW.TIPODOCUMENTO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGODOCUMENTO' VALUE :OLD.CODIGODOCUMENTO, 'DESCRIPCIONDOCUMENTO' VALUE :OLD.DESCRIPCIONDOCUMENTO, 'ESTADODOCUMENTO' VALUE :OLD.ESTADODOCUMENTO, 'TIPODOCUMENTO' VALUE :OLD.TIPODOCUMENTO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('documentoCredito_type', v_pk, v_event, v_payload, 'FCME_USER.DOCUMENTOCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_ESTCONVCRED  ON FCME_USER.ESTADOCONVENIOCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ESTCONVCRED
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."ESTADOCONVENIOCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ESTADOREGISTROCONVENIO' VALUE :NEW.ESTADOREGISTROCONVENIO, 'DESCRIPCIONESTADO' VALUE :NEW.DESCRIPCIONESTADO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ESTADOREGISTROCONVENIO' VALUE :NEW.ESTADOREGISTROCONVENIO, 'DESCRIPCIONESTADO' VALUE :NEW.DESCRIPCIONESTADO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ESTADOREGISTROCONVENIO' VALUE :OLD.ESTADOREGISTROCONVENIO, 'DESCRIPCIONESTADO' VALUE :OLD.DESCRIPCIONESTADO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('estadoConvenioCredito_type', v_pk, v_event, v_payload, 'FCME_USER.ESTADOCONVENIOCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_ESTADOLEGAL  ON FCME_USER.ESTADOLEGALTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ESTADOLEGAL
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."ESTADOLEGALTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'NUMEROJUICIO' VALUE :NEW.NUMEROJUICIO, 'MONTO' VALUE :NEW.MONTO, 'ESTADO' VALUE :NEW.ESTADO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'NUMEROJUICIO' VALUE :NEW.NUMEROJUICIO, 'MONTO' VALUE :NEW.MONTO, 'ESTADO' VALUE :NEW.ESTADO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'NUMEROJUICIO' VALUE :OLD.NUMEROJUICIO, 'MONTO' VALUE :OLD.MONTO, 'ESTADO' VALUE :OLD.ESTADO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('estadoLegalType', v_pk, v_event, v_payload, 'FCME_USER.ESTADOLEGALTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_ETAPJUDCRED  ON FCME_USER.ETAPAJUDICIALCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ETAPJUDCRED
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."ETAPAJUDICIALCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOETAP' VALUE :NEW.CODIGOETAP, 'CODIGOEMPRESAMEDICOBR' VALUE :NEW.CODIGOEMPRESAMEDICOBR, 'CODIGOMEDI' VALUE :NEW.CODIGOMEDI);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOETAP' VALUE :NEW.CODIGOETAP, 'CODIGOEMPRESAMEDICOBR' VALUE :NEW.CODIGOEMPRESAMEDICOBR, 'CODIGOMEDI' VALUE :NEW.CODIGOMEDI);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOETAP' VALUE :OLD.CODIGOETAP, 'CODIGOEMPRESAMEDICOBR' VALUE :OLD.CODIGOEMPRESAMEDICOBR, 'CODIGOMEDI' VALUE :OLD.CODIGOMEDI);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('etapaJudicialCredito_type', v_pk, v_event, v_payload, 'FCME_USER.ETAPAJUDICIALCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_FECHASPROCESO  ON FCME_USER.FECHASPROCESOTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_FECHASPROCESO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."FECHASPROCESOTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'CODIGOSUBSISTEMA' VALUE :NEW.CODIGOSUBSISTEMA, 'CODIGOGRUPOPRODUCTO' VALUE :NEW.CODIGOGRUPOPRODUCTO, 'CODIGOPRODUCTO' VALUE :NEW.CODIGOPRODUCTO, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA, 'CODIGOSUBSISTEMA' VALUE :NEW.CODIGOSUBSISTEMA, 'CODIGOGRUPOPRODUCTO' VALUE :NEW.CODIGOGRUPOPRODUCTO, 'CODIGOPRODUCTO' VALUE :NEW.CODIGOPRODUCTO, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA, 'CODIGOSUBSISTEMA' VALUE :OLD.CODIGOSUBSISTEMA, 'CODIGOGRUPOPRODUCTO' VALUE :OLD.CODIGOGRUPOPRODUCTO, 'CODIGOPRODUCTO' VALUE :OLD.CODIGOPRODUCTO, 'CODIGOSUCURSAL' VALUE :OLD.CODIGOSUCURSAL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('fechasProcesoType', v_pk, v_event, v_payload, 'FCME_USER.FECHASPROCESOTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_FLUJOTRABAJOCREDITO  ON FCME_USER.FLUJOTRABAJOCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_FLUJOTRABAJOCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."FLUJOTRABAJOCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'FECHAGENERACION' VALUE :NEW.FECHAGENERACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'SECUENCIASEGUMIENTO' VALUE :NEW.SECUENCIASEGUMIENTO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'FECHAGENERACION' VALUE :NEW.FECHAGENERACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'SECUENCIASEGUMIENTO' VALUE :NEW.SECUENCIASEGUMIENTO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOUSUARIO' VALUE :OLD.CODIGOUSUARIO, 'FECHAGENERACION' VALUE :OLD.FECHAGENERACION, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'SECUENCIASEGUMIENTO' VALUE :OLD.SECUENCIASEGUMIENTO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('flujoTrabajoCredito_type', v_pk, v_event, v_payload, 'FCME_USER.FLUJOTRABAJOCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_GARANTIACREDITO  ON FCME_USER.GARANTIACREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_GARANTIACREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."GARANTIACREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAGARANTIA' VALUE :NEW.SECUENCIAGARANTIA, 'CODIGOPROGRAMAVIVIENDA' VALUE :NEW.CODIGOPROGRAMAVIVIENDA, 'CODIGOTIPOVIVIENDA' VALUE :NEW.CODIGOTIPOVIVIENDA, 'NUMEROVIVIENDA' VALUE :NEW.NUMEROVIVIENDA, 'NUMEROMANZANA' VALUE :NEW.NUMEROMANZANA, 'NUMEROBLOQUE' VALUE :NEW.NUMEROBLOQUE, 'SECUENCIAAVALUO' VALUE :NEW.SECUENCIAAVALUO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAENTREGA' VALUE :NEW.FECHAENTREGA, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'CODIGOUSUARIOVERIFICA' VALUE :NEW.CODIGOUSUARIOVERIFICA, 'CODIGOGARANTIA' VALUE :NEW.CODIGOGARANTIA, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAGARANTIA' VALUE :NEW.SECUENCIAGARANTIA, 'CODIGOPROGRAMAVIVIENDA' VALUE :NEW.CODIGOPROGRAMAVIVIENDA, 'CODIGOTIPOVIVIENDA' VALUE :NEW.CODIGOTIPOVIVIENDA, 'NUMEROVIVIENDA' VALUE :NEW.NUMEROVIVIENDA, 'NUMEROMANZANA' VALUE :NEW.NUMEROMANZANA, 'NUMEROBLOQUE' VALUE :NEW.NUMEROBLOQUE, 'SECUENCIAAVALUO' VALUE :NEW.SECUENCIAAVALUO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAENTREGA' VALUE :NEW.FECHAENTREGA, 'FECHAVERIFICACION' VALUE :NEW.FECHAVERIFICACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'CODIGOUSUARIOVERIFICA' VALUE :NEW.CODIGOUSUARIOVERIFICA, 'CODIGOGARANTIA' VALUE :NEW.CODIGOGARANTIA, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAGARANTIA' VALUE :OLD.SECUENCIAGARANTIA, 'CODIGOPROGRAMAVIVIENDA' VALUE :OLD.CODIGOPROGRAMAVIVIENDA, 'CODIGOTIPOVIVIENDA' VALUE :OLD.CODIGOTIPOVIVIENDA, 'NUMEROVIVIENDA' VALUE :OLD.NUMEROVIVIENDA, 'NUMEROMANZANA' VALUE :OLD.NUMEROMANZANA, 'NUMEROBLOQUE' VALUE :OLD.NUMEROBLOQUE, 'SECUENCIAAVALUO' VALUE :OLD.SECUENCIAAVALUO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHAENTREGA' VALUE :OLD.FECHAENTREGA, 'FECHAVERIFICACION' VALUE :OLD.FECHAVERIFICACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'CODIGOUSUARIOVERIFICA' VALUE :OLD.CODIGOUSUARIOVERIFICA, 'CODIGOGARANTIA' VALUE :OLD.CODIGOGARANTIA, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('garantiaCredito_type', v_pk, v_event, v_payload, 'FCME_USER.GARANTIACREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_GESTCOBRASIG  ON FCME_USER.GESTIONCOBRANZAASIGNACION_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_GESTCOBRASIG
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."GESTIONCOBRANZAASIGNACION_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'CODIGOUSUARIOASIGNADO' VALUE :NEW.CODIGOUSUARIOASIGNADO, 'CODIGOUSUARIOASIGNADOANTERIORMENTE' VALUE :NEW.CODIGOUSUARIOASIGNADOANTERIORMENTE, 'TIPOCALIFICACIONHOMOLOGADO' VALUE :NEW.TIPOCALIFICACIONHOMOLOGADO, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOASIGNACION' VALUE :NEW.CODIGOASIGNACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'CODIGOUSUARIOASIGNADO' VALUE :NEW.CODIGOUSUARIOASIGNADO, 'CODIGOUSUARIOASIGNADOANTERIORMENTE' VALUE :NEW.CODIGOUSUARIOASIGNADOANTERIORMENTE, 'TIPOCALIFICACIONHOMOLOGADO' VALUE :NEW.TIPOCALIFICACIONHOMOLOGADO, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOASIGNACION' VALUE :NEW.CODIGOASIGNACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'CODIGOUSUARIOASIGNADO' VALUE :OLD.CODIGOUSUARIOASIGNADO, 'CODIGOUSUARIOASIGNADOANTERIORMENTE' VALUE :OLD.CODIGOUSUARIOASIGNADOANTERIORMENTE, 'TIPOCALIFICACIONHOMOLOGADO' VALUE :OLD.TIPOCALIFICACIONHOMOLOGADO, 'FECHACORTE' VALUE :OLD.FECHACORTE, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'CODIGOASIGNACION' VALUE :OLD.CODIGOASIGNACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('gestionCobranzaAsignacion_type', v_pk, v_event, v_payload, 'FCME_USER.GESTIONCOBRANZAASIGNACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_GESTCOMUCRED  ON FCME_USER.GESTIONCOMUNICACIONCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_GESTCOMUCRED
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."GESTIONCOMUNICACIONCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'FECHACARGA' VALUE :NEW.FECHACARGA, 'FECHAGUIA' VALUE :NEW.FECHAGUIA, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'ESTADOGESTIONTLLAMADA' VALUE :NEW.ESTADOGESTIONTLLAMADA, 'RESULTADOGESTIONLLAMADA' VALUE :NEW.RESULTADOGESTIONLLAMADA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ESTADOGESTIONMAIL' VALUE :NEW.ESTADOGESTIONMAIL, 'ESTADOGESTIONSMS' VALUE :NEW.ESTADOGESTIONSMS, 'SECUENCIAGESTIONSMS' VALUE :NEW.SECUENCIAGESTIONSMS);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'FECHACARGA' VALUE :NEW.FECHACARGA, 'FECHAGUIA' VALUE :NEW.FECHAGUIA, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'ESTADOGESTIONTLLAMADA' VALUE :NEW.ESTADOGESTIONTLLAMADA, 'RESULTADOGESTIONLLAMADA' VALUE :NEW.RESULTADOGESTIONLLAMADA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ESTADOGESTIONMAIL' VALUE :NEW.ESTADOGESTIONMAIL, 'ESTADOGESTIONSMS' VALUE :NEW.ESTADOGESTIONSMS, 'SECUENCIAGESTIONSMS' VALUE :NEW.SECUENCIAGESTIONSMS);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'FECHACARGA' VALUE :OLD.FECHACARGA, 'FECHAGUIA' VALUE :OLD.FECHAGUIA, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'ESTADOGESTIONTLLAMADA' VALUE :OLD.ESTADOGESTIONTLLAMADA, 'RESULTADOGESTIONLLAMADA' VALUE :OLD.RESULTADOGESTIONLLAMADA, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'ESTADOGESTIONMAIL' VALUE :OLD.ESTADOGESTIONMAIL, 'ESTADOGESTIONSMS' VALUE :OLD.ESTADOGESTIONSMS, 'SECUENCIAGESTIONSMS' VALUE :OLD.SECUENCIAGESTIONSMS);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('gestionComunicacionCredito_type', v_pk, v_event, v_payload, 'FCME_USER.GESTIONCOMUNICACIONCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_GRUPOCREDITODETALLE  ON FCME_USER.GRUPOCREDITODETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_GRUPOCREDITODETALLE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."GRUPOCREDITODETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOGRUPO' VALUE :NEW.CODIGOGRUPO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOGRUPO' VALUE :NEW.CODIGOGRUPO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOGRUPO' VALUE :OLD.CODIGOGRUPO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('grupoCreditoDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.GRUPOCREDITODETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_GRUPOCREDDOCUMENTO  ON FCME_USER.GRUPOCREDITODOCUMENTO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_GRUPOCREDDOCUMENTO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."GRUPOCREDITODOCUMENTO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGODOCUMENTO' VALUE :NEW.CODIGODOCUMENTO, 'ESTADOCREDITO' VALUE :NEW.ESTADOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGODOCUMENTO' VALUE :NEW.CODIGODOCUMENTO, 'ESTADOCREDITO' VALUE :NEW.ESTADOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'CODIGODOCUMENTO' VALUE :OLD.CODIGODOCUMENTO, 'ESTADOCREDITO' VALUE :OLD.ESTADOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('grupoCreditoDocumento_type', v_pk, v_event, v_payload, 'FCME_USER.GRUPOCREDITODOCUMENTO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_INFORMACIONLEGAL  ON FCME_USER.INFORMACIONLEGAL_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_INFORMACIONLEGAL
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."INFORMACIONLEGAL_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOUSUARIORECEPTA' VALUE :NEW.CODIGOUSUARIORECEPTA, 'DESCRIPCIONREFERENCIA' VALUE :NEW.DESCRIPCIONREFERENCIA, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOUSUARIORECEPTA' VALUE :NEW.CODIGOUSUARIORECEPTA, 'DESCRIPCIONREFERENCIA' VALUE :NEW.DESCRIPCIONREFERENCIA, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOUSUARIORECEPTA' VALUE :OLD.CODIGOUSUARIORECEPTA, 'DESCRIPCIONREFERENCIA' VALUE :OLD.DESCRIPCIONREFERENCIA, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('informacionLegal_type', v_pk, v_event, v_payload, 'FCME_USER.INFORMACIONLEGAL_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_LIQDIARIACRED  ON FCME_USER.LIQUIDACIONDIARIACREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_LIQDIARIACRED
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."LIQUIDACIONDIARIACREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIALIQUIDACION' VALUE :NEW.SECUENCIALIQUIDACION, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'MONTORUBRO' VALUE :NEW.MONTORUBRO, 'ESTADOCREDITO' VALUE :NEW.ESTADOCREDITO, 'ESTADOLIQUIDACIONDIARIA' VALUE :NEW.ESTADOLIQUIDACIONDIARIA, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIALIQUIDACION' VALUE :NEW.SECUENCIALIQUIDACION, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO, 'MONTORUBRO' VALUE :NEW.MONTORUBRO, 'ESTADOCREDITO' VALUE :NEW.ESTADOCREDITO, 'ESTADOLIQUIDACIONDIARIA' VALUE :NEW.ESTADOLIQUIDACIONDIARIA, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIALIQUIDACION' VALUE :OLD.SECUENCIALIQUIDACION, 'CODIGORUBRO' VALUE :OLD.CODIGORUBRO, 'MONTORUBRO' VALUE :OLD.MONTORUBRO, 'ESTADOCREDITO' VALUE :OLD.ESTADOCREDITO, 'ESTADOLIQUIDACIONDIARIA' VALUE :OLD.ESTADOLIQUIDACIONDIARIA, 'FECHACORTE' VALUE :OLD.FECHACORTE, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('liquidacionDiariaCredito_type', v_pk, v_event, v_payload, 'FCME_USER.LIQUIDACIONDIARIACREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_MOVCONTACRED  ON FCME_USER.MOVIMIENTOCONTABLECREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_MOVCONTACRED
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."MOVIMIENTOCONTABLECREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('movimientoContableCredito_type', v_pk, v_event, v_payload, 'FCME_USER.MOVIMIENTOCONTABLECREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_OBLIGACIONROL  ON FCME_USER.OBLIGACIONROL_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_OBLIGACIONROL
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."OBLIGACIONROL_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'TIPODESCUENTO' VALUE :NEW.TIPODESCUENTO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'TIPODESCUENTO' VALUE :NEW.TIPODESCUENTO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'TIPODESCUENTO' VALUE :OLD.TIPODESCUENTO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('obligacionRol_type', v_pk, v_event, v_payload, 'FCME_USER.OBLIGACIONROL_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_OPERACIONCONYUGAL  ON FCME_USER.OPERACIONCONYUGAL_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_OPERACIONCONYUGAL
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."OPERACIONCONYUGAL_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'CODIGOTIPODEUD' VALUE :NEW.CODIGOTIPODEUD);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'CODIGOTIPODEUD' VALUE :NEW.CODIGOTIPODEUD);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'CODIGOTIPODEUD' VALUE :OLD.CODIGOTIPODEUD);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('operacionConyugal_type', v_pk, v_event, v_payload, 'FCME_USER.OPERACIONCONYUGAL_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PAGOCREDITO  ON FCME_USER.PAGOCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PAGOCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."PAGOCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CEDULAPROVEEDOR' VALUE :NEW.CEDULAPROVEEDOR, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOINSTICION' VALUE :NEW.CODIGOINSTICION, 'CODIGOPAGO' VALUE :NEW.CODIGOPAGO, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'FECHAPROCESO' VALUE :NEW.FECHAPROCESO, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'SECUENCIARECAUDACION' VALUE :NEW.SECUENCIARECAUDACION, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'VALORABONO' VALUE :NEW.VALORABONO, 'VALORPAGADO' VALUE :NEW.VALORPAGADO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CEDULAPROVEEDOR' VALUE :NEW.CEDULAPROVEEDOR, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOINSTICION' VALUE :NEW.CODIGOINSTICION, 'CODIGOPAGO' VALUE :NEW.CODIGOPAGO, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAPAGO' VALUE :NEW.FECHAPAGO, 'FECHAPROCESO' VALUE :NEW.FECHAPROCESO, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'SECUENCIARECAUDACION' VALUE :NEW.SECUENCIARECAUDACION, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'VALORABONO' VALUE :NEW.VALORABONO, 'VALORPAGADO' VALUE :NEW.VALORPAGADO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CEDULAPROVEEDOR' VALUE :OLD.CEDULAPROVEEDOR, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'CODIGOINSTICION' VALUE :OLD.CODIGOINSTICION, 'CODIGOPAGO' VALUE :OLD.CODIGOPAGO, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHAPAGO' VALUE :OLD.FECHAPAGO, 'FECHAPROCESO' VALUE :OLD.FECHAPROCESO, 'SECUENCIAABONO' VALUE :OLD.SECUENCIAABONO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'SECUENCIARECAUDACION' VALUE :OLD.SECUENCIARECAUDACION, 'SECUENCIAROL' VALUE :OLD.SECUENCIAROL, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'TIPOINSTITUCION' VALUE :OLD.TIPOINSTITUCION, 'TIPOPAGO' VALUE :OLD.TIPOPAGO, 'VALORABONO' VALUE :OLD.VALORABONO, 'VALORPAGADO' VALUE :OLD.VALORPAGADO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('pagoCredito_type', v_pk, v_event, v_payload, 'FCME_USER.PAGOCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PAGOSCREDITO  ON FCME_USER.PAGOSCREDITOTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PAGOSCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."PAGOSCREDITOTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUOTA' VALUE :NEW.CODIGOCUOTA, 'FECHAHORA' VALUE :NEW.FECHAHORA, 'CODIGOCONCEPTO' VALUE :NEW.CODIGOCONCEPTO, 'FECHAPAGO' VALUE :NEW.FECHAPAGO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCUOTA' VALUE :NEW.CODIGOCUOTA, 'FECHAHORA' VALUE :NEW.FECHAHORA, 'CODIGOCONCEPTO' VALUE :NEW.CODIGOCONCEPTO, 'FECHAPAGO' VALUE :NEW.FECHAPAGO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOCUOTA' VALUE :OLD.CODIGOCUOTA, 'FECHAHORA' VALUE :OLD.FECHAHORA, 'CODIGOCONCEPTO' VALUE :OLD.CODIGOCONCEPTO, 'FECHAPAGO' VALUE :OLD.FECHAPAGO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('pagosCreditoType', v_pk, v_event, v_payload, 'FCME_USER.PAGOSCREDITOTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PERSONACREDITO  ON FCME_USER.PERSONACREDITOTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PERSONACREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."PERSONACREDITOTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMERODIRECCION' VALUE :NEW.NUMERODIRECCION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMERODIRECCION' VALUE :NEW.NUMERODIRECCION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOIDENTIFICACION' VALUE :OLD.TIPOIDENTIFICACION, 'NUMERODIRECCION' VALUE :OLD.NUMERODIRECCION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('personaCreditoType', v_pk, v_event, v_payload, 'FCME_USER.PERSONACREDITOTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PERSONACXPCXC  ON FCME_USER.PERSONACXPCXCTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PERSONACXPCXC
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."PERSONACXPCXCTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :NEW.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'CODIGOCUENTA' VALUE :NEW.CODIGOCUENTA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOTIPOIDENTIFICACION' VALUE :OLD.CODIGOTIPOIDENTIFICACION, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'CODIGOCUENTA' VALUE :OLD.CODIGOCUENTA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('personaCxPCxCType', v_pk, v_event, v_payload, 'FCME_USER.PERSONACXPCXCTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PLANPAGOAJUSTE  ON FCME_USER.PLANPAGOAJUSTE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PLANPAGOAJUSTE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."PLANPAGOAJUSTE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONOPROCESO' VALUE :NEW.SECUENCIAABONOPROCESO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'MONTOCAPITAL' VALUE :NEW.MONTOCAPITAL, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTODIVIDENDO' VALUE :NEW.MONTODIVIDENDO, 'MONTOSEGURO' VALUE :NEW.MONTOSEGURO, 'MONTOSEGUROVEHICULO' VALUE :NEW.MONTOSEGUROVEHICULO, 'MONTOCOBROINCENDIO' VALUE :NEW.MONTOCOBROINCENDIO, 'MONTOCOMISION' VALUE :NEW.MONTOCOMISION, 'MONTOCOSTOSEMISION' VALUE :NEW.MONTOCOSTOSEMISION, 'MONTOINTERESPRIMERMES' VALUE :NEW.MONTOINTERESPRIMERMES, 'PLAZODIAS' VALUE :NEW.PLAZODIAS, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONOPROCESO' VALUE :NEW.SECUENCIAABONOPROCESO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'MONTOCAPITAL' VALUE :NEW.MONTOCAPITAL, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTODIVIDENDO' VALUE :NEW.MONTODIVIDENDO, 'MONTOSEGURO' VALUE :NEW.MONTOSEGURO, 'MONTOSEGUROVEHICULO' VALUE :NEW.MONTOSEGUROVEHICULO, 'MONTOCOBROINCENDIO' VALUE :NEW.MONTOCOBROINCENDIO, 'MONTOCOMISION' VALUE :NEW.MONTOCOMISION, 'MONTOCOSTOSEMISION' VALUE :NEW.MONTOCOSTOSEMISION, 'MONTOINTERESPRIMERMES' VALUE :NEW.MONTOINTERESPRIMERMES, 'PLAZODIAS' VALUE :NEW.PLAZODIAS, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAABONOPROCESO' VALUE :OLD.SECUENCIAABONOPROCESO, 'FECHAVENCIMIENTO' VALUE :OLD.FECHAVENCIMIENTO, 'MONTOCUOTA' VALUE :OLD.MONTOCUOTA, 'MONTOCAPITAL' VALUE :OLD.MONTOCAPITAL, 'MONTOINTERES' VALUE :OLD.MONTOINTERES, 'MONTODIVIDENDO' VALUE :OLD.MONTODIVIDENDO, 'MONTOSEGURO' VALUE :OLD.MONTOSEGURO, 'MONTOSEGUROVEHICULO' VALUE :OLD.MONTOSEGUROVEHICULO, 'MONTOCOBROINCENDIO' VALUE :OLD.MONTOCOBROINCENDIO, 'MONTOCOMISION' VALUE :OLD.MONTOCOMISION, 'MONTOCOSTOSEMISION' VALUE :OLD.MONTOCOSTOSEMISION, 'MONTOINTERESPRIMERMES' VALUE :OLD.MONTOINTERESPRIMERMES, 'PLAZODIAS' VALUE :OLD.PLAZODIAS, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('planPagoAjuste_type', v_pk, v_event, v_payload, 'FCME_USER.PLANPAGOAJUSTE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PLANPAGO  ON FCME_USER.PLANPAGO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PLANPAGO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."PLANPAGO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'ESTADOGASTOJUDICIAL' VALUE :NEW.ESTADOGASTOJUDICIAL, 'ESTADOPAGOFCME' VALUE :NEW.ESTADOPAGOFCME, 'ESTADOVENCIMIENTO' VALUE :NEW.ESTADOVENCIMIENTO, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHAINICVENC' VALUE :NEW.FECHAINICVENC, 'FECHAPAGOFCME' VALUE :NEW.FECHAPAGOFCME, 'FECHAULTIMOENVIO' VALUE :NEW.FECHAULTIMOENVIO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'INDICADORREVERSODEVENGO' VALUE :NEW.INDICADORREVERSODEVENGO, 'MOABONOMORASOLCA' VALUE :NEW.MOABONOMORASOLCA, 'MODEVENGODIARIO' VALUE :NEW.MODEVENGODIARIO, 'MONTOABONOCAPITALCAPITAL' VALUE :NEW.MONTOABONOCAPITALCAPITAL, 'MONTOABONOINTERESCAPITAL' VALUE :NEW.MONTOABONOINTERESCAPITAL, 'MONTOABONOMORA' VALUE :NEW.MONTOABONOMORA, 'MONTOCAPITAL' VALUE :NEW.MONTOCAPITAL, 'MONTOCOBROINCENDIO' VALUE :NEW.MONTOCOBROINCENDIO, 'MONTOCOMISION' VALUE :NEW.MONTOCOMISION, 'MONTOCOSTOSEMISION' VALUE :NEW.MONTOCOSTOSEMISION, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'MONTODEVENGADOINTERES' VALUE :NEW.MONTODEVENGADOINTERES, 'MONTODEVENGADOMORA' VALUE :NEW.MONTODEVENGADOMORA, 'MONTODEVENGOACUMULADO' VALUE :NEW.MONTODEVENGOACUMULADO, 'MONTODIVIDENDO' VALUE :NEW.MONTODIVIDENDO, 'MONTOGASTOSJUDICIALES' VALUE :NEW.MONTOGASTOSJUDICIALES, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTOINTERESPRIMERMES' VALUE :NEW.MONTOINTERESPRIMERMES, 'MONTORASTREOSATELITAL' VALUE :NEW.MONTORASTREOSATELITAL, 'MONTOSEGURO' VALUE :NEW.MONTOSEGURO, 'MONTOSEGUROVEHICULO' VALUE :NEW.MONTOSEGUROVEHICULO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'SECUENCIADOCUMENTO' VALUE :NEW.SECUENCIADOCUMENTO, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'ESTADOGASTOJUDICIAL' VALUE :NEW.ESTADOGASTOJUDICIAL, 'ESTADOPAGOFCME' VALUE :NEW.ESTADOPAGOFCME, 'ESTADOVENCIMIENTO' VALUE :NEW.ESTADOVENCIMIENTO, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'FECHAINICVENC' VALUE :NEW.FECHAINICVENC, 'FECHAPAGOFCME' VALUE :NEW.FECHAPAGOFCME, 'FECHAULTIMOENVIO' VALUE :NEW.FECHAULTIMOENVIO, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'INDICADORREVERSODEVENGO' VALUE :NEW.INDICADORREVERSODEVENGO, 'MOABONOMORASOLCA' VALUE :NEW.MOABONOMORASOLCA, 'MODEVENGODIARIO' VALUE :NEW.MODEVENGODIARIO, 'MONTOABONOCAPITALCAPITAL' VALUE :NEW.MONTOABONOCAPITALCAPITAL, 'MONTOABONOINTERESCAPITAL' VALUE :NEW.MONTOABONOINTERESCAPITAL, 'MONTOABONOMORA' VALUE :NEW.MONTOABONOMORA, 'MONTOCAPITAL' VALUE :NEW.MONTOCAPITAL, 'MONTOCOBROINCENDIO' VALUE :NEW.MONTOCOBROINCENDIO, 'MONTOCOMISION' VALUE :NEW.MONTOCOMISION, 'MONTOCOSTOSEMISION' VALUE :NEW.MONTOCOSTOSEMISION, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'MONTODEVENGADOINTERES' VALUE :NEW.MONTODEVENGADOINTERES, 'MONTODEVENGADOMORA' VALUE :NEW.MONTODEVENGADOMORA, 'MONTODEVENGOACUMULADO' VALUE :NEW.MONTODEVENGOACUMULADO, 'MONTODIVIDENDO' VALUE :NEW.MONTODIVIDENDO, 'MONTOGASTOSJUDICIALES' VALUE :NEW.MONTOGASTOSJUDICIALES, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTOINTERESPRIMERMES' VALUE :NEW.MONTOINTERESPRIMERMES, 'MONTORASTREOSATELITAL' VALUE :NEW.MONTORASTREOSATELITAL, 'MONTOSEGURO' VALUE :NEW.MONTOSEGURO, 'MONTOSEGUROVEHICULO' VALUE :NEW.MONTOSEGUROVEHICULO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'SECUENCIADOCUMENTO' VALUE :NEW.SECUENCIADOCUMENTO, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'ESTADOGASTOJUDICIAL' VALUE :OLD.ESTADOGASTOJUDICIAL, 'ESTADOPAGOFCME' VALUE :OLD.ESTADOPAGOFCME, 'ESTADOVENCIMIENTO' VALUE :OLD.ESTADOVENCIMIENTO, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'FECHAINICVENC' VALUE :OLD.FECHAINICVENC, 'FECHAPAGOFCME' VALUE :OLD.FECHAPAGOFCME, 'FECHAULTIMOENVIO' VALUE :OLD.FECHAULTIMOENVIO, 'FECHAVENCIMIENTO' VALUE :OLD.FECHAVENCIMIENTO, 'INDICADORREVERSODEVENGO' VALUE :OLD.INDICADORREVERSODEVENGO, 'MOABONOMORASOLCA' VALUE :OLD.MOABONOMORASOLCA, 'MODEVENGODIARIO' VALUE :OLD.MODEVENGODIARIO, 'MONTOABONOCAPITALCAPITAL' VALUE :OLD.MONTOABONOCAPITALCAPITAL, 'MONTOABONOINTERESCAPITAL' VALUE :OLD.MONTOABONOINTERESCAPITAL, 'MONTOABONOMORA' VALUE :OLD.MONTOABONOMORA, 'MONTOCAPITAL' VALUE :OLD.MONTOCAPITAL, 'MONTOCOBROINCENDIO' VALUE :OLD.MONTOCOBROINCENDIO, 'MONTOCOMISION' VALUE :OLD.MONTOCOMISION, 'MONTOCOSTOSEMISION' VALUE :OLD.MONTOCOSTOSEMISION, 'MONTOCUOTA' VALUE :OLD.MONTOCUOTA, 'MONTODEVENGADOINTERES' VALUE :OLD.MONTODEVENGADOINTERES, 'MONTODEVENGADOMORA' VALUE :OLD.MONTODEVENGADOMORA, 'MONTODEVENGOACUMULADO' VALUE :OLD.MONTODEVENGOACUMULADO, 'MONTODIVIDENDO' VALUE :OLD.MONTODIVIDENDO, 'MONTOGASTOSJUDICIALES' VALUE :OLD.MONTOGASTOSJUDICIALES, 'MONTOINTERES' VALUE :OLD.MONTOINTERES, 'MONTOINTERESPRIMERMES' VALUE :OLD.MONTOINTERESPRIMERMES, 'MONTORASTREOSATELITAL' VALUE :OLD.MONTORASTREOSATELITAL, 'MONTOSEGURO' VALUE :OLD.MONTOSEGURO, 'MONTOSEGUROVEHICULO' VALUE :OLD.MONTOSEGUROVEHICULO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'SECUENCIADOCUMENTO' VALUE :OLD.SECUENCIADOCUMENTO, 'SECUENCIAROL' VALUE :OLD.SECUENCIAROL, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('planPago_type', v_pk, v_event, v_payload, 'FCME_USER.PLANPAGO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PLAZOVENCIDO  ON FCME_USER.PLAZOVENCIDO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PLAZOVENCIDO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."PLAZOVENCIDO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIA' VALUE :NEW.SECUENCIA, 'ESTADO' VALUE :NEW.ESTADO, 'FECHACARGA' VALUE :NEW.FECHACARGA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'MONTOABONOCAPITALCAPITAL' VALUE :NEW.MONTOABONOCAPITALCAPITAL, 'MONTOABONOINTERESCAPITAL' VALUE :NEW.MONTOABONOINTERESCAPITAL, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIA' VALUE :NEW.SECUENCIA, 'ESTADO' VALUE :NEW.ESTADO, 'FECHACARGA' VALUE :NEW.FECHACARGA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAMODIFICACION' VALUE :NEW.FECHAMODIFICACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'MONTOABONOCAPITALCAPITAL' VALUE :NEW.MONTOABONOCAPITALCAPITAL, 'MONTOABONOINTERESCAPITAL' VALUE :NEW.MONTOABONOINTERESCAPITAL, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIA' VALUE :OLD.SECUENCIA, 'ESTADO' VALUE :OLD.ESTADO, 'FECHACARGA' VALUE :OLD.FECHACARGA, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAMODIFICACION' VALUE :OLD.FECHAMODIFICACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'MONTOABONOCAPITALCAPITAL' VALUE :OLD.MONTOABONOCAPITALCAPITAL, 'MONTOABONOINTERESCAPITAL' VALUE :OLD.MONTOABONOINTERESCAPITAL, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('plazoVencido_type', v_pk, v_event, v_payload, 'FCME_USER.PLAZOVENCIDO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PRECALIFCRED  ON FCME_USER.PRECALIFICACIONCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PRECALIFCRED
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."PRECALIFICACIONCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAPRECALIFICACION' VALUE :NEW.SECUENCIAPRECALIFICACION, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'ANIO' VALUE :NEW.ANIO, 'MONTOCREDITO' VALUE :NEW.MONTOCREDITO, 'MONTODIVIDENDO' VALUE :NEW.MONTODIVIDENDO, 'MONTOOTROSGASTOS' VALUE :NEW.MONTOOTROSGASTOS, 'TIPOGARANTIA' VALUE :NEW.TIPOGARANTIA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOPRECALIFICACION' VALUE :NEW.CODIGOPRECALIFICACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAPRECALIFICACION' VALUE :NEW.SECUENCIAPRECALIFICACION, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'ANIO' VALUE :NEW.ANIO, 'MONTOCREDITO' VALUE :NEW.MONTOCREDITO, 'MONTODIVIDENDO' VALUE :NEW.MONTODIVIDENDO, 'MONTOOTROSGASTOS' VALUE :NEW.MONTOOTROSGASTOS, 'TIPOGARANTIA' VALUE :NEW.TIPOGARANTIA, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOPRECALIFICACION' VALUE :NEW.CODIGOPRECALIFICACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAPRECALIFICACION' VALUE :OLD.SECUENCIAPRECALIFICACION, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'ANIO' VALUE :OLD.ANIO, 'MONTOCREDITO' VALUE :OLD.MONTOCREDITO, 'MONTODIVIDENDO' VALUE :OLD.MONTODIVIDENDO, 'MONTOOTROSGASTOS' VALUE :OLD.MONTOOTROSGASTOS, 'TIPOGARANTIA' VALUE :OLD.TIPOGARANTIA, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOPRECALIFICACION' VALUE :OLD.CODIGOPRECALIFICACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('precalificacionCredito_type', v_pk, v_event, v_payload, 'FCME_USER.PRECALIFICACIONCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PROCESOACCION  ON FCME_USER.PROCESOACCIONTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PROCESOACCION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."PROCESOACCIONTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'PROCESO' VALUE :NEW.PROCESO, 'ACCION' VALUE :NEW.ACCION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'PROCESO' VALUE :NEW.PROCESO, 'ACCION' VALUE :NEW.ACCION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'PROCESO' VALUE :OLD.PROCESO, 'ACCION' VALUE :OLD.ACCION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('procesoAccionType', v_pk, v_event, v_payload, 'FCME_USER.PROCESOACCIONTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RECUPCONV  ON FCME_USER.RECUPERACIONCONVENIO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RECUPCONV
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."RECUPERACIONCONVENIO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'TIPORECUPERACION' VALUE :NEW.TIPORECUPERACION, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'ESTADOMOVIMIENTO' VALUE :NEW.ESTADOMOVIMIENTO, 'ESTADOAUTORIZACION' VALUE :NEW.ESTADOAUTORIZACION, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'TIPOREVERSO' VALUE :NEW.TIPOREVERSO, 'FECHAABONO' VALUE :NEW.FECHAABONO, 'FECHAMOVIMIENTO' VALUE :NEW.FECHAMOVIMIENTO, 'FECHAREVERSO' VALUE :NEW.FECHAREVERSO, 'CODIGOUSUARIOLIQUIDA' VALUE :NEW.CODIGOUSUARIOLIQUIDA, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'CODIGOUSUARIOREVERSA' VALUE :NEW.CODIGOUSUARIOREVERSA, 'ESTADOVENCIMIENTO' VALUE :NEW.ESTADOVENCIMIENTO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'DESCRIPCIONLIQD' VALUE :NEW.DESCRIPCIONLIQD, 'NUMERODIASATRAZO' VALUE :NEW.NUMERODIASATRAZO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'TIPORECUPERACION' VALUE :NEW.TIPORECUPERACION, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'ESTADOMOVIMIENTO' VALUE :NEW.ESTADOMOVIMIENTO, 'ESTADOAUTORIZACION' VALUE :NEW.ESTADOAUTORIZACION, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'TIPOREVERSO' VALUE :NEW.TIPOREVERSO, 'FECHAABONO' VALUE :NEW.FECHAABONO, 'FECHAMOVIMIENTO' VALUE :NEW.FECHAMOVIMIENTO, 'FECHAREVERSO' VALUE :NEW.FECHAREVERSO, 'CODIGOUSUARIOLIQUIDA' VALUE :NEW.CODIGOUSUARIOLIQUIDA, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA, 'CODIGOUSUARIOREVERSA' VALUE :NEW.CODIGOUSUARIOREVERSA, 'ESTADOVENCIMIENTO' VALUE :NEW.ESTADOVENCIMIENTO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'DESCRIPCIONLIQD' VALUE :NEW.DESCRIPCIONLIQD, 'NUMERODIASATRAZO' VALUE :NEW.NUMERODIASATRAZO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAABONO' VALUE :OLD.SECUENCIAABONO, 'TIPORECUPERACION' VALUE :OLD.TIPORECUPERACION, 'MONTOMOVIMIENTO' VALUE :OLD.MONTOMOVIMIENTO, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'NUMEROCOMPROBANTECONTABLE' VALUE :OLD.NUMEROCOMPROBANTECONTABLE, 'ESTADOMOVIMIENTO' VALUE :OLD.ESTADOMOVIMIENTO, 'ESTADOAUTORIZACION' VALUE :OLD.ESTADOAUTORIZACION, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'TIPOREVERSO' VALUE :OLD.TIPOREVERSO, 'FECHAABONO' VALUE :OLD.FECHAABONO, 'FECHAMOVIMIENTO' VALUE :OLD.FECHAMOVIMIENTO, 'FECHAREVERSO' VALUE :OLD.FECHAREVERSO, 'CODIGOUSUARIOLIQUIDA' VALUE :OLD.CODIGOUSUARIOLIQUIDA, 'CODIGOUSUARIOCONFIRMA' VALUE :OLD.CODIGOUSUARIOCONFIRMA, 'CODIGOUSUARIOREVERSA' VALUE :OLD.CODIGOUSUARIOREVERSA, 'ESTADOVENCIMIENTO' VALUE :OLD.ESTADOVENCIMIENTO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'DESCRIPCIONLIQD' VALUE :OLD.DESCRIPCIONLIQD, 'NUMERODIASATRAZO' VALUE :OLD.NUMERODIASATRAZO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('recuperacionConvenio_type', v_pk, v_event, v_payload, 'FCME_USER.RECUPERACIONCONVENIO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RECUPERACIONCREDITO  ON FCME_USER.RECUPERACIONCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RECUPERACIONCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."RECUPERACIONCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOUSUARIOLIQUIDA' VALUE :NEW.CODIGOUSUARIOLIQUIDA, 'CODIGOUSUARIOREVERSA' VALUE :NEW.CODIGOUSUARIOREVERSA, 'DESCRIPCIONLIQUIDACION' VALUE :NEW.DESCRIPCIONLIQUIDACION, 'ESTADOMOVIMIENTO' VALUE :NEW.ESTADOMOVIMIENTO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAABONO' VALUE :NEW.FECHAABONO, 'FECHACONTABLE' VALUE :NEW.FECHACONTABLE, 'FECHAMOVIMIENTO' VALUE :NEW.FECHAMOVIMIENTO, 'FECHAREVERSION' VALUE :NEW.FECHAREVERSION, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'NUMERODIASATRASO' VALUE :NEW.NUMERODIASATRASO, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPORECUPERACION' VALUE :NEW.TIPORECUPERACION, 'TIPOREVERSO' VALUE :NEW.TIPOREVERSO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOUSUARIOLIQUIDA' VALUE :NEW.CODIGOUSUARIOLIQUIDA, 'CODIGOUSUARIOREVERSA' VALUE :NEW.CODIGOUSUARIOREVERSA, 'DESCRIPCIONLIQUIDACION' VALUE :NEW.DESCRIPCIONLIQUIDACION, 'ESTADOMOVIMIENTO' VALUE :NEW.ESTADOMOVIMIENTO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHAABONO' VALUE :NEW.FECHAABONO, 'FECHACONTABLE' VALUE :NEW.FECHACONTABLE, 'FECHAMOVIMIENTO' VALUE :NEW.FECHAMOVIMIENTO, 'FECHAREVERSION' VALUE :NEW.FECHAREVERSION, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'NUMERODIASATRASO' VALUE :NEW.NUMERODIASATRASO, 'SECUENCIAABONO' VALUE :NEW.SECUENCIAABONO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPORECUPERACION' VALUE :NEW.TIPORECUPERACION, 'TIPOREVERSO' VALUE :NEW.TIPOREVERSO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'CODIGOUSUARIOLIQUIDA' VALUE :OLD.CODIGOUSUARIOLIQUIDA, 'CODIGOUSUARIOREVERSA' VALUE :OLD.CODIGOUSUARIOREVERSA, 'DESCRIPCIONLIQUIDACION' VALUE :OLD.DESCRIPCIONLIQUIDACION, 'ESTADOMOVIMIENTO' VALUE :OLD.ESTADOMOVIMIENTO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHAABONO' VALUE :OLD.FECHAABONO, 'FECHACONTABLE' VALUE :OLD.FECHACONTABLE, 'FECHAMOVIMIENTO' VALUE :OLD.FECHAMOVIMIENTO, 'FECHAREVERSION' VALUE :OLD.FECHAREVERSION, 'MONTOMOVIMIENTO' VALUE :OLD.MONTOMOVIMIENTO, 'NUMEROCOMPROBANTECONTABLE' VALUE :OLD.NUMEROCOMPROBANTECONTABLE, 'NUMERODIASATRASO' VALUE :OLD.NUMERODIASATRASO, 'SECUENCIAABONO' VALUE :OLD.SECUENCIAABONO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'TIPORECUPERACION' VALUE :OLD.TIPORECUPERACION, 'TIPOREVERSO' VALUE :OLD.TIPOREVERSO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('recuperacionCredito_type', v_pk, v_event, v_payload, 'FCME_USER.RECUPERACIONCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_REFERENCIACLIENTE  ON FCME_USER.REFERENCIACLIENTE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_REFERENCIACLIENTE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REFERENCIACLIENTE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'CODIGOTIPODEUD' VALUE :NEW.CODIGOTIPODEUD, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'CODIGORFAM' VALUE :NEW.CODIGORFAM);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'CODIGOTIPODEUD' VALUE :NEW.CODIGOTIPODEUD, 'FECHAINGRESO' VALUE :NEW.FECHAINGRESO, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'CODIGORFAM' VALUE :NEW.CODIGORFAM);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'CODIGOCEDU' VALUE :OLD.CODIGOCEDU, 'CODIGOTIPODEUD' VALUE :OLD.CODIGOTIPODEUD, 'FECHAINGRESO' VALUE :OLD.FECHAINGRESO, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'CODIGORFAM' VALUE :OLD.CODIGORFAM);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('referenciaCliente_type', v_pk, v_event, v_payload, 'FCME_USER.REFERENCIACLIENTE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_REFERENCIADEUDOR  ON FCME_USER.REFERENCIADEUDOR_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_REFERENCIADEUDOR
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REFERENCIADEUDOR_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOTIPODEUDOR' VALUE :NEW.CODIGOTIPODEUDOR, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOTIPODEUDOR' VALUE :NEW.CODIGOTIPODEUDOR, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO, 'FECHACREACION' VALUE :NEW.FECHACREACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOTIPODEUDOR' VALUE :OLD.CODIGOTIPODEUDOR, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO, 'FECHACREACION' VALUE :OLD.FECHACREACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('referenciaDeudor_type', v_pk, v_event, v_payload, 'FCME_USER.REFERENCIADEUDOR_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_REFICRED  ON FCME_USER.REFINANCIAMIENTOCREDITOTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_REFICRED
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REFINANCIAMIENTOCREDITOTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'MONTO' VALUE :NEW.MONTO, 'FECHAREPROGRAMACION' VALUE :NEW.FECHAREPROGRAMACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'MONTO' VALUE :NEW.MONTO, 'FECHAREPROGRAMACION' VALUE :NEW.FECHAREPROGRAMACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'MONTO' VALUE :OLD.MONTO, 'FECHAREPROGRAMACION' VALUE :OLD.FECHAREPROGRAMACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('refinanciamientoCreditoType', v_pk, v_event, v_payload, 'FCME_USER.REFINANCIAMIENTOCREDITOTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_REPORTESBSCABECERA  ON FCME_USER.REPORTESBSCABECERA_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_REPORTESBSCABECERA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REPORTESBSCABECERA_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOESTRUCTURA' VALUE :NEW.CODIGOESTRUCTURA, 'CODIGOENTIDAD' VALUE :NEW.CODIGOENTIDAD, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHAGENERACION' VALUE :NEW.FECHAGENERACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOGENERACION' VALUE :NEW.CODIGOUSUARIOGENERACION, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOESTRUCTURA' VALUE :NEW.CODIGOESTRUCTURA, 'CODIGOENTIDAD' VALUE :NEW.CODIGOENTIDAD, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHAGENERACION' VALUE :NEW.FECHAGENERACION, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOUSUARIOGENERACION' VALUE :NEW.CODIGOUSUARIOGENERACION, 'CODIGOUSUARIOCONFIRMA' VALUE :NEW.CODIGOUSUARIOCONFIRMA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOESTRUCTURA' VALUE :OLD.CODIGOESTRUCTURA, 'CODIGOENTIDAD' VALUE :OLD.CODIGOENTIDAD, 'FECHACORTE' VALUE :OLD.FECHACORTE, 'FECHAGENERACION' VALUE :OLD.FECHAGENERACION, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'CODIGOUSUARIOGENERACION' VALUE :OLD.CODIGOUSUARIOGENERACION, 'CODIGOUSUARIOCONFIRMA' VALUE :OLD.CODIGOUSUARIOCONFIRMA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('reporteSBSCabecera_type', v_pk, v_event, v_payload, 'FCME_USER.REPORTESBSCABECERA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_REPORTESBSDETALLE  ON FCME_USER.REPORTESBSDETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_REPORTESBSDETALLE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REPORTESBSDETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOESTR' VALUE :NEW.CODIGOESTR, 'CODIGOENTI' VALUE :NEW.CODIGOENTI, 'FECHACORT' VALUE :NEW.FECHACORT, 'CODIGOUSUARIOGENERAL' VALUE :NEW.CODIGOUSUARIOGENERAL, 'CODIGOUSUARIOCONF' VALUE :NEW.CODIGOUSUARIOCONF, 'FECHACONF' VALUE :NEW.FECHACONF, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOENTICOPECANC' VALUE :NEW.CODIGOENTICOPECANC, 'FECHACORTCOPECANC' VALUE :NEW.FECHACORTCOPECANC, 'CODIGOUSUARIOGENERALCOPECANC' VALUE :NEW.CODIGOUSUARIOGENERALCOPECANC, 'CODIGOUSUARIOCONFCOPECANC' VALUE :NEW.CODIGOUSUARIOCONFCOPECANC, 'CODIGOENTICOPECONC' VALUE :NEW.CODIGOENTICOPECONC, 'FECHACORTCOPECONC' VALUE :NEW.FECHACORTCOPECONC, 'CODIGOUSUARIOGENERALCOPECONC' VALUE :NEW.CODIGOUSUARIOGENERALCOPECONC, 'CODIGOUSUARIOCONFCOPECONC' VALUE :NEW.CODIGOUSUARIOCONFCOPECONC, 'FECHACORTCSALOPER' VALUE :NEW.FECHACORTCSALOPER, 'CODIGOUSUARIOGENERALCSALOPER' VALUE :NEW.CODIGOUSUARIOGENERALCSALOPER, 'CODIGOUSUARIOCONFCSALOPER' VALUE :NEW.CODIGOUSUARIOCONFCSALOPER, 'FECHACORTCSUJRIES' VALUE :NEW.FECHACORTCSUJRIES, 'CODIGOUSUARIOGENERALCSUJRIES' VALUE :NEW.CODIGOUSUARIOGENERALCSUJRIES, 'CODIGOUSUARIOCONFCSUJRIES' VALUE :NEW.CODIGOUSUARIOCONFCSUJRIES);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOESTR' VALUE :NEW.CODIGOESTR, 'CODIGOENTI' VALUE :NEW.CODIGOENTI, 'FECHACORT' VALUE :NEW.FECHACORT, 'CODIGOUSUARIOGENERAL' VALUE :NEW.CODIGOUSUARIOGENERAL, 'CODIGOUSUARIOCONF' VALUE :NEW.CODIGOUSUARIOCONF, 'FECHACONF' VALUE :NEW.FECHACONF, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION, 'CODIGOENTICOPECANC' VALUE :NEW.CODIGOENTICOPECANC, 'FECHACORTCOPECANC' VALUE :NEW.FECHACORTCOPECANC, 'CODIGOUSUARIOGENERALCOPECANC' VALUE :NEW.CODIGOUSUARIOGENERALCOPECANC, 'CODIGOUSUARIOCONFCOPECANC' VALUE :NEW.CODIGOUSUARIOCONFCOPECANC, 'CODIGOENTICOPECONC' VALUE :NEW.CODIGOENTICOPECONC, 'FECHACORTCOPECONC' VALUE :NEW.FECHACORTCOPECONC, 'CODIGOUSUARIOGENERALCOPECONC' VALUE :NEW.CODIGOUSUARIOGENERALCOPECONC, 'CODIGOUSUARIOCONFCOPECONC' VALUE :NEW.CODIGOUSUARIOCONFCOPECONC, 'FECHACORTCSALOPER' VALUE :NEW.FECHACORTCSALOPER, 'CODIGOUSUARIOGENERALCSALOPER' VALUE :NEW.CODIGOUSUARIOGENERALCSALOPER, 'CODIGOUSUARIOCONFCSALOPER' VALUE :NEW.CODIGOUSUARIOCONFCSALOPER, 'FECHACORTCSUJRIES' VALUE :NEW.FECHACORTCSUJRIES, 'CODIGOUSUARIOGENERALCSUJRIES' VALUE :NEW.CODIGOUSUARIOGENERALCSUJRIES, 'CODIGOUSUARIOCONFCSUJRIES' VALUE :NEW.CODIGOUSUARIOCONFCSUJRIES);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOESTR' VALUE :OLD.CODIGOESTR, 'CODIGOENTI' VALUE :OLD.CODIGOENTI, 'FECHACORT' VALUE :OLD.FECHACORT, 'CODIGOUSUARIOGENERAL' VALUE :OLD.CODIGOUSUARIOGENERAL, 'CODIGOUSUARIOCONF' VALUE :OLD.CODIGOUSUARIOCONF, 'FECHACONF' VALUE :OLD.FECHACONF, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION, 'CODIGOENTICOPECANC' VALUE :OLD.CODIGOENTICOPECANC, 'FECHACORTCOPECANC' VALUE :OLD.FECHACORTCOPECANC, 'CODIGOUSUARIOGENERALCOPECANC' VALUE :OLD.CODIGOUSUARIOGENERALCOPECANC, 'CODIGOUSUARIOCONFCOPECANC' VALUE :OLD.CODIGOUSUARIOCONFCOPECANC, 'CODIGOENTICOPECONC' VALUE :OLD.CODIGOENTICOPECONC, 'FECHACORTCOPECONC' VALUE :OLD.FECHACORTCOPECONC, 'CODIGOUSUARIOGENERALCOPECONC' VALUE :OLD.CODIGOUSUARIOGENERALCOPECONC, 'CODIGOUSUARIOCONFCOPECONC' VALUE :OLD.CODIGOUSUARIOCONFCOPECONC, 'FECHACORTCSALOPER' VALUE :OLD.FECHACORTCSALOPER, 'CODIGOUSUARIOGENERALCSALOPER' VALUE :OLD.CODIGOUSUARIOGENERALCSALOPER, 'CODIGOUSUARIOCONFCSALOPER' VALUE :OLD.CODIGOUSUARIOCONFCSALOPER, 'FECHACORTCSUJRIES' VALUE :OLD.FECHACORTCSUJRIES, 'CODIGOUSUARIOGENERALCSUJRIES' VALUE :OLD.CODIGOUSUARIOGENERALCSUJRIES, 'CODIGOUSUARIOCONFCSUJRIES' VALUE :OLD.CODIGOUSUARIOCONFCSUJRIES);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('reporteSBSDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.REPORTESBSDETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RPTSBSGRCOD  ON FCME_USER.REPORTESBSGARANTECODEUDOR_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RPTSBSGRCOD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REPORTESBSGARANTECODEUDOR_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'CODIGOCAUSAELIMINACIONGARANTIA' VALUE :NEW.CODIGOCAUSAELIMINACIONGARANTIA, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'CODIGOCAUSAELIMINACIONGARANTIA' VALUE :NEW.CODIGOCAUSAELIMINACIONGARANTIA, 'FECHAELIMINACION' VALUE :NEW.FECHAELIMINACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'NUMEROCEDULA' VALUE :OLD.NUMEROCEDULA, 'TIPOIDENTIFICACION' VALUE :OLD.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :OLD.NUMEROOPERACIONES, 'CODIGOCAUSAELIMINACIONGARANTIA' VALUE :OLD.CODIGOCAUSAELIMINACIONGARANTIA, 'FECHAELIMINACION' VALUE :OLD.FECHAELIMINACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('reporteSBSGaranteCodeudor_type', v_pk, v_event, v_payload, 'FCME_USER.REPORTESBSGARANTECODEUDOR_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RPTSBSGARR  ON FCME_USER.REPORTESBSGARANTIAREAL_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RPTSBSGARR
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REPORTESBSGARANTIAREAL_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROIDENTIFICACION' VALUE :NEW.NUMEROIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'TIPOGARANTIA' VALUE :NEW.TIPOGARANTIA, 'NUMEROGARANTIA' VALUE :NEW.NUMEROGARANTIA, 'NUMEROREGISTROGARANTIA' VALUE :NEW.NUMEROREGISTROGARANTIA, 'DESCRIPCIONGARANTIA' VALUE :NEW.DESCRIPCIONGARANTIA, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROIDENTIFICACION' VALUE :NEW.NUMEROIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'TIPOGARANTIA' VALUE :NEW.TIPOGARANTIA, 'NUMEROGARANTIA' VALUE :NEW.NUMEROGARANTIA, 'NUMEROREGISTROGARANTIA' VALUE :NEW.NUMEROREGISTROGARANTIA, 'DESCRIPCIONGARANTIA' VALUE :NEW.DESCRIPCIONGARANTIA, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'NUMEROIDENTIFICACION' VALUE :OLD.NUMEROIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :OLD.NUMEROOPERACIONES, 'TIPOGARANTIA' VALUE :OLD.TIPOGARANTIA, 'NUMEROGARANTIA' VALUE :OLD.NUMEROGARANTIA, 'NUMEROREGISTROGARANTIA' VALUE :OLD.NUMEROREGISTROGARANTIA, 'DESCRIPCIONGARANTIA' VALUE :OLD.DESCRIPCIONGARANTIA, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('reporteSBSGarantiaReal_type', v_pk, v_event, v_payload, 'FCME_USER.REPORTESBSGARANTIAREAL_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RPTSBSOPANT  ON FCME_USER.REPORTESBSOPERACIONANTERIOR_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RPTSBSOPANT
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REPORTESBSOPERACIONANTERIOR_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'NUMEROOPERACIONANTERIOR' VALUE :NEW.NUMEROOPERACIONANTERIOR, 'FECHACONCESION' VALUE :NEW.FECHACONCESION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'NUMEROOPERACIONANTERIOR' VALUE :NEW.NUMEROOPERACIONANTERIOR, 'FECHACONCESION' VALUE :NEW.FECHACONCESION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'NUMEROOPERACIONES' VALUE :OLD.NUMEROOPERACIONES, 'NUMEROOPERACIONANTERIOR' VALUE :OLD.NUMEROOPERACIONANTERIOR, 'FECHACONCESION' VALUE :OLD.FECHACONCESION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('reporteSBSOperacionAnterior_type', v_pk, v_event, v_payload, 'FCME_USER.REPORTESBSOPERACIONANTERIOR_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RPTSBSOPCANC  ON FCME_USER.REPORTESBSOPERACIONCANCELADA_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RPTSBSOPCANC
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REPORTESBSOPERACIONCANCELADA_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'IDENTIFICACION' VALUE :NEW.IDENTIFICACION, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'IDENTIFICACION' VALUE :OLD.IDENTIFICACION, 'TIPOIDENTIFICACION' VALUE :OLD.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :OLD.NUMEROOPERACIONES);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('reporteSBSOperacionCancelada_type', v_pk, v_event, v_payload, 'FCME_USER.REPORTESBSOPERACIONCANCELADA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RPTSBSOPCONC  ON FCME_USER.REPORTESBSOPERACIONCONCEDIDA_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RPTSBSOPCONC
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REPORTESBSOPERACIONCONCEDIDA_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOCANTON' VALUE :NEW.CODIGOCANTON, 'MONTOCREDITO' VALUE :NEW.MONTOCREDITO, 'MONTOTEA' VALUE :NEW.MONTOTEA, 'TASAINTERES' VALUE :NEW.TASAINTERES, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'COTIPOCRED' VALUE :NEW.COTIPOCRED);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOCANTON' VALUE :NEW.CODIGOCANTON, 'MONTOCREDITO' VALUE :NEW.MONTOCREDITO, 'MONTOTEA' VALUE :NEW.MONTOTEA, 'TASAINTERES' VALUE :NEW.TASAINTERES, 'FECHAVENCIMIENTO' VALUE :NEW.FECHAVENCIMIENTO, 'COTIPOCRED' VALUE :NEW.COTIPOCRED);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'TIPOIDENTIFICACION' VALUE :OLD.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :OLD.NUMEROOPERACIONES, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'CODIGOCANTON' VALUE :OLD.CODIGOCANTON, 'MONTOCREDITO' VALUE :OLD.MONTOCREDITO, 'MONTOTEA' VALUE :OLD.MONTOTEA, 'TASAINTERES' VALUE :OLD.TASAINTERES, 'FECHAVENCIMIENTO' VALUE :OLD.FECHAVENCIMIENTO, 'COTIPOCRED' VALUE :OLD.COTIPOCRED);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('reporteSBSOperacionConcedida_type', v_pk, v_event, v_payload, 'FCME_USER.REPORTESBSOPERACIONCONCEDIDA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RPTSBSSALOP  ON FCME_USER.REPORTESBSSALDOOPERACION_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RPTSBSSALOP
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REPORTESBSSALDOOPERACION_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'TIPOCALIFICACION' VALUE :NEW.TIPOCALIFICACION, 'TASAINTERES' VALUE :NEW.TASAINTERES, 'MONTOPORVENCER' VALUE :NEW.MONTOPORVENCER, 'MONTOVENCIMIENTO' VALUE :NEW.MONTOVENCIMIENTO, 'MONTONODEVENGAINTERES' VALUE :NEW.MONTONODEVENGAINTERES, 'MONTOCAPITALCREDITO' VALUE :NEW.MONTOCAPITALCREDITO, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'MONTOINTERESMORA' VALUE :NEW.MONTOINTERESMORA, 'MONTODEMANDAJUDICIAL' VALUE :NEW.MONTODEMANDAJUDICIAL, 'MONTOCARTERACASTIGADA' VALUE :NEW.MONTOCARTERACASTIGADA, 'MONTOPROVISIONCONSTITUIDA' VALUE :NEW.MONTOPROVISIONCONSTITUIDA, 'MONTOPROVISIONREQUERIDA' VALUE :NEW.MONTOPROVISIONREQUERIDA, 'NUMERODIASMOROSIDAD' VALUE :NEW.NUMERODIASMOROSIDAD, 'COTAMO' VALUE :NEW.COTAMO, 'COTIPOCRED' VALUE :NEW.COTIPOCRED, 'MONTOCOSTOOPERATIVP' VALUE :NEW.MONTOCOSTOOPERATIVP, 'MONTOCUENTAINDIVIDUAL' VALUE :NEW.MONTOCUENTAINDIVIDUAL, 'MONTOSUJETPAPROVISION' VALUE :NEW.MONTOSUJETPAPROVISION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'TIPOCALIFICACION' VALUE :NEW.TIPOCALIFICACION, 'TASAINTERES' VALUE :NEW.TASAINTERES, 'MONTOPORVENCER' VALUE :NEW.MONTOPORVENCER, 'MONTOVENCIMIENTO' VALUE :NEW.MONTOVENCIMIENTO, 'MONTONODEVENGAINTERES' VALUE :NEW.MONTONODEVENGAINTERES, 'MONTOCAPITALCREDITO' VALUE :NEW.MONTOCAPITALCREDITO, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'MONTOINTERESMORA' VALUE :NEW.MONTOINTERESMORA, 'MONTODEMANDAJUDICIAL' VALUE :NEW.MONTODEMANDAJUDICIAL, 'MONTOCARTERACASTIGADA' VALUE :NEW.MONTOCARTERACASTIGADA, 'MONTOPROVISIONCONSTITUIDA' VALUE :NEW.MONTOPROVISIONCONSTITUIDA, 'MONTOPROVISIONREQUERIDA' VALUE :NEW.MONTOPROVISIONREQUERIDA, 'NUMERODIASMOROSIDAD' VALUE :NEW.NUMERODIASMOROSIDAD, 'COTAMO' VALUE :NEW.COTAMO, 'COTIPOCRED' VALUE :NEW.COTIPOCRED, 'MONTOCOSTOOPERATIVP' VALUE :NEW.MONTOCOSTOOPERATIVP, 'MONTOCUENTAINDIVIDUAL' VALUE :NEW.MONTOCUENTAINDIVIDUAL, 'MONTOSUJETPAPROVISION' VALUE :NEW.MONTOSUJETPAPROVISION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'TIPOIDENTIFICACION' VALUE :OLD.TIPOIDENTIFICACION, 'NUMEROOPERACIONES' VALUE :OLD.NUMEROOPERACIONES, 'TIPOCALIFICACION' VALUE :OLD.TIPOCALIFICACION, 'TASAINTERES' VALUE :OLD.TASAINTERES, 'MONTOPORVENCER' VALUE :OLD.MONTOPORVENCER, 'MONTOVENCIMIENTO' VALUE :OLD.MONTOVENCIMIENTO, 'MONTONODEVENGAINTERES' VALUE :OLD.MONTONODEVENGAINTERES, 'MONTOCAPITALCREDITO' VALUE :OLD.MONTOCAPITALCREDITO, 'MONTOCUOTA' VALUE :OLD.MONTOCUOTA, 'MONTOINTERESMORA' VALUE :OLD.MONTOINTERESMORA, 'MONTODEMANDAJUDICIAL' VALUE :OLD.MONTODEMANDAJUDICIAL, 'MONTOCARTERACASTIGADA' VALUE :OLD.MONTOCARTERACASTIGADA, 'MONTOPROVISIONCONSTITUIDA' VALUE :OLD.MONTOPROVISIONCONSTITUIDA, 'MONTOPROVISIONREQUERIDA' VALUE :OLD.MONTOPROVISIONREQUERIDA, 'NUMERODIASMOROSIDAD' VALUE :OLD.NUMERODIASMOROSIDAD, 'COTAMO' VALUE :OLD.COTAMO, 'COTIPOCRED' VALUE :OLD.COTIPOCRED, 'MONTOCOSTOOPERATIVP' VALUE :OLD.MONTOCOSTOOPERATIVP, 'MONTOCUENTAINDIVIDUAL' VALUE :OLD.MONTOCUENTAINDIVIDUAL, 'MONTOSUJETPAPROVISION' VALUE :OLD.MONTOSUJETPAPROVISION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('reporteSBSSaldoOperacion_type', v_pk, v_event, v_payload, 'FCME_USER.REPORTESBSSALDOOPERACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RPTSBSSJTO  ON FCME_USER.REPORTESBSSUJETORIESGO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RPTSBSSJTO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."REPORTESBSSUJETORIESGO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'CODIGOPROFESION' VALUE :NEW.CODIGOPROFESION, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOCANTON' VALUE :NEW.CODIGOCANTON, 'CODIGOPARROQUIA' VALUE :NEW.CODIGOPARROQUIA, 'COGNRO' VALUE :NEW.COGNRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'NUMEROCEDULA' VALUE :NEW.NUMEROCEDULA, 'TIPOIDENTIFICACION' VALUE :NEW.TIPOIDENTIFICACION, 'CODIGOPROFESION' VALUE :NEW.CODIGOPROFESION, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CODIGOCANTON' VALUE :NEW.CODIGOCANTON, 'CODIGOPARROQUIA' VALUE :NEW.CODIGOPARROQUIA, 'COGNRO' VALUE :NEW.COGNRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'NUMEROCEDULA' VALUE :OLD.NUMEROCEDULA, 'TIPOIDENTIFICACION' VALUE :OLD.TIPOIDENTIFICACION, 'CODIGOPROFESION' VALUE :OLD.CODIGOPROFESION, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'CODIGOCANTON' VALUE :OLD.CODIGOCANTON, 'CODIGOPARROQUIA' VALUE :OLD.CODIGOPARROQUIA, 'COGNRO' VALUE :OLD.COGNRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('reporteSBSSujetoRiesgo_type', v_pk, v_event, v_payload, 'FCME_USER.REPORTESBSSUJETORIESGO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RUBROCOBRANZA  ON FCME_USER.RUBROCOBRANZA_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RUBROCOBRANZA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."RUBROCOBRANZA_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'TIPORUBROPAGO' VALUE :NEW.TIPORUBROPAGO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'TIPORUBROPAGO' VALUE :NEW.TIPORUBROPAGO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'TIPOPAGO' VALUE :OLD.TIPOPAGO, 'TIPORUBROPAGO' VALUE :OLD.TIPORUBROPAGO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('rubroCobranza_type', v_pk, v_event, v_payload, 'FCME_USER.RUBROCOBRANZA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RUBRCOBRDETA  ON FCME_USER.RUBROSCOBRANZADETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RUBRCOBRDETA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."RUBROSCOBRANZADETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCRED' VALUE :NEW.TIPOCRED, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'TIPORUBRPAGO' VALUE :NEW.TIPORUBRPAGO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCRED' VALUE :NEW.TIPOCRED, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'TIPORUBRPAGO' VALUE :NEW.TIPORUBRPAGO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOCRED' VALUE :OLD.TIPOCRED, 'TIPOPAGO' VALUE :OLD.TIPOPAGO, 'TIPORUBRPAGO' VALUE :OLD.TIPORUBRPAGO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('rubrosCobranzaDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.RUBROSCOBRANZADETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SALDOCARTERADETALLE  ON FCME_USER.SALDOCARTERADETALLE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SALDOCARTERADETALLE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SALDOCARTERADETALLE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'FECHACORT' VALUE :NEW.FECHACORT, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'NUMERODCTO' VALUE :NEW.NUMERODCTO, 'MONTOSALDOCAPIVCDO' VALUE :NEW.MONTOSALDOCAPIVCDO, 'MONTOSALDOCAPIXVEN' VALUE :NEW.MONTOSALDOCAPIXVEN, 'MONTOINTEDVGO' VALUE :NEW.MONTOINTEDVGO, 'MONTOINTEABNO' VALUE :NEW.MONTOINTEABNO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'FECHACORT' VALUE :NEW.FECHACORT, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'NUMERODCTO' VALUE :NEW.NUMERODCTO, 'MONTOSALDOCAPIVCDO' VALUE :NEW.MONTOSALDOCAPIVCDO, 'MONTOSALDOCAPIXVEN' VALUE :NEW.MONTOSALDOCAPIXVEN, 'MONTOINTEDVGO' VALUE :NEW.MONTOINTEDVGO, 'MONTOINTEABNO' VALUE :NEW.MONTOINTEABNO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'FECHACORT' VALUE :OLD.FECHACORT, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'NUMERODCTO' VALUE :OLD.NUMERODCTO, 'MONTOSALDOCAPIVCDO' VALUE :OLD.MONTOSALDOCAPIVCDO, 'MONTOSALDOCAPIXVEN' VALUE :OLD.MONTOSALDOCAPIXVEN, 'MONTOINTEDVGO' VALUE :OLD.MONTOINTEDVGO, 'MONTOINTEABNO' VALUE :OLD.MONTOINTEABNO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('saldoCarteraDetalle_type', v_pk, v_event, v_payload, 'FCME_USER.SALDOCARTERADETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SALDOCARTERA  ON FCME_USER.SALDOCARTERA_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SALDOCARTERA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SALDOCARTERA_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'FECHASALDO' VALUE :NEW.FECHASALDO, 'MONTOCAPITAL' VALUE :NEW.MONTOCAPITAL, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTOSALDOCAPITALPORVENCER' VALUE :NEW.MONTOSALDOCAPITALPORVENCER, 'MONTOSALDOCAPITALVENCIDO' VALUE :NEW.MONTOSALDOCAPITALVENCIDO, 'MONTOABONADOALCAPITALPORVENCER' VALUE :NEW.MONTOABONADOALCAPITALPORVENCER, 'MONTOABONADOALCAPITALVENCIDO' VALUE :NEW.MONTOABONADOALCAPITALVENCIDO, 'MONTOABONADOALINTERES' VALUE :NEW.MONTOABONADOALINTERES, 'MONTOABONOMORA' VALUE :NEW.MONTOABONOMORA, 'MONTOINTERESREVERSADO' VALUE :NEW.MONTOINTERESREVERSADO, 'MONTOINTERESDEVENGADO' VALUE :NEW.MONTOINTERESDEVENGADO, 'MONTOINTERESABONADO' VALUE :NEW.MONTOINTERESABONADO, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'MONTOABONOCAPITALCAPITAL' VALUE :NEW.MONTOABONOCAPITALCAPITAL, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'FECHASALDO' VALUE :NEW.FECHASALDO, 'MONTOCAPITAL' VALUE :NEW.MONTOCAPITAL, 'MONTOINTERES' VALUE :NEW.MONTOINTERES, 'MONTOSALDOCAPITALPORVENCER' VALUE :NEW.MONTOSALDOCAPITALPORVENCER, 'MONTOSALDOCAPITALVENCIDO' VALUE :NEW.MONTOSALDOCAPITALVENCIDO, 'MONTOABONADOALCAPITALPORVENCER' VALUE :NEW.MONTOABONADOALCAPITALPORVENCER, 'MONTOABONADOALCAPITALVENCIDO' VALUE :NEW.MONTOABONADOALCAPITALVENCIDO, 'MONTOABONADOALINTERES' VALUE :NEW.MONTOABONADOALINTERES, 'MONTOABONOMORA' VALUE :NEW.MONTOABONOMORA, 'MONTOINTERESREVERSADO' VALUE :NEW.MONTOINTERESREVERSADO, 'MONTOINTERESDEVENGADO' VALUE :NEW.MONTOINTERESDEVENGADO, 'MONTOINTERESABONADO' VALUE :NEW.MONTOINTERESABONADO, 'NUMEROOPERACIONES' VALUE :NEW.NUMEROOPERACIONES, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'MONTOABONOCAPITALCAPITAL' VALUE :NEW.MONTOABONOCAPITALCAPITAL, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'FECHASALDO' VALUE :OLD.FECHASALDO, 'MONTOCAPITAL' VALUE :OLD.MONTOCAPITAL, 'MONTOINTERES' VALUE :OLD.MONTOINTERES, 'MONTOSALDOCAPITALPORVENCER' VALUE :OLD.MONTOSALDOCAPITALPORVENCER, 'MONTOSALDOCAPITALVENCIDO' VALUE :OLD.MONTOSALDOCAPITALVENCIDO, 'MONTOABONADOALCAPITALPORVENCER' VALUE :OLD.MONTOABONADOALCAPITALPORVENCER, 'MONTOABONADOALCAPITALVENCIDO' VALUE :OLD.MONTOABONADOALCAPITALVENCIDO, 'MONTOABONADOALINTERES' VALUE :OLD.MONTOABONADOALINTERES, 'MONTOABONOMORA' VALUE :OLD.MONTOABONOMORA, 'MONTOINTERESREVERSADO' VALUE :OLD.MONTOINTERESREVERSADO, 'MONTOINTERESDEVENGADO' VALUE :OLD.MONTOINTERESDEVENGADO, 'MONTOINTERESABONADO' VALUE :OLD.MONTOINTERESABONADO, 'NUMEROOPERACIONES' VALUE :OLD.NUMEROOPERACIONES, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'MONTOABONOCAPITALCAPITAL' VALUE :OLD.MONTOABONOCAPITALCAPITAL, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('saldoCartera_type', v_pk, v_event, v_payload, 'FCME_USER.SALDOCARTERA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SALDOCXPCXC  ON FCME_USER.SALDOCXPCXCTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SALDOCXPCXC
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SALDOCXPCXCTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOPRODUCTO' VALUE :NEW.CODIGOPRODUCTO, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'CODIGOOFICINA' VALUE :NEW.CODIGOOFICINA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOPRODUCTO' VALUE :NEW.CODIGOPRODUCTO, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'CODIGOOFICINA' VALUE :NEW.CODIGOOFICINA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOPRODUCTO' VALUE :OLD.CODIGOPRODUCTO, 'CODIGOSUCURSAL' VALUE :OLD.CODIGOSUCURSAL, 'CODIGOOFICINA' VALUE :OLD.CODIGOOFICINA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('saldoCxPCxCType', v_pk, v_event, v_payload, 'FCME_USER.SALDOCXPCXCTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SALDOVINCULADO  ON FCME_USER.SALDOVINCULADOTYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SALDOVINCULADO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SALDOVINCULADOTYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'CODIGOOFICINA' VALUE :NEW.CODIGOOFICINA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSUCURSAL' VALUE :NEW.CODIGOSUCURSAL, 'CODIGOOFICINA' VALUE :NEW.CODIGOOFICINA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOSUCURSAL' VALUE :OLD.CODIGOSUCURSAL, 'CODIGOOFICINA' VALUE :OLD.CODIGOOFICINA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('saldoVinculadoType', v_pk, v_event, v_payload, 'FCME_USER.SALDOVINCULADOTYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SEGAUTR  ON FCME_USER.SEGUIMIENTOAUTORIZACION_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SEGAUTR
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SEGUIMIENTOAUTORIZACION_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIASEGUIMIENTO' VALUE :NEW.SECUENCIASEGUIMIENTO, 'CODIGOMOTIVORECHAZO' VALUE :NEW.CODIGOMOTIVORECHAZO, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'ESTADOSEGUIMIENTO' VALUE :NEW.ESTADOSEGUIMIENTO, 'DESCRIPCIONOBSERVACIONES' VALUE :NEW.DESCRIPCIONOBSERVACIONES, 'FECHAACTUALIZACION' VALUE :NEW.FECHAACTUALIZACION, 'CODIGOUSUARIOTRANSACCION' VALUE :NEW.CODIGOUSUARIOTRANSACCION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIASEGUIMIENTO' VALUE :NEW.SECUENCIASEGUIMIENTO, 'CODIGOMOTIVORECHAZO' VALUE :NEW.CODIGOMOTIVORECHAZO, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'ESTADOSEGUIMIENTO' VALUE :NEW.ESTADOSEGUIMIENTO, 'DESCRIPCIONOBSERVACIONES' VALUE :NEW.DESCRIPCIONOBSERVACIONES, 'FECHAACTUALIZACION' VALUE :NEW.FECHAACTUALIZACION, 'CODIGOUSUARIOTRANSACCION' VALUE :NEW.CODIGOUSUARIOTRANSACCION, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIASEGUIMIENTO' VALUE :OLD.SECUENCIASEGUIMIENTO, 'CODIGOMOTIVORECHAZO' VALUE :OLD.CODIGOMOTIVORECHAZO, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'ESTADOSEGUIMIENTO' VALUE :OLD.ESTADOSEGUIMIENTO, 'DESCRIPCIONOBSERVACIONES' VALUE :OLD.DESCRIPCIONOBSERVACIONES, 'FECHAACTUALIZACION' VALUE :OLD.FECHAACTUALIZACION, 'CODIGOUSUARIOTRANSACCION' VALUE :OLD.CODIGOUSUARIOTRANSACCION, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('seguimientoAutorizacion_type', v_pk, v_event, v_payload, 'FCME_USER.SEGUIMIENTOAUTORIZACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SEGUROCREDITO  ON FCME_USER.SEGUROCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SEGUROCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SEGUROCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOPROVEEDOR' VALUE :NEW.CODIGOPROVEEDOR, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOPROVEEDOR' VALUE :NEW.CODIGOPROVEEDOR, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOPROVEEDOR' VALUE :OLD.CODIGOPROVEEDOR, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('seguroCredito_type', v_pk, v_event, v_payload, 'FCME_USER.SEGUROCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SOBRANTECAUCION  ON FCME_USER.SOBRANTECAUCIÓN_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SOBRANTECAUCION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SOBRANTECAUCIÓN_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOSOBRANTE' VALUE :NEW.TIPOSOBRANTE, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'VACUENTAAUTO' VALUE :NEW.VACUENTAAUTO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOSOBRANTE' VALUE :NEW.TIPOSOBRANTE, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'VACUENTAAUTO' VALUE :NEW.VACUENTAAUTO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOSOBRANTE' VALUE :OLD.TIPOSOBRANTE, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'VACUENTAAUTO' VALUE :OLD.VACUENTAAUTO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('sobranteCaucion_type', v_pk, v_event, v_payload, 'FCME_USER.SOBRANTECAUCIÓN_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SOBRANTECREDITO  ON FCME_USER.SOBRANTECREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SOBRANTECREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SOBRANTECREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIASOBRANTES' VALUE :NEW.SECUENCIASOBRANTES, 'TIPOSOBRANTE' VALUE :NEW.TIPOSOBRANTE, 'TIPOAPLICACION' VALUE :NEW.TIPOAPLICACION, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'CEDULAPROVEEDOR' VALUE :NEW.CEDULAPROVEEDOR, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION, 'CODIGOPAGO' VALUE :NEW.CODIGOPAGO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'SECUENCIARECAUDACION' VALUE :NEW.SECUENCIARECAUDACION, 'MONTOSOBRANTE' VALUE :NEW.MONTOSOBRANTE, 'MONTODISPONIBLESOBRANTE' VALUE :NEW.MONTODISPONIBLESOBRANTE, 'DESCRIPCIONOPERACIONREFERENCIA' VALUE :NEW.DESCRIPCIONOPERACIONREFERENCIA, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHAPROCESO' VALUE :NEW.FECHAPROCESO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOINSTICION' VALUE :NEW.CODIGOINSTICION, 'ESTADODEVOLUCIONESMASIVAS' VALUE :NEW.ESTADODEVOLUCIONESMASIVAS, 'FECHALADEVOLUCION' VALUE :NEW.FECHALADEVOLUCION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIASOBRANTES' VALUE :NEW.SECUENCIASOBRANTES, 'TIPOSOBRANTE' VALUE :NEW.TIPOSOBRANTE, 'TIPOAPLICACION' VALUE :NEW.TIPOAPLICACION, 'TIPOPAGO' VALUE :NEW.TIPOPAGO, 'CEDULAPROVEEDOR' VALUE :NEW.CEDULAPROVEEDOR, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'TIPOINSTITUCION' VALUE :NEW.TIPOINSTITUCION, 'CODIGOPAGO' VALUE :NEW.CODIGOPAGO, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'SECUENCIARECAUDACION' VALUE :NEW.SECUENCIARECAUDACION, 'MONTOSOBRANTE' VALUE :NEW.MONTOSOBRANTE, 'MONTODISPONIBLESOBRANTE' VALUE :NEW.MONTODISPONIBLESOBRANTE, 'DESCRIPCIONOPERACIONREFERENCIA' VALUE :NEW.DESCRIPCIONOPERACIONREFERENCIA, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'FECHACORTE' VALUE :NEW.FECHACORTE, 'FECHAPROCESO' VALUE :NEW.FECHAPROCESO, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOINSTICION' VALUE :NEW.CODIGOINSTICION, 'ESTADODEVOLUCIONESMASIVAS' VALUE :NEW.ESTADODEVOLUCIONESMASIVAS, 'FECHALADEVOLUCION' VALUE :NEW.FECHALADEVOLUCION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIASOBRANTES' VALUE :OLD.SECUENCIASOBRANTES, 'TIPOSOBRANTE' VALUE :OLD.TIPOSOBRANTE, 'TIPOAPLICACION' VALUE :OLD.TIPOAPLICACION, 'TIPOPAGO' VALUE :OLD.TIPOPAGO, 'CEDULAPROVEEDOR' VALUE :OLD.CEDULAPROVEEDOR, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'TIPOINSTITUCION' VALUE :OLD.TIPOINSTITUCION, 'CODIGOPAGO' VALUE :OLD.CODIGOPAGO, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'SECUENCIAROL' VALUE :OLD.SECUENCIAROL, 'SECUENCIARECAUDACION' VALUE :OLD.SECUENCIARECAUDACION, 'MONTOSOBRANTE' VALUE :OLD.MONTOSOBRANTE, 'MONTODISPONIBLESOBRANTE' VALUE :OLD.MONTODISPONIBLESOBRANTE, 'DESCRIPCIONOPERACIONREFERENCIA' VALUE :OLD.DESCRIPCIONOPERACIONREFERENCIA, 'NUMEROCOMPROBANTECONTABLE' VALUE :OLD.NUMEROCOMPROBANTECONTABLE, 'FECHACORTE' VALUE :OLD.FECHACORTE, 'FECHAPROCESO' VALUE :OLD.FECHAPROCESO, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOINSTICION' VALUE :OLD.CODIGOINSTICION, 'ESTADODEVOLUCIONESMASIVAS' VALUE :OLD.ESTADODEVOLUCIONESMASIVAS, 'FECHALADEVOLUCION' VALUE :OLD.FECHALADEVOLUCION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('sobranteCredito_type', v_pk, v_event, v_payload, 'FCME_USER.SOBRANTECREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SOBRDIST  ON FCME_USER.SOBRANTEDISTRIBUCION_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SOBRDIST
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SOBRANTEDISTRIBUCION_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIASOBRANTES' VALUE :NEW.SECUENCIASOBRANTES, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'TIPOAPLICACION' VALUE :NEW.TIPOAPLICACION, 'MONTOAPLICADO' VALUE :NEW.MONTOAPLICADO, 'DESCRIPCIONREFERENCIA' VALUE :NEW.DESCRIPCIONREFERENCIA, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'FECHAAPLICACION' VALUE :NEW.FECHAAPLICACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIASOBRANTES' VALUE :NEW.SECUENCIASOBRANTES, 'SECUENCIAREGISTRO' VALUE :NEW.SECUENCIAREGISTRO, 'TIPOAPLICACION' VALUE :NEW.TIPOAPLICACION, 'MONTOAPLICADO' VALUE :NEW.MONTOAPLICADO, 'DESCRIPCIONREFERENCIA' VALUE :NEW.DESCRIPCIONREFERENCIA, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'FECHAAPLICACION' VALUE :NEW.FECHAAPLICACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIASOBRANTES' VALUE :OLD.SECUENCIASOBRANTES, 'SECUENCIAREGISTRO' VALUE :OLD.SECUENCIAREGISTRO, 'TIPOAPLICACION' VALUE :OLD.TIPOAPLICACION, 'MONTOAPLICADO' VALUE :OLD.MONTOAPLICADO, 'DESCRIPCIONREFERENCIA' VALUE :OLD.DESCRIPCIONREFERENCIA, 'NUMEROCOMPROBANTECONTABLE' VALUE :OLD.NUMEROCOMPROBANTECONTABLE, 'FECHAAPLICACION' VALUE :OLD.FECHAAPLICACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('sobranteDistribucion_type', v_pk, v_event, v_payload, 'FCME_USER.SOBRANTEDISTRIBUCION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SOLIDARIOCREDITO  ON FCME_USER.SOLIDARIOCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SOLIDARIOCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."SOLIDARIOCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'INDICADORSOLIDARIO' VALUE :NEW.INDICADORSOLIDARIO, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'INDICADORSOLIDARIO' VALUE :NEW.INDICADORSOLIDARIO, 'MONTOCUOTA' VALUE :NEW.MONTOCUOTA, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'INDICADORSOLIDARIO' VALUE :OLD.INDICADORSOLIDARIO, 'MONTOCUOTA' VALUE :OLD.MONTOCUOTA, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('solidarioCredito_type', v_pk, v_event, v_payload, 'FCME_USER.SOLIDARIOCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_TASAINTERESCREDITO  ON FCME_USER.TASAINTERESCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_TASAINTERESCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."TASAINTERESCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOMONEDA' VALUE :NEW.CODIGOMONEDA, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOMONEDA' VALUE :NEW.CODIGOMONEDA, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOMONEDA' VALUE :OLD.CODIGOMONEDA, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('tasaInteresCredito_type', v_pk, v_event, v_payload, 'FCME_USER.TASAINTERESCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_TIPOCREDITO  ON FCME_USER.TIPOCREDITO_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_TIPOCREDITO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."TIPOCREDITO_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'DESCRIPCIONCREDITO' VALUE :NEW.DESCRIPCIONCREDITO, 'CODIGOGRUPO' VALUE :NEW.CODIGOGRUPO, 'ESTADOCREDITO' VALUE :NEW.ESTADOCREDITO, 'ESTADOGARANTE' VALUE :NEW.ESTADOGARANTE, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'DESCRIPCIONCREDITO' VALUE :NEW.DESCRIPCIONCREDITO, 'CODIGOGRUPO' VALUE :NEW.CODIGOGRUPO, 'ESTADOCREDITO' VALUE :NEW.ESTADOCREDITO, 'ESTADOGARANTE' VALUE :NEW.ESTADOGARANTE, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'DESCRIPCIONCREDITO' VALUE :OLD.DESCRIPCIONCREDITO, 'CODIGOGRUPO' VALUE :OLD.CODIGOGRUPO, 'ESTADOCREDITO' VALUE :OLD.ESTADOCREDITO, 'ESTADOGARANTE' VALUE :OLD.ESTADOGARANTE, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('tipoCredito_type', v_pk, v_event, v_payload, 'FCME_USER.TIPOCREDITO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_TIPOSOBRANTE  ON FCME_USER.TIPOSOBRANTE_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_TIPOSOBRANTE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."TIPOSOBRANTE_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOSOBRANTE' VALUE :NEW.TIPOSOBRANTE, 'DESCRIPCIONSOBRANTE' VALUE :NEW.DESCRIPCIONSOBRANTE, 'ESTADOSOBRANTE' VALUE :NEW.ESTADOSOBRANTE);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPOSOBRANTE' VALUE :NEW.TIPOSOBRANTE, 'DESCRIPCIONSOBRANTE' VALUE :NEW.DESCRIPCIONSOBRANTE, 'ESTADOSOBRANTE' VALUE :NEW.ESTADOSOBRANTE);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPOSOBRANTE' VALUE :OLD.TIPOSOBRANTE, 'DESCRIPCIONSOBRANTE' VALUE :OLD.DESCRIPCIONSOBRANTE, 'ESTADOSOBRANTE' VALUE :OLD.ESTADOSOBRANTE);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('tipoSobrante_type', v_pk, v_event, v_payload, 'FCME_USER.TIPOSOBRANTE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_TRANSRECUP  ON FCME_USER.TRANSACCIONRECUPERACION_TYPE --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_TRANSRECUP
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER."TRANSACCIONRECUPERACION_TYPE"
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPORECP' VALUE :NEW.TIPORECP, 'ESTADORECP' VALUE :NEW.ESTADORECP, 'INDICADORRECA' VALUE :NEW.INDICADORRECA, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'SECUENCIAABNO' VALUE :NEW.SECUENCIAABNO, 'TIPORECPRECUP' VALUE :NEW.TIPORECPRECUP, 'CODIGOREGISTRO' VALUE :NEW.CODIGOREGISTRO, 'FECHAMOVIMIENTO' VALUE :NEW.FECHAMOVIMIENTO, 'FECHAABNO' VALUE :NEW.FECHAABNO, 'FECHACONTABLE' VALUE :NEW.FECHACONTABLE, 'FECHAREVZ' VALUE :NEW.FECHAREVZ, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'INDICADORCONFFOND' VALUE :NEW.INDICADORCONFFOND, 'CODIGOAUTORIZACION' VALUE :NEW.CODIGOAUTORIZACION, 'TEXTOLIQUIDACION' VALUE :NEW.TEXTOLIQUIDACION, 'CODIGOUSUARIOCONF' VALUE :NEW.CODIGOUSUARIOCONF, 'CODIGOUSUARIOLIQUIDACION' VALUE :NEW.CODIGOUSUARIOLIQUIDACION, 'NUMERODIASATRA' VALUE :NEW.NUMERODIASATRA, 'TIPOREVZ' VALUE :NEW.TIPOREVZ, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOUSUARIOREVZ' VALUE :NEW.CODIGOUSUARIOREVZ, 'TIPODIARIO' VALUE :NEW.TIPODIARIO, 'INDICADORCONTABLEREVZ' VALUE :NEW.INDICADORCONTABLEREVZ);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPORECP' VALUE :NEW.TIPORECP, 'ESTADORECP' VALUE :NEW.ESTADORECP, 'INDICADORRECA' VALUE :NEW.INDICADORRECA, 'TIPOCREDITO' VALUE :NEW.TIPOCREDITO, 'ANIOCREDITO' VALUE :NEW.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :NEW.SECUENCIACREDITO, 'SECUENCIAABNO' VALUE :NEW.SECUENCIAABNO, 'TIPORECPRECUP' VALUE :NEW.TIPORECPRECUP, 'CODIGOREGISTRO' VALUE :NEW.CODIGOREGISTRO, 'FECHAMOVIMIENTO' VALUE :NEW.FECHAMOVIMIENTO, 'FECHAABNO' VALUE :NEW.FECHAABNO, 'FECHACONTABLE' VALUE :NEW.FECHACONTABLE, 'FECHAREVZ' VALUE :NEW.FECHAREVZ, 'MONTOMOVIMIENTO' VALUE :NEW.MONTOMOVIMIENTO, 'NUMEROCOMPROBANTECONTABLE' VALUE :NEW.NUMEROCOMPROBANTECONTABLE, 'INDICADORCONFFOND' VALUE :NEW.INDICADORCONFFOND, 'CODIGOAUTORIZACION' VALUE :NEW.CODIGOAUTORIZACION, 'TEXTOLIQUIDACION' VALUE :NEW.TEXTOLIQUIDACION, 'CODIGOUSUARIOCONF' VALUE :NEW.CODIGOUSUARIOCONF, 'CODIGOUSUARIOLIQUIDACION' VALUE :NEW.CODIGOUSUARIOLIQUIDACION, 'NUMERODIASATRA' VALUE :NEW.NUMERODIASATRA, 'TIPOREVZ' VALUE :NEW.TIPOREVZ, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'CODIGOUSUARIOREVZ' VALUE :NEW.CODIGOUSUARIOREVZ, 'TIPODIARIO' VALUE :NEW.TIPODIARIO, 'INDICADORCONTABLEREVZ' VALUE :NEW.INDICADORCONTABLEREVZ);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPORECP' VALUE :OLD.TIPORECP, 'ESTADORECP' VALUE :OLD.ESTADORECP, 'INDICADORRECA' VALUE :OLD.INDICADORRECA, 'TIPOCREDITO' VALUE :OLD.TIPOCREDITO, 'ANIOCREDITO' VALUE :OLD.ANIOCREDITO, 'SECUENCIACREDITO' VALUE :OLD.SECUENCIACREDITO, 'SECUENCIAABNO' VALUE :OLD.SECUENCIAABNO, 'TIPORECPRECUP' VALUE :OLD.TIPORECPRECUP, 'CODIGOREGISTRO' VALUE :OLD.CODIGOREGISTRO, 'FECHAMOVIMIENTO' VALUE :OLD.FECHAMOVIMIENTO, 'FECHAABNO' VALUE :OLD.FECHAABNO, 'FECHACONTABLE' VALUE :OLD.FECHACONTABLE, 'FECHAREVZ' VALUE :OLD.FECHAREVZ, 'MONTOMOVIMIENTO' VALUE :OLD.MONTOMOVIMIENTO, 'NUMEROCOMPROBANTECONTABLE' VALUE :OLD.NUMEROCOMPROBANTECONTABLE, 'INDICADORCONFFOND' VALUE :OLD.INDICADORCONFFOND, 'CODIGOAUTORIZACION' VALUE :OLD.CODIGOAUTORIZACION, 'TEXTOLIQUIDACION' VALUE :OLD.TEXTOLIQUIDACION, 'CODIGOUSUARIOCONF' VALUE :OLD.CODIGOUSUARIOCONF, 'CODIGOUSUARIOLIQUIDACION' VALUE :OLD.CODIGOUSUARIOLIQUIDACION, 'NUMERODIASATRA' VALUE :OLD.NUMERODIASATRA, 'TIPOREVZ' VALUE :OLD.TIPOREVZ, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'CODIGOUSUARIOREVZ' VALUE :OLD.CODIGOUSUARIOREVZ, 'TIPODIARIO' VALUE :OLD.TIPODIARIO, 'INDICADORCONTABLEREVZ' VALUE :OLD.INDICADORCONTABLEREVZ);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('transaccionRecuperacion_type', v_pk, v_event, v_payload, 'FCME_USER.TRANSACCIONRECUPERACION_TYPE', SYSTIMESTAMP);
END;
/
