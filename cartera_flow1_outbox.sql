/* ==========================================================================
   CARTERA - Flujo 1 OUTBOX TRIGGERS (segmento legacy -> cdc_outbox)
   Scope:
     tabla legacy (dbX.dbo.<ltbl>)
       -> trg_outbox_<ltbl>          (anti-loop SESSION_CONTEXT)
       -> fcme_canonicos.cdc_outbox

   NO incluye: wrappers Oracle, module_config, TRG_PROCESS_CDC_INBOX.
   NO ejecuta cambios -- archivo de revision unicamente.
   ========================================================================== */
/* RESUMEN: 91 types -> 60 triggers (uno por tabla legacy) */


/* ---------- BD: dbCG  (3 triggers) ---------- */
USE [dbCG];
GO

/* ----- trg_outbox_cgtbgara_hipo_cdio  ON dbCG.dbo.cgtbgara_hipo_cdio (1 type) -----
      - garantiaCredito_type
*/
IF OBJECT_ID(N'dbCG.dbo.cgtbgara_hipo_cdio', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCG.dbo.cgtbgara_hipo_cdio no existe; trigger trg_outbox_cgtbgara_hipo_cdio no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_cgtbgara_hipo_cdio', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cgtbgara_hipo_cdio;
GO

IF OBJECT_ID(N'dbCG.dbo.cgtbgara_hipo_cdio', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_cgtbgara_hipo_cdio
ON dbo.[cgtbgara_hipo_cdio]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''garantiaCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_gara_hipo]),CONVERT(NVARCHAR(200), i.[ci_cedu])),
            tt.t,
            @op,
            (SELECT x.[nu_bloq],x.[co_usua_conf],x.[sc_gara_hipo],x.[nu_vill],x.[co_ciud],x.[st_regi],x.[co_prov],x.[nu_manz],x.[fe_elim],x.[fe_ingr],x.[co_empr],x.[ci_cedu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCG.dbo.cgtbgara_hipo_cdio'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_gara_hipo]),CONVERT(NVARCHAR(200), d.[ci_cedu])),
            tt.t,
            N''DELETE'',
            (SELECT x.[nu_bloq],x.[co_usua_conf],x.[sc_gara_hipo],x.[nu_vill],x.[co_ciud],x.[st_regi],x.[co_prov],x.[nu_manz],x.[fe_elim],x.[fe_ingr],x.[co_empr],x.[ci_cedu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCG.dbo.cgtbgara_hipo_cdio'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_cgtbprod_cnta_auto  ON dbCG.dbo.cgtbprod_cnta_auto (2 types) -----
      - cuentaAutomaticaDetalle_type
      - cuentaAutomatica_type
*/
IF OBJECT_ID(N'dbCG.dbo.cgtbprod_cnta_auto', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCG.dbo.cgtbprod_cnta_auto no existe; trigger trg_outbox_cgtbprod_cnta_auto no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_cgtbprod_cnta_auto', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cgtbprod_cnta_auto;
GO

IF OBJECT_ID(N'dbCG.dbo.cgtbprod_cnta_auto', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_cgtbprod_cnta_auto
ON dbo.[cgtbprod_cnta_auto]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cuentaAutomaticaDetalle_type''),(N''cuentaAutomatica_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_prod]),CONVERT(NVARCHAR(200), i.[co_fond])),
            tt.t,
            @op,
            (SELECT x.[co_cnta_auto],x.[co_empr],x.[co_fond],x.[co_prod] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCG.dbo.cgtbprod_cnta_auto'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_prod]),CONVERT(NVARCHAR(200), d.[co_fond])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_cnta_auto],x.[co_empr],x.[co_fond],x.[co_prod] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCG.dbo.cgtbprod_cnta_auto'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_cgtbrepo_anls_cnta  ON dbCG.dbo.cgtbrepo_anls_cnta (1 type) -----
      - cuentaPorCobrarType
*/
IF OBJECT_ID(N'dbCG.dbo.cgtbrepo_anls_cnta', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCG.dbo.cgtbrepo_anls_cnta no existe; trigger trg_outbox_cgtbrepo_anls_cnta no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_cgtbrepo_anls_cnta', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cgtbrepo_anls_cnta;
GO

IF OBJECT_ID(N'dbCG.dbo.cgtbrepo_anls_cnta', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_cgtbrepo_anls_cnta
ON dbo.[cgtbrepo_anls_cnta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cuentaPorCobrarType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_cnta_cble]),CONVERT(NVARCHAR(200), i.[co_usua])),
            tt.t,
            @op,
            (SELECT x.[co_cnta_cble],x.[co_usua] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCG.dbo.cgtbrepo_anls_cnta'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_cnta_cble]),CONVERT(NVARCHAR(200), d.[co_usua])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_cnta_cble],x.[co_usua] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCG.dbo.cgtbrepo_anls_cnta'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ---------- BD: dbCR  (47 triggers) ---------- */
USE [dbCR];
GO

/* ----- trg_outbox_crtbabno_extr  ON dbCR.dbo.crtbabno_extr (1 type) -----
      - abonoExtraordinario_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbabno_extr', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbabno_extr no existe; trigger trg_outbox_crtbabno_extr no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbabno_extr', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbabno_extr;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbabno_extr', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbabno_extr
ON dbo.[crtbabno_extr]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''abonoExtraordinario_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_abno])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[co_usua_conf],x.[sc_abno],x.[ti_cred],x.[mo_abno_extr],x.[st_regi],x.[fe_elim],x.[co_proc],x.[fe_autr],x.[ds_refe],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbabno_extr'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_abno])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[co_usua_conf],x.[sc_abno],x.[ti_cred],x.[mo_abno_extr],x.[st_regi],x.[fe_elim],x.[co_proc],x.[fe_autr],x.[ds_refe],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbabno_extr'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcart_calf_prov  ON dbCR.dbo.crtbcart_calf_prov (1 type) -----
      - calificacionCartera_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcart_calf_prov', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcart_calf_prov no existe; trigger trg_outbox_crtbcart_calf_prov no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcart_calf_prov', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcart_calf_prov;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcart_calf_prov', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcart_calf_prov
ON dbo.[crtbcart_calf_prov]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''calificacionCartera_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_calf]),CONVERT(NVARCHAR(200), i.[fe_cort])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[nu_dcto],x.[ti_calf],x.[aa_cred],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcart_calf_prov'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_calf]),CONVERT(NVARCHAR(200), d.[fe_cort])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[nu_dcto],x.[ti_calf],x.[aa_cred],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcart_calf_prov'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcaut_cred  ON dbCR.dbo.crtbcaut_cred (1 type) -----
      - contabilizacionCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcaut_cred', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcaut_cred no existe; trigger trg_outbox_crtbcaut_cred no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcaut_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcaut_cred;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcaut_cred', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcaut_cred
ON dbo.[crtbcaut_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''contabilizacionCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_regi])),
            tt.t,
            @op,
            (SELECT x.[st_regi],x.[fe_elim],x.[ds_asien_cnta],x.[sc_regi],x.[co_usua_elim],x.[co_empr],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcaut_cred'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_regi])),
            tt.t,
            N''DELETE'',
            (SELECT x.[st_regi],x.[fe_elim],x.[ds_asien_cnta],x.[sc_regi],x.[co_usua_elim],x.[co_empr],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcaut_cred'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbccbr_cred_judi  ON dbCR.dbo.crtbccbr_cred_judi (7 types) -----
      - autorizacionCredito_type
      - caucionCredito_type
      - conceptoGastoJudicialType
      - etapaJudicialCredito_type
      - medidaJudicialType
      - precalificacionCredito_type
      - unidadJudicialType
*/
IF OBJECT_ID(N'dbCR.dbo.crtbccbr_cred_judi', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbccbr_cred_judi no existe; trigger trg_outbox_crtbccbr_cred_judi no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbccbr_cred_judi', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbccbr_cred_judi;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbccbr_cred_judi', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbccbr_cred_judi
ON dbo.[crtbccbr_cred_judi]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''autorizacionCredito_type''),(N''caucionCredito_type''),(N''conceptoGastoJudicialType''),(N''etapaJudicialCredito_type''),(N''medidaJudicialType''),(N''precalificacionCredito_type''),(N''unidadJudicialType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_cobr])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_elim],x.[co_usua_ingr],x.[fe_modi],x.[sc_cobr],x.[fe_sald_cred],x.[co_usua_elim],x.[co_empr],x.[aa_cred],x.[co_etap],x.[co_medi],x.[co_rubr],x.[mo_sald_venc],x.[mo_sald_cred],x.[mo_otro],x.[ti_cobr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbccbr_cred_judi'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_cobr])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_elim],x.[co_usua_ingr],x.[fe_modi],x.[sc_cobr],x.[fe_sald_cred],x.[co_usua_elim],x.[co_empr],x.[aa_cred],x.[co_etap],x.[co_medi],x.[co_rubr],x.[mo_sald_venc],x.[mo_sald_cred],x.[mo_otro],x.[ti_cobr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbccbr_cred_judi'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcdeb_cnta  ON dbCR.dbo.crtbcdeb_cnta (3 types) -----
      - cuentaCuotasType
      - cuentaCxPCxCType
      - cuentaType
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcdeb_cnta', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcdeb_cnta no existe; trigger trg_outbox_crtbcdeb_cnta no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcdeb_cnta', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcdeb_cnta;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcdeb_cnta', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcdeb_cnta
ON dbo.[crtbcdeb_cnta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cuentaCuotasType''),(N''cuentaCxPCxCType''),(N''cuentaType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_debi]),CONVERT(NVARCHAR(200), i.[nu_anio])),
            tt.t,
            @op,
            (SELECT x.[fe_proc],x.[fe_conf],x.[nu_anio],x.[fe_elim],x.[co_empr],x.[sc_debi],x.[co_usua_elim],x.[co_usua_veri],x.[co_usua_ingr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcdeb_cnta'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_debi]),CONVERT(NVARCHAR(200), d.[nu_anio])),
            tt.t,
            N''DELETE'',
            (SELECT x.[fe_proc],x.[fe_conf],x.[nu_anio],x.[fe_elim],x.[co_empr],x.[sc_debi],x.[co_usua_elim],x.[co_usua_veri],x.[co_usua_ingr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcdeb_cnta'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcobr_judi_deta  ON dbCR.dbo.crtbcobr_judi_deta (1 type) -----
      - cobranzaJudicialDetalle_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcobr_judi_deta', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcobr_judi_deta no existe; trigger trg_outbox_crtbcobr_judi_deta no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcobr_judi_deta', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcobr_judi_deta;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcobr_judi_deta', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcobr_judi_deta
ON dbo.[crtbcobr_judi_deta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cobranzaJudicialDetalle_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_cobr_judi]),CONVERT(NVARCHAR(200), i.[ti_rubr_pagd])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[po_desc],x.[sc_cobr_judi],x.[mo_mvto],x.[mo_aplic],x.[aa_cred],x.[ti_rubr_pagd] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcobr_judi_deta'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_cobr_judi]),CONVERT(NVARCHAR(200), d.[ti_rubr_pagd])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[po_desc],x.[sc_cobr_judi],x.[mo_mvto],x.[mo_aplic],x.[aa_cred],x.[ti_rubr_pagd] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcobr_judi_deta'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcobr_judi_dist  ON dbCR.dbo.crtbcobr_judi_dist (2 types) -----
      - cobranzaJudicialDistribucion_type
      - cobranzaJudicial_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcobr_judi_dist', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcobr_judi_dist no existe; trigger trg_outbox_crtbcobr_judi_dist no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcobr_judi_dist', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcobr_judi_dist;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcobr_judi_dist', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcobr_judi_dist
ON dbo.[crtbcobr_judi_dist]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cobranzaJudicialDistribucion_type''),(N''cobranzaJudicial_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_cobr_judi])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[sc_cobr_judi],x.[fe_carg],x.[co_rol],x.[ti_abno],x.[st_regi],x.[aa_cred],x.[fe_elim],x.[ds_url],x.[co_empr],x.[ti_cobr],x.[nu_cpbt],x.[fe_modi],x.[fe_depo],x.[fe_liqu_cred],x.[mo_carg],x.[ti_proc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcobr_judi_dist'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_cobr_judi])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[sc_cobr_judi],x.[fe_carg],x.[co_rol],x.[ti_abno],x.[st_regi],x.[aa_cred],x.[fe_elim],x.[ds_url],x.[co_empr],x.[ti_cobr],x.[nu_cpbt],x.[fe_modi],x.[fe_depo],x.[fe_liqu_cred],x.[mo_carg],x.[ti_proc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcobr_judi_dist'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbconv_pago  ON dbCR.dbo.crtbconv_pago (1 type) -----
      - convenioPagoCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbconv_pago', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbconv_pago no existe; trigger trg_outbox_crtbconv_pago no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbconv_pago', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbconv_pago;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbconv_pago', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbconv_pago
ON dbo.[crtbconv_pago]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''convenioPagoCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_conv])),
            tt.t,
            @op,
            (SELECT x.[co_usua_conf],x.[ti_cred],x.[st_apli_gara],x.[fe_autr],x.[mo_cobr_gast],x.[mo_intr_venc],x.[fe_ingr_calc],x.[ce_esta_civi],x.[fe_fall_afil],x.[qs_cred],x.[mo_intr_mora],x.[st_regi],x.[fe_elim],x.[ds_obsr],x.[mo_cuot_inic],x.[mo_capi_venc],x.[fe_perd_conv],x.[co_empr],x.[ds_refe],x.[ce_esta_afil],x.[sc_conv],x.[co_proc],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbconv_pago'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_conv])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_usua_conf],x.[ti_cred],x.[st_apli_gara],x.[fe_autr],x.[mo_cobr_gast],x.[mo_intr_venc],x.[fe_ingr_calc],x.[ce_esta_civi],x.[fe_fall_afil],x.[qs_cred],x.[mo_intr_mora],x.[st_regi],x.[fe_elim],x.[ds_obsr],x.[mo_cuot_inic],x.[mo_capi_venc],x.[fe_perd_conv],x.[co_empr],x.[ds_refe],x.[ce_esta_afil],x.[sc_conv],x.[co_proc],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbconv_pago'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcred_autr_deta  ON dbCR.dbo.crtbcred_autr_deta (1 type) -----
      - autorizacionCreditoDetalle_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcred_autr_deta', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcred_autr_deta no existe; trigger trg_outbox_crtbcred_autr_deta no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_autr_deta', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcred_autr_deta;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcred_autr_deta', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcred_autr_deta
ON dbo.[crtbcred_autr_deta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''autorizacionCreditoDetalle_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_autr_deta])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_autr_deta],x.[st_autr_deta],x.[sc_autr_deta],x.[sc_rngo_usua],x.[fe_modi],x.[fe_ingr],x.[sc_cred_autr],x.[co_empr],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_autr_deta'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_autr_deta])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_autr_deta],x.[st_autr_deta],x.[sc_autr_deta],x.[sc_rngo_usua],x.[fe_modi],x.[fe_ingr],x.[sc_cred_autr],x.[co_empr],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_autr_deta'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcred_liqd_diar  ON dbCR.dbo.crtbcred_liqd_diar (2 types) -----
      - liquidacionDiariaCredito_type
      - movimientoContableCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcred_liqd_diar', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcred_liqd_diar no existe; trigger trg_outbox_crtbcred_liqd_diar no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_liqd_diar', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcred_liqd_diar;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcred_liqd_diar', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcred_liqd_diar
ON dbo.[crtbcred_liqd_diar]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''liquidacionDiariaCredito_type''),(N''movimientoContableCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_liqd])),
            tt.t,
            @op,
            (SELECT x.[sc_liqd],x.[qs_cred],x.[mo_rubr],x.[ti_cred],x.[st_cred],x.[st_liqd_diar],x.[co_empr],x.[aa_cred],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_liqd_diar'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_liqd])),
            tt.t,
            N''DELETE'',
            (SELECT x.[sc_liqd],x.[qs_cred],x.[mo_rubr],x.[ti_cred],x.[st_cred],x.[st_liqd_diar],x.[co_empr],x.[aa_cred],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_liqd_diar'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcred_part  ON dbCR.dbo.crtbcred_part (2 types) -----
      - flujoTrabajoCredito_type
      - personaCreditoType
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcred_part', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcred_part no existe; trigger trg_outbox_crtbcred_part no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_part', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcred_part;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcred_part', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcred_part
ON dbo.[crtbcred_part]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''flujoTrabajoCredito_type''),(N''personaCreditoType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_elim],x.[nu_iden],x.[co_usua_modi],x.[aa_cred],x.[ti_iden] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_part'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_elim],x.[nu_iden],x.[co_usua_modi],x.[aa_cred],x.[ti_iden] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_part'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcred_plzo_venc  ON dbCR.dbo.crtbcred_plzo_venc (1 type) -----
      - plazoVencido_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcred_plzo_venc', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcred_plzo_venc no existe; trigger trg_outbox_crtbcred_plzo_venc no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_plzo_venc', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcred_plzo_venc;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcred_plzo_venc', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcred_plzo_venc
ON dbo.[crtbcred_plzo_venc]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''plazoVencido_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_cred_plzo])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[mo_abno_capi],x.[ti_cred],x.[fe_carg],x.[fe_elim],x.[sc_cred_plzo],x.[st_cred_plzo],x.[fe_modi],x.[mo_abno_intr],x.[co_empr],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_plzo_venc'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_cred_plzo])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[mo_abno_capi],x.[ti_cred],x.[fe_carg],x.[fe_elim],x.[sc_cred_plzo],x.[st_cred_plzo],x.[fe_modi],x.[mo_abno_intr],x.[co_empr],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_plzo_venc'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbcred_prea_whts  ON dbCR.dbo.crtbcred_prea_whts (5 types) -----
      - costoFinancieroCredito_type
      - creditoType
      - desembolsoCredito_type
      - pagosCreditoType
      - refinanciamientoCreditoType
*/
IF OBJECT_ID(N'dbCR.dbo.crtbcred_prea_whts', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbcred_prea_whts no existe; trigger trg_outbox_crtbcred_prea_whts no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbcred_prea_whts', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbcred_prea_whts;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbcred_prea_whts', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbcred_prea_whts
ON dbo.[crtbcred_prea_whts]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''costoFinancieroCredito_type''),(N''creditoType''),(N''desembolsoCredito_type''),(N''pagosCreditoType''),(N''refinanciamientoCreditoType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[sc_prea]),CONVERT(NVARCHAR(200), i.[ci_cedu])),
            tt.t,
            @op,
            (SELECT x.[st_regi],x.[ti_cred_cncd],x.[mo_cred],x.[mo_suel_liqd],x.[sc_prea],x.[ci_cedu],x.[ds_oper],x.[fe_aprb],x.[co_rol],x.[fe_elim],x.[co_usua_ingr],x.[ti_calf],x.[co_usua_elim],x.[co_usua_aprb],x.[co_comb],x.[fe_ingr],x.[nu_plzo],x.[ds_mail] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_prea_whts'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[sc_prea]),CONVERT(NVARCHAR(200), d.[ci_cedu])),
            tt.t,
            N''DELETE'',
            (SELECT x.[st_regi],x.[ti_cred_cncd],x.[mo_cred],x.[mo_suel_liqd],x.[sc_prea],x.[ci_cedu],x.[ds_oper],x.[fe_aprb],x.[co_rol],x.[fe_elim],x.[co_usua_ingr],x.[ti_calf],x.[co_usua_elim],x.[co_usua_aprb],x.[co_comb],x.[fe_ingr],x.[nu_plzo],x.[ds_mail] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbcred_prea_whts'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbctrl_oper_ante_sibs  ON dbCR.dbo.crtbctrl_oper_ante_sibs (1 type) -----
      - reporteSBSOperacionAnterior_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbctrl_oper_ante_sibs', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbctrl_oper_ante_sibs no existe; trigger trg_outbox_crtbctrl_oper_ante_sibs no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbctrl_oper_ante_sibs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbctrl_oper_ante_sibs;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbctrl_oper_ante_sibs', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbctrl_oper_ante_sibs
ON dbo.[crtbctrl_oper_ante_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''reporteSBSOperacionAnterior_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[sc_regi]),CONVERT(NVARCHAR(200), i.[sc_regi_arch]),CONVERT(NVARCHAR(200), i.[ci_cedu]),CONVERT(NVARCHAR(200), i.[nu_oper]),CONVERT(NVARCHAR(200), i.[nu_oper_ante])),
            tt.t,
            @op,
            (SELECT x.[sc_regi],x.[fe_ingr],x.[nu_oper_ante],x.[nu_oper],x.[sc_regi_arch],x.[ci_cedu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbctrl_oper_ante_sibs'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[sc_regi]),CONVERT(NVARCHAR(200), d.[sc_regi_arch]),CONVERT(NVARCHAR(200), d.[ci_cedu]),CONVERT(NVARCHAR(200), d.[nu_oper]),CONVERT(NVARCHAR(200), d.[nu_oper_ante])),
            tt.t,
            N''DELETE'',
            (SELECT x.[sc_regi],x.[fe_ingr],x.[nu_oper_ante],x.[nu_oper],x.[sc_regi_arch],x.[ci_cedu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbctrl_oper_ante_sibs'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbdbso_devo  ON dbCR.dbo.crtbdbso_devo (2 types) -----
      - desembolsoDevolucion_type
      - devolucionCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbdbso_devo', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbdbso_devo no existe; trigger trg_outbox_crtbdbso_devo no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbdbso_devo', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbdbso_devo;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbdbso_devo', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbdbso_devo
ON dbo.[crtbdbso_devo]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''desembolsoDevolucion_type''),(N''devolucionCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_devo]),CONVERT(NVARCHAR(200), i.[aa_devo]),CONVERT(NVARCHAR(200), i.[qs_devo]),CONVERT(NVARCHAR(200), i.[qs_dbso])),
            tt.t,
            @op,
            (SELECT x.[ds_pago],x.[co_usua],x.[qs_dbso],x.[ti_devo],x.[ti_cnta],x.[co_bnco_acre],x.[co_tord],x.[no_bene],x.[nu_cnta],x.[mo_dbso],x.[co_bene],x.[qs_devo],x.[nu_orde],x.[fe_pago],x.[st_dbso],x.[co_bnco],x.[aa_devo] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdbso_devo'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_devo]),CONVERT(NVARCHAR(200), d.[aa_devo]),CONVERT(NVARCHAR(200), d.[qs_devo]),CONVERT(NVARCHAR(200), d.[qs_dbso])),
            tt.t,
            N''DELETE'',
            (SELECT x.[ds_pago],x.[co_usua],x.[qs_dbso],x.[ti_devo],x.[ti_cnta],x.[co_bnco_acre],x.[co_tord],x.[no_bene],x.[nu_cnta],x.[mo_dbso],x.[co_bene],x.[qs_devo],x.[nu_orde],x.[fe_pago],x.[st_dbso],x.[co_bnco],x.[aa_devo] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdbso_devo'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbdeud_conv  ON dbCR.dbo.crtbdeud_conv (1 type) -----
      - referenciaDeudor_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbdeud_conv', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbdeud_conv no existe; trigger trg_outbox_crtbdeud_conv no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbdeud_conv', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbdeud_conv;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbdeud_conv', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbdeud_conv
ON dbo.[crtbdeud_conv]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''referenciaDeudor_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_conv]),CONVERT(NVARCHAR(200), i.[co_tipo_deud])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[fe_modi_deud],x.[fe_elim_deud],x.[ti_cred],x.[st_regi],x.[fe_crea_deud],x.[co_tipo_deud],x.[aa_cred],x.[co_empr],x.[sc_conv] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdeud_conv'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_conv]),CONVERT(NVARCHAR(200), d.[co_tipo_deud])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[fe_modi_deud],x.[fe_elim_deud],x.[ti_cred],x.[st_regi],x.[fe_crea_deud],x.[co_tipo_deud],x.[aa_cred],x.[co_empr],x.[sc_conv] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdeud_conv'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbdevo_masi_deta  ON dbCR.dbo.crtbdevo_masi_deta (2 types) -----
      - devolucionMasivaDetalle_type
      - devolucionMasiva_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbdevo_masi_deta', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbdevo_masi_deta no existe; trigger trg_outbox_crtbdevo_masi_deta no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbdevo_masi_deta', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbdevo_masi_deta;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbdevo_masi_deta', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbdevo_masi_deta
ON dbo.[crtbdevo_masi_deta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''devolucionMasivaDetalle_type''),(N''devolucionMasiva_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_devo_deta]),CONVERT(NVARCHAR(200), i.[sc_devo_masi])),
            tt.t,
            @op,
            (SELECT x.[sc_mvto],x.[co_rubr_rol],x.[sc_sobr],x.[co_liqd_rubr],x.[mo_disp],x.[sc_devo_deta],x.[st_devo_deta],x.[mo_mvto],x.[sc_devo_masi],x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdevo_masi_deta'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_devo_deta]),CONVERT(NVARCHAR(200), d.[sc_devo_masi])),
            tt.t,
            N''DELETE'',
            (SELECT x.[sc_mvto],x.[co_rubr_rol],x.[sc_sobr],x.[co_liqd_rubr],x.[mo_disp],x.[sc_devo_deta],x.[st_devo_deta],x.[mo_mvto],x.[sc_devo_masi],x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdevo_masi_deta'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbdocu_cred  ON dbCR.dbo.crtbdocu_cred (2 types) -----
      - documentoCredito_type
      - grupoCreditoDocumento_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbdocu_cred', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbdocu_cred no existe; trigger trg_outbox_crtbdocu_cred no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbdocu_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbdocu_cred;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbdocu_cred', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbdocu_cred
ON dbo.[crtbdocu_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''documentoCredito_type''),(N''grupoCreditoDocumento_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[co_docu]),
            tt.t,
            @op,
            (SELECT x.[st_docu],x.[ds_docu],x.[co_docu],x.[ti_docu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdocu_cred'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[co_docu]),
            tt.t,
            N''DELETE'',
            (SELECT x.[st_docu],x.[ds_docu],x.[co_docu],x.[ti_docu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdocu_cred'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbdsal_oper  ON dbCR.dbo.crtbdsal_oper (4 types) -----
      - personaCxPCxCType
      - reporteSBSOperacionCancelada_type
      - reporteSBSOperacionConcedida_type
      - reporteSBSSaldoOperacion_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbdsal_oper', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbdsal_oper no existe; trigger trg_outbox_crtbdsal_oper no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbdsal_oper', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbdsal_oper;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbdsal_oper', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbdsal_oper
ON dbo.[crtbdsal_oper]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''personaCxPCxCType''),(N''reporteSBSOperacionCancelada_type''),(N''reporteSBSOperacionConcedida_type''),(N''reporteSBSSaldoOperacion_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[sc_regi]),CONVERT(NVARCHAR(200), i.[ci_cedu]),CONVERT(NVARCHAR(200), i.[ti_iden]),CONVERT(NVARCHAR(200), i.[nu_oper])),
            tt.t,
            @op,
            (SELECT x.[ti_calf],x.[ti_iden],x.[co_tamo],x.[sc_regi],x.[ci_cedu],x.[nu_oper],x.[co_tipo_cred],x.[mo_cuot],x.[fe_docu],x.[pr_inte],x.[mo_prov_requ],x.[mo_capi_cred],x.[pr_inte_mora],x.[mo_cart_cast],x.[mo_suje_prov],x.[mo_venc],x.[mo_prov_cons],x.[mo_cnta_indv],x.[mo_dema_judi],x.[mo_ndev_inte],x.[mo_cost_oper],x.[nu_dias_moro] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdsal_oper'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[sc_regi]),CONVERT(NVARCHAR(200), d.[ci_cedu]),CONVERT(NVARCHAR(200), d.[ti_iden]),CONVERT(NVARCHAR(200), d.[nu_oper])),
            tt.t,
            N''DELETE'',
            (SELECT x.[ti_calf],x.[ti_iden],x.[co_tamo],x.[sc_regi],x.[ci_cedu],x.[nu_oper],x.[co_tipo_cred],x.[mo_cuot],x.[fe_docu],x.[pr_inte],x.[mo_prov_requ],x.[mo_capi_cred],x.[pr_inte_mora],x.[mo_cart_cast],x.[mo_suje_prov],x.[mo_venc],x.[mo_prov_cons],x.[mo_cnta_indv],x.[mo_dema_judi],x.[mo_ndev_inte],x.[mo_cost_oper],x.[nu_dias_moro] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdsal_oper'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbdvgo_cart_deta  ON dbCR.dbo.crtbdvgo_cart_deta (2 types) -----
      - devengamientoCarteraDetalle_type
      - devengamientoCartera_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbdvgo_cart_deta', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbdvgo_cart_deta no existe; trigger trg_outbox_crtbdvgo_cart_deta no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbdvgo_cart_deta', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbdvgo_cart_deta;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbdvgo_cart_deta', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbdvgo_cart_deta
ON dbo.[crtbdvgo_cart_deta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''devengamientoCarteraDetalle_type''),(N''devengamientoCartera_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_dvgo_deta]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[mo_dvgo_xven],x.[mo_sald_capi],x.[nu_dcto],x.[co_dvgo_deta],x.[fe_ultm_cort],x.[mo_ajus],x.[mo_reve],x.[co_empr],x.[aa_cred],x.[fe_cort],x.[mo_dvgo_venc] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdvgo_cart_deta'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_dvgo_deta]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[mo_dvgo_xven],x.[mo_sald_capi],x.[nu_dcto],x.[co_dvgo_deta],x.[fe_ultm_cort],x.[mo_ajus],x.[mo_reve],x.[co_empr],x.[aa_cred],x.[fe_cort],x.[mo_dvgo_venc] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdvgo_cart_deta'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbdvgo_cart_deta_diar  ON dbCR.dbo.crtbdvgo_cart_deta_diar (1 type) -----
      - calificacionCarteraDetalle_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbdvgo_cart_deta_diar', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbdvgo_cart_deta_diar no existe; trigger trg_outbox_crtbdvgo_cart_deta_diar no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbdvgo_cart_deta_diar', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbdvgo_cart_deta_diar;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbdvgo_cart_deta_diar', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbdvgo_cart_deta_diar
ON dbo.[crtbdvgo_cart_deta_diar]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''calificacionCarteraDetalle_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[nu_dcto])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[nu_dcto],x.[mo_reve],x.[mo_dvgo],x.[aa_cred],x.[fe_cort],x.[sc_sald] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdvgo_cart_deta_diar'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[nu_dcto])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[nu_dcto],x.[mo_reve],x.[mo_dvgo],x.[aa_cred],x.[fe_cort],x.[sc_sald] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbdvgo_cart_deta_diar'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbesta_conv_cred  ON dbCR.dbo.crtbesta_conv_cred (1 type) -----
      - estadoConvenioCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbesta_conv_cred', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbesta_conv_cred no existe; trigger trg_outbox_crtbesta_conv_cred no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbesta_conv_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbesta_conv_cred;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbesta_conv_cred', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbesta_conv_cred
ON dbo.[crtbesta_conv_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''estadoConvenioCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[st_regi_conv]),
            tt.t,
            @op,
            (SELECT x.[st_regi],x.[ds_esta],x.[st_regi_conv] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbesta_conv_cred'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[st_regi_conv]),
            tt.t,
            N''DELETE'',
            (SELECT x.[st_regi],x.[ds_esta],x.[st_regi_conv] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbesta_conv_cred'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbgest_cart_asig  ON dbCR.dbo.crtbgest_cart_asig (1 type) -----
      - gestionCobranzaAsignacion_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbgest_cart_asig', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbgest_cart_asig no existe; trigger trg_outbox_crtbgest_cart_asig no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbgest_cart_asig', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbgest_cart_asig;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbgest_cart_asig', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbgest_cart_asig
ON dbo.[crtbgest_cart_asig]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''gestionCobranzaAsignacion_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_gest_cart_asig]),CONVERT(NVARCHAR(200), i.[sc_gene_cart_asig])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[co_usua_ante],x.[ti_calf_homo],x.[co_gest_cart_asig],x.[co_usua_gest],x.[aa_cred],x.[fe_cort],x.[sc_gene_cart_asig] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbgest_cart_asig'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_gest_cart_asig]),CONVERT(NVARCHAR(200), d.[sc_gene_cart_asig])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[co_usua_ante],x.[ti_calf_homo],x.[co_gest_cart_asig],x.[co_usua_gest],x.[aa_cred],x.[fe_cort],x.[sc_gene_cart_asig] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbgest_cart_asig'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbgest_cred  ON dbCR.dbo.crtbgest_cred (1 type) -----
      - gestionComunicacionCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbgest_cred', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbgest_cred no existe; trigger trg_outbox_crtbgest_cred no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbgest_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbgest_cred;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbgest_cred', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbgest_cred
ON dbo.[crtbgest_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''gestionComunicacionCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ci_cedu_ejec])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_pago],x.[sc_pago],x.[st_gest],x.[aa_cred],x.[ci_cedu_ejec] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbgest_cred'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ci_cedu_ejec])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_pago],x.[sc_pago],x.[st_gest],x.[aa_cred],x.[ci_cedu_ejec] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbgest_cred'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbinfo_gara_real_sibs  ON dbCR.dbo.crtbinfo_gara_real_sibs (1 type) -----
      - reporteSBSGarantiaReal_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbinfo_gara_real_sibs', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbinfo_gara_real_sibs no existe; trigger trg_outbox_crtbinfo_gara_real_sibs no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbinfo_gara_real_sibs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbinfo_gara_real_sibs;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbinfo_gara_real_sibs', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbinfo_gara_real_sibs
ON dbo.[crtbinfo_gara_real_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''reporteSBSGarantiaReal_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[fe_cort])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[st_cred],x.[nu_gara_oper],x.[ti_gara],x.[nu_regi],x.[ds_gara],x.[ti_cred],x.[aa_cred],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbinfo_gara_real_sibs'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[fe_cort])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[st_cred],x.[nu_gara_oper],x.[ti_gara],x.[nu_regi],x.[ds_gara],x.[ti_cred],x.[aa_cred],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbinfo_gara_real_sibs'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbinfo_legl  ON dbCR.dbo.crtbinfo_legl (1 type) -----
      - informacionLegal_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbinfo_legl', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbinfo_legl no existe; trigger trg_outbox_crtbinfo_legl no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbinfo_legl', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbinfo_legl;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbinfo_legl', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbinfo_legl
ON dbo.[crtbinfo_legl]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''informacionLegal_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[co_usua_recp],x.[st_regi],x.[fe_elim],x.[fe_modi],x.[ds_refe],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbinfo_legl'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[co_usua_recp],x.[st_regi],x.[fe_elim],x.[fe_modi],x.[ds_refe],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbinfo_legl'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbobli_rol  ON dbCR.dbo.crtbobli_rol (1 type) -----
      - obligacionRol_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbobli_rol', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbobli_rol no existe; trigger trg_outbox_crtbobli_rol no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbobli_rol', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbobli_rol;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbobli_rol', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbobli_rol
ON dbo.[crtbobli_rol]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''obligacionRol_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[ti_desc],x.[co_rol],x.[st_regi],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbobli_rol'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[ti_desc],x.[co_rol],x.[st_regi],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbobli_rol'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtboper_canc  ON dbCR.dbo.crtboper_canc (1 type) -----
      - cancelacionCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtboper_canc', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtboper_canc no existe; trigger trg_outbox_crtboper_canc no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtboper_canc', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtboper_canc;
GO

IF OBJECT_ID(N'dbCR.dbo.crtboper_canc', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtboper_canc
ON dbo.[crtboper_canc]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cancelacionCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_cred_ante]),CONVERT(NVARCHAR(200), i.[aa_cred_ante]),CONVERT(NVARCHAR(200), i.[qs_cred_ante])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[ti_cred_ante],x.[mo_intr],x.[mo_gast_judi],x.[aa_cred],x.[aa_cred_ante],x.[qs_cred_ante] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtboper_canc'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_cred_ante]),CONVERT(NVARCHAR(200), d.[aa_cred_ante]),CONVERT(NVARCHAR(200), d.[qs_cred_ante])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[ti_cred_ante],x.[mo_intr],x.[mo_gast_judi],x.[aa_cred],x.[aa_cred_ante],x.[qs_cred_ante] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtboper_canc'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtboper_dref_liqd  ON dbCR.dbo.crtboper_dref_liqd (1 type) -----
      - operacionConyugal_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtboper_dref_liqd', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtboper_dref_liqd no existe; trigger trg_outbox_crtboper_dref_liqd no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtboper_dref_liqd', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtboper_dref_liqd;
GO

IF OBJECT_ID(N'dbCR.dbo.crtboper_dref_liqd', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtboper_dref_liqd
ON dbo.[crtboper_dref_liqd]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''operacionConyugal_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[qs_refe])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[va_liqd],x.[co_tipo_deud],x.[aa_cred],x.[qs_refe] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtboper_dref_liqd'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[qs_refe])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[va_liqd],x.[co_tipo_deud],x.[aa_cred],x.[qs_refe] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtboper_dref_liqd'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbplan_ajus  ON dbCR.dbo.crtbplan_ajus (1 type) -----
      - planPagoAjuste_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbplan_ajus', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbplan_ajus no existe; trigger trg_outbox_crtbplan_ajus no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbplan_ajus', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbplan_ajus;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbplan_ajus', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbplan_ajus
ON dbo.[crtbplan_ajus]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''planPagoAjuste_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_abno]),CONVERT(NVARCHAR(200), i.[nu_dcto])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[sc_abno],x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[pl_dias],x.[mo_capi],x.[mo_incd],x.[mo_comi],x.[mo_intr],x.[fe_vcto],x.[mo_inte_pmes],x.[aa_cred],x.[mo_segu],x.[nu_dcto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbplan_ajus'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_abno]),CONVERT(NVARCHAR(200), d.[nu_dcto])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[sc_abno],x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[pl_dias],x.[mo_capi],x.[mo_incd],x.[mo_comi],x.[mo_intr],x.[fe_vcto],x.[mo_inte_pmes],x.[aa_cred],x.[mo_segu],x.[nu_dcto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbplan_ajus'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbplpg_conv  ON dbCR.dbo.crtbplpg_conv (1 type) -----
      - cuotaConvenio_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbplpg_conv', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbplpg_conv no existe; trigger trg_outbox_crtbplpg_conv no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbplpg_conv', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbplpg_conv;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbplpg_conv', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbplpg_conv
ON dbo.[crtbplpg_conv]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cuotaConvenio_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[nu_dcto]),CONVERT(NVARCHAR(200), i.[sc_dcto]),CONVERT(NVARCHAR(200), i.[sc_conv])),
            tt.t,
            @op,
            (SELECT x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[mo_abno_mora],x.[fe_vcto],x.[mo_segu],x.[mo_incd],x.[mo_dvgo_diar],x.[sc_dcto],x.[sc_rol],x.[fe_intr],x.[qs_cred],x.[mo_dvgd_intr],x.[st_cred],x.[fe_elim],x.[st_vcto],x.[mo_intr],x.[in_reve_dvgo],x.[mo_dvgd_mora],x.[mo_dvgo_acum],x.[fe_ultm_envi],x.[nu_anos],x.[mo_abno_capi],x.[nu_dias],x.[mo_capi],x.[sc_conv],x.[mo_comi],x.[mo_abno_intr],x.[mo_gast_judi],x.[fe_inic_venc],x.[mo_inte_pmes],x.[aa_cred],x.[nu_dcto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbplpg_conv'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[nu_dcto]),CONVERT(NVARCHAR(200), d.[sc_dcto]),CONVERT(NVARCHAR(200), d.[sc_conv])),
            tt.t,
            N''DELETE'',
            (SELECT x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[mo_abno_mora],x.[fe_vcto],x.[mo_segu],x.[mo_incd],x.[mo_dvgo_diar],x.[sc_dcto],x.[sc_rol],x.[fe_intr],x.[qs_cred],x.[mo_dvgd_intr],x.[st_cred],x.[fe_elim],x.[st_vcto],x.[mo_intr],x.[in_reve_dvgo],x.[mo_dvgd_mora],x.[mo_dvgo_acum],x.[fe_ultm_envi],x.[nu_anos],x.[mo_abno_capi],x.[nu_dias],x.[mo_capi],x.[sc_conv],x.[mo_comi],x.[mo_abno_intr],x.[mo_gast_judi],x.[fe_inic_venc],x.[mo_inte_pmes],x.[aa_cred],x.[nu_dcto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbplpg_conv'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbrecu_conv  ON dbCR.dbo.crtbrecu_conv (4 types) -----
      - detalleRecuperacion_type
      - recuperacionConvenio_type
      - recuperacionCredito_type
      - transaccionRecuperacion_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbrecu_conv', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbrecu_conv no existe; trigger trg_outbox_crtbrecu_conv no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbrecu_conv', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbrecu_conv;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbrecu_conv', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbrecu_conv
ON dbo.[crtbrecu_conv]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''detalleRecuperacion_type''),(N''recuperacionConvenio_type''),(N''recuperacionCredito_type''),(N''transaccionRecuperacion_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[qs_abno])),
            tt.t,
            @op,
            (SELECT x.[qs_abno],x.[qs_cred],x.[ti_cred],x.[st_mvto],x.[mo_mvto],x.[aa_cred],x.[ds_liqd],x.[co_usua_conf],x.[fe_mvto],x.[st_autr],x.[co_usua_revz],x.[ti_recp],x.[fe_revz],x.[co_usua_liqd],x.[st_regi],x.[ti_revz],x.[nu_cpbt_cble],x.[fe_abno],x.[nu_dias_atra],x.[co_rol],x.[fe_cble],x.[ti_diar],x.[in_cble_revz],x.[in_conf_fond] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbrecu_conv'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[qs_abno])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_abno],x.[qs_cred],x.[ti_cred],x.[st_mvto],x.[mo_mvto],x.[aa_cred],x.[ds_liqd],x.[co_usua_conf],x.[fe_mvto],x.[st_autr],x.[co_usua_revz],x.[ti_recp],x.[fe_revz],x.[co_usua_liqd],x.[st_regi],x.[ti_revz],x.[nu_cpbt_cble],x.[fe_abno],x.[nu_dias_atra],x.[co_rol],x.[fe_cble],x.[ti_diar],x.[in_cble_revz],x.[in_conf_fond] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbrecu_conv'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbrepo_sobr  ON dbCR.dbo.crtbrepo_sobr (1 type) -----
      - reporteSBSSujetoRiesgo_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbrepo_sobr', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbrepo_sobr no existe; trigger trg_outbox_crtbrepo_sobr no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbrepo_sobr', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbrepo_sobr;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbrepo_sobr', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbrepo_sobr
ON dbo.[crtbrepo_sobr]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''reporteSBSSujetoRiesgo_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[qs_Cred]),CONVERT(NVARCHAR(200), i.[co_rol]),CONVERT(NVARCHAR(200), i.[co_prov])),
            tt.t,
            @op,
            (SELECT x.[qs_Cred],x.[co_rol],x.[co_prov],x.[nu_ctas],x.[ti_inst],x.[co_usua] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbrepo_sobr'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[qs_Cred]),CONVERT(NVARCHAR(200), d.[co_rol]),CONVERT(NVARCHAR(200), d.[co_prov])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_Cred],x.[co_rol],x.[co_prov],x.[nu_ctas],x.[ti_inst],x.[co_usua] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbrepo_sobr'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbrngo_autr_cred  ON dbCR.dbo.crtbrngo_autr_cred (1 type) -----
      - cuotaCreditoType
*/
IF OBJECT_ID(N'dbCR.dbo.crtbrngo_autr_cred', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbrngo_autr_cred no existe; trigger trg_outbox_crtbrngo_autr_cred no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbrngo_autr_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbrngo_autr_cred;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbrngo_autr_cred', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbrngo_autr_cred
ON dbo.[crtbrngo_autr_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cuotaCreditoType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_rngo])),
            tt.t,
            @op,
            (SELECT x.[co_usua],x.[co_grup],x.[fe_elim],x.[fe_ingr],x.[co_empr],x.[sc_rngo] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbrngo_autr_cred'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_rngo])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_usua],x.[co_grup],x.[fe_elim],x.[fe_ingr],x.[co_empr],x.[sc_rngo] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbrngo_autr_cred'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbrngo_intr_cred  ON dbCR.dbo.crtbrngo_intr_cred (1 type) -----
      - tasaInteresCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbrngo_intr_cred', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbrngo_intr_cred no existe; trigger trg_outbox_crtbrngo_intr_cred no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbrngo_intr_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbrngo_intr_cred;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbrngo_intr_cred', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbrngo_intr_cred
ON dbo.[crtbrngo_intr_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''tasaInteresCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_rngo]),CONVERT(NVARCHAR(200), i.[co_usua]),CONVERT(NVARCHAR(200), i.[co_carg])),
            tt.t,
            @op,
            (SELECT x.[co_empr],x.[sc_rngo],x.[co_usua],x.[co_carg] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbrngo_intr_cred'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_rngo]),CONVERT(NVARCHAR(200), d.[co_usua]),CONVERT(NVARCHAR(200), d.[co_carg])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_empr],x.[sc_rngo],x.[co_usua],x.[co_carg] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbrngo_intr_cred'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbsald_cart  ON dbCR.dbo.crtbsald_cart (1 type) -----
      - saldoCartera_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbsald_cart', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbsald_cart no existe; trigger trg_outbox_crtbsald_cart no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbsald_cart', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbsald_cart;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbsald_cart', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbsald_cart
ON dbo.[crtbsald_cart]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''saldoCartera_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[fe_sald]),CONVERT(NVARCHAR(200), i.[co_fond]),CONVERT(NVARCHAR(200), i.[ti_cred])),
            tt.t,
            @op,
            (SELECT x.[mo_sald_capi_xven],x.[mo_abno_capi],x.[ti_cred],x.[mo_abno_capi_xven],x.[mo_abno_capi_vcdo],x.[mo_abno_inte],x.[mo_capi],x.[co_fond],x.[nu_oper],x.[fe_sald],x.[mo_abno_mora],x.[mo_inte],x.[co_empr],x.[mo_sald_capi_vcdo],x.[mo_inte_reve_vcdo] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsald_cart'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[fe_sald]),CONVERT(NVARCHAR(200), d.[co_fond]),CONVERT(NVARCHAR(200), d.[ti_cred])),
            tt.t,
            N''DELETE'',
            (SELECT x.[mo_sald_capi_xven],x.[mo_abno_capi],x.[ti_cred],x.[mo_abno_capi_xven],x.[mo_abno_capi_vcdo],x.[mo_abno_inte],x.[mo_capi],x.[co_fond],x.[nu_oper],x.[fe_sald],x.[mo_abno_mora],x.[mo_inte],x.[co_empr],x.[mo_sald_capi_vcdo],x.[mo_inte_reve_vcdo] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsald_cart'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbsald_cart_deta  ON dbCR.dbo.crtbsald_cart_deta (1 type) -----
      - saldoCarteraDetalle_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbsald_cart_deta', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbsald_cart_deta no existe; trigger trg_outbox_crtbsald_cart_deta no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbsald_cart_deta', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbsald_cart_deta;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbsald_cart_deta', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbsald_cart_deta
ON dbo.[crtbsald_cart_deta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''saldoCarteraDetalle_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[fe_cort])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[mo_sald_capi_xven],x.[ti_cred],x.[mo_inte_abno],x.[nu_dcto],x.[mo_sald_capi_vcdo],x.[mo_inte_dvgo],x.[co_empr],x.[aa_cred],x.[fe_cort] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsald_cart_deta'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[fe_cort])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[mo_sald_capi_xven],x.[ti_cred],x.[mo_inte_abno],x.[nu_dcto],x.[mo_sald_capi_vcdo],x.[mo_inte_dvgo],x.[co_empr],x.[aa_cred],x.[fe_cort] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsald_cart_deta'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbsegi_autr_ofic  ON dbCR.dbo.crtbsegi_autr_ofic (1 type) -----
      - seguimientoAutorizacion_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbsegi_autr_ofic', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbsegi_autr_ofic no existe; trigger trg_outbox_crtbsegi_autr_ofic no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbsegi_autr_ofic', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbsegi_autr_ofic;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbsegi_autr_ofic', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbsegi_autr_ofic
ON dbo.[crtbsegi_autr_ofic]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''seguimientoAutorizacion_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[sc_segi])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_actu],x.[co_prov],x.[co_usua_tran],x.[st_segi],x.[ds_obsr],x.[sc_segi],x.[co_empr],x.[aa_cred],x.[co_moti] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsegi_autr_ofic'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[sc_segi])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[fe_actu],x.[co_prov],x.[co_usua_tran],x.[st_segi],x.[ds_obsr],x.[sc_segi],x.[co_empr],x.[aa_cred],x.[co_moti] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsegi_autr_ofic'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbsegu_cred  ON dbCR.dbo.crtbsegu_cred (1 type) -----
      - seguroCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbsegu_cred', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbsegu_cred no existe; trigger trg_outbox_crtbsegu_cred no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbsegu_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbsegu_cred;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbsegu_cred', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbsegu_cred
ON dbo.[crtbsegu_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''seguroCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_segu]),CONVERT(NVARCHAR(200), i.[co_moti])),
            tt.t,
            @op,
            (SELECT x.[sc_segu],x.[co_moti],x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsegu_cred'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_segu]),CONVERT(NVARCHAR(200), d.[co_moti])),
            tt.t,
            N''DELETE'',
            (SELECT x.[sc_segu],x.[co_moti],x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsegu_cred'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbsobr_caut  ON dbCR.dbo.crtbsobr_caut (1 type) -----
      - sobranteCaucion_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbsobr_caut', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbsobr_caut no existe; trigger trg_outbox_crtbsobr_caut no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbsobr_caut', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbsobr_caut;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbsobr_caut', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbsobr_caut
ON dbo.[crtbsobr_caut]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''sobranteCaucion_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_sobr]),CONVERT(NVARCHAR(200), i.[co_fond])),
            tt.t,
            @op,
            (SELECT x.[co_empr],x.[ti_sobr],x.[co_fond],x.[va_cnta_auto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsobr_caut'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_sobr]),CONVERT(NVARCHAR(200), d.[co_fond])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_empr],x.[ti_sobr],x.[co_fond],x.[va_cnta_auto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbsobr_caut'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtbtipo_cred_sibs  ON dbCR.dbo.crtbtipo_cred_sibs (1 type) -----
      - tipoCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtbtipo_cred_sibs', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtbtipo_cred_sibs no existe; trigger trg_outbox_crtbtipo_cred_sibs no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtbtipo_cred_sibs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtbtipo_cred_sibs;
GO

IF OBJECT_ID(N'dbCR.dbo.crtbtipo_cred_sibs', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtbtipo_cred_sibs
ON dbo.[crtbtipo_cred_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''tipoCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_tcre]),
            tt.t,
            @op,
            (SELECT x.[co_grup_fcme],x.[ds_tcre],x.[st_tcre],x.[co_tcre],x.[sc_tcre] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbtipo_cred_sibs'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_tcre]),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_grup_fcme],x.[ds_tcre],x.[st_tcre],x.[co_tcre],x.[sc_tcre] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtbtipo_cred_sibs'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtpagos  ON dbCR.dbo.crtpagos (1 type) -----
      - pagoCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtpagos', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtpagos no existe; trigger trg_outbox_crtpagos no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtpagos', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtpagos;
GO

IF OBJECT_ID(N'dbCR.dbo.crtpagos', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtpagos
ON dbo.[crtpagos]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''pagoCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[qs_abno]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[sc_reca])),
            tt.t,
            @op,
            (SELECT x.[qs_abno],x.[ce_regi],x.[qs_cred],x.[ti_cred],x.[sc_reca],x.[fx_pago],x.[aa_cred],x.[va_pagr],x.[co_prov],x.[co_fond],x.[ti_pago],x.[va_abno],x.[ci_rol],x.[fx_proc],x.[sc_rol],x.[ti_inst],x.[co_paga] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtpagos'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[qs_abno]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[sc_reca])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_abno],x.[ce_regi],x.[qs_cred],x.[ti_cred],x.[sc_reca],x.[fx_pago],x.[aa_cred],x.[va_pagr],x.[co_prov],x.[co_fond],x.[ti_pago],x.[va_abno],x.[ci_rol],x.[fx_proc],x.[sc_rol],x.[ti_inst],x.[co_paga] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtpagos'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtplpag  ON dbCR.dbo.crtplpag (1 type) -----
      - planPago_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtplpag', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtplpag no existe; trigger trg_outbox_crtplpag no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtplpag', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtplpag;
GO

IF OBJECT_ID(N'dbCR.dbo.crtplpag', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtplpag
ON dbo.[crtplpag]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''planPago_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[nu_dcto]),CONVERT(NVARCHAR(200), i.[sc_dcto])),
            tt.t,
            @op,
            (SELECT x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[mo_abno_mora],x.[mo_segu],x.[mo_dvgo_diar],x.[mo_incd],x.[sc_dcto],x.[sc_rol],x.[mo_rast],x.[qs_cred],x.[mo_dvgd_intr],x.[fe_elim],x.[st_vcto],x.[st_pago_fcme],x.[mo_intr],x.[in_reve_dvgo],x.[mo_dvgd_mora],x.[mo_dvgo_acum],x.[fe_ultm_envi],x.[mo_abno_capi],x.[fx_pago_fcme],x.[mo_capi],x.[mo_comi],x.[mo_abno_intr],x.[mo_gast_judi],x.[fe_inic_venc],x.[mo_inte_pmes],x.[aa_cred],x.[nu_dcto] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtplpag'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[nu_dcto]),CONVERT(NVARCHAR(200), d.[sc_dcto])),
            tt.t,
            N''DELETE'',
            (SELECT x.[ti_cred],x.[mo_dvdo],x.[mo_cuot],x.[mo_abno_mora],x.[mo_segu],x.[mo_dvgo_diar],x.[mo_incd],x.[sc_dcto],x.[sc_rol],x.[mo_rast],x.[qs_cred],x.[mo_dvgd_intr],x.[fe_elim],x.[st_vcto],x.[st_pago_fcme],x.[mo_intr],x.[in_reve_dvgo],x.[mo_dvgd_mora],x.[mo_dvgo_acum],x.[fe_ultm_envi],x.[mo_abno_capi],x.[fx_pago_fcme],x.[mo_capi],x.[mo_comi],x.[mo_abno_intr],x.[mo_gast_judi],x.[fe_inic_venc],x.[mo_inte_pmes],x.[aa_cred],x.[nu_dcto] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtplpag'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtrepo_sobr  ON dbCR.dbo.crtrepo_sobr (3 types) -----
      - reporteSBSCabecera_type
      - reporteSBSDetalle_type
      - tipoSobrante_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtrepo_sobr', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtrepo_sobr no existe; trigger trg_outbox_crtrepo_sobr no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtrepo_sobr', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtrepo_sobr;
GO

IF OBJECT_ID(N'dbCR.dbo.crtrepo_sobr', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtrepo_sobr
ON dbo.[crtrepo_sobr]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''reporteSBSCabecera_type''),(N''reporteSBSDetalle_type''),(N''tipoSobrante_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_sobr]),CONVERT(NVARCHAR(200), i.[co_usua])),
            tt.t,
            @op,
            (SELECT x.[co_afil],x.[co_usua],x.[fe_devo],x.[fe_cort],x.[co_rol],x.[co_inst],x.[ti_sobr],x.[ds_obse] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtrepo_sobr'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_sobr]),CONVERT(NVARCHAR(200), d.[co_usua])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_afil],x.[co_usua],x.[fe_devo],x.[fe_cort],x.[co_rol],x.[co_inst],x.[ti_sobr],x.[ds_obse] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtrepo_sobr'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtrubros_cobr  ON dbCR.dbo.crtrubros_cobr (2 types) -----
      - rubroCobranza_type
      - rubrosCobranzaDetalle_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtrubros_cobr', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtrubros_cobr no existe; trigger trg_outbox_crtrubros_cobr no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtrubros_cobr', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtrubros_cobr;
GO

IF OBJECT_ID(N'dbCR.dbo.crtrubros_cobr', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtrubros_cobr
ON dbo.[crtrubros_cobr]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''rubroCobranza_type''),(N''rubrosCobranzaDetalle_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_rubr_pago]),CONVERT(NVARCHAR(200), i.[ti_pago])),
            tt.t,
            @op,
            (SELECT x.[ti_cred],x.[co_empr],x.[ti_rubr_pago],x.[ti_pago] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtrubros_cobr'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_rubr_pago]),CONVERT(NVARCHAR(200), d.[ti_pago])),
            tt.t,
            N''DELETE'',
            (SELECT x.[ti_cred],x.[co_empr],x.[ti_rubr_pago],x.[ti_pago] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtrubros_cobr'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtsobrante  ON dbCR.dbo.crtsobrante (2 types) -----
      - sobranteCredito_type
      - sobranteDistribucion_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtsobrante', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtsobrante no existe; trigger trg_outbox_crtsobrante no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtsobrante', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtsobrante;
GO

IF OBJECT_ID(N'dbCR.dbo.crtsobrante', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtsobrante
ON dbo.[crtsobrante]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''sobranteCredito_type''),(N''sobranteDistribucion_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[sc_sobr]),
            tt.t,
            @op,
            (SELECT x.[ti_apli],x.[co_fond],x.[ti_pago],x.[st_devo],x.[fe_devo],x.[fe_proc],x.[ds_oper_refe],x.[sc_rol],x.[co_paga],x.[ti_sobr],x.[mo_disp],x.[nu_cpbt_cble],x.[ci_rol],x.[co_empr],x.[mo_sobr],x.[ti_inst],x.[sc_reca],x.[sc_sobr],x.[co_prov],x.[fe_cort],x.[ds_obse] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtsobrante'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[sc_sobr]),
            tt.t,
            N''DELETE'',
            (SELECT x.[ti_apli],x.[co_fond],x.[ti_pago],x.[st_devo],x.[fe_devo],x.[fe_proc],x.[ds_oper_refe],x.[sc_rol],x.[co_paga],x.[ti_sobr],x.[mo_disp],x.[nu_cpbt_cble],x.[ci_rol],x.[co_empr],x.[mo_sobr],x.[ti_inst],x.[sc_reca],x.[sc_sobr],x.[co_prov],x.[fe_cort],x.[ds_obse] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtsobrante'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_crtsolid  ON dbCR.dbo.crtsolid (1 type) -----
      - solidarioCredito_type
*/
IF OBJECT_ID(N'dbCR.dbo.crtsolid', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCR.dbo.crtsolid no existe; trigger trg_outbox_crtsolid no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_crtsolid', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_crtsolid;
GO

IF OBJECT_ID(N'dbCR.dbo.crtsolid', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_crtsolid
ON dbo.[crtsolid]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''solidarioCredito_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[qs_cred]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[ti_cred],x.[mo_cuot],x.[in_soli],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtsolid'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[qs_cred]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[ti_cred],x.[mo_cuot],x.[in_soli],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCR.dbo.crtsolid'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ---------- BD: dbCT  (4 triggers) ---------- */
USE [dbCT];
GO

/* ----- trg_outbox_cttbesta_docu_inve  ON dbCT.dbo.cttbesta_docu_inve (1 type) -----
      - estadoLegalType
*/
IF OBJECT_ID(N'dbCT.dbo.cttbesta_docu_inve', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCT.dbo.cttbesta_docu_inve no existe; trigger trg_outbox_cttbesta_docu_inve no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_cttbesta_docu_inve', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cttbesta_docu_inve;
GO

IF OBJECT_ID(N'dbCT.dbo.cttbesta_docu_inve', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_cttbesta_docu_inve
ON dbo.[cttbesta_docu_inve]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''estadoLegalType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[nu_oper]),
            tt.t,
            @op,
            (SELECT x.[ds_esta_docu_inve],x.[in_revi_docu],x.[mo_reca_inve],x.[nu_oper] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCT.dbo.cttbesta_docu_inve'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[nu_oper]),
            tt.t,
            N''DELETE'',
            (SELECT x.[ds_esta_docu_inve],x.[in_revi_docu],x.[mo_reca_inve],x.[nu_oper] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCT.dbo.cttbesta_docu_inve'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_cttbproc_obse_tran  ON dbCT.dbo.cttbproc_obse_tran (1 type) -----
      - procesoAccionType
*/
IF OBJECT_ID(N'dbCT.dbo.cttbproc_obse_tran', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCT.dbo.cttbproc_obse_tran no existe; trigger trg_outbox_cttbproc_obse_tran no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_cttbproc_obse_tran', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cttbproc_obse_tran;
GO

IF OBJECT_ID(N'dbCT.dbo.cttbproc_obse_tran', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_cttbproc_obse_tran
ON dbo.[cttbproc_obse_tran]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''procesoAccionType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_proc_obse]),CONVERT(NVARCHAR(200), i.[co_apli]),CONVERT(NVARCHAR(200), i.[co_func]),CONVERT(NVARCHAR(200), i.[nu_tran]),CONVERT(NVARCHAR(200), i.[ti_loca])),
            tt.t,
            @op,
            (SELECT x.[nu_tran],x.[co_proc_obse],x.[co_apli],x.[co_func],x.[ti_loca] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCT.dbo.cttbproc_obse_tran'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_proc_obse]),CONVERT(NVARCHAR(200), d.[co_apli]),CONVERT(NVARCHAR(200), d.[co_func]),CONVERT(NVARCHAR(200), d.[nu_tran]),CONVERT(NVARCHAR(200), d.[ti_loca])),
            tt.t,
            N''DELETE'',
            (SELECT x.[nu_tran],x.[co_proc_obse],x.[co_apli],x.[co_func],x.[ti_loca] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCT.dbo.cttbproc_obse_tran'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_cttbrepo_gene  ON dbCT.dbo.cttbrepo_gene (1 type) -----
      - reporteSBSGaranteCodeudor_type
*/
IF OBJECT_ID(N'dbCT.dbo.cttbrepo_gene', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCT.dbo.cttbrepo_gene no existe; trigger trg_outbox_cttbrepo_gene no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_cttbrepo_gene', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cttbrepo_gene;
GO

IF OBJECT_ID(N'dbCT.dbo.cttbrepo_gene', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_cttbrepo_gene
ON dbo.[cttbrepo_gene]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''reporteSBSGaranteCodeudor_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])),
            tt.t,
            @op,
            (SELECT x.[qs_cred],x.[nu_oper_canc],x.[nu_rol_indi],x.[nu_plaz],x.[co_inst_gara],x.[fe_naci],x.[ti_calf],x.[ti_cred],x.[aa_cred] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCT.dbo.cttbrepo_gene'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])),
            tt.t,
            N''DELETE'',
            (SELECT x.[qs_cred],x.[nu_oper_canc],x.[nu_rol_indi],x.[nu_plaz],x.[co_inst_gara],x.[fe_naci],x.[ti_calf],x.[ti_cred],x.[aa_cred] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCT.dbo.cttbrepo_gene'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_cttbtran_inve_auxi  ON dbCT.dbo.cttbtran_inve_auxi (1 type) -----
      - auxDatosCobrosAdicionalesType
*/
IF OBJECT_ID(N'dbCT.dbo.cttbtran_inve_auxi', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbCT.dbo.cttbtran_inve_auxi no existe; trigger trg_outbox_cttbtran_inve_auxi no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_cttbtran_inve_auxi', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_cttbtran_inve_auxi;
GO

IF OBJECT_ID(N'dbCT.dbo.cttbtran_inve_auxi', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_cttbtran_inve_auxi
ON dbo.[cttbtran_inve_auxi]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''auxDatosCobrosAdicionalesType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ci_cedu]),CONVERT(NVARCHAR(200), i.[sc_auxi])),
            tt.t,
            @op,
            (SELECT x.[co_usua],x.[mo_inve_fide],x.[ci_cedu],x.[sc_auxi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCT.dbo.cttbtran_inve_auxi'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ci_cedu]),CONVERT(NVARCHAR(200), d.[sc_auxi])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_usua],x.[mo_inve_fide],x.[ci_cedu],x.[sc_auxi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbCT.dbo.cttbtran_inve_auxi'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ---------- BD: dbFC  (6 triggers) ---------- */
USE [dbFC];
GO

/* ----- trg_outbox_fctbdeta_liqd_cred  ON dbFC.dbo.fctbdeta_liqd_cred (1 type) -----
      - grupoCreditoDetalle_type
*/
IF OBJECT_ID(N'dbFC.dbo.fctbdeta_liqd_cred', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbFC.dbo.fctbdeta_liqd_cred no existe; trigger trg_outbox_fctbdeta_liqd_cred no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_fctbdeta_liqd_cred', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbdeta_liqd_cred;
GO

IF OBJECT_ID(N'dbFC.dbo.fctbdeta_liqd_cred', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_fctbdeta_liqd_cred
ON dbo.[fctbdeta_liqd_cred]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''grupoCreditoDetalle_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_liqd]),CONVERT(NVARCHAR(200), i.[sc_deta]),CONVERT(NVARCHAR(200), i.[ti_cred]),CONVERT(NVARCHAR(200), i.[aa_cred]),CONVERT(NVARCHAR(200), i.[qs_cred])),
            tt.t,
            @op,
            (SELECT x.[sc_liqd],x.[co_rubr],x.[sc_deta],x.[ti_cred],x.[aa_cred],x.[qs_cred],x.[co_empr] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.fctbdeta_liqd_cred'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_liqd]),CONVERT(NVARCHAR(200), d.[sc_deta]),CONVERT(NVARCHAR(200), d.[ti_cred]),CONVERT(NVARCHAR(200), d.[aa_cred]),CONVERT(NVARCHAR(200), d.[qs_cred])),
            tt.t,
            N''DELETE'',
            (SELECT x.[sc_liqd],x.[co_rubr],x.[sc_deta],x.[ti_cred],x.[aa_cred],x.[qs_cred],x.[co_empr] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.fctbdeta_liqd_cred'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_fctbdinf_liqd_cnta_sibs  ON dbFC.dbo.fctbdinf_liqd_cnta_sibs (1 type) -----
      - cuentasEnLegalType
*/
IF OBJECT_ID(N'dbFC.dbo.fctbdinf_liqd_cnta_sibs', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbFC.dbo.fctbdinf_liqd_cnta_sibs no existe; trigger trg_outbox_fctbdinf_liqd_cnta_sibs no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_fctbdinf_liqd_cnta_sibs', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbdinf_liqd_cnta_sibs;
GO

IF OBJECT_ID(N'dbFC.dbo.fctbdinf_liqd_cnta_sibs', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_fctbdinf_liqd_cnta_sibs
ON dbo.[fctbdinf_liqd_cnta_sibs]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cuentasEnLegalType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[sc_regi]),CONVERT(NVARCHAR(200), i.[ci_cedu])),
            tt.t,
            @op,
            (SELECT x.[mo_desc],x.[ti_iden],x.[sc_regi],x.[ci_cedu] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.fctbdinf_liqd_cnta_sibs'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[sc_regi]),CONVERT(NVARCHAR(200), d.[ci_cedu])),
            tt.t,
            N''DELETE'',
            (SELECT x.[mo_desc],x.[ti_iden],x.[sc_regi],x.[ci_cedu] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.fctbdinf_liqd_cnta_sibs'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_fctbmvto_impr_esta_cnta  ON dbFC.dbo.fctbmvto_impr_esta_cnta (1 type) -----
      - cuentaPersonasType
*/
IF OBJECT_ID(N'dbFC.dbo.fctbmvto_impr_esta_cnta', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbFC.dbo.fctbmvto_impr_esta_cnta no existe; trigger trg_outbox_fctbmvto_impr_esta_cnta no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_fctbmvto_impr_esta_cnta', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbmvto_impr_esta_cnta;
GO

IF OBJECT_ID(N'dbFC.dbo.fctbmvto_impr_esta_cnta', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_fctbmvto_impr_esta_cnta
ON dbo.[fctbmvto_impr_esta_cnta]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''cuentaPersonasType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[ci_cedu]),CONVERT(NVARCHAR(200), i.[co_tran]),CONVERT(NVARCHAR(200), i.[co_grup_cnta]),CONVERT(NVARCHAR(200), i.[co_rol])),
            tt.t,
            @op,
            (SELECT x.[co_grup_cnta],x.[nu_prio],x.[co_tipo_tran],x.[ci_cedu],x.[co_tran],x.[co_rol] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.fctbmvto_impr_esta_cnta'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[ci_cedu]),CONVERT(NVARCHAR(200), d.[co_tran]),CONVERT(NVARCHAR(200), d.[co_grup_cnta]),CONVERT(NVARCHAR(200), d.[co_rol])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_grup_cnta],x.[nu_prio],x.[co_tipo_tran],x.[ci_cedu],x.[co_tran],x.[co_rol] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.fctbmvto_impr_esta_cnta'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_fctbproc_tseg_noti  ON dbFC.dbo.fctbproc_tseg_noti (1 type) -----
      - fechasProcesoType
*/
IF OBJECT_ID(N'dbFC.dbo.fctbproc_tseg_noti', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbFC.dbo.fctbproc_tseg_noti no existe; trigger trg_outbox_fctbproc_tseg_noti no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_fctbproc_tseg_noti', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_fctbproc_tseg_noti;
GO

IF OBJECT_ID(N'dbFC.dbo.fctbproc_tseg_noti', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_fctbproc_tseg_noti
ON dbo.[fctbproc_tseg_noti]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''fechasProcesoType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_proc]),CONVERT(NVARCHAR(200), i.[co_tseg]),CONVERT(NVARCHAR(200), i.[co_noti]),CONVERT(NVARCHAR(200), i.[sc_regi])),
            tt.t,
            @op,
            (SELECT x.[co_proc],x.[co_usua_crea],x.[co_mens_telf],x.[co_noti],x.[co_empr],x.[co_tseg],x.[sc_regi] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.fctbproc_tseg_noti'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_proc]),CONVERT(NVARCHAR(200), d.[co_tseg]),CONVERT(NVARCHAR(200), d.[co_noti]),CONVERT(NVARCHAR(200), d.[sc_regi])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_proc],x.[co_usua_crea],x.[co_mens_telf],x.[co_noti],x.[co_empr],x.[co_tseg],x.[sc_regi] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.fctbproc_tseg_noti'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_sfct_afiliado_referencias  ON dbFC.dbo.sfct_afiliado_referencias (1 type) -----
      - referenciaCliente_type
*/
IF OBJECT_ID(N'dbFC.dbo.sfct_afiliado_referencias', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbFC.dbo.sfct_afiliado_referencias no existe; trigger trg_outbox_sfct_afiliado_referencias no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_sfct_afiliado_referencias', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_afiliado_referencias;
GO

IF OBJECT_ID(N'dbFC.dbo.sfct_afiliado_referencias', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_sfct_afiliado_referencias
ON dbo.[sfct_afiliado_referencias]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''referenciaCliente_type'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[sc_refe]),CONVERT(NVARCHAR(200), i.[co_tref]),CONVERT(NVARCHAR(200), i.[ci_cedula])),
            tt.t,
            @op,
            (SELECT x.[sc_refe],x.[st_cart],x.[co_tref],x.[fe_ingr],x.[ci_cedula] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.sfct_afiliado_referencias'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[sc_refe]),CONVERT(NVARCHAR(200), d.[co_tref]),CONVERT(NVARCHAR(200), d.[ci_cedula])),
            tt.t,
            N''DELETE'',
            (SELECT x.[sc_refe],x.[st_cart],x.[co_tref],x.[fe_ingr],x.[ci_cedula] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.sfct_afiliado_referencias'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO

/* ----- trg_outbox_sfct_saldos_diarios_afiliados  ON dbFC.dbo.sfct_saldos_diarios_afiliados (2 types) -----
      - saldoCxPCxCType
      - saldoVinculadoType
*/
IF OBJECT_ID(N'dbFC.dbo.sfct_saldos_diarios_afiliados', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy dbFC.dbo.sfct_saldos_diarios_afiliados no existe; trigger trg_outbox_sfct_saldos_diarios_afiliados no creado';
ELSE IF OBJECT_ID(N'dbo.trg_outbox_sfct_saldos_diarios_afiliados', N'TR') IS NOT NULL
    DROP TRIGGER dbo.trg_outbox_sfct_saldos_diarios_afiliados;
GO

IF OBJECT_ID(N'dbFC.dbo.sfct_saldos_diarios_afiliados', N'U') IS NOT NULL
    EXEC(N'
CREATE TRIGGER dbo.trg_outbox_sfct_saldos_diarios_afiliados
ON dbo.[sfct_saldos_diarios_afiliados]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N''is_replicating'')), 0) = 1 RETURN;
    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted) RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N''UPDATE''
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N''INSERT''
            ELSE N''DELETE''
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES (N''saldoCxPCxCType''),(N''saldoVinculadoType'');

    IF @op IN (N''INSERT'', N''UPDATE'')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), i.[fx_saldo]),CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_fond]),CONVERT(NVARCHAR(200), i.[ci_cedula])),
            tt.t,
            @op,
            (SELECT x.[co_empr],x.[co_fond],x.[fx_saldo],x.[ci_cedula] FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.sfct_saldos_diarios_afiliados'',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS(''|'',CONVERT(NVARCHAR(200), d.[fx_saldo]),CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_fond]),CONVERT(NVARCHAR(200), d.[ci_cedula])),
            tt.t,
            N''DELETE'',
            (SELECT x.[co_empr],x.[co_fond],x.[fx_saldo],x.[ci_cedula] FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N''dbFC.dbo.sfct_saldos_diarios_afiliados'',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
');
GO
