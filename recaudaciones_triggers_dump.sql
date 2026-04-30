/* ============================================================
   DUMP TRIGGERS RECAUDACIONES (F1 + F2) - DDL completo
   Snapshot generado a partir de _deploy_recaudaciones_flow1.py / _deploy_recaudaciones_flow2.py
   ============================================================ */

/* ############################################################
   FLUJO 1 - Recaudaciones dbRC -> fcme_canonicos.cdc_outbox
   Filtro: triggers cuya definicion publica un aggregate_type de Recaudaciones
   ############################################################ */

USE [dbRC];
GO

/* TOTAL F1 (dbRC) Recaudaciones: 11 triggers */

/* --- trg_outbox_rctbapli_reca  ON dbo.rctbapli_reca  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbapli_reca', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbapli_reca;
GO
CREATE TRIGGER dbo.trg_outbox_rctbapli_reca
ON dbo.[rctbapli_reca]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[sc_apli_reca]),CONVERT(NVARCHAR(200), i.[ci_cedu]),CONVERT(NVARCHAR(200), i.[ti_reca]),CONVERT(NVARCHAR(200), i.[co_rol])), N'aplicacionRecaudacion_type', @op,
            (SELECT x.[sc_apli_reca],x.[ci_cedu],x.[ti_reca],x.[co_rol],x.[co_inst],x.[co_prov],x.[mo_reca],x.[mo_rubr] FROM inserted x WHERE x.[sc_apli_reca]=i.[sc_apli_reca] AND x.[ci_cedu]=i.[ci_cedu] AND x.[ti_reca]=i.[ti_reca] AND x.[co_rol]=i.[co_rol] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbapli_reca', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[sc_apli_reca]),CONVERT(NVARCHAR(200), d.[ci_cedu]),CONVERT(NVARCHAR(200), d.[ti_reca]),CONVERT(NVARCHAR(200), d.[co_rol])), N'aplicacionRecaudacion_type', N'DELETE',
            (SELECT x.[sc_apli_reca],x.[ci_cedu],x.[ti_reca],x.[co_rol],x.[co_inst],x.[co_prov],x.[mo_reca],x.[mo_rubr] FROM deleted x WHERE x.[sc_apli_reca]=d.[sc_apli_reca] AND x.[ci_cedu]=d.[ci_cedu] AND x.[ti_reca]=d.[ti_reca] AND x.[co_rol]=d.[co_rol] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbapli_reca', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbcaut  ON dbo.rctbcaut  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbcaut', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbcaut;
GO
CREATE TRIGGER dbo.trg_outbox_rctbcaut
ON dbo.[rctbcaut]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[ti_reca]),CONVERT(NVARCHAR(200), i.[ti_dsto])), N'caucionRecaudacion_type', @op,
            (SELECT x.[co_empr],x.[ti_reca],x.[ti_dsto],x.[co_cnta_auto_noid],x.[co_cnta_auto_papl] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[ti_reca]=i.[ti_reca] AND x.[ti_dsto]=i.[ti_dsto] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbcaut', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[ti_reca]),CONVERT(NVARCHAR(200), d.[ti_dsto])), N'caucionRecaudacion_type', N'DELETE',
            (SELECT x.[co_empr],x.[ti_reca],x.[ti_dsto],x.[co_cnta_auto_noid],x.[co_cnta_auto_papl] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[ti_reca]=d.[ti_reca] AND x.[ti_dsto]=d.[ti_dsto] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbcaut', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbdevo_rind  ON dbo.rctbdevo_rind  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbdevo_rind', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbdevo_rind;
GO
CREATE TRIGGER dbo.trg_outbox_rctbdevo_rind
ON dbo.[rctbdevo_rind]
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
        SELECT CONVERT(NVARCHAR(200), i.[sc_devo]), N'devolucionRendicion_type', @op,
            (SELECT x.[sc_devo],x.[ci_cedu],x.[ti_reca],x.[ti_dsto],x.[nu_cpbt],x.[co_cnta],x.[mo_devo],x.[fe_depo] FROM inserted x WHERE x.[sc_devo]=i.[sc_devo] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbdevo_rind', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[sc_devo]), N'devolucionRendicion_type', N'DELETE',
            (SELECT x.[sc_devo],x.[ci_cedu],x.[ti_reca],x.[ti_dsto],x.[nu_cpbt],x.[co_cnta],x.[mo_devo],x.[fe_depo] FROM deleted x WHERE x.[sc_devo]=d.[sc_devo] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbdevo_rind', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbesta_cnta  ON dbo.rctbesta_cnta  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbesta_cnta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbesta_cnta;
GO
CREATE TRIGGER dbo.trg_outbox_rctbesta_cnta
ON dbo.[rctbesta_cnta]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_cpbt])), N'estadoCuenta_type', @op,
            (SELECT x.[co_empr],x.[sc_cpbt],x.[co_cnta],x.[nu_cpbt],x.[fe_depo],x.[mo_depo] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[sc_cpbt]=i.[sc_cpbt] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbesta_cnta', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_cpbt])), N'estadoCuenta_type', N'DELETE',
            (SELECT x.[co_empr],x.[sc_cpbt],x.[co_cnta],x.[nu_cpbt],x.[fe_depo],x.[mo_depo] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[sc_cpbt]=d.[sc_cpbt] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbesta_cnta', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbesta_reca  ON dbo.rctbesta_reca  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbesta_reca', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbesta_reca;
GO
CREATE TRIGGER dbo.trg_outbox_rctbesta_reca
ON dbo.[rctbesta_reca]
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
        SELECT CONVERT(NVARCHAR(200), i.[st_reca]), N'estadoRecaudacion_type', @op,
            (SELECT x.[st_reca],x.[ds_estado],x.[no_corto],x.[ci_tipo],x.[ti_esta] FROM inserted x WHERE x.[st_reca]=i.[st_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbesta_reca', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[st_reca]), N'estadoRecaudacion_type', N'DELETE',
            (SELECT x.[st_reca],x.[ds_estado],x.[no_corto],x.[ci_tipo],x.[ti_esta] FROM deleted x WHERE x.[st_reca]=d.[st_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbesta_reca', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbreca  ON dbo.rctbreca  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbreca', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbreca;
GO
CREATE TRIGGER dbo.trg_outbox_rctbreca
ON dbo.[rctbreca]
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
        SELECT CONVERT(NVARCHAR(200), i.[sc_reca]), N'recaudacion_type', @op,
            (SELECT x.[ti_reca],x.[st_reca],x.[mo_reca],x.[sc_reca],x.[fe_ingr],x.[fe_autr] FROM inserted x WHERE x.[sc_reca]=i.[sc_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbreca', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[sc_reca]), N'recaudacion_type', N'DELETE',
            (SELECT x.[ti_reca],x.[st_reca],x.[mo_reca],x.[sc_reca],x.[fe_ingr],x.[fe_autr] FROM deleted x WHERE x.[sc_reca]=d.[sc_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbreca', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbrind  ON dbo.rctbrind  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbrind', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbrind;
GO
CREATE TRIGGER dbo.trg_outbox_rctbrind
ON dbo.[rctbrind]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_reca])), N'rendicion_type', @op,
            (SELECT x.[co_empr],x.[sc_reca],x.[ci_cedula],x.[co_rol],x.[co_inst],x.[co_prov],x.[ti_reca],x.[nu_cpbt] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[sc_reca]=i.[sc_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbrind', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_reca])), N'rendicion_type', N'DELETE',
            (SELECT x.[co_empr],x.[sc_reca],x.[ci_cedula],x.[co_rol],x.[co_inst],x.[co_prov],x.[ti_reca],x.[nu_cpbt] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[sc_reca]=d.[sc_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbrind', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbcsal_reca  ON dbo.rctbcsal_reca  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbcsal_reca', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbcsal_reca;
GO
CREATE TRIGGER dbo.trg_outbox_rctbcsal_reca
ON dbo.[rctbcsal_reca]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_fond]),CONVERT(NVARCHAR(200), i.[fe_cort]),CONVERT(NVARCHAR(200), i.[sc_rol]),CONVERT(NVARCHAR(200), i.[co_rol]),CONVERT(NVARCHAR(200), i.[ti_reca])), N'saldoRecaudacion_type', @op,
            (SELECT x.[co_empr],x.[co_fond],x.[fe_cort],x.[sc_rol],x.[co_rol],x.[ti_reca],x.[mo_gene] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_fond]=i.[co_fond] AND x.[fe_cort]=i.[fe_cort] AND x.[sc_rol]=i.[sc_rol] AND x.[co_rol]=i.[co_rol] AND x.[ti_reca]=i.[ti_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbcsal_reca', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_fond]),CONVERT(NVARCHAR(200), d.[fe_cort]),CONVERT(NVARCHAR(200), d.[sc_rol]),CONVERT(NVARCHAR(200), d.[co_rol]),CONVERT(NVARCHAR(200), d.[ti_reca])), N'saldoRecaudacion_type', N'DELETE',
            (SELECT x.[co_empr],x.[co_fond],x.[fe_cort],x.[sc_rol],x.[co_rol],x.[ti_reca],x.[mo_gene] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_fond]=d.[co_fond] AND x.[fe_cort]=d.[fe_cort] AND x.[sc_rol]=d.[sc_rol] AND x.[co_rol]=d.[co_rol] AND x.[ti_reca]=d.[ti_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbcsal_reca', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbcsci_bce  ON dbo.rctbcsci_bce  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbcsci_bce', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbcsci_bce;
GO
CREATE TRIGGER dbo.trg_outbox_rctbcsci_bce
ON dbo.[rctbcsci_bce]
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
        SELECT CONVERT(NVARCHAR(200), i.[sc_scib]), N'sciRecaudacion_type', @op,
            (SELECT x.[sc_scib],x.[ci_rol],x.[fe_gene],x.[ho_gene],x.[nu_envi],x.[nu_regi_tota],x.[mo_gene_tota] FROM inserted x WHERE x.[sc_scib]=i.[sc_scib] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbcsci_bce', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[sc_scib]), N'sciRecaudacion_type', N'DELETE',
            (SELECT x.[sc_scib],x.[ci_rol],x.[fe_gene],x.[ho_gene],x.[nu_envi],x.[nu_regi_tota],x.[mo_gene_tota] FROM deleted x WHERE x.[sc_scib]=d.[sc_scib] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbcsci_bce', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbtipo_desc  ON dbo.rctbtipo_desc  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbtipo_desc', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbtipo_desc;
GO
CREATE TRIGGER dbo.trg_outbox_rctbtipo_desc
ON dbo.[rctbtipo_desc]
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
        SELECT CONVERT(NVARCHAR(200), i.[ti_desc]), N'tipoDescuento_type', @op,
            (SELECT x.[ti_desc],x.[no_tipo_desc],x.[ds_tipo_desc],x.[st_regi] FROM inserted x WHERE x.[ti_desc]=i.[ti_desc] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbtipo_desc', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[ti_desc]), N'tipoDescuento_type', N'DELETE',
            (SELECT x.[ti_desc],x.[no_tipo_desc],x.[ds_tipo_desc],x.[st_regi] FROM deleted x WHERE x.[ti_desc]=d.[ti_desc] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbtipo_desc', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_rctbtrec  ON dbo.rctbtrec  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_rctbtrec', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_rctbtrec;
GO
CREATE TRIGGER dbo.trg_outbox_rctbtrec
ON dbo.[rctbtrec]
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
        SELECT CONVERT(NVARCHAR(200), i.[ti_reca]), N'transaccionRecaudacion_type', @op,
            (SELECT x.[ti_reca],x.[no_reca],x.[co_fond],x.[ti_proc] FROM inserted x WHERE x.[ti_reca]=i.[ti_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbtrec', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[ti_reca]), N'transaccionRecaudacion_type', N'DELETE',
            (SELECT x.[ti_reca],x.[no_reca],x.[co_fond],x.[ti_proc] FROM deleted x WHERE x.[ti_reca]=d.[ti_reca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbRC.dbo.rctbtrec', SYSUTCDATETIME()
        FROM deleted d;
END
GO


/* ############################################################
   FLUJO 2 - Recaudaciones FCME_USER -> FCME_USER.CDC_OUTBOX
   ############################################################ */

/* TOTAL F2 (FCME_USER) Recaudaciones: 11 triggers */

/* --- TRG_OUTBOX_APLICACIONRECAUD  ON FCME_USER.APLICACIONRECAUDACION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_APLICACIONRECAUD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.APLICACIONRECAUDACION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAAPLIRECA' VALUE :NEW.SECUENCIAAPLIRECA, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'TIPORECA' VALUE :NEW.TIPORECA, 'CODIGOROL' VALUE :NEW.CODIGOROL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIAAPLIRECA' VALUE :NEW.SECUENCIAAPLIRECA, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'TIPORECA' VALUE :NEW.TIPORECA, 'CODIGOROL' VALUE :NEW.CODIGOROL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIAAPLIRECA' VALUE :OLD.SECUENCIAAPLIRECA, 'CODIGOCEDU' VALUE :OLD.CODIGOCEDU, 'TIPORECA' VALUE :OLD.TIPORECA, 'CODIGOROL' VALUE :OLD.CODIGOROL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('aplicacionRecaudacion_type', v_pk, v_event, v_payload, 'FCME_USER.APLICACIONRECAUDACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CAUCIONRECAUD  ON FCME_USER.CAUCIONRECAUDACION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CAUCIONRECAUD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.CAUCIONRECAUDACION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPORECA' VALUE :NEW.TIPORECA, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'TIPODSTO' VALUE :NEW.TIPODSTO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPORECA' VALUE :NEW.TIPORECA, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'TIPODSTO' VALUE :NEW.TIPODSTO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPORECA' VALUE :OLD.TIPORECA, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'TIPODSTO' VALUE :OLD.TIPODSTO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('caucionRecaudacion_type', v_pk, v_event, v_payload, 'FCME_USER.CAUCIONRECAUDACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_DEVOLUCIONRENDICION  ON FCME_USER.DEVOLUCIONRENDICION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_DEVOLUCIONRENDICION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.DEVOLUCIONRENDICION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIADEVO' VALUE :NEW.SECUENCIADEVO, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'TIPORECA' VALUE :NEW.TIPORECA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIADEVO' VALUE :NEW.SECUENCIADEVO, 'CODIGOCEDU' VALUE :NEW.CODIGOCEDU, 'TIPORECA' VALUE :NEW.TIPORECA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIADEVO' VALUE :OLD.SECUENCIADEVO, 'CODIGOCEDU' VALUE :OLD.CODIGOCEDU, 'TIPORECA' VALUE :OLD.TIPORECA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('devolucionRendicion_type', v_pk, v_event, v_payload, 'FCME_USER.DEVOLUCIONRENDICION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_ESTADOCUENTA  ON FCME_USER.ESTADOCUENTA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ESTADOCUENTA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.ESTADOCUENTA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACPBT' VALUE :NEW.SECUENCIACPBT, 'CODIGOCNTA' VALUE :NEW.CODIGOCNTA, 'NUMEROCPBT' VALUE :NEW.NUMEROCPBT);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIACPBT' VALUE :NEW.SECUENCIACPBT, 'CODIGOCNTA' VALUE :NEW.CODIGOCNTA, 'NUMEROCPBT' VALUE :NEW.NUMEROCPBT);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIACPBT' VALUE :OLD.SECUENCIACPBT, 'CODIGOCNTA' VALUE :OLD.CODIGOCNTA, 'NUMEROCPBT' VALUE :OLD.NUMEROCPBT);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('estadoCuenta_type', v_pk, v_event, v_payload, 'FCME_USER.ESTADOCUENTA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_ESTADORECAUDACION  ON FCME_USER.ESTADORECAUDACION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ESTADORECAUDACION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.ESTADORECAUDACION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ESTADORECA' VALUE :NEW.ESTADORECA, 'DESCRIPCIONESTADO' VALUE :NEW.DESCRIPCIONESTADO, 'NOMBRECORTO' VALUE :NEW.NOMBRECORTO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'ESTADORECA' VALUE :NEW.ESTADORECA, 'DESCRIPCIONESTADO' VALUE :NEW.DESCRIPCIONESTADO, 'NOMBRECORTO' VALUE :NEW.NOMBRECORTO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'ESTADORECA' VALUE :OLD.ESTADORECA, 'DESCRIPCIONESTADO' VALUE :OLD.DESCRIPCIONESTADO, 'NOMBRECORTO' VALUE :OLD.NOMBRECORTO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('estadoRecaudacion_type', v_pk, v_event, v_payload, 'FCME_USER.ESTADORECAUDACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RECAUDACION  ON FCME_USER.RECAUDACION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RECAUDACION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.RECAUDACION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIA_RECA' VALUE :NEW.SECUENCIA_RECA, 'TIPO_RECA' VALUE :NEW.TIPO_RECA, 'ESTADO_RECA' VALUE :NEW.ESTADO_RECA, 'MONTO_RECA' VALUE :NEW.MONTO_RECA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIA_RECA' VALUE :NEW.SECUENCIA_RECA, 'TIPO_RECA' VALUE :NEW.TIPO_RECA, 'ESTADO_RECA' VALUE :NEW.ESTADO_RECA, 'MONTO_RECA' VALUE :NEW.MONTO_RECA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIA_RECA' VALUE :OLD.SECUENCIA_RECA, 'TIPO_RECA' VALUE :OLD.TIPO_RECA, 'ESTADO_RECA' VALUE :OLD.ESTADO_RECA, 'MONTO_RECA' VALUE :OLD.MONTO_RECA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('recaudacion_type', v_pk, v_event, v_payload, 'FCME_USER.RECAUDACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RENDICION  ON FCME_USER.RENDICION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RENDICION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.RENDICION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO_CEDULA' VALUE :NEW.CODIGO_CEDULA, 'CODIGO_ROL' VALUE :NEW.CODIGO_ROL, 'TIPO_RECA' VALUE :NEW.TIPO_RECA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO_CEDULA' VALUE :NEW.CODIGO_CEDULA, 'CODIGO_ROL' VALUE :NEW.CODIGO_ROL, 'TIPO_RECA' VALUE :NEW.TIPO_RECA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGO_CEDULA' VALUE :OLD.CODIGO_CEDULA, 'CODIGO_ROL' VALUE :OLD.CODIGO_ROL, 'TIPO_RECA' VALUE :OLD.TIPO_RECA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('rendicion_type', v_pk, v_event, v_payload, 'FCME_USER.RENDICION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SALDORECAUDACION  ON FCME_USER.SALDORECAUDACION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SALDORECAUDACION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.SALDORECAUDACION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'TIPORECA' VALUE :NEW.TIPORECA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'SECUENCIAROL' VALUE :NEW.SECUENCIAROL, 'CODIGOROL' VALUE :NEW.CODIGOROL, 'TIPORECA' VALUE :NEW.TIPORECA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'SECUENCIAROL' VALUE :OLD.SECUENCIAROL, 'CODIGOROL' VALUE :OLD.CODIGOROL, 'TIPORECA' VALUE :OLD.TIPORECA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('saldoRecaudacion_type', v_pk, v_event, v_payload, 'FCME_USER.SALDORECAUDACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SCIRECAUDACION  ON FCME_USER.SCIRECAUDACION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SCIRECAUDACION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.SCIRECAUDACION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIASCIB' VALUE :NEW.SECUENCIASCIB, 'CODIGOROL' VALUE :NEW.CODIGOROL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'SECUENCIASCIB' VALUE :NEW.SECUENCIASCIB, 'CODIGOROL' VALUE :NEW.CODIGOROL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'SECUENCIASCIB' VALUE :OLD.SECUENCIASCIB, 'CODIGOROL' VALUE :OLD.CODIGOROL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('sciRecaudacion_type', v_pk, v_event, v_payload, 'FCME_USER.SCIRECAUDACION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_TIPODESCUENTO  ON FCME_USER.TIPODESCUENTO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_TIPODESCUENTO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.TIPODESCUENTO_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPODESC' VALUE :NEW.TIPODESC, 'NOMBRETIPODESC' VALUE :NEW.NOMBRETIPODESC, 'DESCRIPCIONTIPODESC' VALUE :NEW.DESCRIPCIONTIPODESC);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPODESC' VALUE :NEW.TIPODESC, 'NOMBRETIPODESC' VALUE :NEW.NOMBRETIPODESC, 'DESCRIPCIONTIPODESC' VALUE :NEW.DESCRIPCIONTIPODESC);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPODESC' VALUE :OLD.TIPODESC, 'NOMBRETIPODESC' VALUE :OLD.NOMBRETIPODESC, 'DESCRIPCIONTIPODESC' VALUE :OLD.DESCRIPCIONTIPODESC);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('tipoDescuento_type', v_pk, v_event, v_payload, 'FCME_USER.TIPODESCUENTO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_TRANSACCIONRECAUD  ON FCME_USER.TRANSACCIONRECAUDACION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_TRANSACCIONRECAUD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.TRANSACCIONRECAUDACION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPORECA' VALUE :NEW.TIPORECA, 'NOMBRERECA' VALUE :NEW.NOMBRERECA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'TIPOPROC' VALUE :NEW.TIPOPROC);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'TIPORECA' VALUE :NEW.TIPORECA, 'NOMBRERECA' VALUE :NEW.NOMBRERECA, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'TIPOPROC' VALUE :NEW.TIPOPROC);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'TIPORECA' VALUE :OLD.TIPORECA, 'NOMBRERECA' VALUE :OLD.NOMBRERECA, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'TIPOPROC' VALUE :OLD.TIPOPROC);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('transaccionRecaudacion_type', v_pk, v_event, v_payload, 'FCME_USER.TRANSACCIONRECAUDACION_TYPE', SYSTIMESTAMP);
END;
/
