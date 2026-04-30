/* ============================================================
   DUMP TRIGGERS SEGURIDAD (F1 + F2) - DDL completo
   Snapshot generado a partir de _deploy_seguridad_flow1.py / _deploy_seguridad_flow2.py
   ============================================================ */

/* ############################################################
   FLUJO 1 - Seguridad dbSG -> fcme_canonicos.cdc_outbox
   Filtro: triggers cuya definicion publica un aggregate_type de Seguridad
   ############################################################ */

USE [dbSG];
GO

/* TOTAL F1 (dbSG) Seguridad: 11 triggers */

/* --- trg_outbox_sgtbapli  ON dbo.sgtbapli  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbapli', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbapli;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbapli
ON dbo.[sgtbapli]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_apli]),CONVERT(NVARCHAR(200), i.[ti_loca])), N'aplicacionFuncion_type', @op,
            (SELECT x.[co_apli],x.[ti_loca],x.[no_apli],x.[st_apli] FROM inserted x WHERE x.[co_apli]=i.[co_apli] AND x.[ti_loca]=i.[ti_loca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbapli', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_apli]),CONVERT(NVARCHAR(200), d.[ti_loca])), N'aplicacionFuncion_type', N'DELETE',
            (SELECT x.[co_apli],x.[ti_loca],x.[no_apli],x.[st_apli] FROM deleted x WHERE x.[co_apli]=d.[co_apli] AND x.[ti_loca]=d.[ti_loca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbapli', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbtran  ON dbo.sgtbtran  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbtran', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbtran;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbtran
ON dbo.[sgtbtran]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_apli]),CONVERT(NVARCHAR(200), i.[co_func]),CONVERT(NVARCHAR(200), i.[nu_tran]),CONVERT(NVARCHAR(200), i.[ti_loca])), N'auditoriaFlujo_type', @op,
            (SELECT x.[co_apli],x.[co_func],x.[nu_tran],x.[ti_loca],x.[no_tran] FROM inserted x WHERE x.[co_apli]=i.[co_apli] AND x.[co_func]=i.[co_func] AND x.[nu_tran]=i.[nu_tran] AND x.[ti_loca]=i.[ti_loca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbtran', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_apli]),CONVERT(NVARCHAR(200), d.[co_func]),CONVERT(NVARCHAR(200), d.[nu_tran]),CONVERT(NVARCHAR(200), d.[ti_loca])), N'auditoriaFlujo_type', N'DELETE',
            (SELECT x.[co_apli],x.[co_func],x.[nu_tran],x.[ti_loca],x.[no_tran] FROM deleted x WHERE x.[co_apli]=d.[co_apli] AND x.[co_func]=d.[co_func] AND x.[nu_tran]=d.[nu_tran] AND x.[ti_loca]=d.[ti_loca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbtran', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbcnts  ON dbo.sgtbcnts  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbcnts', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbcnts;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbcnts
ON dbo.[sgtbcnts]
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
        SELECT CONVERT(NVARCHAR(200), i.[co_cnts]), N'cuentaNostroType', @op,
            (SELECT x.[co_cnts],x.[nu_iden],x.[no_cnts],x.[st_cnts],x.[ds_mail] FROM inserted x WHERE x.[co_cnts]=i.[co_cnts] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbcnts', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[co_cnts]), N'cuentaNostroType', N'DELETE',
            (SELECT x.[co_cnts],x.[nu_iden],x.[no_cnts],x.[st_cnts],x.[ds_mail] FROM deleted x WHERE x.[co_cnts]=d.[co_cnts] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbcnts', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbempr  ON dbo.sgtbempr  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbempr', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbempr;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbempr
ON dbo.[sgtbempr]
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
        SELECT CONVERT(NVARCHAR(200), i.[co_empr]), N'empresa_type', @op,
            (SELECT x.[co_empr],x.[no_empr],x.[st_empr],x.[nu_ruc],x.[no_desc] FROM inserted x WHERE x.[co_empr]=i.[co_empr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbempr', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[co_empr]), N'empresa_type', N'DELETE',
            (SELECT x.[co_empr],x.[no_empr],x.[st_empr],x.[nu_ruc],x.[no_desc] FROM deleted x WHERE x.[co_empr]=d.[co_empr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbempr', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbfirm  ON dbo.sgtbfirm  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbfirm', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbfirm;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbfirm
ON dbo.[sgtbfirm]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_usua]),CONVERT(NVARCHAR(200), i.[fe_firm]),CONVERT(NVARCHAR(200), i.[ho_firm])), N'firmaSeguridad_type', @op,
            (SELECT x.[co_empr],x.[co_usua],x.[no_maqu],x.[no_usua_nt],x.[fe_firm],x.[ho_firm] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_usua]=i.[co_usua] AND x.[fe_firm]=i.[fe_firm] AND x.[ho_firm]=i.[ho_firm] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbfirm', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_usua]),CONVERT(NVARCHAR(200), d.[fe_firm]),CONVERT(NVARCHAR(200), d.[ho_firm])), N'firmaSeguridad_type', N'DELETE',
            (SELECT x.[co_empr],x.[co_usua],x.[no_maqu],x.[no_usua_nt],x.[fe_firm],x.[ho_firm] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_usua]=d.[co_usua] AND x.[fe_firm]=d.[fe_firm] AND x.[ho_firm]=d.[ho_firm] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbfirm', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbfond  ON dbo.sgtbfond  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbfond', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbfond;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbfond
ON dbo.[sgtbfond]
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
        SELECT CONVERT(NVARCHAR(200), i.[co_fond]), N'fondoSeguridad_type', @op,
            (SELECT x.[co_fond],x.[no_fond],x.[st_fond],x.[in_part],x.[co_rubr_prim] FROM inserted x WHERE x.[co_fond]=i.[co_fond] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbfond', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[co_fond]), N'fondoSeguridad_type', N'DELETE',
            (SELECT x.[co_fond],x.[no_fond],x.[st_fond],x.[in_part],x.[co_rubr_prim] FROM deleted x WHERE x.[co_fond]=d.[co_fond] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbfond', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbloca  ON dbo.sgtbloca  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbloca', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbloca;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbloca
ON dbo.[sgtbloca]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_loca])), N'localidad_type', @op,
            (SELECT x.[co_empr],x.[co_loca],x.[no_loca],x.[co_prov],x.[ti_loca],x.[ci_repr],x.[no_repr] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_loca]=i.[co_loca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbloca', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_loca])), N'localidad_type', N'DELETE',
            (SELECT x.[co_empr],x.[co_loca],x.[no_loca],x.[co_prov],x.[ti_loca],x.[ci_repr],x.[no_repr] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_loca]=d.[co_loca] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbloca', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbpara  ON dbo.sgtbpara  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbpara', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbpara;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbpara
ON dbo.[sgtbpara]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_para])), N'parametroSeguridad_type', @op,
            (SELECT x.[co_empr],x.[co_para],x.[no_para],x.[va_para],x.[in_prov] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_para]=i.[co_para] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbpara', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_para])), N'parametroSeguridad_type', N'DELETE',
            (SELECT x.[co_empr],x.[co_para],x.[no_para],x.[va_para],x.[in_prov] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_para]=d.[co_para] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbpara', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbpass  ON dbo.sgtbpass  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbpass', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbpass;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbpass
ON dbo.[sgtbpass]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_usua]),CONVERT(NVARCHAR(200), i.[sc_pass])), N'passwordSeguridad_type', @op,
            (SELECT x.[co_empr],x.[co_usua],x.[sc_pass],x.[ds_pass],x.[fe_ingr],x.[st_pass],x.[ti_pass] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_usua]=i.[co_usua] AND x.[sc_pass]=i.[sc_pass] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbpass', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_usua]),CONVERT(NVARCHAR(200), d.[sc_pass])), N'passwordSeguridad_type', N'DELETE',
            (SELECT x.[co_empr],x.[co_usua],x.[sc_pass],x.[ds_pass],x.[fe_ingr],x.[st_pass],x.[ti_pass] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_usua]=d.[co_usua] AND x.[sc_pass]=d.[sc_pass] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbpass', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbusua  ON dbo.sgtbusua  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbusua', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbusua;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbusua
ON dbo.[sgtbusua]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_usua])), N'usuarioSeguridad_type', @op,
            (SELECT x.[co_empr],x.[co_usua],x.[no_usua],x.[fe_ingr],x.[fe_expi],x.[ds_pass],x.[nu_cedu] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_usua]=i.[co_usua] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbusua', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_usua])), N'usuarioSeguridad_type', N'DELETE',
            (SELECT x.[co_empr],x.[co_usua],x.[no_usua],x.[fe_ingr],x.[fe_expi],x.[ds_pass],x.[nu_cedu] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_usua]=d.[co_usua] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbusua', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_sgtbconf_serv_apli  ON dbo.sgtbconf_serv_apli  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_sgtbconf_serv_apli', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_sgtbconf_serv_apli;
GO
CREATE TRIGGER dbo.trg_outbox_sgtbconf_serv_apli
ON dbo.[sgtbconf_serv_apli]
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
        SELECT CONVERT(NVARCHAR(200), i.[sc_serv]), N'usuarioServicio_type', @op,
            (SELECT x.[sc_serv],x.[co_serv_apli],x.[no_serv_apli],x.[sc_tipo],x.[no_usua],x.[ds_pass],x.[st_regi] FROM inserted x WHERE x.[sc_serv]=i.[sc_serv] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbconf_serv_apli', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[sc_serv]), N'usuarioServicio_type', N'DELETE',
            (SELECT x.[sc_serv],x.[co_serv_apli],x.[no_serv_apli],x.[sc_tipo],x.[no_usua],x.[ds_pass],x.[st_regi] FROM deleted x WHERE x.[sc_serv]=d.[sc_serv] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbSG.dbo.sgtbconf_serv_apli', SYSUTCDATETIME()
        FROM deleted d;
END
GO


/* ############################################################
   FLUJO 2 - Seguridad FCME_USER -> FCME_USER.CDC_OUTBOX
   ############################################################ */

/* TOTAL F2 (FCME_USER) Seguridad: 11 triggers */

/* --- TRG_OUTBOX_APLICACIONFUNCION  ON FCME_USER.APLICACIONFUNCION_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_APLICACIONFUNCION
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.APLICACIONFUNCION_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOAPLICACION' VALUE :NEW.CODIGOAPLICACION, 'TIPOLOCALIDAD' VALUE :NEW.TIPOLOCALIDAD, 'NOMBREAPLICACION' VALUE :NEW.NOMBREAPLICACION, 'ESTADOAPLICACION' VALUE :NEW.ESTADOAPLICACION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOAPLICACION' VALUE :NEW.CODIGOAPLICACION, 'TIPOLOCALIDAD' VALUE :NEW.TIPOLOCALIDAD, 'NOMBREAPLICACION' VALUE :NEW.NOMBREAPLICACION, 'ESTADOAPLICACION' VALUE :NEW.ESTADOAPLICACION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOAPLICACION' VALUE :OLD.CODIGOAPLICACION, 'TIPOLOCALIDAD' VALUE :OLD.TIPOLOCALIDAD, 'NOMBREAPLICACION' VALUE :OLD.NOMBREAPLICACION, 'ESTADOAPLICACION' VALUE :OLD.ESTADOAPLICACION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('aplicacionFuncion_type', v_pk, v_event, v_payload, 'FCME_USER.APLICACIONFUNCION_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_AUDITORIAFLUJO  ON FCME_USER.AUDITORIAFLUJO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_AUDITORIAFLUJO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.AUDITORIAFLUJO_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'CODIGOSUBPROCESO' VALUE :NEW.CODIGOSUBPROCESO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOPROCESO' VALUE :NEW.CODIGOPROCESO, 'CODIGOSUBPROCESO' VALUE :NEW.CODIGOSUBPROCESO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOPROCESO' VALUE :OLD.CODIGOPROCESO, 'CODIGOSUBPROCESO' VALUE :OLD.CODIGOSUBPROCESO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('auditoriaFlujo_type', v_pk, v_event, v_payload, 'FCME_USER.AUDITORIAFLUJO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CUENTANOSTRO  ON FCME_USER.CUENTANOSTRO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CUENTANOSTRO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.CUENTANOSTRO_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO' VALUE :NEW.CODIGO, 'NOMBRE' VALUE :NEW.NOMBRE, 'ESTADO' VALUE :NEW.ESTADO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGO' VALUE :NEW.CODIGO, 'NOMBRE' VALUE :NEW.NOMBRE, 'ESTADO' VALUE :NEW.ESTADO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGO' VALUE :OLD.CODIGO, 'NOMBRE' VALUE :OLD.NOMBRE, 'ESTADO' VALUE :OLD.ESTADO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cuentaNostroType', v_pk, v_event, v_payload, 'FCME_USER.CUENTANOSTRO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_EMPRESA  ON FCME_USER.EMPRESA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_EMPRESA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.EMPRESA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'NOMBREEMPRESA' VALUE :NEW.NOMBREEMPRESA, 'ESTADOEMPRESA' VALUE :NEW.ESTADOEMPRESA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'NOMBREEMPRESA' VALUE :NEW.NOMBREEMPRESA, 'ESTADOEMPRESA' VALUE :NEW.ESTADOEMPRESA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'NOMBREEMPRESA' VALUE :OLD.NOMBREEMPRESA, 'ESTADOEMPRESA' VALUE :OLD.ESTADOEMPRESA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('empresa_type', v_pk, v_event, v_payload, 'FCME_USER.EMPRESA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_FIRMASEGURIDAD  ON FCME_USER.FIRMASEGURIDAD_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_FIRMASEGURIDAD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.FIRMASEGURIDAD_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'FECHAFIRMAENTRADASALIDA' VALUE :NEW.FECHAFIRMAENTRADASALIDA, 'HORAFIRMAENTRADASALIDA' VALUE :NEW.HORAFIRMAENTRADASALIDA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'FECHAFIRMAENTRADASALIDA' VALUE :NEW.FECHAFIRMAENTRADASALIDA, 'HORAFIRMAENTRADASALIDA' VALUE :NEW.HORAFIRMAENTRADASALIDA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOUSUARIO' VALUE :OLD.CODIGOUSUARIO, 'FECHAFIRMAENTRADASALIDA' VALUE :OLD.FECHAFIRMAENTRADASALIDA, 'HORAFIRMAENTRADASALIDA' VALUE :OLD.HORAFIRMAENTRADASALIDA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('firmaSeguridad_type', v_pk, v_event, v_payload, 'FCME_USER.FIRMASEGURIDAD_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_FONDOSEGURIDAD  ON FCME_USER.FONDOSEGURIDAD_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_FONDOSEGURIDAD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.FONDOSEGURIDAD_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'NOMBREFONDO' VALUE :NEW.NOMBREFONDO, 'ESTADOFONDO' VALUE :NEW.ESTADOFONDO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOFONDO' VALUE :NEW.CODIGOFONDO, 'NOMBREFONDO' VALUE :NEW.NOMBREFONDO, 'ESTADOFONDO' VALUE :NEW.ESTADOFONDO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOFONDO' VALUE :OLD.CODIGOFONDO, 'NOMBREFONDO' VALUE :OLD.NOMBREFONDO, 'ESTADOFONDO' VALUE :OLD.ESTADOFONDO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('fondoSeguridad_type', v_pk, v_event, v_payload, 'FCME_USER.FONDOSEGURIDAD_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_LOCALIDAD  ON FCME_USER.LOCALIDAD_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_LOCALIDAD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.LOCALIDAD_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOLOCALIDAD' VALUE :NEW.CODIGOLOCALIDAD, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CEDULAREPRESENTANTE' VALUE :NEW.CEDULAREPRESENTANTE);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOLOCALIDAD' VALUE :NEW.CODIGOLOCALIDAD, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA, 'CEDULAREPRESENTANTE' VALUE :NEW.CEDULAREPRESENTANTE);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOLOCALIDAD' VALUE :OLD.CODIGOLOCALIDAD, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA, 'CEDULAREPRESENTANTE' VALUE :OLD.CEDULAREPRESENTANTE);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('localidad_type', v_pk, v_event, v_payload, 'FCME_USER.LOCALIDAD_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PARAMETROSEGURIDAD  ON FCME_USER.PARAMETROSEGURIDAD_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PARAMETROSEGURIDAD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PARAMETROSEGURIDAD_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOPARAMETRO' VALUE :NEW.CODIGOPARAMETRO, 'NOMBREPARAMETRO' VALUE :NEW.NOMBREPARAMETRO, 'VALORPARAMETRO' VALUE :NEW.VALORPARAMETRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOPARAMETRO' VALUE :NEW.CODIGOPARAMETRO, 'NOMBREPARAMETRO' VALUE :NEW.NOMBREPARAMETRO, 'VALORPARAMETRO' VALUE :NEW.VALORPARAMETRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOPARAMETRO' VALUE :OLD.CODIGOPARAMETRO, 'NOMBREPARAMETRO' VALUE :OLD.NOMBREPARAMETRO, 'VALORPARAMETRO' VALUE :OLD.VALORPARAMETRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('parametroSeguridad_type', v_pk, v_event, v_payload, 'FCME_USER.PARAMETROSEGURIDAD_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PASSWORDSEGURIDAD  ON FCME_USER.PASSWORDSEGURIDAD_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PASSWORDSEGURIDAD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PASSWORDSEGURIDAD_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'SECUENCIACAMBIOCONTRASENIA' VALUE :NEW.SECUENCIACAMBIOCONTRASENIA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'SECUENCIACAMBIOCONTRASENIA' VALUE :NEW.SECUENCIACAMBIOCONTRASENIA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOUSUARIO' VALUE :OLD.CODIGOUSUARIO, 'SECUENCIACAMBIOCONTRASENIA' VALUE :OLD.SECUENCIACAMBIOCONTRASENIA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('passwordSeguridad_type', v_pk, v_event, v_payload, 'FCME_USER.PASSWORDSEGURIDAD_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_USUARIOSEGURIDAD  ON FCME_USER.USUARIOSEGURIDAD_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_USUARIOSEGURIDAD
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.USUARIOSEGURIDAD_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOUSUARIO' VALUE :OLD.CODIGOUSUARIO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('usuarioSeguridad_type', v_pk, v_event, v_payload, 'FCME_USER.USUARIOSEGURIDAD_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_USUARIOSERVICIO  ON FCME_USER.USUARIOSERVICIO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_USUARIOSERVICIO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.USUARIOSERVICIO_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'CONTRASENIA' VALUE :NEW.CONTRASENIA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOUSUARIO' VALUE :NEW.CODIGOUSUARIO, 'CONTRASENIA' VALUE :NEW.CONTRASENIA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOUSUARIO' VALUE :OLD.CODIGOUSUARIO, 'CONTRASENIA' VALUE :OLD.CONTRASENIA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('usuarioServicio_type', v_pk, v_event, v_payload, 'FCME_USER.USUARIOSERVICIO_TYPE', SYSTIMESTAMP);
END;
/
