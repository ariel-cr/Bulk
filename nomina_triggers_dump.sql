/* ============================================================
   DUMP TRIGGERS NOMINA (F1 + F2) - DDL completo
   Snapshot generado del estado actual de las BDs
   ============================================================ */

/* ############################################################
   FLUJO 1 - Nomina dbNO -> fcme_canonicos.cdc_outbox
   Filtro: triggers cuya definicion publica un aggregate_type de Nomina
   ############################################################ */

USE [dbNO];
GO

/* TOTAL F1 (dbNO) Nomina: 21 triggers */

/* --- trg_outbox_notbcant  ON dbo.notbcant  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbcant', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbcant;
GO
CREATE TRIGGER dbo.trg_outbox_notbcant
ON dbo.[notbcant]
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
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[nu_anio]),CONVERT(NVARCHAR(200), i.[sc_anti]),CONVERT(NVARCHAR(200), i.[co_empl])),
            N'anticipoNominaType',
            @op,
            (SELECT x.[co_empr],x.[nu_anio],x.[sc_anti],x.[co_empl],x.[fe_rol_idst],x.[mo_soli],x.[mo_dese],x.[in_autr] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[nu_anio]=i.[nu_anio] AND x.[sc_anti]=i.[sc_anti] AND x.[co_empl]=i.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcant',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[nu_anio]),CONVERT(NVARCHAR(200), d.[sc_anti]),CONVERT(NVARCHAR(200), d.[co_empl])),
            N'anticipoNominaType',
            N'DELETE',
            (SELECT x.[co_empr],x.[nu_anio],x.[sc_anti],x.[co_empl],x.[fe_rol_idst],x.[mo_soli],x.[mo_dese],x.[in_autr] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[nu_anio]=d.[nu_anio] AND x.[sc_anti]=d.[sc_anti] AND x.[co_empl]=d.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcant',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_notbcarg  ON dbo.notbcarg  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbcarg', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbcarg;
GO
CREATE TRIGGER dbo.trg_outbox_notbcarg
ON dbo.[notbcarg]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_carg])), N'cargoGeneralType', @op,
            (SELECT x.[co_empr],x.[co_carg],x.[no_carg],x.[co_carg_iess],x.[mo_suel] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_carg]=i.[co_carg] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcarg', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_carg])), N'cargoGeneralType', N'DELETE',
            (SELECT x.[co_empr],x.[co_carg],x.[no_carg],x.[co_carg_iess],x.[mo_suel] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_carg]=d.[co_carg] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcarg', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbcarg_admi  ON dbo.notbcarg_admi  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbcarg_admi', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbcarg_admi;
GO
CREATE TRIGGER dbo.trg_outbox_notbcarg_admi
ON dbo.[notbcarg_admi]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_carg_admi])), N'cargoLaboralType', @op,
            (SELECT x.[co_empr],x.[co_carg_admi],x.[ds_carg_admi],x.[st_regi] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_carg_admi]=i.[co_carg_admi] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcarg_admi', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_carg_admi])), N'cargoLaboralType', N'DELETE',
            (SELECT x.[co_empr],x.[co_carg_admi],x.[ds_carg_admi],x.[st_regi] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_carg_admi]=d.[co_carg_admi] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcarg_admi', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbcgfm_carga  ON dbo.notbcgfm  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbcgfm_carga', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbcgfm_carga;
GO
CREATE TRIGGER dbo.trg_outbox_notbcgfm_carga
ON dbo.[notbcgfm]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_empl]),CONVERT(NVARCHAR(200), i.[sc_cgfm])), N'cargaFamiliarType', @op,
            (SELECT x.[co_empr],x.[co_empl],x.[sc_cgfm],x.[ti_rela],x.[no_nomb] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_empl]=i.[co_empl] AND x.[sc_cgfm]=i.[sc_cgfm] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcgfm', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_empl]),CONVERT(NVARCHAR(200), d.[sc_cgfm])), N'cargaFamiliarType', N'DELETE',
            (SELECT x.[co_empr],x.[co_empl],x.[sc_cgfm],x.[ti_rela],x.[no_nomb] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_empl]=d.[co_empl] AND x.[sc_cgfm]=d.[sc_cgfm] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcgfm', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbcnom  ON dbo.notbcnom  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbcnom', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbcnom;
GO
CREATE TRIGGER dbo.trg_outbox_notbcnom
ON dbo.[notbcnom]
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
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_nomi])),
            N'catalogoNominaType',
            @op,
            (SELECT x.[co_empr],x.[co_nomi],x.[no_nomi],x.[nu_peri_pago],x.[ti_pago],x.[st_nomi] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_nomi]=i.[co_nomi] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcnom',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_nomi])),
            N'catalogoNominaType',
            N'DELETE',
            (SELECT x.[co_empr],x.[co_nomi],x.[no_nomi],x.[nu_peri_pago],x.[ti_pago],x.[st_nomi] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_nomi]=d.[co_nomi] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcnom',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_notbcrol  ON dbo.notbcrol  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbcrol', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbcrol;
GO
CREATE TRIGGER dbo.trg_outbox_notbcrol
ON dbo.[notbcrol]
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
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[nu_rol])),
            N'nominaCabeceraType',
            @op,
            (SELECT x.[co_empr],x.[nu_rol],x.[co_nomi],x.[nu_peri_pago],x.[fe_gene],x.[fe_ingr],x.[fe_conf] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[nu_rol]=i.[nu_rol] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcrol',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[nu_rol])),
            N'nominaCabeceraType',
            N'DELETE',
            (SELECT x.[co_empr],x.[nu_rol],x.[co_nomi],x.[nu_peri_pago],x.[fe_gene],x.[fe_ingr],x.[fe_conf] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[nu_rol]=d.[nu_rol] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcrol',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_notbcvia  ON dbo.notbcvia  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbcvia', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbcvia;
GO
CREATE TRIGGER dbo.trg_outbox_notbcvia
ON dbo.[notbcvia]
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
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_empl]),CONVERT(NVARCHAR(200), i.[sc_viat])),
            N'viaticoNominaType',
            @op,
            (SELECT x.[co_empr],x.[co_empl],x.[sc_viat],x.[st_viat],x.[mo_viat],x.[fe_ingr] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_empl]=i.[co_empl] AND x.[sc_viat]=i.[sc_viat] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcvia',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_empl]),CONVERT(NVARCHAR(200), d.[sc_viat])),
            N'viaticoNominaType',
            N'DELETE',
            (SELECT x.[co_empr],x.[co_empl],x.[sc_viat],x.[st_viat],x.[mo_viat],x.[fe_ingr] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_empl]=d.[co_empl] AND x.[sc_viat]=d.[sc_viat] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbcvia',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_notbdrol  ON dbo.notbdrol  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbdrol', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbdrol;
GO
CREATE TRIGGER dbo.trg_outbox_notbdrol
ON dbo.[notbdrol]
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
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[nu_rol]),CONVERT(NVARCHAR(200), i.[co_empl]),CONVERT(NVARCHAR(200), i.[co_rubr])),
            N'rolPagoType',
            @op,
            (SELECT x.[co_empr],x.[nu_rol],x.[co_empl],x.[co_rubr],x.[mo_rol_pago] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[nu_rol]=i.[nu_rol] AND x.[co_empl]=i.[co_empl] AND x.[co_rubr]=i.[co_rubr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbdrol',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[nu_rol]),CONVERT(NVARCHAR(200), d.[co_empl]),CONVERT(NVARCHAR(200), d.[co_rubr])),
            N'rolPagoType',
            N'DELETE',
            (SELECT x.[co_empr],x.[nu_rol],x.[co_empl],x.[co_rubr],x.[mo_rol_pago] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[nu_rol]=d.[nu_rol] AND x.[co_empl]=d.[co_empl] AND x.[co_rubr]=d.[co_rubr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbdrol',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_notbempl_empleado  ON dbo.notbempl  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbempl_empleado', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbempl_empleado;
GO
CREATE TRIGGER dbo.trg_outbox_notbempl_empleado
ON dbo.[notbempl]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_empl])), N'empleadoType', @op,
            (SELECT x.[co_empr],x.[co_empl],x.[no_empl],x.[no_dire],x.[co_carg] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_empl]=i.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_empl])), N'empleadoType', N'DELETE',
            (SELECT x.[co_empr],x.[co_empl],x.[no_empl],x.[no_dire],x.[co_carg] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_empl]=d.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbempl_audi  ON dbo.notbempl_audi  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbempl_audi', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbempl_audi;
GO
CREATE TRIGGER dbo.trg_outbox_notbempl_audi
ON dbo.[notbempl_audi]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_empl]),CONVERT(NVARCHAR(200), i.[fe_ingr]),CONVERT(NVARCHAR(200), i.[ho_ingr])), N'empleadoAuditoriaType', @op,
            (SELECT x.[co_empr],x.[co_empl],x.[fe_ingr],x.[ho_ingr],x.[ti_cont] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_empl]=i.[co_empl] AND x.[fe_ingr]=i.[fe_ingr] AND x.[ho_ingr]=i.[ho_ingr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl_audi', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_empl]),CONVERT(NVARCHAR(200), d.[fe_ingr]),CONVERT(NVARCHAR(200), d.[ho_ingr])), N'empleadoAuditoriaType', N'DELETE',
            (SELECT x.[co_empr],x.[co_empl],x.[fe_ingr],x.[ho_ingr],x.[ti_cont] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_empl]=d.[co_empl] AND x.[fe_ingr]=d.[fe_ingr] AND x.[ho_ingr]=d.[ho_ingr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl_audi', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbempl_deta  ON dbo.notbempl_deta  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbempl_deta', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbempl_deta;
GO
CREATE TRIGGER dbo.trg_outbox_notbempl_deta
ON dbo.[notbempl_deta]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_empl])), N'empleadoDetalleType', @op,
            (SELECT x.[co_empr],x.[co_empl],x.[ti_cont],x.[in_prue],x.[ti_peri_cont] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_empl]=i.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl_deta', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_empl])), N'empleadoDetalleType', N'DELETE',
            (SELECT x.[co_empr],x.[co_empl],x.[ti_cont],x.[in_prue],x.[ti_peri_cont] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_empl]=d.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbempl_deta', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbfirm  ON dbo.notbfirm  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbfirm', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbfirm;
GO
CREATE TRIGGER dbo.trg_outbox_notbfirm
ON dbo.[notbfirm]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_empl]),CONVERT(NVARCHAR(200), i.[sc_firm])), N'firmaHorarioType', @op,
            (SELECT x.[co_empr],x.[co_empl],x.[sc_firm],x.[ti_regi],x.[fe_firm] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_empl]=i.[co_empl] AND x.[sc_firm]=i.[sc_firm] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbfirm', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_empl]),CONVERT(NVARCHAR(200), d.[sc_firm])), N'firmaHorarioType', N'DELETE',
            (SELECT x.[co_empr],x.[co_empl],x.[sc_firm],x.[ti_regi],x.[fe_firm] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_empl]=d.[co_empl] AND x.[sc_firm]=d.[sc_firm] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbfirm', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbfond_rese  ON dbo.notbfond_rese  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbfond_rese', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbfond_rese;
GO
CREATE TRIGGER dbo.trg_outbox_notbfond_rese
ON dbo.[notbfond_rese]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_empl])), N'fondoReservaType', @op,
            (SELECT x.[co_empr],x.[co_empl],x.[ti_acre],x.[co_usua_ingr],x.[fe_ingr] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_empl]=i.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbfond_rese', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_empl])), N'fondoReservaType', N'DELETE',
            (SELECT x.[co_empr],x.[co_empl],x.[ti_acre],x.[co_usua_ingr],x.[fe_ingr] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_empl]=d.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbfond_rese', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbhieg  ON dbo.notbhieg  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbhieg', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbhieg;
GO
CREATE TRIGGER dbo.trg_outbox_notbhieg
ON dbo.[notbhieg]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_empl]),CONVERT(NVARCHAR(200), i.[nu_ano]),CONVERT(NVARCHAR(200), i.[nu_mes])), N'historialIngresoType', @op,
            (SELECT x.[co_empr],x.[co_empl],x.[nu_ano],x.[nu_mes],x.[mo_suel] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_empl]=i.[co_empl] AND x.[nu_ano]=i.[nu_ano] AND x.[nu_mes]=i.[nu_mes] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbhieg', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_empl]),CONVERT(NVARCHAR(200), d.[nu_ano]),CONVERT(NVARCHAR(200), d.[nu_mes])), N'historialIngresoType', N'DELETE',
            (SELECT x.[co_empr],x.[co_empl],x.[nu_ano],x.[nu_mes],x.[mo_suel] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_empl]=d.[co_empl] AND x.[nu_ano]=d.[nu_ano] AND x.[nu_mes]=d.[nu_mes] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbhieg', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbnive_acad_empl  ON dbo.notbnive_acad_empl  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbnive_acad_empl', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbnive_acad_empl;
GO
CREATE TRIGGER dbo.trg_outbox_notbnive_acad_empl
ON dbo.[notbnive_acad_empl]
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
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_empl])), N'nivelAcademicoType', @op,
            (SELECT x.[co_empr],x.[co_empl],x.[co_inst],x.[co_titu],x.[in_egre] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_empl]=i.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbnive_acad_empl', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_empl])), N'nivelAcademicoType', N'DELETE',
            (SELECT x.[co_empr],x.[co_empl],x.[co_inst],x.[co_titu],x.[in_egre] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_empl]=d.[co_empl] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbnive_acad_empl', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbpago_nomi  ON dbo.notbpago_nomi  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbpago_nomi', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbpago_nomi;
GO
CREATE TRIGGER dbo.trg_outbox_notbpago_nomi
ON dbo.[notbpago_nomi]
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
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[sc_rol]),CONVERT(NVARCHAR(200), i.[co_bene]),CONVERT(NVARCHAR(200), i.[rf_pago]),CONVERT(NVARCHAR(200), i.[mo_pago]),CONVERT(NVARCHAR(200), i.[sc_deta])),
            N'pagoNominaType',
            @op,
            (SELECT x.[co_empr],x.[sc_rol],x.[fe_rol],x.[nu_rol],x.[co_orig],x.[ci_bnco],x.[mo_pago] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[sc_rol]=i.[sc_rol] AND x.[co_bene]=i.[co_bene] AND x.[rf_pago]=i.[rf_pago] AND x.[mo_pago]=i.[mo_pago] AND x.[sc_deta]=i.[sc_deta] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbpago_nomi',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[sc_rol]),CONVERT(NVARCHAR(200), d.[co_bene]),CONVERT(NVARCHAR(200), d.[rf_pago]),CONVERT(NVARCHAR(200), d.[mo_pago]),CONVERT(NVARCHAR(200), d.[sc_deta])),
            N'pagoNominaType',
            N'DELETE',
            (SELECT x.[co_empr],x.[sc_rol],x.[fe_rol],x.[nu_rol],x.[co_orig],x.[ci_bnco],x.[mo_pago] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[sc_rol]=d.[sc_rol] AND x.[co_bene]=d.[co_bene] AND x.[rf_pago]=d.[rf_pago] AND x.[mo_pago]=d.[mo_pago] AND x.[sc_deta]=d.[sc_deta] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbpago_nomi',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_notbpara  ON dbo.notbpara  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbpara', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbpara;
GO
CREATE TRIGGER dbo.trg_outbox_notbpara
ON dbo.[notbpara]
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
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_frec_pago_rol])),
            N'configuracionNominaType',
            @op,
            (SELECT x.[co_empr],x.[co_frec_pago_rol],x.[qs_cnta_bnco],x.[co_rubr_pres],x.[co_rubr_sobg] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_frec_pago_rol]=i.[co_frec_pago_rol] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbpara',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_frec_pago_rol])),
            N'configuracionNominaType',
            N'DELETE',
            (SELECT x.[co_empr],x.[co_frec_pago_rol],x.[qs_cnta_bnco],x.[co_rubr_pres],x.[co_rubr_sobg] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_frec_pago_rol]=d.[co_frec_pago_rol] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbpara',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_notbpara_gene  ON dbo.notbpara_gene  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbpara_gene', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbpara_gene;
GO
CREATE TRIGGER dbo.trg_outbox_notbpara_gene
ON dbo.[notbpara_gene]
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
        SELECT CONVERT(NVARCHAR(200), i.co_para), N'parametroNominaType', @op,
            (SELECT x.co_para, x.co_tipo_para, x.ti_valo, x.ds_par1, x.ds_par2, x.st_regi FROM inserted x WHERE x.co_para=i.co_para FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbpara_gene', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.co_para), N'parametroNominaType', N'DELETE',
            (SELECT x.co_para, x.co_tipo_para, x.ti_valo, x.ds_par1, x.ds_par2, x.st_regi FROM deleted x WHERE x.co_para=d.co_para FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbpara_gene', SYSUTCDATETIME()
        FROM deleted d;
END
GO

/* --- trg_outbox_notbpatr  ON dbo.notbpatr  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbpatr', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbpatr;
GO
CREATE TRIGGER dbo.trg_outbox_notbpatr
ON dbo.[notbpatr]
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
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[nu_patr])),
            N'patronalNominaType',
            @op,
            (SELECT x.[co_empr],x.[nu_patr],x.[no_patr],x.[ti_iden_patr],x.[nu_iden_patr] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[nu_patr]=i.[nu_patr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbpatr',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[nu_patr])),
            N'patronalNominaType',
            N'DELETE',
            (SELECT x.[co_empr],x.[nu_patr],x.[no_patr],x.[ti_iden_patr],x.[nu_iden_patr] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[nu_patr]=d.[nu_patr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbpatr',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_notbrubr  ON dbo.notbrubr  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbrubr', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbrubr;
GO
CREATE TRIGGER dbo.trg_outbox_notbrubr
ON dbo.[notbrubr]
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
            CONCAT_WS('|',CONVERT(NVARCHAR(200), i.[co_empr]),CONVERT(NVARCHAR(200), i.[co_rubr])),
            N'rubroNominaType',
            @op,
            (SELECT x.[co_empr],x.[co_rubr],x.[no_rubr_abre],x.[no_rubr],x.[fe_ingr],x.[in_dbcr],x.[ti_rubr] FROM inserted x WHERE x.[co_empr]=i.[co_empr] AND x.[co_rubr]=i.[co_rubr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbrubr',
            SYSUTCDATETIME()
        FROM inserted i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONCAT_WS('|',CONVERT(NVARCHAR(200), d.[co_empr]),CONVERT(NVARCHAR(200), d.[co_rubr])),
            N'rubroNominaType',
            N'DELETE',
            (SELECT x.[co_empr],x.[co_rubr],x.[no_rubr_abre],x.[no_rubr],x.[fe_ingr],x.[in_dbcr],x.[ti_rubr] FROM deleted x WHERE x.[co_empr]=d.[co_empr] AND x.[co_rubr]=d.[co_rubr] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbrubr',
            SYSUTCDATETIME()
        FROM deleted d;
    END
END
GO

/* --- trg_outbox_notbsect_iess  ON dbo.notbsect_iess  disabled=False --- */
IF OBJECT_ID(N'dbo.trg_outbox_notbsect_iess', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbsect_iess;
GO
CREATE TRIGGER dbo.trg_outbox_notbsect_iess
ON dbo.[notbsect_iess]
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
        SELECT CONVERT(NVARCHAR(200), i.[co_sect]), N'sectorIessType', @op,
            (SELECT x.[co_sect],x.[co_sect_iess],x.[ds_sect_iess],x.[co_tact_sect],x.[co_estr_ocup] FROM inserted x WHERE x.[co_sect]=i.[co_sect] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbsect_iess', SYSUTCDATETIME()
        FROM inserted i;
    ELSE
        INSERT INTO fcme_canonicos.dbo.cdc_outbox (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT CONVERT(NVARCHAR(200), d.[co_sect]), N'sectorIessType', N'DELETE',
            (SELECT x.[co_sect],x.[co_sect_iess],x.[ds_sect_iess],x.[co_tact_sect],x.[co_estr_ocup] FROM deleted x WHERE x.[co_sect]=d.[co_sect] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbNO.dbo.notbsect_iess', SYSUTCDATETIME()
        FROM deleted d;
END
GO


/* ############################################################
   FLUJO 2 - Nomina FCME_USER -> FCME_USER.CDC_OUTBOX
   ############################################################ */

/* TOTAL F2 (FCME_USER) Nomina: 21 triggers */

/* --- TRG_OUTBOX_ANTICIPONOMINA  ON FCME_USER.ANTICIPONOMINA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ANTICIPONOMINA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.ANTICIPONOMINA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'ANIO' VALUE :NEW.ANIO, 'SECUENCIAANTICIPO' VALUE :NEW.SECUENCIAANTICIPO, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'ANIO' VALUE :NEW.ANIO, 'SECUENCIAANTICIPO' VALUE :NEW.SECUENCIAANTICIPO, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'ANIO' VALUE :OLD.ANIO, 'SECUENCIAANTICIPO' VALUE :OLD.SECUENCIAANTICIPO, 'CODIGOEMPLEADO' VALUE :OLD.CODIGOEMPLEADO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('anticipoNominaType', v_pk, v_event, v_payload, 'FCME_USER.ANTICIPONOMINA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CARGAFAMILIAR  ON FCME_USER.CARGAFAMILIAR_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CARGAFAMILIAR
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.CARGAFAMILIAR_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :OLD.CODIGOEMPLEADO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cargaFamiliarType', v_pk, v_event, v_payload, 'FCME_USER.CARGAFAMILIAR_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CARGOGENERAL  ON FCME_USER.CARGOGENERAL_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CARGOGENERAL
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.CARGOGENERAL_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGONOMINA' VALUE :NEW.CODIGONOMINA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGONOMINA' VALUE :NEW.CODIGONOMINA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGONOMINA' VALUE :OLD.CODIGONOMINA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cargoGeneralType', v_pk, v_event, v_payload, 'FCME_USER.CARGOGENERAL_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CARGOLABORAL  ON FCME_USER.CARGOLABORAL_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CARGOLABORAL
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.CARGOLABORAL_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOCARGADMINISTRADOR' VALUE :NEW.CODIGOCARGADMINISTRADOR);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOCARGADMINISTRADOR' VALUE :NEW.CODIGOCARGADMINISTRADOR);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOCARGADMINISTRADOR' VALUE :OLD.CODIGOCARGADMINISTRADOR);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('cargoLaboralType', v_pk, v_event, v_payload, 'FCME_USER.CARGOLABORAL_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CATALOGONOMINA  ON FCME_USER.CATALOGONOMINA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CATALOGONOMINA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.CATALOGONOMINA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOMOTIVOAUDITORIA' VALUE :NEW.CODIGOMOTIVOAUDITORIA, 'DESCRIPCIONADICIONAL' VALUE :NEW.DESCRIPCIONADICIONAL, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOMOTIVOAUDITORIA' VALUE :NEW.CODIGOMOTIVOAUDITORIA, 'DESCRIPCIONADICIONAL' VALUE :NEW.DESCRIPCIONADICIONAL, 'ESTADOREGISTRO' VALUE :NEW.ESTADOREGISTRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOMOTIVOAUDITORIA' VALUE :OLD.CODIGOMOTIVOAUDITORIA, 'DESCRIPCIONADICIONAL' VALUE :OLD.DESCRIPCIONADICIONAL, 'ESTADOREGISTRO' VALUE :OLD.ESTADOREGISTRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('catalogoNominaType', v_pk, v_event, v_payload, 'FCME_USER.CATALOGONOMINA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_CONFIGNOMINA  ON FCME_USER.CONFIGURACIONNOMINA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_CONFIGNOMINA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.CONFIGURACIONNOMINA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'NOMBREINSTITUCION' VALUE :NEW.NOMBREINSTITUCION);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'NOMBREINSTITUCION' VALUE :NEW.NOMBREINSTITUCION);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOINSTITUCION' VALUE :OLD.CODIGOINSTITUCION, 'NOMBREINSTITUCION' VALUE :OLD.NOMBREINSTITUCION);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('configuracionNominaType', v_pk, v_event, v_payload, 'FCME_USER.CONFIGURACIONNOMINA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_EMPLEADOAUDITORIA  ON FCME_USER.EMPLEADOAUDITORIA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_EMPLEADOAUDITORIA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.EMPLEADOAUDITORIA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO, 'FECHAINGRESOEMPLEADO' VALUE :NEW.FECHAINGRESOEMPLEADO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO, 'FECHAINGRESOEMPLEADO' VALUE :NEW.FECHAINGRESOEMPLEADO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :OLD.CODIGOEMPLEADO, 'FECHAINGRESOEMPLEADO' VALUE :OLD.FECHAINGRESOEMPLEADO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('empleadoAuditoriaType', v_pk, v_event, v_payload, 'FCME_USER.EMPLEADOAUDITORIA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_EMPLEADODETALLE  ON FCME_USER.EMPLEADODETALLE_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_EMPLEADODETALLE
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.EMPLEADODETALLE_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPL' VALUE :NEW.CODIGOEMPL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPL' VALUE :NEW.CODIGOEMPL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOEMPL' VALUE :OLD.CODIGOEMPL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('empleadoDetalleType', v_pk, v_event, v_payload, 'FCME_USER.EMPLEADODETALLE_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_EMPLEADO  ON FCME_USER.EMPLEADO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_EMPLEADO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.EMPLEADO_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCARGO' VALUE :NEW.CODIGOCARGO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOCARGO' VALUE :NEW.CODIGOCARGO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOCARGO' VALUE :OLD.CODIGOCARGO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('empleadoType', v_pk, v_event, v_payload, 'FCME_USER.EMPLEADO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_FIRMAHORARIO  ON FCME_USER.FIRMAHORARIO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_FIRMAHORARIO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.FIRMAHORARIO_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :OLD.CODIGOEMPLEADO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('firmaHorarioType', v_pk, v_event, v_payload, 'FCME_USER.FIRMAHORARIO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_FONDORESERVA  ON FCME_USER.FONDORESERVA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_FONDORESERVA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.FONDORESERVA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO, 'TIPOACREDITACIONFONDORESERVA' VALUE :NEW.TIPOACREDITACIONFONDORESERVA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO, 'TIPOACREDITACIONFONDORESERVA' VALUE :NEW.TIPOACREDITACIONFONDORESERVA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :OLD.CODIGOEMPLEADO, 'TIPOACREDITACIONFONDORESERVA' VALUE :OLD.TIPOACREDITACIONFONDORESERVA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('fondoReservaType', v_pk, v_event, v_payload, 'FCME_USER.FONDORESERVA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_HISTORIALINGRESO  ON FCME_USER.HISTORIALINGRESO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_HISTORIALINGRESO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.HISTORIALINGRESO_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO, 'ANIO' VALUE :NEW.ANIO, 'MES' VALUE :NEW.MES);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO, 'ANIO' VALUE :NEW.ANIO, 'MES' VALUE :NEW.MES);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :OLD.CODIGOEMPLEADO, 'ANIO' VALUE :OLD.ANIO, 'MES' VALUE :OLD.MES);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('historialIngresoType', v_pk, v_event, v_payload, 'FCME_USER.HISTORIALINGRESO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_NIVELACADEMICO  ON FCME_USER.NIVELACADEMICO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_NIVELACADEMICO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.NIVELACADEMICO_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'CODIGOTITULO' VALUE :NEW.CODIGOTITULO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO, 'CODIGOINSTITUCION' VALUE :NEW.CODIGOINSTITUCION, 'CODIGOTITULO' VALUE :NEW.CODIGOTITULO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :OLD.CODIGOEMPLEADO, 'CODIGOINSTITUCION' VALUE :OLD.CODIGOINSTITUCION, 'CODIGOTITULO' VALUE :OLD.CODIGOTITULO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('nivelAcademicoType', v_pk, v_event, v_payload, 'FCME_USER.NIVELACADEMICO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_NOMINACABECERA  ON FCME_USER.NOMINACABECERA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_NOMINACABECERA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.NOMINACABECERA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGONOMINA' VALUE :NEW.CODIGONOMINA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGONOMINA' VALUE :NEW.CODIGONOMINA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGONOMINA' VALUE :OLD.CODIGONOMINA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('nominaCabeceraType', v_pk, v_event, v_payload, 'FCME_USER.NOMINACABECERA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PAGONOMINA  ON FCME_USER.PAGONOMINA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PAGONOMINA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PAGONOMINA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CEDULABENEFICIARIO' VALUE :NEW.CEDULABENEFICIARIO, 'CODIGOBANCO' VALUE :NEW.CODIGOBANCO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CEDULABENEFICIARIO' VALUE :NEW.CEDULABENEFICIARIO, 'CODIGOBANCO' VALUE :NEW.CODIGOBANCO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CEDULABENEFICIARIO' VALUE :OLD.CEDULABENEFICIARIO, 'CODIGOBANCO' VALUE :OLD.CODIGOBANCO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('pagoNominaType', v_pk, v_event, v_payload, 'FCME_USER.PAGONOMINA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PARAMETRONOMINA  ON FCME_USER.PARAMETRONOMINA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PARAMETRONOMINA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PARAMETRONOMINA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFRECUENCIAPAGOROL' VALUE :NEW.CODIGOFRECUENCIAPAGOROL);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOFRECUENCIAPAGOROL' VALUE :NEW.CODIGOFRECUENCIAPAGOROL);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOFRECUENCIAPAGOROL' VALUE :OLD.CODIGOFRECUENCIAPAGOROL);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('parametroNominaType', v_pk, v_event, v_payload, 'FCME_USER.PARAMETRONOMINA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_PATRONALNOMINA  ON FCME_USER.PATRONALNOMINA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_PATRONALNOMINA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.PATRONALNOMINA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOPROVINCIA' VALUE :NEW.CODIGOPROVINCIA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOPROVINCIA' VALUE :OLD.CODIGOPROVINCIA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('patronalNominaType', v_pk, v_event, v_payload, 'FCME_USER.PATRONALNOMINA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_ROLPAGO  ON FCME_USER.ROLPAGO_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_ROLPAGO
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.ROLPAGO_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGONOMINA' VALUE :NEW.CODIGONOMINA);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGONOMINA' VALUE :NEW.CODIGONOMINA);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGONOMINA' VALUE :OLD.CODIGONOMINA);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('rolPagoType', v_pk, v_event, v_payload, 'FCME_USER.ROLPAGO_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_RUBRONOMINA  ON FCME_USER.RUBRONOMINA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_RUBRONOMINA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.RUBRONOMINA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGORUBRO' VALUE :NEW.CODIGORUBRO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGORUBRO' VALUE :OLD.CODIGORUBRO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('rubroNominaType', v_pk, v_event, v_payload, 'FCME_USER.RUBRONOMINA_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_SECTORIESS  ON FCME_USER.SECTORIESS_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_SECTORIESS
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.SECTORIESS_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSECTOR' VALUE :NEW.CODIGOSECTOR, 'CODIGOGESTIONIESS' VALUE :NEW.CODIGOGESTIONIESS, 'DESCRIPCIONSECTORIESS' VALUE :NEW.DESCRIPCIONSECTORIESS);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOSECTOR' VALUE :NEW.CODIGOSECTOR, 'CODIGOGESTIONIESS' VALUE :NEW.CODIGOGESTIONIESS, 'DESCRIPCIONSECTORIESS' VALUE :NEW.DESCRIPCIONSECTORIESS);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOSECTOR' VALUE :OLD.CODIGOSECTOR, 'CODIGOGESTIONIESS' VALUE :OLD.CODIGOGESTIONIESS, 'DESCRIPCIONSECTORIESS' VALUE :OLD.DESCRIPCIONSECTORIESS);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('sectorIessType', v_pk, v_event, v_payload, 'FCME_USER.SECTORIESS_TYPE', SYSTIMESTAMP);
END;
/

/* --- TRG_OUTBOX_VIATICONOMINA  ON FCME_USER.VIATICONOMINA_TYPE  status=ENABLED --- */
CREATE OR REPLACE
TRIGGER FCME_USER.TRG_OUTBOX_VIATICONOMINA
AFTER INSERT OR UPDATE OR DELETE ON FCME_USER.VIATICONOMINA_TYPE
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
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO);
    ELSIF UPDATING THEN
        v_event := 'UPDATE';
        v_pk    := TO_CHAR(:NEW.ID);
        v_payload := JSON_OBJECT('ID' VALUE :NEW.ID, 'CODIGOEMPRESA' VALUE :NEW.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :NEW.CODIGOEMPLEADO);
    ELSE
        v_event := 'DELETE';
        v_pk    := TO_CHAR(:OLD.ID);
        v_payload := JSON_OBJECT('ID' VALUE :OLD.ID, 'CODIGOEMPRESA' VALUE :OLD.CODIGOEMPRESA, 'CODIGOEMPLEADO' VALUE :OLD.CODIGOEMPLEADO);
    END IF;
    INSERT INTO FCME_USER.CDC_OUTBOX (AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, PAYLOAD, SOURCE_TABLE, CREATED_AT)
    VALUES ('viaticoNominaType', v_pk, v_event, v_payload, 'FCME_USER.VIATICONOMINA_TYPE', SYSTIMESTAMP);
END;
/
