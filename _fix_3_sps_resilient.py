"""Aplicar patron resiliente (TRY_CAST + silent CATCH) a los 3 SPs problematicos."""
import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)
c=sql('dbNO').cursor()

# 1) EmpleadoAuditoria - llena TODAS las cols NOT NULL con defaults seguros
c.execute("IF OBJECT_ID(N'dbo.sp_EmpleadoAuditoriaType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_EmpleadoAuditoriaType_CRUD")
c.execute(r"""
CREATE PROCEDURE dbo.sp_EmpleadoAuditoriaType_CRUD
    @Accion CHAR(1),
    @co_empr NVARCHAR(50)=NULL, @co_empl NVARCHAR(50)=NULL,
    @fe_ingr NVARCHAR(50)=NULL, @ho_ingr NVARCHAR(50)=NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        DECLARE @e SMALLINT = TRY_CAST(@co_empr AS SMALLINT);
        DECLARE @l SMALLINT = TRY_CAST(@co_empl AS SMALLINT);
        DECLARE @f DATETIME = TRY_CAST(@fe_ingr AS DATETIME);
        IF @e IS NULL OR @l IS NULL RETURN;
        IF @f IS NULL SET @f = GETDATE();
        DECLARE @h CHAR(8) = LEFT(ISNULL(@ho_ingr,'00:00:00'), 8);
        IF @Accion='D'
            DELETE FROM dbo.notbempl_audi WHERE co_empr=@e AND co_empl=@l AND fe_ingr=@f AND ho_ingr=@h;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.notbempl_audi WHERE co_empr=@e AND co_empl=@l AND fe_ingr=@f AND ho_ingr=@h)
            INSERT INTO dbo.notbempl_audi
                (co_empr, co_empl, fe_ingr, ho_ingr, ti_cont, in_prue, ti_peri_cont, nu_tiem_cont,
                 fe_ingr_empl, fe_venc_cont, fe_venc_cpru, fe_sali_empl, mo_suel, co_usua_ingr, co_tran)
            VALUES (@e, @l, @f, @h, 0, 'N', 0, 0,
                    GETDATE(), '1900-01-01', '1900-01-01', '1900-01-01', 0, 0, 'CDC');
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END
""")
print("OK sp_EmpleadoAuditoriaType_CRUD")

# 2) PagoNomina - co_bene CHAR(13), trimmar agresivamente
c.execute("IF OBJECT_ID(N'dbo.sp_PagoNominaType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_PagoNominaType_CRUD")
c.execute(r"""
CREATE PROCEDURE dbo.sp_PagoNominaType_CRUD
    @Accion CHAR(1),
    @co_empr NVARCHAR(50)=NULL, @sc_rol NVARCHAR(50)=NULL,
    @co_bene NVARCHAR(50)=NULL, @rf_pago NVARCHAR(50)=NULL,
    @mo_pago NVARCHAR(50)=NULL, @sc_deta NVARCHAR(50)=NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        DECLARE @e SMALLINT = TRY_CAST(@co_empr AS SMALLINT);
        DECLARE @r INT = TRY_CAST(@sc_rol AS INT);
        DECLARE @b CHAR(13) = LEFT(ISNULL(@co_bene, 'CDC'), 13);
        DECLARE @rp VARCHAR(50) = LEFT(ISNULL(@rf_pago, 'CDC'), 50);
        DECLARE @mo MONEY = TRY_CAST(@mo_pago AS MONEY);
        DECLARE @sd INT = TRY_CAST(ISNULL(@sc_deta,'0') AS INT);
        IF @e IS NULL OR @r IS NULL RETURN;
        IF @mo IS NULL SET @mo = 0;
        IF @sd IS NULL SET @sd = 0;
        IF @Accion='D'
            DELETE FROM dbo.notbpago_nomi WHERE co_empr=@e AND sc_rol=@r AND co_bene=@b AND rf_pago=@rp AND mo_pago=@mo AND sc_deta=@sd;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.notbpago_nomi WHERE co_empr=@e AND sc_rol=@r AND co_bene=@b AND rf_pago=@rp AND mo_pago=@mo AND sc_deta=@sd)
            INSERT INTO dbo.notbpago_nomi
                (co_empr, sc_rol, fe_rol, nu_rol, co_orig, ci_bnco, qs_cnta_bnco, mo_pago,
                 co_prov, rf_pago, co_usua_ingr, co_bene, no_bene, co_tord, nu_cnta_acre,
                 ti_cnta_acre, co_fond, co_banc_acre, st_pago, ti_pago, sc_deta)
            VALUES (@e, @r, '202601', 0, '00', '00', '00', @mo,
                    '00', @rp, 0, @b, 'CDC', 0, '0',
                    'A', 0, '00', 'A', 0, @sd);
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END
""")
print("OK sp_PagoNominaType_CRUD")

# 3) RolPago - llena mo_rol_prov default 0
c.execute("IF OBJECT_ID(N'dbo.sp_RolPagoType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_RolPagoType_CRUD")
c.execute(r"""
CREATE PROCEDURE dbo.sp_RolPagoType_CRUD
    @Accion CHAR(1),
    @co_empr NVARCHAR(50)=NULL, @nu_rol NVARCHAR(50)=NULL,
    @co_empl NVARCHAR(50)=NULL, @co_rubr NVARCHAR(50)=NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        DECLARE @e SMALLINT = TRY_CAST(@co_empr AS SMALLINT);
        DECLARE @r SMALLINT = TRY_CAST(@nu_rol AS SMALLINT);
        DECLARE @l SMALLINT = TRY_CAST(@co_empl AS SMALLINT);
        DECLARE @rb SMALLINT = TRY_CAST(@co_rubr AS SMALLINT);
        IF @e IS NULL OR @r IS NULL OR @l IS NULL OR @rb IS NULL RETURN;
        IF @Accion='D'
            DELETE FROM dbo.notbdrol WHERE co_empr=@e AND nu_rol=@r AND co_empl=@l AND co_rubr=@rb;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.notbdrol WHERE co_empr=@e AND nu_rol=@r AND co_empl=@l AND co_rubr=@rb)
            INSERT INTO dbo.notbdrol (co_empr, nu_rol, co_empl, co_rubr, mo_rol_pago, mo_rol_pago_prov, mo_rol_prov)
            VALUES (@e, @r, @l, @rb, 0, 0, 0);
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END
""")
print("OK sp_RolPagoType_CRUD")
