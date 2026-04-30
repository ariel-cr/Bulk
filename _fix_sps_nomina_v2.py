"""Re-deploy 3 SPs CRUD con todos los cols NOT NULL llenados con defaults."""
import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)
c=sql('dbNO').cursor()

# 1) sp_EmpleadoAuditoriaType_CRUD
c.execute("IF OBJECT_ID(N'dbo.sp_EmpleadoAuditoriaType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_EmpleadoAuditoriaType_CRUD")
ddl_audi = r"""
CREATE PROCEDURE dbo.sp_EmpleadoAuditoriaType_CRUD
    @Accion CHAR(1),
    @co_empr NVARCHAR(50)=NULL, @co_empl NVARCHAR(50)=NULL,
    @fe_ingr NVARCHAR(50)=NULL, @ho_ingr NVARCHAR(50)=NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        IF @co_empr IS NULL OR @co_empl IS NULL OR @fe_ingr IS NULL OR @ho_ingr IS NULL RETURN;
        DECLARE @fe_ingr_dt DATETIME = TRY_CAST(@fe_ingr AS DATETIME);
        IF @fe_ingr_dt IS NULL SET @fe_ingr_dt = GETDATE();
        IF @Accion = 'D'
            DELETE FROM dbo.notbempl_audi WHERE co_empr=@co_empr AND co_empl=@co_empl AND fe_ingr=@fe_ingr_dt AND ho_ingr=@ho_ingr;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.notbempl_audi WHERE co_empr=@co_empr AND co_empl=@co_empl AND fe_ingr=@fe_ingr_dt AND ho_ingr=@ho_ingr)
            INSERT INTO dbo.notbempl_audi
                (co_empr, co_empl, fe_ingr, ho_ingr, ti_cont, in_prue, ti_peri_cont, nu_tiem_cont,
                 fe_ingr_empl, fe_venc_cont, fe_venc_cpru, fe_sali_empl, mo_suel, co_usua_ingr, co_tran)
            VALUES (@co_empr, @co_empl, @fe_ingr_dt, @ho_ingr, 0, 'N', 0, 0,
                    GETDATE(), '1900-01-01', '1900-01-01', '1900-01-01', 0, 0, 'CDC');
    END TRY
    BEGIN CATCH
        DECLARE @msg NVARCHAR(2048) = ERROR_MESSAGE();
        RAISERROR (@msg, 16, 1);
    END CATCH
END
"""
c.execute(ddl_audi)
print("OK sp_EmpleadoAuditoriaType_CRUD")

# 2) sp_PagoNominaType_CRUD - co_bene es CHAR(13), trimmar
c.execute("IF OBJECT_ID(N'dbo.sp_PagoNominaType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_PagoNominaType_CRUD")
ddl_pago = r"""
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
        IF @co_empr IS NULL OR @sc_rol IS NULL OR @co_bene IS NULL RETURN;
        SET @co_bene = LEFT(@co_bene, 13);
        SET @rf_pago = LEFT(ISNULL(@rf_pago, 'CDC'), 50);
        DECLARE @sc_rol_int INT = TRY_CAST(@sc_rol AS INT);
        DECLARE @sc_deta_int INT = TRY_CAST(ISNULL(@sc_deta, '0') AS INT);
        DECLARE @mo MONEY = TRY_CAST(@mo_pago AS MONEY);
        IF @mo IS NULL SET @mo = 0;
        IF @Accion = 'D'
            DELETE FROM dbo.notbpago_nomi WHERE co_empr=@co_empr AND sc_rol=@sc_rol_int AND co_bene=@co_bene AND rf_pago=@rf_pago AND mo_pago=@mo AND sc_deta=@sc_deta_int;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.notbpago_nomi WHERE co_empr=@co_empr AND sc_rol=@sc_rol_int AND co_bene=@co_bene AND rf_pago=@rf_pago AND mo_pago=@mo AND sc_deta=@sc_deta_int)
            INSERT INTO dbo.notbpago_nomi
                (co_empr, sc_rol, fe_rol, nu_rol, co_orig, ci_bnco, qs_cnta_bnco, mo_pago,
                 co_prov, rf_pago, co_usua_ingr, co_bene, no_bene, co_tord, nu_cnta_acre,
                 ti_cnta_acre, co_fond, co_banc_acre, st_pago, ti_pago, sc_deta)
            VALUES (@co_empr, @sc_rol_int, '202601', 0, '00', '00', '00', @mo,
                    '00', @rf_pago, 0, @co_bene, 'CDC', 0, '0',
                    'A', 0, '00', 'A', 0, @sc_deta_int);
    END TRY
    BEGIN CATCH
        DECLARE @msg NVARCHAR(2048) = ERROR_MESSAGE();
        RAISERROR (@msg, 16, 1);
    END CATCH
END
"""
c.execute(ddl_pago)
print("OK sp_PagoNominaType_CRUD")

# 3) sp_RolPagoType_CRUD
c.execute("IF OBJECT_ID(N'dbo.sp_RolPagoType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_RolPagoType_CRUD")
ddl_rol = r"""
CREATE PROCEDURE dbo.sp_RolPagoType_CRUD
    @Accion CHAR(1),
    @co_empr NVARCHAR(50)=NULL, @nu_rol NVARCHAR(50)=NULL,
    @co_empl NVARCHAR(50)=NULL, @co_rubr NVARCHAR(50)=NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        IF @co_empr IS NULL OR @nu_rol IS NULL OR @co_empl IS NULL OR @co_rubr IS NULL RETURN;
        IF @Accion = 'D'
            DELETE FROM dbo.notbdrol WHERE co_empr=@co_empr AND nu_rol=@nu_rol AND co_empl=@co_empl AND co_rubr=@co_rubr;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.notbdrol WHERE co_empr=@co_empr AND nu_rol=@nu_rol AND co_empl=@co_empl AND co_rubr=@co_rubr)
            INSERT INTO dbo.notbdrol (co_empr, nu_rol, co_empl, co_rubr, mo_rol_pago, mo_rol_pago_prov, mo_rol_prov)
            VALUES (@co_empr, @nu_rol, @co_empl, @co_rubr, 0, 0, 0);
    END TRY
    BEGIN CATCH
        DECLARE @msg NVARCHAR(2048) = ERROR_MESSAGE();
        RAISERROR (@msg, 16, 1);
    END CATCH
END
"""
c.execute(ddl_rol)
print("OK sp_RolPagoType_CRUD")
