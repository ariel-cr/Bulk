"""Robustecer 2 SP CRUD para Seguridad F2 que insertan en tablas con muchos NOT NULL.
Llenar TODOS los NOT NULL con defaults seguros.
"""
import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)
c=sql('dbSG').cursor()

# 1) sp_CuentaNostroType_CRUD - tabla con 5 NOT NULL: co_cnts, nu_iden, no_cnts, st_cnts, ds_mail
c.execute("IF OBJECT_ID(N'dbo.sp_CuentaNostroType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_CuentaNostroType_CRUD")
ddl1 = r"""
CREATE PROCEDURE dbo.sp_CuentaNostroType_CRUD
    @Accion CHAR(1),
    @co_cnts NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        DECLARE @co_cnts_t SMALLINT = TRY_CAST(@co_cnts AS SMALLINT);
        IF @co_cnts_t IS NULL RETURN;
        IF @Accion = 'D'
            DELETE FROM dbo.sgtbcnts WHERE co_cnts = @co_cnts_t;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.sgtbcnts WHERE co_cnts = @co_cnts_t)
            INSERT INTO dbo.sgtbcnts (co_cnts, nu_iden, no_cnts, st_cnts, ds_mail)
            VALUES (@co_cnts_t, '0', 'CDC', 'A', 'cdc@cdc.com');
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END
"""
c.execute(ddl1)
print("OK sp_CuentaNostroType_CRUD")

# 2) sp_UsuarioServicioType_CRUD - tabla con muchos NOT NULL incluso fechas y co_usua_*
c.execute("IF OBJECT_ID(N'dbo.sp_UsuarioServicioType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_UsuarioServicioType_CRUD")
ddl2 = r"""
CREATE PROCEDURE dbo.sp_UsuarioServicioType_CRUD
    @Accion CHAR(1),
    @sc_serv NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        DECLARE @sc_serv_t INT = TRY_CAST(@sc_serv AS INT);
        IF @sc_serv_t IS NULL RETURN;
        IF @Accion = 'D'
            DELETE FROM dbo.sgtbconf_serv_apli WHERE sc_serv = @sc_serv_t;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.sgtbconf_serv_apli WHERE sc_serv = @sc_serv_t)
        BEGIN
            -- sc_serv es identity en algunos casos, omitirlo
            BEGIN TRY
                SET IDENTITY_INSERT dbo.sgtbconf_serv_apli ON;
                INSERT INTO dbo.sgtbconf_serv_apli
                    (sc_serv, co_serv_apli, no_serv_apli, sc_tipo, no_usua, st_regi,
                     fe_ingr, fe_modi, fe_elim, co_usua_ingr, co_usua_modi, co_usua_elim)
                VALUES (@sc_serv_t, 'X1', 'CDC', 1, 'cdc', 'A',
                        GETDATE(), GETDATE(), '1900-01-01', 0, 0, 0);
                SET IDENTITY_INSERT dbo.sgtbconf_serv_apli OFF;
            END TRY
            BEGIN CATCH
                -- intentar sin IDENTITY_INSERT
                BEGIN TRY
                    INSERT INTO dbo.sgtbconf_serv_apli
                        (co_serv_apli, no_serv_apli, sc_tipo, no_usua, st_regi,
                         fe_ingr, fe_modi, fe_elim, co_usua_ingr, co_usua_modi, co_usua_elim)
                    VALUES ('X1', 'CDC', 1, 'cdc', 'A',
                            GETDATE(), GETDATE(), '1900-01-01', 0, 0, 0);
                END TRY
                BEGIN CATCH
                    RETURN;
                END CATCH
            END CATCH
        END
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END
"""
c.execute(ddl2)
print("OK sp_UsuarioServicioType_CRUD")
