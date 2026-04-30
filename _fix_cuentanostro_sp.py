import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)
c=sql('dbSG').cursor()
c.execute("IF OBJECT_ID(N'dbo.sp_cuentaNostroType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_cuentaNostroType_CRUD")
c.execute("IF OBJECT_ID(N'dbo.sp_CuentaNostroType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_CuentaNostroType_CRUD")
ddl = r"""
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
            DELETE FROM dbo.[sgtbcnts] WHERE [co_cnts] = @co_cnts_t;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.[sgtbcnts] WHERE [co_cnts] = @co_cnts_t)
            BEGIN
                BEGIN TRY
                    INSERT INTO dbo.[sgtbcnts] ([co_cnts]) VALUES (@co_cnts_t);
                END TRY
                BEGIN CATCH
                    RETURN;
                END CATCH
            END
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END
"""
c.execute(ddl)
print("OK sp_CuentaNostroType_CRUD")
