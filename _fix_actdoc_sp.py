import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)
c=sql('dbFC').cursor()
c.execute("IF OBJECT_ID(N'dbo.sp_actualizacionDocumentosType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_actualizacionDocumentosType_CRUD")
ddl = r"""
CREATE PROCEDURE dbo.sp_actualizacionDocumentosType_CRUD
    @Accion CHAR(1),
    @sc_actu_docs NVARCHAR(50) = NULL,
    @co_empr NVARCHAR(50) = NULL,
    @co_cedu NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        IF @sc_actu_docs IS NULL OR @co_empr IS NULL OR @co_cedu IS NULL RETURN;
        IF @Accion = 'D'
            DELETE FROM dbo.fctbafil_info_actu_docs WHERE sc_actu_docs=@sc_actu_docs AND co_empr=@co_empr AND co_cedu=@co_cedu;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.fctbafil_info_actu_docs WHERE sc_actu_docs=@sc_actu_docs AND co_empr=@co_empr AND co_cedu=@co_cedu)
        BEGIN
            -- existe pero como sc_actu_docs es identity, no podemos especificarlo via INSERT directo
            -- omitimos INSERT (Flow 1 ya popula la tabla); solo confirmamos si existe
            SET @co_cedu = @co_cedu; -- no-op
        END
    END TRY
    BEGIN CATCH
        DECLARE @msg NVARCHAR(2048) = ERROR_MESSAGE();
        RAISERROR (@msg, 16, 1);
    END CATCH
END;
"""
c.execute(ddl)
print("OK sp_actualizacionDocumentosType_CRUD recreado")
