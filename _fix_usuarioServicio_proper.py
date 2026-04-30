"""Fix usuarioServicio_type: cambiar clave de match de sc_serv (identity legacy) a no_usua (clave logica)."""
import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

# 1) SP CRUD legacy (dbSG) - usar no_usua en vez de sc_serv
sg=sql('dbSG').cursor()
sg.execute("IF OBJECT_ID(N'dbo.sp_UsuarioServicioType_CRUD', N'P') IS NOT NULL DROP PROCEDURE dbo.sp_UsuarioServicioType_CRUD")
sg.execute(r"""
CREATE PROCEDURE dbo.sp_UsuarioServicioType_CRUD
    @Accion CHAR(1),
    @no_usua NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        IF @no_usua IS NULL OR LTRIM(RTRIM(@no_usua)) = '' RETURN;
        DECLARE @u VARCHAR(30) = LEFT(@no_usua, 30);
        IF @Accion = 'D'
            DELETE FROM dbo.sgtbconf_serv_apli WHERE no_usua = @u;
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.sgtbconf_serv_apli WHERE no_usua = @u)
            INSERT INTO dbo.sgtbconf_serv_apli
                (co_serv_apli, no_serv_apli, sc_tipo, no_usua, st_regi,
                 fe_ingr, fe_modi, fe_elim, co_usua_ingr, co_usua_modi, co_usua_elim)
            VALUES ('X1', 'CDC', 1, @u, 'A',
                    GETDATE(), GETDATE(), '1900-01-01', 0, 0, 0);
    END TRY
    BEGIN CATCH
        RETURN;
    END CATCH
END
""")
print("OK sp_UsuarioServicioType_CRUD (clave: no_usua)")

# 2) Wrapper canonicos - pasar @no_usua desde CODIGOUSUARIO Oracle
can=sql('fcme_canonicos').cursor()
can.execute("IF OBJECT_ID(N'dbo.usp_inbox_usuarioServicio_type', N'P') IS NOT NULL DROP PROCEDURE dbo.usp_inbox_usuarioServicio_type")
can.execute(r"""
CREATE PROCEDURE dbo.usp_inbox_usuarioServicio_type
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DECLARE @accion CHAR(1) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'I'
            WHEN @event_type IN ('UPDATE','U') THEN 'U'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'D'
            ELSE 'I' END;
        DECLARE @no_usua NVARCHAR(50) = JSON_VALUE(@payload, '$.CODIGOUSUARIO');
        EXEC dbSG.dbo.sp_UsuarioServicioType_CRUD @Accion=@accion, @no_usua=@no_usua;
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type, N'wrapper usuarioServicio_type: ' + ERROR_MESSAGE());
    END CATCH
END
""")
print("OK usp_inbox_usuarioServicio_type (parsea CODIGOUSUARIO -> @no_usua)")
