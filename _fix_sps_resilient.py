"""Hacer los 4 SP CRUD problematicos mas tolerantes:
   - TRY_CAST para evitar conversion errors con valores ENC:...
   - Salir silenciosamente si el cast falla en vez de RAISERROR
   - Eliminar el RAISERROR del CATCH (que doomeaba la transaction)
"""
import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)
c=sql('dbNO').cursor()

SPS = [
    {'name':'sp_AnticipoNominaType_CRUD','tbl':'notbcant','pk':['co_empr','nu_anio','sc_anti','co_empl'],
     'types':{'co_empr':'SMALLINT','nu_anio':'SMALLINT','sc_anti':'INT','co_empl':'SMALLINT'}},
    {'name':'sp_FirmaHorarioType_CRUD','tbl':'notbfirm','pk':['co_empr','co_empl','sc_firm'],
     'types':{'co_empr':'SMALLINT','co_empl':'SMALLINT','sc_firm':'INT'}},
    {'name':'sp_NivelAcademicoType_CRUD','tbl':'notbnive_acad_empl','pk':['co_empr','co_empl'],
     'types':{'co_empr':'SMALLINT','co_empl':'SMALLINT'}},
    {'name':'sp_ViaticoNominaType_CRUD','tbl':'notbcvia','pk':['co_empr','co_empl','sc_viat'],
     'types':{'co_empr':'SMALLINT','co_empl':'SMALLINT','sc_viat':'INT'}},
]

for s in SPS:
    pk_params = ", ".join(f"@{c} NVARCHAR(50) = NULL" for c in s['pk'])
    declares = "\n        ".join(f"DECLARE @{c}_typed {s['types'][c]} = TRY_CAST(@{c} AS {s['types'][c]});" for c in s['pk'])
    null_check = " OR ".join(f"@{c}_typed IS NULL" for c in s['pk'])
    pk_match = " AND ".join(f"[{c}] = @{c}_typed" for c in s['pk'])
    cols_q = ",".join(f"[{c}]" for c in s['pk'])
    vals_q = ",".join(f"@{c}_typed" for c in s['pk'])
    drop_ddl = f"IF OBJECT_ID(N'dbo.{s['name']}', N'P') IS NOT NULL DROP PROCEDURE dbo.{s['name']};"
    body = f"""
CREATE PROCEDURE dbo.{s['name']}
    @Accion CHAR(1),
    {pk_params}
AS
BEGIN
    SET NOCOUNT ON;
    EXEC sp_set_session_context N'is_replicating', 1;
    BEGIN TRY
        {declares}
        IF {null_check} RETURN;  -- silencioso, no RAISERROR
        IF @Accion = 'D'
            DELETE FROM dbo.[{s['tbl']}] WHERE {pk_match};
        ELSE IF NOT EXISTS (SELECT 1 FROM dbo.[{s['tbl']}] WHERE {pk_match})
            INSERT INTO dbo.[{s['tbl']}] ({cols_q}) VALUES ({vals_q});
    END TRY
    BEGIN CATCH
        -- silencioso para no doomar la transaccion del trigger en canonicos
        RETURN;
    END CATCH
END
"""
    try:
        c.execute(drop_ddl)
        c.execute(body)
        print(f"  CREATE {s['name']}")
    except Exception as e:
        print(f"  FAIL {s['name']}: {str(e)[:200]}")
