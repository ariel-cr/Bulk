import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)
c=sql('dbNO').cursor()
c.execute("IF OBJECT_ID(N'dbo.trg_outbox_notbpara_gene', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_notbpara_gene")
ddl = r"""
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
"""
c.execute(ddl)
print("OK trg_outbox_notbpara_gene creado")
