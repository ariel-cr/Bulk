"""Despliega SOLO el trigger piloto: dbFC.dbo.trg_outbox_fctbafil_actu"""
import pyodbc, re

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}

def c(db):
    s = (f"DRIVER={DB['driver']};SERVER={DB['server']};"
         f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

SQL = r"""
USE [dbFC];
""".strip()

DROP_SQL = "IF OBJECT_ID(N'dbo.trg_outbox_fctbafil_actu', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_fctbafil_actu;"

CREATE_SQL = """
CREATE TRIGGER dbo.trg_outbox_fctbafil_actu
ON dbo.[fctbafil_actu]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop
    IF CONVERT(NVARCHAR(50), SESSION_CONTEXT(N'cdc_origin')) IS NOT NULL
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), i.[ci_cedu]),
            N'fctbafil_actu',
            @op,
            (SELECT x.* FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_actu',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CONVERT(NVARCHAR(200), d.[ci_cedu]),
            N'fctbafil_actu',
            N'DELETE',
            (SELECT x.* FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'dbFC.dbo.fctbafil_actu',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
""".strip()

cur = c("dbFC").cursor()

print(">> DROP trigger si existe")
cur.execute(DROP_SQL)
print("   ok")

print(">> CREATE TRIGGER dbo.trg_outbox_fctbafil_actu")
cur.execute(CREATE_SQL)
print("   ok")

# Validaciones
print("\n>> Verificacion en sys.triggers:")
cur.execute("""
  SELECT s.name AS sch, o.name AS tbl, tr.name AS trg, tr.is_disabled
  FROM sys.triggers tr
  JOIN sys.objects o ON tr.parent_id = o.object_id
  JOIN sys.schemas s ON o.schema_id = s.schema_id
  WHERE tr.name = 'trg_outbox_fctbafil_actu'
""")
for r in cur.fetchall():
    print(f"   {r.sch}.{r.tbl}  ->  {r.trg}  (disabled={bool(r.is_disabled)})")

print("\n>> Conteo actual cdc_outbox (fcme_canonicos):")
cur2 = c("fcme_canonicos").cursor()
cur2.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
print(f"   rows = {cur2.fetchone()[0]}")

print("\nPILOTO DESPLEGADO. Para probar end-to-end ejecuta en SSMS:")
print("   USE dbFC;")
print("   UPDATE dbo.fctbafil_actu SET fx_actu = GETDATE() WHERE ci_cedu = (SELECT TOP 1 ci_cedu FROM dbo.fctbafil_actu);")
print("   SELECT TOP 5 * FROM fcme_canonicos.dbo.cdc_outbox ORDER BY id DESC;")
