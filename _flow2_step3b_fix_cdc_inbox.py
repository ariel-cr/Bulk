"""Recrea cdc_inbox con IDENTITY en id y defaults en created_at/processed.
La tabla esta vacia, asi que es seguro re-crearla.
"""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = sql("fcme_canonicos").cursor()

# Verificar vacio
c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox")
n = c.fetchone()[0]
if n > 0:
    raise SystemExit(f"ABORT: cdc_inbox tiene {n} filas. No puedo recrear sin perder datos.")
print(f"cdc_inbox vacia ({n} filas) — seguro recrear")

# Drop trigger y FK existentes (por si)
c.execute("""DECLARE @cmd NVARCHAR(MAX) = N'';
SELECT @cmd = @cmd + N'DROP TRIGGER ' + QUOTENAME(t.name) + N';' + CHAR(10)
FROM sys.triggers t WHERE t.parent_id=OBJECT_ID('dbo.cdc_inbox');
IF LEN(@cmd) > 0 EXEC sp_executesql @cmd;
""")

# DROP y CREATE
c.execute("DROP TABLE dbo.cdc_inbox")
c.execute("""
CREATE TABLE dbo.cdc_inbox (
    id             BIGINT IDENTITY(1,1) NOT NULL,
    aggregate_id   NVARCHAR(200) NOT NULL,
    aggregate_type NVARCHAR(200) NOT NULL,
    event_type     NVARCHAR(50)  NOT NULL,
    payload        NVARCHAR(MAX) NOT NULL,
    source_table   NVARCHAR(200) NULL,
    created_at     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    processed      BIT           NOT NULL DEFAULT 0,
    processed_at   DATETIME2(3)  NULL,
    CONSTRAINT PK_cdc_inbox PRIMARY KEY (id)
)
""")
c.execute("CREATE INDEX IX_cdc_inbox_processed ON dbo.cdc_inbox(processed, id) INCLUDE (aggregate_type)")
print("cdc_inbox recreada con IDENTITY + defaults + indice")

# Verificar
c.execute("""SELECT c.name, t.name tp, c.is_identity, c.is_nullable
             FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
             WHERE c.object_id=OBJECT_ID('dbo.cdc_inbox') ORDER BY c.column_id""")
for r in c.fetchall():
    print(f"  {r.name:<18} {r.tp:<14} identity={r.is_identity} null={r.is_nullable}")
