"""Cleanup:
  1) Deshabilita los 76 triggers trg_outbox_* que yo cree (no los borra).
  2) Archiva y borra eventos huerfanos que YO inserte en legacy.cdc_outbox
     (aggregate_type que NO esta en cdc_inbox_module_config de newcore).
  3) Reset del bridge checkpoint al max actual.
"""
import pyodbc
DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def conn(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# 1) disable triggers mios
print("== [1] Disable 76 triggers trg_outbox_* ==")
disabled = 0
for db in ["dbCG","dbCR","dbCT","dbFC","dbIM","dbNO","dbSV"]:
    c = conn(db).cursor()
    c.execute("""
      SELECT s.name sch, o.name tbl, tr.name tr
      FROM sys.triggers tr
      JOIN sys.objects o ON tr.parent_id=o.object_id
      JOIN sys.schemas s ON o.schema_id=s.schema_id
      WHERE tr.name LIKE 'trg_outbox_%'
    """)
    for r in c.fetchall():
        try:
            c.execute(f"DISABLE TRIGGER [{r.sch}].[{r.tr}] ON [{r.sch}].[{r.tbl}]")
            disabled += 1
        except Exception as e:
            print(f"  fail {db}.{r.tr}: {e}")
print(f"  deshabilitados: {disabled}")

# 2) archivar y borrar huerfanos en legacy.cdc_outbox
print("\n== [2] Huerfanos en legacy.cdc_outbox ==")
cl = conn("fcme_legacy").cursor()
cn = conn("fcme_newcore").cursor()

# obtener aggregate_types validos desde newcore.cdc_inbox_module_config
cn.execute("SELECT DISTINCT aggregate_type FROM dbo.cdc_inbox_module_config WHERE active=1")
valid_types = {r[0] for r in cn.fetchall()}
print(f"  aggregate_types validos en newcore: {len(valid_types)}")

# obtener huerfanos en legacy.cdc_outbox
cl.execute("SELECT DISTINCT aggregate_type FROM dbo.cdc_outbox")
outbox_types = {r[0] for r in cl.fetchall()}
huerfanos = sorted(outbox_types - valid_types)
print(f"  aggregate_types huerfanos: {len(huerfanos)}")
for t in huerfanos[:20]:
    print(f"    {t}")
if len(huerfanos) > 20: print(f"    ... {len(huerfanos)-20} mas")

if huerfanos:
    # crear tabla archivo si no existe
    cl.execute("""
    IF OBJECT_ID(N'dbo.cdc_outbox_archive', N'U') IS NULL
      CREATE TABLE dbo.cdc_outbox_archive (
        archived_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        reason NVARCHAR(200) NOT NULL,
        id BIGINT, aggregate_id NVARCHAR(200), aggregate_type NVARCHAR(200),
        event_type NVARCHAR(200), payload NVARCHAR(MAX),
        source_table NVARCHAR(200), created_at DATETIME2)
    """)
    # archivar
    placeholders = ",".join(["?"] * len(huerfanos))
    cl.execute(f"""
      INSERT INTO dbo.cdc_outbox_archive
       (reason, id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
      SELECT N'huerfano-por-triggers-trg_outbox_', id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at
      FROM dbo.cdc_outbox
      WHERE aggregate_type IN ({placeholders})
    """, *huerfanos)
    arc = cl.rowcount
    cl.execute(f"DELETE FROM dbo.cdc_outbox WHERE aggregate_type IN ({placeholders})", *huerfanos)
    borr = cl.rowcount
    print(f"  archivados: {arc}  borrados: {borr}")

# 3) tambien limpiar huerfanos que ya llegaron a newcore.cdc_inbox sin procesar
print("\n== [3] Huerfanos en newcore.cdc_inbox (sin procesar) ==")
if huerfanos:
    placeholders = ",".join(["?"] * len(huerfanos))
    cn.execute(f"""
      SELECT COUNT(*) FROM dbo.cdc_inbox
      WHERE processed = 0 AND aggregate_type IN ({placeholders})
    """, *huerfanos)
    n = cn.fetchone()[0]
    print(f"  encontrados: {n}")
    if n > 0:
        cn.execute(f"""
          DELETE FROM dbo.cdc_inbox
          WHERE processed = 0 AND aggregate_type IN ({placeholders})
        """, *huerfanos)
        print(f"  borrados: {cn.rowcount}")

# 4) reset checkpoint
print("\n== [4] Reset checkpoint bridge ==")
cl.execute("SELECT ISNULL(MAX(id), 0) FROM dbo.cdc_outbox")
mx = cl.fetchone()[0]
cl.execute("UPDATE dbo.cdc_outbox_bridge_state SET last_sent_id = ? WHERE channel='LEGACY_TO_NEWCORE'", mx)
print(f"  last_sent_id = {mx}")

# 5) estado final
print("\n== [5] Estado final ==")
cl.execute("SELECT COUNT(*) FROM dbo.cdc_outbox"); a = cl.fetchone()[0]
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox"); b = cn.fetchone()[0]
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=1"); c1 = cn.fetchone()[0]
cn.execute("SELECT COUNT(*) FROM dbo.cdc_inbox WHERE processed=0"); c2 = cn.fetchone()[0]
print(f"  legacy.cdc_outbox:     {a}")
print(f"  newcore.cdc_inbox:     {b}   processed={c1}  pending={c2}")
