"""Regenera los 76 triggers activos para que aggregate_type sea el Type
canonico (de cdc_table_to_types.aggregate_type_emit) en lugar del
nombre de la tabla legacy. Si una tabla emite N types, el trigger
inserta N filas en cdc_outbox por cada cambio.
"""
import pyodbc
from collections import defaultdict

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# 1) Mapeo source_table -> [(aggregate_type_emit, ...)]
c = sql("fcme_canonicos").cursor()
c.execute("""
  SELECT source_table, aggregate_type_emit
  FROM dbo.cdc_table_to_types
  WHERE is_active = 1 AND aggregate_type_emit IS NOT NULL
""")
table_types = defaultdict(list)
for r in c.fetchall():
    table_types[r.source_table].append(r.aggregate_type_emit)

print(f"Tablas con types configurados: {len(table_types)}")

# 2) Localizar las BDs de cada tabla y obtener PK + columnas
LEG_DBS = ["dbIM","dbFC","dbCR","dbCG","dbCT","dbNO","dbSV"]
table_to_db = {}
for db in LEG_DBS:
    cc = sql(db).cursor()
    cc.execute("""
      SELECT t.name FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id=s.schema_id
      WHERE s.name='dbo'
    """)
    for r in cc.fetchall():
        if r.name in table_types:
            table_to_db.setdefault(r.name, db)

print(f"Tablas localizadas en BDs legacy: {len(table_to_db)}")

def get_cols(db, tbl):
    c = sql(db).cursor()
    c.execute("""SELECT name FROM sys.columns WHERE object_id=OBJECT_ID(?) ORDER BY column_id""",
              f"dbo.{tbl}")
    return [r.name for r in c.fetchall()]

def get_pk(db, tbl):
    c = sql(db).cursor()
    c.execute("""
      SELECT c.name
      FROM sys.indexes i
      JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
      JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id
      WHERE i.is_primary_key=1 AND i.object_id=OBJECT_ID(?)
      ORDER BY ic.key_ordinal
    """, f"dbo.{tbl}")
    pks = [r.name for r in c.fetchall()]
    if pks: return pks
    # Fallback: primera columna
    cols = get_cols(db, tbl)
    return cols[:1]

def build_trigger(db, tbl, types):
    cols = get_cols(db, tbl)
    pks = get_pk(db, tbl)
    pk_first = pks[0]
    if len(pks) > 1:
        agg_id_expr = " + N'|' + ".join([f"CONVERT(NVARCHAR(100), i.[{p}])" for p in pks])
        agg_id_expr_d = " + N'|' + ".join([f"CONVERT(NVARCHAR(100), d.[{p}])" for p in pks])
    else:
        agg_id_expr = f"CONVERT(NVARCHAR(200), i.[{pk_first}])"
        agg_id_expr_d = f"CONVERT(NVARCHAR(200), d.[{pk_first}])"

    payload_select = ", ".join(f"x.[{c}]" for c in cols)

    types_values = ", ".join(f"(N'{t}')" for t in types)

    body = f"""CREATE TRIGGER dbo.trg_outbox_{tbl}
ON dbo.[{tbl}]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
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

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES {types_values};

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            {agg_id_expr},
            tt.t,
            @op,
            (SELECT {payload_select} FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{db}.dbo.{tbl}',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            {agg_id_expr_d},
            tt.t,
            N'DELETE',
            (SELECT {payload_select} FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{db}.dbo.{tbl}',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
"""
    return body

# 3) Recrear cada trigger
created = 0
errors = []
for tbl, db in table_to_db.items():
    types = table_types[tbl]
    try:
        c = sql(db).cursor()
        c.execute(f"IF OBJECT_ID('dbo.trg_outbox_{tbl}','TR') IS NOT NULL DROP TRIGGER dbo.trg_outbox_{tbl}")
        ddl = build_trigger(db, tbl, types)
        c.execute(ddl)
        created += 1
        print(f"  OK [{db}.{tbl}] -> types: {types}")
    except Exception as e:
        msg = str(e)[:200]
        errors.append((db, tbl, msg))
        print(f"  FAIL [{db}.{tbl}]: {msg}")

print(f"\nTotal triggers regenerados: {created}")
print(f"Errores: {len(errors)}")
for db, tbl, msg in errors[:10]:
    print(f"  {db}.{tbl}: {msg}")
