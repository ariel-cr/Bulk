"""Genera cartera_flow1_outbox.sql - SOLO el segmento F1:
   tabla legacy -> trg_outbox_<tabla> -> fcme_canonicos.cdc_outbox

Reglas para no daniar:
  1) Agrupa multiples agg_types que apuntan a la misma tabla legacy
     -> UN solo trigger por (ldb, ltbl) que emite N aggregate_types
  2) Anti-loop SESSION_CONTEXT('is_replicating')
  3) Maneja INSERT/UPDATE/DELETE explicitamente
  4) DROP TRIGGER solo del trigger EXACTO que generamos (mismo nombre)
  5) Si la tabla legacy no existe, IMPRIME [SKIP] y no crea nada
  6) Genera SOLO el .sql (no ejecuta)

Patron de batches (resuelve "CREATE TRIGGER must be first stmt"):
  -- Batch 1: skip-or-drop (sin CREATE)
  IF OBJECT_ID(...tabla...) IS NULL  PRINT [SKIP]
  ELSE IF OBJECT_ID(...trigger...) IS NOT NULL  DROP TRIGGER
  GO
  -- Batch 2: CREATE wrapped en EXEC con escape correcto de comillas
  IF OBJECT_ID(...tabla...) IS NOT NULL  EXEC(N'CREATE TRIGGER ...')
  GO
"""
import json
from collections import defaultdict, Counter

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    specs = json.load(f)

# Agrupar por (ldb, ltbl)
groups = defaultdict(list)
for s in specs:
    if "ltbl" not in s:
        continue
    groups[(s["ldb"], s["ltbl"])].append(s)

def build_trigger_body(db, tbl, agg_types, lkey, pcols):
    """Construye el cuerpo del CREATE TRIGGER con patron ROW_NUMBER.
    Garantiza 1 evento = 1 fila aunque lkey no sea unique.
    Tras envolver en EXEC(N'...'), las comillas simples se duplican.
    """
    if len(lkey) > 1:
        agg_id_i = "CONCAT_WS('|'," + ",".join(f"CONVERT(NVARCHAR(200), i.[{k}])" for k in lkey) + ")"
        agg_id_d = agg_id_i.replace("i.[","d.[")
    else:
        agg_id_i = f"CONVERT(NVARCHAR(200), i.[{lkey[0]}])"
        agg_id_d = f"CONVERT(NVARCHAR(200), d.[{lkey[0]}])"
    pcols_q = ",".join(f"x.[{c}]" for c in pcols)
    types_values = ",".join(f"(N'{a}')" for a in agg_types)

    body = f"""
CREATE TRIGGER dbo.trg_outbox_{tbl}
ON dbo.[{tbl}]
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

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    DECLARE @types TABLE (t NVARCHAR(200));
    INSERT INTO @types (t) VALUES {types_values};

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            {agg_id_i},
            tt.t,
            @op,
            (SELECT {pcols_q} FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{db}.dbo.{tbl}',
            SYSUTCDATETIME()
        FROM #ins i CROSS JOIN @types tt;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            {agg_id_d},
            tt.t,
            N'DELETE',
            (SELECT {pcols_q} FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{db}.dbo.{tbl}',
            SYSUTCDATETIME()
        FROM #del d CROSS JOIN @types tt;
    END
END
"""
    return body

out = []
out.append("""/* ==========================================================================
   CARTERA - Flujo 1 OUTBOX TRIGGERS (segmento legacy -> cdc_outbox)
   Scope:
     tabla legacy (dbX.dbo.<ltbl>)
       -> trg_outbox_<ltbl>          (anti-loop SESSION_CONTEXT)
       -> fcme_canonicos.cdc_outbox

   NO incluye: wrappers Oracle, module_config, TRG_PROCESS_CDC_INBOX.
   NO ejecuta cambios -- archivo de revision unicamente.
   ========================================================================== */
""")
out.append(f"/* RESUMEN: {len(specs)} types -> {len(groups)} triggers (uno por tabla legacy) */\n\n")

dbs_used = sorted(set(k[0] for k in groups))
for ldb in dbs_used:
    grp_in_db = [(k,v) for k,v in groups.items() if k[0]==ldb]
    out.append(f"\n/* ---------- BD: {ldb}  ({len(grp_in_db)} triggers) ---------- */\n")
    out.append(f"USE [{ldb}];\nGO\n")

    for (db, tbl), entries in sorted(grp_in_db, key=lambda x: x[0][1]):
        agg_types = [e["agg"] for e in entries]
        base = entries[0]
        lkey = base.get("lkey") or []

        all_pcols = []
        for e in entries:
            for c in e.get("pcols", []):
                if c not in all_pcols:
                    all_pcols.append(c)
        pcols = all_pcols

        if not lkey and pcols:
            lkey = [pcols[0]]
        if not lkey:
            out.append(f"\n/* SKIPPED {db}.{tbl}: sin PK detectada */\n")
            continue

        trg = f"trg_outbox_{tbl}"
        body = build_trigger_body(db, tbl, agg_types, lkey, pcols)
        # Escape para EXEC(N'...'): toda comilla simple del cuerpo se duplica
        body_escaped = body.replace("'", "''")

        comment_types = "\n".join(f"      - {a}" for a in agg_types)
        out.append(f"""
/* ----- {trg}  ON {db}.dbo.{tbl} ({len(agg_types)} type{"s" if len(agg_types)>1 else ""}) -----
{comment_types}
*/
IF OBJECT_ID(N'{db}.dbo.{tbl}', N'U') IS NULL
    PRINT N'[SKIP] tabla legacy {db}.dbo.{tbl} no existe; trigger {trg} no creado';
ELSE IF OBJECT_ID(N'dbo.{trg}', N'TR') IS NOT NULL
    DROP TRIGGER dbo.{trg};
GO

IF OBJECT_ID(N'{db}.dbo.{tbl}', N'U') IS NOT NULL
    EXEC(N'{body_escaped}');
GO
""")

content = "".join(out)
with open(r"C:\Users\Usuario\Downloads\Bulk\cartera_flow1_outbox.sql","w",encoding="utf-8") as f:
    f.write(content)

print(f"OK")
print(f"  Total types     : {len(specs)}")
print(f"  Triggers a crear: {len(groups)}")
db_count = Counter(k[0] for k in groups)
for db, n in db_count.most_common():
    print(f"    {db}: {n} triggers")
print(f"\nArchivo: cartera_flow1_outbox.sql ({len(content):,} bytes)")
