"""Dump completo (DDL) de todos los triggers outbox del modulo PARTICIPE."""
import pyodbc, oracledb

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

out=[]
out.append('/* ============================================================')
out.append('   DUMP COMPLETO DE TRIGGERS OUTBOX - modulo PARTICIPE')
out.append('   Snapshot generado del estado actual de las BDs')
out.append('   ============================================================ */')
out.append('')

# === FLUJO 1: SQL Server ===
out.append('/* ############################################################')
out.append('   FLUJO 1 - SQL Server legacy (publica a fcme_canonicos.cdc_outbox)')
out.append('   ############################################################ */')
out.append('')

total_f1 = 0
for db in ['dbIM','dbFC','dbCR','dbCG','dbCT','dbNO','dbSV']:
    c = sql(db).cursor()
    c.execute("""SELECT t.name AS trg, OBJECT_NAME(t.parent_id) AS parent, t.is_disabled,
                        OBJECT_DEFINITION(t.object_id) AS body
                 FROM sys.triggers t
                 WHERE t.name LIKE 'trg_outbox_%'
                 ORDER BY parent""")
    rows = c.fetchall()
    if not rows:
        continue
    out.append(f'/* ----- BD: {db}  ({len(rows)} triggers) ----- */')
    out.append(f'USE [{db}];')
    out.append('GO')
    out.append('')
    for r in rows:
        out.append(f'/* --- {r.trg}  ON dbo.{r.parent}  disabled={r.is_disabled} --- */')
        out.append(f"IF OBJECT_ID(N'dbo.{r.trg}', N'TR') IS NOT NULL DROP TRIGGER dbo.{r.trg};")
        out.append('GO')
        if r.body:
            body = r.body.strip()
            if not body.upper().startswith('CREATE'):
                body = body
            out.append(body)
        else:
            out.append('-- (cuerpo vacio o no accesible)')
        out.append('GO')
        out.append('')
    total_f1 += len(rows)

out.append(f'/* TOTAL FLUJO 1 = {total_f1} triggers */')
out.append('')

# === FLUJO 2: Oracle ===
out.append('/* ############################################################')
out.append('   FLUJO 2 - Oracle FCME_USER (publica a FCME_USER.CDC_OUTBOX)')
out.append('   ############################################################ */')
out.append('')

o = oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1').cursor()
o.execute("""SELECT trigger_name, table_name, status FROM all_triggers
             WHERE owner='FCME_USER' AND trigger_name LIKE 'TRG_OUTBOX_%'
             ORDER BY table_name, trigger_name""")
trigs = o.fetchall()
total_f2 = len(trigs)

for trg_name, table_name, status in trigs:
    out.append(f'/* --- {trg_name}  ON FCME_USER.{table_name}  status={status} --- */')
    o.execute("""SELECT text FROM all_source
                 WHERE owner='FCME_USER' AND name=:1 AND type='TRIGGER'
                 ORDER BY line""", [trg_name])
    src_lines = [r[0] for r in o.fetchall()]
    if not src_lines:
        # algunos triggers se almacenan en all_triggers.trigger_body
        o.execute("""SELECT description, trigger_body FROM all_triggers
                     WHERE owner='FCME_USER' AND trigger_name=:1""", [trg_name])
        rec = o.fetchone()
        if rec:
            desc = rec[0] or ''
            body = rec[1].read() if hasattr(rec[1],'read') else (rec[1] or '')
            out.append(f'CREATE OR REPLACE TRIGGER FCME_USER.{trg_name}')
            out.append(desc)
            out.append(body)
            out.append('/')
    else:
        out.append('CREATE OR REPLACE')
        out.append(''.join(src_lines))
        out.append('/')
    out.append('')

out.append(f'/* TOTAL FLUJO 2 = {total_f2} triggers */')
out.append('')

with open('all_triggers_dump.sql', 'w', encoding='utf-8') as f:
    f.write('\n'.join(out))
print(f'Wrote {len(out)} lines  to all_triggers_dump.sql')
print(f'  Flujo 1: {total_f1} triggers')
print(f'  Flujo 2: {total_f2} triggers')
