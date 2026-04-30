"""Dump completo de triggers Nomina (F1 + F2) con DDL."""
import pyodbc, oracledb, re

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

# Lista canonica de aggregate_types de Nomina
NOMINA_AGGS = {
    'anticipoNominaType','cargaFamiliarType','cargoGeneralType','cargoLaboralType',
    'catalogoNominaType','configuracionNominaType','empleadoAuditoriaType','empleadoDetalleType',
    'empleadoType','firmaHorarioType','fondoReservaType','historialIngresoType',
    'nivelAcademicoType','nominaCabeceraType','pagoNominaType','parametroNominaType',
    'patronalNominaType','rolPagoType','rubroNominaType','sectorIessType','viaticoNominaType'
}
# Tablas destino F2 (FCME_USER)
F2_DESTS = {
    'ANTICIPONOMINA_TYPE','CARGAFAMILIAR_TYPE','CARGOGENERAL_TYPE','CARGOLABORAL_TYPE',
    'CATALOGONOMINA_TYPE','CONFIGURACIONNOMINA_TYPE','EMPLEADOAUDITORIA_TYPE','EMPLEADODETALLE_TYPE',
    'EMPLEADO_TYPE','FIRMAHORARIO_TYPE','FONDORESERVA_TYPE','HISTORIALINGRESO_TYPE',
    'NIVELACADEMICO_TYPE','NOMINACABECERA_TYPE','PAGONOMINA_TYPE','PARAMETRONOMINA_TYPE',
    'PATRONALNOMINA_TYPE','ROLPAGO_TYPE','RUBRONOMINA_TYPE','SECTORIESS_TYPE','VIATICONOMINA_TYPE'
}

out = []
out.append('/* ============================================================')
out.append('   DUMP TRIGGERS NOMINA (F1 + F2) - DDL completo')
out.append('   Snapshot generado del estado actual de las BDs')
out.append('   ============================================================ */')
out.append('')

# === FLUJO 1: dbNO ===
out.append('/* ############################################################')
out.append('   FLUJO 1 - Nomina dbNO -> fcme_canonicos.cdc_outbox')
out.append('   Filtro: triggers cuya definicion publica un aggregate_type de Nomina')
out.append('   ############################################################ */')
out.append('')
out.append('USE [dbNO];')
out.append('GO')
out.append('')

c = sql('dbNO').cursor()
c.execute("""SELECT t.name AS trg, OBJECT_NAME(t.parent_id) AS parent, t.is_disabled,
                    OBJECT_DEFINITION(t.object_id) AS body
             FROM sys.triggers t
             WHERE t.name LIKE 'trg_outbox_%'
             ORDER BY parent, trg""")
all_dbno = c.fetchall()
nomina_f1 = []
for r in all_dbno:
    body = r.body or ''
    types_in = set(re.findall(r"N'([A-Za-z][A-Za-z0-9_]*[Tt]ype)'", body))
    if types_in & NOMINA_AGGS:
        nomina_f1.append(r)

out.append(f'/* TOTAL F1 (dbNO) Nomina: {len(nomina_f1)} triggers */')
out.append('')
for r in nomina_f1:
    out.append(f'/* --- {r.trg}  ON dbo.{r.parent}  disabled={r.is_disabled} --- */')
    out.append(f"IF OBJECT_ID(N'dbo.{r.trg}', N'TR') IS NOT NULL DROP TRIGGER dbo.{r.trg};")
    out.append('GO')
    out.append((r.body or '-- (cuerpo no accesible)').strip())
    out.append('GO')
    out.append('')

# === FLUJO 2: Oracle FCME_USER ===
out.append('')
out.append('/* ############################################################')
out.append('   FLUJO 2 - Nomina FCME_USER -> FCME_USER.CDC_OUTBOX')
out.append('   ############################################################ */')
out.append('')

orcl = oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1').cursor()
orcl.execute("""SELECT trigger_name, table_name, status FROM all_triggers
                WHERE owner='FCME_USER' AND trigger_name LIKE 'TRG_OUTBOX_%'
                ORDER BY table_name, trigger_name""")
all_oracle = [(r[0], r[1], r[2]) for r in orcl.fetchall()]
nomina_f2 = [t for t in all_oracle if t[1] in F2_DESTS]

out.append(f'/* TOTAL F2 (FCME_USER) Nomina: {len(nomina_f2)} triggers */')
out.append('')
for trg, table, status in nomina_f2:
    out.append(f'/* --- {trg}  ON FCME_USER.{table}  status={status} --- */')
    orcl.execute("""SELECT text FROM all_source WHERE owner='FCME_USER' AND name=:1 AND type='TRIGGER' ORDER BY line""", [trg])
    src_lines = [r[0] for r in orcl.fetchall()]
    if src_lines:
        out.append('CREATE OR REPLACE')
        out.append(''.join(src_lines).strip())
        out.append('/')
    else:
        out.append('-- (cuerpo no accesible via all_source)')
    out.append('')

with open('nomina_triggers_dump.sql','w',encoding='utf-8') as f:
    f.write('\n'.join(out))

print(f'Wrote nomina_triggers_dump.sql ({sum(len(l) for l in out)} chars, {len(out)} sections)')
print(f'  F1: {len(nomina_f1)} triggers')
print(f'  F2: {len(nomina_f2)} triggers')
print(f'  Total: {len(nomina_f1)+len(nomina_f2)}')
