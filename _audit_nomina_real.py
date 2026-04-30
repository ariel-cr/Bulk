"""Auditoria honesta del estado actual de Nomina F1+F2."""
import pyodbc, oracledb
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

TYPES = [
    ('anticipoNominaType','ANTICIPONOMINA_TYPE','notbcant'),
    ('cargaFamiliarType','CARGAFAMILIAR_TYPE','notbcgfm'),
    ('cargoGeneralType','CARGOGENERAL_TYPE','notbcarg'),
    ('cargoLaboralType','CARGOLABORAL_TYPE','notbcarg_admi'),
    ('catalogoNominaType','CATALOGONOMINA_TYPE','notbcnom'),
    ('configuracionNominaType','CONFIGURACIONNOMINA_TYPE','notbpara'),
    ('empleadoAuditoriaType','EMPLEADOAUDITORIA_TYPE','notbempl_audi'),
    ('empleadoDetalleType','EMPLEADODETALLE_TYPE','notbempl_deta'),
    ('empleadoType','EMPLEADO_TYPE','notbempl'),
    ('firmaHorarioType','FIRMAHORARIO_TYPE','notbfirm'),
    ('fondoReservaType','FONDORESERVA_TYPE','notbfond_rese'),
    ('historialIngresoType','HISTORIALINGRESO_TYPE','notbhieg'),
    ('nivelAcademicoType','NIVELACADEMICO_TYPE','notbnive_acad_empl'),
    ('nominaCabeceraType','NOMINACABECERA_TYPE','notbcrol'),
    ('pagoNominaType','PAGONOMINA_TYPE','notbpago_nomi'),
    ('parametroNominaType','PARAMETRONOMINA_TYPE','notbpara_gene'),
    ('patronalNominaType','PATRONALNOMINA_TYPE','notbpatr'),
    ('rolPagoType','ROLPAGO_TYPE','notbdrol'),
    ('rubroNominaType','RUBRONOMINA_TYPE','notbrubr'),
    ('sectorIessType','SECTORIESS_TYPE','notbsect_iess'),
    ('viaticoNominaType','VIATICONOMINA_TYPE','notbcvia'),
]

orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()
can=sql('fcme_canonicos').cursor()
no=sql('dbNO').cursor()

print('='*100)
print('AUDIT NOMINA - estado actual real (no asumido)')
print('='*100)

# Verificar piezas por type
print(f"\n{'#':>2} {'TYPE':<28} {'F1.trg':<8} {'F1.cfg':<8} {'F1.sp':<8} {'F2.trg':<8} {'F2.wrp':<8} {'F2.mc':<8} {'F2.sp':<8}")
print('-'*100)

# Una sola query para cargar todo
no.execute("SELECT name FROM sys.triggers WHERE name LIKE 'trg_outbox_%'")
f1_triggers={r.name for r in no.fetchall()}
o.execute("SELECT trigger_name, table_name FROM all_triggers WHERE owner='FCME_USER' AND trigger_name LIKE 'TRG_OUTBOX_%'")
f2_triggers={r[1] for r in o.fetchall()}
o.execute("SELECT aggregate_type FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE active=1")
f1_cfg={r[0] for r in o.fetchall()}
o.execute("SELECT object_name FROM all_objects WHERE owner='FCME_USER' AND object_name LIKE 'USP_INBOX_%' AND object_type='PROCEDURE'")
f1_sps={r[0] for r in o.fetchall()}
can.execute("SELECT aggregate_type FROM dbo.cdc_inbox_module_config WHERE active=1")
f2_cfg={r.aggregate_type for r in can.fetchall()}
can.execute("SELECT name FROM sys.objects WHERE type='P' AND name LIKE 'usp_inbox_%'")
f2_wrappers={r.name for r in can.fetchall()}
no.execute("SELECT name FROM sys.objects WHERE type='P' AND (name LIKE 'sp_%Type_CRUD' OR name LIKE 'sp_%_CRUD')")
f2_sps={r.name.lower() for r in no.fetchall()}

complete_f1=0; complete_f2=0
incomplete=[]
for agg, dest, ltbl in TYPES:
    has_f1_trg = f"trg_outbox_{ltbl}" in f1_triggers or f"trg_outbox_{ltbl}_carga" in f1_triggers or f"trg_outbox_{ltbl}_empleado" in f1_triggers
    has_f1_cfg = agg in f1_cfg
    sp_name_oracle = 'USP_INBOX_' + agg[:-4].upper()  # remove 'Type'
    sp_oracle_simple = 'USP_INBOX_' + (agg[0].upper()+agg[1:-4]).upper()
    has_f1_sp = any(s for s in f1_sps if 'INBOX' in s and (agg.upper()[:-4] in s or agg[:-4].upper().replace('_','') in s))
    has_f2_trg = dest in f2_triggers
    has_f2_wrp = f"usp_inbox_{agg}" in f2_wrappers
    has_f2_cfg = agg in f2_cfg
    base = agg[0].upper()+agg[1:]
    f2_sp_name = f"sp_{base}_CRUD".lower()
    has_f2_sp = f2_sp_name in f2_sps
    flags = (has_f1_trg, has_f1_cfg, has_f1_sp, has_f2_trg, has_f2_wrp, has_f2_cfg, has_f2_sp)
    if has_f1_trg and has_f1_cfg and has_f1_sp: complete_f1 += 1
    if has_f2_trg and has_f2_wrp and has_f2_cfg and has_f2_sp: complete_f2 += 1
    print(f"{TYPES.index((agg,dest,ltbl))+1:>2} {agg:<28} "
          f"{'Y' if has_f1_trg else 'N':<8} "
          f"{'Y' if has_f1_cfg else 'N':<8} "
          f"{'Y' if has_f1_sp else 'N':<8} "
          f"{'Y' if has_f2_trg else 'N':<8} "
          f"{'Y' if has_f2_wrp else 'N':<8} "
          f"{'Y' if has_f2_cfg else 'N':<8} "
          f"{'Y' if has_f2_sp else 'N':<8}")
    missing=[]
    if not has_f1_trg: missing.append('F1.trg')
    if not has_f1_cfg: missing.append('F1.cfg')
    if not has_f1_sp:  missing.append('F1.sp')
    if not has_f2_trg: missing.append('F2.trg')
    if not has_f2_wrp: missing.append('F2.wrp')
    if not has_f2_cfg: missing.append('F2.mc')
    if not has_f2_sp:  missing.append('F2.sp')
    if missing:
        incomplete.append((agg, missing))

print()
print(f'F1 completo: {complete_f1}/21')
print(f'F2 completo: {complete_f2}/21')
if incomplete:
    print(f'\n[FALTANTES]:')
    for agg, miss in incomplete: print(f'  {agg:<28} -> {miss}')
else:
    print(f'\nTODAS las piezas presentes')

# Ultimos errores recientes
print()
print('='*100)
print('Errores recientes (ultimas 30 min) por aggregate_type Nomina')
print('='*100)
nomina_aggs="','".join(t[0] for t in TYPES)
o.execute(f"""SELECT AGGREGATE_TYPE, COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS
              WHERE AGGREGATE_TYPE IN ('{nomina_aggs}') AND ERROR_DATE > SYSTIMESTAMP - 0.021
              GROUP BY AGGREGATE_TYPE ORDER BY 1""")
oracle_errs=o.fetchall()
print(f'\n[Oracle FCME_USER.CDC_INBOX_ERRORS - F1 errors]: {len(oracle_errs)} types con error')
for r in oracle_errs: print(f'  {r[0]:<28} count={r[1]}')

can.execute(f"""SELECT aggregate_type, COUNT(*) FROM dbo.cdc_inbox_errors
              WHERE aggregate_type IN ('{nomina_aggs}') AND created_at > DATEADD(MINUTE,-30,SYSDATETIME())
              GROUP BY aggregate_type ORDER BY 1""")
sql_errs=can.fetchall()
print(f'\n[SQL fcme_canonicos.cdc_inbox_errors - F2 errors]: {len(sql_errs)} types con error')
for r in sql_errs: print(f'  {r.aggregate_type:<28} count={r[1]}')

orcl.close()
print()
print('='*100)
print('RESUMEN HONESTO:')
print(f'  F1 cableado: {complete_f1}/21')
print(f'  F2 cableado: {complete_f2}/21')
print(f'  F1 errors recientes: {len(oracle_errs)} types')
print(f'  F2 errors recientes: {len(sql_errs)} types')
print('='*100)
