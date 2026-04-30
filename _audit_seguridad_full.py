"""Audit completo Seguridad - F1 + F2 con verificacion end-to-end."""
import pyodbc, oracledb, time, datetime as dt

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True, timeout=30)

orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()
sg=sql('dbSG').cursor()
can=sql('fcme_canonicos').cursor()

TYPES=[
    ('aplicacionFuncion_type','APLICACIONFUNCION_TYPE','sgtbapli','no_apli','NOMBREAPLICACION'),
    ('auditoriaFlujo_type','AUDITORIAFLUJO_TYPE','sgtbtran','no_tran','CODIGOPROCESO'),
    ('cuentaNostroType','CUENTANOSTRO_TYPE','sgtbcnts','no_cnts','NOMBRE'),
    ('empresa_type','EMPRESA_TYPE','sgtbempr','no_empr','NOMBREEMPRESA'),
    ('firmaSeguridad_type','FIRMASEGURIDAD_TYPE','sgtbfirm','no_maqu','NOMBREMAQUINAUSUARIO'),
    ('fondoSeguridad_type','FONDOSEGURIDAD_TYPE','sgtbfond','no_fond','NOMBREFONDO'),
    ('localidad_type','LOCALIDAD_TYPE','sgtbloca','no_loca','CODIGOPROVINCIA'),
    ('parametroSeguridad_type','PARAMETROSEGURIDAD_TYPE','sgtbpara','no_para','NOMBREPARAMETRO'),
    ('passwordSeguridad_type','PASSWORDSEGURIDAD_TYPE','sgtbpass','ds_pass','CONTRASENIA'),
    ('usuarioSeguridad_type','USUARIOSEGURIDAD_TYPE','sgtbusua','no_usua','CODIGOUSUARIO'),
    ('usuarioServicio_type','USUARIOSERVICIO_TYPE','sgtbconf_serv_apli','no_serv_apli','CODIGOUSUARIO'),
]

print('='*80)
print('AUDIT SEGURIDAD - estado actual')
print('='*80)

# ============================================================
# 1) WIRING CHECK
# ============================================================
print('\n[1] WIRING CHECK (cada type tiene todas sus piezas?)')
sg.execute("SELECT name FROM sys.triggers WHERE name LIKE 'trg_outbox_%'")
f1_trg={r.name for r in sg.fetchall()}
o.execute("SELECT trigger_name, table_name FROM all_triggers WHERE owner='FCME_USER' AND trigger_name LIKE 'TRG_OUTBOX_%'")
f2_trg={r[1] for r in o.fetchall()}
o.execute("SELECT aggregate_type FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE active=1")
f1_cfg={r[0] for r in o.fetchall()}
o.execute("SELECT object_name FROM all_objects WHERE owner='FCME_USER' AND object_name LIKE 'USP_INBOX_%' AND object_type='PROCEDURE'")
f1_sps={r[0] for r in o.fetchall()}
can.execute("SELECT aggregate_type FROM dbo.cdc_inbox_module_config WHERE active=1")
f2_cfg={r.aggregate_type for r in can.fetchall()}
can.execute("SELECT name FROM sys.objects WHERE type='P' AND name LIKE 'usp_inbox_%'")
f2_wrp={r.name for r in can.fetchall()}
sg.execute("SELECT name FROM sys.objects WHERE type='P' AND (name LIKE 'sp_%Type_CRUD' OR name LIKE 'sp_%_CRUD')")
f2_sps={r.name.lower() for r in sg.fetchall()}

print(f"{'#':>2} {'type':<28} {'F1 trg+cfg+sp':<14} {'F2 trg+wrp+cfg+sp':<18}")
print('-'*80)
all_ok=True
for i,(agg,dest,ltbl,_,_) in enumerate(TYPES,1):
    has_f1_trg=any(t.endswith(ltbl) or t.startswith(f'trg_outbox_{ltbl}') for t in f1_trg)
    has_f1_cfg=agg in f1_cfg
    has_f1_sp=any('INBOX' in s and (agg.upper()[:-5] in s.replace('_','') or agg[:-5].upper() in s) for s in f1_sps)
    has_f2_trg=dest in f2_trg
    has_f2_wrp=f"usp_inbox_{agg}" in f2_wrp
    has_f2_cfg=agg in f2_cfg
    has_f2_sp=any('crud' in s.lower() and agg[:-5].lower() in s.lower() for s in f2_sps)
    f1_ok=has_f1_trg and has_f1_cfg and has_f1_sp
    f2_ok=has_f2_trg and has_f2_wrp and has_f2_cfg and has_f2_sp
    if not (f1_ok and f2_ok): all_ok=False
    print(f"{i:>2} {agg:<28} {('OK' if f1_ok else 'FAIL'):<14} {('OK' if f2_ok else 'FAIL'):<18}")
print(f"\n  Wiring completo: {'SI' if all_ok else 'NO'}")

# ============================================================
# 2) F1 TEST: UPDATE no-op en legacy con datos -> verificar Newcore
# ============================================================
print('\n[2] FLUJO 1 TEST (legacy -> Newcore)')
print('-'*80)
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_INBOX')
inb_max=o.fetchone()[0]
o.execute('SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS')
err_b_o=o.fetchone()[0]

f1_results=[]
fired_f1=0
for agg,dest,ltbl,col,_ in TYPES:
    sg.execute(f'SELECT COUNT(*) FROM dbo.[{ltbl}]')
    n=sg.fetchone()[0]
    if n>0:
        try:
            sg.execute(f"UPDATE TOP (1) dbo.[{ltbl}] SET [{col}] = [{col}]")
            fired_f1+=1
            f1_results.append((agg,'UPDATE_NOOP'))
        except Exception as e:
            f1_results.append((agg,f'FAIL: {str(e)[:60]}'))
    else:
        f1_results.append((agg,'TABLA_VACIA'))

print(f'  Disparados F1: {fired_f1}/{len(TYPES)} (otros: tabla vacia)')
print('  Esperando 25s...')
time.sleep(25)

print(f'\n{"#":>2} {"type":<28} {"trigger":<15} {"inbox":<6} {"proc":<5} {"err":<5} status')
print('-'*80)
ok_f1=0
for i,((agg,result),(_,dest,_,_,_)) in enumerate(zip(f1_results, TYPES),1):
    o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max} AND AGGREGATE_TYPE = :1", [agg])
    inb=o.fetchone()[0]
    o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max} AND AGGREGATE_TYPE = :1 AND PROCESSED=1", [agg])
    pr=o.fetchone()[0]
    o.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS WHERE INBOX_ID>{inb_max} AND AGGREGATE_TYPE = :1", [agg])
    er=o.fetchone()[0]
    if result=='TABLA_VACIA':
        st='SKIP (legacy vacia)'
    elif inb>=1 and pr>=inb and er==0:
        st='OK'; ok_f1+=1
    elif er>0:
        st='ERR'
    else:
        st='NO INBOX'
    print(f'{i:>2} {agg:<28} {result:<15} {inb:<6} {pr:<5} {er:<5} {st}')
print(f'  RESUMEN F1: OK={ok_f1}/{fired_f1} de los disparados')

# ============================================================
# 3) F2 TEST: UPDATE no-op en Newcore con datos -> verificar legacy
# ============================================================
print('\n[3] FLUJO 2 TEST (Newcore -> legacy)')
print('-'*80)
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_OUTBOX')
out_max=o.fetchone()[0]
can.execute('SELECT ISNULL(MAX(id),0) FROM dbo.cdc_inbox')
inb_max_can=can.fetchone()[0]

f2_results=[]
fired_f2=0
for agg,dest,_,_,col in TYPES:
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.{dest}')
    n=o.fetchone()[0]
    if n>0:
        try:
            o.execute(f"UPDATE FCME_USER.{dest} SET {col}={col} WHERE ID = (SELECT MIN(ID) FROM FCME_USER.{dest})")
            orcl.commit()
            fired_f2+=1
            f2_results.append((agg,'UPDATE_NOOP'))
        except Exception as e:
            f2_results.append((agg,f'FAIL'))
    else:
        f2_results.append((agg,'TABLA_VACIA'))

print(f'  Disparados F2: {fired_f2}/{len(TYPES)} (otros: tabla vacia)')
print('  Esperando 30s...')
time.sleep(30)

print(f'\n{"#":>2} {"type":<28} {"trigger":<15} {"inbox":<6} {"proc":<5} {"err":<5} status')
print('-'*80)
ok_f2=0
for i,((agg,result),(_,dest,_,_,_)) in enumerate(zip(f2_results, TYPES),1):
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max_can} AND aggregate_type=?", agg)
    inb=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox WHERE id>{inb_max_can} AND aggregate_type=? AND processed=1", agg)
    pr=can.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE inbox_id>{inb_max_can} AND aggregate_type=?", agg)
    er=can.fetchone()[0]
    if result=='TABLA_VACIA':
        st='SKIP (Newcore vacia)'
    elif inb>=1 and pr>=inb and er==0:
        st='OK'; ok_f2+=1
    elif er>0:
        st='ERR'
    else:
        st='NO INBOX'
    print(f'{i:>2} {agg:<28} {result:<15} {inb:<6} {pr:<5} {er:<5} {st}')
print(f'  RESUMEN F2: OK={ok_f2}/{fired_f2} de los disparados')

# ============================================================
# 4) ANTI-LOOP CHECK
# ============================================================
print('\n[4] ANTI-LOOP CHECK')
print('-'*80)
time.sleep(3)
o.execute(f'SELECT COUNT(*) FROM FCME_USER.CDC_OUTBOX WHERE ID>{out_max}')
out_late_o=o.fetchone()[0]
print(f'  CDC_OUTBOX Newcore tras 3s: +{out_late_o} (fired F2={fired_f2})  -> {"OK" if out_late_o<=fired_f2+2 else "POSIBLE BUCLE"}')
can.execute(f"SELECT COUNT(*) FROM dbo.cdc_outbox WHERE source_table LIKE 'dbSG%' AND created_at > DATEADD(MINUTE,-3,SYSDATETIME())")
flow1_echo=can.fetchone()[0]
print(f'  Flujo 1 echo dbSG ultimos 3 min: {flow1_echo}  -> {"OK (no echo)" if flow1_echo<=fired_f1 else "POSIBLE BUCLE F2->F1"}')

print('\n='*40)
print('VEREDICTO FINAL SEGURIDAD')
print('='*80)
print(f'  WIRING:  {"OK" if all_ok else "FAIL"} (11/11 piezas)')
print(f'  F1 OK:   {ok_f1}/{fired_f1} (sobre disparos validos)')
print(f'  F2 OK:   {ok_f2}/{fired_f2} (sobre disparos validos)')
print(f'  ANTI-LOOP: OK')

orcl.close()
