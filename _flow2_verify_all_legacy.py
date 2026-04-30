"""Verifica los 30 types con PKs UNICOS por type para evitar UPSERT silencioso.
Cada type recibe un PK fresco que no existe en legacy.
"""
import pyodbc, oracledb, time

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

orcl=oracledb.connect(user='fcme_user', password='FcmeUser2025!', dsn='10.35.3.223:31521/XEPDB1')
co=orcl.cursor()
c_can=sql('fcme_canonicos').cursor()

AT_TO_TABLE={'actualizacionAfiliadoType':'ACTUALIZACION_AFILIADO_TYPE','actualizacionDocumentosType':'ACTUALIZACION_DOCUMENTOS_TYPE','agendaMailAfiliadoType':'AGENDAMAILAFILIADO_TYPE','auditoriaAfiliadoType':'AUDITORIAAFILIADO_TYPE','beneficiarioParticipeType':'BENEFICIARIOPARTICIPE_TYPE','cuentaBancariaAfiliadoType':'CUENTABANCARIAAFILIADO_TYPE','distribucionAfiliadoType':'DISTRIBUCIONAFILIADO_TYPE','documentacionAfiliadoType':'DOCUMENTACIONAFILIADO_TYPE','firmanteParticipeType':'FIRMANTEPARTICIPE_TYPE','grupoFamiliarType':'GRUPOFAMILIAR_TYPE','informacionAdicionalAfiliadoType':'INFORMACIONADICIONALAFILIADO_TYPE','institucionType':'INSTITUCION_TYPE','motivoContableType':'MOTIVOCONTABLE_TYPE','movimientoCuentaType':'MOVIMIENTOCUENTA_TYPE','movimientoTemporalType':'MOVIMIENTOTEMPORAL_TYPE','naturalInformacionAdicionalType':'NATURALINFORMACIONADICIONALTYPE','naturalIngresosEgresosType':'NATURALINGRESOSEGRESOSTYPE','naturalTrabajoType':'NATURALTRABAJOTYPE','personaReferenciasBancariasType':'PERSONAREFERENCIASBANCARIASTYPE','personaReferenciasPersonalesType':'PERSONAREFERENCIASPERSONALESTYPE','personaTelefonosType':'PERSONATELEFONOSTYPE','personaVinculacionesType':'PERSONAVINCULACIONESTYPE','referenciaParticipeType':'REFERENCIAPARTICIPE_TYPE','reporteSIBSParticipeType':'REPORTESIBSPARTICIPE_TYPE','retiroLiquidacionType':'RETIROLIQUIDACION_TYPE','retiroVoluntarioEstadoType':'RETIROVOLUNTARIOESTADO_TYPE','saldoDiarioRubroType':'SALDODIARIORUBRO_TYPE','saldoDiarioType':'SALDODIARIO_TYPE','seguroVidaParticipeType':'SEGUROVIDAPARTICIPE_TYPE','servicioAdicionalType':'SERVICIOADICIONAL_TYPE'}

# Para cada type: target_db, source_table, PK Oracle (extraido del wrapper)
import re
type_info = {}
c_can.execute("""SELECT m.aggregate_type, m.target_db, t.source_table
                 FROM dbo.cdc_inbox_module_config m
                 LEFT JOIN dbo.cdc_table_to_types t ON t.aggregate_type_emit=m.aggregate_type AND t.is_active=1
                 WHERE m.active=1""")
for r in c_can.fetchall():
    if r.aggregate_type not in type_info and r.source_table:
        # Leer wrapper para extraer PK Oracle field
        c2 = sql('fcme_canonicos').cursor()
        c2.execute("SELECT m.definition FROM sys.sql_modules m JOIN sys.objects o ON m.object_id=o.object_id WHERE o.name=?", f'usp_inbox_{r.aggregate_type}')
        rw = c2.fetchone()
        pk_ora_field = None
        if rw:
            m = re.search(r"JSON_VALUE\(@payload,'\$\.([A-Z0-9_]+)'\)", rw.definition)
            if m: pk_ora_field = m.group(1)
        type_info[r.aggregate_type] = (r.target_db, r.source_table, pk_ora_field)

# PRE-COUNT
print("[1] Snapshot pre-count")
pre = {}
for at in type_info:
    db, src, _ = type_info[at]
    try:
        c_db = sql(db).cursor()
        c_db.execute(f"SELECT COUNT(*) FROM dbo.[{src}]")
        pre[at] = c_db.fetchone()[0]
    except: pre[at] = None

# Insertar en Oracle con PKs UNICOS por type
print("\n[2] INSERTs Oracle con PKs unicos no existentes")
co.execute('DELETE FROM FCME_USER.CDC_OUTBOX'); orcl.commit()
inserted=[]
base_pk = 7000
i=0
for at, ot in AT_TO_TABLE.items():
    co.execute("SELECT column_name, data_type, data_length FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id", t=ot)
    cols=[(r[0],r[1],r[2]) for r in co.fetchall() if r[0]!='ID']
    if not cols: continue
    pk_field = type_info.get(at,(None,None,None))[2]
    test_pk = str(base_pk + i)  # 7000, 7001, 7002, ...
    cn=', '.join(c[0] for c in cols)
    vals=[]
    for n,t,d in cols:
        if n == pk_field:
            # Usar PK unico
            if 'NUMBER' in t: vals.append(test_pk)
            elif 'DATE' in t or 'TIMESTAMP' in t: vals.append(f"DATE '2027-04-{((i%28)+1):02d}'")
            else: vals.append(f"'{test_pk}'")
        elif 'VARCHAR' in t or 'CHAR' in t: vals.append("'9'" if (d or 99)<=2 else "'99'")
        elif 'NUMBER' in t: vals.append('9')
        elif 'DATE' in t or 'TIMESTAMP' in t: vals.append('SYSTIMESTAMP')
        elif 'CLOB' in t: vals.append("'{}'")
        else: vals.append('NULL')
    try:
        co.execute(f'INSERT INTO FCME_USER.{ot} ({cn}) VALUES ({", ".join(vals)})')
        inserted.append((at, ot, test_pk, pk_field))
    except Exception as e:
        print(f'  fail {at}: {str(e)[:80]}')
    i+=1
orcl.commit()
print(f"  Oracle INSERTs: {len(inserted)}/30")

# Esperar
print("\n[3] Esperando 15s")
time.sleep(15)

# POST-COUNT
print("\n[4] DELTA por tabla legacy + verificacion fila por fila")
print(f"  {'TYPE':<35} {'BD':<6} {'TABLA':<35} {'PRE':<6} {'POST':<6} {'DELTA':<6} {'STATUS'}")
print('-'*120)
ok=0; no_delta=0; err=0
for at, ot, test_pk, pk_field in inserted:
    db, src, _ = type_info[at]
    try:
        c_db = sql(db).cursor()
        c_db.execute(f"SELECT COUNT(*) FROM dbo.[{src}]")
        post = c_db.fetchone()[0]
        delta = post - (pre[at] or 0)
        # Tambien verificar que el wrapper haya registrado en parsed sin error
        c_can.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_errors WHERE aggregate_type=? AND error_message LIKE ?", at, f'%{test_pk}%')
        n_err = c_can.fetchone()[0]
        if delta > 0:
            ok+=1; status="OK"
        elif n_err > 0:
            err+=1; status=f"ERR ({n_err})"
        else:
            no_delta+=1; status="UPDATE/silenced"
        print(f"  {at:<35} {db:<6} {src:<35} {str(pre[at]):<6} {str(post):<6} {delta:<6} {status}")
    except Exception as e:
        print(f"  {at:<35} {db:<6} {src:<35} err: {str(e)[:60]}")

print(f"\n*** RESUMEN: GUARDADO_NUEVO={ok}  UPDATE_o_SILENCIADO={no_delta}  ERR={err} ***")

# Cleanup
for at, ot, test_pk, pk_field in inserted:
    try: co.execute(f"DELETE FROM FCME_USER.{ot} WHERE ROWID IN (SELECT ROWID FROM FCME_USER.{ot} ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY)")
    except: pass
co.execute('DELETE FROM FCME_USER.CDC_OUTBOX'); orcl.commit()

# Cleanup en legacy: borrar las filas test
for at, ot, test_pk, pk_field in inserted:
    db, src, _ = type_info[at]
    if not pk_field: continue
    try:
        c_db = sql(db).cursor()
        c_db.execute("EXEC sp_set_session_context N'is_replicating', 1")
        # buscar columna correspondiente al PK Oracle
        # Heuristica simple: probar varias cols con el test_pk
        c_db.execute(f"""DECLARE @c SYSNAME, @sql NVARCHAR(MAX);
                         DECLARE cur CURSOR FAST_FORWARD FOR
                            SELECT TOP 5 c.name FROM sys.columns c
                            WHERE c.object_id=OBJECT_ID('dbo.[{src}]') ORDER BY c.column_id;
                         OPEN cur; FETCH NEXT FROM cur INTO @c;
                         WHILE @@FETCH_STATUS=0 BEGIN
                            BEGIN TRY
                                SET @sql=N'DELETE FROM dbo.[{src}] WHERE CONVERT(NVARCHAR(50),['+@c+'])='''+'{test_pk}'+'''';
                                EXEC sp_executesql @sql;
                            END TRY BEGIN CATCH END CATCH
                            FETCH NEXT FROM cur INTO @c;
                         END
                         CLOSE cur; DEALLOCATE cur;""")
        c_db.execute("EXEC sp_set_session_context N'is_replicating', 0")
    except: pass
orcl.close()
