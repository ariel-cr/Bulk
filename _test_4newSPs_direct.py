"""Test directo de los 4 SPs nuevos: inyecta evento en CDC_INBOX."""
import oracledb, json, time
o=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
c=o.cursor()

TESTS=[
    ('personaFirmasType','dbIM.dbo.imtbbene_firm','PERSONAFIRMASTYPE',
     {'co_bene':'TEST_BENE_1','sc_vivi':'1','co_prog':'X','fe_firm':'2026-01-01','ds_obse':'tst'}),
    ('imagenesType','dbFC.dbo.fctbpart_foto','IMAGENESTYPE',
     {'ci_cedu':'TEST_IMG_1','no_arch':'foto_test.jpg','ds_ruta':'/tmp/foto.jpg'}),
    ('comisionParticipe_type','dbCT.dbo.cttbcomi_cred','COMISIONPARTICIPE_TYPE',
     {'ti_cred':'1','aa_cred':'2026','qs_cred':'1','ci_ejec':'TEST_EJEC_1','st_comi':'A'}),
    ('juridicoInformacionBasicaType','dbFC.dbo.fctbjuri_inst','JURIDICOINFORMACIONBASICATYPE',
     {'co_juri':'TEST_JUR_1','co_empr':'1','ds_juri':'Test Juri','st_regi':'A'}),
]

c.execute("SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_INBOX")
inb_max=c.fetchone()[0]
print(f"baseline inbox.max_id={inb_max}")

results=[]
for agg, src, dest, payload in TESTS:
    c.execute(f"SELECT COUNT(*) FROM FCME_USER.{dest}")
    before=c.fetchone()[0]
    # Insertar en CDC_INBOX directamente (esto dispara TRG_PROCESS_CDC_INBOX que llama al SP via module_config)
    pl_json=json.dumps(payload)
    try:
        c.execute("""INSERT INTO FCME_USER.CDC_INBOX
                     (ID, AGGREGATE_TYPE, AGGREGATE_ID, EVENT_TYPE, SOURCE_TABLE, PAYLOAD, PROCESSED)
                     VALUES (FCME_USER.CDC_INBOX_SEQ.NEXTVAL, :1, :2, 'INSERT', :3, :4, 0)""",
                  [agg, payload.get('co_bene') or payload.get('ci_cedu') or 'X', src, pl_json])
        o.commit()
    except Exception as e:
        print(f"  {agg}: INSERT INBOX FAIL: {e}")
        continue
    time.sleep(1)
    c.execute(f"SELECT COUNT(*) FROM FCME_USER.{dest}")
    after=c.fetchone()[0]
    delta=after-before
    # ver errores nuevos
    c.execute(f"SELECT ERROR_MESSAGE FROM FCME_USER.CDC_INBOX_ERRORS WHERE INBOX_ID > {inb_max} AND AGGREGATE_TYPE = :1 ORDER BY ERROR_DATE DESC", [agg])
    errs=c.fetchall()
    err_str=errs[0][0][:200] if errs else None
    status='OK' if delta>=1 else f'NO ROW (err: {err_str})' if err_str else 'NO ROW'
    print(f"  {agg:<35} {dest:<35}  before={before} after={after} delta={delta}  {status}")
    results.append((agg, dest, delta, err_str))

print()
print(f"Resumen: {sum(1 for r in results if r[2]>=1)}/{len(results)} OK")
o.close()
