"""Test focalizado SOLO de los 4 types nuevos."""
import pyodbc, oracledb, time, sys
from db import get_table_columns, get_pk_columns, get_fk_values
from data_generator import generate_fake_value
from config import get_connection

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1')
o=orcl.cursor()
can=sql('fcme_canonicos').cursor()

NEW=[
    ('personaFirmasType','dbIM','imtbbene_firm','PERSONAFIRMASTYPE'),
    ('imagenesType','dbFC','fctbpart_foto','IMAGENESTYPE'),
    ('comisionParticipe_type','dbCT','cttbcomi_cred','COMISIONPARTICIPE_TYPE'),
    ('juridicoInformacionBasicaType','dbFC','fctbjuri_inst','JURIDICOINFORMACIONBASICATYPE'),
]

RUN_TS=int(time.time())
RUN_OFFSET=(RUN_TS%1000000000)*100
print(f'RUN_TS={RUN_TS}', flush=True)

# Baseline
o.execute('SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_INBOX')
inb_max=o.fetchone()[0]
o.execute('SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS')
err_b=o.fetchone()[0]
print(f'baseline inbox.max_id={inb_max} errors={err_b}', flush=True)

# Inserts
for i,(agg,db,tbl,dst) in enumerate(NEW):
    print(f'\n--- {agg} ---', flush=True)
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.{dst}')
    before=o.fetchone()[0]
    print(f'  {dst} before={before}', flush=True)
    try:
        cols=get_table_columns(db,'dbo',tbl)
        conn=get_connection(db)
        pks=get_pk_columns(conn,'dbo',tbl)
        fkv=get_fk_values(conn,'dbo',tbl)
        cur=conn.cursor()
        offset=RUN_OFFSET+i*1000
        col_names=[]; vals=[]
        for c in cols:
            if c.get('is_identity'): continue
            v=generate_fake_value(c, i, offset, is_pk=(c['name'] in pks), fk_values=fkv)
            col_names.append(c['name']); vals.append(v)
        ph=','.join('?' for _ in col_names)
        sql_ins=f"INSERT INTO dbo.[{tbl}] ("+','.join(f'[{n}]' for n in col_names)+f') VALUES ({ph})'
        try:
            cur.execute(sql_ins, vals)
            print(f'  INSERT OK', flush=True)
        except Exception as e:
            msg=str(e).replace('\n',' ')
            if 'PRIMARY KEY' in msg or 'trigger execution' in msg or 'FOREIGN KEY' in msg:
                first_col=next((c['name'] for c in cols if not c.get('is_identity')), None)
                cur.execute(f"UPDATE TOP (1) dbo.[{tbl}] SET [{first_col}] = [{first_col}]")
                print(f'  INSERT failed -> UPDATE noop', flush=True)
            else:
                raise
        conn.close()
    except Exception as e:
        print(f'  ERR: {str(e)[:200]}', flush=True)
        continue

# Esperar
print('\nEsperando 60s propagacion...', flush=True)
time.sleep(60)

# Validar
print('\n--- RESULTADOS ---', flush=True)
o.execute('SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS')
err_a=o.fetchone()[0]
print(f'errors+={err_a-err_b}', flush=True)
ok=0
for i,(agg,db,tbl,dst) in enumerate(NEW):
    o.execute(f'SELECT COUNT(*) FROM FCME_USER.{dst}')
    after=o.fetchone()[0]
    o.execute(f"SELECT ERROR_MESSAGE FROM FCME_USER.CDC_INBOX_ERRORS WHERE INBOX_ID > {inb_max} AND AGGREGATE_TYPE = :1 ORDER BY ERROR_DATE DESC FETCH FIRST 1 ROWS ONLY", [agg])
    e=o.fetchone()
    err_msg=(e[0] if e else None)
    status='OK' if not err_msg else f'ERR: {err_msg[:120]}'
    print(f'  {agg:<35} -> {dst}  after={after}  {status}', flush=True)
    if not err_msg: ok+=1

print(f'\nResumen: {ok}/{len(NEW)} OK', flush=True)
orcl.close()
