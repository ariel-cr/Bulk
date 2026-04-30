"""Fix 3 SPs CRUD que fallaron por NOT NULL / truncation."""
import pyodbc
DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

# Inspect 3 tables NOT NULL cols + lengths
c=sql('dbNO').cursor()
for t in ['notbempl_audi','notbpago_nomi','notbdrol']:
    print(f'\n=== {t} cols (NOT NULL solamente) ===')
    c.execute(f"""SELECT c.name, ty.name typ, c.max_length, c.is_nullable, c.is_identity
                  FROM sys.columns c JOIN sys.types ty ON c.user_type_id=ty.user_type_id
                  WHERE c.object_id=OBJECT_ID('dbo.{t}') AND c.is_nullable=0
                  ORDER BY c.column_id""")
    for r in c.fetchall():
        print(f'  {r.name:<25} {r.typ:<12} maxlen={r.max_length} identity={r.is_identity}')
