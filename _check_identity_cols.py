"""Lista columnas IDENTITY/GENERATED ALWAYS de las tablas FCME_USER del fix.
El INSERT del wrapper NO debe incluir esas columnas."""
import oracledb
ORA = dict(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
cn = oracledb.connect(**ORA); c = cn.cursor()

c.execute("""
    SELECT table_name, column_name, generation_type, identity_options
    FROM all_tab_identity_cols
    WHERE owner='FCME_USER'
    ORDER BY table_name, column_name
""")
rows = c.fetchall()
print(f"Total IDENTITY columns en FCME_USER: {len(rows)}\n")
print(f"{'TABLE':<45} {'COLUMN':<25} {'GEN':<14}")
print("-"*90)
for r in rows:
    print(f"{r[0]:<45} {r[1]:<25} {r[2]:<14}")
cn.close()
