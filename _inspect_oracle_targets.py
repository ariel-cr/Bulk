"""Ve columnas de las tablas Oracle destino para los 3 Types piloto."""
import oracledb
orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!",
                        dsn="10.35.3.223:31521/XEPDB1")
c = orcl.cursor()

# buscar tablas que matchean con nuestros 3 Types
# aggregate_type = actualizacionAfiliadoType -> ACTUALIZACION_AFILIADO_TYPE
# aggregate_type = naturalTrabajoType        -> NATURALTRABAJOTYPE
# aggregate_type = personaTelefonosType      -> PERSONATELEFONOSTYPE

for tbl in ["ACTUALIZACION_AFILIADO_TYPE", "NATURALTRABAJOTYPE", "PERSONATELEFONOSTYPE"]:
    print(f"\n== FCME_USER.{tbl} ==")
    c.execute("""
      SELECT column_name, data_type, data_length, nullable
      FROM all_tab_columns
      WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id
    """, t=tbl)
    rows = c.fetchall()
    if not rows:
        print("  (no existe)")
        continue
    for r in rows:
        print(f"  {r[0]:<35} {r[1]:<15} len={r[2]}  null={r[3]}")
    # PK
    c.execute("""
      SELECT cols.column_name
      FROM all_constraints cons, all_cons_columns cols
      WHERE cons.constraint_type='P'
        AND cons.constraint_name=cols.constraint_name
        AND cons.owner=cols.owner AND cons.owner='FCME_USER'
        AND cols.table_name=:t
      ORDER BY cols.position
    """, t=tbl)
    pks = [r[0] for r in c.fetchall()]
    print(f"  PK: {pks}")
