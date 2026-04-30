"""Dump wrapper source + intenta INSERT directo para ver error real."""
import oracledb, json
ORA = dict(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")

with open(r"C:\Users\Usuario\Downloads\Bulk\_cartera_specs.json","r",encoding="utf-8") as f:
    specs = json.load(f)
agg_to_dest = {s["agg"]: s for s in specs}

cn = oracledb.connect(**ORA); cn.autocommit=False
c = cn.cursor()

for agg, sp_name in [
    ("solidarioCredito_type", "USP_INBOX_SOLIDARIOCREDITO"),
    ("fechasProcesoType",     "USP_INBOX_FECHASPROCESO"),
    ("estadoLegalType",       "USP_INBOX_ESTADOLEGAL"),
]:
    spec = agg_to_dest[agg]
    print(f"\n{'='*100}\n>>> {agg}  -> {spec['dest']}  (proc {sp_name})\n{'='*100}")
    print("\n-- Source del wrapper --")
    c.execute("""SELECT text FROM all_source WHERE owner='FCME_USER' AND name=:1
                 AND type='PROCEDURE' ORDER BY line""",[sp_name])
    for (line,) in c.fetchall():
        print(line.rstrip())

    # tomar un payload real
    c.execute("""SELECT payload FROM CDC_INBOX WHERE AGGREGATE_TYPE=:1 FETCH FIRST 1 ROWS ONLY""",[agg])
    r = c.fetchone()
    if not r: continue
    payload = r[0]
    if hasattr(payload,'read'): payload = payload.read()
    print("\n-- Test directo: INSERT con valores extraidos --")
    # construir el INSERT que el wrapper haria, ejecutarlo SIN catch
    # extraer dest_match
    dest = spec["dest"]
    dest_match = spec.get("dest_match", [])
    seen_o = set()
    cols, vals = [], []
    bind = {}
    data = json.loads(payload) if isinstance(payload,str) else json.loads(payload.decode())
    for ocol, lcol in dest_match:
        if ocol in seen_o: continue
        seen_o.add(ocol)
        cols.append(ocol)
        vals.append(":"+lcol.lower())
        bind[lcol.lower()] = data.get(lcol)
    sql = f"INSERT INTO FCME_USER.{dest} ({','.join(cols)}) VALUES ({','.join(vals)})"
    print(f"  SQL: {sql[:300]}")
    print(f"  bind: {bind}")
    try:
        c.execute(sql, bind)
        print(f"  -> OK (rollback)")
    except Exception as e:
        print(f"  -> ERR: {str(e)[:500]}")
    cn.rollback()

cn.close()
