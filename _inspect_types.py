"""Inspecciona SPs participes.sp_*Type (sin _crud ni _dep) en fcme_legacy
y detecta BDs originales y tablas referenciadas por cada Type."""
import re
import pyodbc
from collections import defaultdict

DB_CONFIG = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123",
    "database": "fcme_legacy",
}

KNOWN_DBS = {
    "dbAD","dbAF","dbBi","dbCG","dbCGH","dbCI","dbCR","dbCRH","dbCT","dbCU",
    "dbEN","dbFC","dbFCH","dbGN","dbIESS_CAM","dbIM","dbIN","dbMT","dbNG",
    "dbNO","dbRC","dbRG","dbRGCV","dbRN","dbSC","dbSG","dbSV","dbTS","dbZN",
    "dbafluis","dbcgluis","dbcrluis","fcme_canonicos","fcme_legacy","fcme_newcore",
}

def get_conn():
    s = (f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};"
         f"DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};"
         f"PWD={DB_CONFIG['password']}")
    return pyodbc.connect(s)

def list_type_sps(cur):
    # Solo los _type / Type: terminan en 'type' o 'Type' (sin _crud y sin _dep)
    cur.execute("""
        SELECT s.name AS schema_name, o.name AS sp_name
        FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type = 'P'
          AND s.name = 'participes'
          AND o.name NOT LIKE '%_crud'
          AND o.name NOT LIKE '%_dep'
          AND (o.name LIKE '%_type' OR o.name LIKE '%Type')
        ORDER BY o.name
    """)
    return [(r.schema_name, r.sp_name) for r in cur.fetchall()]

def get_sp_body(cur, schema, name):
    cur.execute(
        "SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS body",
        f"[{schema}].[{name}]"
    )
    row = cur.fetchone()
    return row.body if row and row.body else ""

# db.schema.table (con o sin corchetes)
TABLE_REF_RE = re.compile(
    r"""(?ix)
    (?:\[(?P<db1>[A-Za-z0-9_]+)\]|(?P<db2>[A-Za-z0-9_]+))
    \.
    (?:\[(?P<sch1>[A-Za-z0-9_]+)\]|(?P<sch2>[A-Za-z0-9_]+))?
    \.
    (?:\[(?P<tbl1>[A-Za-z0-9_]+)\]|(?P<tbl2>[A-Za-z0-9_]+))
    """
)

def analyze(body):
    dbs = defaultdict(set)   # db -> set "schema.table"
    for m in TABLE_REF_RE.finditer(body):
        db = m.group("db1") or m.group("db2")
        sch = m.group("sch1") or m.group("sch2") or ""
        tbl = m.group("tbl1") or m.group("tbl2")
        if db in KNOWN_DBS:
            dbs[db].add(f"{sch}.{tbl}" if sch else tbl)
    return dbs

def main():
    conn = get_conn()
    cur = conn.cursor()
    sps = list_type_sps(cur)
    print(f"TOTAL SPs Type en participes: {len(sps)}\n")
    print("="*130)
    print(f"{'TYPE SP':<52} {'BDs originales':<25} TABLAS ORIGINALES")
    print("="*130)

    detail = {}
    for schema, name in sps:
        body = get_sp_body(cur, schema, name)
        dbs = analyze(body)
        detail[name] = dbs
        dbs_list = ", ".join(sorted(dbs.keys())) or "(ninguna externa)"
        tabs = []
        for db in sorted(dbs.keys()):
            for t in sorted(dbs[db]):
                tabs.append(f"{db}.{t}")
        # imprime con wrap simple
        first = True
        if not tabs:
            print(f"{name:<52} {dbs_list:<25} -")
            continue
        for t in tabs:
            if first:
                print(f"{name:<52} {dbs_list:<25} {t}")
                first = False
            else:
                print(f"{'':<52} {'':<25} {t}")
    conn.close()

    # Resumen inverso: BD -> Types que la usan
    print("\n\n" + "="*80)
    print("RESUMEN: BD ORIGINAL  ->  TYPES que la referencian")
    print("="*80)
    by_db = defaultdict(set)
    for sp, dbs in detail.items():
        for db in dbs.keys():
            by_db[db].add(sp)
    for db in sorted(by_db.keys()):
        print(f"\n{db}  ({len(by_db[db])} Types)")
        for sp in sorted(by_db[db]):
            print(f"   - {sp}")

    # Resumen inverso: tabla -> Types
    print("\n\n" + "="*80)
    print("RESUMEN: TABLA ORIGINAL  ->  TYPES")
    print("="*80)
    by_tab = defaultdict(set)
    for sp, dbs in detail.items():
        for db, tabs in dbs.items():
            for t in tabs:
                by_tab[f"{db}.{t}"].add(sp)
    for t in sorted(by_tab.keys()):
        print(f"\n{t}")
        for sp in sorted(by_tab[t]):
            print(f"   - {sp}")

if __name__ == "__main__":
    main()
