"""Analiza fcme_canonicos.participes.sp_*Type (sin _crud/_dep):
- Lista las tablas y BDs originales (legacy) referenciadas por cada SP.
- Genera resumen por BD y por tabla."""
import re
import pyodbc
from collections import defaultdict

DB = {
    "server": "10.35.3.64,1433",
    "driver": "{SQL Server}",
    "username": "sa",
    "password": "YourPassword123",
    "database": "fcme_canonicos",
}

KNOWN_DBS = {
    "dbAD","dbAF","dbBi","dbCG","dbCGH","dbCI","dbCR","dbCRH","dbCT","dbCU",
    "dbEN","dbFC","dbFCH","dbGN","dbIESS_CAM","dbIM","dbIN","dbMT","dbNG",
    "dbNO","dbRC","dbRG","dbRGCV","dbRN","dbSC","dbSG","dbSV","dbTS","dbZN",
    "dbafluis","dbcgluis","dbcrluis","fcme_canonicos","fcme_legacy","fcme_newcore",
}
# BDs del sistema para excluir del listado
SYS_DBS = {"fcme_canonicos","fcme_legacy","fcme_newcore"}

def conn():
    s = (f"DRIVER={DB['driver']};SERVER={DB['server']};"
         f"DATABASE={DB['database']};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s)

def list_type_sps(cur):
    cur.execute("""
        SELECT s.name AS sch, o.name AS nm
        FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id
        WHERE o.type='P' AND s.name='participes'
          AND (o.name LIKE '%Type' OR o.name LIKE '%_type')
          AND o.name NOT LIKE '%_crud' AND o.name NOT LIKE '%_dep'
        ORDER BY o.name
    """)
    return [(r.sch, r.nm) for r in cur.fetchall()]

def get_body(cur, sch, nm):
    cur.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?)) AS b", f"[{sch}].[{nm}]")
    r = cur.fetchone()
    return r.b or ""

TBL_RE = re.compile(r"""(?ix)
    (?:\[(?P<db1>[A-Za-z0-9_]+)\]|(?P<db2>[A-Za-z0-9_]+))
    \.
    (?:\[(?P<sch1>[A-Za-z0-9_]+)\]|(?P<sch2>[A-Za-z0-9_]+))?
    \.
    (?:\[(?P<tbl1>[A-Za-z0-9_]+)\]|(?P<tbl2>[A-Za-z0-9_]+))
""")

def analyze(body):
    dbs = defaultdict(set)
    for m in TBL_RE.finditer(body):
        db = m.group("db1") or m.group("db2")
        sch = m.group("sch1") or m.group("sch2") or ""
        tbl = m.group("tbl1") or m.group("tbl2")
        if db in KNOWN_DBS and db not in SYS_DBS:
            key = f"{sch}.{tbl}" if sch else tbl
            dbs[db].add(key)
    return dbs

def main():
    cur = conn().cursor()
    sps = list_type_sps(cur)
    print(f"TYPES encontrados: {len(sps)}\n")
    detail = {}
    print("="*140)
    print(f"{'TYPE SP':<52} {'BDs':<25} TABLAS ORIGINALES")
    print("="*140)
    for sch, nm in sps:
        dbs = analyze(get_body(cur, sch, nm))
        detail[nm] = dbs
        dbs_list = ", ".join(sorted(dbs.keys())) or "(ninguna)"
        tabs = [f"{db}.{t}" for db in sorted(dbs) for t in sorted(dbs[db])]
        if not tabs:
            print(f"{nm:<52} {dbs_list:<25} -"); continue
        print(f"{nm:<52} {dbs_list:<25} {tabs[0]}")
        for t in tabs[1:]:
            print(f"{'':<52} {'':<25} {t}")

    # BD -> Types
    print("\n" + "="*80)
    print("RESUMEN: BD ORIGINAL -> TYPES que la usan")
    print("="*80)
    by_db = defaultdict(set)
    for sp, dbs in detail.items():
        for db in dbs: by_db[db].add(sp)
    for db in sorted(by_db):
        print(f"\n[{db}]  ({len(by_db[db])} types)")
        for sp in sorted(by_db[db]): print(f"   - {sp}")

    # Tabla -> Types
    print("\n" + "="*80)
    print("RESUMEN: TABLA ORIGINAL -> TYPES")
    print("="*80)
    by_tab = defaultdict(set)
    for sp, dbs in detail.items():
        for db, tabs in dbs.items():
            for t in tabs: by_tab[f"{db}.{t}"].add(sp)
    for t in sorted(by_tab):
        print(f"\n{t}")
        for sp in sorted(by_tab[t]): print(f"   - {sp}")

if __name__ == "__main__":
    main()
