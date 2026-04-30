"""Aplica cartera_flow1_outbox.sql contra SQL Server con seguridad.

Modos:
  --dry-run        : Parsea el SQL y reporta que haria, NO ejecuta nada
  --check          : Pre-flight: verifica que las tablas existan y reporta
                     triggers preexistentes potencialmente conflictivos
  --apply          : Ejecuta el SQL real (despues de --dry-run y --check)
  --only-db dbCR   : Aplica solo a una BD especifica
  --only-tbl crtoblig : Aplica solo a una tabla especifica

Uso recomendado:
  python _apply_cartera_flow1.py --check
  python _apply_cartera_flow1.py --dry-run
  python _apply_cartera_flow1.py --apply --only-tbl crtoblig   # piloto
  python _apply_cartera_flow1.py --apply                        # full
"""
import sys, re, argparse
import pyodbc

DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}

def conn(db):
    s = f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}"
    return pyodbc.connect(s, autocommit=True, timeout=30)

def parse_sql(path):
    """Parsea el SQL en bloques por (db, trg_name, batches)."""
    text = open(path,"r",encoding="utf-8").read()
    blocks = []
    current_db = None
    # split por GO
    statements = re.split(r'^\s*GO\s*$', text, flags=re.MULTILINE)

    pending = {"db":None,"trg":None,"tbl":None,"batches":[]}

    for stmt in statements:
        s = stmt.strip()
        if not s:
            continue
        # USE [db]
        m = re.search(r'^USE\s+\[(\w+)\]', s, re.MULTILINE)
        if m:
            current_db = m.group(1)
            continue
        # Detectar bloque trigger por header comment
        m = re.search(r'/\*\s*-----\s+(trg_outbox_\w+)\s+ON\s+(\w+)\.dbo\.(\w+)', s)
        if m:
            if pending["batches"]:
                blocks.append(pending)
            pending = {"db":current_db,"trg":m.group(1),"tbl":m.group(3),"batches":[s]}
        else:
            pending["batches"].append(s)

    if pending["batches"]:
        blocks.append(pending)

    # Eliminar bloques sin trigger (header del archivo)
    return [b for b in blocks if b["trg"]]

def cmd_check(blocks, args):
    """Pre-flight: tabla existe? trigger ya existe?"""
    print(f"[*] PRE-FLIGHT CHECK ({len(blocks)} triggers)\n")
    by_db = {}
    for b in blocks:
        by_db.setdefault(b["db"], []).append(b)

    issues = 0
    for db, items in by_db.items():
        if args.only_db and db != args.only_db:
            continue
        print(f"--- BD: {db} ({len(items)} triggers) ---")
        c = conn(db).cursor()
        for b in items:
            if args.only_tbl and b["tbl"] != args.only_tbl:
                continue
            # Tabla existe?
            c.execute(f"SELECT COUNT(*) FROM sys.tables WHERE name = ?", b["tbl"])
            tbl_exists = c.fetchone()[0] > 0
            # Trigger ya existe?
            c.execute(f"SELECT COUNT(*) FROM sys.triggers WHERE name = ?", b["trg"])
            trg_exists = c.fetchone()[0] > 0
            # Otros triggers en la misma tabla?
            other_triggers = []
            if tbl_exists:
                c.execute(f"""SELECT t.name FROM sys.triggers t
                              JOIN sys.tables tb ON t.parent_id = tb.object_id
                              WHERE tb.name = ? AND t.name <> ?""", b["tbl"], b["trg"])
                other_triggers = [r[0] for r in c.fetchall()]

            flag = "OK"
            note = ""
            if not tbl_exists:
                flag = "SKIP"; note = "tabla no existe"; issues += 1
            elif trg_exists:
                flag = "REPLACE"; note = "trigger MIO ya existe (sera reemplazado)"
            if other_triggers:
                note += f" | otros triggers: {', '.join(other_triggers)}"
            print(f"  [{flag:7}] {b['tbl']:<40} -> {b['trg']:<40} {note}")
        print()
    print(f"Issues: {issues}")
    return issues

def cmd_dry_run(blocks, args):
    """Imprime que haria sin ejecutar."""
    print(f"[*] DRY RUN ({len(blocks)} triggers)\n")
    by_db = {}
    for b in blocks:
        by_db.setdefault(b["db"], []).append(b)
    for db, items in by_db.items():
        if args.only_db and db != args.only_db:
            continue
        print(f"--- BD: {db} ({len(items)} triggers) ---")
        for b in items:
            if args.only_tbl and b["tbl"] != args.only_tbl:
                continue
            n_batches = len(b["batches"])
            print(f"  EXEC {b['trg']:<45} ({n_batches} batches, {sum(len(x) for x in b['batches']):,} chars)")

def cmd_apply(blocks, args):
    """Ejecuta los SQL realmente."""
    print(f"[*] APPLY ({len(blocks)} triggers)\n")
    by_db = {}
    for b in blocks:
        by_db.setdefault(b["db"], []).append(b)
    n_ok = 0; n_err = 0; n_skip = 0
    for db, items in by_db.items():
        if args.only_db and db != args.only_db:
            continue
        print(f"--- BD: {db} ---")
        cn = conn(db); cn.autocommit = True; c = cn.cursor()
        for b in items:
            if args.only_tbl and b["tbl"] != args.only_tbl:
                continue
            try:
                for batch in b["batches"]:
                    c.execute(batch)
                # Verificar trigger creado
                c.execute("SELECT COUNT(*) FROM sys.triggers WHERE name = ?", b["trg"])
                if c.fetchone()[0] > 0:
                    print(f"  OK   {b['trg']}")
                    n_ok += 1
                else:
                    print(f"  SKIP {b['trg']} (tabla no existe)")
                    n_skip += 1
            except Exception as e:
                print(f"  ERR  {b['trg']}: {str(e)[:200]}")
                n_err += 1
        cn.close()
    print(f"\nResultado: OK={n_ok} SKIP={n_skip} ERR={n_err}")
    return n_err

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--check", action="store_true")
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    ap.add_argument("--only-db", default=None)
    ap.add_argument("--only-tbl", default=None)
    ap.add_argument("--sql", default=r"C:\Users\Usuario\Downloads\Bulk\cartera_flow1_outbox.sql")
    args = ap.parse_args()

    blocks = parse_sql(args.sql)
    print(f"[+] {len(blocks)} bloques de trigger parseados de {args.sql}\n")

    if args.check:    return cmd_check(blocks, args)
    if args.dry_run:  return cmd_dry_run(blocks, args)
    if args.apply:    return cmd_apply(blocks, args)

if __name__ == "__main__":
    sys.exit(main() or 0)
