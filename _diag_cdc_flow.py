"""Diagnostico flujo CDC: outbox canonicos -> kafka -> inbox newcore (SQL Server)."""
import pymssql
import json
import urllib.request
import socket
import sys

SRV = "10.35.3.64"
PORT = 1433
USER = "sa"
PWD = "YourPassword123"
KAFKA_HOSTS = [("10.35.3.64", 8083), ("10.35.3.223", 8083)]


def conn(db):
    return pymssql.connect(server=SRV, port=PORT, user=USER, password=PWD, database=db)


def section(t):
    print("\n" + "=" * 70)
    print(t)
    print("=" * 70)


# --- 1. Estado de tablas CDC en las 3 bases ---
section("1) Conteos y rangos en cdc_outbox / cdc_inbox")
for db in ["fcme_legacy", "fcme_canonicos", "fcme_newcore"]:
    print(f"\n[{db}]")
    try:
        c = conn(db).cursor(as_dict=True)
    except Exception as e:
        print(f"  ERROR conectando: {e}")
        continue
    for t in ["cdc_outbox", "cdc_inbox"]:
        try:
            c.execute(f"SELECT COUNT(*) AS n FROM dbo.{t}")
            n = c.fetchone()["n"]
            if n > 0:
                c.execute(f"SELECT MIN(id) AS mn, MAX(id) AS mx, MAX(created_at) AS mc FROM dbo.{t}")
                r = c.fetchone()
                print(f"  {t:<12} rows={n:>7}  id=[{r['mn']}..{r['mx']}]  last_created={r['mc']}")
                c.execute(f"SELECT TOP 3 id, aggregate_type, event_type, source_table, created_at FROM dbo.{t} ORDER BY id DESC")
                for row in c.fetchall():
                    print(f"     id={row['id']} type={row['aggregate_type']} ev={row['event_type']} src={row['source_table']} at={row['created_at']}")
            else:
                print(f"  {t:<12} rows=0")
        except Exception as e:
            print(f"  {t}: {e}")

# --- 2. Estado columnas de procesamiento en outbox (si existen) ---
section("2) Columnas de control de proceso en outbox/inbox")
for db in ["fcme_canonicos", "fcme_newcore"]:
    for t in ["cdc_outbox", "cdc_inbox"]:
        try:
            c = conn(db).cursor(as_dict=True)
            c.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
            """, (t,))
            cols = c.fetchall()
            if cols:
                print(f"  {db}.{t}: " + ", ".join(f"{r['COLUMN_NAME']}({r['DATA_TYPE']})" for r in cols))
        except Exception as e:
            pass

# --- 3. Conectividad a Kafka Connect ---
section("3) Kafka Connect REST")
for host, port in KAFKA_HOSTS:
    try:
        s = socket.create_connection((host, port), timeout=3)
        s.close()
        print(f"  {host}:{port} -> ABIERTO")
        try:
            with urllib.request.urlopen(f"http://{host}:{port}/connectors", timeout=5) as resp:
                data = json.loads(resp.read())
                cdc_related = [n for n in data if "cdc" in n.lower() or "canonicos" in n.lower() or "outbox" in n.lower() or "inbox" in n.lower()]
                print(f"    total conectores: {len(data)}  | cdc/canonicos relacionados: {len(cdc_related)}")
                for n in cdc_related[:30]:
                    print(f"      - {n}")
        except Exception as e:
            print(f"    error listando conectores: {e}")
    except Exception as e:
        print(f"  {host}:{port} -> CERRADO ({e})")
