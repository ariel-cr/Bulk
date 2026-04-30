"""Revisa cuerpo del SP y del trigger compound en Oracle."""
import oracledb
orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!", dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()

print("== USP_INBOX_PARTICIPES (primeras 80 lineas) ==")
co.execute("""SELECT text FROM all_source
              WHERE owner='FCME_USER' AND name='USP_INBOX_PARTICIPES' AND type='PROCEDURE'
              ORDER BY line FETCH FIRST 80 ROWS ONLY""")
for r in co.fetchall(): print(r[0].rstrip())

print("\n== TRG_PROCESS_CDC_INBOX ==")
co.execute("""SELECT text FROM all_source
              WHERE owner='FCME_USER' AND name='TRG_PROCESS_CDC_INBOX' AND type='TRIGGER'
              ORDER BY line""")
for r in co.fetchall(): print(r[0].rstrip())
