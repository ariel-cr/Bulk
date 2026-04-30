"""Lee el cuerpo de TRG_PROCESS_CDC_INBOX para entender que hace en cada caso."""
import oracledb
ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}
o = oracledb.connect(**ORA).cursor()
o.execute("""SELECT line, text FROM all_source
             WHERE owner='FCME_USER' AND name='TRG_PROCESS_CDC_INBOX' AND type='TRIGGER'
             ORDER BY line""")
for ln, txt in o.fetchall():
    print(f"{ln:4} {txt.rstrip()}")
