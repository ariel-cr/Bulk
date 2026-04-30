import oracledb
ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}
orcl = oracledb.connect(**ORA); orcl.autocommit=False
o = orcl.cursor()
o.execute("UPDATE FCME_USER.CDC_INBOX_MODULE_CONFIG SET SP_NAME='USP_INBOX_CTAAUTODETA' WHERE AGGREGATE_TYPE='cuentaAutomaticaDetalle_type'")
print(f"  Updated rows: {o.rowcount}")
orcl.commit()
# Verificar
o.execute("SELECT SP_NAME, ACTIVE FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE='cuentaAutomaticaDetalle_type'")
r = o.fetchone()
print(f"  Now points to: {r[0]} active={r[1]}")
o.execute("SELECT COUNT(*) FROM all_objects WHERE owner='FCME_USER' AND object_type='PROCEDURE' AND object_name='USP_INBOX_CTAAUTODETA'")
print(f"  Wrapper exists: {o.fetchone()[0] > 0}")
orcl.close()
