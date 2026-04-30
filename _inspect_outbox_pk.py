"""Inspecciona estructura de cdc_outbox en fcme_legacy y las PKs
de las tablas legacy del modulo PARTICIPE."""
import pyodbc
from collections import defaultdict

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}

TABLES = {
    "dbFC": [
        "fctbafil_actu","fctbafil_info_actu_docs","fctbagen_mail","fctbarea_lbrl",
        "fctbaudi_actu_afil","fctbaudi_movi","sfct_afiliado_auditor",
        "sfct_motivo_mant_afiliados","sfct_beneficiario","sfct_beneficiario_retiro",
        "sfct_padbs","fctbafil_auto_docs","fctbafil_unif","fctbcart_rpag",
        "fctbfcha_afil","fctbfcha_afil_dcto","sfct_firmante","sfct_grupo_fami",
        "fctbactv_suje_cred","fctbafil_dcap","fctbafil_gast_pers","fctbafil_info_adic",
        "sfct_afiliado_fondos","fctbinst_info_adic","sfct_institucion",
        "sfct_motivo_cnta_cble","sfct_movimiento","fctbrubr_rent","fctbagru_moti_repo",
        "sfct_afiliado","sfct_afiliado_referencias","sfct_banco","sfct_motivo",
        "sfct_movimiento_temp","sfct_ciudad","sfct_afiliado_otros","sfct_afiliado_rubro",
        "fctbotro_ingr_afil","fctbotro_ingr_cony","fctbagen_telf_part",
        "sfct_conyuge","fctbafil_ahor_refe","sfct_referencias","fctbcinf_part_sibs",
        "fctbdinf_liqd_cnta_sibs","fctbdinf_part_sibs","sfct_retiro",
        "fctbrvol_esta_afil","sfct_cabecera_rol","sfct_detalle_rol","sfct_rubro_rol",
        "fctbsald_diar_afil_rubr","sfct_saldos_diarios_afiliados","fctbsald_diar_rubr",
        "fctbcser_adic","fctbesta_civi","fctbgene_sibs","fctbpara_serv_adic",
    ],
    "dbNO": ["notbempl","notbcgfm"],
    "dbCG": ["cgtbprvd"],
    "dbCT": ["cttbafil_audi","cttbmatr_dist_afil","cttbtabl_afil"],
    "dbCR": ["crtboper_cony","crtoblig"],
    "dbIM": ["imtbmiem_cony"],
    "dbSV": ["svtbcaus","svtbdisc","svtbefec","svtbfmpg","svtbstro",
             "svtbstro_bene","svtbstro_cred","svtbstro_deta","svtbstro_exte"],
}

def conn(db):
    s = (f"DRIVER={DB['driver']};SERVER={DB['server']};"
         f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s)

# 1) cdc_outbox schema en fcme_legacy
print("== cdc_outbox en fcme_legacy ==")
c = conn("fcme_legacy").cursor()
c.execute("""
  SELECT c.name, t.name AS data_type, c.max_length, c.is_nullable, c.is_identity,
         OBJECT_SCHEMA_NAME(c.object_id) AS sch
  FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
  WHERE c.object_id = OBJECT_ID('dbo.cdc_outbox')
  ORDER BY c.column_id
""")
for r in c.fetchall():
    print(f"  {r.name:<30} {r.data_type:<15} max={r.max_length:<6} null={r.is_nullable}  id={r.is_identity}")

# 2) PKs por tabla
print("\n== PKs de tablas legacy ==")
pk_map = {}
for db, tables in TABLES.items():
    try:
        cur = conn(db).cursor()
    except Exception as e:
        print(f"\n{db}: NO ACCESIBLE ({e})"); continue
    print(f"\n--- {db} ---")
    for t in tables:
        cur.execute("""
          SELECT c.name
          FROM sys.indexes i
          JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
          JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id
          WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 1
          ORDER BY ic.key_ordinal
        """, f"dbo.{t}")
        pks = [r.name for r in cur.fetchall()]
        pk_map[f"{db}.{t}"] = pks
        print(f"  {t:<40} PK: {pks if pks else '(sin PK)'}")

# salida resumen
print("\n== MAP PKs ==")
for k, v in pk_map.items():
    print(f"  {k} -> {v}")
