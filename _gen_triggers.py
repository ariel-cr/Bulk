"""Genera cdc_outbox_triggers.sql con los triggers AFTER I/U/D
para todas las tablas legacy del modulo PARTICIPE, agrupados por Type canonico."""
import pyodbc
from textwrap import dedent

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}

def conn(db):
    s = (f"DRIVER={DB['driver']};SERVER={DB['server']};"
         f"DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s)

# Mapeo Type canonico -> lista de (db_legacy, tabla) que alimenta
# Ordenados por Type, con pilotos primero
TYPE_TO_TABLES = {
  "sp_actualizacionAfiliado_type":   [("dbFC","fctbafil_actu")],
  "sp_actualizacionDocumentos_type": [("dbFC","fctbafil_info_actu_docs")],
  "sp_agendaMailAfiliado_type":      [("dbFC","fctbagen_mail")],
  "sp_areaLaboralParticipe_type":    [("dbFC","fctbarea_lbrl")],
  "sp_auditoriaAfiliado_type":       [("dbFC","fctbaudi_actu_afil"),("dbFC","fctbaudi_movi"),
                                      ("dbFC","sfct_afiliado_auditor"),("dbFC","sfct_motivo_mant_afiliados"),
                                      ("dbCT","cttbafil_audi"),("dbCT","cttbtabl_afil")],
  "sp_beneficiarioParticipe_type":   [("dbFC","sfct_beneficiario"),("dbFC","sfct_beneficiario_retiro")],
  "sp_cuentaBancariaAfiliado_type":  [("dbFC","sfct_padbs")],
  "sp_distribucionAfiliado_type":    [("dbCT","cttbmatr_dist_afil")],
  "sp_documentacionAfiliado_type":   [("dbFC","fctbafil_auto_docs"),("dbFC","fctbafil_unif"),
                                      ("dbFC","fctbcart_rpag"),("dbFC","fctbfcha_afil"),
                                      ("dbFC","fctbfcha_afil_dcto")],
  "sp_firmanteParticipe_type":       [("dbFC","sfct_firmante")],
  "sp_grupoFamiliar_type":           [("dbFC","sfct_grupo_fami")],
  "sp_informacionAdicionalAfiliado_type":[("dbFC","fctbactv_suje_cred"),("dbFC","fctbafil_dcap"),
                                          ("dbFC","fctbafil_gast_pers"),("dbFC","fctbafil_info_adic"),
                                          ("dbFC","sfct_afiliado_fondos")],
  "sp_institucion_type":             [("dbFC","fctbinst_info_adic"),("dbFC","sfct_institucion")],
  "sp_motivoContable_type":          [("dbFC","sfct_motivo_cnta_cble")],
  "sp_movimientoCuenta_type":        [("dbFC","sfct_movimiento"),("dbFC","fctbrubr_rent"),
                                      ("dbFC","fctbagru_moti_repo"),("dbFC","sfct_afiliado"),
                                      ("dbFC","sfct_afiliado_referencias"),("dbFC","sfct_banco"),
                                      ("dbFC","sfct_motivo")],
  "sp_movimientoTemporal_type":      [("dbFC","sfct_movimiento_temp")],
  "sp_naturalInformacionAdicionalType":[("dbFC","fctbafil_info_actu_docs"),("dbFC","sfct_afiliado"),
                                        ("dbFC","sfct_banco"),("dbFC","sfct_beneficiario"),
                                        ("dbFC","sfct_ciudad"),("dbFC","sfct_institucion"),
                                        ("dbNO","notbempl"),("dbNO","notbcgfm")],
  "sp_naturalInformacionBasicaType": [("dbFC","fctbafil_info_actu_docs"),("dbFC","sfct_afiliado"),
                                      ("dbFC","sfct_beneficiario"),("dbFC","sfct_ciudad"),
                                      ("dbNO","notbempl"),("dbNO","notbcgfm")],
  "sp_naturalIngresosEgresosType":   [("dbFC","sfct_afiliado_otros"),("dbFC","sfct_afiliado_rubro")],
  "sp_naturalTrabajoType":           [("dbFC","fctbafil_actu"),("dbFC","sfct_afiliado")],
  "sp_otrosIngresosAfiliado_type":   [("dbFC","fctbotro_ingr_afil"),("dbFC","fctbotro_ingr_cony")],
  "sp_personaDireccionesType":       [("dbFC","fctbafil_info_actu_docs"),("dbFC","fctbagen_mail"),
                                      ("dbFC","sfct_afiliado"),("dbFC","sfct_banco"),
                                      ("dbFC","sfct_ciudad"),("dbFC","sfct_institucion"),
                                      ("dbCG","cgtbprvd"),("dbNO","notbempl")],
  "sp_personaReferenciasBancariasType":[("dbFC","sfct_afiliado"),("dbFC","sfct_afiliado_referencias"),
                                        ("dbFC","sfct_banco"),("dbCG","cgtbprvd")],
  "sp_personaReferenciasPersonalesType":[("dbFC","fctbafil_ahor_refe")],
  "sp_personaTelefonosType":         [("dbFC","fctbafil_actu"),("dbFC","fctbagen_telf_part"),
                                      ("dbFC","sfct_afiliado")],
  "sp_personaType":                  [("dbFC","fctbafil_info_actu_docs"),("dbFC","sfct_afiliado"),
                                      ("dbFC","sfct_banco"),("dbFC","sfct_beneficiario"),
                                      ("dbFC","sfct_ciudad"),("dbFC","sfct_institucion"),
                                      ("dbCG","cgtbprvd"),("dbNO","notbempl"),("dbNO","notbcgfm")],
  "sp_personaVinculacionesType":     [("dbFC","sfct_afiliado"),("dbFC","sfct_beneficiario"),
                                      ("dbFC","sfct_conyuge"),("dbCR","crtboper_cony"),
                                      ("dbCR","crtoblig"),("dbIM","imtbmiem_cony"),("dbNO","notbempl")],
  "sp_referenciaParticipe_type":     [("dbFC","sfct_referencias")],
  "sp_reporteSIBSParticipe_type":    [("dbFC","fctbcinf_part_sibs"),("dbFC","fctbdinf_liqd_cnta_sibs"),
                                      ("dbFC","fctbdinf_part_sibs")],
  "sp_retiroLiquidacion_type":       [("dbFC","sfct_afiliado_referencias"),("dbFC","sfct_retiro")],
  "sp_retiroVoluntarioEstado_type":  [("dbFC","fctbrvol_esta_afil")],
  "sp_rolNomina_type":                [("dbFC","sfct_cabecera_rol"),("dbFC","sfct_detalle_rol"),
                                       ("dbFC","sfct_rubro_rol")],
  "sp_saldoDiario_type":             [("dbFC","fctbrubr_rent"),("dbFC","fctbsald_diar_afil_rubr"),
                                      ("dbFC","sfct_saldos_diarios_afiliados")],
  "sp_saldoDiarioRubro_type":        [("dbFC","fctbsald_diar_rubr")],
  "sp_seguroVidaParticipe_type":     [("dbSV","svtbcaus"),("dbSV","svtbdisc"),("dbSV","svtbefec"),
                                      ("dbSV","svtbfmpg"),("dbSV","svtbstro"),("dbSV","svtbstro_bene"),
                                      ("dbSV","svtbstro_cred"),("dbSV","svtbstro_deta"),
                                      ("dbSV","svtbstro_exte")],
  "sp_servicioAdicional_type":       [("dbFC","fctbcser_adic"),("dbFC","fctbesta_civi"),
                                      ("dbFC","fctbgene_sibs"),("dbFC","fctbpara_serv_adic"),
                                      ("dbFC","sfct_afiliado")],
}

# Cachear PKs y columnas
pk_cache = {}
col_cache = {}
def _cur(db):
    return conn(db).cursor()

def get_pk(db, table):
    key = (db, table)
    if key in pk_cache: return pk_cache[key]
    cur = _cur(db)
    cur.execute("""
      SELECT c.name
      FROM sys.indexes i
      JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
      JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id
      WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 1
      ORDER BY ic.key_ordinal
    """, f"dbo.{table}")
    pks = [r.name for r in cur.fetchall()]
    pk_cache[key] = pks
    return pks

def get_cols(db, table):
    key = (db, table)
    if key in col_cache: return col_cache[key]
    cur = _cur(db)
    cur.execute("""
      SELECT c.name, t.name AS tp
      FROM sys.columns c
      JOIN sys.types t ON c.user_type_id=t.user_type_id
      WHERE c.object_id = OBJECT_ID(?)
      ORDER BY c.column_id
    """, f"dbo.{table}")
    # excluir tipos LOB obsoletos que no soportan SELECT * INTO con facilidad
    cols = [r.name for r in cur.fetchall()]
    col_cache[key] = cols
    return cols

def build_trigger(db, table, types):
    trg_name = f"trg_outbox_{table}"
    full_src = f"{db}.dbo.{table}"
    pks = get_pk(db, table)
    cols = get_cols(db, table)
    types_csv = ",".join(types)
    cols_list_i = ", ".join(f"x.[{c}]" for c in cols)
    cols_list_d = cols_list_i

    if pks:
        if len(pks) == 1:
            pk_concat_ins = f"CONVERT(NVARCHAR(200), i.[{pks[0]}])"
            pk_concat_del = f"CONVERT(NVARCHAR(200), d.[{pks[0]}])"
        else:
            pk_concat_ins = "CONCAT_WS('|', " + ", ".join(
                f"CONVERT(NVARCHAR(200), i.[{c}])" for c in pks) + ")"
            pk_concat_del = "CONCAT_WS('|', " + ", ".join(
                f"CONVERT(NVARCHAR(200), d.[{c}])" for c in pks) + ")"
    else:
        # sin PK: hash SHA1 del JSON del row como aggregate_id
        pk_concat_ins = ("CONVERT(NVARCHAR(100), HASHBYTES('SHA1', "
                        "(SELECT __COLS_I__ FROM #ins x WHERE x.rn=i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2)")
        pk_concat_del = ("CONVERT(NVARCHAR(100), HASHBYTES('SHA1', "
                        "(SELECT __COLS_D__ FROM #del x WHERE x.rn=d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2)")

    sql = f"""
USE [{db}];
GO
IF OBJECT_ID(N'dbo.{trg_name}', N'TR') IS NOT NULL
    DROP TRIGGER dbo.{trg_name};
GO
-- Types canonicos que dependen de esta tabla: {types_csv}
CREATE TRIGGER dbo.{trg_name}
ON dbo.[{table}]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- Anti-loop: si el cambio proviene del inbox (replicacion), no publicar
    IF ISNULL(CONVERT(BIT, SESSION_CONTEXT(N'is_replicating')), 0) = 1
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        RETURN;

    DECLARE @op NVARCHAR(10) =
        CASE
            WHEN EXISTS(SELECT 1 FROM inserted) AND EXISTS(SELECT 1 FROM deleted) THEN N'UPDATE'
            WHEN EXISTS(SELECT 1 FROM inserted) THEN N'INSERT'
            ELSE N'DELETE'
        END;

    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #ins FROM inserted;
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS rn, * INTO #del FROM deleted;

    IF @op IN (N'INSERT', N'UPDATE')
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            {pk_concat_ins},
            N'{table}',
            @op,
            (SELECT __COLS_I__ FROM #ins x WHERE x.rn = i.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{full_src}',
            SYSUTCDATETIME()
        FROM #ins i;
    END
    ELSE
    BEGIN
        INSERT INTO fcme_canonicos.dbo.cdc_outbox
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            {pk_concat_del},
            N'{table}',
            N'DELETE',
            (SELECT __COLS_D__ FROM #del x WHERE x.rn = d.rn FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            N'{full_src}',
            SYSUTCDATETIME()
        FROM #del d;
    END

    DROP TABLE #ins;
    DROP TABLE #del;
END;
GO
""".strip() + "\n"
    # Sustituir placeholders de columnas (sin rn)
    sql = sql.replace("__COLS_I__", cols_list_i).replace("__COLS_D__", cols_list_d)
    return sql

# invertir mapeo: (db,table) -> set(types)
tbl_types = {}
for typ, tbls in TYPE_TO_TABLES.items():
    for db, t in tbls:
        tbl_types.setdefault((db, t), set()).add(typ)

# ordenar agrupado por BD para ejecutar USE por bloque
order = sorted(tbl_types.keys(), key=lambda x: (x[0], x[1]))

out = []
out.append("/* =====================================================================")
out.append("   CDC OUTBOX TRIGGERS - Modulo PARTICIPE")
out.append("   Generado automaticamente. Patron: AFTER INSERT/UPDATE/DELETE")
out.append("   Anti-loop via SESSION_CONTEXT('cdc_origin').")
out.append("   Target: fcme_canonicos.dbo.cdc_outbox")
out.append("   ===================================================================== */")
out.append("")

current_db = None
for (db, t) in order:
    if db != current_db:
        out.append("")
        out.append(f"/* ------------------------------------------------------------------")
        out.append(f"   BD ORIGINAL: {db}")
        out.append(f"   ------------------------------------------------------------------ */")
        current_db = db
    types = sorted(tbl_types[(db, t)])
    out.append(build_trigger(db, t, types))

sql_text = "\n".join(out)
path = r"C:\Users\Usuario\Downloads\Bulk\cdc_outbox_triggers.sql"
with open(path, "w", encoding="utf-8") as f:
    f.write(sql_text)

print(f"OK -> {path}")
print(f"Total tablas (triggers): {len(order)}")
by_db = {}
for (d,_) in order: by_db[d] = by_db.get(d,0)+1
for d, n in by_db.items(): print(f"  {d}: {n} triggers")
