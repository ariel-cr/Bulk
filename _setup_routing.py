"""1) Inspecciona eventos viejos (event_type='CREATED')
2) Crea tabla cdc_table_to_types en fcme_canonicos
3) Pobla con mapeo del modulo PARTICIPE
4) Snapshot + borrado de eventos viejos
"""
import pyodbc, json

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123","database":"fcme_canonicos"}

def conn():
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};"
       f"DATABASE={DB['database']};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

cur = conn().cursor()

# 1) Inspeccionar eventos con convencion vieja
print("=== [1] Eventos viejos en cdc_outbox (event_type NOT IN INSERT/UPDATE/DELETE) ===")
cur.execute("""
  SELECT id, aggregate_type, aggregate_id, event_type, source_table, created_at,
         SUBSTRING(payload,1,300) AS p
  FROM dbo.cdc_outbox
  WHERE event_type NOT IN ('INSERT','UPDATE','DELETE')
  ORDER BY id
""")
old_rows = cur.fetchall()
for r in old_rows:
    print(f"  id={r.id} type={r.aggregate_type}  event={r.event_type}  src={r.source_table}  created={r.created_at}")
    print(f"    agg_id={r.aggregate_id}")
    print(f"    payload: {r.p}")
print(f"  TOTAL viejos: {len(old_rows)}\n")

# 2) Crear tabla de enrutamiento
print("=== [2] Crear dbo.cdc_table_to_types ===")
cur.execute("""
IF OBJECT_ID(N'dbo.cdc_table_to_types', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cdc_table_to_types (
        source_table    NVARCHAR(200) NOT NULL,
        canonical_type  NVARCHAR(200) NOT NULL,
        module_name     NVARCHAR(100) NOT NULL,
        is_active       BIT NOT NULL DEFAULT 1,
        created_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_cdc_table_to_types PRIMARY KEY (source_table, canonical_type)
    );
    CREATE INDEX IX_cdc_table_to_types_src ON dbo.cdc_table_to_types(source_table) WHERE is_active = 1;
END
""")
print("  ok\n")

# 3) Poblar
# mapeo extraido del analisis previo de los SPs sp_*Type
MAPPING = {
  "sp_actualizacionAfiliado_type":     [("dbFC","fctbafil_actu")],
  "sp_actualizacionDocumentos_type":   [("dbFC","fctbafil_info_actu_docs")],
  "sp_agendaMailAfiliado_type":        [("dbFC","fctbagen_mail")],
  "sp_areaLaboralParticipe_type":      [("dbFC","fctbarea_lbrl")],
  "sp_auditoriaAfiliado_type":         [("dbFC","fctbaudi_actu_afil"),("dbFC","fctbaudi_movi"),
                                        ("dbFC","sfct_afiliado_auditor"),("dbFC","sfct_motivo_mant_afiliados"),
                                        ("dbCT","cttbafil_audi"),("dbCT","cttbtabl_afil")],
  "sp_beneficiarioParticipe_type":     [("dbFC","sfct_beneficiario"),("dbFC","sfct_beneficiario_retiro")],
  "sp_cuentaBancariaAfiliado_type":    [("dbFC","sfct_padbs")],
  "sp_distribucionAfiliado_type":      [("dbCT","cttbmatr_dist_afil")],
  "sp_documentacionAfiliado_type":     [("dbFC","fctbafil_auto_docs"),("dbFC","fctbafil_unif"),
                                        ("dbFC","fctbcart_rpag"),("dbFC","fctbfcha_afil"),
                                        ("dbFC","fctbfcha_afil_dcto")],
  "sp_firmanteParticipe_type":         [("dbFC","sfct_firmante")],
  "sp_grupoFamiliar_type":             [("dbFC","sfct_grupo_fami")],
  "sp_informacionAdicionalAfiliado_type":[("dbFC","fctbactv_suje_cred"),("dbFC","fctbafil_dcap"),
                                          ("dbFC","fctbafil_gast_pers"),("dbFC","fctbafil_info_adic"),
                                          ("dbFC","sfct_afiliado_fondos")],
  "sp_institucion_type":               [("dbFC","fctbinst_info_adic"),("dbFC","sfct_institucion")],
  "sp_motivoContable_type":            [("dbFC","sfct_motivo_cnta_cble")],
  "sp_movimientoCuenta_type":          [("dbFC","sfct_movimiento"),("dbFC","fctbrubr_rent"),
                                        ("dbFC","fctbagru_moti_repo"),("dbFC","sfct_afiliado"),
                                        ("dbFC","sfct_afiliado_referencias"),("dbFC","sfct_banco"),
                                        ("dbFC","sfct_motivo")],
  "sp_movimientoTemporal_type":        [("dbFC","sfct_movimiento_temp")],
  "sp_naturalInformacionAdicionalType":[("dbFC","fctbafil_info_actu_docs"),("dbFC","sfct_afiliado"),
                                        ("dbFC","sfct_banco"),("dbFC","sfct_beneficiario"),
                                        ("dbFC","sfct_ciudad"),("dbFC","sfct_institucion"),
                                        ("dbNO","notbempl"),("dbNO","notbcgfm")],
  "sp_naturalInformacionBasicaType":   [("dbFC","fctbafil_info_actu_docs"),("dbFC","sfct_afiliado"),
                                        ("dbFC","sfct_beneficiario"),("dbFC","sfct_ciudad"),
                                        ("dbNO","notbempl"),("dbNO","notbcgfm")],
  "sp_naturalIngresosEgresosType":     [("dbFC","sfct_afiliado_otros"),("dbFC","sfct_afiliado_rubro")],
  "sp_naturalTrabajoType":             [("dbFC","fctbafil_actu"),("dbFC","sfct_afiliado")],
  "sp_otrosIngresosAfiliado_type":     [("dbFC","fctbotro_ingr_afil"),("dbFC","fctbotro_ingr_cony")],
  "sp_personaDireccionesType":         [("dbFC","fctbafil_info_actu_docs"),("dbFC","fctbagen_mail"),
                                        ("dbFC","sfct_afiliado"),("dbFC","sfct_banco"),
                                        ("dbFC","sfct_ciudad"),("dbFC","sfct_institucion"),
                                        ("dbCG","cgtbprvd"),("dbNO","notbempl")],
  "sp_personaReferenciasBancariasType":[("dbFC","sfct_afiliado"),("dbFC","sfct_afiliado_referencias"),
                                        ("dbFC","sfct_banco"),("dbCG","cgtbprvd")],
  "sp_personaReferenciasPersonalesType":[("dbFC","fctbafil_ahor_refe")],
  "sp_personaTelefonosType":           [("dbFC","fctbafil_actu"),("dbFC","fctbagen_telf_part"),
                                        ("dbFC","sfct_afiliado")],
  "sp_personaType":                    [("dbFC","fctbafil_info_actu_docs"),("dbFC","sfct_afiliado"),
                                        ("dbFC","sfct_banco"),("dbFC","sfct_beneficiario"),
                                        ("dbFC","sfct_ciudad"),("dbFC","sfct_institucion"),
                                        ("dbCG","cgtbprvd"),("dbNO","notbempl"),("dbNO","notbcgfm")],
  "sp_personaVinculacionesType":       [("dbFC","sfct_afiliado"),("dbFC","sfct_beneficiario"),
                                        ("dbFC","sfct_conyuge"),("dbCR","crtboper_cony"),
                                        ("dbCR","crtoblig"),("dbIM","imtbmiem_cony"),("dbNO","notbempl")],
  "sp_referenciaParticipe_type":       [("dbFC","sfct_referencias")],
  "sp_reporteSIBSParticipe_type":      [("dbFC","fctbcinf_part_sibs"),("dbFC","fctbdinf_liqd_cnta_sibs"),
                                        ("dbFC","fctbdinf_part_sibs")],
  "sp_retiroLiquidacion_type":         [("dbFC","sfct_afiliado_referencias"),("dbFC","sfct_retiro")],
  "sp_retiroVoluntarioEstado_type":    [("dbFC","fctbrvol_esta_afil")],
  "sp_rolNomina_type":                 [("dbFC","sfct_cabecera_rol"),("dbFC","sfct_detalle_rol"),
                                        ("dbFC","sfct_rubro_rol")],
  "sp_saldoDiario_type":               [("dbFC","fctbrubr_rent"),("dbFC","fctbsald_diar_afil_rubr"),
                                        ("dbFC","sfct_saldos_diarios_afiliados")],
  "sp_saldoDiarioRubro_type":          [("dbFC","fctbsald_diar_rubr")],
  "sp_seguroVidaParticipe_type":       [("dbSV","svtbcaus"),("dbSV","svtbdisc"),("dbSV","svtbefec"),
                                        ("dbSV","svtbfmpg"),("dbSV","svtbstro"),("dbSV","svtbstro_bene"),
                                        ("dbSV","svtbstro_cred"),("dbSV","svtbstro_deta"),
                                        ("dbSV","svtbstro_exte")],
  "sp_servicioAdicional_type":         [("dbFC","fctbcser_adic"),("dbFC","fctbesta_civi"),
                                        ("dbFC","fctbgene_sibs"),("dbFC","fctbpara_serv_adic"),
                                        ("dbFC","sfct_afiliado")],
}

# construir filas (source_table usa solo el nombre, sin la BD — es la convencion que emite el trigger)
rows = []
for typ, lst in MAPPING.items():
    for _, tbl in lst:
        rows.append((tbl, typ, "PARTICIPE"))
# dedup
rows = sorted(set(rows))
print(f"=== [3] Poblar cdc_table_to_types ({len(rows)} mapeos) ===")

# truncar solo filas de PARTICIPE y reinsertar (idempotente)
cur.execute("DELETE FROM dbo.cdc_table_to_types WHERE module_name='PARTICIPE'")
cur.fast_executemany = True
cur.executemany(
    "INSERT INTO dbo.cdc_table_to_types (source_table, canonical_type, module_name) VALUES (?,?,?)",
    rows
)
cur.execute("SELECT COUNT(*) FROM dbo.cdc_table_to_types WHERE module_name='PARTICIPE'")
print(f"  filas insertadas: {cur.fetchone()[0]}")

# distinct source_tables
cur.execute("SELECT COUNT(DISTINCT source_table) FROM dbo.cdc_table_to_types WHERE module_name='PARTICIPE'")
print(f"  tablas legacy distintas: {cur.fetchone()[0]}")

# distinct canonical_types
cur.execute("SELECT COUNT(DISTINCT canonical_type) FROM dbo.cdc_table_to_types WHERE module_name='PARTICIPE'")
print(f"  types canonicos distintos: {cur.fetchone()[0]}\n")

# 4) Snapshot + borrado de eventos viejos
print("=== [4] Snapshot + borrado de eventos viejos ===")
# guardarlos a tabla de archivo
cur.execute("""
IF OBJECT_ID(N'dbo.cdc_outbox_archive', N'U') IS NULL
    CREATE TABLE dbo.cdc_outbox_archive (
        archived_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        reason      NVARCHAR(200) NOT NULL,
        id BIGINT, aggregate_id NVARCHAR(200), aggregate_type NVARCHAR(200),
        event_type NVARCHAR(200), payload NVARCHAR(MAX),
        source_table NVARCHAR(200), created_at DATETIME2
    )
""")
cur.execute("""
INSERT INTO dbo.cdc_outbox_archive
    (reason, id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
SELECT N'legacy-format-pre-convivencia', id, aggregate_id, aggregate_type, event_type, payload, source_table, created_at
FROM dbo.cdc_outbox
WHERE event_type NOT IN ('INSERT','UPDATE','DELETE')
""")
print(f"  archivados: {cur.rowcount}")
cur.execute("""
DELETE FROM dbo.cdc_outbox
WHERE event_type NOT IN ('INSERT','UPDATE','DELETE')
""")
print(f"  borrados de cdc_outbox: {cur.rowcount}")

# resumen final
cur.execute("SELECT COUNT(*) FROM dbo.cdc_outbox")
print(f"\n  cdc_outbox total actual: {cur.fetchone()[0]}")
cur.execute("""
  SELECT aggregate_type, event_type, COUNT(*) n
  FROM dbo.cdc_outbox GROUP BY aggregate_type, event_type ORDER BY n DESC
""")
print("  distribucion:")
for r in cur.fetchall():
    print(f"    {r.aggregate_type:<35} {r.event_type:<10} n={r.n}")
