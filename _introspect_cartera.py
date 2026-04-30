"""Fase 1 - Introspeccion read-only del modulo CARTERA.

Para cada uno de los 88 types listados:
  - Verifica existencia de FCME_USER.<TYPE_TABLE>
  - Lista columnas + PK
  - Verifica triggers preexistentes (F2): TRG_OUTBOX_<X> en Oracle
  - Verifica wrappers preexistentes: USP_INBOX_<X> Oracle + usp_inbox_<x> canonicos
  - Verifica entries en CDC_INBOX_MODULE_CONFIG (Oracle + canonicos)
  - Heuristica match tabla legacy en dbCR/dbFC/dbCG/dbCT por prefijo

Salida: _introspect_cartera.json + _introspect_cartera.csv (resumen tabular).
NO modifica nada.
"""
import json, csv, sys
import oracledb, pyodbc

DB = {'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
ORA = {'user':'fcme_user','password':'FcmeUser2025!','dsn':'10.35.3.223:31521/XEPDB1'}

TYPES = [
    "abonoExtraordinario_type","autorizacionCreditoDetalle_type","autorizacionCredito_type",
    "auxDatosCobrosAdicionalesType","calificacionCarteraDetalle_type","calificacionCartera_type",
    "cancelacionCredito_type","caucionCredito_type","cobranzaJudicialDetalle_type",
    "cobranzaJudicialDistribucion_type","cobranzaJudicial_type","conceptoGastoJudicialType",
    "contabilizacionCredito_type","convenioPagoCredito_type","costoFinancieroCredito_type",
    "creditoType","cuentaAutomaticaDetalle_type","cuentaAutomatica_type","cuentaCuotasType",
    "cuentaCxPCxCType","cuentaPersonasType","cuentaPorCobrarType","cuentaType",
    "cuentasEnLegalType","cuotaConvenio_type","cuotaCreditoType","desembolsoCredito_type",
    "desembolsoDevolucion_type","detalleRecuperacion_type","devengamientoCarteraDetalle_type",
    "devengamientoCartera_type","devolucionCredito_type","devolucionMasivaDetalle_type",
    "devolucionMasiva_type","documentoCredito_type","estadoConvenioCredito_type",
    "estadoLegalType","etapaJudicialCredito_type","fechasProcesoType","flujoTrabajoCredito_type",
    "garantiaCredito_type","gestionCobranzaAsignacion_type","gestionComunicacionCredito_type",
    "grupoCreditoDetalle_type","grupoCreditoDocumento_type","informacionLegal_type",
    "liquidacionDiariaCredito_type","medidaJudicialType","movimientoContableCredito_type",
    "obligacionRol_type","operacionConyugal_type","pagoCredito_type","pagosCreditoType",
    "personaCreditoType","personaCxPCxCType","planPagoAjuste_type","planPago_type",
    "plazoVencido_type","precalificacionCredito_type","procesoAccionType",
    "recuperacionConvenio_type","recuperacionCredito_type","referenciaCliente_type",
    "referenciaDeudor_type","refinanciamientoCreditoType","reporteSBSCabecera_type",
    "reporteSBSDetalle_type","reporteSBSGaranteCodeudor_type","reporteSBSGarantiaReal_type",
    "reporteSBSOperacionAnterior_type","reporteSBSOperacionCancelada_type",
    "reporteSBSOperacionConcedida_type","reporteSBSSaldoOperacion_type","reporteSBSSujetoRiesgo_type",
    "rubroCobranza_type","rubrosCobranzaDetalle_type","saldoCarteraDetalle_type","saldoCartera_type",
    "saldoCxPCxCType","saldoVinculadoType","seguimientoAutorizacion_type","seguroCredito_type",
    "sobranteCaucion_type","sobranteCredito_type","sobranteDistribucion_type",
    "solidarioCredito_type","tasaInteresCredito_type","tipoCredito_type","tipoSobrante_type",
    "transaccionRecuperacion_type","unidadJudicialType",
]

LEGACY_DBS = ["dbCR","dbFC","dbCG","dbCT","dbIM","dbNO","dbSV"]

SPECIAL_TABLES = {
    # Caso anomalo: tabla en Oracle tiene tilde
    "sobranteCaucion_type": "SOBRANTECAUCIÓN_TYPE",
}

def type_to_table(agg_type: str) -> str:
    """Convierte aggregate_type -> tabla destino Oracle.
    Convencion observada en FCME_USER:
      - agg_type termina en '_type' (snake) -> '<X>_TYPE' (con underscore)
      - agg_type termina en 'Type' (camel)  -> '<X>TYPE'  (adyacente)
    Casos especiales en SPECIAL_TABLES.
    """
    if agg_type in SPECIAL_TABLES:
        return SPECIAL_TABLES[agg_type]
    s = agg_type
    if s.endswith("_type"):
        return s[:-5].upper() + "_TYPE"
    if s.endswith("Type"):
        return s[:-4].upper() + "TYPE"
    return s.upper()

def trg_oracle_name(agg_type: str) -> str:
    """Nombre TRG_OUTBOX_<X> truncado a 30 chars Oracle."""
    base = type_to_table(agg_type).replace("_TYPE","")
    name = f"TRG_OUTBOX_{base}"
    return name[:30]

def usp_oracle_name(agg_type: str) -> str:
    base = type_to_table(agg_type).replace("_TYPE","")
    name = f"USP_INBOX_{base}"
    return name[:30]

def main():
    print(f"[*] Introspect CARTERA - {len(TYPES)} types")
    orcl = oracledb.connect(**ORA)
    o = orcl.cursor()

    sg = pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=master;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=30)
    sgc = sg.cursor()

    # Pre-cache todas las tablas legacy candidatas (prefijos crt*, *cred*, *cobr*, *judi*, *gar*, *recu*, *plpag*, *sbs*, *sobr*, *sgto*, etc.)
    legacy_tables = {}  # db -> [table_name]
    for db in LEGACY_DBS:
        try:
            sgc.execute(f"SELECT name FROM {db}.sys.tables WHERE type='U'")
            legacy_tables[db] = [r[0] for r in sgc.fetchall()]
        except Exception as e:
            print(f"  [!] {db} no accesible: {str(e)[:120]}")
            legacy_tables[db] = []

    results = []
    for agg in TYPES:
        dest_table = type_to_table(agg)
        trg_name = trg_oracle_name(agg)
        usp_name = usp_oracle_name(agg)

        row = {
            "agg_type": agg,
            "dest_table": dest_table,
            "trg_name": trg_name,
            "usp_name": usp_name,
            "dest_exists": False,
            "dest_columns": [],
            "dest_pk": [],
            "trg_existing": False,
            "trg_status": None,
            "usp_existing": False,
            "usp_status": None,
            "module_config_oracle": False,
            "module_config_canonicos": False,
            "wrapper_canonicos": False,
            "legacy_candidates": [],
        }

        # 1) Existencia de la tabla TYPE
        o.execute("SELECT COUNT(*) FROM all_tables WHERE owner='FCME_USER' AND table_name=:t", [dest_table])
        row["dest_exists"] = o.fetchone()[0] > 0

        if row["dest_exists"]:
            o.execute("SELECT column_name, data_type, data_length, nullable FROM all_tab_columns WHERE owner='FCME_USER' AND table_name=:t ORDER BY column_id", [dest_table])
            row["dest_columns"] = [{"name":r[0],"type":r[1],"len":r[2],"null":r[3]} for r in o.fetchall()]

            o.execute("""SELECT acc.column_name FROM all_constraints ac
                         JOIN all_cons_columns acc ON ac.owner=acc.owner AND ac.constraint_name=acc.constraint_name
                         WHERE ac.owner='FCME_USER' AND ac.table_name=:t AND ac.constraint_type='P' ORDER BY acc.position""", [dest_table])
            row["dest_pk"] = [r[0] for r in o.fetchall()]

        # 2) Trigger F2 preexistente
        o.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_type='TRIGGER' AND object_name=:t", [trg_name])
        r = o.fetchone()
        if r:
            row["trg_existing"] = True
            row["trg_status"] = r[0]

        # 3) Wrapper Oracle preexistente
        o.execute("SELECT status FROM all_objects WHERE owner='FCME_USER' AND object_type='PROCEDURE' AND object_name=:t", [usp_name])
        r = o.fetchone()
        if r:
            row["usp_existing"] = True
            row["usp_status"] = r[0]

        # 4) module_config Oracle
        try:
            o.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_MODULE_CONFIG WHERE AGGREGATE_TYPE=:t", [agg])
            row["module_config_oracle"] = o.fetchone()[0] > 0
        except Exception:
            pass

        # 5) module_config canonicos + wrapper canonicos
        try:
            sgc.execute(f"SELECT COUNT(*) FROM fcme_canonicos.dbo.cdc_inbox_module_config WHERE aggregate_type = ?", agg)
            row["module_config_canonicos"] = sgc.fetchone()[0] > 0

            wrapper = f"usp_inbox_{agg}"
            sgc.execute(f"SELECT COUNT(*) FROM fcme_canonicos.sys.objects WHERE type='P' AND name = ?", wrapper)
            row["wrapper_canonicos"] = sgc.fetchone()[0] > 0
        except Exception:
            pass

        # 6) Heuristica match tabla legacy
        # Tomamos las primeras 4-6 letras del agg en lower
        base = agg.replace("_type","").replace("Type","").lower()
        # tokens posibles (no-prefijo)
        tokens = []
        # prefijo dbCR clasico: crt + 4 letras
        tokens.append("credito" if "credito" in base else None)
        tokens.append("oblig" if "credito" in base else None)
        tokens.append("recu" if "recuper" in base else None)
        tokens.append("plpag" if "planpago" in base else None)
        tokens.append("plpag" if "cuotacredito" in base else None)
        tokens.append("solid" if "solidario" in base else None)
        tokens.append("solid" if "personagarante" in base else None)
        tokens.append("cobr" if "cobranza" in base else None)
        tokens.append("judi" if "judicial" in base or "legal" in base else None)
        tokens.append("gara" if "garantia" in base else None)
        tokens.append("seg" if "seguro" in base else None)
        tokens.append("sbs" if "sbs" in base else None)
        tokens.append("plzv" if "plazoven" in base else None)
        tokens.append("desemb" if "desembolso" in base else None)
        tokens.append("devolu" if "devolucion" in base else None)
        tokens.append("conv" if "convenio" in base else None)
        tokens.append("preca" if "precalif" in base else None)
        tokens.append("calif" if "calificacion" in base else None)
        tokens.append("aban" if "abono" in base else None)
        tokens.append("auto" if "autorizacion" in base else None)
        tokens.append("mov" if "movimiento" in base else None)
        tokens.append("conta" if "contabili" in base else None)
        tokens.append("flujo" if "flujo" in base else None)
        tokens.append("docu" if "documento" in base else None)
        tokens.append("refer" if "referencia" in base else None)
        tokens.append("rol" if "rol" in base else None)
        tokens.append("rubr" if "rubro" in base else None)
        tokens.append("cuent" if "cuent" in base else None)
        tokens.append("pago" if "pago" in base else None)
        tokens.append("sobr" if "sobrant" in base else None)
        tokens.append("conyug" if "conyug" in base else None)
        tokens.append("tasa" if "tasa" in base else None)
        tokens.append("tipo" if base.startswith("tipo") else None)
        tokens.append("perso" if "persona" in base else None)
        tokens.append("etapa" if "etapa" in base else None)
        tokens.append("medi" if "medida" in base else None)
        tokens.append("uni" if "unidad" in base else None)
        tokens.append("conce" if "concepto" in base else None)
        tokens.append("acci" if "accion" in base else None)
        tokens.append("fech" if "fecha" in base else None)
        tokens.append("seg" if "seguimiento" in base else None)
        tokens.append("cauci" if "caucion" in base else None)
        tokens.append("masiva" if "masiva" in base else None)
        tokens.append("estado" if "estado" in base else None)
        tokens.append("trab" if "flujotrabajo" in base else None)
        tokens.append("dist" if "distribu" in base else None)
        tokens.append("liq" if "liquid" in base else None)
        tokens.append("vinc" if "vincul" in base else None)
        tokens.append("trans" if "transaccion" in base else None)
        tokens.append("aux" if "aux" in base else None)
        tokens.append("rese" if "reserva" in base else None)
        tokens.append("aval" if "aval" in base else None)
        tokens.append("grupo" if "grupo" in base else None)

        tokens = [t for t in tokens if t]

        for db, tbls in legacy_tables.items():
            for t in tbls:
                tl = t.lower()
                # match si comparte al menos 1 token
                hits = [tk for tk in tokens if tk in tl]
                if hits:
                    row["legacy_candidates"].append({"db":db,"tbl":t,"matched":hits})
        # rank por num matches desc
        row["legacy_candidates"].sort(key=lambda x: (-len(x["matched"]), x["tbl"]))
        row["legacy_candidates"] = row["legacy_candidates"][:6]

        results.append(row)
        flag_dest = "OK " if row["dest_exists"] else "MISS"
        flag_trg  = "T+" if row["trg_existing"] else "T-"
        flag_usp  = "U+" if row["usp_existing"] else "U-"
        flag_cfg  = "C+" if row["module_config_oracle"] else "C-"
        cands = ",".join(c['tbl'] for c in row["legacy_candidates"][:3])
        print(f"  [{flag_dest}] {agg:<40} dest={dest_table:<38} {flag_trg} {flag_usp} {flag_cfg}  cands=[{cands}]")

    # Persistir
    with open("_introspect_cartera.json","w",encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    with open("_introspect_cartera.csv","w",encoding="utf-8",newline="") as f:
        w = csv.writer(f)
        w.writerow(["agg_type","dest_table","dest_exists","#cols","pk",
                    "trg","trg_status","usp","usp_status",
                    "cfg_oracle","cfg_canonicos","wrapper_canonicos",
                    "legacy_top1","legacy_top2","legacy_top3"])
        for r in results:
            cands = r["legacy_candidates"]
            w.writerow([
                r["agg_type"], r["dest_table"], r["dest_exists"],
                len(r["dest_columns"]), "|".join(r["dest_pk"]),
                "Y" if r["trg_existing"] else "N", r["trg_status"] or "",
                "Y" if r["usp_existing"] else "N", r["usp_status"] or "",
                "Y" if r["module_config_oracle"] else "N",
                "Y" if r["module_config_canonicos"] else "N",
                "Y" if r["wrapper_canonicos"] else "N",
                f"{cands[0]['db']}.{cands[0]['tbl']}" if len(cands)>0 else "",
                f"{cands[1]['db']}.{cands[1]['tbl']}" if len(cands)>1 else "",
                f"{cands[2]['db']}.{cands[2]['tbl']}" if len(cands)>2 else "",
            ])

    # Resumen
    n_dest = sum(1 for r in results if r["dest_exists"])
    n_trg  = sum(1 for r in results if r["trg_existing"])
    n_usp  = sum(1 for r in results if r["usp_existing"])
    n_cfg  = sum(1 for r in results if r["module_config_oracle"])
    n_can  = sum(1 for r in results if r["wrapper_canonicos"])
    print(f"\n=== RESUMEN CARTERA ({len(TYPES)} types) ===")
    print(f"  Tabla TYPE existe en FCME_USER       : {n_dest}/{len(TYPES)}")
    print(f"  TRG_OUTBOX_* preexistente (F2)        : {n_trg}/{len(TYPES)}")
    print(f"  USP_INBOX_* wrapper Oracle preexist.  : {n_usp}/{len(TYPES)}")
    print(f"  CDC_INBOX_MODULE_CONFIG Oracle        : {n_cfg}/{len(TYPES)}")
    print(f"  Wrapper canonicos preexist.           : {n_can}/{len(TYPES)}")
    print(f"\nDetalle JSON: _introspect_cartera.json")
    print(f"Resumen CSV : _introspect_cartera.csv")

    o.close(); orcl.close(); sg.close()

if __name__ == "__main__":
    main()
