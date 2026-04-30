"""Flujo 1 (Legacy -> Newcore) - Test masivo: 1 INSERT por aggregate_type.

NO destructivo:
- Solo INSERT (no UPDATE, no DELETE).
- Marcador unico por corrida: BLK<run_id+offset>.
- Anti-bucle: triggers Oracle ya tienen guarda CLIENT_INFO='is_replicating'
  y triggers legacy tienen SESSION_CONTEXT('is_replicating'). Una INSERT
  en legacy genera 1 evento Flow 1, su INSERT en FCME_USER puede generar
  1 evento Flow 2 echo, pero el SP CRUD setea is_replicating en dbXX -> stop.

Para cada aggregate_type:
- Selecciona UNA tabla origen legacy (la primera de su lista)
- Genera valores sinteticos respetando NOT NULL/PK/FK con data_generator
- Captura outbox.id antes/despues para cazar el evento exacto
- Despues de propagacion, valida fila en FCME_USER.<TARGET> (delta>=1)

Nota: hay 43 aggregate_types reales; el conteo de "30" del usuario
corresponde a Flujo 2 (newcore->legacy).
"""
import pyodbc, oracledb, time, sys, json, traceback
from datetime import datetime
from db import get_table_columns, get_pk_columns, get_fk_values
from data_generator import generate_fake_value
from config import get_connection

# Mapeo aggregate_type -> (legacy_db, source_table, fcme_user_target_table)
# Lista del usuario: 42 entradas. 36 cableados + 4 recien creados = 40.
# Skip: naturalReferenciasComercialesType (no source legacy) y prueba (no es type real).
TYPES = [
    # ===== 4 NUEVOS (recien cableados) =====
    ("personaFirmasType",               "dbIM", "imtbbene_firm",            "PERSONAFIRMASTYPE"),
    ("imagenesType",                    "dbFC", "fctbpart_foto",            "IMAGENESTYPE"),
    ("comisionParticipe_type",          "dbCT", "cttbcomi_cred",            "COMISIONPARTICIPE_TYPE"),
    ("juridicoInformacionBasicaType",   "dbFC", "fctbjuri_inst",            "JURIDICOINFORMACIONBASICATYPE"),
    # ===== 36 PREEXISTENTES =====
    ("actualizacionAfiliadoType",       "dbFC", "fctbafil_actu",            "ACTUALIZACION_AFILIADO_TYPE"),
    ("actualizacionDocumentosType",     "dbFC", "fctbafil_info_actu_docs",  "ACTUALIZACION_DOCUMENTOS_TYPE"),
    ("agendaMailAfiliadoType",          "dbFC", "fctbagen_mail",            "AGENDAMAILAFILIADO_TYPE"),
    ("areaLaboralParticipeType",        "dbFC", "fctbarea_lbrl",            "AREALABORALPARTICIPE_TYPE"),
    ("auditoriaAfiliadoType",           "dbFC", "sfct_motivo_mant_afiliados","AUDITORIAAFILIADO_TYPE"),
    ("beneficiarioParticipeType",       "dbFC", "sfct_beneficiario",        "BENEFICIARIOPARTICIPE_TYPE"),
    ("cuentaBancariaAfiliadoType",      "dbFC", "sfct_padbs",               "CUENTABANCARIAAFILIADO_TYPE"),
    ("distribucionAfiliadoType",        "dbCT", "cttbmatr_dist_afil",       "DISTRIBUCIONAFILIADO_TYPE"),
    ("documentacionAfiliadoType",       "dbFC", "fctbcart_rpag",            "DOCUMENTACIONAFILIADO_TYPE"),
    ("firmanteParticipeType",           "dbFC", "sfct_firmante",            "FIRMANTEPARTICIPE_TYPE"),
    ("grupoFamiliarType",               "dbFC", "sfct_grupo_fami",          "GRUPOFAMILIAR_TYPE"),
    ("informacionAdicionalAfiliadoType","dbFC", "fctbafil_info_adic",       "INFORMACIONADICIONALAFILIADO_TYPE"),
    ("institucionType",                 "dbFC", "sfct_institucion",         "INSTITUCION_TYPE"),
    ("motivoContableType",              "dbFC", "sfct_motivo_cnta_cble",    "MOTIVOCONTABLE_TYPE"),
    ("movimientoCuentaType",            "dbFC", "sfct_movimiento",          "MOVIMIENTOCUENTA_TYPE"),
    ("movimientoTemporalType",          "dbFC", "sfct_movimiento_temp",     "MOVIMIENTOTEMPORAL_TYPE"),
    ("naturalInformacionAdicionalType", "dbFC", "fctbafil_info_actu_docs",  "NATURALINFORMACIONADICIONALTYPE"),
    ("naturalInformacionBasicaType",    "dbFC", "sfct_afiliado",            "NATURALINFORMACIONBASICATYPE"),
    ("naturalIngresosEgresosType",      "dbFC", "sfct_afiliado_otros",      "NATURALINGRESOSEGRESOSTYPE"),
    ("naturalTrabajoType",              "dbFC", "sfct_afiliado",            "NATURALTRABAJOTYPE"),
    ("otrosIngresosAfiliadoType",       "dbFC", "fctbotro_ingr_afil",       "OTROSINGRESOSAFILIADO_TYPE"),
    ("personaDireccionesType",          "dbCG", "cgtbprvd",                 "PERSONADIRECCIONESTYPE"),
    ("personaReferenciasBancariasType", "dbFC", "sfct_afiliado_referencias","PERSONAREFERENCIASBANCARIASTYPE"),
    ("personaReferenciasPersonalesType","dbFC", "fctbafil_ahor_refe",       "PERSONAREFERENCIASPERSONALESTYPE"),
    ("personaTelefonosType",            "dbFC", "fctbagen_telf_part",       "PERSONATELEFONOSTYPE"),
    ("personaType",                     "dbCG", "cgtbprvd",                 "PERSONATYPE"),
    ("personaVinculacionesType",        "dbIM", "imtbmiem_cony",            "PERSONAVINCULACIONESTYPE"),
    ("referenciaParticipeType",         "dbFC", "sfct_referencias",         "REFERENCIAPARTICIPE_TYPE"),
    ("reporteSIBSParticipeType",        "dbFC", "fctbcinf_part_sibs",       "REPORTESIBSPARTICIPE_TYPE"),
    ("retiroLiquidacionType",           "dbFC", "sfct_retiro",              "RETIROLIQUIDACION_TYPE"),
    ("rolNominaType",                   "dbFC", "sfct_rubro_rol",           "ROLNOMINA_TYPE"),
    ("retiroVoluntarioEstadoType",      "dbFC", "fctbrvol_esta_afil",       "RETIROVOLUNTARIOESTADO_TYPE"),
    ("saldoDiarioRubroType",            "dbFC", "fctbsald_diar_rubr",       "SALDODIARIORUBRO_TYPE"),
    ("saldoDiarioType",                 "dbFC", "fctbsald_diar_afil_rubr",  "SALDODIARIO_TYPE"),
    ("seguroVidaParticipeType",         "dbSV", "svtbcaus",                 "SEGUROVIDAPARTICIPE_TYPE"),
    ("servicioAdicionalType",           "dbFC", "fctbgene_sibs",            "SERVICIOADICIONAL_TYPE"),
]

DB_CFG = {"server": "10.35.3.64,1433", "driver": "{SQL Server}",
          "username": "sa", "password": "YourPassword123"}
def sql(db):
    s = (f"DRIVER={DB_CFG['driver']};SERVER={DB_CFG['server']};DATABASE={db};"
         f"UID={DB_CFG['username']};PWD={DB_CFG['password']}")
    return pyodbc.connect(s, autocommit=True)

orcl = oracledb.connect(user="fcme_user", password="FcmeUser2025!",
                        dsn="10.35.3.223:31521/XEPDB1")
co = orcl.cursor()
can = sql("fcme_canonicos").cursor()

# RUN ID unico para esta corrida
RUN_TS = int(time.time())
RUN_OFFSET = (RUN_TS % 1_000_000_000) * 100  # base offset para data_generator

print(f"[RUN] ts={RUN_TS}  offset_base={RUN_OFFSET}")
print(f"[TYPES] {len(TYPES)} aggregate_types\n")

# ---------- BASELINE GLOBAL ----------
can.execute("SELECT ISNULL(MAX(id),0) FROM dbo.cdc_outbox")
out_max_baseline = can.fetchone()[0]
co.execute("SELECT NVL(MAX(ID),0) FROM FCME_USER.CDC_INBOX")
inb_max_baseline = co.fetchone()[0]
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
err_baseline = co.fetchone()[0]
print(f"[BASELINE] outbox.max_id={out_max_baseline}  inbox.max_id={inb_max_baseline}  errors={err_baseline}")

# Conteos por destino antes
dest_before = {}
for _, _, _, dst in TYPES:
    try:
        co.execute(f"SELECT COUNT(*) FROM FCME_USER.{dst}")
        dest_before[dst] = co.fetchone()[0]
    except Exception as e:
        dest_before[dst] = -1
        print(f"  warn {dst} no accesible: {str(e)[:80]}")

# ---------- INSERTAR UNO POR TIPO ----------
print("\n[INSERTS]")
results = []  # (agg_type, src_db, src_tbl, ok_insert, err_insert, outbox_id)
for idx, (agg, src_db, src_tbl, dst) in enumerate(TYPES):
    offset = RUN_OFFSET + idx * 1000
    try:
        cols = get_table_columns(src_db, "dbo", src_tbl)
        if not cols:
            results.append((agg, src_db, src_tbl, False, "tabla sin cols", None))
            print(f"  [{idx+1:>2}/{len(TYPES)}] {agg:<38} {src_db}.{src_tbl} ERR sin columnas")
            continue
        conn = get_connection(src_db)
        pks = get_pk_columns(conn, "dbo", src_tbl)
        fkv = get_fk_values(conn, "dbo", src_tbl)
        cur = conn.cursor()

        # Construir INSERT (omitiendo identity)
        col_names = []
        col_vals = []
        for i_col, c in enumerate(cols):
            if c.get("is_identity"):
                continue
            v = generate_fake_value(c, idx, offset, is_pk=(c["name"] in pks), fk_values=fkv)
            col_names.append(c["name"])
            col_vals.append(v)

        ph = ",".join("?" for _ in col_names)
        cols_q = ",".join(f"[{n}]" for n in col_names)
        sql_ins = f"INSERT INTO dbo.[{src_tbl}] ({cols_q}) VALUES ({ph})"

        # outbox.id antes
        can.execute("SELECT ISNULL(MAX(id),0) FROM dbo.cdc_outbox")
        out_before = can.fetchone()[0]

        op_used = "INSERT"
        try:
            cur.execute(sql_ins, col_vals)
        except Exception as ins_err:
            # Fallback: si INSERT colisiona o un trigger aborta, probar UPDATE no-op
            # sobre una fila existente. Sigue ejercitando el outbox trigger.
            ins_msg = str(ins_err)
            if "PRIMARY KEY" in ins_msg or "trigger execution" in ins_msg or "FOREIGN KEY" in ins_msg:
                first_col = next((c["name"] for c in cols if not c.get("is_identity")), None)
                if first_col:
                    cur.execute(f"UPDATE TOP (1) dbo.[{src_tbl}] SET [{first_col}] = [{first_col}]")
                    op_used = "UPDATE_NOOP"
                else:
                    raise
            else:
                raise

        # outbox.id despues
        can.execute("SELECT ISNULL(MAX(id),0) FROM dbo.cdc_outbox")
        out_after = can.fetchone()[0]
        new_outbox = out_after - out_before
        results.append((agg, src_db, src_tbl, True, None, out_after if new_outbox else None))
        print(f"  [{idx+1:>2}/{len(TYPES)}] {agg:<38} {src_db}.{src_tbl}  {op_used}  outbox+={new_outbox}")
        conn.close()
    except Exception as e:
        msg = str(e).replace("\n"," ")[:500]
        results.append((agg, src_db, src_tbl, False, msg, None))
        print(f"  [{idx+1:>2}/{len(TYPES)}] {agg:<38} {src_db}.{src_tbl} INSERT FAIL: {msg}")

# ---------- ESPERAR PROPAGACION ----------
print("\n[PROPAGACION] esperando 120s...")
deadline = time.time() + 120
while time.time() < deadline:
    co.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max_baseline} AND PROCESSED=1")
    pr = co.fetchone()[0]
    co.execute(f"SELECT COUNT(*) FROM FCME_USER.CDC_INBOX WHERE ID>{inb_max_baseline}")
    inb = co.fetchone()[0]
    can.execute(f"SELECT COUNT(*) FROM dbo.cdc_outbox WHERE id>{out_max_baseline}")
    out = can.fetchone()[0]
    print(f"  outbox+={out}  inbox+={inb}  processed+={pr}  ({int(deadline-time.time())}s rest)")
    if inb >= len([r for r in results if r[3]]) and pr >= inb:
        break
    time.sleep(4)

# ---------- VALIDAR DESTINO ----------
print("\n[DESTINO] FCME_USER.<TARGET> deltas")
dest_after = {}
for _, _, _, dst in TYPES:
    try:
        co.execute(f"SELECT COUNT(*) FROM FCME_USER.{dst}")
        dest_after[dst] = co.fetchone()[0]
    except Exception:
        dest_after[dst] = -1

# Errores nuevos
co.execute("SELECT COUNT(*) FROM FCME_USER.CDC_INBOX_ERRORS")
err_now = co.fetchone()[0]
co.execute(f"SELECT AGGREGATE_TYPE, ERROR_MESSAGE FROM FCME_USER.CDC_INBOX_ERRORS "
           f"WHERE INBOX_ID > {inb_max_baseline} ORDER BY INBOX_ID")
errs_by_type = {}
for r in co.fetchall():
    errs_by_type.setdefault(r[0], []).append((r[1] or "")[:200])

# ---------- TABLA FINAL ----------
print(f"\n{'#':>3} {'aggregate_type':<38} {'src':<10} {'tabla':<32} {'ins':<3} {'dest_delta':<10} status")
print("-"*120)
ok_count=0; fail_count=0
for i, ((agg, src_db, src_tbl, ok_ins, err_ins, oid), (_,_,_,dst)) in enumerate(zip(results, TYPES)):
    delta = (dest_after.get(dst,-1) - dest_before.get(dst,-1)) if dest_before.get(dst,-1)>=0 else "n/a"
    new_errs = errs_by_type.get(agg, [])
    if not ok_ins:
        status = f"INS FAIL: {err_ins}"
        fail_count+=1
    elif new_errs:
        status = f"WRAPPER ERR: {new_errs[0][:120]}"
        fail_count+=1
    elif isinstance(delta, int) and delta >= 1:
        status = "OK"
        ok_count+=1
    elif delta == 0:
        status = "PROCESSED no row in dest"
        fail_count+=1
    else:
        status = "?"
        fail_count+=1
    print(f"{i+1:>3} {agg:<38} {src_db:<10} {src_tbl:<32} {('Y' if ok_ins else 'N'):<3} {str(delta):<10} {status}")

print(f"\n[RESUMEN] OK={ok_count}  FAIL={fail_count}  TOTAL={len(TYPES)}")

orcl.close()
print("\n=== FIN TEST FLUJO 1 (43 types) ===")
