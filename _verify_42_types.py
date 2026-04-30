"""Verifica los 42 aggregate_types del usuario contra las BDs originales:
  - hay trigger legacy que emita el aggregate_type? (Flujo 1 publicacion)
  - existe en FCME_USER.CDC_INBOX_MODULE_CONFIG y SP_NAME es valido?
  - existe la tabla destino FCME_USER.<TYPE>?
"""
import pyodbc, oracledb, re

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

USER_LIST = """actualizacionAfiliado_type
actualizacionDocumentos_type
agendaMailAfiliado_type
areaLaboralParticipe_type
auditoriaAfiliado_type
beneficiarioParticipe_type
comisionParticipe_type
cuentaBancariaAfiliado_type
distribucionAfiliado_type
documentacionAfiliado_type
firmanteParticipe_type
grupoFamiliar_type
imagenesType
informacionAdicionalAfiliado_type
institucion_type
juridicoInformacionBasicaType
motivoContable_type
movimientoCuenta_type
movimientoTemporal_type
naturalInformacionAdicionalType
naturalInformacionBasicaType
naturalIngresosEgresosType
naturalReferenciasComercialesType
naturalTrabajoType
otrosIngresosAfiliado_type
personaDireccionesType
personaFirmasType
personaReferenciasBancariasType
personaReferenciasPersonalesType
personaTelefonosType
personaType
personaVinculacionesType
prueba
referenciaParticipe_type
reporteSIBSParticipe_type
retiroLiquidacion_type
retiroVoluntarioEstado_type
rolNomina_type
saldoDiarioRubro_type
saldoDiario_type
seguroVidaParticipe_type
servicioAdicional_type""".strip().splitlines()

# 1) Cosechar triggers legacy reales y los aggregate_types que emiten
LEG_DBS=['dbIM','dbFC','dbCR','dbCG','dbCT','dbNO','dbSV']
trig_emits={}  # agg_type (lower) -> set((db,table))
for db in LEG_DBS:
    c=sql(db).cursor()
    c.execute("""SELECT t.name AS trg, OBJECT_NAME(t.parent_id) AS parent, OBJECT_DEFINITION(t.object_id) AS body
                 FROM sys.triggers t WHERE t.name LIKE 'trg_outbox_%'""")
    for r in c.fetchall():
        body=r.body or ''
        types=set(re.findall(r"N'([A-Za-z][A-Za-z0-9_]*[Tt]ype)'", body))
        types|=set(re.findall(r"'([A-Za-z][A-Za-z0-9]+[Tt]ype)'", body))
        for t in types:
            trig_emits.setdefault(t.lower(), set()).add((db, r.parent))

# 2) Oracle: module_config + tablas TYPE
o=oracledb.connect(user='fcme_user',password='FcmeUser2025!',dsn='10.35.3.223:31521/XEPDB1').cursor()
o.execute("SELECT AGGREGATE_TYPE, SP_NAME, ACTIVE FROM FCME_USER.CDC_INBOX_MODULE_CONFIG")
mc={r[0].lower(): (r[0], r[1], r[2]) for r in o.fetchall()}
o.execute("SELECT table_name FROM all_tables WHERE owner='FCME_USER'")
tablas={r[0]: r[0] for r in o.fetchall()}

def normalize(t):
    """actualizacionAfiliado_type -> actualizacionAfiliadoType"""
    if t.endswith('_type'):
        return t[:-5] + 'Type'
    return t

def find_dest(t):
    """Heuristica: convertir agg_type a candidato de tabla FCME_USER"""
    n=normalize(t)
    upper=n.upper()
    cands=[upper.replace('TYPE','_TYPE'), upper]
    for c in cands:
        if c in tablas: return c
    base=re.sub(r'_?TYPE$','',upper)
    for k in tablas:
        if k.startswith(base) and k.endswith('TYPE'): return k
    return None

# 3) Reportar
print(f"{'#':>2} {'aggregate_type':<40} {'trigger':<8} {'mod_cfg':<8} {'dest':<35} {'src_legacy'}")
print("-"*140)
ok=0; missing=0
result=[]
for i, agg in enumerate(USER_LIST, 1):
    key=normalize(agg).lower()
    has_trig=key in trig_emits
    has_mc=key in mc
    dest=find_dest(agg)
    src=sorted(trig_emits.get(key, set()))[:1]
    src_str=f"{src[0][0]}.{src[0][1]}" if src else "—"
    flag_t='Y' if has_trig else 'N'
    flag_m='Y' if has_mc else 'N'
    flag_d=dest if dest else 'NO'
    if has_trig and has_mc and dest:
        ok+=1
    else:
        missing+=1
    print(f"{i:>2} {agg:<40} {flag_t:<8} {flag_m:<8} {(dest or 'MISSING'):<35} {src_str}")
    result.append((agg, has_trig, has_mc, dest, src[0] if src else None))

print(f"\n[RESUMEN] cableados completos: {ok}/{len(USER_LIST)}   incompletos: {missing}")

# Detallar incompletos
inc=[r for r in result if not (r[1] and r[2] and r[3])]
if inc:
    print("\n[FALTANTES]:")
    for r in inc:
        det=[]
        if not r[1]: det.append("sin trigger legacy")
        if not r[2]: det.append("sin module_config")
        if not r[3]: det.append("sin tabla destino")
        print(f"  {r[0]:<40} -> {', '.join(det)}")

o.connection.close()
