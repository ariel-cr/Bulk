"""Reescribe los 30 wrappers para llamar a los SPs ORIGINALES de participes.*
en lugar de los CRUDs auto-generados (que tenian bugs).

Para cada type:
1) Inspeccionar firma del SP original
2) Generar wrapper que extrae cada @param del payload JSON
3) Pasar @accion = INSERT/UPDATE/DELETE segun event_type

NO toca: los SPs originales (participes.*), los CRUDs que yo generé (siguen pero no se usan),
triggers, dispatcher, infra Kafka.
"""
import pyodbc

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)
c=sql('fcme_canonicos').cursor()

# Mapping aggregate_type -> nombre SP original (en participes.*)
TYPE_TO_SP = {
    'actualizacionAfiliadoType':'sp_actualizacionAfiliado_type_crud',
    'actualizacionDocumentosType':'sp_actualizacionDocumentos_type_crud',
    'agendaMailAfiliadoType':'sp_agendaMailAfiliado_type_crud',
    'auditoriaAfiliadoType':'sp_auditoriaAfiliado_type_crud',
    'beneficiarioParticipeType':'sp_beneficiarioParticipe_type_crud',
    'cuentaBancariaAfiliadoType':'sp_cuentaBancariaAfiliado_type_crud',
    'distribucionAfiliadoType':'sp_distribucionAfiliado_type_crud',
    'documentacionAfiliadoType':'sp_documentacionAfiliado_type_crud',
    'firmanteParticipeType':'sp_firmanteParticipe_type_crud',
    'grupoFamiliarType':'sp_grupoFamiliar_type_crud',
    'informacionAdicionalAfiliadoType':'sp_informacionAdicionalAfiliado_type_crud',
    'institucionType':'sp_institucion_type_crud',
    'motivoContableType':'sp_motivoContable_type_crud',
    'movimientoCuentaType':'sp_movimientoCuenta_type_crud',
    'movimientoTemporalType':'sp_movimientoTemporal_type_crud',
    'naturalInformacionAdicionalType':'sp_naturalInformacionAdicionalType_crud',
    'naturalIngresosEgresosType':'sp_naturalIngresosEgresosType_crud',
    'naturalTrabajoType':'sp_naturalTrabajoType_crud',
    'personaReferenciasBancariasType':'sp_personaReferenciasBancariasType_crud',
    'personaReferenciasPersonalesType':'sp_personaReferenciasPersonalesType_crud',
    'personaTelefonosType':'sp_personaTelefonosType_crud',
    'personaVinculacionesType':'sp_personaVinculacionesType_crud',
    'referenciaParticipeType':'sp_referenciaParticipe_type_crud',
    'reporteSIBSParticipeType':'sp_reporteSIBSParticipe_type_crud',
    'retiroLiquidacionType':'sp_retiroLiquidacion_type_crud',
    'retiroVoluntarioEstadoType':'sp_retiroVoluntarioEstado_type_crud',
    'saldoDiarioRubroType':'sp_saldoDiarioRubro_type_crud',
    'saldoDiarioType':'sp_saldoDiario_type_crud',
    'seguroVidaParticipeType':'sp_seguroVidaParticipe_type_crud',
    'servicioAdicionalType':'sp_servicioAdicional_type_crud',
}

# Cols paginacion que NO van en INSERT/UPDATE/DELETE
PAGINATION_PARAMS = {'numeroPagina','cantidadPorPagina','numeroPaginas'}

def get_sp_params(sch, sp):
    c.execute("""SELECT p.name, t.name AS tp, p.max_length, p.has_default_value
                 FROM sys.parameters p
                 JOIN sys.types t ON p.user_type_id=t.user_type_id
                 WHERE p.object_id=OBJECT_ID(?)
                 ORDER BY p.parameter_id""", f'{sch}.{sp}')
    return [(r.name.lstrip('@'), r.tp, r.max_length) for r in c.fetchall()]

print("="*70)
print(f"Regenerando {len(TYPE_TO_SP)} wrappers para apuntar a participes.*")
print("="*70)

deployed = []
skipped = []
for at, sp_name in TYPE_TO_SP.items():
    params = get_sp_params('participes', sp_name)
    if not params:
        skipped.append((at, sp_name, 'no params'))
        continue

    # Filtrar parametros: omitir paginacion, accion (la pasamos calculada)
    work_params = [(n, t, l) for n, t, l in params if n.lower() not in {'accion','numeropagina','cantidadporpagina'}]

    # Construir DECLAREs y EXEC params
    def camel_to_snake_upper(name):
        # codigoCedu -> CODIGO_CEDU; descripcionTipoReferencia -> DESCRIPCION_TIPO_REFERENCIA
        result = []
        for i, ch in enumerate(name):
            if i > 0 and ch.isupper() and not name[i-1].isupper():
                result.append('_')
            result.append(ch.upper())
        return ''.join(result)

    decls = []
    exec_args = ['        @accion = @accion']
    for pname, ptp, plen in work_params:
        # Oracle TYPE puede tener cols SNAKE_CASE (CODIGO_CEDU) o CAMEL (CODIGOTIPOREFERENCIA).
        # Probar ambos formatos: si el SP original usa camelCase param y Oracle usa snake,
        # convertir camel->SNAKE_UPPER. Si Oracle usa concat, usar UPPER directo.
        # Estrategia: COALESCE(JSON_VALUE snake, JSON_VALUE concat)
        snake_field = camel_to_snake_upper(pname)
        concat_field = pname.upper()
        # Construir el tipo SQL
        tp_low = ptp.lower()
        # Buscar valor en payload con ambos formatos (SNAKE y CONCAT)
        # COALESCE: usa el que no sea NULL
        snake_jv = f"JSON_VALUE(@payload,'$.{snake_field}')"
        concat_jv = f"JSON_VALUE(@payload,'$.{concat_field}')"
        raw_expr = f"COALESCE({snake_jv}, {concat_jv})" if snake_field != concat_field else snake_jv

        if tp_low in ('varchar','nvarchar','char','nchar'):
            actual = plen if plen >= 0 else 4000
            if 'n' in tp_low:
                actual = actual // 2 if plen > 0 else 4000
            tp_clause = f"{ptp.upper()}({actual})" if plen > 0 else f"{ptp.upper()}(MAX)"
            cast_expr = raw_expr
        elif tp_low in ('smallint','int','bigint','tinyint'):
            tp_clause = ptp.upper()
            cast_expr = f"TRY_CAST({raw_expr} AS {tp_clause})"
        elif tp_low in ('bit',):
            tp_clause = 'BIT'
            cast_expr = f"TRY_CAST({raw_expr} AS BIT)"
        elif tp_low in ('decimal','numeric','money','float','real'):
            tp_clause = 'DECIMAL(18,6)'
            cast_expr = f"TRY_CAST({raw_expr} AS DECIMAL(18,6))"
        elif tp_low in ('date','datetime','datetime2','smalldatetime'):
            tp_clause = 'DATETIME2'
            cast_expr = f"TRY_CAST({raw_expr} AS DATETIME2)"
        else:
            tp_clause = 'NVARCHAR(MAX)'
            cast_expr = raw_expr

        decls.append(f"        DECLARE @v_{pname} {tp_clause} = {cast_expr};")
        exec_args.append(f"        @{pname} = @v_{pname}")

    decls_block = "\n".join(decls)
    args_block = ",\n".join(exec_args)

    wrap_name = f"usp_inbox_{at}"
    body = f"""CREATE OR ALTER PROCEDURE dbo.{wrap_name}
    @inbox_id BIGINT, @aggregate_id NVARCHAR(200), @aggregate_type NVARCHAR(200),
    @source_table NVARCHAR(200), @event_type NVARCHAR(50), @payload NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;
    BEGIN TRY
        -- Mapear event_type Oracle -> @accion del SP original (UPPERCASE)
        DECLARE @accion VARCHAR(10) = CASE
            WHEN @event_type IN ('INSERT','I') THEN 'INSERT'
            WHEN @event_type IN ('UPDATE','U') THEN 'UPDATE'
            WHEN @event_type IN ('DELETE','D','DELETED') THEN 'DELETE'
            ELSE 'INSERT' END;

        -- Audit del wrapper (mantenemos cdc_inbox_parsed)
        INSERT INTO dbo.cdc_inbox_parsed (inbox_id, aggregate_type, aggregate_id, event_type, pk_value, sample_field)
        VALUES (@inbox_id, @aggregate_type, @aggregate_id, @event_type, @aggregate_id, @aggregate_id);

        -- Extraer parametros desde payload JSON
{decls_block}

        -- Llamar SP original del equipo (schema participes.*)
        EXEC participes.{sp_name}
{args_block};
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (@inbox_id, @aggregate_type, @event_type,
                N'wrapper {at}: ' + ERROR_MESSAGE());
    END CATCH
END"""

    try:
        c.execute(body)
        deployed.append((at, sp_name, len(work_params)))
    except Exception as e:
        skipped.append((at, sp_name, str(e)[:200]))

print(f"\n  Wrappers redirigidos a SP originales: {len(deployed)}")
for at, sp, n in deployed[:35]:
    print(f"    usp_inbox_{at} -> participes.{sp} ({n} params)")
if skipped:
    print(f"\n  Skipped: {len(skipped)}")
    for at, sp, why in skipped:
        print(f"    {at}: {why}")
