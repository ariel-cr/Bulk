"""Diagnostico de los 14 types silenciados + arregla 3 mapeos rotos.

Estrategia:
A) Buscar las 3 tablas faltantes (fctbaudi_actu_afil, sfct_institucion, sfct_conyuge) en otras BDs
B) Crear sp_log_crud_error en canonicos para que CRUDs registren errores cross-DB
C) Modificar los 30 CRUDs para que en CATCH llamen al log
D) Re-test con PKs frescos y ver errores reales

NO toca los 10 types que funcionan ahora (su CRUD/wrapper siguen igual,
solo agregamos logging adicional al CATCH).
"""
import pyodbc, oracledb, re

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

LEG_DBS=['dbIM','dbFC','dbCR','dbCG','dbCT','dbNO','dbSV']

# ===== A) Buscar tablas faltantes =====
print("="*70)
print("[A] Buscar las 3 tablas faltantes en TODAS las BDs legacy")
print("="*70)
MISSING=['fctbaudi_actu_afil','sfct_institucion','sfct_conyuge']
found={}
for tbl in MISSING:
    print(f"\n  Buscando '{tbl}':")
    for db in LEG_DBS:
        try:
            c=sql(db).cursor()
            c.execute("SELECT COUNT(*) FROM sys.tables WHERE name=?", tbl)
            if c.fetchone()[0]>0:
                print(f"    -> existe en {db}")
                found[tbl]=db
        except: pass
    if tbl not in found:
        print(f"    -> NO encontrada en ninguna BD legacy")

# Actualizar cdc_table_to_types para los que se encontraron
print("\n  Actualizando target_db en cdc_table_to_types (donde aplique)...")
c=sql('fcme_canonicos').cursor()
mapping_fix = {
    'fctbaudi_actu_afil': 'auditoriaAfiliadoType',
    'sfct_institucion': 'naturalInformacionAdicionalType',
    'sfct_conyuge': 'personaVinculacionesType'
}
for tbl, at in mapping_fix.items():
    correct_db = found.get(tbl)
    if correct_db:
        c.execute("UPDATE dbo.cdc_inbox_module_config SET target_db=? WHERE aggregate_type=?", correct_db, at)
        print(f"    {at}: target_db={correct_db}")
    else:
        # Desactivar el type para que no acumule errores
        c.execute("UPDATE dbo.cdc_inbox_module_config SET active=0 WHERE aggregate_type=?", at)
        print(f"    {at}: tabla {tbl} no existe -> active=0")

# ===== B) Crear sp_log_crud_error en canonicos =====
print("\n" + "="*70)
print("[B] Crear dbo.usp_log_crud_error en canonicos (cross-DB target)")
print("="*70)
c.execute("""
CREATE OR ALTER PROCEDURE dbo.usp_log_crud_error
    @inbox_id BIGINT = NULL,
    @aggregate_type NVARCHAR(200) = NULL,
    @event_type NVARCHAR(50) = NULL,
    @error_message NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO dbo.cdc_inbox_errors (inbox_id, aggregate_type, event_type, error_message)
        VALUES (ISNULL(@inbox_id, 0), @aggregate_type, @event_type, @error_message);
    END TRY
    BEGIN CATCH
        -- nunca propaga
    END CATCH
END
""")
print("  usp_log_crud_error creado")

# ===== C) Modificar los 30 CRUDs para que en CATCH llamen al log =====
print("\n" + "="*70)
print("[C] Agregar log al CATCH de cada CRUD")
print("="*70)
total=0
for db in LEG_DBS:
    cdb=sql(db).cursor()
    cdb.execute("""SELECT o.name, m.definition FROM sys.objects o
                   JOIN sys.sql_modules m ON o.object_id=m.object_id
                   WHERE o.type='P' AND o.name LIKE 'sp_%_TYPE_CRUD'""")
    for r in cdb.fetchall():
        defn = r.definition
        # Verificar si ya tiene log
        if 'usp_log_crud_error' in defn:
            continue
        # Agregar al CATCH: EXEC fcme_canonicos.dbo.usp_log_crud_error
        # Reemplazar el CATCH simple para que registre antes
        new = defn.replace(
            "BEGIN CATCH\n        EXEC sp_set_session_context N'is_replicating', 0;\n        -- THROW; (silenced)\n    END CATCH",
            """BEGIN CATCH
        DECLARE @err NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC sp_set_session_context N'is_replicating', 0;
        BEGIN TRY
            EXEC fcme_canonicos.dbo.usp_log_crud_error
                @aggregate_type = N'""" + r.name.replace('sp_','').replace('_CRUD','') + """',
                @event_type = @Accion,
                @error_message = @err;
        END TRY BEGIN CATCH END CATCH
    END CATCH"""
        )
        if new == defn:
            # Patron alternativo
            new = defn.replace(
                "-- THROW; (silenced)",
                """DECLARE @err NVARCHAR(MAX) = ERROR_MESSAGE();
        BEGIN TRY
            EXEC fcme_canonicos.dbo.usp_log_crud_error
                @aggregate_type = N'""" + r.name.replace('sp_','').replace('_CRUD','') + """',
                @event_type = @Accion,
                @error_message = @err;
        END TRY BEGIN CATCH END CATCH"""
            )
        new = new.replace('CREATE   PROCEDURE','CREATE OR ALTER PROCEDURE').replace('CREATE PROCEDURE','CREATE OR ALTER PROCEDURE')
        try:
            cdb.execute(new)
            total+=1
        except Exception as e:
            print(f"  fail {db}.{r.name}: {str(e)[:120]}")
print(f"  CRUDs actualizados con log: {total}")

print("\n=== Ahora hacer re-test masivo para ver errores reales ===")
