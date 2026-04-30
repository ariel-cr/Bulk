"""Paso 2 (Flujo 2): tabla cdc_inbox_module_config en fcme_canonicos.

Mapea aggregate_type (Type canonico viniendo desde Newcore) -> wrapper SP en canonicos.
El dispatcher generico usp_process_cdc_inbox lee esta tabla para saber
a que SP llamar segun el aggregate_type del evento entrante.
"""
import pyodbc

DB = {"server":"10.35.3.64,1433","driver":"{SQL Server}",
      "username":"sa","password":"YourPassword123"}
def sql(db):
    s=(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}")
    return pyodbc.connect(s, autocommit=True)

c = sql("fcme_canonicos").cursor()

print("="*70)
print("[2.1] Verificar si la tabla ya existe")
print("="*70)
c.execute("""SELECT s.name sch, t.name tbl FROM sys.tables t
             JOIN sys.schemas s ON t.schema_id=s.schema_id
             WHERE t.name='cdc_inbox_module_config'""")
rows = c.fetchall()
if rows:
    print(f"  ya existe: {rows[0].sch}.{rows[0].tbl}")
else:
    print("  no existe — creando")

print("\n" + "="*70)
print("[2.2] Crear tabla cdc_inbox_module_config")
print("="*70)
c.execute("""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='cdc_inbox_module_config')
BEGIN
    CREATE TABLE dbo.cdc_inbox_module_config (
        aggregate_type NVARCHAR(200) NOT NULL,
        sp_name        NVARCHAR(300) NOT NULL,
        target_db      NVARCHAR(50)  NULL,
        module_name    NVARCHAR(50)  NULL,
        active         BIT           NOT NULL DEFAULT 1,
        created_at     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at     DATETIME2(3)  NULL,
        CONSTRAINT PK_cdc_inbox_module_config PRIMARY KEY (aggregate_type)
    );
    PRINT 'tabla creada';
END
ELSE
    PRINT 'tabla ya existia';
""")

print("\n" + "="*70)
print("[2.3] Estructura final")
print("="*70)
c.execute("""SELECT c.name, t.name tp, c.max_length, c.is_nullable, dc.definition AS df
             FROM sys.columns c
             JOIN sys.types t ON c.user_type_id=t.user_type_id
             LEFT JOIN sys.default_constraints dc ON dc.parent_object_id=c.object_id AND dc.parent_column_id=c.column_id
             WHERE c.object_id=OBJECT_ID('dbo.cdc_inbox_module_config')
             ORDER BY c.column_id""")
for r in c.fetchall():
    df = r.df or ''
    print(f"  {r.name:<18} {r.tp:<14} max={r.max_length:<5} null={r.is_nullable}  default={df}")

# PK
c.execute("""SELECT i.name FROM sys.indexes i
             WHERE i.object_id=OBJECT_ID('dbo.cdc_inbox_module_config') AND i.is_primary_key=1""")
pk = c.fetchone()
print(f"\n  PK: {pk.name if pk else 'NINGUNA'}")

# Indice por active (queries comunes)
c.execute("""IF NOT EXISTS (SELECT 1 FROM sys.indexes
                            WHERE object_id=OBJECT_ID('dbo.cdc_inbox_module_config')
                              AND name='IX_module_config_active')
BEGIN
    CREATE INDEX IX_module_config_active ON dbo.cdc_inbox_module_config(active, aggregate_type)
        INCLUDE (sp_name);
    PRINT 'indice IX_module_config_active creado';
END""")

print("\n" + "="*70)
print("[2.4] Test INSERT/UPSERT")
print("="*70)
c.execute("""INSERT INTO dbo.cdc_inbox_module_config (aggregate_type, sp_name, target_db, module_name, active)
             VALUES ('_test', 'dbo.usp_inbox_test', 'dbFC', 'PARTICIPE', 1)""")
c.execute("SELECT * FROM dbo.cdc_inbox_module_config WHERE aggregate_type='_test'")
cols = [d[0] for d in c.description]
for r in c.fetchall():
    print("  " + " | ".join(f"{cols[i]}={v}" for i,v in enumerate(r)))

c.execute("DELETE FROM dbo.cdc_inbox_module_config WHERE aggregate_type='_test'")
print("\n  test row borrada")

c.execute("SELECT COUNT(*) FROM dbo.cdc_inbox_module_config")
print(f"  filas finales: {c.fetchone()[0]}")

print("\n=== PASO 2 OK ===")
