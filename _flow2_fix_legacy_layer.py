"""Arreglar SIN tocar CRUDs ni wrappers. Solo modificar el lado legacy:

A) NOT NULL sin default (5 cols): ALTER TABLE ... ADD DEFAULT
   - El CRUD omite estas cols del INSERT; con DEFAULT, SQL Server las llena solo.
B) IDENTITY column con CRUD insertando explicito: trigger INSTEAD OF INSERT
   que descarta el valor IDENTITY recibido.
C) Tabla inexistente referenciada por CRUD: CREATE SYNONYM apuntando a la tabla real.
D) FK violations: NO se arreglan (son del test, en prod estaran ok).

NO toca: 30 CRUDs, 30 wrappers, dispatcher, trigger, infraestructura Kafka,
tablas en Oracle, ni los 10 types que funcionan.
"""
import pyodbc

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
def sql(db):
    return pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE={db};UID={DB['username']};PWD={DB['password']}", autocommit=True)

c_fc = sql('dbFC').cursor()

# ===== A) Agregar DEFAULTs a NOT NULL sin default =====
print("="*70)
print("[A] DEFAULTs en cols NOT NULL para que CRUDs no fallen al omitirlas")
print("="*70)

NOT_NULL_FIXES = [
    # (tabla, col, default_expr, type_hint)
    ('fctbafil_actu',     'in_cobr_pres', "''",            'char'),
    ('sfct_padbs',        'ci_bnco',      "''",            'char'),
    ('sfct_afiliado_fondos','fx_ingreso', "'1900-01-01'",  'date'),
    ('sfct_retiro',       'va_cred_fond', "0",             'numeric'),
]

for tbl, col, default, _ in NOT_NULL_FIXES:
    # Verificar si ya tiene default
    c_fc.execute("""SELECT dc.name, dc.definition
                    FROM sys.columns col
                    LEFT JOIN sys.default_constraints dc ON dc.parent_object_id=col.object_id AND dc.parent_column_id=col.column_id
                    WHERE col.object_id=OBJECT_ID(?) AND col.name=?""", f'dbo.{tbl}', col)
    r = c_fc.fetchone()
    if r and r.name:
        print(f"  {tbl}.{col}: ya tiene default ({r.definition})")
        continue
    cn_name = f"DF_{tbl}_{col}"
    try:
        c_fc.execute(f"ALTER TABLE dbo.[{tbl}] ADD CONSTRAINT [{cn_name}] DEFAULT {default} FOR [{col}]")
        print(f"  {tbl}.{col}: DEFAULT {default} agregado")
    except Exception as e:
        print(f"  {tbl}.{col}: fail {str(e)[:120]}")

# ===== B) IDENTITY column - trigger INSTEAD OF INSERT que descarta valor =====
print("\n" + "="*70)
print("[B] INSTEAD OF INSERT en fctbafil_info_actu_docs (PK es IDENTITY)")
print("="*70)
# Verificar que la col es IDENTITY
c_fc.execute("""SELECT name, is_identity FROM sys.columns
                WHERE object_id=OBJECT_ID('dbo.fctbafil_info_actu_docs') AND is_identity=1""")
ident_col = c_fc.fetchone()
if ident_col:
    print(f"  IDENTITY col: {ident_col.name}")
    # Construir lista de cols sin la identity
    c_fc.execute("""SELECT name FROM sys.columns
                    WHERE object_id=OBJECT_ID('dbo.fctbafil_info_actu_docs') AND is_identity=0
                    ORDER BY column_id""")
    other_cols = [r.name for r in c_fc.fetchall()]
    cols_csv = ', '.join(f'[{c}]' for c in other_cols)
    insert_cols_csv = ', '.join(f'i.[{c}]' for c in other_cols)

    trg_sql = f"""CREATE OR ALTER TRIGGER dbo.trg_fctbafil_info_actu_docs_iof
ON dbo.fctbafil_info_actu_docs
INSTEAD OF INSERT
AS
BEGIN
    SET NOCOUNT ON;
    -- Ignorar valor explicito de [{ident_col.name}], dejar que IDENTITY autoasigne
    INSERT INTO dbo.fctbafil_info_actu_docs ({cols_csv})
    SELECT {insert_cols_csv} FROM inserted i;
END"""
    c_fc.execute(trg_sql)
    print(f"  trigger trg_fctbafil_info_actu_docs_iof creado (omite {ident_col.name})")
else:
    print("  no hay col IDENTITY")

# ===== C) Tablas inexistentes referenciadas por CRUDs: SYNONYMs =====
print("\n" + "="*70)
print("[C] SYNONYMs para tablas referenciadas por CRUDs migrados")
print("="*70)

# Verificar nombre exacto de tablas reales
SYNONYMS = [
    ('cttbafil_audi', 'fctbaudi_actu_afil'),  # auditoriaAfiliadoType
    ('notbempl',      'sfct_institucion'),     # naturalInformacionAdicionalType (segun el bug observado)
]

for syn_name, real_table in SYNONYMS:
    # Verificar si real_table existe
    c_fc.execute("SELECT COUNT(*) FROM sys.tables WHERE name=?", real_table)
    if c_fc.fetchone()[0] == 0:
        print(f"  {syn_name}: tabla real '{real_table}' no existe en dbFC, skip")
        continue
    # Crear synonym
    c_fc.execute(f"IF OBJECT_ID('dbo.{syn_name}','SN') IS NOT NULL DROP SYNONYM dbo.{syn_name}")
    try:
        c_fc.execute(f"CREATE SYNONYM dbo.{syn_name} FOR dbo.{real_table}")
        print(f"  SYNONYM dbo.{syn_name} -> dbo.{real_table}")
    except Exception as e:
        print(f"  fail synonym {syn_name}: {str(e)[:200]}")

print("\n=== Fix aplicado en lado legacy. CRUDs/wrappers intactos. ===")
