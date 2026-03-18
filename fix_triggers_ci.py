"""Fix CUENTAS_INDIVIDUALES triggers to match SP expected field names."""
import pyodbc

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=10.35.3.64,1433;DATABASE=fcme_newcore;UID=sa;PWD=YourPassword123')
cur = conn.cursor()

# 1. cuentaBloqueoFondos: rename fields to match SP
print("1. Fixing trg_cuentaBloqueoFondos_outbox...")
cur.execute("""
ALTER TRIGGER [CUENTAS_INDIVIDUALES].[trg_cuentaBloqueoFondos_outbox]
ON [CUENTAS_INDIVIDUALES].[CUENTABLOQUEOFONDOSTYPE]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CAST(SESSION_CONTEXT(N'is_replicating') AS INT), 0) = 1 RETURN;

    IF EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT INTO [fcme_newcore].[dbo].[cdc_outbox]
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CAST(i.bloqueoId AS NVARCHAR(100)),
            'cuentaBloqueoFondos',
            CASE WHEN EXISTS (SELECT 1 FROM deleted) THEN 'UPDATED' ELSE 'CREATED' END,
            (SELECT
                i.cuentaId                 AS cuenta,
                i.secuencialBloqueoFondos  AS secuencialBloqueoFondos,
                i.monto                    AS valorBloqueo,
                i.montoLiberado            AS montoLiberado,
                i.montoPendiente           AS montoPendiente,
                i.fechaBloqueo             AS fechaContable,
                i.usuario                  AS usuario,
                i.oficina                  AS oficina,
                i.estado                   AS estatusBloqueo,
                i.codigoConcepto           AS codigoConcepto,
                i.cuentaOrigen             AS cuentaOrigen,
                i.motivo                   AS referencia,
                i.codigoEmpresa            AS codigoEmpresa
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            'CUENTAS_INDIVIDUALES.CUENTABLOQUEOFONDOSTYPE',
            GETDATE()
        FROM inserted i;
    END

    IF NOT EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO [fcme_newcore].[dbo].[cdc_outbox]
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CAST(d.bloqueoId AS NVARCHAR(100)),
            'cuentaBloqueoFondos',
            'DELETED',
            (SELECT
                d.cuentaId                 AS cuenta,
                d.secuencialBloqueoFondos  AS secuencialBloqueoFondos,
                d.monto                    AS valorBloqueo,
                d.montoLiberado            AS montoLiberado,
                d.montoPendiente           AS montoPendiente,
                d.fechaBloqueo             AS fechaContable,
                d.usuario                  AS usuario,
                d.oficina                  AS oficina,
                d.estado                   AS estatusBloqueo,
                d.codigoConcepto           AS codigoConcepto,
                d.cuentaOrigen             AS cuentaOrigen,
                d.motivo                   AS referencia,
                d.codigoEmpresa            AS codigoEmpresa
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            'CUENTAS_INDIVIDUALES.CUENTABLOQUEOFONDOSTYPE',
            GETDATE()
        FROM deleted d;
    END
END
""")
conn.commit()
print("  OK")

# 2. saldosType: completely different field names
print("2. Fixing trg_saldosType_outbox...")
cur.execute("""
ALTER TRIGGER [CUENTAS_INDIVIDUALES].[trg_saldosType_outbox]
ON [CUENTAS_INDIVIDUALES].[SALDOSTYPE]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CAST(SESSION_CONTEXT(N'is_replicating') AS INT), 0) = 1 RETURN;

    IF EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT INTO [fcme_newcore].[dbo].[cdc_outbox]
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CAST(i.saldoId AS NVARCHAR(100)),
            'saldosType',
            CASE WHEN EXISTS (SELECT 1 FROM deleted) THEN 'UPDATED' ELSE 'CREATED' END,
            (SELECT
                i.cuentaId         AS cuenta,
                i.tipoSaldo        AS categoria,
                i.monto            AS saldoMonedaCuenta,
                i.monto            AS saldoMonedaOficial,
                i.codigoEmpresa    AS codigoEmpresa
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            'CUENTAS_INDIVIDUALES.SALDOSTYPE',
            GETDATE()
        FROM inserted i;
    END

    IF NOT EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO [fcme_newcore].[dbo].[cdc_outbox]
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CAST(d.saldoId AS NVARCHAR(100)),
            'saldosType',
            'DELETED',
            (SELECT
                d.cuentaId         AS cuenta,
                d.tipoSaldo        AS categoria,
                d.monto            AS saldoMonedaCuenta,
                d.monto            AS saldoMonedaOficial,
                d.codigoEmpresa    AS codigoEmpresa
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            'CUENTAS_INDIVIDUALES.SALDOSTYPE',
            GETDATE()
        FROM deleted d;
    END
END
""")
conn.commit()
print("  OK")

# 3. cuentasPorCobrar: rename personaId->cuenta via 'cuenta' column
print("3. Fixing trg_cuentasPorCobrar_outbox...")
cur.execute("""
ALTER TRIGGER [CUENTAS_INDIVIDUALES].[trg_cuentasPorCobrar_outbox]
ON [CUENTAS_INDIVIDUALES].[CUENTASPORCOBRARTYPES]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CAST(SESSION_CONTEXT(N'is_replicating') AS INT), 0) = 1 RETURN;

    IF EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT INTO [fcme_newcore].[dbo].[cdc_outbox]
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CAST(i.cuentaPorCobrarId AS NVARCHAR(100)),
            'cuentasPorCobrar',
            CASE WHEN EXISTS (SELECT 1 FROM deleted) THEN 'UPDATED' ELSE 'CREATED' END,
            (SELECT
                i.cuenta               AS cuenta,
                i.fechaCreacion        AS fechaCreacion,
                i.fechaVencimiento     AS fechaVencimiento,
                i.numeroDocumento      AS numeroDocumento,
                i.codigoConcepto       AS codigoConcepto,
                i.moneda               AS moneda,
                i.montoOriginal        AS montoOriginal,
                i.montoPendiente       AS montoPendiente,
                i.detalle              AS detalle,
                i.fechaContable        AS fechaContable,
                i.operacionAsociada    AS operacionAsociada,
                i.codigoEmpresa        AS codigoEmpresa
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            'CUENTAS_INDIVIDUALES.CUENTASPORCOBRARTYPES',
            GETDATE()
        FROM inserted i;
    END

    IF NOT EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO [fcme_newcore].[dbo].[cdc_outbox]
            (aggregate_id, aggregate_type, event_type, payload, source_table, created_at)
        SELECT
            CAST(d.cuentaPorCobrarId AS NVARCHAR(100)),
            'cuentasPorCobrar',
            'DELETED',
            (SELECT
                d.cuenta               AS cuenta,
                d.fechaCreacion        AS fechaCreacion,
                d.fechaVencimiento     AS fechaVencimiento,
                d.numeroDocumento      AS numeroDocumento,
                d.codigoConcepto       AS codigoConcepto,
                d.moneda               AS moneda,
                d.montoOriginal        AS montoOriginal,
                d.montoPendiente       AS montoPendiente,
                d.detalle              AS detalle,
                d.fechaContable        AS fechaContable,
                d.operacionAsociada    AS operacionAsociada,
                d.codigoEmpresa        AS codigoEmpresa
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            'CUENTAS_INDIVIDUALES.CUENTASPORCOBRARTYPES',
            GETDATE()
        FROM deleted d;
    END
END
""")
conn.commit()
print("  OK")

# 4. cuentaType - already mostly correct, no changes needed
print("4. trg_cuentaType_outbox - Sin cambios necesarios")
print("   Los 4 campos faltantes no existen en la tabla TYPE y el SP los acepta como NULL")

conn.close()
print("\nTODOS LOS TRIGGERS ACTUALIZADOS")
