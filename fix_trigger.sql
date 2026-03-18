-- FIX: trg_imtbcben_outbox
-- Bug: ROW_NUMBER() dentro de subquery FOR JSON siempre retorna 1
-- Fix: Mover ROW_NUMBER a un CTE externo + offset desde MAX destino

ALTER TRIGGER dbo.trg_imtbcben_outbox
ON dbo.imtbcben
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF ISNULL(CAST(SESSION_CONTEXT(N'is_replicating') AS INT), 0) = 1 RETURN;

    -- =============================================
    -- PROPIETARIOTYPE MODEL (INSERT/UPDATE)
    -- =============================================

    IF EXISTS (SELECT 1 FROM inserted)
    BEGIN
        -- CTE para calcular ROW_NUMBER fuera del subquery FOR JSON
        -- Se suma el MAX actual del destino para garantizar IDs unicos
        ;WITH numbered AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY i.co_bene)
                    + ISNULL((SELECT MAX(propietarioId) FROM fcme_newcore.INMUEBLES.PROPIETARIOTYPE), 0) AS rn,
                i.co_bene,
                i.st_bene,
                i.fe_ingr
            FROM inserted i
            WHERE i.st_bene = 'E'
        )
        INSERT INTO fcme_legacy.dbo.cdc_outbox
        (
            aggregate_id,
            aggregate_type,
            event_type,
            payload,
            source_table
        )
        SELECT
            CAST(RTRIM(n.co_bene) AS NVARCHAR(50)),

            'propietarioType',

            CASE
                WHEN EXISTS (SELECT 1 FROM deleted) THEN 'UPDATED'
                ELSE 'CREATED'
            END,

            (
                SELECT
                    n.rn AS propietarioId,

                    RTRIM(n.co_bene) AS identificacion,

                    'PROPIETARIO - ' + RTRIM(n.co_bene) AS nombrePropietario,

                    'BENEFICIARIO' AS tipoPropietario,

                    NULL AS direccion,
                    NULL AS telefono,
                    NULL AS correo,

                    CASE
                        WHEN n.st_bene = 'E' THEN 1
                        ELSE 0
                    END AS activo,

                    n.fe_ingr AS fechaRegistro

                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),

            'imtbcben'

        FROM numbered n;
    END

    -- PROPIETARIOTYPE DELETE
    IF NOT EXISTS (SELECT 1 FROM inserted)
       AND EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO fcme_legacy.dbo.cdc_outbox
        (
            aggregate_id,
            aggregate_type,
            event_type,
            payload,
            source_table
        )
        SELECT
            CAST(RTRIM(d.co_bene) AS NVARCHAR(50)),

            'propietarioType',

            'DELETED',

            (
                SELECT
                    ISNULL(p.propietarioId, 0) AS propietarioId,

                    RTRIM(d.co_bene) AS identificacion,

                    'PROPIETARIO - ' + RTRIM(d.co_bene) AS nombrePropietario,

                    'BENEFICIARIO' AS tipoPropietario,

                    NULL AS direccion,
                    NULL AS telefono,
                    NULL AS correo,

                    0 AS activo,

                    d.fe_ingr AS fechaRegistro

                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),

            'imtbcben'

        FROM deleted d
        LEFT JOIN fcme_newcore.INMUEBLES.PROPIETARIOTYPE p
            ON p.identificacion = RTRIM(d.co_bene);
    END


    -- =============================================
    -- RESERVAINMUEBLETYPE MODEL (sin cambios)
    -- =============================================

    IF EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT INTO fcme_legacy.dbo.cdc_outbox
        (
            aggregate_id,
            aggregate_type,
            event_type,
            payload,
            source_table
        )
        SELECT
            CAST(
                CAST(i.co_prog AS VARCHAR(10)) + '-' +
                RTRIM(i.co_bene) + '-' +
                CAST(i.sc_casa AS VARCHAR(10))
            AS NVARCHAR(120)),

            'reservaInmuebleType',

            CASE
                WHEN EXISTS (SELECT 1 FROM deleted) THEN 'UPDATED'
                ELSE 'CREATED'
            END,

            (
                SELECT
                    CAST(i.co_prog AS VARCHAR(10)) + '-' +
                    RTRIM(i.co_bene) + '-' +
                    CAST(i.sc_casa AS VARCHAR(10)) AS reservaInmuebleId,

                    CAST(i.co_prog AS VARCHAR(10)) + '-' +
                    CAST(i.nu_manz AS VARCHAR(10)) + '-' +
                    CAST(i.nu_bloq AS VARCHAR(10)) + '-' +
                    CAST(i.nu_vivi AS VARCHAR(10)) AS inmuebleId,

                    RTRIM(i.co_bene) AS clienteId,

                    i.fe_ingr AS fechaOferta,

                    'USD' AS codigoMoneda,

                    i.mo_resr_vivi AS valorReserva,

                    i.nu_mese_plaz AS numeroCuotas,

                    CASE
                        WHEN i.nu_mese_plaz > 0
                        THEN i.mo_resr_vivi / i.nu_mese_plaz
                        ELSE 0
                    END AS valorCuota,

                    i.mo_entr_vivi AS valorEntrega,

                    i.st_bene AS estadoReserva,

                    CASE
                        WHEN i.in_bono = 'S' THEN 1
                        ELSE 0
                    END AS tieneBono,

                    i.nu_bono AS numeroBono,

                    i.st_bono AS estadoBono,

                    i.mo_acre_bono AS montoBono,

                    i.ds_adic AS observacion,

                    i.fe_ingr AS fechaRegistro

                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),

            'imtbcben'

        FROM inserted i;
    END

    -- RESERVAINMUEBLETYPE DELETE (sin cambios)
    IF NOT EXISTS (SELECT 1 FROM inserted)
       AND EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO fcme_legacy.dbo.cdc_outbox
        (
            aggregate_id,
            aggregate_type,
            event_type,
            payload,
            source_table
        )
        SELECT
            CAST(
                CAST(d.co_prog AS VARCHAR(10)) + '-' +
                RTRIM(d.co_bene) + '-' +
                CAST(d.sc_casa AS VARCHAR(10))
            AS NVARCHAR(120)),

            'reservaInmuebleType',

            'DELETED',

            (
                SELECT
                    CAST(d.co_prog AS VARCHAR(10)) + '-' +
                    RTRIM(d.co_bene) + '-' +
                    CAST(d.sc_casa AS VARCHAR(10)) AS reservaInmuebleId,

                    CAST(d.co_prog AS VARCHAR(10)) + '-' +
                    CAST(d.nu_manz AS VARCHAR(10)) + '-' +
                    CAST(d.nu_bloq AS VARCHAR(10)) + '-' +
                    CAST(d.nu_vivi AS VARCHAR(10)) AS inmuebleId,

                    RTRIM(d.co_bene) AS clienteId,

                    d.fe_ingr AS fechaOferta,

                    'USD' AS codigoMoneda,

                    d.mo_resr_vivi AS valorReserva,

                    d.nu_mese_plaz AS numeroCuotas,

                    CASE
                        WHEN d.nu_mese_plaz > 0
                        THEN d.mo_resr_vivi / d.nu_mese_plaz
                        ELSE 0
                    END AS valorCuota,

                    d.mo_entr_vivi AS valorEntrega,

                    d.st_bene AS estadoReserva,

                    CASE
                        WHEN d.in_bono = 'S' THEN 1
                        ELSE 0
                    END AS tieneBono,

                    d.nu_bono AS numeroBono,

                    d.st_bono AS estadoBono,

                    d.mo_acre_bono AS montoBono,

                    d.ds_adic AS observacion,

                    d.fe_ingr AS fechaRegistro

                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),

            'imtbcben'

        FROM deleted d;
    END

END;
