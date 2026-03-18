"""
Modulo: CUENTAS_INDIVIDUALES
Responsable: [NOMBRE]
Bases originales: dbRC (bloqueo fondos), dbFC (cuentas, saldos, cuentas por cobrar)
"""

MODULE = {
    "MODULE_NAME": "CUENTAS_INDIVIDUALES",
    "ORIGINAL_DB": "dbFC",  # BD principal para el desplegable

    # Tablas originales con triggers CDC
    "ORIGINAL_TABLES": [
        "sfct_afiliado",            # -> cuentaType
        "fctbmvto_pend_cbro",       # -> cuentasPorCobrar
        "fctbsald_diar_afil_rubr",  # -> saldosType
    ],

    # Tablas en dbRC (se agregan aparte porque es otra BD)
    "ORIGINAL_TABLES_DBRC": [
        "rctbesta_cnta_bloq",       # -> cuentaBloqueoFondos
    ],

    # Mapeo: tabla TYPE en newcore -> (bd_original, tabla_final)
    "NEWCORE_TO_FINAL": {
        "CUENTABLOQUEOFONDOSTYPE": ("dbRC", "rctbesta_cnta_bloq"),
        "CUENTATYPE": ("dbFC", "sfct_afiliado"),
        "CUENTASPORCOBRARTYPES": ("dbFC", "fctbmvto_pend_cbro"),
        "SALDOSTYPE": ("dbFC", "fctbsald_diar_afil_rubr"),
    },
}
