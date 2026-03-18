"""
Modulo: CUENTAS_INDIVIDUALES
Responsable: [NOMBRE]
Bases originales: dbRC, dbFC
"""

MODULE = {
    "MODULE_NAME": "CUENTAS_INDIVIDUALES",
    "ORIGINAL_DB": None,  # Usa multiples BDs, no una sola

    "ORIGINAL_TABLES": [],

    "NEWCORE_TO_FINAL": {
        "CUENTABLOQUEOFONDOSTYPE": ("dbRC", "rctbesta_cnta_bloq"),
        "CUENTATYPE": ("dbFC", "sfct_afiliado"),
        "CUENTASPORCOBRARTYPES": ("dbFC", "fctbmvto_pend_cbro"),
        "SALDOSTYPE": ("dbFC", "fctbsald_diar_afil_rubr"),
    },
}
