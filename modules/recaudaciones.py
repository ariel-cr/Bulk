"""
Modulo: RECAUDACIONES
Responsable: [NOMBRE]
Bases originales: dbRC, dbFC, dbCR, dbCT
"""

MODULE = {
    "MODULE_NAME": "RECAUDACIONES",
    "ORIGINAL_DB": None,

    "ORIGINAL_TABLES": [],

    "NEWCORE_TO_FINAL": {
        "RECAUDACIONTYPE": ("dbRC", "rctbreca"),
        "RECAUDACIONDETALLETYPE": ("dbRC", "rctbrind_deta"),
        "RECUPERACIONTYPE": ("dbRC", "rctbdsto_rol"),
        "RECUPERACIONDETALLETYPE": ("dbRC", "rctbdsto_rol"),
        "ROLPAGOTYPE": ("dbFC", "sfct_cabecera_rol"),
        "TRANSACCIONCUENTATYPE": ("dbFC", "sfct_movimiento"),
        "TRANSACCIONPRESTAMOTYPE": ("dbCR", "crtrecup"),
        "DISTRITOTYPE": ("dbCT", "cttbdist_cant"),
    },
}
