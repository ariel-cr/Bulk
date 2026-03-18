"""
Modulo: SEGURIDAD
Responsable: [NOMBRE]
Base original: dbSG
"""

MODULE = {
    "MODULE_NAME": "SEGURIDAD",
    "ORIGINAL_DB": "dbSG",

    "ORIGINAL_TABLES": [],

    "NEWCORE_TO_FINAL": {
        "USUARIOTYPE": ("dbSG", "sgtbusua"),
        "ROLTYPE": ("dbSG", "sgtbrol"),
        "PERFILACCESOTYPE": ("dbSG", "sgtbperf"),
        "OPCIONMENUTYPE": ("dbSG", "sgtbtran"),
        "PERFILOPCIONMENUTYPE": ("dbSG", "sgtbautr_perf"),
        "USUARIOPERFILACCESOTYPE": ("dbSG", "sgtbperf_usua"),
    },
}
