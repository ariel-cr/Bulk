"""
Modulo: CONTABILIDAD
Responsable: [NOMBRE]
Base original: dbCT
"""

MODULE = {
    "MODULE_NAME": "CONTABILIDAD",
    "ORIGINAL_DB": None,

    "ORIGINAL_TABLES": [],

    "NEWCORE_TO_FINAL": {
        "COMPROBANTETYPE": ("dbCT", "cttbcmpr"),
        "COMPROBANTEDETALLETYPE": ("dbCT", "cttbdcmpr"),
        "CONCEPTOCONTABLETYPE": ("dbCT", "cttbcncl"),
        "CONCILIACIONBANCARIATYPE": ("dbCT", "cttbconc"),
        "CONTROLCONTABLETYPE": ("dbCT", "cttbctrl"),
        "CONTROLMESTYPE": ("dbCT", "cttbctrl_mes"),
        "CUENTACONTABLETYPE": ("dbCT", "cttbccta"),
        "EXTRACTOBANCARIOTYPE": ("dbCT", "cttbextr"),
    },
}
