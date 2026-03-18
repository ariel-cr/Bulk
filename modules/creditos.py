"""
Modulo: CREDITOS
Responsable: [NOMBRE]
Base original: dbCR
"""

MODULE = {
    "MODULE_NAME": "CREDITOS",
    "ORIGINAL_DB": "dbCR",

    "ORIGINAL_TABLES": [
        "crtbcobr_judi_dist",
        "crtbgara_real",
        "crtbgest_cart_asig",
        "crtbgest_deta_cbnz",
        "crtbmedi_cobr",
        "crtboper_segu",
        "crtoblig",
        "crtplpag",
        "crtrecup",
        "crtsolid",
    ],

    "NEWCORE_TO_FINAL": {
        "CREDITOGENERALTYPE": ("dbCR", "crtoblig"),
        "CREDITOTYPE": ("dbCR", "crtoblig"),
        "PRESTAMOTYPE": ("dbCR", "crtoblig"),
        "REFINANCIAMIENTOCREDITOTYPE": ("dbCR", "crtoblig"),
        "PERSONAGARANTETYPE": ("dbCR", "crtsolid"),
        "CUOTACREDITOTYPE": ("dbCR", "crtplpag"),
        "PAGOSCREDITOTYPE": ("dbCR", "crtrecup"),
    },
}
