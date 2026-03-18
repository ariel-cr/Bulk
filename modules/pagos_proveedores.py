"""
Modulo: PAGOS_PROVEEDORES
Responsable: [NOMBRE]
Base original: dbCG
"""

MODULE = {
    "MODULE_NAME": "PAGOS_PROVEEDORES",
    "ORIGINAL_DB": "dbCG",

    "ORIGINAL_TABLES": [
        "cgtbcasi",
        "cgtbcaut_cpla",
        "cgtbcaut_dpla",
        "cgtbcfac",
        "cgtbcier_proc",
        "cgtbcncl",
        "cgtbcncl_deta",
        "cgtbconc",
        "cgtbdasi",
        "cgtbdcto",
        "cgtbdfac_nota_cred",
        "cgtbdfac_prod",
        "cgtbdfac_rete",
        "cgtbfact_dbso",
        "cgtbgara_hipo_cdio",
        "cgtbgara_vehi_cdio",
        "cgtbplcn",
        "cgtbprod",
        "cgtbprvd",
        "cgtbrete",
    ],

    "NEWCORE_TO_FINAL": {
        "PROVEEDORTYPE": ("dbCG", "cgtbprvd"),
        "DOCUMENTOPROVEEDORTYPE": ("dbCG", "cgtbcfac"),
        "DOCUMENTOPROVEEDORDETALLETYPE": ("dbCG", "cgtbcfac"),
        "PAGOPROVEEDORTYPE": ("dbCG", "cgtbfact_dbso"),
        "PAGODOCUMENTOTYPE": ("dbCG", "cgtbdfac_nota_cred"),
        "RETENCIONPAGOTYPE": ("dbCG", "cgtbdfac_rete"),
        "TIPODOCUMENTOPROVEEDORTYPE": ("dbCG", "cgtbdcto"),
    },
}
