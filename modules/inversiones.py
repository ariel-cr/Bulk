"""
Modulo: INVERSIONES
Responsable: [NOMBRE]
Base original: dbIN
"""

MODULE = {
    "MODULE_NAME": "INVERSIONES",
    "ORIGINAL_DB": "dbIN",

    "ORIGINAL_TABLES": [
        "intbcabe_dbso_inve",
        "intbcinv",
        "intbdcto_cble",
        "intbdinv",
        "intbdpag",
        "intbdpre_inve",
        "intbemis",
        "intbgara",
        "intbprec_diar",
        "intbvinv",
        "intbvinv_audi",
    ],

    "NEWCORE_TO_FINAL": {
        "INVERSIONTYPE": ("dbIN", "intbcinv"),
        "INVERSIONRENTAVARIABLETYPE": ("dbIN", "intbdinv"),
        "TABLAAMORTIZACIONTYPE": ("dbIN", "intbvinv"),
        "HISTORIALINVERSIONTYPE": ("dbIN", "intbvinv_audi"),
        "EMISORDETALLETYPE": ("dbIN", "intbemis"),
        "PRECANCELACIONTYPE": ("dbIN", "intbdpre_inve"),
        "PRECIOCIERRETYPE": ("dbIN", "intbprec_diar"),
        "VECTORPRECIOSTYPE": ("dbIN", "intbprec_diar"),
        "RENTABILIDADTYPE": ("dbIN", "intbcinv"),
        "BANCODETALLETYPE": ("dbIN", "intbcabe_dbso_inve"),
    },
}
