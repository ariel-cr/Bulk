"""
Modulo: PARTICIPE
Responsable: [NOMBRE]
Base original: dbFC
"""

MODULE = {
    "MODULE_NAME": "PARTICIPE",
    "ORIGINAL_DB": "dbFC",

    "ORIGINAL_TABLES": [
        "fctbafil_actu",
        "fctbafil_info_adic",
        "fctbagen_telf",
        "fctbmvto_pend_cbro",
        "fctbotro_ingr_afil",
        "fctbsald_diar_afil_rubr",
        "sfct_afiliado",
        "sfct_afiliado_auditor",
        "sfct_beneficiario",
        "sfct_cabecera_rol",
        "sfct_estados_afiliado",
        "sfct_movimiento",
    ],

    "NEWCORE_TO_FINAL": {
        "PERSONATYPE": ("dbFC", "sfct_afiliado"),
        "DIRECCIONTYPE": ("dbFC", "fctbafil_actu"),
        "TELEFONOTYPE": ("dbFC", "fctbagen_telf"),
        "INFOADICIONALTYPE": ("dbFC", "fctbafil_info_adic"),
        "INFOBASICATYPE": ("dbFC", "fctbafil_actu"),
        "INGRESOSEGRESOSTYPE": ("dbFC", "fctbotro_ingr_afil"),
        "PERSONAVINCULADATYPE": ("dbFC", "sfct_beneficiario"),
        "SALDOPERSONANATURALTYPE": ("dbFC", "fctbsald_diar_afil_rubr"),
        "HISTORICOESTADOSTYPE": ("dbFC", "sfct_afiliado_auditor"),
    },
}
