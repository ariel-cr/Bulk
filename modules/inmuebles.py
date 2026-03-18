"""
Modulo: INMUEBLES
Responsable: [NOMBRE]
Base original: dbIM
"""

MODULE = {
    "MODULE_NAME": "INMUEBLES",
    "ORIGINAL_DB": "dbIM",

    # Tablas en dbIM que tienen triggers CDC
    "ORIGINAL_TABLES": [
        "imtbbene_entr",
        "imtbbene_firm",
        "imtbcben",
        "imtbcpro",
        "imtbdivi",
        "imtbmanz",
        "imtbmejo_tram",
        "imtbtviv",
    ],

    # Mapeo: tabla TYPE en newcore -> (bd_original, tabla_final)
    "NEWCORE_TO_FINAL": {
        "CIUDADELATYPE": ("dbIM", "imtbcpro"),
        "INMUEBLETYPE": ("dbIM", "imtbdivi"),
        "MANZANATYPE": ("dbIM", "imtbmanz"),
        "PROGRAMAVIVIENDATYPE": ("dbIM", "imtbcpro"),
        "PROPIETARIOTYPE": ("dbIM", "imtbcben"),
        "RESERVAINMUEBLETYPE": ("dbIM", "imtbcben"),
        "VENTAINMUEBLETYPE": ("dbIM", "imtbbene_entr"),
        "GASTOMANTENIMIENTOTYPE": ("dbIM", "imtbmejo_tram"),
        "ORDENTRABAJOTYPE": ("dbIM", "imtbmejo_tram"),
    },
}
