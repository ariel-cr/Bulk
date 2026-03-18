"""
Modulos CDC - Cada archivo define la configuracion de un modulo.

Cada persona trabaja SOLO en su archivo de modulo.
Para agregar tablas, mapeos o personalizar el generador, edita tu modulo.

Estructura de cada modulo:
    MODULE_NAME: nombre del schema en fcme_newcore
    ORIGINAL_DB: nombre de la base de datos original (dbIM, dbCR, etc.)
    ORIGINAL_TABLES: lista de tablas con triggers CDC en la BD original
    NEWCORE_TO_FINAL: dict que mapea tabla TYPE -> (db, tabla_final)
"""

from modules.inmuebles import MODULE as INMUEBLES
from modules.cuentas_individuales import MODULE as CUENTAS_INDIVIDUALES
from modules.creditos import MODULE as CREDITOS
from modules.contabilidad import MODULE as CONTABILIDAD
from modules.garantias import MODULE as GARANTIAS
from modules.inversiones import MODULE as INVERSIONES
from modules.pagos_proveedores import MODULE as PAGOS_PROVEEDORES
from modules.participe import MODULE as PARTICIPE
from modules.recaudaciones import MODULE as RECAUDACIONES
from modules.seguridad import MODULE as SEGURIDAD
from modules.cobranzas import MODULE as COBRANZAS

ALL_MODULES = {
    "INMUEBLES": INMUEBLES,
    "CUENTAS_INDIVIDUALES": CUENTAS_INDIVIDUALES,
    "CREDITOS": CREDITOS,
    "CONTABILIDAD": CONTABILIDAD,
    "GARANTIAS": GARANTIAS,
    "INVERSIONES": INVERSIONES,
    "PAGOS_PROVEEDORES": PAGOS_PROVEEDORES,
    "PARTICIPE": PARTICIPE,
    "RECAUDACIONES": RECAUDACIONES,
    "SEGURIDAD": SEGURIDAD,
    "COBRANZAS": COBRANZAS,
}


def get_all_original_dbs():
    """Retorna dict de BD original -> tablas para legacy_to_newcore.
    Extrae las tablas de ORIGINAL_TABLES y tambien de NEWCORE_TO_FINAL
    para soportar modulos con tablas en multiples BDs."""
    result = {}
    for mod in ALL_MODULES.values():
        # Tablas explicitas
        db = mod.get("ORIGINAL_DB")
        tables = mod.get("ORIGINAL_TABLES", [])
        if db and tables:
            if db not in result:
                result[db] = []
            for t in tables:
                if t not in result[db]:
                    result[db].append(t)

        # Tablas del mapeo NEWCORE_TO_FINAL (cubre multiples BDs)
        for type_table, (final_db, final_table) in mod.get("NEWCORE_TO_FINAL", {}).items():
            if final_db not in result:
                result[final_db] = []
            if final_table not in result[final_db]:
                result[final_db].append(final_table)

    return result


def get_all_newcore_to_final():
    """Retorna dict completo de tabla TYPE -> (db, tabla_final)"""
    result = {}
    for mod in ALL_MODULES.values():
        result.update(mod.get("NEWCORE_TO_FINAL", {}))
    return result
