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
    """Retorna dict de BD original -> tablas para legacy_to_newcore"""
    result = {}
    for mod in ALL_MODULES.values():
        db = mod.get("ORIGINAL_DB")
        tables = mod.get("ORIGINAL_TABLES", [])
        if db and tables:
            if db not in result:
                result[db] = []
            result[db].extend(tables)
    return result


def get_all_newcore_to_final():
    """Retorna dict completo de tabla TYPE -> (db, tabla_final)"""
    result = {}
    for mod in ALL_MODULES.values():
        result.update(mod.get("NEWCORE_TO_FINAL", {}))
    return result
