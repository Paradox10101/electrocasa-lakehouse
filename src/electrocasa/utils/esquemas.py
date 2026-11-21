import json

CATALOGO = (
    "producto_id STRING, nombre_producto STRING, categoria STRING, marca STRING, precio_lista STRING, "
    "activo STRING"
)

SUCURSALES = (
    "sucursal_id STRING, nombre_sucursal STRING, distrito STRING, ciudad STRING, region STRING, "
    "fecha_apertura STRING"
)

AUDITORIA_COPY_INTO = (
    "source_file STRING, ingestion_timestamp TIMESTAMP, source_system STRING, batch_id STRING, job_run_id STRING"
)

COLUMNAS_AUDITORIA = ("source_file", "ingestion_timestamp", "source_system", "batch_id")

CLAVES_OBLIGATORIAS = ("tabla", "carpeta", "formato", "esquema", "sistema", "opciones")


def cargar_fuentes(ruta):
    with open(ruta, encoding="utf-8") as archivo:
        fuentes = json.load(archivo)
    if not isinstance(fuentes, list) or not fuentes:
        raise ValueError(f"La configuración de fuentes en {ruta} debe ser una lista no vacía")
    for fuente in fuentes:
        faltantes = [c for c in CLAVES_OBLIGATORIAS if c not in fuente]
        if faltantes:
            raise ValueError(f"A la fuente {fuente.get('tabla', '?')} le faltan las claves {faltantes}")
        if fuente["formato"] not in ("csv", "json"):
            raise ValueError(f"Formato no soportado en {fuente['tabla']}: {fuente['formato']}")
    return fuentes
