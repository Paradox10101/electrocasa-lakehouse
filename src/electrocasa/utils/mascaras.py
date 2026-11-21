def funcion_dni(catalogo):
    return f"{catalogo}.silver.mascara_dni"


def funcion_salario(catalogo):
    return f"{catalogo}.silver.mascara_salario"


def esquema_eventos(catalogo):
    return (
        "id_empleado STRING NOT NULL, nombre STRING, "
        f"dni STRING MASK {funcion_dni(catalogo)}, email STRING, "
        f"salario DECIMAL(10,2) MASK {funcion_salario(catalogo)}, salario_corregido BOOLEAN, "
        "sucursal_id STRING, cargo STRING, tipo_evento STRING, fecha_evento DATE, fecha_registro TIMESTAMP, "
        "lote STRING, source_file STRING, ingestion_timestamp TIMESTAMP"
    )


def esquema_historia(catalogo):
    orden = "STRUCT<fecha_evento: DATE, fecha_registro: TIMESTAMP>"
    return (
        "id_empleado STRING NOT NULL, nombre STRING, "
        f"dni STRING MASK {funcion_dni(catalogo)}, email STRING, "
        f"salario DECIMAL(10,2) MASK {funcion_salario(catalogo)}, salario_corregido BOOLEAN, "
        "sucursal_id STRING, cargo STRING, tipo_evento STRING, fecha_evento DATE, fecha_registro TIMESTAMP, "
        f"lote STRING, __START_AT {orden}, __END_AT {orden}"
    )


def esquema_vigentes(catalogo):
    return (
        "id_empleado STRING NOT NULL, nombre STRING, "
        f"dni STRING MASK {funcion_dni(catalogo)}, email STRING, "
        f"salario DECIMAL(10,2) MASK {funcion_salario(catalogo)}, "
        "sucursal_id STRING, cargo STRING, vigente_desde DATE"
    )


def esquema_dni_duplicados(catalogo):
    return (
        f"dni STRING MASK {funcion_dni(catalogo)}, empleados_distintos BIGINT, ids_empleado ARRAY<STRING>"
    )
