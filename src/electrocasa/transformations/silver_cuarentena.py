from functools import reduce

from pyspark import pipelines as dp

from src.electrocasa.utils import preparacion as P

CATALOGO = spark.conf.get("electrocasa.catalogo")


def _tabla(capa, nombre):
    return spark.read.table(f"{CATALOGO}.{capa}.{nombre}")


@dp.materialized_view(
    name=f"{CATALOGO}.silver.cuarentena",
    comment=(
        "Registros rechazados por una regla crítica: fuente, llave, reglas que fallaron, lote, archivo, "
        "fechas de ingesta y de procesamiento, y el registro original en JSON para reprocesarlo"
    ),
    table_properties={"quality": "silver"},
)
def cuarentena():
    catalogo = _tabla("silver", "catalogo")
    ventas_silver = _tabla("silver", "ventas")
    rechazos = [
        P.rechazos(P.ventas_validadas(_tabla("bronze", "ventas"), catalogo), "ventas", "venta_id"),
        P.rechazos(P.devoluciones_validadas(_tabla("bronze", "devoluciones"), ventas_silver), "devoluciones",
                   "devolucion_id"),
        P.rechazos(P.resenas_validadas(_tabla("bronze", "resenas"), catalogo), "resenas", "resena_id"),
        P.rechazos(P.empleados_validados(_tabla("bronze", "empleados")), "empleados", "id_empleado"),
        P.rechazos(P.catalogo_snapshots(_tabla("bronze", "catalogo")), "catalogo", "producto_id"),
        P.rechazos(P.sucursales_validadas(_tabla("bronze", "sucursales")), "sucursales", "sucursal_id"),
        P.rechazos(P.tracking_validado(_tabla("bronze", "tracking_envios"), ventas_silver), "tracking_envios",
                   "evento_id"),
    ]
    return reduce(lambda a, b: a.unionByName(b), rechazos)
