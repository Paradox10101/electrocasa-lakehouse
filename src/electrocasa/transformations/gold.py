from pyspark import pipelines as dp

from src.electrocasa.utils import gold_logica as G
from src.electrocasa.utils import mascaras as M

CATALOGO = spark.conf.get("electrocasa.catalogo")
PROPIEDADES = {"quality": "gold"}


def _silver(nombre):
    return spark.read.table(f"{CATALOGO}.silver.{nombre}")


@dp.materialized_view(
    name=f"{CATALOGO}.gold.ventas_netas_sucursal_mes",
    comment="Pregunta 1: ventas, devoluciones, ventas netas, cantidad de ventas y ticket promedio por sucursal y mes",
    table_properties=PROPIEDADES,
)
def ventas_netas_sucursal_mes():
    return G.ventas_netas_sucursal_mes(_silver("ventas"), _silver("devoluciones"), _silver("sucursales"))


@dp.materialized_view(
    name=f"{CATALOGO}.gold.top_productos_unidades",
    comment="Pregunta 2a: los 10 productos más vendidos por unidades entre marzo y octubre de 2026",
    table_properties=PROPIEDADES,
)
def top_productos_unidades():
    return G.top_productos_unidades(_silver("ventas"), _silver("catalogo"))


@dp.materialized_view(
    name=f"{CATALOGO}.gold.top_productos_devolucion",
    comment="Pregunta 2b: los 10 productos con mayor tasa de devolución (devoluciones entre ventas del producto)",
    table_properties=PROPIEDADES,
)
def top_productos_devolucion():
    return G.top_productos_devolucion(_silver("ventas"), _silver("devoluciones"), _silver("catalogo"))


@dp.materialized_view(
    name=f"{CATALOGO}.gold.dotacion_sucursal_mes",
    comment="Pregunta 3: empleados activos por sucursal al último día de cada mes, según la historia SCD Type 2",
    table_properties=PROPIEDADES,
)
def dotacion_sucursal_mes():
    return G.dotacion_sucursal_mes(_silver("empleados_historia"), _silver("empleados_eventos"), _silver("sucursales"))


@dp.materialized_view(
    name=f"{CATALOGO}.gold.ventas_vendedor_mes",
    comment="Pregunta 4: ventas de tienda física por vendedor, sucursal que tenía en la fecha de la venta y mes",
    table_properties=PROPIEDADES,
)
def ventas_vendedor_mes():
    return G.ventas_por_vendedor_mes(_silver("ventas"), _silver("empleados_historia"), _silver("sucursales"))


@dp.materialized_view(
    name=f"{CATALOGO}.gold.resenas_negativas_categoria_mes",
    comment="Pregunta 5: porcentaje de reseñas negativas (1 o 2) por categoría del último snapshot y mes",
    table_properties=PROPIEDADES,
)
def resenas_negativas_categoria_mes():
    return G.resenas_negativas_categoria_mes(_silver("resenas"), _silver("catalogo"))


@dp.materialized_view(
    name=f"{CATALOGO}.gold.resenas_negativas_origen_mes",
    comment="Reto VARIANT: reseñas negativas por origen, versión de la app y campaña, leídos del campo metadata",
    table_properties=PROPIEDADES,
)
def resenas_negativas_origen_mes():
    return G.resenas_negativas_por_origen(_silver("resenas"), _silver("catalogo"))


@dp.materialized_view(
    name=f"{CATALOGO}.gold.despachos_courier",
    comment="Pregunta 6: horas promedio de pendiente a entregado y porcentaje de envíos devueltos por courier",
    table_properties=PROPIEDADES,
)
def despachos_courier():
    return G.despachos_por_courier(_silver("tracking_envios"))


@dp.materialized_view(
    name=f"{CATALOGO}.gold.empleados_vigentes",
    comment="Versión vigente de cada empleado; dni y salario solo se ven reales para electrocasa_ingenieria",
    schema=M.esquema_vigentes(CATALOGO),
    table_properties=PROPIEDADES,
)
def empleados_vigentes():
    return G.empleados_vigentes(_silver("empleados_historia"))
