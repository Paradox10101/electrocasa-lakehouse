from pyspark import pipelines as dp

from src.electrocasa.utils import preparacion as P
from src.electrocasa.utils.reglas import ADVERTENCIA, DESCARTE

CATALOGO = spark.conf.get("electrocasa.catalogo")


@dp.materialized_view(
    name=f"{CATALOGO}.silver.ventas",
    comment="Ventas limpias, normalizadas y sin reingestas; una fila por venta_id",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(DESCARTE["ventas"])
@dp.expect_all(ADVERTENCIA["ventas"])
def ventas():
    bronze = spark.read.table(f"{CATALOGO}.bronze.ventas")
    catalogo = spark.read.table(f"{CATALOGO}.silver.catalogo")
    return P.ventas_validadas(bronze, catalogo).select(*P.SILVER["ventas"])


@dp.materialized_view(
    name=f"{CATALOGO}.silver.devoluciones",
    comment="Devoluciones validadas contra el monto de su venta de origen; una fila por devolucion_id",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(DESCARTE["devoluciones"])
@dp.expect_all(ADVERTENCIA["devoluciones"])
def devoluciones():
    bronze = spark.read.table(f"{CATALOGO}.bronze.devoluciones")
    ventas_silver = spark.read.table(f"{CATALOGO}.silver.ventas")
    return P.devoluciones_validadas(bronze, ventas_silver).select(*P.SILVER["devoluciones"])


@dp.materialized_view(
    name=f"{CATALOGO}.silver.resenas",
    comment="Reseñas con calificación válida, sin repetidas y con metadata como VARIANT",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(DESCARTE["resenas"])
@dp.expect_all(ADVERTENCIA["resenas"])
def resenas():
    bronze = spark.read.table(f"{CATALOGO}.bronze.resenas")
    catalogo = spark.read.table(f"{CATALOGO}.silver.catalogo")
    return P.resenas_validadas(bronze, catalogo).select(*P.SILVER["resenas"])


@dp.materialized_view(
    name=f"{CATALOGO}.silver.tracking_envios",
    comment="Eventos de tracking con estado y courier estandarizados y sin mensajes reprocesados",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(DESCARTE["tracking_envios"])
@dp.expect_all(ADVERTENCIA["tracking_envios"])
def tracking_envios():
    bronze = spark.read.table(f"{CATALOGO}.bronze.tracking_envios")
    ventas_silver = spark.read.table(f"{CATALOGO}.silver.ventas")
    validado = P.tracking_validado(bronze, ventas_silver)
    return P.tracking_sin_reprocesos(validado).select(*P.SILVER["tracking_envios"])
