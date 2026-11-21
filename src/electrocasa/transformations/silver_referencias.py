from pyspark import pipelines as dp

from src.electrocasa.utils import preparacion as P
from src.electrocasa.utils.reglas import ADVERTENCIA, DESCARTE, FALLA

CATALOGO = spark.conf.get("electrocasa.catalogo")


@dp.materialized_view(
    name=f"{CATALOGO}.silver.catalogo_snapshots",
    comment="Un registro por producto y snapshot del catálogo, con categoría y precio normalizados",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(DESCARTE["catalogo"])
@dp.expect_all(ADVERTENCIA["catalogo"])
def catalogo_snapshots():
    bronze = spark.read.table(f"{CATALOGO}.bronze.catalogo")
    return P.catalogo_snapshots(bronze).select(*P.SILVER["catalogo_snapshots"])


@dp.materialized_view(
    name=f"{CATALOGO}.silver.catalogo",
    comment="Estado de cada producto en el último snapshot en que aparece; los descontinuados quedan marcados",
    table_properties={"quality": "silver"},
)
def catalogo():
    snapshots = spark.read.table(f"{CATALOGO}.silver.catalogo_snapshots")
    return P.catalogo_actual(snapshots)


@dp.materialized_view(
    name=f"{CATALOGO}.silver.sucursales",
    comment="Sucursales con la ciudad normalizada",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_fail(FALLA["sucursales"])
@dp.expect_all(ADVERTENCIA["sucursales"])
def sucursales():
    bronze = spark.read.table(f"{CATALOGO}.bronze.sucursales")
    return P.sucursales_validadas(bronze).select(*P.SILVER["sucursales"])
