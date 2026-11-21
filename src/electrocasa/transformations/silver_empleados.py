from pyspark import pipelines as dp
from pyspark.sql import functions as F

from src.electrocasa.utils import gold_logica as G
from src.electrocasa.utils import mascaras as M
from src.electrocasa.utils import preparacion as P
from src.electrocasa.utils.reglas import ADVERTENCIA, DESCARTE

CATALOGO = spark.conf.get("electrocasa.catalogo")
HISTORIA = f"{CATALOGO}.silver.empleados_historia"


@dp.temporary_view(
    name="empleados_cambios",
    comment="Eventos de RR.HH. normalizados que alimentan la historia SCD Type 2",
)
@dp.expect_all_or_drop(DESCARTE["empleados"])
@dp.expect_all(ADVERTENCIA["empleados"])
def empleados_cambios():
    bronze = spark.readStream.table(f"{CATALOGO}.bronze.empleados")
    return P.preparar_empleados(bronze).select(*P.SILVER["empleados_cambios"])


dp.create_streaming_table(
    name=HISTORIA,
    comment="Historia SCD Type 2 de cada empleado: una versión por cambio, con su vigencia en __START_AT y __END_AT",
    schema=M.esquema_historia(CATALOGO),
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=HISTORIA,
    source="empleados_cambios",
    keys=["id_empleado"],
    sequence_by=F.struct("fecha_evento", "fecha_registro"),
    apply_as_deletes=F.expr("tipo_evento = 'baja'"),
    ignore_null_updates_column_list=["dni"],
    stored_as_scd_type=2,
    name="empleados_historia_scd2",
)


@dp.materialized_view(
    name=f"{CATALOGO}.silver.empleados_eventos",
    comment="Eventos de RR.HH. válidos y normalizados, con dni y salario enmascarados",
    schema=M.esquema_eventos(CATALOGO),
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(DESCARTE["empleados"])
def empleados_eventos():
    bronze = spark.read.table(f"{CATALOGO}.bronze.empleados")
    return P.preparar_empleados(bronze).select(
        *P.SILVER["empleados_cambios"], "source_file", "ingestion_timestamp"
    )


@dp.materialized_view(
    name=f"{CATALOGO}.silver.dni_duplicados",
    comment="DNI registrados en más de un empleado, para revisión de RR.HH.",
    schema=M.esquema_dni_duplicados(CATALOGO),
    table_properties={"quality": "silver"},
)
def dni_duplicados():
    return G.dni_duplicados(spark.read.table(f"{CATALOGO}.silver.empleados_eventos"))
