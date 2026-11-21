from pyspark import pipelines as dp
from pyspark.sql import functions as F

from src.electrocasa.utils.esquemas import cargar_fuentes
from src.electrocasa.utils.normalizacion import lote_desde_archivo

CATALOGO = spark.conf.get("electrocasa.catalogo")
RUTA_LANDING = spark.conf.get("electrocasa.ruta_landing")
RUTA_CONFIG = spark.conf.get("electrocasa.config_fuentes")


def definir_bronze(fuente):
    @dp.table(
        name=f"{CATALOGO}.bronze.{fuente['tabla']}",
        comment=f"{fuente['tabla']} tal como llega al volume de landing, con columnas técnicas de auditoría",
        table_properties={"quality": "bronze", "delta.appendOnly": "true"},
    )
    def tabla_bronze():
        lector = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fuente["formato"])
            .option("rescuedDataColumn", "_rescued_data")
            .options(**fuente["opciones"])
            .schema(fuente["esquema"])
        )
        return (
            lector.load(f"{RUTA_LANDING}/{fuente['carpeta']}/")
            .withColumn("source_file", F.col("_metadata.file_name"))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_system", F.lit(fuente["sistema"]))
            .withColumn("batch_id", lote_desde_archivo("source_file"))
        )

    return tabla_bronze


for fuente_configurada in cargar_fuentes(RUTA_CONFIG):
    definir_bronze(fuente_configurada)
