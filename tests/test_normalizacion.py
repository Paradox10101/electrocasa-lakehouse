import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.electrocasa.utils import normalizacion as N
from src.electrocasa.utils import preparacion as P


@pytest.fixture(scope="session")
def spark():
    sesion = (SparkSession.builder.master("local[1]").appName("pruebas_electrocasa")
              .config("spark.sql.ansi.enabled", "true").config("spark.ui.enabled", "false").getOrCreate())
    yield sesion
    sesion.stop()


def _valores(spark, valores, expresion):
    df = spark.createDataFrame([(v,) for v in valores], "valor STRING")
    return [fila[0] for fila in df.select(expresion("valor")).collect()]


def test_montos_con_distintos_separadores(spark):
    entrada = ["6792.35", "3.468,69", "407,98", "S/ 1,474.75", "", None, "-1602.5"]
    esperado = [Decimal("6792.35"), Decimal("3468.69"), Decimal("407.98"), Decimal("1474.75"), None, None,
                Decimal("-1602.50")]
    assert _valores(spark, entrada, N.monto) == esperado


def test_fechas_en_tres_formatos(spark):
    entrada = ["2026-06-07", "07/06/2026", "2026-06-07T14:30:00", "fecha rara", ""]
    esperado = [datetime.date(2026, 6, 7)] * 3 + [None, None]
    assert _valores(spark, entrada, N.fecha) == esperado


@pytest.mark.parametrize("variantes, mapa, canonico", [
    (["PLIN", "Plin"], N.METODOS_PAGO, "Plin"),
    (["TC", "td", "Tarjeta de credito", "TARJETA"], N.METODOS_PAGO, "Tarjeta"),
    (["EFV", "efectivo"], N.METODOS_PAGO, "Efectivo"),
    (["TRF", "transferencia"], N.METODOS_PAGO, "Transferencia"),
    (["Línea blanca", "linea_blanca", "LINEA BLANCA"], N.CATEGORIAS, "Línea blanca"),
    (["Climatización", "CLIMATIZACION"], N.CATEGORIAS, "Climatización"),
    (["Cambio Salario", "CAMBIO_SALARIO", "cambio_salario"], N.TIPOS_EVENTO, "cambio_salario"),
    (["cese", "BAJA"], N.TIPOS_EVENTO, "baja"),
    (["En camino", "EN_CAMINO", "en_transito", "En transito"], N.ESTADOS_ENTREGA, "en_transito"),
    (["Olva", "OLVA COURIER", "olva courier", "Olva Courier"], N.COURIERS, "Olva Courier"),
    (["SERVIENTREGA", "servientrega"], N.COURIERS, "Servientrega"),
])
def test_estandarizacion_a_conjunto_cerrado(spark, variantes, mapa, canonico):
    assert set(_valores(spark, variantes, lambda c: N.estandarizar(c, mapa))) == {canonico}


def test_valor_desconocido_queda_nulo(spark):
    assert _valores(spark, ["bitcoin"], lambda c: N.estandarizar(c, N.METODOS_PAGO)) == [None]


def test_fecha_desde_nombre_de_archivo(spark):
    df = spark.createDataFrame([("catalogo_productos_2026-08-01.json",)], "source_file STRING")
    assert df.select(N.fecha_desde_archivo("source_file")).first()[0] == datetime.date(2026, 8, 1)


def test_deduplicacion_prefiere_la_version_valida(spark):
    filas = [
        ("VTA-1", False, datetime.datetime(2026, 5, 31, 10), "lote_01"),
        ("VTA-1", True, datetime.datetime(2026, 8, 31, 10), "reproceso"),
        ("VTA-2", True, datetime.datetime(2026, 5, 31, 10), "lote_01"),
        ("VTA-2", True, datetime.datetime(2026, 8, 31, 10), "lote_02"),
        (None, False, datetime.datetime(2026, 5, 31, 10), "lote_01"),
        (None, False, datetime.datetime(2026, 5, 31, 10), "lote_01"),
    ]
    df = spark.createDataFrame(filas, "venta_id STRING, es_valido BOOLEAN, ingestion_timestamp TIMESTAMP, source_file STRING")
    resultado = {(f["venta_id"], f["source_file"]) for f in P.deduplicar(df, "venta_id").collect()}
    assert ("VTA-1", "reproceso") in resultado
    assert ("VTA-2", "lote_01") in resultado
    assert P.deduplicar(df, "venta_id").filter("venta_id IS NULL").count() == 2


def test_salario_sin_punto_decimal_se_corrige(spark):
    columnas = P.ORIGEN["empleados"]
    fila = ("EMP-1", "Nombre", "123", "a@b.pe", "246616.0", "S01", "Vendedor", "ALTA", "2026-03-01", "2026-03-02 10:00:00")
    df = (spark.createDataFrame([fila], ", ".join(f"{c} STRING" for c in columnas))
          .withColumn("source_file", F.lit("empleados_lote_01_2026-05-31.csv"))
          .withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("batch_id", F.lit("empleados_lote_01_2026-05-31")))
    resultado = P.preparar_empleados(df).first()
    assert resultado["salario"] == Decimal("2466.16")
    assert resultado["salario_corregido"] is True
    assert resultado["tipo_evento"] == "alta"
