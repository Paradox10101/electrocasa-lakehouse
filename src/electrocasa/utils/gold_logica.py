from pyspark.sql import Window
from pyspark.sql import functions as F

from src.electrocasa.utils.normalizacion import mes

INICIO_PERIODO = "2026-03-01"
FIN_PERIODO = "2026-10-31"
SIN_ASIGNAR = "SIN_ASIGNAR"


def en_periodo(columna):
    return F.col(columna).between(F.lit(INICIO_PERIODO).cast("date"), F.lit(FIN_PERIODO).cast("date"))


def _datos_sucursal(sucursales):
    return sucursales.select("sucursal_id", "nombre_sucursal", "ciudad", "region")


def ventas_netas_sucursal_mes(ventas, devoluciones, sucursales):
    v = (ventas.filter(en_periodo("fecha_venta"))
         .groupBy("sucursal_id", mes("fecha_venta").alias("mes"))
         .agg(F.sum("monto_total").alias("ventas"), F.count_distinct("venta_id").alias("cantidad_ventas")))
    d = (devoluciones.filter(en_periodo("fecha_devolucion"))
         .groupBy("sucursal_id", mes("fecha_devolucion").alias("mes"))
         .agg(F.sum("monto_reembolso").alias("devoluciones"), F.count("*").alias("cantidad_devoluciones")))
    cero = F.lit(0).cast("decimal(22,2)")
    unido = v.join(d, ["sucursal_id", "mes"], "full")
    resultado = unido.select(
        "sucursal_id",
        "mes",
        F.coalesce("ventas", cero).alias("ventas"),
        F.coalesce("devoluciones", cero).alias("devoluciones"),
        (F.coalesce("ventas", cero) - F.coalesce("devoluciones", cero)).alias("ventas_netas"),
        F.coalesce("cantidad_ventas", F.lit(0)).alias("cantidad_ventas"),
        F.coalesce("cantidad_devoluciones", F.lit(0)).alias("cantidad_devoluciones"),
        F.when(F.col("cantidad_ventas") > 0, F.round(F.col("ventas") / F.col("cantidad_ventas"), 2))
        .alias("ticket_promedio"),
    )
    return resultado.join(_datos_sucursal(sucursales), "sucursal_id", "left")


def _top(df, columnas_orden, n):
    ventana = Window.orderBy(*columnas_orden)
    return df.withColumn("puesto", F.row_number().over(ventana)).filter(F.col("puesto") <= n)


def top_productos_unidades(ventas, catalogo, n=10):
    agregado = (ventas.filter(en_periodo("fecha_venta")).groupBy("producto_id")
                .agg(F.sum("cantidad").alias("unidades"), F.count_distinct("venta_id").alias("cantidad_ventas")))
    top = _top(agregado, [F.col("unidades").desc(), F.col("producto_id").asc()], n)
    return top.join(catalogo.select("producto_id", "nombre_producto", "categoria", "marca"), "producto_id", "left")


def top_productos_devolucion(ventas, devoluciones, catalogo, n=10):
    vendidos = (ventas.filter(en_periodo("fecha_venta")).groupBy("producto_id")
                .agg(F.count_distinct("venta_id").alias("cantidad_ventas")))
    devueltos = (devoluciones.filter(en_periodo("fecha_devolucion")).groupBy("producto_id")
                 .agg(F.count_distinct("devolucion_id").alias("cantidad_devoluciones")))
    tasa = (vendidos.join(devueltos, "producto_id", "left")
            .withColumn("cantidad_devoluciones", F.coalesce("cantidad_devoluciones", F.lit(0)))
            .withColumn("tasa_devolucion", F.round(F.col("cantidad_devoluciones") / F.col("cantidad_ventas"), 4)))
    orden = [F.col("tasa_devolucion").desc(), F.col("cantidad_devoluciones").desc(), F.col("producto_id").asc()]
    top = _top(tasa, orden, n)
    return top.join(catalogo.select("producto_id", "nombre_producto", "categoria", "marca"), "producto_id", "left")


def cortes_de_mes(spark):
    inicio = F.lit(INICIO_PERIODO).cast("date")
    ultimo_mes = F.trunc(F.lit(FIN_PERIODO).cast("date"), "month")
    meses = spark.range(1).select(F.explode(F.sequence(inicio, ultimo_mes, F.expr("INTERVAL 1 MONTH"))).alias("mes"))
    return meses.select("mes", F.last_day("mes").alias("fecha_corte"))


def _intervalos(versiones):
    return versiones.select(
        "id_empleado",
        F.col("sucursal_id").alias("sucursal_version"),
        F.col("__START_AT.fecha_evento").alias("vigente_desde"),
        F.col("__END_AT.fecha_evento").alias("vigente_hasta"),
        F.col("__START_AT").alias("inicio_version"),
    )


def _vigente(fecha):
    return (F.col("vigente_desde") <= fecha) & (F.col("vigente_hasta").isNull() | (F.col("vigente_hasta") > fecha))


def dotacion_sucursal_mes(versiones, eventos, sucursales):
    cortes = cortes_de_mes(versiones.sparkSession)
    primera_alta = (eventos.filter(F.col("tipo_evento") == "alta").groupBy("id_empleado")
                    .agg(F.min("fecha_evento").alias("primera_alta")))
    activos = (_intervalos(versiones).join(cortes, _vigente(F.col("fecha_corte")))
               .join(primera_alta, "id_empleado")
               .filter(F.col("primera_alta") <= F.col("fecha_corte")))
    conteo = (activos.groupBy(F.col("sucursal_version").alias("sucursal_id"), "mes", "fecha_corte")
              .agg(F.count_distinct("id_empleado").alias("empleados_activos")))
    base = _datos_sucursal(sucursales).crossJoin(cortes)
    return (base.join(conteo, ["sucursal_id", "mes", "fecha_corte"], "left")
            .withColumn("empleados_activos", F.coalesce("empleados_activos", F.lit(0))))


def ventas_por_vendedor_mes(ventas, versiones, sucursales):
    tienda = ventas.filter((F.col("canal") == "tienda_fisica") & en_periodo("fecha_venta"))
    intervalos = _intervalos(versiones)
    cruce = tienda.join(
        intervalos,
        (tienda["vendedor_id"] == intervalos["id_empleado"]) & _vigente(tienda["fecha_venta"]),
        "left",
    )
    unica = Window.partitionBy("venta_id").orderBy(F.col("inicio_version").desc_nulls_last())
    asignada = (cruce.withColumn("_fila", F.row_number().over(unica)).filter("_fila = 1")
                .withColumn("sucursal_id", F.coalesce("sucursal_version", F.lit(SIN_ASIGNAR))))
    agregado = (asignada.groupBy("vendedor_id", "sucursal_id", mes("fecha_venta").alias("mes"))
                .agg(F.sum("monto_total").alias("monto_ventas"), F.count_distinct("venta_id").alias("cantidad_ventas")))
    return agregado.join(_datos_sucursal(sucursales), "sucursal_id", "left")


def resenas_negativas_categoria_mes(resenas, catalogo):
    con_categoria = (resenas.filter(en_periodo("fecha_resena"))
                     .join(catalogo.select("producto_id", "categoria"), "producto_id", "inner")
                     .filter(F.col("categoria").isNotNull()))
    return (con_categoria.groupBy("categoria", mes("fecha_resena").alias("mes"))
            .agg(F.count("*").alias("total_resenas"),
                 F.sum(F.when(F.col("es_negativa"), 1).otherwise(0)).alias("resenas_negativas"))
            .withColumn("porcentaje_negativas",
                        F.round(F.col("resenas_negativas") * 100.0 / F.col("total_resenas"), 2)))


def resenas_negativas_por_origen(resenas, catalogo):
    con_producto = (resenas.filter(en_periodo("fecha_resena"))
                    .join(catalogo.select("producto_id"), "producto_id", "inner"))
    version_app = F.try_variant_get(F.col("metadata"), "$.app.version", "string")
    campania = F.try_variant_get(F.col("metadata"), "$.campania.codigo", "string")
    return (con_producto.groupBy(
                F.coalesce("origen", F.lit("desconocido")).alias("origen"),
                F.coalesce(version_app, F.lit("no_aplica")).alias("version_app"),
                F.coalesce(campania, F.lit("sin_campania")).alias("campania"),
                mes("fecha_resena").alias("mes"))
            .agg(F.count("*").alias("total_resenas"),
                 F.sum(F.when(F.col("es_negativa"), 1).otherwise(0)).alias("resenas_negativas"))
            .withColumn("porcentaje_negativas",
                        F.round(F.col("resenas_negativas") * 100.0 / F.col("total_resenas"), 2)))


def despachos_por_courier(tracking):
    por_envio = tracking.groupBy("tracking_id").agg(
        F.min_by("courier", "fecha_modificacion").alias("courier"),
        F.min(F.when(F.col("estado_entrega") == "pendiente", F.col("fecha_modificacion"))).alias("primer_pendiente"),
        F.min(F.when(F.col("estado_entrega") == "entregado", F.col("fecha_modificacion"))).alias("primer_entregado"),
        F.max(F.when(F.col("estado_entrega") == "devuelto", 1).otherwise(0)).alias("devuelto"),
    )
    horas = (F.unix_timestamp("primer_entregado") - F.unix_timestamp("primer_pendiente")) / 3600.0
    finalizado = F.col("primer_entregado").isNotNull() | (F.col("devuelto") == 1)
    detalle = (por_envio
               .withColumn("horas_hasta_entrega",
                           F.when(F.col("primer_pendiente").isNotNull() & F.col("primer_entregado").isNotNull(), horas))
               .withColumn("finalizado", finalizado))
    return (detalle.groupBy("courier").agg(
                F.count("*").alias("envios"),
                F.count("horas_hasta_entrega").alias("envios_entregados_medidos"),
                F.round(F.avg("horas_hasta_entrega"), 2).alias("horas_promedio_entrega"),
                F.sum(F.when(F.col("finalizado"), 1).otherwise(0)).alias("envios_finalizados"),
                F.sum(F.when(F.col("finalizado") & (F.col("devuelto") == 1), 1).otherwise(0)).alias("envios_devueltos"))
            .withColumn("porcentaje_devueltos",
                        F.round(F.col("envios_devueltos") * 100.0 / F.col("envios_finalizados"), 2)))


def empleados_vigentes(versiones):
    return versiones.filter(F.col("__END_AT").isNull()).select(
        "id_empleado", "nombre", "dni", "email", "salario", "sucursal_id", "cargo",
        F.col("__START_AT.fecha_evento").alias("vigente_desde"),
    )


def dni_duplicados(eventos):
    return (eventos.filter(F.col("dni").isNotNull()).groupBy("dni")
            .agg(F.count_distinct("id_empleado").alias("empleados_distintos"),
                 F.array_sort(F.collect_set("id_empleado")).alias("ids_empleado"))
            .filter(F.col("empleados_distintos") > 1))
