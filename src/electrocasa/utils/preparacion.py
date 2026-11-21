from pyspark.sql import Window
from pyspark.sql import functions as F

from src.electrocasa.utils import normalizacion as N
from src.electrocasa.utils.reglas import reglas_de_rechazo

UMBRAL_SALARIO_SIN_PUNTO = 50000

ORIGEN = {
    "ventas": ("venta_id", "sucursal_id", "producto_id", "vendedor_id", "cantidad", "monto_total", "metodo_pago",
               "fecha_venta", "canal"),
    "empleados": ("id_empleado", "nombre", "dni", "email", "salario", "sucursal_id", "cargo", "tipo_evento",
                  "fecha_evento", "fecha_registro"),
    "devoluciones": ("devolucion_id", "venta_id", "sucursal_id", "producto_id", "motivo", "monto_reembolso",
                     "fecha_devolucion"),
    "resenas": ("resena_id", "producto_id", "cliente_id", "calificacion", "comentario", "tags", "respuestas",
                "fecha_resena", "metadata"),
    "catalogo": ("producto_id", "nombre_producto", "categoria", "marca", "precio_lista", "activo"),
    "sucursales": ("sucursal_id", "nombre_sucursal", "distrito", "ciudad", "region", "fecha_apertura"),
    "tracking_envios": ("evento_id", "tracking_id", "venta_id", "courier", "estado_entrega", "sucursal_origen",
                        "fecha_actualizacion", "fecha_modificacion"),
}

SILVER = {
    "ventas": ("venta_id", "sucursal_id", "producto_id", "vendedor_id", "cantidad", "monto_total", "metodo_pago",
               "fecha_venta", "canal", "producto_en_catalogo", "lote", "source_file", "ingestion_timestamp"),
    "devoluciones": ("devolucion_id", "venta_id", "sucursal_id", "producto_id", "motivo", "monto_reembolso",
                     "fecha_devolucion", "monto_venta", "lote", "source_file", "ingestion_timestamp"),
    "resenas": ("resena_id", "producto_id", "cliente_id", "calificacion", "es_negativa", "comentario", "tags",
                "respuestas", "fecha_resena", "metadata", "origen", "producto_en_catalogo", "lote", "source_file",
                "ingestion_timestamp"),
    "tracking_envios": ("evento_id", "tracking_id", "venta_id", "courier", "estado_entrega", "sucursal_origen",
                        "fecha_actualizacion", "fecha_modificacion", "venta_existente"),
    "catalogo_snapshots": ("producto_id", "snapshot_fecha", "nombre_producto", "categoria", "marca", "precio_lista",
                           "precio_lista_origen", "activo", "lote", "source_file", "ingestion_timestamp"),
    "sucursales": ("sucursal_id", "nombre_sucursal", "distrito", "ciudad", "region", "fecha_apertura", "lote",
                   "source_file", "ingestion_timestamp"),
    "empleados_cambios": ("id_empleado", "nombre", "dni", "email", "salario", "salario_corregido", "sucursal_id",
                          "cargo", "tipo_evento", "fecha_evento", "fecha_registro", "lote"),
}


def _auditoria():
    return [F.col("source_file"), F.col("ingestion_timestamp"), F.col("batch_id").alias("lote")]


def _registro(fuente):
    return F.to_json(F.struct(*[F.col(c) for c in ORIGEN[fuente]])).alias("registro")


def con_validez(df, fuente):
    reglas = reglas_de_rechazo(fuente)
    fallidas = F.array(*[F.when(~F.coalesce(F.expr(condicion), F.lit(False)), F.lit(nombre))
                         for nombre, condicion in reglas.items()])
    df = df.withColumn("reglas_fallidas", F.filter(fallidas, lambda regla: regla.isNotNull()))
    return df.withColumn("es_valido", F.size("reglas_fallidas") == 0)


def deduplicar(df, llaves, orden=None):
    llaves = [llaves] if isinstance(llaves, str) else list(llaves)
    criterio = orden or [F.col("es_valido").desc(), F.col("ingestion_timestamp").asc(), F.col("source_file").asc()]
    ventana = Window.partitionBy(*llaves).orderBy(*criterio)
    con_llave = F.lit(True)
    for llave in llaves:
        con_llave = con_llave & F.col(llave).isNotNull()
    unicos = df.filter(con_llave).withColumn("_fila", F.row_number().over(ventana)).filter("_fila = 1").drop("_fila")
    return unicos.unionByName(df.filter(~con_llave))


def marcar_existencia(df, referencia, llave, columna_flag):
    ids = referencia.select(F.col(llave).alias("_ref")).distinct().withColumn("_existe", F.lit(True))
    return (df.join(ids, df[llave] == ids["_ref"], "left")
            .withColumn(columna_flag, F.coalesce(F.col("_existe"), F.lit(False)))
            .drop("_ref", "_existe"))


def preparar_ventas(bronze):
    return bronze.select(
        N.texto("venta_id").alias("venta_id"),
        N.texto("sucursal_id").alias("sucursal_id"),
        N.texto("producto_id").alias("producto_id"),
        N.texto("vendedor_id").alias("vendedor_id"),
        N.entero("cantidad").alias("cantidad"),
        N.monto("monto_total").alias("monto_total"),
        N.estandarizar("metodo_pago", N.METODOS_PAGO).alias("metodo_pago"),
        N.fecha("fecha_venta").alias("fecha_venta"),
        N.estandarizar("canal", N.CANALES).alias("canal"),
        *_auditoria(),
        _registro("ventas"),
    )


def ventas_validadas(bronze, catalogo):
    base = deduplicar(con_validez(preparar_ventas(bronze), "ventas"), "venta_id")
    return marcar_existencia(base, catalogo, "producto_id", "producto_en_catalogo")


def preparar_devoluciones(bronze):
    return bronze.select(
        N.texto("devolucion_id").alias("devolucion_id"),
        N.texto("venta_id").alias("venta_id"),
        N.texto("sucursal_id").alias("sucursal_id"),
        N.texto("producto_id").alias("producto_id"),
        F.coalesce(F.lower(N.texto("motivo")), F.lit("sin_motivo")).alias("motivo"),
        N.monto("monto_reembolso").alias("monto_reembolso"),
        N.fecha("fecha_devolucion").alias("fecha_devolucion"),
        *_auditoria(),
        _registro("devoluciones"),
    )


def devoluciones_validadas(bronze, ventas_silver):
    montos = ventas_silver.select(F.col("venta_id").alias("_venta"), F.col("monto_total").alias("monto_venta"))
    preparadas = preparar_devoluciones(bronze)
    con_venta = (preparadas.join(montos, preparadas["venta_id"] == montos["_venta"], "left").drop("_venta"))
    return deduplicar(con_validez(con_venta, "devoluciones"), "devolucion_id")


def _fecha_anidada(columna):
    intentos = [F.try_to_timestamp(columna, F.lit(formato)) for formato in N.FORMATOS_FECHA]
    return F.coalesce(*intentos)


def preparar_resenas(bronze):
    respuestas = F.transform(
        F.coalesce(F.col("respuestas"), F.array()),
        lambda r: F.struct(
            F.coalesce(F.trim(r.getField("autor")), F.lit("sin_autor")).alias("autor"),
            F.trim(r.getField("texto")).alias("texto"),
            _fecha_anidada(F.trim(r.getField("fecha"))).alias("fecha"),
        ),
    )
    metadata = F.try_parse_json(N.texto("metadata"))
    calificacion = N.entero("calificacion")
    return bronze.select(
        N.texto("resena_id").alias("resena_id"),
        N.texto("producto_id").alias("producto_id"),
        N.texto("cliente_id").alias("cliente_id"),
        calificacion.alias("calificacion"),
        calificacion.isin(1, 2).alias("es_negativa"),
        N.texto("comentario").alias("comentario"),
        F.coalesce(F.col("tags"), F.array().cast("array<string>")).alias("tags"),
        respuestas.alias("respuestas"),
        N.fecha("fecha_resena").alias("fecha_resena"),
        metadata.alias("metadata"),
        F.try_variant_get(metadata, "$.origen", "string").alias("origen"),
        *_auditoria(),
        _registro("resenas"),
    )


def resenas_validadas(bronze, catalogo):
    base = deduplicar(con_validez(preparar_resenas(bronze), "resenas"), "resena_id")
    return marcar_existencia(base, catalogo, "producto_id", "producto_en_catalogo")


def preparar_empleados(bronze):
    salario_origen = N.monto("salario", "decimal(14,2)")
    corregido = F.coalesce(salario_origen > UMBRAL_SALARIO_SIN_PUNTO, F.lit(False))
    salario = F.when(corregido, salario_origen / 100).otherwise(salario_origen).cast("decimal(10,2)")
    return bronze.select(
        N.texto("id_empleado").alias("id_empleado"),
        N.texto("nombre").alias("nombre"),
        N.texto("dni").alias("dni"),
        F.lower(N.texto("email")).alias("email"),
        salario.alias("salario"),
        corregido.alias("salario_corregido"),
        N.texto("sucursal_id").alias("sucursal_id"),
        N.texto("cargo").alias("cargo"),
        N.estandarizar("tipo_evento", N.TIPOS_EVENTO).alias("tipo_evento"),
        N.fecha("fecha_evento").alias("fecha_evento"),
        N.marca_de_tiempo("fecha_registro").alias("fecha_registro"),
        *_auditoria(),
        _registro("empleados"),
    )


def empleados_validados(bronze):
    return con_validez(preparar_empleados(bronze), "empleados")


def preparar_catalogo(bronze):
    precio = N.monto("precio_lista")
    return bronze.select(
        N.texto("producto_id").alias("producto_id"),
        N.fecha_desde_archivo("source_file").alias("snapshot_fecha"),
        N.texto("nombre_producto").alias("nombre_producto"),
        N.estandarizar("categoria", N.CATEGORIAS).alias("categoria"),
        N.texto("marca").alias("marca"),
        F.when(precio > 0, precio).alias("precio_lista"),
        N.texto("precio_lista").alias("precio_lista_origen"),
        F.when(F.lower(N.texto("activo")) == "true", F.lit(True))
        .when(F.lower(N.texto("activo")) == "false", F.lit(False)).alias("activo"),
        *_auditoria(),
        _registro("catalogo"),
    )


def catalogo_snapshots(bronze):
    orden = [F.col("es_valido").desc(), F.col("precio_lista").isNotNull().desc(), F.col("precio_lista").desc(),
             F.col("ingestion_timestamp").asc()]
    validado = con_validez(preparar_catalogo(bronze), "catalogo")
    return deduplicar(validado, ["producto_id", "snapshot_fecha"], orden)


def catalogo_actual(snapshots):
    por_producto = Window.partitionBy("producto_id")
    reciente = Window.partitionBy("producto_id").orderBy(F.col("snapshot_fecha").desc())
    return (snapshots
            .withColumn("primer_snapshot", F.min("snapshot_fecha").over(por_producto))
            .withColumn("_fila", F.row_number().over(reciente))
            .filter("_fila = 1")
            .drop("_fila")
            .withColumnRenamed("snapshot_fecha", "ultimo_snapshot"))


def preparar_sucursales(bronze):
    return bronze.select(
        N.texto("sucursal_id").alias("sucursal_id"),
        N.texto("nombre_sucursal").alias("nombre_sucursal"),
        N.texto("distrito").alias("distrito"),
        F.initcap(F.lower(N.texto("ciudad"))).alias("ciudad"),
        N.texto("region").alias("region"),
        N.fecha("fecha_apertura").alias("fecha_apertura"),
        *_auditoria(),
        _registro("sucursales"),
    )


def sucursales_validadas(bronze):
    orden = [F.col("es_valido").desc(), F.col("ingestion_timestamp").desc()]
    return deduplicar(con_validez(preparar_sucursales(bronze), "sucursales"), "sucursal_id", orden)


def preparar_tracking(bronze):
    return bronze.select(
        F.col("evento_id").cast("bigint").alias("evento_id"),
        N.texto("tracking_id").alias("tracking_id"),
        N.texto("venta_id").alias("venta_id"),
        F.coalesce(N.estandarizar("courier", N.COURIERS), F.initcap(F.lower(N.texto("courier")))).alias("courier"),
        N.estandarizar("estado_entrega", N.ESTADOS_ENTREGA).alias("estado_entrega"),
        N.texto("sucursal_origen").alias("sucursal_origen"),
        F.col("fecha_actualizacion").cast("date").alias("fecha_actualizacion"),
        F.col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
        F.lit("dbo.TrackingEnvios").alias("source_file"),
        F.col("fecha_modificacion").cast("timestamp").alias("ingestion_timestamp"),
        F.lit("conector_tracking").alias("lote"),
        F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in ORIGEN["tracking_envios"]])).alias("registro"),
    )


def tracking_validado(bronze, ventas_silver):
    base = deduplicar(con_validez(preparar_tracking(bronze), "tracking_envios"), "evento_id")
    return marcar_existencia(base, ventas_silver, "venta_id", "venta_existente")


def tracking_sin_reprocesos(validado):
    orden = [F.col("fecha_modificacion").asc(), F.col("evento_id").asc()]
    validos = deduplicar(validado.filter("es_valido"), ["tracking_id", "estado_entrega"], orden)
    return validos.unionByName(validado.filter(~F.col("es_valido")))


def rechazos(validado, fuente, llave):
    return validado.filter(~F.col("es_valido")).select(
        F.lit(fuente).alias("fuente"),
        F.col(llave).cast("string").alias("llave"),
        F.col("reglas_fallidas"),
        F.col("lote"),
        F.col("source_file").alias("archivo"),
        F.col("ingestion_timestamp").alias("fecha_ingesta"),
        F.current_timestamp().alias("fecha_procesamiento"),
        F.col("registro"),
    )
