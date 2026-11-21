from pyspark.sql import functions as F

FORMATOS_FECHA = ("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "dd/MM/yyyy")

ACENTOS = "áéíóúüÁÉÍÓÚÜ"
SIN_ACENTOS = "aeiouuAEIOUU"

CATEGORIAS = {
    "linea blanca": "Línea blanca",
    "cocina": "Cocina",
    "electronica": "Electrónica",
    "climatizacion": "Climatización",
    "entretenimiento": "Entretenimiento",
}

METODOS_PAGO = {
    "tarjeta": "Tarjeta",
    "tarjeta de credito": "Tarjeta",
    "tarjeta de debito": "Tarjeta",
    "tc": "Tarjeta",
    "td": "Tarjeta",
    "efectivo": "Efectivo",
    "efv": "Efectivo",
    "yape": "Yape",
    "plin": "Plin",
    "transferencia": "Transferencia",
    "trf": "Transferencia",
}

TIPOS_EVENTO = {
    "alta": "alta",
    "baja": "baja",
    "cese": "baja",
    "transferencia": "transferencia",
    "cambio salario": "cambio_salario",
}

ESTADOS_ENTREGA = {
    "pendiente": "pendiente",
    "en transito": "en_transito",
    "en camino": "en_transito",
    "entregado": "entregado",
    "devuelto": "devuelto",
}

CANALES = {"tienda fisica": "tienda_fisica", "ecommerce": "ecommerce"}

COURIERS = {
    "olva": "Olva Courier",
    "olva courier": "Olva Courier",
    "servientrega": "Servientrega",
    "shalom": "Shalom",
}


def texto(columna):
    limpio = F.trim(F.col(columna))
    return F.when(limpio == "", F.lit(None)).otherwise(limpio)


def clave(columna):
    base = F.lower(F.translate(texto(columna), ACENTOS, SIN_ACENTOS))
    return F.trim(F.translate(base, "_-", "  "))


def estandarizar(columna, valores):
    mapa = F.create_map(*[F.lit(v) for par in valores.items() for v in par])
    return mapa[clave(columna)]


def fecha(columna):
    valor = texto(columna)
    intentos = [F.try_to_timestamp(valor, F.lit(formato)) for formato in FORMATOS_FECHA]
    return F.coalesce(*intentos).cast("date")


def marca_de_tiempo(columna):
    valor = texto(columna)
    intentos = [F.try_to_timestamp(valor, F.lit(formato)) for formato in FORMATOS_FECHA]
    return F.coalesce(*intentos)


def numero_texto(columna):
    valor = F.trim(F.replace(texto(columna), F.lit("S/"), F.lit("")))
    tras_ultima_coma = F.substring_index(valor, ",", -1)
    coma_decimal = (F.instr(valor, ",") > 0) & (F.instr(tras_ultima_coma, ".") == 0)
    con_coma_decimal = F.replace(F.replace(valor, F.lit("."), F.lit("")), F.lit(","), F.lit("."))
    sin_separador_miles = F.replace(valor, F.lit(","), F.lit(""))
    return F.when(coma_decimal, con_coma_decimal).otherwise(sin_separador_miles)


def monto(columna, tipo="decimal(12,2)"):
    return numero_texto(columna).try_cast(tipo)


def entero(columna):
    return texto(columna).try_cast("int")


def mes(columna_fecha):
    return F.trunc(F.col(columna_fecha), "month")


def lote_desde_archivo(columna_archivo):
    return F.substring_index(F.col(columna_archivo), ".", 1)


def fecha_desde_archivo(columna_archivo):
    ultimo_tramo = F.substring_index(F.col(columna_archivo), "_", -1)
    return F.try_to_timestamp(F.substring_index(ultimo_tramo, ".", 1), F.lit("yyyy-MM-dd")).cast("date")
