DESCARTE = {
    "ventas": {
        "venta_id_presente": "venta_id IS NOT NULL",
        "monto_positivo": "monto_total IS NOT NULL AND monto_total > 0",
        "cantidad_positiva": "cantidad IS NOT NULL AND cantidad > 0",
        "sucursal_presente": "sucursal_id IS NOT NULL",
        "fecha_valida": "fecha_venta IS NOT NULL",
        "metodo_pago_conocido": "metodo_pago IS NOT NULL",
        "canal_conocido": "canal IS NOT NULL",
        "vendedor_en_tienda_fisica": "canal IS NULL OR canal <> 'tienda_fisica' OR vendedor_id IS NOT NULL",
    },
    "empleados": {
        "evento_ubicable": "id_empleado IS NOT NULL AND fecha_evento IS NOT NULL",
        "tipo_evento_conocido": "tipo_evento IS NOT NULL",
        "fecha_registro_valida": "fecha_registro IS NOT NULL",
        "sucursal_presente": "sucursal_id IS NOT NULL",
    },
    "resenas": {
        "resena_id_presente": "resena_id IS NOT NULL",
        "calificacion_en_escala": "calificacion IS NOT NULL AND calificacion BETWEEN 1 AND 5",
        "fecha_valida": "fecha_resena IS NOT NULL",
    },
    "devoluciones": {
        "devolucion_id_presente": "devolucion_id IS NOT NULL",
        "venta_existente": "monto_venta IS NOT NULL",
        "reembolso_valido": (
            "monto_venta IS NOT NULL AND monto_reembolso IS NOT NULL "
            "AND monto_reembolso >= 0 AND monto_reembolso <= monto_venta"
        ),
        "fecha_valida": "fecha_devolucion IS NOT NULL",
        "sucursal_presente": "sucursal_id IS NOT NULL",
    },
    "tracking_envios": {
        "evento_id_presente": "evento_id IS NOT NULL",
        "tracking_presente": "tracking_id IS NOT NULL",
        "estado_conocido": "estado_entrega IS NOT NULL",
    },
    "catalogo": {
        "producto_id_presente": "producto_id IS NOT NULL",
    },
}

FALLA = {
    "sucursales": {
        "sucursal_id_presente": "sucursal_id IS NOT NULL",
    },
}

ADVERTENCIA = {
    "ventas": {"producto_en_catalogo": "producto_en_catalogo"},
    "empleados": {"dni_presente": "dni IS NOT NULL", "salario_sin_corregir": "NOT salario_corregido"},
    "resenas": {"producto_en_catalogo": "producto_en_catalogo", "comentario_presente": "comentario IS NOT NULL"},
    "devoluciones": {"motivo_informado": "motivo <> 'sin_motivo'"},
    "tracking_envios": {"venta_existente": "venta_existente", "fecha_actualizacion_presente": "fecha_actualizacion IS NOT NULL"},
    "catalogo": {
        "precio_valido": "precio_lista IS NOT NULL",
        "categoria_conocida": "categoria IS NOT NULL",
        "marca_presente": "marca IS NOT NULL",
    },
    "sucursales": {"region_conocida": "region IN ('Lima', 'Norte', 'Sur', 'Centro')"},
}


def reglas_de_rechazo(fuente):
    return {**DESCARTE.get(fuente, {}), **FALLA.get(fuente, {})}
