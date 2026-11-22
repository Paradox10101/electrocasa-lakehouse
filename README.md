# ElectroCasa Lakehouse

![Azure Databricks](https://img.shields.io/badge/Azure_Databricks-serverless-FF3621?logo=databricks&logoColor=white)
![Lakeflow](https://img.shields.io/badge/Lakeflow-Declarative_Pipelines-1B3139)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-gobierno-1B3139)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion-00ADD4)
![PySpark](https://img.shields.io/badge/PySpark-Python-E25A1C?logo=apachespark&logoColor=white)
![CI/CD](https://img.shields.io/badge/CI%2FCD-Declarative_Automation_Bundles-2E7D32)

Plataforma de datos de punta a punta sobre Azure Databricks para ElectroCasa, una cadena de 40 tiendas de electrodomésticos en Perú:
- integra seis fuentes heterogéneas en un modelo Medallion con Lakeflow Spark Declarative Pipelines;
- orquesta todo con Lakeflow Jobs;
- lo gobierna con Unity Catalog;
- lo despliega con un Declarative Automation Bundle de dos targets.

Todo corrió de verdad: cada cifra de este README sale de las ejecuciones del 21 de noviembre de 2026.

Los datos, las sucursales y las personas del caso son ficticios.

> **English summary.**
> - **What:** end-to-end lakehouse on serverless Azure Databricks for a fictional 40-store appliance retailer in Peru.
> - **Ingestion:** six heterogeneous sources land in a Medallion architecture built with Lakeflow Spark Declarative Pipelines. CSV and JSON files arrive through Auto Loader and COPY INTO, and an Azure SQL table through a Lakeflow Connect query-based connector.
> - **Silver:** expectations route rejected records to a queryable quarantine, and employee history is kept as SCD Type 2 with AUTO CDC.
> - **Gold:** answers six business questions.
> - **Governance and delivery:** Unity Catalog controls access with groups, GRANT/REVOKE and column masks. Lakeflow Jobs orchestrates the flow, and a Declarative Automation Bundle deploys it to dev and prod.
> - **Evidence:** every figure comes from real runs on November 21, 2026.

![Tramo de Silver y Gold del pipeline, con las insignias de calidad de cada tabla](docs/img/01b_pipeline_silver_y_gold.png)

## En cifras

| | |
|---|---|
| **Fuentes** | Las 6 del enunciado más la tabla de referencia de sucursales, con tres métodos de ingesta: Auto Loader, COPY INTO y el conector de Lakeflow Connect |
| **Tablas** | 7 en Bronze, 11 en Silver y 9 en Gold |
| **Calidad** | 37 reglas declaradas en `src/electrocasa/utils/reglas.py` (24 descartan, 1 detiene la actualización y 12 advierten); 902 registros en `silver.cuarentena`, cada uno con su fuente y su regla |
| **Ventas** | 18.000 distintas, 17.488 válidas y 512 rechazadas, sin duplicados después de reingestar los tres lotes |
| **Historia de Empleados** | 4.049 versiones SCD Type 2, con 63 eventos tardíos ubicados en su lugar |
| **Tracking** | 15.139 eventos en un solo `MERGE`; las corridas siguientes del conector no duplicaron nada |
| **Orquestación** | Job de 4 tareas; los tres lotes en SUCCESS, entre 5 y 6 minutos cada uno |
| **Despliegue** | Bundle con `dev` (5 recursos) y `prod` (10, con permisos) |

## Vistazo rápido

<table>
<tr>
<td width="50%"><a href="docs/img/02_job_orquestacion.png"><img src="docs/img/02_job_orquestacion.png" alt="Job de orquestación con sus cuatro tareas en verde"></a><br><sub>Job: COPY INTO, conector de Tracking, pipeline y validación, en el lote 3</sub></td>
<td width="50%"><a href="docs/img/03_calidad_silver_ventas.png"><img src="docs/img/03_calidad_silver_ventas.png" alt="Expectativas de silver.ventas"></a><br><sub>Calidad: reglas de <code>silver.ventas</code>, 17.488 escritas y 512 descartadas</sub></td>
</tr>
<tr>
<td width="50%"><a href="docs/img/04_linaje_gold.png"><img src="docs/img/04_linaje_gold.png" alt="Linaje de una tabla de Gold"></a><br><sub>Linaje en Unity Catalog: Gold hacia sus tablas de Silver</sub></td>
<td width="50%"><a href="docs/img/05a_revoke.png"><img src="docs/img/05a_revoke.png" alt="SHOW GRANTS antes y después del REVOKE"></a><br><sub>Gobierno: <code>SHOW GRANTS</code> antes y después del <code>REVOKE</code></sub></td>
</tr>
</table>

Las capturas tienen tapados el usuario y el nombre de la cuenta. El detalle de cada una está en la [sección 14](#14-evidencia-de-ejecución).

## Cómo revisar este repositorio

Cada pauta del enunciado y dónde se cumple:

| Pauta del enunciado | Dónde está |
|---|---|
| Repositorio con `databricks.yml`, `resources/`, `src/`, `contratos/`, `notebooks/00_setup.ipynb` y README | Raíz del repositorio ([sección 3](#3-estructura-del-repositorio)) |
| Catálogo propio, schemas por capa y volume de landing | `notebooks/00_setup.ipynb` ([sección 9](#9-gobierno-y-seguridad)) |
| Al menos dos métodos de ingesta de archivos, justificados | Auto Loader en `src/electrocasa/transformations/bronze.py` (por metadata, con `src/electrocasa/config/fuentes_auto_loader.json`) y COPY INTO en `notebooks/01_carga_copy_into.ipynb` ([sección 4](#4-ingesta-método-por-fuente)) |
| Tracking con el conector query-based de SQL Server, cursor `fecha_modificacion` y llave `evento_id` | `resources/electrocasa_tracking.yml` ([sección 4](#4-ingesta-método-por-fuente)); dos corridas en la [sección 14.2](#142-las-dos-corridas-del-conector) |
| Contrato de datos por fuente, alineado con el código | `contratos/` y `src/electrocasa/utils/reglas.py` |
| Expectativas con política justificada y rechazos consultables | `src/electrocasa/utils/reglas.py`, `transformations/silver_*.py` y `silver.cuarentena` ([sección 5](#5-calidad-expectativas-políticas-y-cuarentena)) |
| Empleados en SCD Type 2 con `sequence_by` de la fuente | `src/electrocasa/transformations/silver_empleados.py` ([sección 6](#6-historia-de-empleados-scd-type-2)) |
| Normalización de fechas, montos, categorías, métodos de pago, eventos, estados, couriers y ciudad | `src/electrocasa/utils/normalizacion.py` y `preparacion.py` |
| Gold que responde las seis preguntas | `src/electrocasa/transformations/gold.py` y `utils/gold_logica.py` ([sección 8](#8-gold-una-tabla-por-pregunta)) |
| Job con dependencias, dos tipos de tarea, validación, trigger, reintentos y alertas | `resources/electrocasa_job.yml` y `notebooks/02_validacion.ipynb` ([sección 10](#10-orquestación)) |
| Bundle con dos targets, desplegable desde el repositorio | `databricks.yml` y `resources/` ([sección 11](#11-cicd-con-el-bundle)); comandos para desplegarlo en otro workspace en la [sección 13](#13-despliegue-paso-a-paso) |
| Monitoreo y justificación del compute | `notebooks/90_monitoreo.ipynb` ([sección 12](#12-monitoreo-compute-y-costos)) |
| Grupos, `GRANT`, `REVOKE`, acceso de auditoría y masking de `dni` y `salario` | `notebooks/00_setup.ipynb` y `src/electrocasa/utils/mascaras.py` ([sección 9](#9-gobierno-y-seguridad)) |
| Decisiones justificadas | [Sección 7](#7-decisiones-de-diseño) |
| Evidencia de ejecución | [Sección 14](#14-evidencia-de-ejecución), con capturas del pipeline y del job |
| Retos opcionales | [Sección 15](#15-retos-opcionales) |

Justificaciones que pide el enunciado y dónde están:

| Justificación | Dónde |
|---|---|
| Método de ingesta de cada fuente | [Sección 4](#4-ingesta-método-por-fuente) |
| Política de violación de cada expectativa (las 37 reglas) | [Sección 5](#5-calidad-expectativas-políticas-y-cuarentena) |
| Cómo se implementó el SCD Type 2 y de dónde sale `sequence_by` | [Sección 6](#6-historia-de-empleados-scd-type-2) |
| Huérfanos, dni repetido, salarios fuera de rango, formato de la máscara, modo del conector y descontinuados | [Sección 7](#7-decisiones-de-diseño) |
| Trigger del job, reintentos y alertas | [Sección 10](#10-orquestación) |
| Compute de cada recurso | [Sección 12](#12-monitoreo-compute-y-costos) |
| Tablas de sistema no disponibles: error y alternativa | [Sección 14.4](#144-gobierno) |

## Contenido

1. [Caso y preguntas de negocio](#1-caso-y-preguntas-de-negocio)
2. [Arquitectura](#2-arquitectura)
3. [Estructura del repositorio](#3-estructura-del-repositorio)
4. [Ingesta: método por fuente](#4-ingesta-método-por-fuente)
5. [Calidad: expectativas, políticas y cuarentena](#5-calidad-expectativas-políticas-y-cuarentena)
6. [Historia de Empleados (SCD Type 2)](#6-historia-de-empleados-scd-type-2)
7. [Decisiones de diseño](#7-decisiones-de-diseño)
8. [Gold: una tabla por pregunta](#8-gold-una-tabla-por-pregunta)
9. [Gobierno y seguridad](#9-gobierno-y-seguridad)
10. [Orquestación](#10-orquestación)
11. [CI/CD con el bundle](#11-cicd-con-el-bundle)
12. [Monitoreo, compute y costos](#12-monitoreo-compute-y-costos)
13. [Despliegue paso a paso](#13-despliegue-paso-a-paso)
14. [Evidencia de ejecución](#14-evidencia-de-ejecución)
15. [Retos opcionales](#15-retos-opcionales)
16. [Pruebas locales](#16-pruebas-locales)
17. [Antipatrones evitados](#17-antipatrones-evitados)

## 1. Caso y preguntas de negocio

Cada sucursal exporta sus ventas por separado, RR.HH. lleva sus eventos en su propio sistema, las reseñas se leen a mano y el tracking de despachos vive en la base del courier. La capa Gold responde, para marzo a octubre de 2026 y solo con datos limpios de Silver:

| # | Pregunta | Tabla Gold |
|---|---|---|
| 1 | Ventas, devoluciones, ventas netas, cantidad de ventas y ticket promedio por sucursal y mes | `gold.ventas_netas_sucursal_mes` |
| 2 | Top 10 de productos por unidades y top 10 por tasa de devolución | `gold.top_productos_unidades`, `gold.top_productos_devolucion` |
| 3 | Empleados activos por sucursal al último día de cada mes | `gold.dotacion_sucursal_mes` |
| 4 | Ventas de tienda física por vendedor, sucursal que tenía en la fecha de la venta y mes | `gold.ventas_vendedor_mes` |
| 5 | Porcentaje de reseñas negativas por categoría y mes | `gold.resenas_negativas_categoria_mes` |
| 6 | Horas promedio de pendiente a entregado y porcentaje de devueltos por courier | `gold.despachos_courier` |

## 2. Arquitectura

```mermaid
%%{init: {'flowchart': {'curve': 'basis'}}}%%
flowchart TB
    A("Archivos CSV y JSON<br/>vol_landing por fuente") -->|Auto Loader| B("Bronze streaming<br/>ventas · empleados<br/>devoluciones · reseñas")
    A -->|COPY INTO, tarea del job| C("Bronze Delta<br/>catálogo · sucursales")
    D("Azure SQL<br/>dbo.TrackingEnvios") -->|conector query-based| E("bronze.tracking_envios")
    B --> F("Silver<br/>limpieza, normalización<br/>deduplicación, expectativas")
    C --> F
    E --> F
    F -->|rechazos| G("silver.cuarentena")
    F --> H("Silver empleados_historia<br/>SCD Type 2 con MASK")
    F --> I("Gold<br/>seis preguntas")
    H --> I
    I --> J("electrocasa_analistas<br/>electrocasa_auditoria")
    classDef capa fill:#dbe9f6,stroke:#1b5e9b,color:#000
    classDef rechazo fill:#fbe0dc,stroke:#c0392b,color:#000
    classDef consumo fill:#e3f4e8,stroke:#1f8a4c,color:#000
    class A,B,C,D,E,F,H,I capa
    class G rechazo
    class J consumo
```

| Capa | Tablas | Tipo |
|---|---|---|
| Bronze | `ventas`, `empleados`, `devoluciones`, `resenas` | Streaming Tables con Auto Loader, dentro del pipeline |
| Bronze | `catalogo`, `sucursales` | Tablas Delta cargadas con COPY INTO, fuera del pipeline |
| Bronze | `tracking_envios` | Tabla destino del conector de Lakeflow Connect |
| Silver | `ventas`, `devoluciones`, `resenas`, `tracking_envios`, `catalogo_snapshots`, `catalogo`, `sucursales`, `empleados_eventos`, `dni_duplicados`, `cuarentena` | Materialized Views |
| Silver | `empleados_historia` | Streaming Table destino de AUTO CDC (SCD Type 2) |
| Gold | Las seis preguntas, más `resenas_negativas_origen_mes` y `empleados_vigentes` | Materialized Views |

Bronze no tiene lógica de negocio: guarda todas las columnas como texto y agrega las columnas técnicas `source_file` (`_metadata.file_name`), `ingestion_timestamp`, `source_system` y `batch_id` (el nombre del archivo, que identifica el lote). Las tablas de COPY INTO suman `job_run_id`.

Silver usa Materialized Views porque deduplica entre lotes, cruza fuentes (devoluciones con ventas, reseñas con catálogo) y necesita ver todos los datos para elegir la versión válida de cada llave. El volumen es chico (decenas de miles de filas), así que recalcularlas en cada corrida es barato. La excepción es Empleados, que necesita una fuente de streaming para AUTO CDC.

## 3. Estructura del repositorio

```
electrocasa-lakehouse/
├── databricks.yml                     bundle, variables y targets dev y prod
├── resources/
│   ├── electrocasa_pipeline.yml       pipeline principal (Bronze, Silver y Gold), serverless
│   ├── electrocasa_tracking.yml       conector query-based de SQL Server (Lakeflow Connect)
│   ├── electrocasa_job.yml            job de orquestación con sus cuatro tareas
│   └── electrocasa_utilidades.yml     jobs de setup y de monitoreo, para correrlos por comando
├── src/electrocasa/
│   ├── config/fuentes_auto_loader.json  propiedades de cada fuente de archivos
│   ├── transformations/               código del pipeline: bronze, silver y gold
│   └── utils/                         normalización, reglas, preparación, lógica de Gold y máscaras
├── contratos/                         un contrato de datos por fuente
├── notebooks/
│   ├── 00_setup.ipynb                 catálogo, schemas, volume, máscaras, permisos y REVOKE
│   ├── 01_carga_copy_into.ipynb       tarea del job: COPY INTO de catálogo y sucursales
│   ├── 02_validacion.ipynb            tarea del job: conteos, rechazos y falla si Gold queda vacía
│   ├── 90_monitoreo.ipynb             event log, system.lakeflow y system.billing
│   └── 91_reprocesar_cuarentena.ipynb reproceso de registros rechazados
├── tests/test_normalizacion.py        pruebas locales de la normalización con PySpark
└── README.md
```

La lógica vive en `src/electrocasa/utils/` como funciones de DataFrame a DataFrame, sin decoradores, para poder probarla fuera del pipeline. Los archivos de `transformations/` solo declaran las tablas y llaman a esas funciones. Las reglas de calidad están definidas una sola vez en `utils/reglas.py`: las usan las expectativas de Silver y también la cuarentena, así que las dos no pueden quedar desalineadas.

## 4. Ingesta: método por fuente

| Fuente | Método | Por qué |
|---|---|---|
| Ventas | Auto Loader | La fuente de mayor volumen (18.240 filas en tres lotes). Llega por lotes incrementales y Auto Loader recuerda qué archivos ya leyó, sin reprocesar los anteriores |
| Empleados | Auto Loader | Eventos que se agregan en cada lote; la historia SCD Type 2 con AUTO CDC necesita una fuente de streaming |
| Devoluciones | Auto Loader | Lotes incrementales, igual que ventas |
| Reseñas | Auto Loader | JSON Lines anidado por lotes, leído con schema explícito (`tags` como `array<string>`, `respuestas` como arreglo de structs y `metadata` como texto JSON) |
| Catálogo | COPY INTO | Snapshots completos (un arreglo JSON por archivo): dos archivos en todo el período. No hace falta streaming y COPY INTO es idempotente por archivo. Usa `multiLine` y `primitivesAsString` |
| Sucursales | COPY INTO | Tabla de referencia de 40 filas en un archivo único; se carga una vez y no se duplica si el archivo se vuelve a subir |
| Tracking | Conector query-based de SQL Server | Obligatorio por enunciado: cursor `fecha_modificacion`, llave `evento_id`, modo SCD Type 1 |

- **Auto Loader** se declara dentro del pipeline con `cloudFiles`, y el pipeline gestiona el checkpoint y la ubicación del esquema (no se configuran rutas a mano).
- **Ingesta por metadata:** las propiedades de cada fuente de archivos (carpeta, formato, opciones, schema y sistema de origen) están en `src/electrocasa/config/fuentes_auto_loader.json`. `transformations/bronze.py` lee ese archivo al iniciar y crea una Streaming Table por entrada. Sumar una fuente es agregar un bloque al JSON.
- **CSV** se leen con schema explícito (todo texto en Bronze), nunca con `inferSchema`.
- **COPY INTO** corre en `notebooks/01_carga_copy_into.ipynb`, primera tarea del job, y escribe en `bronze.catalogo` y `bronze.sucursales`. El pipeline las lee como fuente con una lectura batch desde Materialized Views.
- **Tracking:** el conector está declarado en el bundle (`resources/electrocasa_tracking.yml`) con la Connection de Unity Catalog `electrocasa_tracking_sqlserver`, que guarda el usuario y la contraseña. El código no tiene credenciales. Se seleccionan solo las ocho columnas que usa el caso (`include_columns`).
- **Modo SCD Type 1 para Tracking:** cada fila del origen es un evento con su propio `evento_id` y el origen no reescribe eventos. Con `evento_id` como llave, una segunda corrida sin cambios no agrega filas y un evento corregido en el origen se actualiza en vez de duplicarse. No se usa `tracking_id` como llave porque se repite por diseño, y el merge perdería eventos.

## 5. Calidad: expectativas, políticas y cuarentena

- **Dónde están las reglas:** todas viven en `src/electrocasa/utils/reglas.py`, son 37 y las usan el pipeline y la cuarentena.
- **El criterio para elegir la política:**
  - **Descarte:** cuando sin ese dato el registro no se puede ubicar ni sumar en una pregunta de negocio. Va a cuarentena y el resto sigue.
  - **Advertencia:** cuando el registro es un hecho real y sirve aunque le falte algo. Se conserva y la regla mide cuántos casos hay.
  - **Falla:** solo para la tabla de referencia de la que dependen todas las preguntas por sucursal. Detiene la actualización.

| Fuente | Regla | Política | Por qué |
|---|---|---|---|
| Ventas | `venta_id_presente` | Descarte | Es la llave para deduplicar las reingestas: sin ella la venta no se puede contar una sola vez |
| Ventas | `monto_positivo` (después de normalizar el decimal) | Descarte | El monto es la base de las preguntas 1 y 4. Una venta con monto nulo, cero o negativo distorsiona las ventas, pero no justifica detener el pipeline |
| Ventas | `cantidad_positiva` | Descarte | Es la medida de la pregunta 2 (unidades por producto) |
| Ventas | `sucursal_presente` | Descarte | Sin sucursal la venta no entra en la pregunta 1 |
| Ventas | `fecha_valida` | Descarte | Sin fecha no hay mes: no entra en las preguntas 1, 2 y 4 |
| Ventas | `metodo_pago_conocido` | Descarte | Después de estandarizar, un método fuera del conjunto cerrado indica un registro corrupto |
| Ventas | `canal_conocido` | Descarte | El canal decide si la venta entra en la pregunta 4, que es solo de tienda física |
| Ventas | `vendedor_en_tienda_fisica` | Descarte | Una venta de tienda sin vendedor no se puede asignar en la pregunta 4 |
| Ventas | `producto_en_catalogo` | Advertencia | La venta ocurrió: excluirla subestimaría las ventas. Queda marcada con `producto_en_catalogo = false` |
| Catálogo | `producto_id_presente` | Descarte | Sin llave el producto no se cruza con ventas, devoluciones ni reseñas |
| Catálogo | `precio_valido` | Advertencia | El precio no interviene en ninguna pregunta, y descartar el producto dejaría huérfanas sus ventas y reseñas. Queda en nulo y el texto original se conserva |
| Catálogo | `categoria_conocida` | Advertencia | Descartar el producto dejaría huérfanas sus ventas. La regla mide cuántos productos quedarían sin categoría en la pregunta 5 |
| Catálogo | `marca_presente` | Advertencia | La marca no interviene en ninguna pregunta |
| Empleados | `evento_ubicable` (`id_empleado` y `fecha_evento` no nulos) | Descarte | Sin ellos el evento no se puede ubicar en la historia. El `NOT NULL` del schema no frena una llave nula: la frena esta regla |
| Empleados | `tipo_evento_conocido` | Descarte | Sin tipo no se sabe si el evento abre, cambia o cierra una versión |
| Empleados | `fecha_registro_valida` | Descarte | Desempata dos eventos del mismo día en `sequence_by` |
| Empleados | `sucursal_presente` | Descarte | Las preguntas 3 y 4 cuentan por sucursal |
| Empleados | `dni_presente` | Advertencia | La historia conserva el dni anterior (`ignore_null_updates_column_list`). Perder el evento sería peor |
| Empleados | `salario_sin_corregir` | Advertencia | Mide los salarios corregidos (ver sección 7) sin perder eventos válidos |
| Reseñas | `resena_id_presente` | Descarte | Es la llave para deduplicar las reingestas |
| Reseñas | `calificacion_en_escala` | Descarte | El enunciado excluye las calificaciones inválidas de la pregunta 5 |
| Reseñas | `fecha_valida` | Descarte | Sin fecha no hay mes para la pregunta 5 |
| Reseñas | `producto_en_catalogo` | Advertencia | La reseña se conserva, pero no entra en la pregunta 5 (el join con el catálogo la deja fuera), como pide el enunciado |
| Reseñas | `comentario_presente` | Advertencia | El comentario no interviene en ninguna pregunta |
| Devoluciones | `devolucion_id_presente` | Descarte | Es la llave para deduplicar las reingestas |
| Devoluciones | `venta_existente` | Descarte | Sin la venta de origen no se puede validar el reembolso |
| Devoluciones | `reembolso_valido` (`0 <= monto_reembolso <= monto_venta`) | Descarte | Un reembolso imposible altera las ventas netas. Se evalúa sobre el join con `silver.ventas`, porque una expectativa mira una fila a la vez |
| Devoluciones | `fecha_valida` | Descarte | Sin fecha no hay mes en la pregunta 1 |
| Devoluciones | `sucursal_presente` | Descarte | Sin sucursal no entra en la pregunta 1 |
| Devoluciones | `motivo_informado` | Advertencia | El motivo no interviene en ninguna pregunta |
| Tracking | `evento_id_presente` | Descarte | Es la llave primaria del conector |
| Tracking | `tracking_presente` | Descarte | Sin envío el evento no se agrupa en la pregunta 6 |
| Tracking | `estado_conocido` | Descarte | Un estado fuera de `pendiente`, `en_transito`, `entregado` y `devuelto` no se puede ubicar en el ciclo del envío |
| Tracking | `venta_existente` | Advertencia | El envío ocurrió aunque la venta no esté en los archivos (415 eventos). Se conserva marcado |
| Tracking | `fecha_actualizacion_presente` | Advertencia | La pregunta 6 mide con `fecha_modificacion`, no con `fecha_actualizacion` (285 nulos) |
| Sucursales | `sucursal_id_presente` | Falla | Es la referencia de todas las preguntas por sucursal: si llega rota, se detiene la actualización antes de publicar Gold |
| Sucursales | `region_conocida` | Advertencia | La región no interviene en las preguntas; la regla avisa si aparece una nueva |

**Cuarentena consultable y trazable:** `silver.cuarentena` aplica las mismas reglas de descarte (de `utils/reglas.py`) y guarda cada registro rechazado con:

- la fuente;
- la llave;
- la lista de reglas que fallaron;
- el lote y el archivo;
- la fecha de ingesta y la de procesamiento;
- el registro original completo en JSON.

```sql
SELECT fuente, regla, count(*) AS registros
FROM electrocasa_dev.silver.cuarentena
LATERAL VIEW explode(reglas_fallidas) AS regla
GROUP BY fuente, regla
ORDER BY fuente, regla;
```

**Reproceso:** `notebooks/91_reprocesar_cuarentena.ipynb` toma los registros de una fuente, les aplica las correcciones indicadas y los escribe como un archivo nuevo en la carpeta de landing. Silver prefiere la versión válida de cada llave, así que la corrección reemplaza al registro rechazado en la siguiente corrida.

**Reingestas:** Silver deduplica por llave (`venta_id`, `devolucion_id`, `resena_id`), prefiriendo la versión válida y, entre iguales, la primera ingestada. Las filas repetidas entre lotes no duplican nada, y el producto repetido del snapshot de marzo queda una sola vez por snapshot.

## 6. Historia de Empleados (SCD Type 2)

- **Implementación:** `create_auto_cdc_flow` con `stored_as_scd_type=2`, desde una vista temporal en streaming sobre `bronze.empleados`.
- **Orden de los cambios:** `sequence_by = struct(fecha_evento, fecha_registro)`. Ordena por la fecha del evento y, si dos eventos caen el mismo día, por el momento en que RR.HH. los registró. Nunca usa `current_timestamp()`, que daría el mismo valor a todo un lote.
- **Llegada tardía:** AUTO CDC ordena por `sequence_by` y no por orden de llegada. Los 63 eventos de junio a agosto que llegan en el lote 3 se insertan en su lugar y ajustan las vigencias vecinas.
- **Bajas:** `baja` y `cese` se aplican como DELETE (`apply_as_deletes`) y cierran la versión vigente. Un reingreso posterior abre una versión nueva.
- **dni faltante:** si un evento llega sin dni, la historia conserva el anterior (`ignore_null_updates_column_list=["dni"]`).
- **Llave nula:** `id_empleado` es `NOT NULL` en el schema, pero eso no frena una llave nula; la frena la expectativa `evento_ubicable` con descarte.
- **Vigencia:** cada versión vale desde `__START_AT.fecha_evento` (inclusive) hasta `__END_AT.fecha_evento` (exclusive). Un empleado transferido el día *d* cuenta en la sucursal nueva ese día, y uno dado de baja el día *d* ya no cuenta ese día.

## 7. Decisiones de diseño

| Decisión | Elección y motivo |
|---|---|
| Referencias huérfanas | Ventas y reseñas con producto fuera del catálogo se conservan con `producto_en_catalogo = false` (advertencia): son hechos reales. Las reseñas huérfanas no entran en la pregunta 5, como pide el enunciado. Devoluciones sin venta o con venta inexistente van a cuarentena, porque su monto no se puede validar. Eventos de tracking con venta inexistente se conservan con `venta_existente = false`: el envío ocurrió |
| Mismo `dni` en dos empleados | No hay forma de saber cuál es el correcto: se conservan los dos y se listan en `silver.dni_duplicados` para revisión de RR.HH. |
| Salarios fuera de rango | Los 18 casos están entre 136.702 y 310.523, sin decimales, mientras el resto está entre 1.200 y 10.400 con dos decimales (por ejemplo, EMP-00234 tiene 246616.0 en el alta y 2552.86 en el evento siguiente). Se interpretan como un salario sin punto decimal: se dividen entre 100 y quedan marcados con `salario_corregido = true` |
| Formato de la máscara | `dni`: `*****` más los tres últimos dígitos. `salario`: nulo, porque la columna es numérica y no admite asteriscos |
| Conector de Tracking | SCD Type 1 (ver sección 4) |
| Descontinuados del catálogo | Se conservan marcados con `activo = false`: sus ventas y reseñas históricas siguen siendo válidas |
| Política de cada expectativa | Ver sección 5 |
| Auto Loader o COPY INTO | Ver sección 4 |
| Mensajes reprocesados de Tracking | Dentro de cada `tracking_id`, un estado repetido se queda con el primero (menor `fecha_modificacion`). La pregunta 6 usa el primer pendiente, el primer entregado y la existencia de un devuelto, así que la deduplicación no altera ninguna medida |
| Alta sin fecha | Un evento sin `fecha_evento` va a cuarentena. Si es el alta, el empleado no cuenta en la dotación, porque la definición exige un alta con fecha |
| Couriers | El origen escribe el mismo courier de varias formas, incluido Olva como «Olva» y como «Olva Courier». Se mapean a un conjunto cerrado (Olva Courier, Servientrega, Shalom); un courier nuevo queda con initcap hasta agregarlo al mapeo |
| Productos repetidos en marzo | Se conserva el registro con precio válido y, entre dos válidos, el de mayor precio, para que la regla sea determinista |

## 8. Gold: una tabla por pregunta

Todas filtran marzo a octubre de 2026 y leen solo Silver.

- **Pregunta 1:** suma de `monto_total` por `sucursal_id` y mes de `fecha_venta`; suma de `monto_reembolso` por `sucursal_id` y mes de `fecha_devolucion`; full join entre las dos; ventas netas, `count(DISTINCT venta_id)` y ticket promedio (ventas sin restar devoluciones entre cantidad de ventas).
- **Pregunta 2:** suma de `cantidad` por producto, y tasa de devolución como devoluciones del producto entre sus ventas. Los empates se rompen por cantidad de devoluciones y luego por `producto_id`.
- **Pregunta 3:** para cada último día de mes se cuentan las versiones vigentes (`desde <= fecha < hasta`) de empleados con un alta en esa fecha o antes. Las sucursales sin empleados aparecen con 0.
- **Pregunta 4:** cada venta de tienda física se cruza con la versión del vendedor vigente en `fecha_venta`. Sin versión vigente, la venta va a `SIN_ASIGNAR`.
- **Pregunta 5:** reseñas válidas cruzadas con `silver.catalogo` (categoría del último snapshot en que aparece el producto). El inner join deja fuera las reseñas con producto inexistente.
- **Pregunta 6:** por envío, primer `pendiente`, primer `entregado` y si hubo `devuelto`. Por courier, promedio de horas entre los dos primeros (solo envíos entregados) y porcentaje de devueltos sobre envíos finalizados.

Consultas de verificación:

```sql
SELECT * FROM electrocasa_dev.gold.ventas_netas_sucursal_mes ORDER BY sucursal_id, mes;
SELECT * FROM electrocasa_dev.gold.top_productos_unidades ORDER BY puesto;
SELECT * FROM electrocasa_dev.gold.top_productos_devolucion ORDER BY puesto;
SELECT fecha_corte, sum(empleados_activos) FROM electrocasa_dev.gold.dotacion_sucursal_mes GROUP BY fecha_corte ORDER BY fecha_corte;
SELECT * FROM electrocasa_dev.gold.ventas_vendedor_mes WHERE sucursal_id = 'SIN_ASIGNAR';
SELECT * FROM electrocasa_dev.gold.resenas_negativas_categoria_mes ORDER BY categoria, mes;
SELECT * FROM electrocasa_dev.gold.despachos_courier ORDER BY courier;
```

## 9. Gobierno y seguridad

- **Tablas managed** en un catálogo propio por entorno (`electrocasa_dev` y `electrocasa_prod`), con schemas `bronze`, `silver` y `gold`. El volume de landing es `default.vol_landing`, con una carpeta por fuente.
- **Grupos**, creados desde **Settings > Identity and access > Groups**:

  | Grupo | Permisos |
  |---|---|
  | `electrocasa_ingenieria` | `USE CATALOG`; `USE SCHEMA`, `SELECT`, `MODIFY`, `CREATE TABLE` y `CREATE MATERIALIZED VIEW` sobre bronze, silver y gold; `READ VOLUME` y `WRITE VOLUME` sobre el landing |
  | `electrocasa_analistas` | `USE CATALOG`; `USE SCHEMA` y `SELECT` sobre gold |
  | `electrocasa_auditoria` | `USE CATALOG`; `USE SCHEMA` y `SELECT` sobre gold; intento de acceso a `system.access` (ver evidencia) |

- **REVOKE:** `00_setup.ipynb` otorga `SELECT` sobre silver a `electrocasa_analistas`, muestra `SHOW GRANTS`, lo revoca y vuelve a mostrar `SHOW GRANTS`.
- **Masking:** `silver.mascara_dni` y `silver.mascara_salario` usan `is_account_group_member('electrocasa_ingenieria')`. La máscara se declara en el schema de cada tabla del pipeline que tiene esas columnas (cláusula `MASK`), no con un `ALTER TABLE` posterior:
  - `silver.empleados_historia`;
  - `silver.empleados_eventos`;
  - `silver.dni_duplicados`;
  - `gold.empleados_vigentes`.
- **Dueño del pipeline:** el refresh corre con los permisos del dueño del pipeline, que es miembro de `electrocasa_ingenieria`. Por eso `gold.empleados_vigentes`, derivada de la historia, guarda los valores reales, y su propia máscara los vuelve a ocultar para quien no es del grupo (ver evidencia).

## 10. Orquestación

```mermaid
%%{init: {'flowchart': {'curve': 'basis'}}}%%
flowchart LR
    A("1. carga_copy_into<br/>notebook") --> B("2. ingesta_tracking<br/>pipeline del conector")
    B --> C("3. pipeline_principal<br/>Bronze, Silver, Gold")
    C --> D("4. validacion<br/>notebook")
    classDef tarea fill:#dbe9f6,stroke:#1b5e9b,color:#000
    class A,B,C,D tarea
```

- **Tareas:** cuatro, de dos tipos (notebook y pipeline), con dependencias reales. El conector y el pipeline principal van en secuencia, no en paralelo.
- **Validación:**
  - muestra los conteos de Bronze, Silver y Gold;
  - muestra los rechazados por fuente;
  - guarda todo en `default.conteos_por_corrida`;
  - falla si alguna tabla de Gold queda vacía.
- **Disparo:** trigger por llegada de archivo sobre `/Volumes/<catalogo>/default/vol_landing/`.
  - Los lotes llegan cuando el origen los deja, no en horario fijo, así que un horario correría vacío o tarde.
  - `wait_after_last_change_seconds: 180` espera a que termine de subirse todo el lote antes de disparar.
  - `min_time_between_triggers_seconds: 300` evita corridas seguidas.
  - En el target `dev` el modo desarrollo pausa el trigger, y el job se corre a mano.
- **Reintentos:**
  - COPY INTO: 2 reintentos con 1 minuto entre intentos.
  - Conector: 2 reintentos con 2 minutos.
  - Pipeline principal: 1 reintento con 2 minutos.
  - Validación: sin reintentos, porque si falla, falla por los datos.
- **Timeouts:** de 30 a 60 minutos por tarea.
- **Alertas:**
  - correo al fallar el job;
  - aviso si una corrida supera una hora;
  - correo de los pipelines ante fallas de actualización o de flujo.
  - El correo se pasa como variable al desplegar (`correo_alertas`) para no versionarlo. En el despliegue del 21 de noviembre se usó el correo real del usuario del workspace, una cuenta `@hotmail.com` (la primera parte se omite por privacidad), leído con `databricks current-user me`.

## 11. CI/CD con el bundle

- `databricks.yml` en la raíz con el motor de despliegue `direct` (lo pide el conector declarado en el bundle) y los recursos en `resources/`.
- **Targets:**
  - `dev` (modo development, catálogo `electrocasa_dev`);
  - `prod` (modo production, catálogo `electrocasa_prod`, `root_path` explícito y permisos `CAN_MANAGE` para quien despliega).
- **Variables sin valor por defecto:** `correo_alertas` y `bd_tracking`. Se pasan al desplegar, así no quedan en el repositorio.

## 12. Monitoreo, compute y costos

- **Monitoreo:** `notebooks/90_monitoreo.ipynb` consulta:
  - el event log del pipeline (`event_log(TABLE(...))`): avance de cada flujo y métricas de cada expectativa, con registros que pasan y que fallan;
  - el historial de corridas en `system.lakeflow.job_run_timeline`.

  Si una tabla de sistema no está disponible, la celda imprime el error y la alternativa es el historial de corridas del job en la interfaz.
- **Compute:**

  | Recurso | Compute | Motivo |
  |---|---|---|
  | Pipeline principal | Serverless, modo triggered | Corre una vez por lote y procesa poco volumen: no conviene mantener un cluster encendido ni esperar su arranque; nunca en modo continuo |
  | Conector de Tracking | Serverless (lo que despliega Lakeflow Connect para los conectores query-based) | Corre en secuencia antes del pipeline principal |
  | Notebooks del job | Serverless para jobs | Tareas cortas y esporádicas; sin clusters all-purpose en el job productivo |

- **Costos:** la última sección de `90_monitoreo.ipynb` suma `system.billing.usage` por recurso del proyecto. Para identificar los recursos usa `system.lakeflow`, así que en este workspace falló con el mismo error de permisos de la [sección 14.4](#144-gobierno). Por eso los costos medidos no se cuentan como reto logrado.

## 13. Despliegue paso a paso

**Qué despliega el bundle:**
- `databricks.yml` declara los targets `dev` y `prod` y sus variables;
- `resources/` declara el pipeline Medallion, el conector de Tracking, el job de orquestación y los jobs de setup y monitoreo.

**Dónde se probó:** el 21 de noviembre de 2026, con estos mismos archivos, en un workspace serverless de Azure Databricks. El resultado está en la [sección 14](#14-evidencia-de-ejecución). No usa clusters clásicos: todo corre en serverless.

**Requisitos:**

- El CLI de Databricks, autenticado contra el workspace (por ejemplo, con `databricks auth login --host <url del workspace>`). Se usó la versión 1.27.0.
- Los grupos `electrocasa_ingenieria`, `electrocasa_analistas` y `electrocasa_auditoria`, creados en **Settings > Identity and access > Groups**.
  - Quien despliega tiene que ser miembro de `electrocasa_ingenieria`.
  - El pipeline corre con sus permisos, y así las tablas derivadas de Empleados guardan los valores reales (ver sección 9).
- La carpeta `datos/` del material del enunciado. No está en el repositorio.
- El host, la base de datos, el usuario y la contraseña de la Azure SQL de Tracking, que comparte el docente.

Los comandos son de Bash: Azure Cloud Shell, Git Bash, Linux o macOS. En PowerShell, las variables se definen con `$env:NOMBRE = "valor"`.

**1. Connection a Tracking (una sola vez).**
- **Por la interfaz:** en Catalog Explorer, una Connection de tipo SQL Server llamada `electrocasa_tracking_sqlserver`.
- **Por el CLI:** con este bloque, que pide los datos por teclado. La contraseña no se muestra ni queda en ningún archivo:

```bash
read -p "Servidor de Tracking: " SRV; read -p "Usuario: " USR; read -s -p "Contraseña: " CLAVE; echo
SRV="$SRV" USR="$USR" CLAVE="$CLAVE" python3 -c 'import json,os; print(json.dumps({"name":"electrocasa_tracking_sqlserver","connection_type":"SQLSERVER","options":{"host":os.environ["SRV"],"port":"1433","user":os.environ["USR"],"password":os.environ["CLAVE"]}}))' > /tmp/conexion.json
databricks connections create --json @/tmp/conexion.json > /dev/null && echo "Conexión creada"
rm -f /tmp/conexion.json; unset CLAVE
```

**2. Catálogo, schemas y volume de landing.** Van antes del despliegue, porque el pipeline y el conector apuntan al catálogo:

```bash
databricks catalogs create electrocasa_dev
for s in bronze silver gold; do databricks schemas create $s electrocasa_dev; done
databricks volumes create electrocasa_dev default vol_landing MANAGED
```

Si el primer comando responde `Metastore storage root URL does not exist`, el catálogo se crea en Catalog Explorer con **Create catalog** y almacenamiento **Default storage**, y después se corren las otras dos líneas. Así pasó en el workspace serverless de esta ejecución: ahí el almacenamiento por defecto solo se elige desde la interfaz.

**3. Despliegue y setup.**
- **Variables:** el correo de alertas y la base de Tracking se pasan como variables, para no versionarlos.
- **El setup:** `00_setup.ipynb` crea lo que falte del paso 2, las funciones de máscara y los permisos de los tres grupos, y hace la demostración del `REVOKE`.

```bash
export BUNDLE_VAR_correo_alertas="<correo real que recibe las alertas>"
export BUNDLE_VAR_bd_tracking="<base de datos de Tracking>"
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev electrocasa_setup
```

**4. Los tres lotes.**
- **Qué se sube:** cada archivo va a la carpeta de su fuente, dentro de `/Volumes/electrocasa_dev/default/vol_landing/`.
- **Después de cada lote:** se corre el job. En `dev` el modo desarrollo pausa el trigger por llegada de archivo, así que el job se corre a mano.

| Lote | Archivos de `datos/` | Carpeta del volume |
|---|---|---|
| 1 | `ventas/ventas_lote_01_2026-05-31.csv` | `ventas/` |
| 1 | `empleados/empleados_lote_01_2026-05-31.csv` | `empleados/` |
| 1 | `devoluciones/devoluciones_lote_01_2026-05-31.csv` | `devoluciones/` |
| 1 | `resenas/resenas_lote_01_2026-07-31.json` | `resenas/` |
| 1 | `catalogo/catalogo_productos_2026-03-01.json` | `catalogo/` |
| 1 | `sucursales.csv` | `sucursales/` |
| 2 | `ventas/ventas_lote_02_2026-08-31.csv`, `empleados/empleados_lote_02_2026-08-31.csv`, `devoluciones/devoluciones_lote_02_2026-08-31.csv`, `resenas/resenas_lote_02_2026-10-31.json` y `catalogo/catalogo_productos_2026-08-01.json` | La de su fuente |
| 3 | `ventas/ventas_lote_03_2026-10-31.csv`, `empleados/empleados_lote_03_2026-10-31.csv` y `devoluciones/devoluciones_lote_03_2026-10-31.csv` | La de su fuente |

Se pueden subir desde Catalog Explorer (**Upload to this volume**) o con este bloque, cambiando `D` por la ruta de la carpeta `datos/`:

```bash
V=dbfs:/Volumes/electrocasa_dev/default/vol_landing; D=<ruta de la carpeta datos>
subir() { for f in "$@"; do c=$(dirname "$f"); [ "$c" = "." ] && c=sucursales; databricks fs cp "$D/$f" "$V/$c/$(basename "$f")" --overwrite || return 1; done; }
subir ventas/ventas_lote_01_2026-05-31.csv empleados/empleados_lote_01_2026-05-31.csv devoluciones/devoluciones_lote_01_2026-05-31.csv resenas/resenas_lote_01_2026-07-31.json catalogo/catalogo_productos_2026-03-01.json sucursales.csv && databricks bundle run -t dev electrocasa_job
subir ventas/ventas_lote_02_2026-08-31.csv empleados/empleados_lote_02_2026-08-31.csv devoluciones/devoluciones_lote_02_2026-08-31.csv resenas/resenas_lote_02_2026-10-31.json catalogo/catalogo_productos_2026-08-01.json && databricks bundle run -t dev electrocasa_job
subir ventas/ventas_lote_03_2026-10-31.csv empleados/empleados_lote_03_2026-10-31.csv devoluciones/devoluciones_lote_03_2026-10-31.csv && databricks bundle run -t dev electrocasa_job
```

**5. Cierre.**
- `electrocasa_tracking` hace la segunda corrida del conector, que no tiene que traer filas.
- `electrocasa_monitoreo` saca el event log, la historia del conector, los conteos por lote y las respuestas de Gold.

```bash
databricks bundle run -t dev electrocasa_tracking
databricks bundle run -t dev electrocasa_monitoreo
```

**6. Producción.**
- **Pasos:** se repiten los pasos 2 y 3 con `electrocasa_prod` y `-t prod`.
- **Diferencias con dev:**
  - el target `prod` corre en modo production;
  - tiene `root_path` propio y permiso `CAN_MANAGE` para quien despliega;
  - deja activo el trigger por llegada de archivo.

**Resultado esperado.** Con los mismos datos, las cifras deben coincidir con la [sección 14.1](#141-conteos-después-de-cada-lote):
- después del lote 3, `silver.ventas` tiene 17.488 filas y `silver.cuarentena` 902;
- la segunda corrida del conector no agrega versiones a `bronze.tracking_envios`;
- la tarea `validacion` falla si alguna tabla de Gold queda vacía, así que una corrida en verde ya confirma que Gold se llenó.

**Borrar lo desplegado:**

```bash
databricks bundle destroy -t dev --auto-approve
databricks bundle destroy -t prod --auto-approve
databricks catalogs delete electrocasa_dev --force
databricks catalogs delete electrocasa_prod --force
databricks connections delete electrocasa_tracking_sqlserver
```

`bundle destroy` exige que las variables del paso 3 estén definidas; para borrar sirve cualquier valor. Borra los jobs y los pipelines con sus tablas.

## 14. Evidencia de ejecución

Todo corrió el 21 de noviembre de 2026 en un workspace serverless de Azure Databricks (West US 2), con los tres lotes en orden. En las capturas están tapados el usuario y el nombre de la cuenta.

**Pipeline `electrocasa_medallion_dev`:**

- **Corrida:** actualización completa (Refresh all) del 21 de noviembre, de 19:26 a 19:28 UTC, hecha después de estandarizar los couriers. Todos los nodos terminaron en verde.
- **Insignias de Silver:** en los cuadros de Silver, la gris es la cantidad de registros descartados por las expectativas y la amarilla la de registros con advertencia.
- **Ejemplo:** `ventas` descarta 512 y advierte 68. Los 512 coinciden con las ventas rechazadas de la [sección 14.1](#141-conteos-después-de-cada-lote).

![Grafo completo del pipeline, de las fuentes a Gold](docs/img/01_pipeline_grafo_completo.png)

<details>
<summary>El mismo grafo por tramos, para leer los nombres</summary>

Tramo de Empleados: Auto Loader, la vista de cambios, la historia SCD Type 2 y las tablas de Gold de dotación y vendedores. Abajo, `cuarentena` con 902 registros.

![Tramo de Empleados, sucursales y cuarentena](docs/img/01a_pipeline_empleados_y_cuarentena.png)

Tramo de reseñas, ventas, devoluciones y tracking: Silver con sus insignias de expectativas y Gold de couriers, ventas netas y reseñas negativas.

![Tramo de Silver y Gold](docs/img/01b_pipeline_silver_y_gold.png)

Tramo del catálogo: los snapshots, la versión vigente y los dos top 10 de productos.

![Tramo del catálogo y los top de productos](docs/img/01c_pipeline_catalogo_y_productos.png)

</details>

### 14.1 Conteos después de cada lote

Salen de `default.conteos_por_corrida`, que llena la tarea `validacion` en cada corrida del job.

| Tabla | Lote 1 | Lote 2 | Lote 3 |
|---|---|---|---|
| bronze.ventas | 6.783 | 13.622 | 18.240 |
| silver.ventas | 6.604 | 13.118 | 17.488 |
| bronze.empleados | 2.257 | 3.524 | 4.448 |
| silver.empleados_eventos | 2.239 | 3.495 | 4.410 |
| silver.empleados_historia (versiones SCD Type 2) | 2.121 | 3.242 | 4.049 |
| bronze.resenas | 5.292 | 9.280 | 9.280 |
| silver.resenas | 5.226 | 9.097 | 9.097 |
| bronze.devoluciones | 1.387 | 3.091 | 4.217 |
| silver.devoluciones | 1.301 | 2.877 | 3.908 |
| bronze.catalogo | 2.305 | 4.705 | 4.705 |
| silver.catalogo_snapshots | 2.280 | 4.680 | 4.680 |
| silver.catalogo | 2.280 | 2.400 | 2.400 |
| bronze.sucursales / silver.sucursales | 40 / 40 | 40 / 40 | 40 / 40 |
| bronze.tracking_envios / silver.tracking_envios | 15.139 / 15.049 | 15.139 / 15.049 | 15.139 / 15.049 |
| silver.cuarentena | 349 | 700 | 902 |

Lo que demuestran los conteos:

- **Las reingestas no duplican.**
  - Ventas: Bronze acumula las 18.240 filas, incluidas las 240 repetidas entre lotes. Silver tiene 17.488 más 512 rechazadas, que son exactamente los 18.000 `venta_id` distintos.
  - Reseñas: 9.097 más 103 rechazadas, o sea las 9.200 distintas.
  - Devoluciones: 3.908 más 249 rechazadas, o sea las 4.157 distintas.
- **Catálogo:** el producto repetido del snapshot de marzo queda una sola vez (2.305 filas, 2.280 productos). `silver.catalogo` sube a 2.400 con los 120 productos nuevos de agosto.
- **Empleados:** los 38 eventos sin fecha van a cuarentena. Los 63 eventos tardíos del lote 3 se ubican en su lugar de la historia, que crece de 3.242 a 4.049 versiones.
- **Tracking:** el conector trae los 15.139 eventos una sola vez. Silver deja 15.049 después de quitar 90 mensajes reprocesados.

Rechazos en `silver.cuarentena` al cierre del lote 3:

| Fuente | Regla | Registros |
|---|---|---|
| ventas | monto_positivo | 347 |
| ventas | sucursal_presente | 105 |
| ventas | cantidad_positiva | 66 |
| devoluciones | reembolso_valido | 249 |
| devoluciones | venta_existente | 183 |
| resenas | calificacion_en_escala | 103 |
| empleados | evento_ubicable | 38 |

Un registro puede fallar más de una regla: por ejemplo, una devolución sin venta falla `venta_existente` y también `reembolso_valido`.

**Las mismas cifras en el pipeline, en la actualización del lote 3:**

- **Totales:** el panel de `silver.ventas` muestra 17.488 registros escritos (97,2%) y 512 descartados (2,8%).
- **Reglas que fallaron:** las que descartan registros dan 347, 105 y 66, igual que la tabla anterior. `producto_en_catalogo` tiene la acción ALLOW, es decir, advierte sin descartar.
- **Reglas que pasaron:** las otras cinco no tuvieron fallas (5 met, 4 unmet).

![Expectativas de silver.ventas en la actualización del lote 3](docs/img/03_calidad_silver_ventas.png)

### 14.2 Las dos corridas del conector

`DESCRIBE HISTORY electrocasa_dev.bronze.tracking_envios`:

| Versión | Momento (UTC) | Operación | Métricas |
|---|---|---|---|
| 2 | 2026-11-21 18:41:46 | MERGE | `numSourceRows` 15.139, `numTargetRowsInserted` 15.139, `numTargetRowsUpdated` 0 |
| 1 | 2026-11-21 18:41:26 | DLT SETUP | |
| 0 | 2026-11-21 18:41:24 | CREATE TABLE | |

La primera corrida (dentro del job del lote 1) hizo el único `MERGE`, con los 15.139 eventos. Después el conector corrió tres veces más sin agregar ninguna versión:

- dentro del job de los lotes 2 y 3;
- una vez por separado, a las 19:09 UTC, con la actualización terminada en `COMPLETED`.

La ingesta es incremental por el cursor `fecha_modificacion`.

### 14.3 Job y bundle

| Corrida del job `electrocasa_orquestacion_dev` | Inicio | Fin | Resultado |
|---|---|---|---|
| Lote 1 | 18:40:02 | 18:45:26 | SUCCESS |
| Lote 2 | 18:54:08 | 19:00:05 | SUCCESS |
| Lote 3 | 19:02:48 | 19:08:23 | SUCCESS |

Cada corrida ejecutó las cuatro tareas en orden: `carga_copy_into`, `ingesta_tracking`, `pipeline_principal` y `validacion`. La validación no falló en ninguna, así que ninguna tabla de Gold quedó vacía.

![Corrida del lote 3 del job, con las cuatro tareas en verde sobre compute serverless](docs/img/02_job_orquestacion.png)

Despliegue de los dos targets con el CLI de Databricks:

```
$ databricks bundle validate -t dev
Validation OK!
$ databricks bundle deploy -t dev
Created jobs.electrocasa_setup
Created jobs.electrocasa_monitoreo
Created pipelines.electrocasa_tracking
Created pipelines.electrocasa_pipeline
Created jobs.electrocasa_job
Resources: 5 created, 0 changed, 0 deleted, 0 unchanged
$ databricks bundle deploy -t prod
Created jobs.electrocasa_setup
Created jobs.electrocasa_monitoreo
Created pipelines.electrocasa_tracking
Created pipelines.electrocasa_pipeline
Created jobs.electrocasa_job
Created ... .permissions (5)
Resources: 10 created, 0 changed, 0 deleted, 0 unchanged
$ databricks bundle run -t prod electrocasa_setup
"electrocasa_setup_prod" TERMINATED SUCCESS
```

### 14.4 Gobierno

`SHOW GRANTS` sobre el catálogo y los schemas después del setup:

| Principal | Privilegios | Objeto |
|---|---|---|
| electrocasa_ingenieria | USE CATALOG | catálogo electrocasa_dev |
| electrocasa_analistas | USE CATALOG | catálogo electrocasa_dev |
| electrocasa_auditoria | USE CATALOG | catálogo electrocasa_dev |
| electrocasa_ingenieria | USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW | bronze, silver y gold |
| electrocasa_analistas | USE SCHEMA, SELECT | gold |
| electrocasa_auditoria | USE SCHEMA, SELECT | gold |

Demostración de `REVOKE` sobre `electrocasa_dev.silver`:

| Momento | `SHOW GRANTS electrocasa_analistas ON SCHEMA electrocasa_dev.silver` |
|---|---|
| Después de `GRANT SELECT ... TO electrocasa_analistas` | `electrocasa_analistas`, `SELECT`, `SCHEMA`, `electrocasa_dev.silver` |
| Después de `REVOKE SELECT ... FROM electrocasa_analistas` | Sin filas |

![SHOW GRANTS antes y después del REVOKE en la salida del setup](docs/img/05a_revoke.png)

**Máscara de dni y salario.** La consulta a `gold.empleados_vigentes`, una tabla derivada de la historia de Empleados, hecha por un miembro de `electrocasa_ingenieria` (el dueño del pipeline), devuelve los valores reales. Para cualquier otro usuario, la misma máscara declarada en la tabla devuelve el dni con sus tres últimos dígitos y el salario nulo.

| miembro_de_ingenieria | id_empleado | dni | salario |
|---|---|---|---|
| true | EMP-00001 | 22108722 | 3058.00 |
| true | EMP-00003 | 59464684 | 2474.04 |
| true | EMP-00004 | 48867538 | 1532.42 |

En la captura está tapada la columna `nombre`.

![Consulta a gold.empleados_vigentes hecha por un miembro de electrocasa_ingenieria](docs/img/07_mascara_dni_salario.png)

**Acceso de Auditoría a `system`.** El setup intenta cinco `GRANT` para `electrocasa_auditoria`:

- `USE CATALOG` sobre `system`;
- `USE SCHEMA` sobre `system.access`;
- `SELECT` sobre las tablas `audit`, `table_lineage` y `column_lineage`.

El primero falló porque el usuario del proyecto no tiene `MANAGE` sobre el catálogo `system`. La celda marca cinco errores, uno por sentencia.

```
PERMISSION_DENIED: Principal '<usuario del proyecto>' does not have MANAGE on Catalog 'system'.
```

![Salida del setup: los GRANT sobre system rechazados por falta de MANAGE](docs/img/05b_acceso_system.png)

**Tablas de sistema.** En este workspace, `system.lakeflow` tampoco está disponible para el usuario del proyecto:

```
[INSUFFICIENT_PERMISSIONS] Insufficient privileges:
Principal does not have USE SCHEMA on Schema 'system.lakeflow'. SQLSTATE: 42501
```

Por eso el monitoreo usa el event log del pipeline y el historial de corridas del job, y la evidencia del grupo de Auditoría es la pestaña Lineage de Catalog Explorer, como prevé el enunciado. El linaje de `gold.ventas_netas_sucursal_mes` muestra que sale de `silver.ventas`, `silver.devoluciones` y `silver.sucursales`. En la captura está tapado el dueño de cada tabla.

![Linaje de gold.ventas_netas_sucursal_mes en Catalog Explorer](docs/img/04_linaje_gold.png)

### 14.5 Monitoreo: métricas de las expectativas

Del event log del pipeline (`event_log(TABLE(...))`). Las cifras suman las tres actualizaciones, porque cada lote recalcula las Materialized Views de Silver:

| Tabla | Expectativa | Política | Pasan | Fallan |
|---|---|---|---|---|
| silver.ventas | monto_positivo | drop | 37.550 | 735 |
| silver.ventas | sucursal_presente | drop | 38.074 | 211 |
| silver.ventas | cantidad_positiva | drop | 38.142 | 143 |
| silver.ventas | producto_en_catalogo | warn | 38.137 | 148 |
| silver.devoluciones | reembolso_valido | drop | 8.086 | 519 |
| silver.devoluciones | venta_existente | drop | 8.218 | 387 |
| silver.resenas | calificacion_en_escala | drop | 23.420 | 272 |
| silver.resenas | producto_en_catalogo | warn | 23.407 | 285 |
| silver.catalogo_snapshots | precio_valido | warn | 11.412 | 228 |
| empleados_cambios (vista del SCD Type 2) | evento_ubicable | drop | 4.410 | 38 |
| empleados_cambios | salario_sin_corregir | warn | 4.430 | 18 |
| silver.tracking_envios | estado_conocido | drop | 45.147 | 0 |
| silver.tracking_envios | venta_existente | warn | 31.467 | 13.680 |

En la última fila, los 13.680 que fallan son 415 eventos por actualización multiplicados por las corridas: la advertencia mide, no descarta.

### 14.6 Respuestas de Gold

**Pregunta 1, ventas netas** (extracto de la sucursal S01, ElectroCasa San Isidro):

| Mes | Ventas | Devoluciones | Ventas netas | Cantidad de ventas | Ticket promedio |
|---|---|---|---|---|---|
| 2026-03 | 245.865,67 | 16.963,88 | 228.901,79 | 61 | 4.030,58 |
| 2026-04 | 274.098,46 | 39.197,48 | 234.900,98 | 60 | 4.568,31 |
| 2026-05 | 279.739,45 | 50.022,37 | 229.717,08 | 55 | 5.086,17 |
| 2026-10 | 153.897,13 | 62.472,57 | 91.424,56 | 43 | 3.579,00 |

**Pregunta 2, top 3 por unidades:**

1. PRD-02306, Televisor Lenovo Lite 191: 30 unidades.
2. PRD-00751, Calefactor Panasonic Smart 130: 29 unidades.
3. PRD-00125, Aire acondicionado Bosch Lite 667: 27 unidades.

En la tasa de devolución los primeros puestos tienen tasa 1,0: productos cuyas pocas ventas se devolvieron todas. El primero es PRD-01152 con 5 ventas y 5 devoluciones. El desempate es por cantidad de devoluciones.

**Pregunta 3, dotación total al cierre de cada mes:**

| Mes | Mar | Abr | May | Jun | Jul | Ago | Sep | Oct |
|---|---|---|---|---|---|---|---|---|
| Empleados activos | 1.042 | 1.078 | 1.086 | 1.121 | 1.128 | 1.155 | 1.179 | 1.129 |

**Pregunta 4:** 5.505 filas de vendedor, sucursal y mes. Cada venta de tienda se asigna a la sucursal que tenía el vendedor en la fecha de la venta, según la historia SCD Type 2.

**Pregunta 5, reseñas negativas** (extracto):

| Categoría | Mes | Reseñas | Negativas | % |
|---|---|---|---|---|
| Línea blanca | 2026-10 | 357 | 75 | 21,01 |
| Climatización | 2026-05 | 223 | 49 | 21,97 |
| Cocina | 2026-04 | 205 | 27 | 13,17 |

Reto `VARIANT`, reseñas por origen leído del campo `metadata`:

| Origen | Reseñas | Negativas |
|---|---|---|
| app | 4.512 | 811 |
| tienda | 1.328 | 222 |
| web | 3.145 | 534 |

**Pregunta 6, despachos por courier.** Después de estandarizar los nombres quedan tres couriers:

| Courier | Envíos | Entregas con tiempo medido | Horas promedio de entrega | Envíos finalizados | Devueltos | % devueltos |
|---|---|---|---|---|---|---|
| Olva Courier | 1.773 | 1.391 | 103,86 | 1.476 | 85 | 5,76 |
| Servientrega | 1.799 | 1.363 | 103,83 | 1.476 | 113 | 7,66 |
| Shalom | 1.760 | 1.399 | 102,43 | 1.484 | 85 | 5,73 |

- **Tiempos de entrega:** casi iguales, entre 102 y 104 horas.
- **Devoluciones:** Servientrega devuelve más que los otros dos. El porcentaje es sobre los envíos finalizados: 113 de 1.476.

![Respuesta de Gold para la pregunta 6 en la salida del monitoreo](docs/img/06_despachos_courier.png)

## 15. Retos opcionales

| Reto | Dónde |
|---|---|
| Ingesta por metadata | `src/electrocasa/config/fuentes_auto_loader.json` y `transformations/bronze.py` |
| Cuarentena con trazabilidad completa | `silver.cuarentena` y `notebooks/91_reprocesar_cuarentena.ipynb` |
| `VARIANT` en Reseñas | `silver.resenas.metadata` y `gold.resenas_negativas_origen_mes` (origen, versión de la app y campaña) |
| Costos medidos | Consulta lista en `notebooks/90_monitoreo.ipynb`; en este workspace el usuario no tiene acceso a las tablas de sistema (ver 14.4), así que no se cuenta como reto logrado |
| Conector en el bundle | `resources/electrocasa_tracking.yml` |

## 16. Pruebas locales

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-dev.txt
.venv/Scripts/python -m pytest
```

Prueban, con PySpark local:

- la normalización de montos con distintos separadores;
- las fechas en tres formatos;
- la estandarización a conjuntos cerrados;
- la deduplicación que prefiere la versión válida;
- la corrección de salarios.

## 17. Antipatrones evitados

| Antipatrón | Cómo se evita |
|---|---|
| Credenciales en el código | Connection de Unity Catalog para Tracking; correo y base como variables al desplegar |
| Checkpoint en `/tmp` | Auto Loader dentro del pipeline, que gestiona checkpoint y schema |
| `input_file_name()` | `_metadata.file_name` |
| `inferSchema` en CSV | Schema explícito en el JSON de fuentes y tablas de COPY INTO con columnas declaradas |
| Particionar Bronze por fecha de negocio | Bronze sin particiones |
| Lógica de negocio en Bronze | Bronze solo agrega columnas técnicas |
| `trigger(once=True)` o trigger continuo en serverless | Pipelines en modo triggered (`continuous: false`) |
| Clusters all-purpose en el job | Todas las tareas en serverless |
| `dbutils.fs.mount` | Volumes de Unity Catalog |
| `outputMode("complete")` | Streaming Tables en append |
| `current_timestamp()` como `sequence_by` | `struct(fecha_evento, fecha_registro)` |
