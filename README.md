# SI7006-SI6003_Trabajo1

**Trabajo 1 de Almacenamiento y Procesamiento de Grandes Datos — 2026**

> Alejandro Quintero Moreno  
> Santiago Gomez Rueda

# 1. Parte A: Capas Medallion

## 1.1. Descripción

Los datos utilizados en este trabajo provienen del *Bureau of Transportation Statistics* (BTS), perteneciente al Departamento de Transporte de Estados Unidos. El conjunto de datos contiene información sobre vuelos comerciales, incluyendo la aerolínea, los aeropuertos de origen y destino, la fecha del vuelo, los horarios programados y reales, la distancia recorrida y los retrasos de salida y llegada.

El archivo original en formato Parquet ocupa aproximadamente 578 MB y contiene alrededor de 32 millones de registros. Para desarrollar el procesamiento se utilizó Amazon Web Services (AWS), específicamente Amazon S3 para el almacenamiento y Amazon Athena para la consulta de los datos mediante SQL.

Se implementó una arquitectura Medallion compuesta por tres capas:

- **Bronze:** almacenamiento de los datos originales en un formato consultable.
- **Silver:** aplicación de reglas de calidad y separación de registros válidos y rechazados.
- **Gold:** agregación de los datos para responder una pregunta de negocio y facilitar las consultas analíticas.

## 1.2. Capa Bronze

### Almacenamiento y preparación de los datos

Se creó un bucket de Amazon S3 con el nombre `trabajo1-bucket-si7006`. El archivo original en formato Parquet se convirtió a CSV mediante el script `convertir.py` para facilitar la carga y consulta inicial de los datos en Athena.

Después de la conversión, el archivo ocupó aproximadamente 4,7 GB. Este aumento de tamaño se debe a las diferencias entre los formatos: Parquet utiliza almacenamiento columnar y compresión, mientras que CSV representa los datos mediante texto delimitado.

Se subió el archivo CSV a Amazon S3 y se creó la tabla `trabajo1.bronze` en Athena. Las 35 columnas se definieron inicialmente como `STRING`, dejando las conversiones de tipo para las siguientes etapas del procesamiento.

### Exploración inicial

Se examinó la distribución de la variable `DepDel15`, que representa si un vuelo tuvo un retraso de salida de al menos 15 minutos.

La consulta utilizada fue:

```sql
SELECT
    DepDel15,
    COUNT(*) AS registros
FROM trabajo1.bronze
GROUP BY DepDel15
ORDER BY DepDel15;
```

Los resultados obtenidos fueron:

| Valor de `DepDel15` | Interpretación | Registros |
|---|---|---:|
| `0.0` | Sin retraso de salida de 15 minutos o más | 24.989.074 |
| `1.0` | Retraso de salida de 15 minutos o más | 6.843.957 |
| Vacío | Etiqueta no disponible | 553.405 |
| **Total** | | **32.386.436** |

Se identificaron 553.405 registros sin etiqueta en `DepDel15`. La ausencia de esta variable representa un problema para el entrenamiento de un modelo de aprendizaje supervisado, dado que no se conoce la clase que debe predecirse.

Se comprobó que todos estos registros correspondían a vuelos cancelados. Sin embargo, no todos los vuelos cancelados tenían la etiqueta vacía: algunos sí contaban con un valor en `DepDel15`.

Por esta razón, se decidió excluir los registros sin etiqueta del conjunto de datos válido para el modelado, sin asignarles artificialmente una clase.

### Corrección de la ubicación de los datos

Durante la configuración inicial de Athena, la tabla Bronze apuntaba al directorio raíz del bucket, donde también se almacenaban los resultados de las consultas. Para evitar que Athena interpretara esos archivos como parte del conjunto de datos, se creó el directorio `bts_bronze/`, se ubicó allí el CSV y se actualizó la ubicación de la tabla.

Después de la corrección, la tabla Bronze quedó con un total de **32.386.436 registros**.

## 1.3. Capa Silver

### Reglas de calidad

La capa Silver tiene como objetivo separar los registros que cumplen las condiciones de calidad de aquellos que deben rechazarse. Para verificar la integridad del proceso se estableció la siguiente condición:

\[
\text{Filas Bronze} =
\text{Filas válidas} + \text{Filas rechazadas}
\]

Se definieron cinco reglas de calidad:

| Regla | Condición SQL | Justificación |
|---|---|---|
| R1. Etiqueta válida | `TRY_CAST(DepDel15 AS DOUBLE) IN (0, 1)` | Permite conservar únicamente las etiquetas válidas para el aprendizaje supervisado. Se encontraron 553.405 etiquetas vacías, todas asociadas a vuelos cancelados. |
| R2. Fecha válida | `TRY_CAST(FlightDate AS DATE) IS NOT NULL` | Evita fechas ausentes o que no puedan interpretarse correctamente. |
| R3. Distancia positiva | `TRY_CAST(Distance AS DOUBLE) > 0` | Evita distancias no numéricas, ausentes o no positivas. |
| R4. Aerolínea informada | `TRIM(Reporting_Airline) <> ''` | Garantiza que cada registro tenga identificada la aerolínea. |
| R5. Ruta completa | `TRIM(Origin) <> '' AND TRIM(Dest) <> ''` | Garantiza que estén identificados los aeropuertos de origen y destino. |

Las reglas R2 a R5 se incluyeron como controles preventivos de calidad. Las validaciones realizadas no encontraron incumplimientos en esas condiciones. Por tanto, no se afirma que hayan corregido errores existentes, sino que permiten detectar posibles anomalías si aparecen en otros datos o futuras cargas.

### Creación de las tablas Silver

Se crearon dos tablas Iceberg en Amazon Athena:

- `trabajo1.silver_validas`: contiene los registros que cumplen las cinco reglas.
- `trabajo1.silver_rechazadas`: contiene los registros rechazados y una columna adicional, `motivo_rechazo`, que identifica la regla incumplida.

Las tablas se almacenaron en Amazon S3 en formato Parquet mediante Apache Iceberg, lo que permite administrar los datos como tablas analíticas.

### Resultados de la depuración

La reconciliación de registros produjo los siguientes resultados:

| Tabla | Número de registros |
|---|---:|
| Bronze | 32.386.436 |
| Silver válidas | 31.833.031 |
| Silver rechazadas | 553.405 |
| **Diferencia de reconciliación** | **0** |

La diferencia se calculó mediante:

\[
32.386.436 - 31.833.031 - 553.405 = 0
\]

Por lo tanto, todas las filas de Bronze quedaron clasificadas como válidas o rechazadas.

La distribución de los rechazos fue:

| Motivo de rechazo | Registros |
|---|---:|
| R1: etiqueta `DepDel15` vacía o inválida | 553.405 |
| R2: fecha inválida | 0 |
| R3: distancia no positiva o inválida | 0 |
| R4: aerolínea no informada | 0 |
| R5: origen o destino no informado | 0 |
| **Total** | **553.405** |

Todos los registros rechazados incumplieron la regla R1. Esto confirma que, para el conjunto analizado, el principal problema identificado fue la ausencia de la etiqueta objetivo.

## 1.4. Capa Gold

### Pregunta de negocio

Para la capa Gold se definió la siguiente pregunta:

**¿Qué aerolíneas y aeropuertos de origen presentan la mayor proporción de vuelos con retrasos de salida de 15 minutos o más?**

Esta pregunta permite identificar combinaciones de aerolínea y aeropuerto que presentan una mayor proporción de retrasos y que podrían requerir análisis operativo adicional.

### Diseño de la agregación

Se creó la tabla Iceberg `trabajo1.gold_retrasos_aerolinea_origen`, agrupando los datos por aerolínea y aeropuerto de origen.

La tabla contiene las siguientes métricas:

- `total_vuelos`: número de vuelos incluidos en cada combinación.
- `vuelos_con_retraso`: número de vuelos cuyo `DepDel15` es igual a 1.
- `porcentaje_retrasos`: proporción de vuelos con retrasos de al menos 15 minutos.
- `retraso_promedio_minutos`: promedio de los minutos de retraso de salida registrados.

Se excluyeron los vuelos cancelados, las etiquetas inválidas y los registros sin aerolínea u origen informado. Las mismas condiciones de inclusión se utilizaron en las consultas de referencia de Bronze y Silver.

La agregación almacenada en Gold permite responder la pregunta de negocio sin tener que procesar nuevamente todos los registros individuales en cada consulta.

### Resultados de Gold

La validación de la tabla agregada produjo los siguientes resultados:

| Indicador | Resultado |
|---|---:|
| Combinaciones de aerolínea y aeropuerto de origen | 2.141 |
| Total de vuelos incluidos | 31.814.091 |
| Vuelos con retraso de al menos 15 minutos | 6.832.503 |
| Porcentaje global de vuelos con retraso | 21,47 % |

El porcentaje global se calcula dividiendo el número de vuelos retrasados entre el total de vuelos incluidos. Este resultado es una proporción ponderada por el volumen de vuelos, no el promedio simple de los porcentajes de las 2.141 combinaciones.

El 21,47 % representa la proporción de vuelos incluidos que presentaron retrasos de salida de al menos 15 minutos. La tabla Gold permite examinar cómo varía esa proporción entre aerolíneas y aeropuertos.

## 1.5. Comparación de rendimiento entre las capas

### Metodología

Para evaluar el rendimiento de la arquitectura se ejecutó una consulta analítica que calcula, por aerolínea y aeropuerto de origen, el total de vuelos, el número de vuelos retrasados, el porcentaje de retrasos y el retraso promedio en minutos.

La consulta se ejecutó sobre Bronze y Silver utilizando la misma definición de la pregunta de negocio y las mismas condiciones de inclusión. En Gold, la consulta accede a la agregación precalculada almacenada en la tabla Iceberg.

Se registraron tres indicadores de Athena:

- **Datos escaneados:** volumen de datos leído durante la consulta.
- **Tiempo de ejecución:** tiempo reportado para ejecutar la consulta.
- **Tiempo en cola:** tiempo de espera antes de la ejecución.

### Resultados experimentales

| Indicador | Bronze | Silver | Gold |
|---|---:|---:|---:|
| Datos escaneados | 4,54 GB | 37,53 MB | 32,94 KB |
| Tiempo de ejecución | 12,939 s | 3,714 s | 2,723 s |
| Tiempo en cola | 91 ms | 96 ms | 104 ms |

Los valores corresponden a las ejecuciones registradas durante el experimento. El tiempo en cola se presenta por separado del tiempo de ejecución.

### Reducción de datos escaneados

La reducción porcentual de datos escaneados se calculó mediante:

\[
\text{Reducción (\%)} =
\frac{B_{\text{inicial}}-B_{\text{final}}}
{B_{\text{inicial}}}\times100
\]

Al comparar Bronze con Silver, los datos escaneados disminuyeron de 4,54 GB a 37,53 MB, lo que representa una reducción aproximada del **99,17 %**.

Al comparar Bronze con Gold, los datos escaneados disminuyeron de 4,54 GB a 32,94 KB, lo que representa una reducción aproximada del **99,9993 %**.

La reducción de datos escaneados entre Silver y Gold fue aproximadamente del **99,91 %**.

Estas reducciones se calcularon utilizando los valores reportados por Athena y conversiones aproximadas entre GB, MB y KB.

### Reducción del tiempo de ejecución

La reducción porcentual del tiempo de ejecución se calculó mediante:

\[
\text{Reducción (\%)} =
\frac{T_{\text{inicial}}-T_{\text{final}}}
{T_{\text{inicial}}}\times100
\]

La comparación Bronze–Silver muestra una reducción del tiempo de ejecución de aproximadamente **71,30 %**, al pasar de 12,939 a 3,714 segundos.

La comparación Bronze–Gold muestra una reducción aproximada del **78,96 %**, al pasar de 12,939 a 2,723 segundos.

Finalmente, el tiempo de ejecución disminuyó aproximadamente un **26,68 %** entre Silver y Gold.

### Interpretación de los resultados

Los resultados muestran una mejora progresiva en el rendimiento de las consultas analíticas a medida que se avanza en la arquitectura Medallion.

En Bronze, Athena debe leer el archivo CSV original, que contiene más de 32 millones de registros. El formato de texto requiere procesar los datos para interpretar sus columnas y aplicar las operaciones de filtrado y agregación.

En Silver, los registros se almacenan en formato Parquet mediante Iceberg. El formato columnar y la compresión permiten reducir significativamente el volumen de lectura, mientras que la limpieza previa facilita el procesamiento analítico.

En Gold, la agregación se calcula durante la creación de la tabla y queda almacenada. Cuando se consulta esta capa, Athena lee directamente los resultados agregados, en lugar de volver a procesar todos los vuelos. Esto explica la reducción especialmente importante de los datos escaneados.

Las diferencias observadas también pueden verse influidas por la organización física de los archivos, la compresión y las condiciones de ejecución. Por ello, los resultados corresponden a las mediciones obtenidas en este experimento y no garantizan exactamente el mismo rendimiento en todas las ejecuciones.

En conjunto, el experimento demuestra el beneficio de preparar los datos de acuerdo con su propósito: Bronze conserva los datos de entrada, Silver aplica controles de calidad y Gold optimiza el acceso a métricas relevantes para el negocio.

## 1.6. Conclusiones

La implementación de la arquitectura Medallion permitió construir un flujo de procesamiento desde los datos originales hasta una tabla analítica agregada utilizando Amazon S3, Amazon Athena, Parquet e Iceberg.

En Bronze se almacenaron 32.386.436 registros. En Silver se conservaron 31.833.031 registros válidos y se separaron 553.405 registros rechazados, con una diferencia de reconciliación igual a cero. La principal anomalía detectada fue la ausencia de la etiqueta objetivo en registros correspondientes a vuelos cancelados.

En Gold se construyó una agregación por aerolínea y aeropuerto de origen que responde una pregunta de negocio relacionada con la frecuencia de los retrasos. La tabla resultante contiene 2.141 combinaciones y representa 31.814.091 vuelos elegibles.

La comparación de rendimiento mostró que los datos escaneados disminuyeron de 4,54 GB en Bronze a 37,53 MB en Silver y 32,94 KB en Gold. El tiempo de ejecución pasó de 12,939 segundos a 3,714 y 2,723 segundos, respectivamente.

Estos resultados respaldan el uso de una arquitectura Medallion para separar el almacenamiento original, la validación de calidad y la preparación de datos analíticos. En particular, la agregación precalculada de Gold permite responder consultas de negocio con un volumen de lectura considerablemente menor.
