# SI7006-SI6003_Trabajo1

**Trabajo 1 de Almacenamiento y Procesamiento de Grandes Datos — 2026**

> Alejandro Quintero Moreno  
> Santiago Gomez Rueda

## Video: https://www.youtube.com/watch?v=cHrAHfBL724

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

# 2. Parte B: Streaming

## 1. Resumen

Se implementó un flujo de ingesta de eventos de sensores con la siguiente arquitectura:

```
Publicador (boto3, API Publish)
        │  MQTT, tema sensores/<device_id>/datos
        ▼
   AWS IoT Core ── regla regla_sensores_a_sqs
        │
        ▼
   SQS eventos-queue ──(3 recepciones fallidas)──► SQS eventos-dlq
        │
        ▼
   Lambda procesar-eventos (valida el esquema)
        │                         │
        ▼                         ▼
 DynamoDB eventos      DynamoDB eventos_descartados
   (válidos)             (rechazados, con motivo)
```

**Resultado de la prueba:** se publicaron 10.000 eventos, se guardaron 9.332 y se descartaron 668, con el motivo registrado para cada uno. Los conteos cuadran exactamente. El retraso entre el envío y la llegada a DynamoDB fue de 1.555 ms en promedio y 20.174 ms como máximo.

## 2. Esquema de los eventos

Los datos son sintéticos y simulan 20 dispositivos virtuales (`dev-00` a `dev-19`) que reportan temperatura y humedad.

```json
{"device_id":"dev-07","seq":153,"sent_ts":1761200000123,"temperature":23.4,"humidity":55.1,"run_id":"r1"}
```

| Campo | Tipo | Regla de validación | Uso |
|---|---|---|---|
| `device_id` | string | obligatorio, no vacío | Partition key |
| `seq` | entero | obligatorio | Sort key (contador por dispositivo) |
| `sent_ts` | entero (ms epoch) | obligatorio | Cálculo del retraso |
| `temperature` | número | obligatorio, entre -40 y 85 | Medición |
| `humidity` | número | obligatorio, entre 0 y 100 | Medición |
| `run_id` | string | opcional | Identifica la corrida |

## 3. Recursos creados

Todos los recursos se crearon manualmente desde la consola de AWS. La consola de CloudFormation no tiene stacks.

| Recurso | Nombre | Configuración principal |
|---|---|---|
| Regla de IoT Core | `regla_sensores_a_sqs` | SQL: `SELECT *, timestamp() AS iot_ts FROM 'sensores/+/datos'`. Acción: Simple Queue Service sobre `eventos-queue`, sin codificación Base64. Rol: `iot-sqs-role` |
| Cola SQS principal | `eventos-queue` | Standard, visibility timeout de 180 s, DLQ asociada con máximo 3 recepciones |
| Cola SQS de errores | `eventos-dlq` | Standard |
| Función Lambda | `procesar-eventos` | Python 3.12, 256 MB, timeout de 30 s, handler `lambda_function.handler` |
| Trigger de Lambda | SQS `eventos-queue` | Batch size 10, batch window 5 s, *Report batch item failures* activado |
| Tabla DynamoDB | `eventos` | Partition key `device_id` (S), sort key `seq` (N), modo on-demand |
| Tabla DynamoDB | `eventos_descartados` | Partition key `message_id` (S), modo on-demand |

El rol de ejecución de la Lambda tiene la política administrada `AWSLambdaSQSQueueExecutionRole` y una política en línea con `dynamodb:PutItem` sobre las dos tablas.

### Decisiones de configuración

- **Visibility timeout de 180 s:** equivale a seis veces el timeout de la función (30 s), valor recomendado por AWS para evitar que un mensaje en proceso vuelva a ser visible.
- **DLQ con 3 recepciones:** los errores transitorios se reintentan y los mensajes que fallan de forma persistente no bloquean el flujo.
- **Modo on-demand:** evita dimensionar capacidad y absorbe el pico de la prueba.
- **Campo `iot_ts` en la regla:** registra el instante en que IoT Core procesó el mensaje, lo que permite separar tramos del retraso en análisis posteriores.
- **Report batch item failures:** solo se reintentan los mensajes que fallaron dentro de un lote, no el lote completo.

## 4. Llave de DynamoDB y justificación

La tabla `eventos` usa `device_id` como partition key y `seq` como sort key.

- **Distribución de la carga:** al usar `device_id` como partición, las escrituras se reparten entre 20 dispositivos.
- **Consulta natural:** permite obtener todos los eventos de un dispositivo, ordenados, con un `Query` eficiente en lugar de un `Scan`.
- **Unicidad:** la combinación `device_id` + `seq` identifica cada evento de forma única.
- **Idempotencia:** SQS Standard entrega los mensajes al menos una vez, por lo que pueden llegar duplicados. El `PutItem` usa la condición `attribute_not_exists(device_id)`; si la llave ya existe, el evento se registra como descartado con el motivo `duplicado`.
- **Por qué no se usó el timestamp como llave:** dos eventos pueden coincidir en el mismo milisegundo y el reloj del dispositivo no es confiable.

En `eventos_descartados` la llave es el `MessageId` de SQS, que es único y se conserva entre reintentos, de modo que un reintento sobrescribe el mismo registro y no genera filas duplicadas.

## 5. Función Lambda

La función procesa lotes de hasta 10 mensajes. Para cada uno realiza lo siguiente:

1. Intenta interpretar el cuerpo como JSON. Si no es un objeto JSON válido, lo descarta con el motivo `json_invalido`.
2. Valida presencia, tipo y rango de los campos. Si falla, lo descarta con el motivo correspondiente (`campo_faltante:<campo>`, `tipo_invalido:<campo>` o `fuera_de_rango:<campo>`).
3. Si es válido, agrega `stored_ts` (instante de escritura) y lo guarda en `eventos` con la condición de unicidad.
4. Si la llave ya existe, lo registra como `duplicado`.
5. Ante cualquier otro error (por ejemplo, throttling), devuelve el mensaje en `batchItemFailures` para que SQS lo reintente. Tras tres recepciones fallidas pasa a la DLQ.

Variables de entorno: `TABLE=eventos` y `DISCARD_TABLE=eventos_descartados`.

```python
import json, os, time
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

ddb = boto3.resource("dynamodb")
tabla = ddb.Table(os.environ["TABLE"])
descartes = ddb.Table(os.environ["DISCARD_TABLE"])

def es_int(x):  return isinstance(x, int) and not isinstance(x, bool)
def es_num(x):  return isinstance(x, (int, Decimal)) and not isinstance(x, bool) or isinstance(x, Decimal)

def validar(e):
    """Devuelve None si es válido, o el motivo del descarte."""
    for c in ("device_id", "seq", "sent_ts", "temperature", "humidity"):
        if c not in e:
            return f"campo_faltante:{c}"
    if not isinstance(e["device_id"], str) or not e["device_id"]:
        return "tipo_invalido:device_id"
    if not es_int(e["seq"]):         return "tipo_invalido:seq"
    if not es_int(e["sent_ts"]):     return "tipo_invalido:sent_ts"
    if not es_num(e["temperature"]): return "tipo_invalido:temperature"
    if not es_num(e["humidity"]):    return "tipo_invalido:humidity"
    if not -40 <= e["temperature"] <= 85: return "fuera_de_rango:temperature"
    if not 0 <= e["humidity"] <= 100:     return "fuera_de_rango:humidity"
    return None

def descartar(msg_id, motivo, cuerpo):
    descartes.put_item(Item={
        "message_id": msg_id, "motivo": motivo,
        "payload": cuerpo[:2000], "descartado_ts": int(time.time() * 1000)})

def handler(event, context):
    fallos = []
    for r in event["Records"]:
        mid, cuerpo = r["messageId"], r["body"]
        try:
            try:
                e = json.loads(cuerpo, parse_float=Decimal)
                if not isinstance(e, dict):
                    raise ValueError
            except ValueError:
                descartar(mid, "json_invalido", cuerpo); continue

            motivo = validar(e)
            if motivo:
                descartar(mid, motivo, cuerpo); continue

            e["stored_ts"] = int(time.time() * 1000)   # llegada a DynamoDB
            try:
                tabla.put_item(Item=e,
                    ConditionExpression="attribute_not_exists(device_id)")
            except ClientError as ce:
                if ce.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    descartar(mid, "duplicado", cuerpo)
                else:
                    raise
        except Exception as ex:
            print("error", mid, ex)
            fallos.append({"itemIdentifier": mid})   # SQS lo reintenta; tras 3 va a la DLQ
    return {"batchItemFailures": fallos}
```

## 6. Generación y publicación de eventos

El script `publicar.py` genera los eventos y los publica con la API `Publish` de IoT Core (cliente `iot-data` de boto3, QoS 1) usando 30 hilos, ejecutado desde AWS CloudShell. El campo `sent_ts` se fija en el momento exacto del envío.

La carga se compone de:

- **9.900 eventos únicos**, repartidos entre 20 dispositivos.
- **Errores inyectados a propósito** (aproximadamente 2 % cada uno): campo `humidity` ausente, `temperature` con valor de texto y `humidity` fuera de rango (250).
- **100 eventos duplicados**, copias exactas (mismo `device_id` y `seq`) de eventos ya generados, mezcladas al azar.

Total: 10.000 publicaciones. El script registra en `publicados.txt` el número de publicaciones aceptadas por la API, que se usa como base del cuadre.

```python
# publicar.py
import boto3, json, random, time, threading
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor

ENDPOINT = "https://XXXXXXXX-ats.iot.us-east-2.amazonaws.com"   # endpoint iot:Data-ATS
REGION = "us-east-2"
UNICOS, DUPLICADOS, DEVICES, WORKERS = 9900, 100, 20, 30
RUN = "r1"

iot = boto3.client("iot-data", endpoint_url=ENDPOINT, region_name=REGION,
                   config=Config(max_pool_connections=WORKERS + 5))
ok = 0
fallos = 0
lock = threading.Lock()
contador = {f"dev-{i:02d}": 0 for i in range(DEVICES)}

def generar(n):
    dev = f"dev-{n % DEVICES:02d}"
    contador[dev] += 1
    e = {"device_id": dev, "seq": contador[dev], "sent_ts": 0,
         "temperature": round(random.uniform(15, 35), 1),
         "humidity": round(random.uniform(30, 80), 1), "run_id": RUN}
    r = random.random()
    if   r < 0.02: del e["humidity"]                 # -> campo_faltante
    elif r < 0.04: e["temperature"] = "caliente"     # -> tipo_invalido
    elif r < 0.06: e["humidity"] = 250               # -> fuera_de_rango
    return e

def enviar(e):
    global ok, fallos
    e = dict(e)
    e["sent_ts"] = int(time.time() * 1000)           # se fija justo al enviar
    try:
        iot.publish(topic=f"sensores/{e['device_id']}/datos", qos=1,
                    payload=json.dumps(e))
        with lock: ok += 1
    except Exception as ex:
        with lock: fallos += 1
        print("fallo API:", ex)

eventos = [generar(n) for n in range(UNICOS)]
eventos += random.sample(eventos, DUPLICADOS)        # mismos device_id y seq
random.shuffle(eventos)

t0 = time.time()
with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    list(pool.map(enviar, eventos))
dur = time.time() - t0

with open("publicados.txt", "w") as f:
    f.write(str(ok))
print(f"Publicados OK: {ok} | fallos API: {fallos} | duración: {dur:.0f}s")
```

**Salida de la ejecución:**

```
Publicados OK: 10000 | fallos API: 0 | duración: 19s
```

Se publicaron los 10.000 eventos sin fallos de la API, a una tasa aproximada de 525 mensajes por segundo.

## 7. Verificación y cuadre de conteos

El script `reconciliar.py` recorre con `Scan` las dos tablas, cuenta los registros, agrupa los descartados por motivo y calcula el retraso como `stored_ts - sent_ts` para cada evento guardado.

```python
# reconciliar.py
import boto3
from collections import Counter
from statistics import mean
ddb = boto3.resource("dynamodb")

def scan_all(nombre):
    t, items, kw = ddb.Table(nombre), [], {}
    while True:
        r = t.scan(**kw); items += r["Items"]
        if "LastEvaluatedKey" not in r: return items
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

ev  = scan_all("eventos")
des = scan_all("eventos_descartados")
PUBLICADOS = 10000          # valor impreso por publicar.py

motivos = Counter(d["motivo"] for d in des)
print("Guardados:", len(ev))
print("Descartados:", len(des), dict(motivos))
print("Guardados + descartados =", len(ev) + len(des), "| Publicados =", PUBLICADOS)

delays = [int(e["stored_ts"]) - int(e["sent_ts"]) for e in ev]
print(f"Retraso envío→DynamoDB: promedio {mean(delays):.0f} ms | "
      f"máximo {max(delays)} ms | mínimo {min(delays)} ms")
```

**Salida de la ejecución:**

```
Guardados: 9332
Descartados: 668 {'tipo_invalido:temperature': 191, 'duplicado': 94, 'fuera_de_rango:humidity': 189, 'campo_faltante:humidity': 194}
Guardados + descartados = 10000 | Publicados = 10000
Retraso envío→DynamoDB: promedio 1555 ms | máximo 20174 ms | mínimo 39 ms
```

### Cuadre de eventos

| Concepto | Cantidad |
|---|---|
| **Publicados (API OK)** | **10.000** |
| Guardados en DynamoDB (`eventos`) | 9.332 |
| Descartados: `campo_faltante:humidity` | 194 |
| Descartados: `tipo_invalido:temperature` | 191 |
| Descartados: `fuera_de_rango:humidity` | 189 |
| Descartados: `duplicado` | 94 |
| **Total descartados** | **668** |
| **Guardados + descartados** | **10.000** |

La igualdad **publicados = guardados + descartados** se cumple, y los motivos suman exactamente los 668 descartados.

### Motivos de descarte

| Motivo | Origen | Cantidad |
|---|---|---|
| `campo_faltante:humidity` | Eventos publicados sin el campo `humidity` | 194 |
| `tipo_invalido:temperature` | Eventos con `temperature` de tipo texto | 191 |
| `fuera_de_rango:humidity` | Eventos con `humidity` igual a 250 | 189 |
| `duplicado` | Reentrega de un par `device_id` + `seq` ya guardado | 94 |

Los tres primeros motivos corresponden a los errores inyectados por el generador (cerca del 2 % cada uno sobre 9.900 eventos únicos). Se registraron 94 duplicados y no 100 porque seis de las copias provenían de eventos que ya eran inválidos: ambas copias se descartan por su motivo original y no por la condición de unicidad.

## 8. Retraso entre envío y llegada

El retraso se mide como la diferencia entre `stored_ts` (asignado por la Lambda al escribir) y `sent_ts` (asignado por el publicador al enviar). Ambos procesos se ejecutaron en AWS (CloudShell y Lambda), lo que reduce el sesgo por diferencia de relojes.

| Métrica | Valor |
|---|---|
| Promedio | 1.555 ms |
| Máximo | 20.174 ms |
| Mínimo | 39 ms |

**Interpretación:**

- El promedio queda por debajo de la ventana de 5 s del trigger. Con el volumen de la prueba los lotes se completan con 10 mensajes, por lo que la función se invoca sin esperar a que termine la ventana.
- El máximo es cercano a la duración total de la publicación (19 s). La explicación más probable es que el ritmo de publicación (unos 525 mensajes por segundo) superó al principio la capacidad de consumo, de modo que se acumuló una cola y los últimos mensajes esperaron más. Esta hipótesis no se verificó con métricas de CloudWatch (por ejemplo, *Approximate Age Of Oldest Message* en SQS o *Concurrent executions* en Lambda).

## 9. Estado final de las colas

Al terminar el procesamiento, `eventos-queue` y `eventos-dlq` no tenían mensajes disponibles ni en vuelo, lo que confirma que no quedaron eventos pendientes ni mensajes en la cola de errores. La consola de SQS respalda este dato.

## 10. Preguntas finales

### ¿A partir de qué volumen se dejaría esta forma de streaming?

La arquitectura funciona bien hasta unos cientos de eventos por segundo sostenidos: SQS Standard escala casi sin límite, Lambda escala por concurrencia y DynamoDB on-demand absorbe la carga. Los umbrales siguientes son orientativos y dependen del caso:

1. **Alrededor de 1.000 eventos por segundo sostenidos, o decenas de millones de mensajes al mes.** El consumo en lotes de 10 depende mucho de la concurrencia disponible (una cuenta nueva puede tener un límite bajo) y el costo por evento de SQS, Lambda y escritura individual en DynamoDB se vuelve relevante.
2. **Necesidad de releer o reprocesar el historial.** SQS elimina el mensaje al consumirlo y no ofrece *replay*.
3. **Necesidad de orden estricto por dispositivo o de varios consumidores independientes** sobre el mismo flujo (analítica, alertas y almacenamiento). SQS Standard no garantiza orden ni permite que varios consumidores lean el mismo mensaje.
4. **Necesidad de agregaciones por ventanas de tiempo**, como promedios por minuto o detección de anomalías.

### ¿Qué se usaría en su lugar?

**Amazon Kinesis Data Streams.** Ofrece orden por partition key, retención con *replay* (de 24 horas a 365 días), múltiples consumidores y escalado por shards. La regla de IoT Core puede enviar los mensajes directamente a Kinesis. Para el histórico se añadiría **Kinesis Data Firehose hacia S3** (formato Parquet consultable con Athena), y DynamoDB quedaría para consultas de último estado. Si el volumen llegara a cientos de miles de eventos por segundo, o ya existiera un ecosistema Kafka, se evaluaría **Amazon MSK**.
