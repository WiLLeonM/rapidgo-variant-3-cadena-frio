# RapidGo - Pipeline Big Data (Variante 3: Cadena de Frío)

**Integrantes:**
- Wilmer Leon
- Nicolas Martinez
- Yair Mora

## 1. Objetivo del Proyecto

RapidGo es una plataforma de entregas que requiere monitorear productos sensibles. Este proyecto implementa un pipeline Big Data End-to-End para capturar eventos de temperatura y humedad durante el reparto, procesarlos en tiempo real y generar indicadores y alertas cuando se presentan condiciones que pueden comprometer la cadena de frío.

## 2. Arquitectura del Pipeline

El flujo de datos implementado es el siguiente:

`Productor de Eventos (Python) -> Apache Kafka -> Spark Structured Streaming -> Data Lake (Parquet) -> Apache Airflow`

**Función de cada componente:**

- **Productor (Python):** genera eventos de temperatura y humedad asociados a pedidos.
- **Kafka:** recibe y distribuye los eventos mediante el tópico `eventos-cadena-frio`.
- **Spark Structured Streaming:** consume los eventos, valida los datos, aplica transformaciones y calcula indicadores y alertas.
- **Data Lake:** almacena los resultados en las capas Raw, Processed y Curated, utilizando archivos Parquet.
- **Airflow:** orquesta las tareas de validación de calidad, cálculo de KPI y generación de reportes.

## 3. Organización del Repositorio

- `.github/workflows/`: automatización de validaciones mediante GitHub Actions.
- `airflow/`: DAGs y lógica de calidad de datos y generación de reportes.
- `producer/`: código del generador de eventos.
- `spark/`: scripts de procesamiento en streaming y transformaciones.
- `tests/`: pruebas automatizadas del proyecto.
- `samples/`: ejemplos de eventos JSON.
- `datalake/`: almacenamiento local de datos en las capas Raw, Processed y Curated.
- `docs/`: documentación técnica, arquitectura, KPI, ADRs y evidencias.
- `docker-compose.yml`: configuración de los servicios del pipeline.
- `requirements.txt`: dependencias de Python.

## 4. Requisitos Previos y Ejecución

**Requisitos:**

- Docker Desktop con Docker Compose v2.
- Python 3.12 (para las validaciones y pruebas locales).
- Git para clonar el repositorio.

### 4.1. Preparar el proyecto

1. Clonar el repositorio.
2. Abrir una terminal en la carpeta principal del proyecto.
3. Validar la configuración:

```powershell
docker compose config -q
```

### 4.2. Levantar la infraestructura

Iniciar los servicios:

```powershell
docker compose up -d
```

Comprobar su estado:

```powershell
docker compose ps
```

Los servicios principales son Kafka, Zookeeper, Spark y Airflow.

### 4.3. Ejecutar el productor

El productor utiliza `kafka:29092`, por lo que debe ejecutarse dentro de la red Docker del proyecto.

En la configuración utilizada durante la integración, se ejecutó un contenedor temporal de Python conectado a la red del proyecto, montando las carpetas `producer` y `samples`.

La imagen utilizada fue `python:3.12-slim` y se instaló la dependencia `kafka-python` antes de ejecutar:

```bash
python producer.py
```

El productor envía eventos periódicamente al tópico `eventos-cadena-frio`. Para detenerlo, utilizar `Ctrl + C`.

### 4.4. Verificar el procesamiento

Comprobar los registros de Spark:

```powershell
docker compose logs --tail=50 spark-job
```

Verificar los archivos generados en el Data Lake:

```powershell
Get-ChildItem .\datalake -Recurse -Filter *.parquet
```

Las capas de almacenamiento son:

- `datalake/raw/eventos/`
- `datalake/processed/eventos/`
- `datalake/curated/`

### 4.5. Ejecutar Airflow

1. Abrir en el navegador: `http://localhost:8081`.
2. Iniciar sesión con el usuario `admin`.
3. Consultar la contraseña en los registros de Airflow:

```powershell
docker compose logs airflow
```

4. Buscar el mensaje que contiene `Login with username`.
5. Activar el DAG `cadena_frio_calidad_reportes`.
6. Ejecutar el DAG mediante **Trigger DAG**.

El DAG realiza seis tareas:

```text
verificar_datalake
       |
 validar_datos
       |
   +---+---+
   |       |
calidad  calcular
 datos     kpis
   |       |
   +---+---+
       |
 generar_reporte
       |
   resumen_kpi
```

Los reportes se generan en `datalake/reportes/` en formatos JSON, Markdown y CSV.

## 5. Contrato del Evento

El contrato oficial se encuentra documentado en `samples/evento_cadena_frio.json`.

Los campos principales son:

- `evento_id`: identificador único del evento.
- `pedido_id`: identificador del pedido, utilizado también como clave en Kafka.
- `vehiculo_id`: identificador del vehículo.
- `producto_tipo`: tipo de producto monitoreado (`producto_refrigerado`).
- `timestamp`: fecha y hora de generación del evento.
- `temperatura_c`: temperatura registrada en grados Celsius.
- `humedad_pct`: porcentaje de humedad registrado.

Los eventos deben cumplir con el esquema JSON definido en el repositorio para poder ser procesados correctamente.

## 6. Procesamiento y KPI

Spark Structured Streaming realiza las siguientes operaciones:

- Parseo de los eventos recibidos desde Kafka.
- Validación de los campos obligatorios y valores físicos.
- Identificación de eventos fuera del rango permitido.
- Detección de temperaturas críticas.
- Cálculo de KPI por pedido y ventana temporal.
- Generación de alertas de temperatura crítica y exposición prolongada.

Los parámetros y reglas de negocio se encuentran documentados en `docs/kpi_alertas.md`.

## 7. Orquestación con Apache Airflow

El DAG `cadena_frio_calidad_reportes`, ubicado en `airflow/dags/cadena_frio_calidad_dag.py`, ejecuta seis tareas:

| Tarea | Función |
|---|---|
| `verificar_datalake` | Comprueba la existencia de datos Parquet. |
| `validar_datos` | Verifica columnas, valores nulos, valores imposibles y duplicados. |
| `calidad_datos` | Evalúa completitud, validez, unicidad y retención. |
| `calcular_kpis` | Calcula los indicadores y alertas de la cadena de frío. |
| `generar_reporte` | Genera reportes JSON, Markdown y CSV. |
| `resumen_kpi` | Presenta un resumen de los resultados en los registros de Airflow. |

Las tareas intercambian información mediante XCom y utilizan mecanismos de reintento ante errores.

## 8. Pruebas y Validación Continua

El proyecto incorpora pruebas automatizadas con `pytest` y validaciones de esquemas mediante `jsonschema`.

GitHub Actions ejecuta automáticamente las siguientes comprobaciones:

- Configuración de Java 17 y Python 3.12.
- Instalación de dependencias.
- Validación de sintaxis Python.
- Ejecución de pruebas automatizadas.
- Validación de Docker Compose.
- Validación de sintaxis de los DAGs de Airflow.

El workflow se ejecuta ante eventos de `push`, `pull_request` y mediante ejecución manual (`workflow_dispatch`).

## 9. Variables de Configuración

Entre las variables utilizadas se encuentran:

| Variable | Valor predeterminado |
|---|---:|
| `MIN_CALIDAD_PCT` | 95 |
| `MIN_RETENCION_PCT` | 90 |
| `EXPOSICION_MINUTOS` | 10 |
| `MUESTREO_SEGUNDOS` | 2 |
| `MAX_INTERVALO_SEGUNDOS` | 300 |

Los valores pueden ajustarse mediante variables de entorno según la configuración del proyecto.

## 10. Estado Final del Proyecto

Se realizó una ejecución local de integración que permitió comprobar la comunicación entre el productor, Kafka y Spark, la persistencia de los resultados en el Data Lake y la ejecución del DAG de Airflow.

El reporte generado por Airflow permitió verificar los indicadores de calidad y los KPI calculados sobre los eventos procesados.

Asimismo, GitHub Actions completó satisfactoriamente las validaciones automatizadas del proyecto en la ejecución de integración correspondiente.

**Conclusión:** el proyecto integra los componentes principales de un pipeline Big Data End-to-End para el monitoreo de la cadena de frío, incorporando procesamiento en streaming, almacenamiento distribuido por capas, control de calidad de datos, generación de indicadores y automatización de pruebas.
