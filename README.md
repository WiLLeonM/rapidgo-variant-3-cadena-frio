# RapidGo - Pipeline Big Data (Variante 3: Cadena de Frío)

**Integrantes:**
- [Wilmer Leon]
- [Nicolas Martinez]
- [Yair Mora]

## 1. Objetivo del Proyecto
RapidGo es una plataforma de entregas que requiere monitorear productos sensibles. Este proyecto construye un pipeline Big Data End-to-End para capturar eventos de temperatura y humedad durante el reparto, procesarlos en tiempo real y generar alertas si la cadena de frío se rompe.

## 2. Arquitectura Inicial
El flujo de datos planificado para las próximas semanas es el siguiente:
`Productor de Eventos (Python) -> Apache Kafka -> Spark Structured Streaming -> Data Lake (Parquet) -> Apache Airflow (Orquestación)`

## 3. Organización del Repositorio
- `airflow/`: DAGs para orquestación (Semana 3): `cadena_frio_calidad_dag.py` (DAG) y `cadena_frio_logica.py` (lógica probada con pytest).
- `producer/`: Código del generador de eventos (Semana 2).
- `spark/`: Scripts de procesamiento en streaming (Semana 2).
- `tests/`: Pruebas automatizadas.
- `samples/`: Ejemplos de contratos JSON.
- `datalake/`: Almacenamiento local (Raw, Processed, Curated).
- `docs/`: Documentación, ADRs y checkpoints semanales.

### 4. Requisitos Previos y Ejecución

**Requisitos:**
- Docker Desktop (con Docker Compose v2)
- Python 3.11+

**Pasos generales de validación:**
1. Clonar el repositorio.
2. Validar sintaxis de Docker: `docker compose config`

**Ejecución del Pipeline (Estado Semana 2):**
1. Instalar dependencias de Python:
   pip install -r requirements.txt
2. Levantar los servicios de infraestructura (Kafka y Zookeeper):
   docker compose up -d zookeeper kafka
3. Crear el Topic en Kafka (solo la primera vez):
   docker exec -it rapidgo-variant-3-cadena-frio-kafka-1 kafka-topics --create --topic eventos-cadena-frio --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
4. Ejecutar el Productor de eventos:
   python producer/producer.py
5. Comprobar que los eventos llegan a Kafka (en una segunda terminal):
   docker exec -it rapidgo-variant-3-cadena-frio-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic eventos-cadena-frio --from-beginning --property print.key=true

## 5. Contrato del Evento
El contrato oficial se encuentra documentado en `samples/evento_cadena_frio.json`. Los campos principales son:
- `event_id`: Identificador único del evento.
- `equipo`: Identificador del equipo ("equipo-7").
- `pedido_id`: Identificador del pedido (usado como Key en Kafka).
- `vehiculo_id`: Identificador del vehículo.
- `timestamp`: Fecha y hora.
- `temperatura_c`: Temperatura en grados Celsius.
- `humedad_pct`: Porcentaje de humedad.
- `estado`: Estado del pedido ("EN_RUTA").

## 6. Orquestación con Airflow (Semana 3)

DAG `cadena_frio_calidad_reportes` (`airflow/dags/cadena_frio_calidad_dag.py`), 6 tareas:

```
verificar_datalake -> validar_datos -> [calidad_datos, calcular_kpis] -> generar_reporte -> resumen_kpi
```

| Tarea | Qué hace |
|---|---|
| `verificar_datalake` | Comprueba que `raw/` y `processed/` tengan Parquet (reintenta 4 veces si Spark aún no escribió) |
| `validar_datos` | Valida `processed/eventos` contra el contrato: columnas, nulos, valores imposibles, duplicados |
| `calidad_datos` | Completitud, validez, unicidad y retención raw→processed; si no cumple umbrales el DAG falla |
| `calcular_kpis` | KPI 1, 2 y 3 y alertas 1 y 2 por pedido (`docs/kpi_alertas.md`) |
| `generar_reporte` | Escribe JSON, Markdown y CSV en `datalake/reportes/` |
| `resumen_kpi` | Deja el resumen de KPI en el log de la tarea |

Las tareas se pasan información con **XCom**; `retries` está en `default_args` (2) y en `verificar_datalake` (4).

**Ejecutar:**
1. Tener datos en el Data Lake (Kafka + `docker compose up spark-job` + `python producer/producer.py`, ver sección 4 y `spark/README.md`).
2. `docker compose up airflow` (la primera vez instala pandas y pyarrow, tarda unos minutos).
3. Abrir http://localhost:8081, usuario `admin`; la contraseña sale en los logs: `docker compose logs airflow` (buscar *password*).
4. Activar el DAG `cadena_frio_calidad_reportes` y pulsar *Trigger DAG*.
5. Reporte generado en `datalake/reportes/reporte_cadena_frio_<fecha>.md`.

Variables opcionales: `MIN_CALIDAD_PCT` (95), `MIN_RETENCION_PCT` (90), `EXPOSICION_MINUTOS` (10), `MUESTREO_SEGUNDOS` (2), `MAX_INTERVALO_SEGUNDOS` (300).
