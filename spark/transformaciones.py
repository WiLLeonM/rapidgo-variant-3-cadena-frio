"""Transformaciones del pipeline de cadena de frío (variante 3).

Funciones puras (DataFrame -> DataFrame) para poder probarlas sin Kafka.
Los parámetros salen de docs/kpi_alertas.md y son provisionales hasta
validarlos con los datos reales de la ejecución.
"""
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# --- Parámetros (docs/kpi_alertas.md) ---
TEMP_MIN, TEMP_MAX = 2.0, 8.0            # °C permitidos
HUM_MIN, HUM_MAX = 35.0, 65.0            # % permitidos
TEMP_CRITICA_ALTA = 10.0                 # °C: alerta si >= 10
TEMP_CRITICA_BAJA = 0.0                  # °C: alerta si <= 0
# Se pueden ajustar sin tocar el código (variables de entorno):
EXPOSICION_MINUTOS = int(os.getenv("EXPOSICION_MINUTOS", "10"))     # min fuera de rango para alertar
MUESTREO_SEGUNDOS = float(os.getenv("MUESTREO_SEGUNDOS", "60"))     # segundos entre eventos de un mismo pedido

# Contrato del evento (docs/contrato_evento.md)
EVENTO_SCHEMA = StructType([
    StructField("evento_id", StringType()),
    StructField("pedido_id", StringType()),
    StructField("vehiculo_id", StringType()),
    StructField("producto_tipo", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperatura_c", DoubleType()),
    StructField("humedad_pct", DoubleType()),
])


def parsear_eventos(df_kafka: DataFrame) -> DataFrame:
    """Convierte el `value` JSON de Kafka en columnas, conservando partición y offset."""
    return (
        df_kafka
        .select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.from_json(F.col("value").cast("string"), EVENTO_SCHEMA).alias("e"),
        )
        .select("kafka_partition", "kafka_offset", "e.*")
    )


# --- Transformación 1: validación y limpieza ---
def validar_eventos(df: DataFrame) -> DataFrame:
    """Castea el timestamp y descarta eventos incompletos o con valores imposibles."""
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    
    return df.filter(
        F.col("evento_id").isNotNull()
        & F.col("pedido_id").isNotNull()
        & F.col("vehiculo_id").isNotNull()
        & (F.col("producto_tipo") == "producto_refrigerado")
        & F.col("timestamp").isNotNull()
        & F.col("temperatura_c").between(-30, 60)
        & F.col("humedad_pct").between(0, 100)
    )

# --- Transformación 2: marca de fuera de rango ---
def marcar_fuera_de_rango(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("temp_fuera_rango",
                    ~F.col("temperatura_c").between(TEMP_MIN, TEMP_MAX))
        .withColumn("hum_fuera_rango",
                    ~F.col("humedad_pct").between(HUM_MIN, HUM_MAX))
        .withColumn("fuera_de_rango",
                    F.col("temp_fuera_rango") | F.col("hum_fuera_rango"))
    )


# --- Transformación 3: temperatura crítica y columna de partición ---
def marcar_temperatura_critica(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("alerta_temperatura_critica",
                    (F.col("temperatura_c") >= TEMP_CRITICA_ALTA)
                    | (F.col("temperatura_c") <= TEMP_CRITICA_BAJA))
        .withColumn("fecha", F.to_date("timestamp"))
    )


def procesar_eventos(df_kafka: DataFrame) -> DataFrame:
    """Capa Processed: parseo + las 3 transformaciones."""
    return marcar_temperatura_critica(
        marcar_fuera_de_rango(validar_eventos(parsear_eventos(df_kafka)))
    )


# --- Agregación por ventana: KPIs (capa Curated) ---
def kpis_por_ventana(df: DataFrame, ventana: str = "5 minutes",
                     watermark: str = "2 minutes") -> DataFrame:
    """KPIs por pedido y ventana de tiempo.

    - minutos_fuera_rango_estimado -> KPI 1
    - pct_dentro_rango             -> KPI 3
    - temp_media_c                 -> KPI complementario
    """
    return (
        df.withWatermark("timestamp", watermark)
        .groupBy(F.window("timestamp", ventana).alias("ventana"),
                 "pedido_id", "vehiculo_id")
        .agg(
            F.count("*").alias("eventos"),
            F.sum(F.col("fuera_de_rango").cast("int")).alias("eventos_fuera_rango"),
            F.round(F.avg("temperatura_c"), 2).alias("temp_media_c"),
            F.min("temperatura_c").alias("temp_min_c"),
            F.max("temperatura_c").alias("temp_max_c"),
        )
        .withColumn("minutos_fuera_rango_estimado",
                    F.round(F.col("eventos_fuera_rango") * MUESTREO_SEGUNDOS / 60, 2))
        .withColumn("pct_dentro_rango",
                    F.round(100 * (1 - F.col("eventos_fuera_rango") / F.col("eventos")), 2))
        .select(
            F.col("ventana.start").alias("ventana_inicio"),
            F.col("ventana.end").alias("ventana_fin"),
            "pedido_id", "vehiculo_id", "eventos", "eventos_fuera_rango",
            "minutos_fuera_rango_estimado", "pct_dentro_rango",
            "temp_media_c", "temp_min_c", "temp_max_c",
        )
    )


# --- Alerta 1: temperatura crítica ---
def alertas_temperatura_critica(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("alerta_temperatura_critica"))
        .select(
            F.lit("TEMPERATURA_CRITICA").alias("alerta_tipo"),
            F.lit("CRITICA").alias("severidad"),
            "evento_id", "pedido_id", "vehiculo_id", "timestamp",
            "temperatura_c", "fecha",
        )
    )


# --- Alerta 2: exposición prolongada fuera de rango ---
def alertas_exposicion_prolongada(df: DataFrame,
                                  exposicion_min: int = EXPOSICION_MINUTOS,
                                  watermark: str = "2 minutes") -> DataFrame:
    """Alerta si, en una ventana deslizante de (exposicion_min + 1) minutos,
    TODOS los eventos de un pedido están fuera de rango y abarcan al menos
    `exposicion_min` minutos entre el primero y el último.

    Es una aproximación con ventanas (sin estado por episodio): mientras la
    exposición continúa puede repetir la alerta en cada ventana (paso de 1 min).
    """
    ventana = f"{exposicion_min + 1} minutes"
    return (
        df.withWatermark("timestamp", watermark)
        .groupBy(F.window("timestamp", ventana, "1 minute").alias("ventana"),
                 "pedido_id", "vehiculo_id")
        .agg(
            F.count("*").alias("eventos"),
            F.sum(F.col("fuera_de_rango").cast("int")).alias("eventos_fuera_rango"),
            F.min("timestamp").alias("primer_evento"),
            F.max("timestamp").alias("ultimo_evento"),
        )
        .withColumn("duracion_min",
                    (F.unix_timestamp("ultimo_evento")
                     - F.unix_timestamp("primer_evento")) / 60)
        .where((F.col("eventos") == F.col("eventos_fuera_rango"))
               & (F.col("duracion_min") >= exposicion_min))
        .select(
            F.lit("EXPOSICION_PROLONGADA").alias("alerta_tipo"),
            F.lit("ALTA").alias("severidad"),
            "pedido_id", "vehiculo_id", "primer_evento", "ultimo_evento",
            F.round("duracion_min", 2).alias("duracion_min"),
            F.to_date("primer_evento").alias("fecha"),
        )
    )
