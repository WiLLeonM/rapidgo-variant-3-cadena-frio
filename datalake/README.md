# Data Lake local - Cadena de frío

Spark Structured Streaming escribe aquí en formato Parquet; Airflow lee estas capas y genera los reportes. Las carpetas de datos no se versionan (ver `.gitignore`); se crean al ejecutar el pipeline.

| Ruta | Capa | Contenido |
|---|---|---|
| `raw/eventos` | Raw | Mensajes tal como llegan de Kafka (key, value, partition, offset) |
| `processed/eventos` | Processed | Eventos validados, con marcas de fuera de rango y temperatura crítica |
| `curated/kpis_ventana` | Curated | KPI por pedido y ventana de 5 minutos |
| `curated/alertas_temperatura_critica` | Curated | Alerta 1: temperatura crítica |
| `curated/alertas_exposicion_prolongada` | Curated | Alerta 2: exposición prolongada fuera de rango |
| `checkpoints/<capa>` | Spark | Checkpoints de Spark para recuperar el streaming |
| `reportes/` | Airflow | Reportes del DAG en JSON, Markdown y CSV |

## Consultar los resultados

```powershell
Get-ChildItem .\datalake -Recurse -Filter *.parquet
docker compose run --rm --no-deps spark-job /opt/spark/bin/spark-submit --master local[1] /app/spark/mostrar_resultados.py
```

## Eventos inválidos

Los eventos incompletos o con valores físicamente imposibles se descartan en la capa *processed*; la diferencia entre *raw* y *processed* se reporta como retención en el DAG de Airflow (`docs/calidad_datos.md`).
