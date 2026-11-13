# Resultados de Pruebas Finales - Issue #13

## 1. Pruebas Ejecutadas
- **Productor y Kafka:** El productor envía eventos en formato JSON cada 2 segundos al topic `eventos-cadena-frio`. Se verificó que la key (`pedido_id`) llega correctamente.
- **Spark Streaming:** Se procesaron los eventos y se almacenaron en formato Parquet en las capas `raw` y `processed`.
- **Airflow (DAG):** Se ejecutó el DAG `cadena_frio_calidad_reportes` con éxito. La calidad de datos fue APROBADA con 100% de retención.

## 2. Escenario de Alerta y KPI
- **Evento Normal:** Temperatura de 5.0 °C. No genera alerta.
- **Evento de Alerta:** Temperatura de 11.8 °C. El sistema registró **37 alertas de temperatura crítica**.
- **KPI 3 (% dentro de rango):** 82.23% del tiempo monitoreado.

## 3. Evidencias y Enlaces
- **Captura de Airflow en verde:** [La evidencia se encuentra en la pr #20 en la seccion como comentario]
- **Video demostrativo:** [https://youtu.be/bHtZC1hcD0g]