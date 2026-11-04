# Checkpoint Semana 3

1. **Qué quedó funcionando:**
Un DAG de Airflow (`cadena_frio_calidad_reportes`, en `airflow/dags/`) con 6 tareas: verificar el Data Lake, validar datos, calidad, KPI, reporte y resumen. Tiene dependencias, retries y comparte información entre tareas con XCom. Genera un reporte JSON/Markdown/CSV en `datalake/reportes/`.

2. **Valor o evidencia real observada:**
La ejecución manual `manual__2026-11-04T22:33:33` terminó con las 6 tareas en `success` (ver `docs/evidencia/airflow_grid_ejecucion_correcta.png` y `docs/evidencia/airflow_audit_log_resumen_kpi.png`). El reporte generado (`docs/evidencia/reporte_cadena_frio_2026-11-04.md`) analizó 426 eventos de 333 pedidos: calidad aprobada (retención raw → processed del 100 %), 33,87 minutos acumulados fuera de rango, 78,08 % del tiempo dentro de rango, 99 eventos de temperatura crítica y 0 pedidos con exposición prolongada.

3. **Decisión técnica tomada:**
La lógica de calidad y KPI vive en `airflow/dags/cadena_frio_logica.py`, sin importar Airflow, para poder probarla con pytest en GitHub Actions. La calidad es una compuerta: si no se cumplen los umbrales el DAG falla sin reintentar.

4. **Problema encontrado:**
Una ejecución falló en `calidad_datos` con retención del 89,47 % (221 de 247 filas) frente al umbral del 90 %. Con el productor activo, `raw` va por delante de `processed` porque son consultas de streaming distintas. Se detuvo el productor, se esperó a que Spark terminara de escribir y la siguiente ejecución pasó con retención del 100 %. Además, el reporte muestra 424 `evento_id` repetidos de 426 eventos: el productor usa los JSON de `samples/` y no genera un `evento_id` nuevo (solo `event_id`).

5. **Enlaces:**
Issue #8 - PR #
