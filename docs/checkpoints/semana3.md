# Checkpoint Semana 3

1. **Qué quedó funcionando:**
Un DAG de Airflow (`cadena_frio_calidad_reportes`, en `airflow/dags/`) con 6 tareas: verificar el Data Lake, validar datos, calidad, KPI, reporte y resumen. Tiene dependencias, retries y comparte información entre tareas con XCom. Genera un reporte JSON/Markdown/CSV en `datalake/reportes/`.

2. **Valor o evidencia real observada:**
- **Issue #8 (Nicolás):** La ejecución manual `manual__2026-11-04T22:33:33` terminó con las 6 tareas en `success`. El reporte generado analizó 426 eventos de 333 pedidos: calidad aprobada, 33,87 minutos acumulados fuera de rango, 78,08 % del tiempo dentro de rango, 99 eventos de temperatura crítica y 0 pedidos con exposición prolongada.
- **Issue #9 (Yair):** Se realizó una prueba de fallo controlado inyectando un `FileNotFoundError` en la función `verificar_capas`. El log de Airflow mostró el mensaje real: `FileNotFoundError: FALLO CONTROLADO: No se encuentran los archivos del Data Lake para probar recuperación`. Tras eliminar el error, la tarea `verificar_datalake` pasó a estado **success** (verde), demostrando la recuperación del sistema.

3. **Decisión técnica tomada:**
La lógica de calidad y KPI vive en `airflow/dags/cadena_frio_logica.py`, sin importar Airflow, para poder probarla con pytest en GitHub Actions. La calidad es una compuerta: si no se cumplen los umbrales el DAG falla sin reintentar. Para la recuperación ante fallos (Issue #9), se implementaron reintentos (`retries=4` en `verificar_datalake` y `retries=2` por defecto) y uso de XCom (Ver ADR-003).

4. **Problema encontrado:**
- **Issue #8:** Una ejecución falló en `calidad_datos` con retención del 89,47 % frente al umbral del 90 %. Se resolvió deteniendo el productor y esperando a que Spark terminara.
- **Issue #9:** El fallo controlado bloqueó las tareas dependientes. Se resolvió eliminando la línea del error y reiniciando Airflow.

5. **Enlaces:**
- Issue #8
- Issue #9