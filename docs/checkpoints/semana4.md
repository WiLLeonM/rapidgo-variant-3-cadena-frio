# Checkpoint Semana 4

1. **Qué quedó funcionando:**
El pipeline completo (productor → Kafka → Spark Structured Streaming → Data Lake → Airflow) se ejecuta localmente con Docker Compose siguiendo el README. El README quedó actualizado con instalación, arquitectura, ejecución, KPI, alertas, pruebas, resultados, limitaciones y tabla de contribuciones.

2. **Valor o evidencia real observada:**
- **Issue #11:** ejecución de integración del 11/11/2026 con 208 eventos procesados, calidad APROBADA (100 % en completitud, validez, unicidad y retención), 37 alertas de temperatura crítica y DAG con las 6 tareas en éxito. GitHub Actions: ejecución #50 en verde y seis servicios Docker en `Up`. Detalle en `docs/evidencia/Evidencias_integracion_end_to_end_semana_4.pdf`.
- **Issue #12:** README final y documentación al día (`datalake/README.md`, `docs/arquitectura.md`).

3. **Decisión técnica tomada:**
El productor mantiene el mismo `pedido_id` durante cada ejecución y el contrato usa `evento_id` y `producto_tipo`; Spark y la lógica de calidad validan que el producto sea `producto_refrigerado`.

4. **Problema encontrado:**
El README y el productor seguían con campos del contrato anterior (`event_id`, `equipo`). Se alinearon con `docs/schemas/evento_cadena_frio.schema.json`. Además, la prueba final no cubrió una alerta de exposición prolongada (0 casos), lo que se registra como limitación en el README.

5. **Enlaces:**
- [Issue #11](https://github.com/WiLLeonM/rapidgo-variant-3-cadena-frio/issues/11)
- [Issue #12](https://github.com/WiLLeonM/rapidgo-variant-3-cadena-frio/issues/12)
- [Issue #13](https://github.com/WiLLeonM/rapidgo-variant-3-cadena-frio/issues/13)
