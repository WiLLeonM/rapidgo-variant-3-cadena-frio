# Calidad de datos – Cadena de frío RapidGo

## 1. Objetivo

Definir y documentar las reglas de calidad utilizadas para evaluar los eventos de la cadena de frío durante su procesamiento. Estas reglas permiten identificar registros incompletos, inválidos o duplicados, y calcular métricas para evaluar la confiabilidad de los datos.

## 2. Reglas de calidad

Se consideran las siguientes dimensiones:

| Dimensión | Descripción | Criterio |
|---|---|---|
| Completitud | Verifica que los campos obligatorios estén presentes. | Todos los campos requeridos deben existir. |
| Validez | Comprueba que los datos respeten el contrato y sus restricciones. | Tipos, formatos y límites físicos correctos. |
| Unicidad | Identifica eventos duplicados. | El identificador del evento no debe repetirse. |
| Retención | Compara los eventos de entrada con los procesados. | Permite identificar registros descartados durante el procesamiento. |

## 3. Validaciones de negocio

Para los productos refrigerados se utilizan los siguientes rangos operativos:

- Temperatura permitida: entre 2 °C y 8 °C.
- Humedad permitida: entre 35 % y 65 %.
- Temperatura crítica: igual o superior a 10 °C, o igual o inferior a 0 °C.
- Tiempo máximo tolerado fuera de rango: 10 minutos.

Un evento puede cumplir el contrato de datos y, aun así, estar fuera de los rangos operativos. En ese caso se considera estructuralmente válido, pero requiere una alerta de operación.

## 4. Métricas de calidad

Las métricas utilizadas son:

**Total de eventos evaluados:** cantidad de registros sometidos a validación.

**Eventos válidos:** registros que cumplen los campos obligatorios, formatos, tipos y restricciones físicas.

**Eventos inválidos:** registros que incumplen alguna de las condiciones del contrato.

**Eventos duplicados:** registros cuyo identificador ya había sido procesado.

**Porcentaje de validez:**

(Eventos válidos / Total de eventos evaluados) × 100.

**Porcentaje de completitud:**

(Campos obligatorios presentes / Total de campos obligatorios esperados) × 100.

**Porcentaje de retención:**

(Eventos procesados / Eventos recibidos) × 100.

Los eventos fuera de rango operativo se contabilizan separadamente de los eventos inválidos.

## 5. Evidencia de ejecución

En el reporte de calidad generado el 4 de noviembre de 2026 se observaron los siguientes resultados:

- Eventos procesados: 426.
- Pedidos analizados: 333.
- Completitud: 100 %.
- Validez: 100 %.
- Unicidad: 100 %.
- Retención Raw a Processed: 100 %.
- Tiempo acumulado fuera de rango: 33,87 minutos.
- Tiempo dentro del rango permitido: 78,08 %.
- Eventos de temperatura crítica: 99.
- Pedidos con exposición prolongada: 0.

Estos resultados corresponden a una ejecución concreta y no representan valores permanentes del sistema.

## 6. Tratamiento de eventos

Los eventos que incumplen el contrato deben identificarse y contabilizarse como inválidos, evitando que se incorporen a los datos procesados como registros válidos.

Los eventos que cumplen el contrato, pero presentan temperatura o humedad fuera del rango operativo, deben conservarse para su análisis y generar las alertas correspondientes.

## 7. Automatización

Las pruebas automatizadas permiten verificar que las reglas de calidad se cumplan de manera repetible.

Las pruebas se ejecutan mediante pytest y forman parte del workflow de GitHub Actions, junto con las validaciones existentes del proyecto.

## 8. Conclusión

La implementación de métricas de calidad permite evaluar la confiabilidad de los eventos y detectar posibles problemas durante el procesamiento de la cadena de frío. La separación entre errores estructurales y alertas operativas evita clasificar incorrectamente eventos que contienen información válida, pero representan condiciones de riesgo para los productos refrigerados.

## 9. Resultados de ejecución y observaciones

En la ejecución de Airflow del 4 de noviembre de 2026 se procesaron 426 eventos correspondientes a 333 pedidos distintos.

Los resultados de calidad fueron:

- Completitud: 100 % (umbral mínimo: 95 %).
- Validez: 100 % (umbral mínimo: 95 %).
- Unicidad: 100 % (umbral mínimo: 95 %).
- Retención Raw a Processed: 100 % (umbral mínimo: 90 %).

No se encontraron filas con campos obligatorios nulos, valores físicamente imposibles ni duplicados según la combinación `kafka_partition + kafka_offset`.

### Observación sobre los identificadores

El reporte también registra 424 repeticiones informativas de `evento_id`. Por este motivo, el resultado de unicidad del 100 % debe interpretarse según la clave de Kafka utilizada en la validación y no como garantía de que los identificadores de negocio sean únicos.

Se recomienda revisar la generación de `evento_id` en el productor y mantener diferenciadas ambas dimensiones de unicidad.

### Resultado

La ejecución fue aprobada porque las métricas evaluadas superaron sus umbrales mínimos. Sin embargo, la repetición de identificadores de negocio queda registrada como observación técnica para su revisión.
