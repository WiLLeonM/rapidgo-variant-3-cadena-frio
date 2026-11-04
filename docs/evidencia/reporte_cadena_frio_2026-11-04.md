# Reporte de calidad y KPI - Cadena de frío (RapidGo, equipo 7)

- **Fecha de ejecución (Airflow):** 2026-11-04
- **Generado:** 2026-11-04T22:34:22.274308+00:00
- **Estado de calidad:** **APROBADO**

## Data Lake
| Capa | Archivos Parquet |
|---|---|
| `raw/eventos` | 29 |
| `processed/eventos` | 28 |
| `curated/kpis_ventana` | 171 |
| `curated/alertas_temperatura_critica` | 31 |
| `curated/alertas_exposicion_prolongada` | 0 |

## Validación de datos (processed/eventos)
| Métrica | Valor |
|---|---|
| Filas | 426 |
| Pedidos distintos | 333 |
| Filas con nulos en campos obligatorios | 0 |
| Filas con valores físicamente imposibles | 0 |
| Duplicados (kafka_partition+kafka_offset) | 0 |
| `evento_id` repetidos (informativo) | 424 |
| Primer / último evento | 2026-11-04T22:18:03.463193+00:00 / 2026-11-04T22:32:15.006309+00:00 |

## Calidad
| Dimensión | Resultado | Umbral |
|---|---|---|
| Completitud | 100.0 % | 95.0 % |
| Validez | 100.0 % | 95.0 % |
| Unicidad | 100.0 % | 95.0 % |
| Retención raw → processed (426 → 426) | 100.0 % | 90.0 % |

## KPI de la cadena de frío
| KPI | Valor |
|---|---|
| Pedidos monitorizados | 333 |
| Eventos analizados | 426 |
| KPI 1 - Minutos acumulados fuera de rango | 33.87 |
| KPI 2 - Episodios fuera de rango (máx. duración en min) | 98 (4.84) |
| KPI 3 - % del tiempo dentro de rango | 78.08 % |
| Alerta 1 - Eventos de temperatura crítica | 99 |
| Alerta 2 - Pedidos con exposición prolongada | 0 |
| Pedido más afectado | PED-638 (4.87 min fuera de rango) |
