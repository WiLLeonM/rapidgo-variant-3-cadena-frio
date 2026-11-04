"""DAG de calidad y reportes de la cadena de frío (issue #8, hito S3).

verificar_datalake -> validar_datos -> [calidad_datos, calcular_kpis] -> generar_reporte -> resumen_kpi

- Retries: todas las tareas reintentan (default_args); verificar_datalake reintenta más
  veces porque puede que Spark aún no haya escrito datos.
- XCom: cada tarea publica un diccionario pequeño que usa la siguiente
  (verificación -> validación -> calidad/KPI -> reporte -> resumen).
- La calidad es una compuerta: si no se cumplen los umbrales el DAG falla sin
  reintentar (AirflowFailException), porque repetir no arregla datos malos.
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

import cadena_frio_logica as logica

log = logging.getLogger(__name__)

DATALAKE = os.getenv("DATALAKE_PATH", "/opt/airflow/datalake")
REPORTES = os.path.join(DATALAKE, "reportes")


def _verificar_datalake(**_):
    """Tarea 1: las capas raw y processed existen y tienen Parquet."""
    resultado = logica.verificar_capas(DATALAKE)
    log.info("Capas del Data Lake: %s", resultado["capas"])
    return resultado                                    # XCom (return_value)


def _validar_datos(ti, **_):
    """Tarea 2: validación de datos de processed/eventos contra el contrato."""
    ti.xcom_pull(task_ids="verificar_datalake")         # confirma que la verificación publicó
    df = logica.leer_capa(DATALAKE, "processed/eventos")
    validacion = logica.validar_eventos(df)
    log.info("Validación: %s", validacion)
    return validacion


def _calidad_datos(ti, **_):
    """Tarea 3: métricas de calidad y compuerta de aprobación."""
    validacion = ti.xcom_pull(task_ids="validar_datos")
    calidad = logica.evaluar_calidad(validacion, logica.contar_filas_raw(DATALAKE))
    log.info("Calidad: %s", calidad)
    if not calidad["aprobado"]:
        raise AirflowFailException("Calidad insuficiente: " + "; ".join(calidad["fallos"]))
    return calidad


def _calcular_kpis(ti, **_):
    """Tarea 4: KPI 1-3 y alertas por pedido (docs/kpi_alertas.md)."""
    ti.xcom_pull(task_ids="validar_datos")
    df = logica.leer_capa(DATALAKE, "processed/eventos")
    detalle, resumen = logica.calcular_kpis(df)
    ruta = os.path.join(REPORTES, "_tmp_kpis_pedidos.csv")
    os.makedirs(REPORTES, exist_ok=True)
    detalle.to_csv(ruta, index=False)                   # el detalle es grande: va a disco, no a XCom
    log.info("KPI: %s", resumen)
    return {"resumen": resumen, "detalle_csv": ruta}


def _generar_reporte(ti, ds, **_):
    """Tarea 5: reporte JSON + Markdown + CSV por pedido en datalake/reportes/."""
    import pandas as pd

    verificacion = ti.xcom_pull(task_ids="verificar_datalake")
    validacion = ti.xcom_pull(task_ids="validar_datos")
    calidad = ti.xcom_pull(task_ids="calidad_datos")
    kpis = ti.xcom_pull(task_ids="calcular_kpis")
    detalle = pd.read_csv(kpis["detalle_csv"])
    reporte = logica.construir_reporte(ds, verificacion, validacion, calidad, kpis["resumen"])
    rutas = logica.guardar_reporte(REPORTES, ds, reporte, detalle)
    os.remove(kpis["detalle_csv"])
    log.info("Reporte generado: %s", rutas)
    return {"rutas": rutas, "kpis": kpis["resumen"], "aprobado": calidad["aprobado"]}


def _resumen_kpi(ti, **_):
    """Tarea 6: deja el resumen de KPI en el log (evidencia visible en la UI)."""
    r = ti.xcom_pull(task_ids="generar_reporte")
    k = r["kpis"]
    log.info("=" * 60)
    log.info("RESUMEN KPI CADENA DE FRIO")
    log.info("Pedidos: %s | Eventos: %s", k["pedidos"], k["eventos"])
    log.info("KPI 1 minutos fuera de rango: %s", k["minutos_fuera_rango_total"])
    log.info("KPI 2 episodios: %s (max %s min)", k["episodios_fuera_rango"], k["episodio_max_min"])
    log.info("KPI 3 %% dentro de rango: %s", k["pct_dentro_rango_global"])
    log.info("Alertas: %s temperatura critica, %s pedidos con exposicion prolongada",
             k["alertas_temp_critica"], k["pedidos_con_exposicion_prolongada"])
    log.info("Reporte: %s", r["rutas"]["md"])
    log.info("=" * 60)
    return "ok"


default_args = {
    "owner": "equipo-7",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="cadena_frio_calidad_reportes",
    description="Control de calidad de datos y reporte de KPI de la cadena de frío",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["rapidgo", "cadena-frio", "semana-3"],
) as dag:
    verificar_datalake = PythonOperator(
        task_id="verificar_datalake", python_callable=_verificar_datalake,
        retries=4, retry_delay=timedelta(seconds=30),
    )
    validar_datos = PythonOperator(task_id="validar_datos", python_callable=_validar_datos)
    calidad_datos = PythonOperator(task_id="calidad_datos", python_callable=_calidad_datos)
    calcular_kpis = PythonOperator(task_id="calcular_kpis", python_callable=_calcular_kpis)
    generar_reporte = PythonOperator(task_id="generar_reporte", python_callable=_generar_reporte)
    resumen_kpi = PythonOperator(task_id="resumen_kpi", python_callable=_resumen_kpi)

    verificar_datalake >> validar_datos >> [calidad_datos, calcular_kpis] >> generar_reporte >> resumen_kpi
