"""Estructura del DAG de Airflow. Se omite si Airflow no está instalado (p. ej. en CI)."""
import os
import sys

import pytest

pytest.importorskip("airflow.models")  # la carpeta airflow/ del repo se importa como paquete vacío; por eso se pide airflow.models
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))

from airflow.models import DagBag  # noqa: E402

DAGS = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags")


def _dag():
    bag = DagBag(dag_folder=DAGS, include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("cadena_frio_calidad_reportes")


def test_dag_tiene_al_menos_cinco_tareas():
    assert len(_dag().tasks) >= 5


def test_dependencias():
    dag = _dag()
    assert dag.get_task("validar_datos").upstream_task_ids == {"verificar_datalake"}
    assert dag.get_task("generar_reporte").upstream_task_ids == {"calidad_datos", "calcular_kpis"}
    assert dag.get_task("resumen_kpi").upstream_task_ids == {"generar_reporte"}


def test_retries_configurados():
    assert all(t.retries >= 2 for t in _dag().tasks)
