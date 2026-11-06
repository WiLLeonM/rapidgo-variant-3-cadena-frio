
"""Pruebas complementarias de calidad de datos de la cadena de frío."""

import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "airflow", "dags")
)

import cadena_frio_logica as lg


BASE = datetime(2026, 11, 5, 10, 0, tzinfo=timezone.utc)


def crear_eventos(cantidad, humedad_nula=None):
    """Genera eventos de prueba con identificadores y offsets únicos."""
    filas = []

    for i in range(cantidad):
        filas.append({
            "kafka_partition": 0,
            "kafka_offset": i,
            "evento_id": f"evt-{i}",
            "pedido_id": "PED-001",
            "vehiculo_id": "VEH-001",
            "timestamp": BASE + timedelta(minutes=i),
            "temperatura_c": 5.0,
            "humedad_pct": None if i in (humedad_nula or []) else 50.0,
        })

    return pd.DataFrame(filas)


def test_evento_fuera_de_rango_es_estructuralmente_valido():
    """Un evento con temperatura de 9 °C genera alerta, no error estructural."""
    df = crear_eventos(1)
    df.loc[0, "temperatura_c"] = 9.0

    validacion = lg.validar_eventos(df)
    calidad = lg.evaluar_calidad(validacion, filas_raw=1)

    assert validacion["filas_valores_imposibles"] == 0
    assert calidad["validez_pct"] == 100.0
    assert calidad["aprobado"] is True
    assert bool(lg.fuera_de_rango(df).iloc[0]) is True


def test_completitud_exactamente_en_95_por_ciento():
    """El umbral mínimo de completitud del 95 % debe aprobarse."""
    df = crear_eventos(20, humedad_nula=[0])

    validacion = lg.validar_eventos(df)
    calidad = lg.evaluar_calidad(validacion, filas_raw=20)

    assert calidad["completitud_pct"] == 95.0
    assert calidad["aprobado"] is True


def test_completitud_inferior_al_95_por_ciento_rechaza():
    """Una completitud inferior al umbral debe rechazarse."""
    df = crear_eventos(20, humedad_nula=[0, 1])

    validacion = lg.validar_eventos(df)
    calidad = lg.evaluar_calidad(validacion, filas_raw=20)

    assert calidad["completitud_pct"] == 90.0
    assert calidad["aprobado"] is False
    assert any("completitud" in fallo for fallo in calidad["fallos"])


def test_retencion_exactamente_en_90_por_ciento():
    """El umbral mínimo de retención del 90 % debe aprobarse."""
    df = crear_eventos(9)

    validacion = lg.validar_eventos(df)
    calidad = lg.evaluar_calidad(validacion, filas_raw=10)

    assert calidad["retencion_pct"] == 90.0
    assert calidad["aprobado"] is True


def test_retencion_inferior_al_90_por_ciento_rechaza():
    """Una retención inferior al umbral debe rechazarse."""
    df = crear_eventos(8)

    validacion = lg.validar_eventos(df)
    calidad = lg.evaluar_calidad(validacion, filas_raw=10)

    assert calidad["retencion_pct"] == 80.0
    assert calidad["aprobado"] is False
    assert any("retencion" in fallo for fallo in calidad["fallos"])
