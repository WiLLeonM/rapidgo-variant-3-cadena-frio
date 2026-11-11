"""Pruebas de la lógica del DAG de Airflow (no requieren Airflow, Kafka ni Spark)."""
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))
import cadena_frio_logica as lg  # noqa: E402

BASE = datetime(2026, 10, 21, 10, 0, 0, tzinfo=timezone.utc)


def _df(filas):
    """filas: (pedido, minuto, temperatura, humedad)"""
    return pd.DataFrame([
        {
            "kafka_partition": 0, "kafka_offset": i, "evento_id": f"evt-{i}",
            "pedido_id": p, "vehiculo_id": "VEH-001",
            "producto_tipo": "producto_refrigerado",
            "timestamp": BASE + timedelta(minutes=m),
            "temperatura_c": t, "humedad_pct": h,
        }
        for i, (p, m, t, h) in enumerate(filas)
    ])


def test_validacion_datos_limpios():
    v = lg.validar_eventos(_df([("PED-1", 0, 5.0, 50.0), ("PED-1", 1, 6.0, 50.0)]))
    assert v["filas"] == 2
    assert v["filas_con_nulos"] == 0 and v["filas_valores_imposibles"] == 0 and v["duplicados"] == 0


def test_validacion_detecta_nulos_imposibles_y_duplicados():
    df = _df([("PED-1", 0, 5.0, 50.0), ("PED-1", 1, 500.0, 50.0), ("PED-1", 2, 5.0, None)])
    df.loc[3 - 1 + 0, "kafka_offset"] = 0       # el tercer evento repite (partición, offset) del primero
    v = lg.validar_eventos(df)
    assert v["filas_valores_imposibles"] == 1
    assert v["filas_con_nulos"] == 1
    assert v["duplicados"] == 1


def test_validacion_falla_sin_columnas_del_contrato():
    with pytest.raises(ValueError):
        lg.validar_eventos(_df([("PED-1", 0, 5.0, 50.0)]).drop(columns=["pedido_id"]))


def test_validacion_falla_si_esta_vacio():
    with pytest.raises(ValueError):
        lg.validar_eventos(_df([("PED-1", 0, 5.0, 50.0)]).iloc[0:0])


def test_calidad_aprueba_datos_limpios():
    v = lg.validar_eventos(_df([("PED-1", i, 5.0, 50.0) for i in range(10)]))
    c = lg.evaluar_calidad(v, filas_raw=10)
    assert c["aprobado"] and c["fallos"] == []
    assert c["retencion_pct"] == 100.0


def test_calidad_rechaza_si_se_descarta_demasiado():
    v = lg.validar_eventos(_df([("PED-1", i, 5.0, 50.0) for i in range(5)]))
    c = lg.evaluar_calidad(v, filas_raw=10)         # solo llegó el 50 % a processed
    assert not c["aprobado"]
    assert any("retencion" in f for f in c["fallos"])


def test_calidad_rechaza_valores_imposibles():
    filas = [("PED-1", i, 5.0, 50.0) for i in range(8)] + [("PED-1", 8, 99.0, 50.0), ("PED-1", 9, -50.0, 50.0)]
    c = lg.evaluar_calidad(lg.validar_eventos(_df(filas)), filas_raw=10)
    assert not c["aprobado"]
    assert c["validez_pct"] == 80.0


def test_kpi_minutos_fuera_de_rango_y_porcentaje():
    # eventos cada minuto: 0-1 dentro, 2-3 fuera, 4 dentro  -> 2 min fuera de 5 monitorizados
    df = _df([("PED-1", 0, 5, 50), ("PED-1", 1, 5, 50), ("PED-1", 2, 9, 50), ("PED-1", 3, 9, 50), ("PED-1", 4, 5, 50)])
    detalle, resumen = lg.calcular_kpis(df)
    fila = detalle.iloc[0]
    assert fila["minutos_fuera_rango"] == pytest.approx(2.0, abs=0.05)
    assert fila["episodios_fuera_rango"] == 1
    assert fila["episodio_max_min"] == 2.0           # de 10:02 a 10:04 (recuperación)
    assert resumen["pedidos"] == 1


def test_alerta_exposicion_prolongada_a_los_10_minutos():
    corto = _df([("PED-1", m, 9.0, 50.0) for m in range(0, 9)] + [("PED-1", 9, 5.0, 50.0)])
    largo = _df([("PED-2", m, 9.0, 50.0) for m in range(0, 11)] + [("PED-2", 11, 5.0, 50.0)])
    assert not lg.calcular_kpis(corto)[0].iloc[0]["alerta_exposicion_prolongada"]
    assert lg.calcular_kpis(largo)[0].iloc[0]["alerta_exposicion_prolongada"]


def test_alerta_temperatura_critica_limites_inclusivos():
    df = _df([("PED-1", 0, 10.0, 50.0), ("PED-1", 1, 0.0, 50.0), ("PED-1", 2, 4.0, 50.0), ("PED-1", 3, 1.0, 50.0)])
    detalle, resumen = lg.calcular_kpis(df)
    assert resumen["alertas_temp_critica"] == 2     # 10.0 y 0.0; 1.0 está fuera de rango pero no es crítica


def test_hueco_de_datos_no_cuenta_como_exposicion():
    # dos eventos fuera de rango separados por 2 horas: no se asume que estuvo 2 h fuera
    df = _df([("PED-1", 0, 9.0, 50.0), ("PED-1", 120, 9.0, 50.0)])
    detalle, _ = lg.calcular_kpis(df)
    fila = detalle.iloc[0]
    assert fila["episodios_fuera_rango"] == 2
    assert fila["minutos_fuera_rango"] < 1


def test_kpis_por_pedido_son_independientes():
    df = _df([("PED-1", 0, 9, 50), ("PED-1", 1, 9, 50), ("PED-2", 0, 5, 50), ("PED-2", 1, 5, 50)])
    detalle, resumen = lg.calcular_kpis(df)
    por = detalle.set_index("pedido_id")
    assert por.loc["PED-2", "minutos_fuera_rango"] == 0
    assert por.loc["PED-1", "pct_dentro_rango"] < 100
    assert resumen["pedido_mas_afectado"] == "PED-1"


def test_xcom_serializable():
    import json
    detalle, resumen = lg.calcular_kpis(_df([("PED-1", 0, 5, 50), ("PED-1", 1, 9, 50)]))
    json.dumps(resumen)
    json.dumps(lg.validar_eventos(_df([("PED-1", 0, 5, 50)])))


def test_reporte_markdown_y_archivos():
    df = _df([("PED-1", i, 5.0 if i % 3 else 9.0, 50.0) for i in range(12)])
    val = lg.validar_eventos(df)
    cal = lg.evaluar_calidad(val, 12)
    detalle, kpis = lg.calcular_kpis(df)
    reporte = lg.construir_reporte("2026-10-21", {"capas": {"raw/eventos": 1, "processed/eventos": 1}}, val, cal, kpis)
    with tempfile.TemporaryDirectory() as tmp:
        rutas = lg.guardar_reporte(tmp, "2026-10-21", reporte, detalle)
        assert all(os.path.exists(r) for r in rutas.values())
        texto = open(rutas["md"], encoding="utf-8").read()
        assert "APROBADO" in texto and "KPI 1" in texto


def test_verificar_capas_sin_datos_falla():
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(FileNotFoundError):
            lg.verificar_capas(tmp)


def test_verificar_capas_con_parquet():
    with tempfile.TemporaryDirectory() as tmp:
        for capa in lg.CAPAS_REQUERIDAS:
            os.makedirs(os.path.join(tmp, capa, "_spark_metadata"))
            open(os.path.join(tmp, capa, "part-0000.snappy.parquet"), "wb").close()
            open(os.path.join(tmp, capa, "_spark_metadata", "ignorado.parquet"), "wb").close()
        r = lg.verificar_capas(tmp)
        assert r["capas"]["raw/eventos"] == 1
        assert "curated/kpis_ventana" in r["faltantes_opcionales"]
