"""Lógica de control de calidad y KPI de la cadena de frío (variante 3).

Este módulo NO importa Airflow a propósito: así las funciones se pueden probar
con pytest/GitHub Actions sin instalar Airflow. El DAG
(`cadena_frio_calidad_dag.py`) solo las orquesta.

Lee el Data Lake que escribe Spark (spark/streaming_job.py):
  raw/eventos         mensajes tal como llegan de Kafka
  processed/eventos   eventos validados con marcas de rango
  curated/...         KPIs por ventana y alertas

Las reglas salen de docs/kpi_alertas.md. Se pueden ajustar con variables de
entorno sin tocar el código.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd

# --- Parámetros (docs/kpi_alertas.md) ---
TEMP_MIN, TEMP_MAX = 2.0, 8.0
HUM_MIN, HUM_MAX = 35.0, 65.0
TEMP_CRITICA_ALTA, TEMP_CRITICA_BAJA = 10.0, 0.0
TEMP_FISICA = (-30.0, 60.0)      # rango físico válido (contrato del evento)
HUM_FISICA = (0.0, 100.0)

EXPOSICION_MINUTOS = float(os.getenv("EXPOSICION_MINUTOS", "10"))
MUESTREO_SEGUNDOS = float(os.getenv("MUESTREO_SEGUNDOS", "2"))      # el productor envía 1 evento cada 2 s
# Si entre dos eventos de un pedido pasa más que esto, se considera un hueco de
# datos: no se asume que el pedido estuvo todo ese tiempo fuera de rango.
MAX_INTERVALO_SEGUNDOS = float(os.getenv("MAX_INTERVALO_SEGUNDOS", "300"))

# Umbrales de la tarea de calidad (porcentajes)
MIN_CALIDAD_PCT = float(os.getenv("MIN_CALIDAD_PCT", "95"))
MIN_RETENCION_PCT = float(os.getenv("MIN_RETENCION_PCT", "90"))

COLUMNAS_OBLIGATORIAS = [
    "evento_id",
    "pedido_id",
    "vehiculo_id",
    "producto_tipo",
    "timestamp",
    "temperatura_c",
    "humedad_pct",
]
CAPAS_REQUERIDAS = ["raw/eventos", "processed/eventos"]
CAPAS_OPCIONALES = [
    "curated/kpis_ventana",
    "curated/alertas_temperatura_critica",
    "curated/alertas_exposicion_prolongada",
]


def a_python(obj):
    """Convierte numpy/pandas a tipos nativos para que el resultado viaje por XCom (JSON)."""
    if isinstance(obj, dict):
        return {str(k): a_python(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [a_python(v) for v in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if hasattr(obj, "item"):          # escalares de numpy
        return a_python(obj.item())
    if isinstance(obj, float) and obj != obj:   # NaN -> None
        return None
    return obj


# ---------------------------------------------------------------- Data Lake
def archivos_parquet(ruta: str) -> list[str]:
    """Lista los .parquet de una carpeta (recursivo), ignorando _spark_metadata."""
    encontrados = []
    for raiz, dirs, archivos in os.walk(ruta):
        dirs[:] = [d for d in dirs if not d.startswith("_")]
        encontrados += [os.path.join(raiz, a) for a in archivos if a.endswith(".parquet")]
    return encontrados


def verificar_capas(datalake: str) -> dict:
    """Comprueba que las capas del Data Lake existan y tengan archivos Parquet.

    Lanza FileNotFoundError si falta una capa requerida (raw o processed): en ese
    caso Airflow reintenta, porque puede que Spark aún no haya escrito datos.
    """
    resultado = {"datalake": datalake, "capas": {}, "faltantes_opcionales": []}
    faltantes = []
    
    for capa in CAPAS_REQUERIDAS + CAPAS_OPCIONALES:
        n = len(archivos_parquet(os.path.join(datalake, capa)))
        resultado["capas"][capa] = n
        if n == 0 and capa in CAPAS_REQUERIDAS:
            faltantes.append(capa)
        elif n == 0:
            resultado["faltantes_opcionales"].append(capa)
    if faltantes:
        raise FileNotFoundError(
            f"Sin archivos Parquet en {faltantes} dentro de '{datalake}'. "
            "¿Está corriendo el spark-job y el productor?"
        )
    return resultado


def leer_capa(datalake: str, capa: str, columnas: list[str] | None = None) -> pd.DataFrame:
    """Lee una capa Parquet completa (requiere pyarrow)."""
    return pd.read_parquet(os.path.join(datalake, capa), columns=columnas)


def contar_filas_raw(datalake: str) -> int:
    return int(len(leer_capa(datalake, "raw/eventos", columnas=["offset"])))


# ---------------------------------------------------------------- Validación
def fuera_de_rango(df: pd.DataFrame) -> pd.Series:
    """Regla de docs/kpi_alertas.md (límites inclusivos)."""
    return ~df["temperatura_c"].between(TEMP_MIN, TEMP_MAX) | ~df["humedad_pct"].between(HUM_MIN, HUM_MAX)


def validar_eventos(df: pd.DataFrame) -> dict:
    """Valida la capa processed contra el contrato del evento.

    Lanza ValueError si la estructura es inaceptable (vacía o sin columnas del
    contrato). Los problemas de calidad de los valores se devuelven como métricas
    para que la tarea de calidad decida si el lote se aprueba.
    """
    faltantes = [c for c in COLUMNAS_OBLIGATORIAS if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas del contrato en processed/eventos: {faltantes}")
    if df.empty:
        raise ValueError("processed/eventos no tiene filas")
    
    producto_invalido = (
        df["producto_tipo"] != "producto_refrigerado"
    ) & df["producto_tipo"].notna()

    filas = len(df)
    ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    obligatorios = df[COLUMNAS_OBLIGATORIAS].copy()
    obligatorios["timestamp"] = ts
    con_nulos = obligatorios.isna().any(axis=1)

    temp_imposible = ~df["temperatura_c"].between(*TEMP_FISICA) & df["temperatura_c"].notna()
    hum_imposible = ~df["humedad_pct"].between(*HUM_FISICA) & df["humedad_pct"].notna()
    valores_imposibles = temp_imposible | hum_imposible

    # Duplicados: la identidad real de un mensaje es (partición, offset) de Kafka.
    # evento_id solo se informa: si el productor lo repite, no es un fallo del pipeline.
    if {"kafka_partition", "kafka_offset"} <= set(df.columns):
        duplicados = int(df.duplicated(subset=["kafka_partition", "kafka_offset"]).sum())
        clave_duplicados = "kafka_partition+kafka_offset"
    else:
        duplicados = int(df.duplicated(subset=["evento_id", "pedido_id", "timestamp"]).sum())
        clave_duplicados = "evento_id+pedido_id+timestamp"
    evento_id_repetidos = int(df["evento_id"].duplicated().sum())

    return a_python({
        "filas": filas,
        "filas_con_nulos": int(con_nulos.sum()),
        "filas_valores_imposibles": int(valores_imposibles.sum()),
        "filas_producto_invalido": int(producto_invalido.sum()),
        "duplicados": duplicados,
        "clave_duplicados": clave_duplicados,
        "evento_id_repetidos_informativo": evento_id_repetidos,
        "pedidos": int(df["pedido_id"].nunique()),
        "primer_evento": ts.min(),
        "ultimo_evento": ts.max(),
    })


# ---------------------------------------------------------------- Calidad
def evaluar_calidad(validacion: dict, filas_raw: int) -> dict:
    """Convierte las métricas de validación en porcentajes y decide aprobado/rechazado."""
    filas = validacion["filas"]

    def pct_ok(malas: int) -> float:
        return round(100.0 * (1 - malas / filas), 2)

    completitud = pct_ok(validacion["filas_con_nulos"])
    malas_validez = (
    validacion["filas_valores_imposibles"]
    + validacion["filas_producto_invalido"]
)
validez = pct_ok(malas_validez)
    unicidad = pct_ok(validacion["duplicados"])
    # Parte de lo recibido en Kafka (raw) que llegó a processed sin descartarse.
    retencion = round(min(100.0, 100.0 * filas / filas_raw), 2) if filas_raw else 0.0

    fallos = []
    for nombre, valor, minimo in [
        ("completitud", completitud, MIN_CALIDAD_PCT),
        ("validez", validez, MIN_CALIDAD_PCT),
        ("unicidad", unicidad, MIN_CALIDAD_PCT),
        ("retencion_raw_a_processed", retencion, MIN_RETENCION_PCT),
    ]:
        if valor < minimo:
            fallos.append(f"{nombre}={valor}% < {minimo}%")

    return a_python({
        "filas_raw": filas_raw,
        "filas_processed": filas,
        "completitud_pct": completitud,
        "validez_pct": validez,
        "unicidad_pct": unicidad,
        "retencion_pct": retencion,
        "umbral_calidad_pct": MIN_CALIDAD_PCT,
        "umbral_retencion_pct": MIN_RETENCION_PCT,
        "aprobado": not fallos,
        "fallos": fallos,
    })


# ---------------------------------------------------------------- KPI
def _episodios_pedido(g: pd.DataFrame, max_intervalo: pd.Timedelta) -> list[dict]:
    """Episodios (rachas consecutivas fuera de rango) de un pedido ya ordenado por tiempo."""
    ts = g["timestamp"].reset_index(drop=True)
    fuera = g["fuera"].reset_index(drop=True)
    corte = ts.diff() > max_intervalo                      # hueco de datos: corta la racha
    grupo = ((fuera != fuera.shift()) | corte).cumsum()
    episodios = []
    for _, idx in grupo.groupby(grupo).groups.items():
        idx = list(idx)
        if not fuera.iloc[idx[0]]:
            continue
        inicio, ultimo = ts.iloc[idx[0]], ts.iloc[idx[-1]]
        sig = idx[-1] + 1
        # Se recupera si el siguiente evento llega a tiempo y está dentro de rango.
        if sig < len(ts) and not fuera.iloc[sig] and (ts.iloc[sig] - ultimo) <= max_intervalo:
            fin, abierto = ts.iloc[sig], False
        else:
            fin, abierto = ultimo, True
        episodios.append({
            "inicio": inicio, "fin": fin, "abierto": abierto,
            "duracion_min": round((fin - inicio).total_seconds() / 60, 2),
        })
    return episodios


def calcular_kpis(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """KPI 1, 2 y 3 y alertas 1 y 2 de docs/kpi_alertas.md, por pedido.

    Cada evento representa el intervalo hasta el siguiente evento del mismo
    pedido; el último evento (o el que precede a un hueco de datos) usa la
    frecuencia de muestreo.
    """
    datos = df[["pedido_id", "vehiculo_id", "timestamp", "temperatura_c", "humedad_pct"]].copy()
    datos["timestamp"] = pd.to_datetime(datos["timestamp"], utc=True)
    datos = datos.dropna().sort_values(["pedido_id", "timestamp"])
    datos["fuera"] = fuera_de_rango(datos)
    datos["critica"] = (datos["temperatura_c"] >= TEMP_CRITICA_ALTA) | (datos["temperatura_c"] <= TEMP_CRITICA_BAJA)

    max_int = pd.Timedelta(seconds=MAX_INTERVALO_SEGUNDOS)
    muestreo = pd.Timedelta(seconds=MUESTREO_SEGUNDOS)
    sig = datos.groupby("pedido_id")["timestamp"].shift(-1)
    intervalo = (sig - datos["timestamp"])
    intervalo = intervalo.where(intervalo.notna() & (intervalo <= max_int), muestreo)
    datos["intervalo_min"] = intervalo.dt.total_seconds() / 60

    filas = []
    for pedido, g in datos.groupby("pedido_id", sort=True):
        total_min = g["intervalo_min"].sum()
        fuera_min = g.loc[g["fuera"], "intervalo_min"].sum()          # KPI 1
        eps = _episodios_pedido(g, max_int)                            # KPI 2
        largo = max((e["duracion_min"] for e in eps), default=0.0)
        filas.append({
            "pedido_id": pedido,
            "vehiculo_id": g["vehiculo_id"].iloc[-1],
            "eventos": len(g),
            "minutos_monitorizados": round(total_min, 4),
            "minutos_fuera_rango": round(fuera_min, 4),                                    # KPI 1
            "pct_dentro_rango": round(100 * (1 - fuera_min / total_min), 2) if total_min else None,  # KPI 3
            "episodios_fuera_rango": len(eps),
            "episodio_max_min": largo,                                                      # KPI 2
            "alertas_temp_critica": int(g["critica"].sum()),                                # Alerta 1
            "alerta_exposicion_prolongada": bool(largo >= EXPOSICION_MINUTOS),              # Alerta 2
        })
    detalle = pd.DataFrame(filas)

    total = detalle["minutos_monitorizados"].sum()
    fuera_total = detalle["minutos_fuera_rango"].sum()
    peor = detalle.sort_values(["minutos_fuera_rango", "pedido_id"], ascending=[False, True]).iloc[0]
    resumen = a_python({
        "pedidos": len(detalle),
        "eventos": int(detalle["eventos"].sum()),
        "minutos_fuera_rango_total": round(fuera_total, 2),
        "pct_dentro_rango_global": round(100 * (1 - fuera_total / total), 2) if total else None,
        "episodios_fuera_rango": int(detalle["episodios_fuera_rango"].sum()),
        "episodio_max_min": float(detalle["episodio_max_min"].max()),
        "alertas_temp_critica": int(detalle["alertas_temp_critica"].sum()),
        "pedidos_con_exposicion_prolongada": int(detalle["alerta_exposicion_prolongada"].sum()),
        "pedido_mas_afectado": peor["pedido_id"],
        "minutos_fuera_rango_pedido_mas_afectado": round(float(peor["minutos_fuera_rango"]), 2),
    })
    return detalle, resumen


# ---------------------------------------------------------------- Reporte
def construir_reporte(fecha_ejecucion: str, verificacion: dict, validacion: dict,
                      calidad: dict, kpis: dict) -> dict:
    return a_python({
        "titulo": "Reporte de calidad y KPI - Cadena de frío (RapidGo, equipo 7)",
        "fecha_ejecucion": fecha_ejecucion,
        "generado_en": datetime.now(timezone.utc),
        "capas_datalake": verificacion["capas"],
        "validacion": validacion,
        "calidad": calidad,
        "kpis": kpis,
    })


def reporte_markdown(r: dict) -> str:
    v, c, k = r["validacion"], r["calidad"], r["kpis"]
    estado = "APROBADO" if c["aprobado"] else "RECHAZADO"
    capas = "\n".join(f"| `{capa}` | {n} |" for capa, n in r["capas_datalake"].items())
    return f"""# {r['titulo']}

- **Fecha de ejecución (Airflow):** {r['fecha_ejecucion']}
- **Generado:** {r['generado_en']}
- **Estado de calidad:** **{estado}**

## Data Lake
| Capa | Archivos Parquet |
|---|---|
{capas}

## Validación de datos (processed/eventos)
| Métrica | Valor |
|---|---|
| Filas | {v['filas']} |
| Pedidos distintos | {v['pedidos']} |
| Filas con nulos en campos obligatorios | {v['filas_con_nulos']} |
| Filas con valores físicamente imposibles | {v['filas_valores_imposibles']} |
| Duplicados ({v['clave_duplicados']}) | {v['duplicados']} |
| `evento_id` repetidos (informativo) | {v['evento_id_repetidos_informativo']} |
| Primer / último evento | {v['primer_evento']} / {v['ultimo_evento']} |

## Calidad
| Dimensión | Resultado | Umbral |
|---|---|---|
| Completitud | {c['completitud_pct']} % | {c['umbral_calidad_pct']} % |
| Validez | {c['validez_pct']} % | {c['umbral_calidad_pct']} % |
| Unicidad | {c['unicidad_pct']} % | {c['umbral_calidad_pct']} % |
| Retención raw → processed ({c['filas_raw']} → {c['filas_processed']}) | {c['retencion_pct']} % | {c['umbral_retencion_pct']} % |

## KPI de la cadena de frío
| KPI | Valor |
|---|---|
| Pedidos monitorizados | {k['pedidos']} |
| Eventos analizados | {k['eventos']} |
| KPI 1 - Minutos acumulados fuera de rango | {k['minutos_fuera_rango_total']} |
| KPI 2 - Episodios fuera de rango (máx. duración en min) | {k['episodios_fuera_rango']} ({k['episodio_max_min']}) |
| KPI 3 - % del tiempo dentro de rango | {k['pct_dentro_rango_global']} % |
| Alerta 1 - Eventos de temperatura crítica | {k['alertas_temp_critica']} |
| Alerta 2 - Pedidos con exposición prolongada | {k['pedidos_con_exposicion_prolongada']} |
| Pedido más afectado | {k['pedido_mas_afectado']} ({k['minutos_fuera_rango_pedido_mas_afectado']} min fuera de rango) |
"""


def guardar_reporte(directorio: str, fecha: str, reporte: dict, detalle: pd.DataFrame) -> dict:
    """Escribe JSON, Markdown y CSV por pedido en `directorio` y devuelve las rutas."""
    os.makedirs(directorio, exist_ok=True)
    base = os.path.join(directorio, f"reporte_cadena_frio_{fecha}")
    rutas = {"json": base + ".json", "md": base + ".md", "csv_pedidos": base + "_pedidos.csv"}
    with open(rutas["json"], "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2)
    with open(rutas["md"], "w", encoding="utf-8") as f:
        f.write(reporte_markdown(reporte))
    detalle.to_csv(rutas["csv_pedidos"], index=False)
    return rutas
