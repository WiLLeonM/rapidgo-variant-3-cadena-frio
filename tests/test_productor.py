
import json
import sys
import uuid
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "producer"))

from producer import cargar_y_actualizar_evento


SCHEMA_PATH = ROOT_DIR / "docs" / "schemas" / "evento_cadena_frio.schema.json"
SAMPLE_PATH = ROOT_DIR / "samples" / "evento_cadena_frio.json"

with SCHEMA_PATH.open(encoding="utf-8") as archivo:
    ESQUEMA = json.load(archivo)

VALIDADOR = Draft202012Validator(
    ESQUEMA,
    format_checker=FormatChecker()
)


def test_productor_genera_evento_compatible_con_contrato():
    evento = cargar_y_actualizar_evento(str(SAMPLE_PATH))

    assert VALIDADOR.is_valid(evento)


def test_productor_genera_identificador_uuid_unico():
    evento1 = cargar_y_actualizar_evento(str(SAMPLE_PATH))
    evento2 = cargar_y_actualizar_evento(str(SAMPLE_PATH))

    assert evento1["evento_id"] != evento2["evento_id"]
    uuid.UUID(evento1["evento_id"])
    uuid.UUID(evento2["evento_id"])


def test_productor_no_agrega_campos_fuera_del_contrato():
    evento = cargar_y_actualizar_evento(str(SAMPLE_PATH))

    assert "event_id" not in evento
    assert "equipo" not in evento
    assert "evento_id" in evento
