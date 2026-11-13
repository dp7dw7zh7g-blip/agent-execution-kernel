"""Esquema canónico del sobre de mensaje UNAE–DTOS (rev. 1.1.1).

Única fuente de verdad: `schemas/unae_dtos_message.schema.v1_1_1.json`. El nombre genérico `schemas/unae_dtos_message.schema.json` es solo un
alias de compatibilidad (copia idéntica, vigilada por `tests/integrity_tests/test_schema_governance.py`); el código del adaptador NO debe
apuntar a él. El esquema v1.1 original se conserva en `docs/contratos/unae_dtos_message.schema.v1_1_original.json` (hash de la línea base).

Nota: la validación del sobre en ejecución (`adapter.validate_envelope`) está escrita en código y NO lee este archivo; las pruebas
(`SchemaAlignment`, y `SchemaConformance` si hay `jsonschema`) comprueban que ambos coinciden. Este módulo da el punto único y explícito
desde el que cualquier componente que necesite el esquema (validación con `jsonschema`, documentación, pruebas) lo carga.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SCHEMA_REVISION = "1.1.1"
SCHEMA_ID = "urn:unae-dtos:message:1.1.1"
SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "schemas" / "unae_dtos_message.schema.v1_1_1.json"


def load_canonical_schema() -> dict[str, Any]:
    """Carga el esquema v1.1.1 y comprueba que es el que se espera (no una versión distinta bajo el mismo nombre)."""
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema = json.load(f)
    if schema.get("$id") != SCHEMA_ID:
        raise RuntimeError(f"{SCHEMA_PATH.name}: $id {schema.get('$id')!r}, se esperaba {SCHEMA_ID!r}")
    return schema
