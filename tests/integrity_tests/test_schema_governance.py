"""Gobernanza de esquemas (Parte 4, Fase 3): una sola fuente de verdad para el sobre v1.1.1."""
import json
import re
import unittest
from pathlib import Path

from adapter.schema import SCHEMA_ID, SCHEMA_PATH, load_canonical_schema

ROOT = Path(__file__).resolve().parents[2]
SCHEMAS = ROOT / "schemas"
ALIAS = SCHEMAS / "unae_dtos_message.schema.json"
V1_1_ORIGINAL = ROOT / "docs" / "contratos" / "unae_dtos_message.schema.v1_1_original.json"


class SchemaGovernance(unittest.TestCase):
    def test_canonical_schema_is_v1_1_1(self):
        self.assertEqual(SCHEMA_PATH, SCHEMAS / "unae_dtos_message.schema.v1_1_1.json")
        self.assertEqual(load_canonical_schema()["$id"], SCHEMA_ID)

    def test_generic_name_is_an_identical_alias(self):
        self.assertEqual(ALIAS.read_bytes(), SCHEMA_PATH.read_bytes(), "el alias debe ser copia byte a byte del canónico")
        self.assertEqual(json.loads(ALIAS.read_text(encoding="utf-8")), load_canonical_schema())

    def test_only_the_two_expected_files_live_in_schemas(self):
        self.assertEqual(sorted(p.name for p in SCHEMAS.iterdir()),
                         ["unae_dtos_message.schema.json", "unae_dtos_message.schema.v1_1_1.json"])

    def test_v1_1_original_is_archived_and_distinct(self):
        original = json.loads(V1_1_ORIGINAL.read_text(encoding="utf-8"))
        self.assertEqual(original["$id"], "urn:unae-dtos:message:1.1")
        self.assertNotEqual(original, load_canonical_schema())

    def test_code_never_points_at_the_generic_or_the_v1_1_schema(self):
        """Ningún módulo de src/ debe cargar el nombre genérico ni el esquema v1.1: solo `adapter.schema` conoce la ruta, y es la v1.1.1."""
        pat = re.compile(r"unae_dtos_message[._]schema(?!\.v1_1_1\.json)")
        offenders = [p.relative_to(ROOT).as_posix() for p in (ROOT / "src").rglob("*.py")
                     if pat.search(p.read_text(encoding="utf-8"))]
        # schema.py cita el nombre genérico solo en su docstring (explica el alias); ningún otro módulo puede citarlo.
        self.assertEqual(offenders, ["src/adapter/schema.py"])


if __name__ == "__main__":
    unittest.main()
