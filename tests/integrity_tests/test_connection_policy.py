"""Política de conexión SQLite de los cuatro almacenes (D-63): se MIDE, no se supone.

Los cuatro (UNAE, DTOS, adaptador, capa proactiva) abren sus conexiones por separado y no comparten código a propósito:
UNAE y DTOS son originales parchados que no deben depender de un módulo externo, y la capa proactiva solo puede importar
`dtos.canonical.canonical_json` (D-49, lo vigila test_shadow). Lo que SÍ se vigila aquí es que la política efectiva coincida.

Si esta prueba falla tras un cambio deliberado de la política de un almacén, actualice la tabla ESPERADO y deje constancia en DECISIONES.
"""
import re
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from adapter import UNAEDTOSAdapter
from dtos.store import SQLiteStore
from proactive.shadow import ShadowLog
from unae import Store

PRAGMAS = ("journal_mode", "busy_timeout", "foreign_keys", "synchronous", "journal_size_limit")
SRC = Path(__file__).resolve().parents[2] / "src"

# synchronous: 2 = FULL. foreign_keys del adaptador = 0: su esquema NO declara claves foráneas (se comprueba más abajo); si algún día
# las declara, esta prueba obliga a activar el PRAGMA en `UNAEDTOSAdapter._connect`.
COMUN = {"journal_mode": "wal", "busy_timeout": 10000, "synchronous": 2, "journal_size_limit": 64 * 1024 * 1024}  # D-73/D-74
ESPERADO = {
    "unae": {**COMUN, "foreign_keys": 1},
    "dtos": {**COMUN, "foreign_keys": 1},
    "proactive": {**COMUN, "foreign_keys": 1},
    "adapter": {**COMUN, "foreign_keys": 0},
}


def medir(conn) -> dict:
    return {k: conn.execute(f"PRAGMA {k}").fetchone()[0] for k in PRAGMAS}


class ConnectionPolicy(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.d = Path(self._tmp.name)

    def test_unae(self):
        s = Store(str(self.d / "u.sqlite"))
        self.addCleanup(s.close)
        with closing(s.connect()) as c:
            self.assertEqual(medir(c), ESPERADO["unae"])

    def test_dtos(self):
        s = SQLiteStore(self.d / "d.sqlite")
        self.addCleanup(s.close)
        self.assertEqual(medir(s._conn), ESPERADO["dtos"])

    def test_proactive(self):
        log = ShadowLog(self.d / "s.sqlite")
        with closing(log._connect()) as c:
            self.assertEqual(medir(c), ESPERADO["proactive"])

    def test_adapter(self):
        a = UNAEDTOSAdapter(self.d / "a.sqlite")
        with closing(a._connect()) as c:
            self.assertEqual(medir(c), ESPERADO["adapter"])

    def test_adapter_schema_declares_no_foreign_keys(self):
        code = (SRC / "adapter" / "adapter.py").read_text(encoding="utf-8")
        self.assertIsNone(re.search(r"\b(REFERENCES|FOREIGN\s+KEY)\b", code, re.I),
                          "el esquema del adaptador declara claves foráneas: active PRAGMA foreign_keys=ON en `_connect` y actualice ESPERADO")


if __name__ == "__main__":
    unittest.main()
