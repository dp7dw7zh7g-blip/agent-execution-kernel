"""WAL verificado en los cuatro almacenes (D-70, parche 0018).

Hasta g0.8 r24 solo DTOS comprobaba el resultado de `PRAGMA journal_mode=WAL` (D-57). UNAE, el adaptador y la capa proactiva lo pedían y
seguían, de modo que en un sistema de archivos sin soporte de WAL degradaban en silencio a otro modo, sin la garantía de lectores/escritor
desacoplados que su diseño supone. Ahora los cuatro lanzan RuntimeError al abrir.

Una base en memoria devuelve journal_mode «memory»: es la forma real (sin simular nada) de provocar un modo distinto de WAL.
Cada almacén conserva su propio código a propósito (D-63): lo que se vigila aquí es el COMPORTAMIENTO común, igual que test_connection_policy.
"""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from adapter import UNAEDTOSAdapter
from dtos.store import SQLiteStore
from proactive.shadow import ShadowLog
from unae import Store


class WalRequired(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.d = Path(self._tmp.name)

    def assert_rejects_non_wal(self, factory):
        with self.assertRaises(RuntimeError) as cm:
            factory(":memory:")
        msg = str(cm.exception)
        self.assertIn("WAL", msg)
        self.assertIn("memory", msg, "el mensaje debe decir qué modo devolvió SQLite")

    def test_unae_rejects_a_database_that_cannot_use_wal(self):
        self.assert_rejects_non_wal(lambda p: Store(p, keep_open=False))

    def test_adapter_rejects_a_database_that_cannot_use_wal(self):
        self.assert_rejects_non_wal(UNAEDTOSAdapter)

    def test_proactive_rejects_a_database_that_cannot_use_wal(self):
        self.assert_rejects_non_wal(ShadowLog)

    def test_dtos_still_opens_a_regular_file_in_wal(self):
        """DTOS ya verificaba WAL desde el parche 0010 (D-57, con su propia prueba) y admite `:memory:`; aquí solo se fija que no cambió."""
        s = SQLiteStore(self.d / "d.sqlite")
        self.addCleanup(s.close)
        self.assertEqual(s._conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")

    def test_all_four_open_normally_on_a_regular_file(self):
        s = Store(str(self.d / "u.sqlite"))
        self.addCleanup(s.close)
        UNAEDTOSAdapter(self.d / "a.sqlite")
        ShadowLog(self.d / "s.sqlite")
        for name in ("u.sqlite", "a.sqlite", "s.sqlite"):
            c = sqlite3.connect(self.d / name)
            try:
                self.assertEqual(c.execute("PRAGMA journal_mode").fetchone()[0], "wal", name)
            finally:
                c.close()

    def test_a_failed_open_leaves_no_database_behind_in_the_wrong_mode(self):
        with self.assertRaises(RuntimeError):
            Store(":memory:", keep_open=False)
        # y un Store válido posterior sobre un archivo sigue funcionando (no queda estado global roto)
        s = Store(str(self.d / "ok.sqlite"), keep_open=False)
        self.assertGreaterEqual(s.schema_version(), 1)


if __name__ == "__main__":
    unittest.main()
