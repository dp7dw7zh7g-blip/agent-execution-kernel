import tempfile
import unittest
from pathlib import Path

from unae import Store


class PerfOptionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "u.sqlite"
        self.wal = Path(str(self.db) + "-wal")

    def tearDown(self):
        self.tmp.cleanup()

    def touch(self, s):
        s.configure_account("__GLOBAL__", 10, 10, 10, 1)

    def test_synchronous_is_validated_and_applied(self):
        with self.assertRaises(ValueError):
            Store(self.db, synchronous="OFF")
        for mode, code in (("FULL", 2), ("NORMAL", 1)):
            with Store(self.db, synchronous=mode) as s:
                c = s.connect()
                self.assertEqual(c.execute("PRAGMA synchronous").fetchone()[0], code)
                c.close()

    def test_anchor_keeps_wal_between_operations(self):
        with Store(self.db, keep_open=True) as s:
            self.touch(s)
            self.assertTrue(self.wal.exists())          # sin ancla, SQLite lo borra al cerrar la última conexión
        s2 = Store(self.db, keep_open=False)
        self.touch(s2)
        self.assertFalse(self.wal.exists())

    def test_close_is_idempotent_and_store_keeps_working(self):
        s = Store(self.db)
        s.close(); s.close()
        self.touch(s)                                   # sin ancla funciona igual, solo más lento
        self.assertEqual(s.verify_integrity(), [])


if __name__ == "__main__":
    unittest.main()
