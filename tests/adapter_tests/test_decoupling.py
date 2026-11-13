"""D-63: el adaptador no toca la conexión de los Stores y el `processor` de `receive` ya no recibe un `sqlite3.Connection` crudo.

Dos grupos:
  - Frontera: ningún módulo de `src/adapter` usa `_conn` ni abre `.connect()` de otro Store (solo la propia `self._connect()` de su base).
  - LocalTx: el processor puede escribir en la transacción local, pero no confirmarla, revertirla, cerrarla ni abrir otra.
"""
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path

from adapter import LocalResult, LocalTx, UNAEDTOSAdapter

from test_envelope import op_request

ADAPTER_SRC = Path(__file__).resolve().parents[2] / "src" / "adapter"


class Boundary(unittest.TestCase):
    def test_no_adapter_module_reaches_into_a_store_connection(self):
        offenders = []
        for f in sorted(ADAPTER_SRC.glob("*.py")):
            for n, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
                code = line.split("#", 1)[0]
                if re.search(r"(?<!self)\._conn\b", code) or re.search(r"\.connect\(\)", code):
                    offenders.append(f"{f.name}:{n}: {line.strip()}")
        self.assertEqual(offenders, [], "el adaptador debe hablar con los Stores solo por su API pública")

    def test_the_only_sql_connection_opened_by_the_adapter_is_its_own(self):
        code = (ADAPTER_SRC / "adapter.py").read_text(encoding="utf-8")
        self.assertEqual(len(re.findall(r"sqlite3\.connect\(", code)), 1, "una sola apertura de conexión: `UNAEDTOSAdapter._connect`")
        for f in sorted(ADAPTER_SRC.glob("*.py")):
            if f.name != "adapter.py":
                self.assertNotIn("sqlite3.connect(", f.read_text(encoding="utf-8"), f.name)


class LocalTxTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db = str(Path(self._tmp.name) / "adapter.db")
        self.a = UNAEDTOSAdapter(self.db)

    def sql(self, query):
        c = sqlite3.connect(self.db)
        try:
            return c.execute(query).fetchall()
        finally:
            c.close()

    def inbox_rows(self):
        return self.sql("SELECT COUNT(*) FROM adapter_inbox")[0][0]

    def receive_with(self, fn, m=None):
        return self.a.receive(m or op_request(), fn)

    def test_processor_receives_a_localtx_not_a_connection(self):
        seen = []
        self.receive_with(lambda tx, msg: seen.append(tx))
        self.assertIsInstance(seen[0], LocalTx)
        self.assertNotIsInstance(seen[0], sqlite3.Connection)
        for forbidden in ("commit", "rollback", "close", "cursor", "executescript", "set_authorizer", "isolation_level"):
            self.assertFalse(hasattr(seen[0], forbidden), forbidden)

    def test_local_write_is_atomic_with_the_inbox_row(self):
        def ok(tx, msg):
            tx.execute("CREATE TABLE local_effect(message_id TEXT PRIMARY KEY, n INTEGER)")
            tx.executemany("INSERT INTO local_effect VALUES (?, ?)", [(msg["message_id"], 1)])
            r = tx.execute("SELECT n FROM local_effect WHERE message_id=?", (msg["message_id"],))
            self.assertIsInstance(r, LocalResult)
            self.assertEqual(r.fetchone()[0], 1)
            self.assertFalse(hasattr(r, "connection"))
        self.receive_with(ok)
        self.assertEqual((self.inbox_rows(), self.sql("SELECT COUNT(*) FROM local_effect")[0][0]), (1, 1))

    def test_failure_after_the_local_write_rolls_back_both(self):
        def boom(tx, msg):
            tx.execute("CREATE TABLE local_effect(x)")
            tx.execute("INSERT INTO local_effect VALUES (1)")
            raise RuntimeError("fallo local")
        with self.assertRaises(RuntimeError):
            self.receive_with(boom)
        self.assertEqual(self.inbox_rows(), 0)
        self.assertEqual(self.sql("SELECT name FROM sqlite_master WHERE name='local_effect'"), [])

    def test_transaction_control_is_denied_even_when_disguised(self):
        attempts = ["COMMIT", "commit", "END", "ROLLBACK", "BEGIN", "BEGIN IMMEDIATE", "SAVEPOINT s", "/* x */ COMMIT", "-- x\nCOMMIT",
                    "PRAGMA journal_mode=DELETE", "ATTACH DATABASE ':memory:' AS other", "DETACH DATABASE other"]
        for stmt in attempts:
            with self.subTest(stmt=stmt):
                with self.assertRaises(sqlite3.DatabaseError):
                    self.receive_with(lambda tx, msg, s=stmt: tx.execute(s))
                self.assertEqual(self.inbox_rows(), 0, "la sentencia denegada no deja la fila del inbox confirmada")

    def test_a_processor_that_tries_to_commit_cannot_persist_before_failing(self):
        def sneaky(tx, msg):
            tx.execute("CREATE TABLE local_effect(x)")
            try:
                tx.execute("COMMIT")
            except sqlite3.DatabaseError:
                pass
            raise RuntimeError("fallo después del intento de commit")
        with self.assertRaises(RuntimeError):
            self.receive_with(sneaky)
        self.assertEqual(self.inbox_rows(), 0)
        self.assertEqual(self.sql("SELECT name FROM sqlite_master WHERE name='local_effect'"), [])

    def test_the_authorizer_is_removed_after_the_processor_even_on_failure(self):
        with self.assertRaises(sqlite3.DatabaseError):
            self.receive_with(lambda tx, msg: tx.execute("COMMIT"))
        self.assertEqual(self.receive_with(None).status, "ACCEPTED")           # receive posterior sin processor: usa BEGIN/COMMIT internos
        m2 = op_request()
        self.assertEqual(self.receive_with(lambda tx, msg: tx.execute("SELECT 1").fetchone(), m2).status, "ACCEPTED")
        self.assertEqual(self.inbox_rows(), 2)


if __name__ == "__main__":
    unittest.main()
