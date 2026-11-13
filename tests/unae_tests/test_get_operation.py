"""Lectura pública `Store.get_operation` (parche 0004, D-36): sustituye al SELECT directo sobre `execution_journal` que hacía el puente."""
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from unae import Store


class GetOperationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.s = Store(str(Path(self.tmp.name) / "u.sqlite"), keyring={"prov-A": b"secret-A" * 4})
        self.s.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 4)
        self.s.configure_account("task-1", 1000, 100000, 10_000_000, 4)
        self.s.create_task("task-1", {"n": 0})
        self.addCleanup(self.s.close)

    def reserve(self, op="op-1", res="res-1", key="k1"):
        self.assertEqual(self.s.reserve_and_register(task_id="task-1", operation_id=op, reservation_id=res, idempotency_key=key,
                         fencing_token=1, compute_units=10, time_ms=10, memory_bytes=10), "RESERVED")

    def test_unknown_operation_is_none(self):
        for missing in ("no-existe", "", " op-1", "OP-1"):
            self.assertIsNone(self.s.get_operation(missing), missing)

    def test_non_string_is_a_type_error(self):
        for bad in (None, 5, ["op-1"], b"op-1"):
            with self.assertRaises(TypeError):
                self.s.get_operation(bad)  # type: ignore[arg-type]

    def test_follows_the_operation_through_its_states(self):
        self.reserve()
        got = self.s.get_operation("op-1")
        self.assertEqual({k: got[k] for k in ("operation_id", "task_id", "reservation_id", "idempotency_key", "status", "fencing_token")},
                         {"operation_id": "op-1", "task_id": "task-1", "reservation_id": "res-1", "idempotency_key": "k1",
                          "status": "RESERVED", "fencing_token": 1})
        self.assertIsNone(got["provider_operation_id"])
        self.assertEqual(got["recovery_count"], 0)
        self.assertTrue(self.s.mark_running("op-1", 1, "p-1", provider_id="prov-A"))
        got = self.s.get_operation("op-1")
        self.assertEqual((got["status"], got["provider_operation_id"]), ("RUNNING", "p-1"))
        self.assertTrue(self.s.mark_unknown("op-1"))
        self.assertEqual(self.s.get_operation("op-1")["status"], "UNKNOWN")
        self.assertEqual(self.s.recover_interrupted(), 0)

    def test_recovery_count_is_exposed(self):
        self.reserve()
        self.s.mark_running("op-1", 1, "p-1", provider_id="prov-A")
        self.assertEqual(self.s.recover_interrupted(), 1)
        got = self.s.get_operation("op-1")
        self.assertEqual((got["status"], got["recovery_count"]), ("UNKNOWN", 1))

    def test_returns_a_copy_and_does_not_write(self):
        self.reserve()
        before = self.s.get_operation("op-1")
        before["status"] = "SUCCEEDED"
        before["fencing_token"] = 99
        with closing(self.s.connect()) as c:
            raw_before = tuple(c.execute("SELECT * FROM execution_journal").fetchone())
        for _ in range(3):
            self.s.get_operation("op-1")
        with closing(self.s.connect()) as c:
            raw_after = tuple(c.execute("SELECT * FROM execution_journal").fetchone())
        self.assertEqual(raw_before, raw_after)  # incluye updated_at: no hubo escritura
        self.assertEqual(self.s.get_operation("op-1")["status"], "RESERVED")

    def test_agrees_with_the_journal_for_every_operation(self):
        for i in range(3):
            self.reserve(op=f"op-{i}", res=f"res-{i}", key=f"k{i}")
        with closing(self.s.connect()) as c:
            rows = c.execute("SELECT operation_id,status,fencing_token FROM execution_journal ORDER BY operation_id").fetchall()
        self.assertEqual([(r["operation_id"], r["status"], r["fencing_token"]) for r in rows],
                         [(o["operation_id"], o["status"], o["fencing_token"]) for o in (self.s.get_operation(f"op-{i}") for i in range(3))])


if __name__ == "__main__":
    unittest.main()
