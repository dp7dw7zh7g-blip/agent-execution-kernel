import tempfile
from contextlib import closing
import unittest
from pathlib import Path

from unae import Store, CommitStatus
from unae.store import CommitRequest


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "unae.sqlite")
        self.s = Store(self.db)
        self.s.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 4)
        self.s.configure_account("task-1", 1000, 100000, 10_000_000, 4)
        self.s.create_task("task-1", {"n": 0})

    def tearDown(self):
        self.s.close()                  # libera la conexión ancla (keep_open): en Windows impide borrar el .sqlite
        self.tmp.cleanup()

    def reserve(self):
        return self.s.reserve_and_register(task_id="task-1", operation_id="op-1", reservation_id="res-1",
            idempotency_key="idem-1", fencing_token=1, compute_units=10, time_ms=100,
            memory_bytes=1024, account_ids=("__GLOBAL__",))

    def test_reservation_idempotency_and_conflict(self):
        self.assertEqual(self.reserve(), "RESERVED")
        self.assertEqual(self.reserve(), "ALREADY_RESERVED")
        conflict = self.s.reserve_and_register(task_id="task-1", operation_id="op-x", reservation_id="res-x",
            idempotency_key="idem-1", fencing_token=1, compute_units=11, time_ms=100,
            memory_bytes=1024, account_ids=("__GLOBAL__",))
        self.assertEqual(conflict, "KEY_CONFLICT")

    def test_recovery_preserves_unknown_and_resources(self):
        self.assertEqual(self.reserve(), "RESERVED")
        self.assertTrue(self.s.mark_running("op-1", 1, "provider-1"))
        self.assertEqual(self.s.recover_interrupted(), 1)
        with closing(self.s.connect()) as c:
            row = c.execute("SELECT status FROM execution_journal WHERE operation_id='op-1'").fetchone()
            account = c.execute("SELECT reserved_compute,active_memory_bytes FROM budget_accounts WHERE account_id='__GLOBAL__'").fetchone()
        self.assertEqual(row[0], "UNKNOWN")
        self.assertEqual(tuple(account), (10, 1024))

    def test_atomic_commit_and_duplicate(self):
        self.assertEqual(self.reserve(), "RESERVED")
        self.assertTrue(self.s.mark_running("op-1", 1, "provider-1"))
        self.assertEqual(self.s.record_receipt(receipt_id="receipt-1", operation_id="op-1", reservation_id="res-1",
            outcome="SUCCEEDED", actual_compute=8, actual_time_ms=80, peak_memory_bytes=900,
            provider_operation_id="provider-1"), "RECORDED")
        req = CommitRequest("commit-1", "task-1", "op-1", "res-1", "receipt-1", 0, 1, {"n": 1}, "event-1")
        self.assertEqual(self.s.coordinated_commit(req), CommitStatus.COMMITTED)
        self.assertEqual(self.s.coordinated_commit(req), CommitStatus.ALREADY_COMMITTED)
        with closing(self.s.connect()) as c:
            state = c.execute("SELECT version,payload FROM task_state WHERE task_id='task-1'").fetchone()
            account = c.execute("SELECT compute_used,reserved_compute,active_memory_bytes,active_count FROM budget_accounts WHERE account_id='__GLOBAL__'").fetchone()
            outbox = c.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], '{"n":1}')
        self.assertEqual(tuple(account), (8, 0, 0, 0))
        self.assertEqual(outbox, 1)

    def test_stale_authority_rejected(self):
        self.assertEqual(self.reserve(), "RESERVED")
        self.assertTrue(self.s.mark_running("op-1", 1, "provider-1"))
        self.assertEqual(self.s.record_receipt(receipt_id="receipt-1", operation_id="op-1", reservation_id="res-1",
            outcome="SUCCEEDED", actual_compute=8, actual_time_ms=80, peak_memory_bytes=900,
            provider_operation_id="provider-1"), "RECORDED")
        self.s.acquire_authority("task-1", "worker-2")
        req = CommitRequest("commit-1", "task-1", "op-1", "res-1", "receipt-1", 0, 1, {"n": 1}, "event-1")
        self.assertEqual(self.s.coordinated_commit(req), CommitStatus.STALE_AUTHORITY)
        with closing(self.s.connect()) as c:
            self.assertEqual(c.execute("SELECT version FROM task_state WHERE task_id='task-1'").fetchone()[0], 0)


    def test_idempotency_key_rejects_different_operation_identity(self):
        self.assertEqual(self.reserve(), "RESERVED")
        result = self.s.reserve_and_register(
            task_id="task-1", operation_id="op-2", reservation_id="res-2",
            idempotency_key="idem-1", fencing_token=1, compute_units=10,
            time_ms=100, memory_bytes=1024, account_ids=("__GLOBAL__",),
        )
        self.assertEqual(result, "KEY_CONFLICT")
        with closing(self.s.connect()) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM execution_journal").fetchone()[0], 1)
            self.assertEqual(c.execute("SELECT reserved_compute FROM budget_accounts WHERE account_id='__GLOBAL__'").fetchone()[0], 10)

    def test_receipt_must_include_matching_provider_operation_id(self):
        self.assertEqual(self.reserve(), "RESERVED")
        self.assertTrue(self.s.mark_running("op-1", 1, "provider-1"))
        result = self.s.record_receipt(
            receipt_id="receipt-missing-provider-id", operation_id="op-1", reservation_id="res-1",
            outcome="SUCCEEDED", actual_compute=8, actual_time_ms=80,
            peak_memory_bytes=900, provider_operation_id=None,
        )
        self.assertEqual(result, "INVALID_RECEIPT")
        with closing(self.s.connect()) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM receipts").fetchone()[0], 0)
            self.assertEqual(c.execute("SELECT status FROM execution_journal WHERE operation_id='op-1'").fetchone()[0], "RUNNING")

    def test_receipt_with_mismatched_provider_operation_id_rejected(self):
        self.assertEqual(self.reserve(), "RESERVED")
        self.assertTrue(self.s.mark_running("op-1", 1, "provider-1"))
        result = self.s.record_receipt(
            receipt_id="receipt-wrong-provider-id", operation_id="op-1", reservation_id="res-1",
            outcome="SUCCEEDED", actual_compute=8, actual_time_ms=80,
            peak_memory_bytes=900, provider_operation_id="provider-OTHER",
        )
        self.assertEqual(result, "INVALID_RECEIPT")


if __name__ == "__main__":
    unittest.main()
