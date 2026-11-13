import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from unae import Store
from unae.store import fingerprint


class IdempotentRetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = Store(str(Path(self.tmp.name) / "u.sqlite"))
        self.s.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 8)
        self.s.configure_account("task-1", 1000, 100000, 10_000_000, 8)
        self.s.create_task("task-1", {"n": 0})

    def tearDown(self):
        self.s.close(); self.tmp.cleanup()

    def reserve(self, token=1, op="op-1", rid="res-1", key="k1", cu=10):
        return self.s.reserve_and_register(task_id="task-1", operation_id=op, reservation_id=rid, idempotency_key=key,
                                           fencing_token=token, compute_units=cu, time_ms=100, memory_bytes=1024)

    def held(self):
        with closing(self.s.connect()) as c:
            return c.execute("SELECT reserved_compute,active_count FROM budget_accounts WHERE account_id='__GLOBAL__'").fetchone()[:]

    def test_same_token_retry_is_unchanged_then_cancelled_after_release(self):
        self.assertEqual(self.reserve(), "RESERVED")
        self.assertEqual(self.reserve(), "ALREADY_RESERVED")
        self.assertTrue(self.s.release_reservation("op-1", 1))
        self.assertEqual(self.reserve(), "CANCELLED")                 # ya no dice "reservada"
        self.assertEqual(self.held(), (0, 0))

    def test_new_owner_retry_of_fenced_out_request_reports_cancelled_and_new_key_works(self):
        self.assertEqual(self.reserve(), "RESERVED")
        t = self.s.acquire_authority("task-1", "w2")                  # fencing cancela la RESERVED del dueño anterior
        self.assertEqual(self.reserve(token=t), "CANCELLED")          # antes: KEY_CONFLICT
        self.assertEqual(self.reserve(token=t, op="op-2", rid="res-2", key="k2"), "RESERVED")
        self.assertEqual(self.held(), (10, 1))

    def test_new_owner_retry_of_running_request_sees_existing_reservation(self):
        self.reserve(); self.s.mark_running("op-1", 1, "p1")
        t = self.s.acquire_authority("task-1", "w2")                  # RUNNING -> UNKNOWN, conserva recursos
        self.assertEqual(self.reserve(token=t), "ALREADY_RESERVED")   # hay que reconciliarla, no duplicarla
        self.assertEqual(self.held(), (10, 1))

    def test_old_owner_retry_after_rotation_is_stale(self):
        self.reserve(); self.s.mark_running("op-1", 1, "p1")
        self.s.acquire_authority("task-1", "w2")
        self.assertEqual(self.reserve(token=1), "STALE_AUTHORITY")

    def test_different_request_same_key_still_conflicts_regardless_of_token(self):
        self.reserve()
        t = self.s.acquire_authority("task-1", "w2")
        self.assertEqual(self.reserve(token=t, cu=11), "KEY_CONFLICT")
        self.assertEqual(self.reserve(token=1, cu=11), "KEY_CONFLICT")

    def test_rows_saved_with_legacy_4_3N_fingerprint_are_still_recognised(self):
        self.reserve()
        legacy = fingerprint({"task_id": "task-1", "operation_id": "op-1", "reservation_id": "res-1", "idempotency_key": "k1",
                              "fencing_token": 1, "compute_units": 10, "time_ms": 100, "memory_bytes": 1024,
                              "account_ids": ["__GLOBAL__", "task-1"]})
        with closing(self.s.connect()) as c:
            c.execute("UPDATE execution_journal SET request_fingerprint=? WHERE operation_id='op-1'", (legacy,))
        self.assertEqual(self.reserve(), "ALREADY_RESERVED")


if __name__ == "__main__":
    unittest.main()
