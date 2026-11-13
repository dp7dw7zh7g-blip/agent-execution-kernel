import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from unae import Store, CommitStatus
from unae.store import CommitRequest


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = Store(str(Path(self.tmp.name) / "u.sqlite"))
        self.s.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 4)
        self.s.configure_account("task-1", 1000, 100000, 10_000_000, 4)
        self.s.create_task("task-1", {"n": 0})

    def tearDown(self):
        self.s.close()                  # libera la conexión ancla (keep_open): en Windows impide borrar el .sqlite
        self.tmp.cleanup()

    def reserve(self, token=1, op="op-1", rid="res-1", key="k1"):
        return self.s.reserve_and_register(task_id="task-1", operation_id=op, reservation_id=rid, idempotency_key=key,
            fencing_token=token, compute_units=10, time_ms=100, memory_bytes=1024, account_ids=("__GLOBAL__",))

    def counters(self):
        with closing(self.s.connect()) as c:
            return tuple(c.execute("SELECT reserved_compute,reserved_time_ms,active_memory_bytes,active_count FROM budget_accounts WHERE account_id='__GLOBAL__'").fetchone())

    def status(self, op="op-1"):
        with closing(self.s.connect()) as c:
            return c.execute("SELECT status FROM execution_journal WHERE operation_id=?", (op,)).fetchone()[0]

    def test_release_reserved_frees_budget_once(self):
        self.assertEqual(self.reserve(), "RESERVED")
        self.assertEqual(self.counters(), (10, 100, 1024, 1))
        self.assertTrue(self.s.release_reservation("op-1", 1))
        self.assertEqual(self.counters(), (0, 0, 0, 0))
        self.assertEqual(self.status(), "CANCELLED")
        self.assertFalse(self.s.release_reservation("op-1", 1))
        self.assertEqual(self.counters(), (0, 0, 0, 0))

    def test_release_refuses_running_and_stale_token(self):
        self.reserve(); self.s.mark_running("op-1", 1, "p1")
        self.assertFalse(self.s.release_reservation("op-1", 1))
        self.assertEqual(self.counters(), (10, 100, 1024, 1))
        self.reserve(op="op-2", rid="res-2", key="k2")
        self.s.acquire_authority("task-1", "w2")
        self.assertEqual(self.status("op-2"), "CANCELLED")

    def test_authority_change_cancels_reserved_and_unknowns_running(self):
        self.reserve(); self.reserve(op="op-2", rid="res-2", key="k2")
        self.s.mark_running("op-1", 1, "p1")
        token = self.s.acquire_authority("task-1", "w2")
        self.assertEqual(token, 2)
        self.assertEqual(self.status("op-1"), "UNKNOWN")
        self.assertEqual(self.status("op-2"), "CANCELLED")
        self.assertEqual(self.counters(), (10, 100, 1024, 1))  # solo la RUNNING conserva recursos

    def test_full_recovery_after_authority_change(self):
        self.reserve(); self.s.mark_running("op-1", 1, "p1")
        token = self.s.acquire_authority("task-1", "w2")
        self.assertFalse(self.s.begin_reconciliation("op-1", 1))      # token viejo
        self.assertTrue(self.s.begin_reconciliation("op-1", token))
        self.assertFalse(self.s.begin_reconciliation("op-1", token))  # ya no está UNKNOWN
        self.assertEqual(self.s.record_receipt(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED",
            actual_compute=5, actual_time_ms=50, peak_memory_bytes=512, provider_operation_id="p1"), "RECORDED")
        st = self.s.coordinated_commit(CommitRequest("c1", "task-1", "op-1", "res-1", "r1", 0, token, {"n": 1}, "e1"))
        self.assertEqual(st, CommitStatus.COMMITTED)
        self.assertEqual(self.counters(), (0, 0, 0, 0))

    def test_new_authority_can_reserve_after_cleanup(self):
        self.reserve(); token = self.s.acquire_authority("task-1", "w2")
        self.assertEqual(self.reserve(token=token, op="op-n", rid="res-n", key="kn"), "RESERVED")
        self.assertEqual(self.counters(), (10, 100, 1024, 1))

    def test_duplicate_operation_id_returns_status_not_exception(self):
        self.reserve()
        self.assertEqual(self.reserve(key="k2", rid="res-2"), "OPERATION_CONFLICT")   # mismo operation_id
        self.assertEqual(self.reserve(op="op-2", key="k3"), "OPERATION_CONFLICT")     # mismo reservation_id
        self.assertEqual(self.counters(), (10, 100, 1024, 1))

    def test_expire_only_old_reserved(self):
        self.reserve(); self.reserve(op="op-2", rid="res-2", key="k2"); self.s.mark_running("op-2", 1, "p2")
        import time
        self.assertEqual(self.s.expire_reservations(60), 0)                       # aún recientes
        self.assertEqual(self.s.expire_reservations(60, now=time.time() + 120), 1)  # solo la RESERVED
        self.assertEqual(self.status("op-1"), "CANCELLED")
        self.assertEqual(self.status("op-2"), "RUNNING")
        self.assertEqual(self.counters(), (10, 100, 1024, 1))
        with self.assertRaises(ValueError):
            self.s.expire_reservations(0)

    def test_overrun_is_recorded_visible_and_blocks_new_reservations(self):
        self.reserve(); self.s.mark_running("op-1", 1, "p1")
        self.s.record_receipt(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED",
            actual_compute=1500, actual_time_ms=50, peak_memory_bytes=512, provider_operation_id="p1")
        st = self.s.coordinated_commit(CommitRequest("c1", "task-1", "op-1", "res-1", "r1", 0, 1, {"n": 1}, "e1"))
        self.assertEqual(st, CommitStatus.COMMITTED)                       # la acción ya ocurrió: se acepta
        over = self.s.list_overruns("op-1")
        self.assertEqual(len(over), 1); self.assertEqual(over[0]["compute_overrun"], 1490)
        self.assertEqual(self.s.list_overruns("nadie"), [])
        self.assertEqual(self.s.account_headroom("__GLOBAL__")["compute"], -500)   # límite 1000, usado 1500
        self.assertEqual(self.reserve(op="op-2", rid="res-2", key="k2"), "REJECTED_BUDGET")

if __name__ == "__main__":
    unittest.main()
