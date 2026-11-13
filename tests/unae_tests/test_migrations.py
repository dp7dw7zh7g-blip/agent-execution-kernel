import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from unae import Store
from unae.store import SCHEMA_VERSION, CommitRequest


def _cols(store, table):
    with closing(store.connect()) as c:
        return [r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "u.sqlite")

    def tearDown(self):
        self.tmp.cleanup()

    def test_fresh_db_is_current(self):
        s = Store(self.db)
        self.assertEqual(s.schema_version(), SCHEMA_VERSION)
        self.assertIn("claimed_by", _cols(s, "outbox"))
        Store(self.db)                                   # reabrir es idempotente
        self.assertEqual(s.schema_version(), SCHEMA_VERSION)

    def test_legacy_v1_db_is_migrated_and_keeps_data(self):
        s = Store(self.db)
        s.configure_account("__GLOBAL__", 100, 1000, 1000, 4); s.configure_account("task-1", 100, 1000, 1000, 4)
        s.create_task("task-1", {"n": 0})
        s.reserve_and_register(task_id="task-1", operation_id="op-1", reservation_id="res-1", idempotency_key="k1",
                               fencing_token=1, compute_units=1, time_ms=1, memory_bytes=1)
        s.mark_running("op-1", 1, "p1")
        s.record_receipt(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED",
                         actual_compute=1, actual_time_ms=1, peak_memory_bytes=1, provider_operation_id="p1")
        s.coordinated_commit(CommitRequest("c1", "task-1", "op-1", "res-1", "r1", 0, 1, {"n": 1}, "e1"))
        with closing(s.connect()) as c:                  # simular base creada por 4.3N
            c.execute("ALTER TABLE outbox DROP COLUMN claimed_by"); c.execute("ALTER TABLE outbox DROP COLUMN claim_expires_at")
            c.execute("ALTER TABLE execution_journal DROP COLUMN provider_id")
            c.execute("ALTER TABLE receipts DROP COLUMN provider_id"); c.execute("ALTER TABLE receipts DROP COLUMN signature")
            for col in ("outcome", "attestation", "via_reconciliation"):
                c.execute(f"ALTER TABLE commit_log DROP COLUMN {col}")
            c.execute("DROP TRIGGER trg_journal_fsm"); c.execute("DROP TRIGGER trg_reservation_fsm")
            c.execute("UPDATE schema_version SET version=1")
        self.assertNotIn("claimed_by", _cols(s, "outbox"))
        s2 = Store(self.db)
        self.assertEqual(s2.schema_version(), SCHEMA_VERSION)
        self.assertIn("claim_expires_at", _cols(s2, "outbox"))
        self.assertIn("signature", _cols(s2, "receipts")); self.assertIn("provider_id", _cols(s2, "execution_journal"))
        self.assertEqual([e["event_id"] for e in s2.pending_events()], ["e1"])
        self.assertEqual(s2.dispatch_outbox(lambda e: None)["sent"], 1)

    def test_newer_db_is_refused_and_untouched(self):
        s = Store(self.db)
        with closing(s.connect()) as c:
            c.execute("UPDATE schema_version SET version=?", (SCHEMA_VERSION + 1,))
        with self.assertRaises(RuntimeError):
            Store(self.db)
        with closing(s.connect()) as c:
            self.assertEqual(c.execute("SELECT version FROM schema_version").fetchone()[0], SCHEMA_VERSION + 1)


if __name__ == "__main__":
    unittest.main()
