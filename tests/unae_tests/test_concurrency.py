"""Multiprocess contention tests for the UNAE reference package.

Los workers arrancan con spawn, abren su conexión y esperan en una Barrier antes de reservar,
así las transacciones compiten de verdad en lugar de ejecutarse una tras otra por la latencia de arranque.
"""
import multiprocessing as mp
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from unae import Store

ROUNDS = 5


def _reserve_worker(db_path, task_id, operation_id, reservation_id, idempotency_key,
                    compute_units, barrier, queue):
    try:
        store = Store(db_path)
        barrier.wait(timeout=20)
        result = store.reserve_and_register(
            task_id=task_id, operation_id=operation_id, reservation_id=reservation_id,
            idempotency_key=idempotency_key, fencing_token=1, compute_units=compute_units,
            time_ms=10, memory_bytes=10, account_ids=("__GLOBAL__",),
        )
        queue.put((operation_id, result, None))
    except Exception as exc:  # surface child-process failures to the test
        queue.put((operation_id, None, repr(exc)))


class MultiprocessReservationTests(unittest.TestCase):
    def _fresh_db(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = str(Path(tmp.name) / "unae.sqlite")
        store = Store(db)
        store.configure_account("__GLOBAL__", 10, 1000, 1000, 8)
        store.configure_account("task-1", 10, 1000, 1000, 8)
        store.create_task("task-1", {"n": 0})
        return db

    def _run_pair(self, db, compute_units=7, same_key=False):
        ctx = mp.get_context("spawn")
        queue, barrier = ctx.Queue(), ctx.Barrier(2)
        if same_key:
            specs = [("op-1", "res-1", "idem-1"), ("op-1", "res-1", "idem-1")]
        else:
            specs = [("op-1", "res-1", "idem-1"), ("op-2", "res-2", "idem-2")]
        processes = [ctx.Process(target=_reserve_worker,
                                 args=(db, "task-1", op, res, key, compute_units, barrier, queue))
                     for op, res, key in specs]
        for process in processes:
            process.start()
        results = [queue.get(timeout=30) for _ in processes]
        for process in processes:
            process.join(timeout=20)
            self.assertFalse(process.is_alive(), "worker did not terminate")
            self.assertEqual(process.exitcode, 0)
        errors = [row for row in results if row[2] is not None]
        self.assertEqual(errors, [], f"child process errors: {errors}")
        return sorted(result for _, result, _ in results)

    def _ledger(self, db):
        with closing(Store(db).connect()) as conn:
            reserved = conn.execute("SELECT reserved_compute FROM budget_accounts WHERE account_id='__GLOBAL__'").fetchone()[0]
            count = conn.execute("SELECT COUNT(*) FROM reservations").fetchone()[0]
        return reserved, count

    def test_concurrent_reservations_do_not_overbook_compute(self):
        for i in range(ROUNDS):
            with self.subTest(round=i):
                db = self._fresh_db()
                self.assertEqual(self._run_pair(db, 7, same_key=False), ["REJECTED_BUDGET", "RESERVED"])
                self.assertEqual(self._ledger(db), (7, 1))

    def test_concurrent_same_idempotency_request_is_reserved_once(self):
        for i in range(ROUNDS):
            with self.subTest(round=i):
                db = self._fresh_db()
                self.assertEqual(self._run_pair(db, 7, same_key=True), ["ALREADY_RESERVED", "RESERVED"])
                self.assertEqual(self._ledger(db), (7, 1))


if __name__ == "__main__":
    unittest.main()
