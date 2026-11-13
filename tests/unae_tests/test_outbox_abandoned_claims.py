"""Parche 0015 (D-66), hallazgo I-2: un evento que mata al worker no contaba intentos.

Los intentos solo subían en `mark_event_failed`, dentro de `except Exception`. Si el sender termina el proceso (OOM, os._exit, fallo de una
librería nativa) el lease vence, otro worker reclama el evento y vuelve a morir: un bucle de caídas sin fin, sin llegar nunca a DEAD.
Ahora reclamar un claim vencido y abandonado cuenta como un intento y, al llegar a max_attempts, el evento pasa a DEAD.
Sin el parche 0015 estas pruebas FALLAN (el evento sigue PENDING y reclamable para siempre).
"""
import tempfile
import time
import unittest
from pathlib import Path

from unae import Store, CommitStatus
from unae.store import CommitRequest


class AbandonedClaimsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = Store(str(Path(self.tmp.name) / "u.sqlite"))
        self.s.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 4)
        self.s.configure_account("task-1", 1000, 100000, 10_000_000, 4)
        self.s.create_task("task-1", {"n": 0})

    def tearDown(self):
        self.s.close()
        self.tmp.cleanup()

    def commit(self, n: int):
        op, res, rc = f"op-{n}", f"res-{n}", f"r{n}"
        self.assertEqual(self.s.reserve_and_register(task_id="task-1", operation_id=op, reservation_id=res, idempotency_key=f"k{n}",
            fencing_token=1, compute_units=1, time_ms=1, memory_bytes=1), "RESERVED")
        self.s.mark_running(op, 1, f"p{n}")
        self.s.record_receipt(receipt_id=rc, operation_id=op, reservation_id=res, outcome="SUCCEEDED",
            actual_compute=1, actual_time_ms=1, peak_memory_bytes=1, provider_operation_id=f"p{n}")
        st = self.s.coordinated_commit(CommitRequest(f"c{n}", "task-1", op, res, rc, n - 1, 1, {"n": n}, f"e{n}"))
        self.assertEqual(st, CommitStatus.COMMITTED)

    def test_a_worker_that_keeps_dying_leads_the_event_to_dead(self):
        self.commit(1)
        t = time.time()
        for i in range(1, 6):  # max_attempts=5: el evento se entrega a cinco workers (intentos previos 0…4), igual que con mark_event_failed
            got = self.s.claim_events(f"w{i}", lease_s=10, now=t, max_attempts=5)
            self.assertEqual([e["event_id"] for e in got], ["e1"], f"reclamo {i}")
            self.assertEqual(got[0]["attempts"], i - 1)  # el primer reclamo no es un abandono; los siguientes sí
            t += 100  # el worker murió: no hubo mark_event_sent ni mark_event_failed y el lease venció
        self.assertEqual(self.s.claim_events("w6", lease_s=10, now=t, max_attempts=5), [])  # el quinto abandono lo manda a DEAD
        self.assertEqual([e["event_id"] for e in self.s.dead_events()], ["e1"])
        self.assertEqual(self.s.dead_events()[0]["attempts"], 5)
        self.assertEqual(self.s.claim_events("w7", lease_s=10, now=t + 100, max_attempts=5), [])  # ya no se reclama
        self.assertEqual(self.s.pending_events(), [])

    def test_dispatch_outbox_applies_its_max_attempts_to_abandoned_claims(self):
        self.commit(1)
        t0 = time.time() - 1000
        self.assertEqual(len(self.s.claim_events("dead-1", lease_s=10, now=t0)), 1)          # el primer worker muere con el evento reclamado
        self.assertEqual(len(self.s.claim_events("dead-2", lease_s=10, now=t0 + 100)), 1)    # el segundo también (1 intento contado)
        seen = []
        res = self.s.dispatch_outbox(seen.append, max_attempts=2, worker_id="healthy")       # ambos leases ya vencieron en tiempo real
        self.assertEqual(seen, [])                                                             # el tercer reclamo agota max_attempts=2: no se envía
        self.assertEqual(res, {"sent": 0, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual([e["event_id"] for e in self.s.dead_events()], ["e1"])

    def test_a_first_claim_and_a_released_claim_do_not_count(self):
        self.commit(1)
        got = self.s.claim_events("w1", lease_s=30)
        self.assertEqual(got[0]["attempts"], 0)                                   # el primer reclamo no es un intento
        self.assertEqual(self.s.release_events(["e1"], "w1"), 1)                   # liberar a tiempo no es abandono
        got = self.s.claim_events("w2", lease_s=30)
        self.assertEqual(got[0]["attempts"], 0)

    def test_a_healthy_send_after_one_abandonment_still_succeeds(self):
        self.commit(1)
        t = time.time()
        self.s.claim_events("w1", lease_s=10, now=t)
        got = self.s.claim_events("w2", lease_s=30, now=t + 100)                   # w1 murió; w2 lo retoma (1 intento contado)
        self.assertEqual(got[0]["attempts"], 1)
        self.assertTrue(self.s.mark_event_sent("e1", "w2"))
        self.assertEqual(self.s.pending_events(), [])

    def test_claim_events_validates_max_attempts(self):
        for bad in (0, -1, True, "5", 2.5):
            with self.assertRaises(ValueError):
                self.s.claim_events("w1", max_attempts=bad)

    def test_event_shape_is_unchanged(self):
        self.commit(1)
        got = self.s.claim_events("w1")
        self.assertEqual(sorted(got[0]), ["attempts", "commit_id", "event_id", "event_type", "payload"])


if __name__ == "__main__":
    unittest.main()
