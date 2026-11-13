import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from unae import Store, CommitStatus
from unae.store import CommitRequest


class OutboxTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = Store(str(Path(self.tmp.name) / "u.sqlite"))
        self.s.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 4)
        self.s.configure_account("task-1", 1000, 100000, 10_000_000, 4)
        self.s.create_task("task-1", {"n": 0})

    def tearDown(self):
        self.s.close()                  # libera la conexión ancla (keep_open): en Windows impide borrar el .sqlite
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

    def test_dispatch_sends_once_in_order(self):
        self.commit(1); self.commit(2)
        seen = []
        self.assertEqual(self.s.dispatch_outbox(seen.append), {"sent": 2, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual([e["event_id"] for e in seen], ["e1", "e2"])
        self.assertEqual(seen[1]["payload"], {"task_id": "task-1", "version": 2})
        self.assertEqual(self.s.dispatch_outbox(seen.append), {"sent": 0, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual(self.s.pending_events(), [])

    def test_failures_retry_then_go_dead(self):
        self.commit(1)
        def boom(event): raise RuntimeError("down")
        self.assertEqual(self.s.dispatch_outbox(boom, max_attempts=3), {"sent": 0, "failed": 1, "dead": 0, "lost": 0})
        self.assertEqual(self.s.dispatch_outbox(boom, max_attempts=3), {"sent": 0, "failed": 1, "dead": 0, "lost": 0})
        self.assertEqual(self.s.dispatch_outbox(boom, max_attempts=3), {"sent": 0, "failed": 0, "dead": 1, "lost": 0})
        self.assertEqual(self.s.pending_events(), [])          # DEAD ya no se reenvía
        self.assertIsNone(self.s.mark_event_failed("e1"))
        self.assertFalse(self.s.mark_event_sent("e1"))

    def test_recovers_after_transient_failure(self):
        self.commit(1)
        calls = {"n": 0}
        def flaky(event):
            calls["n"] += 1
            if calls["n"] == 1: raise RuntimeError("blip")
        self.assertEqual(self.s.dispatch_outbox(flaky)["failed"], 1)
        self.assertEqual(self.s.dispatch_outbox(flaky)["sent"], 1)

    def test_consume_once_dedupes_and_rolls_back_on_error(self):
        self.commit(1)
        with closing(self.s.connect()) as c:
            c.execute("CREATE TABLE IF NOT EXISTS effects(event_id TEXT)"); c.commit()
        def apply(conn, event_id): conn.execute("INSERT INTO effects VALUES(?)", (event_id,))
        def bad(conn, event_id):
            conn.execute("INSERT INTO effects VALUES(?)", ("LEAK",)); raise RuntimeError("x")
        with self.assertRaises(RuntimeError):
            self.s.consume_once("c1", "e1", bad)
        self.assertTrue(self.s.consume_once("c1", "e1", apply))
        self.assertFalse(self.s.consume_once("c1", "e1", apply))      # duplicado
        self.assertTrue(self.s.consume_once("c2", "e1", apply))       # otro consumidor sí
        with closing(self.s.connect()) as c:
            rows = [r[0] for r in c.execute("SELECT event_id FROM effects").fetchall()]
        self.assertEqual(rows, ["e1", "e1"])                           # sin "LEAK"

    def test_lease_blocks_second_worker_until_expiry(self):
        import time
        self.commit(1)
        self.assertEqual([e["event_id"] for e in self.s.claim_events("w1", lease_s=30)], ["e1"])
        self.assertEqual(self.s.claim_events("w2"), [])                                  # reclamado por w1
        self.assertEqual([e["event_id"] for e in self.s.claim_events("w2", now=time.time() + 100)], ["e1"])  # lease vencido
        self.assertFalse(self.s.mark_event_sent("e1", worker_id="w1"))                   # w1 perdió el claim
        self.assertTrue(self.s.mark_event_sent("e1", worker_id="w2"))

    def test_concurrent_dispatchers_do_not_double_send(self):
        self.commit(1)
        seen_inner, inner = [], []
        def outer_sender(event):
            inner.append(self.s.dispatch_outbox(seen_inner.append, worker_id="w2"))     # otro worker corre durante el envío
        self.assertEqual(self.s.dispatch_outbox(outer_sender, worker_id="w1")["sent"], 1)
        self.assertEqual(inner[0]["sent"], 0)
        self.assertEqual(seen_inner, [])

    def test_failure_releases_claim_for_retry(self):
        self.commit(1)
        def boom(event): raise RuntimeError("down")
        self.s.dispatch_outbox(boom, worker_id="w1")
        self.assertEqual([e["event_id"] for e in self.s.claim_events("w2")], ["e1"])

    def test_lease_lost_is_reported(self):
        import time
        self.commit(1)
        def slow(event):                                                                 # w1 tarda; w2 toma el evento vencido
            self.s.claim_events("w2", now=time.time() + 100)
        self.assertEqual(self.s.dispatch_outbox(slow, worker_id="w1", lease_s=30)["lost"], 1)


    # ---- Pausa lógica (nivel 1): detener el despacho no es un fallo y no gasta intentos ----

    def attempts(self, event_id: str) -> int:
        return next(e["attempts"] for e in self.s.pending_events() if e["event_id"] == event_id)

    def test_paused_before_claim_touches_nothing(self):
        self.commit(1)
        sent = []
        self.assertEqual(self.s.dispatch_outbox(sent.append, paused=lambda: True), {"sent": 0, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual(sent, [])
        self.assertEqual(self.attempts("e1"), 0)
        self.assertEqual([e["event_id"] for e in self.s.claim_events("w2")], ["e1"])     # ni siquiera quedó reclamado

    def test_pause_does_not_burn_attempts_even_with_max_attempts_1(self):
        self.commit(1)
        for _ in range(10):
            self.s.dispatch_outbox(lambda e: None, max_attempts=1, paused=lambda: True)
        self.assertEqual(self.attempts("e1"), 0)
        self.assertEqual(self.s.dispatch_outbox(lambda e: None, max_attempts=1, paused=lambda: False)["sent"], 1)
        self.assertEqual(self.s.pending_events(), [])                                    # no quedó DEAD

    def test_paused_false_behaves_like_no_paused(self):
        self.commit(1); self.commit(2)
        seen = []
        self.assertEqual(self.s.dispatch_outbox(seen.append, paused=lambda: False), {"sent": 2, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual([e["event_id"] for e in seen], ["e1", "e2"])

    def test_real_failure_still_counts_when_not_paused(self):
        self.commit(1)
        def boom(event): raise RuntimeError("down")
        self.assertEqual(self.s.dispatch_outbox(boom, paused=lambda: False)["failed"], 1)
        self.assertEqual(self.attempts("e1"), 1)

    def test_pause_midway_releases_the_unsent_claims_without_counting_attempts(self):
        self.commit(1); self.commit(2); self.commit(3)
        seen = []
        res = self.s.dispatch_outbox(seen.append, worker_id="w1", paused=lambda: len(seen) >= 1)
        self.assertEqual(res, {"sent": 1, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual([e["event_id"] for e in seen], ["e1"])
        self.assertEqual([e["attempts"] for e in self.s.pending_events()], [0, 0])
        self.assertEqual([e["event_id"] for e in self.s.claim_events("w2")], ["e2", "e3"])   # libres ya, sin esperar al lease

    def test_paused_that_raises_releases_claims_and_propagates(self):
        self.commit(1); self.commit(2)
        calls = {"n": 0}
        def flaky_pause():
            calls["n"] += 1
            if calls["n"] == 3: raise RuntimeError("cannot tell")                          # 1.ª: previa; 2.ª: antes de e1; 3.ª: antes de e2
            return False
        seen = []
        with self.assertRaises(RuntimeError):
            self.s.dispatch_outbox(seen.append, worker_id="w1", paused=flaky_pause)
        self.assertEqual([e["event_id"] for e in seen], ["e1"])
        self.assertEqual([e["event_id"] for e in self.s.claim_events("w2")], ["e2"])
        self.assertEqual(self.attempts("e2"), 0)

    def test_release_events_only_releases_own_pending_claims(self):
        self.commit(1); self.commit(2)
        self.assertEqual(len(self.s.claim_events("w1")), 2)
        self.assertEqual(self.s.release_events(["e1", "e2"], "w2"), 0)                     # no son de w2
        self.assertEqual(self.s.claim_events("w3"), [])
        self.assertTrue(self.s.mark_event_sent("e2", worker_id="w1"))
        self.assertEqual(self.s.release_events(["e1", "e2", "nope"], "w1"), 1)             # e2 ya SENT; "nope" no existe
        self.assertEqual([e["event_id"] for e in self.s.claim_events("w3")], ["e1"])
        self.assertEqual(self.s.release_events([], "w1"), 0)
        with self.assertRaises(ValueError):
            self.s.release_events(["e1"], " ")

    def test_paused_must_be_callable(self):
        self.commit(1)
        with self.assertRaisesRegex(TypeError, "función sin argumentos"):                  # el mensaje propio, no un TypeError cualquiera
            self.s.dispatch_outbox(lambda e: None, paused=True)
        self.assertEqual(self.attempts("e1"), 0)


class OutboxHardeningTests(unittest.TestCase):
    """Parche 0014 (D-65): I5 (payload ilegible -> DEAD, sin bloquear la cola) e I3 (lease renovado por evento)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = Store(str(Path(self.tmp.name) / "u.sqlite"))
        self.s.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 4)
        self.s.configure_account("task-1", 1000, 100000, 10_000_000, 4)
        self.s.create_task("task-1", {"n": 0})

    def tearDown(self):
        self.s.close()
        self.tmp.cleanup()

    commit = OutboxTests.commit

    def corrupt(self, event_id: str, text: str = "no-json"):
        with closing(self.s.connect()) as c:
            c.execute("UPDATE outbox SET payload=? WHERE event_id=?", (text, event_id))
            c.commit()

    def test_unreadable_payload_goes_dead_and_does_not_block_the_queue(self):
        self.commit(1); self.commit(2)
        self.corrupt("e1")
        seen = []
        self.assertEqual(self.s.dispatch_outbox(seen.append), {"sent": 1, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual([e["event_id"] for e in seen], ["e2"])
        dead = self.s.dead_events()
        self.assertEqual([d["event_id"] for d in dead], ["e1"])
        self.assertEqual(dead[0]["payload"], "no-json")           # el texto original se conserva para la revisión manual
        self.assertEqual(self.s.dispatch_outbox(seen.append), {"sent": 0, "failed": 0, "dead": 0, "lost": 0})

    def test_claim_failure_after_commit_does_not_hide_the_real_error(self):
        """Antes: el ROLLBACK sin transacción activa lanzaba OperationalError y tapaba la causa real."""
        self.commit(1)
        def hook(point):
            if point == "claim_events:after_commit":
                raise RuntimeError("fallo real")
        self.s._fault_hook = hook
        with self.assertRaises(RuntimeError) as cm:
            self.s.claim_events("w1")
        self.assertEqual(str(cm.exception), "fallo real")

    def test_claim_does_not_leave_the_whole_batch_claimed_when_one_row_is_bad(self):
        for n in (1, 2, 3):
            self.commit(n)
        self.corrupt("e1")
        got = self.s.claim_events("w1", lease_s=60)
        self.assertEqual([e["event_id"] for e in got], ["e2", "e3"])
        self.assertEqual(len(self.s.dead_events()), 1)

    def test_renew_claim_only_for_the_owner(self):
        self.commit(1)
        self.assertEqual(len(self.s.claim_events("w1", lease_s=10, now=100.0)), 1)
        self.assertFalse(self.s.renew_claim("e1", "w2", 10, now=101.0))
        self.assertTrue(self.s.renew_claim("e1", "w1", 10, now=101.0))
        # lease vencido pero nadie lo tomó: el dueño aún puede renovarlo
        self.assertTrue(self.s.renew_claim("e1", "w1", 10, now=500.0))
        # vencido y tomado por otro: el dueño original ya no
        self.assertEqual(len(self.s.claim_events("w2", lease_s=10, now=1000.0)), 1)
        self.assertFalse(self.s.renew_claim("e1", "w1", 10, now=1001.0))
        self.assertTrue(self.s.renew_claim("e1", "w2", 10, now=1001.0))
        with self.assertRaises(ValueError):
            self.s.renew_claim("e1", " ", 10)
        with self.assertRaises(ValueError):
            self.s.renew_claim("e1", "w1", 0)

    class _Clock:
        """Reloj simulado (r21): sin sleep ni hilos, así que el resultado no depende del planificador del SO."""
        def __init__(self, t: float = 1000.0):
            self.t = t

        def __call__(self) -> float:
            return self.t

    def test_event_claimed_by_another_worker_is_not_sent_twice(self):
        """Lote de 4 con lease de 0,6 s: el lease de cada evento se renueva justo antes de su envío; el que otro worker ya tomó NO se envía.
        r21: reloj simulado. Cada envío cabe en el lease (0,5 s < 0,6 s), que es la condición de la garantía; antes la prueba usaba sleeps reales
        y en Windows el acuse de e1 podía retrasarse más que el lease (margen de 0,1 s), con lo que B legítimamente lo reenviaba (al menos una vez)."""
        from unittest import mock
        for n in range(1, 5):
            self.commit(n)
        clock = self._Clock()
        t0 = clock.t
        a, b = [], []
        def slow(e):
            if e["event_id"] == "e2":
                clock.t = t0 + 0.7            # B arranca a los 0,7 s: e1 ya está SENT, e2 renovado hasta t0+1,1, e3 y e4 con lease vencido
                self.s.dispatch_outbox(lambda ev: b.append(ev["event_id"]), worker_id="B", lease_s=5)
            clock.t += 0.5                    # el envío de A tarda 0,5 s (< lease_s)
            a.append(e["event_id"])
        with mock.patch("time.time", clock):
            res = self.s.dispatch_outbox(slow, worker_id="A", lease_s=0.6, limit=4)
        self.assertEqual(set(a) & set(b), set(), "ningún evento enviado por A y por B")
        self.assertEqual(set(a) | set(b), {"e1", "e2", "e3", "e4"}, "no se pierde ninguno")
        self.assertEqual((a, b), (["e1", "e2"], ["e3", "e4"]))
        self.assertEqual(res, {"sent": 2, "failed": 0, "dead": 0, "lost": 2})

    def test_send_longer_than_lease_may_be_resent_at_least_once(self):
        """Contrato documentado (r21): si UN envío supera lease_s, otro worker puede retomar el evento y se envía dos veces (al menos una vez);
        A lo cuenta como 'lost' aunque lo haya enviado. Por eso el consumidor debe deduplicar con consume_once."""
        from unittest import mock
        self.commit(1)
        clock = self._Clock()
        t0 = clock.t
        a, b = [], []
        def slow(e):
            a.append(e["event_id"])
            clock.t = t0 + 0.75               # el acuse llega 0,15 s después de vencer el lease (0,6 s)
            self.s.dispatch_outbox(lambda ev: b.append(ev["event_id"]), worker_id="B", lease_s=5)
        with mock.patch("time.time", clock):
            res = self.s.dispatch_outbox(slow, worker_id="A", lease_s=0.6)
        self.assertEqual((a, b), (["e1"], ["e1"]))
        self.assertEqual(res, {"sent": 0, "failed": 0, "dead": 0, "lost": 1})


if __name__ == "__main__":
    unittest.main()
