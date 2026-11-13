"""Pruebas del observador «tee» (D-50): unitarias y contra un `Store` real de UNAE.

Los Stores de UNAE se cierran con addCleanup (conexión ancla: en Windows impide borrar el .sqlite)."""
import copy
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from proactive import ShadowLog, ShadowObserver, Rule, Proposal, task_state_committed_review_rule, tee_sender
from unae import CommitStatus, Store
from unae.store import CommitRequest

RULE_ID = "r-task-committed-review"


def event(n=1, event_type="TASK_STATE_COMMITTED"):
    return {"event_id": f"e{n}", "commit_id": f"c{n}", "event_type": event_type, "attempts": 0,
            "payload": {"task_id": "task-1", "version": n}}


class TeeUnitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.shadow = ShadowLog(Path(self.tmp.name) / "shadow.sqlite")

    def test_real_sender_runs_first_and_observer_gets_the_event(self):
        order = []
        sender = tee_sender(lambda e: order.append(("real", e["event_id"])), lambda e: order.append(("obs", e["event_id"])))
        sender(event(1))
        self.assertEqual(order, [("real", "e1"), ("obs", "e1")])

    def test_real_failure_propagates_and_is_not_observed(self):
        seen = []
        def boom(e): raise RuntimeError("down")
        sender = tee_sender(boom, seen.append)
        with self.assertRaisesRegex(RuntimeError, "down"):
            sender(event(1))
        self.assertEqual(seen, [])

    def test_observer_failure_is_swallowed_and_reported(self):
        errors = []
        def bad(e): raise ValueError("observer bug")
        tee_sender(lambda e: None, bad, on_error=lambda ev, exc: errors.append((ev["event_id"], str(exc))))(event(1))
        self.assertEqual(errors, [("e1", "observer bug")])
        tee_sender(lambda e: None, bad)(event(2))                                          # sin on_error: tampoco lanza

    def test_failing_error_handler_cannot_break_delivery(self):
        def bad(e): raise ValueError("observer bug")
        def worse(ev, exc): raise RuntimeError("handler bug")
        tee_sender(lambda e: None, bad, on_error=worse)(event(1))

    def test_observer_cannot_alter_the_event_the_dispatcher_keeps(self):
        original = event(1); snapshot = copy.deepcopy(original)
        def vandal(e):
            e["event_id"] = "HACKED"; e["payload"]["version"] = 999
        tee_sender(lambda e: None, vandal)(original)
        self.assertEqual(original, snapshot)

    def test_non_callable_arguments_are_rejected(self):
        with self.assertRaises(TypeError):
            tee_sender(None, lambda e: None)
        with self.assertRaises(TypeError):
            tee_sender(lambda e: None, "no")

    def test_rule_records_task_state_committed_with_opaque_refs(self):
        obs = ShadowObserver(self.shadow, [task_state_committed_review_rule("p7")])
        self.assertEqual(obs.observe(event(3)), 1)
        with closing(sqlite3.connect(self.shadow.path)) as c:
            row = c.execute("SELECT rule_id,policy_version,trigger_ref,kind,payload,kernel_refs FROM would_have_done").fetchone()
        self.assertEqual(row, (RULE_ID, "p7", "e3", "review_task_state", '{"task_id":"task-1","version":3}',
                               '{"commit_id":"c3","task_id":"task-1"}'))

    def test_other_event_types_produce_nothing(self):
        obs = ShadowObserver(self.shadow, [task_state_committed_review_rule()])
        self.assertEqual(obs.observe(event(1, event_type="OTHER")), 0)
        self.assertEqual(self.shadow.summary()["total"], 0)
        self.assertEqual(list(obs.errors), [])

    def test_repeated_event_is_absorbed(self):
        obs = ShadowObserver(self.shadow, [task_state_committed_review_rule()])
        self.assertEqual((obs.observe(event(1)), obs.observe(event(1)), obs.observe(event(1))), (1, 0, 0))
        self.assertEqual(self.shadow.summary()["total"], 1)

    def test_a_failing_rule_does_not_stop_the_others(self):
        errors = []
        def build_bad(e): raise KeyError("x")
        bad = Rule("r-bad", "p1", lambda e: True, build_bad)
        also = Rule("r-also", "p1", lambda e: True, lambda e: Proposal("note", {"n": 1}, "otra regla"))
        obs = ShadowObserver(self.shadow, [bad, task_state_committed_review_rule(), also],
                             on_error=lambda rid, ev, exc: errors.append(rid))
        self.assertEqual(obs.observe(event(1)), 2)
        self.assertEqual(errors, ["r-bad"])
        self.assertEqual([(r, e) for r, e, _ in obs.errors], [("r-bad", "e1")])
        self.assertEqual(self.shadow.summary()["total"], 2)

    def test_invalid_proposal_is_reported_not_raised(self):
        bad = Rule("r-big", "p1", lambda e: True, lambda e: Proposal("note", {"x": "a" * 70000}, "demasiado grande"))
        obs = ShadowObserver(self.shadow, [bad])
        self.assertEqual(obs.observe(event(1)), 0)
        self.assertEqual(len(obs.errors), 1)
        self.assertIn("too large", obs.errors[0][2])

    def test_rule_validation(self):
        ok = lambda e: True
        for kw in (dict(rule_id=" "), dict(policy_version="")):
            with self.subTest(kw=kw), self.assertRaises(ValueError):
                Rule(**{**dict(rule_id="r", policy_version="p", matches=ok, build=ok), **kw})
        with self.assertRaises(TypeError):
            Rule("r", "p", None, ok)
        with self.assertRaisesRegex(ValueError, "duplicate rule_id"):
            ShadowObserver(self.shadow, [task_state_committed_review_rule(), task_state_committed_review_rule("p2")])

    def test_error_history_is_bounded(self):
        bad = Rule("r-bad", "p1", lambda e: True, lambda e: 1 / 0)
        obs = ShadowObserver(self.shadow, [bad])
        for n in range(250):
            obs.observe(event(n))
        self.assertEqual(len(obs.errors), 100)
        self.assertEqual(obs.errors[-1][1], "e249")


class TeeWithRealStoreTests(unittest.TestCase):
    """Los cinco escenarios de la propuesta, sobre un Store real de UNAE."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.shadow = ShadowLog(Path(self.tmp.name) / "shadow.sqlite")
        self.obs = ShadowObserver(self.shadow, [task_state_committed_review_rule()])
        self.s = self.new_store("u1.sqlite")

    def new_store(self, name):
        s = Store(str(Path(self.tmp.name) / name))
        self.addCleanup(s.close)                                  # cierra la conexión ancla (Windows)
        s.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 4)
        s.configure_account("task-1", 1000, 100000, 10_000_000, 4)
        s.create_task("task-1", {"n": 0})
        return s

    def commit(self, s, n):
        op, res, rc = f"op-{n}", f"res-{n}", f"r{n}"
        self.assertEqual(s.reserve_and_register(task_id="task-1", operation_id=op, reservation_id=res, idempotency_key=f"k{n}",
            fencing_token=1, compute_units=1, time_ms=1, memory_bytes=1), "RESERVED")
        s.mark_running(op, 1, f"p{n}")
        s.record_receipt(receipt_id=rc, operation_id=op, reservation_id=res, outcome="SUCCEEDED",
            actual_compute=1, actual_time_ms=1, peak_memory_bytes=1, provider_operation_id=f"p{n}")
        self.assertEqual(s.coordinated_commit(CommitRequest(f"c{n}", "task-1", op, res, rc, n - 1, 1, {"n": n}, f"e{n}")),
                         CommitStatus.COMMITTED)

    def shadow_rows(self):
        with closing(sqlite3.connect(self.shadow.path)) as c:
            return c.execute("SELECT trigger_ref,kind FROM would_have_done ORDER BY trigger_ref").fetchall()

    def test_1_one_delivery_creates_exactly_one_row_per_event(self):
        self.commit(self.s, 1); self.commit(self.s, 2)
        delivered = []
        res = self.s.dispatch_outbox(tee_sender(delivered.append, self.obs.observe))
        self.assertEqual(res, {"sent": 2, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual(self.shadow_rows(), [("e1", "review_task_state"), ("e2", "review_task_state")])
        self.assertEqual(self.s.pending_events(), [])

    def test_2_redelivery_does_not_duplicate(self):
        self.commit(self.s, 1)
        delivered = []
        def sends_then_dies(event):                       # el envío y la observación salen bien, pero la entrega falla después
            tee_sender(delivered.append, self.obs.observe)(event)
            raise RuntimeError("se cayó antes de confirmar")
        self.assertEqual(self.s.dispatch_outbox(sends_then_dies)["failed"], 1)
        self.assertEqual(len(self.s.pending_events()), 1)                      # sigue PENDING: se reenviará
        self.assertEqual(self.shadow.summary()["total"], 1)
        self.assertEqual(self.s.dispatch_outbox(tee_sender(delivered.append, self.obs.observe))["sent"], 1)
        self.assertEqual([e["event_id"] for e in delivered], ["e1", "e1"])     # al menos una vez: llegó dos veces
        self.assertEqual(self.shadow_rows(), [("e1", "review_task_state")])    # y se registró una sola
        self.assertEqual(self.shadow.summary()["total"], 1)

    def test_3_observer_failure_does_not_change_the_dispatch_result(self):
        self.commit(self.s, 1); self.commit(self.s, 2)
        def bad(e): raise RuntimeError("observer bug")
        errors = []
        res = self.s.dispatch_outbox(tee_sender(lambda e: None, bad, on_error=lambda ev, exc: errors.append(ev["event_id"])))
        self.assertEqual(res, {"sent": 2, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual(self.s.pending_events(), [])
        self.assertEqual(errors, ["e1", "e2"])
        self.assertEqual(self.shadow.summary()["total"], 0)

    def test_4_paused_dispatch_observes_nothing_and_resumes_cleanly(self):
        self.commit(self.s, 1); self.commit(self.s, 2)
        sender = tee_sender(lambda e: None, self.obs.observe)
        self.assertEqual(self.s.dispatch_outbox(sender, paused=lambda: True), {"sent": 0, "failed": 0, "dead": 0, "lost": 0})
        self.assertEqual(self.shadow.summary()["total"], 0)
        self.assertEqual([e["attempts"] for e in self.s.pending_events()], [0, 0])
        self.assertEqual(self.s.dispatch_outbox(sender, paused=lambda: False)["sent"], 2)
        self.assertEqual(self.shadow.summary()["total"], 2)

    def test_5_unae_receives_no_extra_writes_and_layer_uses_its_own_file(self):
        control = self.new_store("u_control.sqlite")
        for s in (self.s, control):
            self.commit(s, 1); self.commit(s, 2)
        control.dispatch_outbox(lambda e: None)
        self.s.dispatch_outbox(tee_sender(lambda e: None, self.obs.observe))
        def snapshot(s):
            with closing(sqlite3.connect(s.path)) as c:
                tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
                counts = {t: c.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables}
                return tables, counts, c.execute("SELECT status,COUNT(*) FROM outbox GROUP BY status").fetchall(), s.schema_version()
        self.assertEqual(snapshot(self.s), snapshot(control))
        self.assertFalse(any("would" in t or "shadow" in t for t in snapshot(self.s)[0]))
        self.assertNotEqual(self.s.path, self.shadow.path)
        self.assertEqual(self.shadow.summary()["total"], 2)

    def test_events_from_claim_events_have_the_shape_the_rule_expects(self):
        """Guarda el contrato: si UNAE cambiara el formato de sus eventos, esta prueba lo diría."""
        self.commit(self.s, 1)
        ev = self.s.claim_events("probe")[0]
        self.assertEqual({"event_id", "commit_id", "event_type", "payload", "attempts"}, set(ev))
        self.assertEqual((ev["event_type"], ev["payload"]), ("TASK_STATE_COMMITTED", {"task_id": "task-1", "version": 1}))
        self.assertEqual(self.obs.observe(ev), 1)


if __name__ == "__main__":
    unittest.main()
