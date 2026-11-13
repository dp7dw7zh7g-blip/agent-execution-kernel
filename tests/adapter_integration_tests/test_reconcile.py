"""INTEGRACIÓN del reconciliador (AT-16: se reporta aparte): AT-10, AT-11, AT-14 contra Stores reales."""
import sqlite3
import unittest
from contextlib import closing

from adapter import (MAX_EVIDENCE_SIZE_BYTES, DEFAULT_REJECTED_STRATEGIES, CorrelationMismatch, MessageConflict, ProtocolError, ReconcileRejected, Reconciler)
from test_bridge import Base

STRATEGIES = ("PROVIDER_QUERY", "IDEMPOTENCY_LOOKUP", "EXTERNAL_REFERENCE_LOOKUP", "READ_AFTER_WRITE",
              "EVENT_CONFIRMATION", "INDEPENDENT_OBSERVATION", "HUMAN_CONFIRMATION")


class ReconBase(Base):
    def setUp(self):
        super().setUp()
        self.rec = Reconciler(self.bridge)

    def uncertain(self):
        """Operación UNKNOWN en UNAE y efecto en RECONCILIATION_REQUIRED en DTOS (como tras un timeout)."""
        ids = self.new_op(state="UNKNOWN")
        self.bridge.effects.dispatch(ids["attempt"])
        self.bridge.effects.mark_unknown(ids["attempt"], "test")
        return ids

    def rmsg(self, ids, result="CONFIRMED", **over):
        return self.msg(ids, kind="RECONCILIATION_RESULT", result=result, **over)

    def attempt_status(self, ids):
        return self.dtos.get_effect_attempt(ids["attempt"])["status"]

    def ledger(self):
        return [tuple(r) for r in self.dtos._conn.execute("SELECT operation_id,status FROM effects").fetchall()]

    def unae_commits(self):
        with closing(self.unae.connect()) as c:
            return c.execute("SELECT COUNT(*) FROM commit_log").fetchone()[0]


class AT10_EvidenceAndStrategy(ReconBase):
    def test_unknown_strategy_changes_nothing(self):
        ids = self.uncertain()
        for bad in ("GUESS", "provider_query", "", " PROVIDER_QUERY", None, 5):
            with self.subTest(v=bad), self.assertRaises(ReconcileRejected) as cm:
                self.rec.apply(self.rmsg(ids), strategy=bad, reconciler_id="test-reconciler")
            self.assertEqual(cm.exception.reason, "UNKNOWN_STRATEGY")
        self.assertEqual(self.attempt_status(ids), "RECONCILIATION_REQUIRED")
        self.assertEqual(self.ledger(), [])
        self.assertEqual(self.status(ids), "UNKNOWN")

    def test_empty_evidence_changes_nothing(self):
        ids = self.uncertain()
        with self.assertRaises(ProtocolError):
            self.rec.apply(self.rmsg(ids, evidence_refs=[]), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual((self.attempt_status(ids), self.ledger()), ("RECONCILIATION_REQUIRED", []))

    def test_dtos_itself_refuses_empty_evidence(self):
        ids = self.uncertain()
        with self.assertRaises(ValueError):
            self.bridge.effects.reconcile(ids["attempt"], "PROVIDER_QUERY", "CONFIRMED", {}, ids["op"], "prov-A", ids["idem"])
        self.assertEqual((self.attempt_status(ids), self.ledger()), ("RECONCILIATION_REQUIRED", []))

    def test_every_dtos_strategy_is_accepted(self):
        for i, strategy in enumerate(STRATEGIES):
            with self.subTest(strategy=strategy):
                self.build(name=f"s{i}")  # pila nueva: las operaciones UNKNOWN retienen su cupo de concurrencia (máx. 4)
                self.rec = Reconciler(self.bridge)
                ids = self.uncertain()
                out = self.rec.apply(self.rmsg(ids), strategy=strategy, reconciler_id="test-reconciler")
                self.assertEqual((out.status, out.dtos_status), ("APPLIED", "CONFIRMED"))

    def test_results_that_are_not_effect_reconciliations_are_refused(self):
        ids = self.uncertain()
        for bad in ("SUCCEEDED", "FAILED", "DISPATCHED", "REQUESTED", "RECOVERY_REQUIRED"):
            with self.subTest(v=bad), self.assertRaises(ReconcileRejected) as cm:
                self.rec.apply(self.rmsg(ids, result=bad), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
            self.assertEqual(cm.exception.reason, "RESULT_NOT_APPLICABLE")
        self.assertEqual(self.attempt_status(ids), "RECONCILIATION_REQUIRED")

    def test_only_reconciliation_result_messages(self):
        ids = self.uncertain()
        with self.assertRaises(ProtocolError):
            self.rec.apply(self.msg(ids), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")  # EFFECT_RESULT
        with self.assertRaises(ProtocolError):
            self.rec.apply({"result": "CONFIRMED"}, strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")  # type: ignore[arg-type]


class AT11_NotFoundIsNotRejected(ReconBase):
    def test_not_found_and_unknown_are_inconclusive(self):
        for result in ("NOT_FOUND", "UNKNOWN"):
            with self.subTest(result=result):
                ids = self.uncertain()
                out = self.rec.apply(self.rmsg(ids, result=result), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
                self.assertEqual((out.status, out.dtos_status), ("INCONCLUSIVE", "RECONCILIATION_REQUIRED"))
                self.assertEqual(self.attempt_status(ids), "RECONCILIATION_REQUIRED")
                self.assertNotIn((ids["op"], "REJECTED"), self.ledger())
                self.assertEqual(self.status(ids), "UNKNOWN")
                self.assertEqual([r["outcome"] for r in self.adapter.reconciliation_history(operation_id=ids["op"])], ["INCONCLUSIVE"])

    def test_rejected_only_when_reported_as_rejected_with_evidence(self):
        ids = self.uncertain()
        self.rec.apply(self.rmsg(ids, result="NOT_FOUND"), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual(self.ledger(), [])
        out = self.rec.apply(self.rmsg(ids, result="REJECTED", evidence_refs=["human:ticket-1"]), strategy="HUMAN_CONFIRMATION", reconciler_id="test-reconciler")
        self.assertEqual((out.status, out.dtos_status), ("APPLIED", "REJECTED"))
        self.assertEqual(self.ledger(), [(ids["op"], "REJECTED")])


class AT14_EffectResolutionDoesNotForceUnae(ReconBase):
    def test_confirmed_effect_leaves_unae_untouched(self):
        ids = self.uncertain()
        out = self.rec.apply(self.rmsg(ids, result="CONFIRMED"), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual((out.status, out.dtos_status), ("APPLIED", "CONFIRMED"))
        self.assertEqual(self.ledger(), [(ids["op"], "CONFIRMED")])  # operation_id sale de la correlación, no del mensaje suelto
        self.assertEqual(self.status(ids), "UNKNOWN")               # ni SUCCEEDED ni RECONCILED
        self.assertEqual(self.unae_commits(), 0)                    # ni commit

    def test_partial_is_not_terminal_and_writes_no_ledger_row(self):
        ids = self.uncertain()
        out = self.rec.apply(self.rmsg(ids, result="PARTIAL"), strategy="READ_AFTER_WRITE", reconciler_id="test-reconciler")
        self.assertEqual((out.dtos_status, self.ledger(), self.status(ids)), ("PARTIAL", [], "UNKNOWN"))


class Identity_Idempotency_Audit(ReconBase):
    def test_attempt_not_in_reconciliation_is_refused(self):
        ids = self.new_op(state="UNKNOWN")  # el intento sigue INTENDED
        with self.assertRaises(ReconcileRejected) as cm:
            self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual(cm.exception.reason, "NOT_IN_RECONCILIATION")

    def test_already_resolved_attempt_cannot_be_reconciled_again(self):
        ids = self.uncertain()
        self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        with self.assertRaises(ReconcileRejected) as cm:
            self.rec.apply(self.rmsg(ids, result="REJECTED", evidence_refs=["ev:2"]), strategy="HUMAN_CONFIRMATION", reconciler_id="test-reconciler")
        self.assertEqual(cm.exception.reason, "NOT_IN_RECONCILIATION")
        self.assertEqual(self.ledger(), [(ids["op"], "CONFIRMED")])

    def test_redelivery_of_the_same_message_is_idempotent(self):
        ids = self.uncertain()
        m = self.rmsg(ids)
        self.assertEqual(self.rec.apply(m, strategy="PROVIDER_QUERY", reconciler_id="test-reconciler").status, "APPLIED")
        again = self.rec.apply(dict(m), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual((again.status, again.dtos_status), ("ALREADY_PROCESSED", "CONFIRMED"))
        self.assertEqual(self.ledger(), [(ids["op"], "CONFIRMED")])

    def test_foreign_attempt_is_quarantined_before_anything(self):
        a, b = self.uncertain(), self.uncertain()
        with self.assertRaises(CorrelationMismatch):
            self.rec.apply(self.rmsg(a, effect_attempt_id=b["attempt"]), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual((self.attempt_status(a), self.attempt_status(b), self.ledger()),
                         ("RECONCILIATION_REQUIRED", "RECONCILIATION_REQUIRED", []))
        self.assertEqual(self.quarantine_reasons(), ["ATTEMPT_INTENT_MISMATCH"])

    def test_audit_log_has_pending_then_result_and_is_append_only(self):
        ids = self.uncertain()
        self.rec.apply(self.rmsg(ids), strategy="EVENT_CONFIRMATION", reconciler_id="test-reconciler")
        rows = self.adapter.reconciliation_history(operation_id=ids["op"])
        self.assertEqual([(r["strategy"], r["reported_result"], r["outcome"]) for r in rows],
                         [("EVENT_CONFIRMATION", "CONFIRMED", "PENDING"), ("EVENT_CONFIRMATION", "CONFIRMED", "APPLIED")])
        with closing(sqlite3.connect(self.adapter_db)) as c:
            for sql in ("UPDATE reconciliation_log SET outcome='X'", "DELETE FROM reconciliation_log"):
                with self.assertRaises(sqlite3.DatabaseError):
                    c.execute(sql)

    def test_refusals_are_logged(self):
        ids = self.uncertain()
        with self.assertRaises(ReconcileRejected):
            self.rec.apply(self.rmsg(ids), strategy="GUESS", reconciler_id="test-reconciler")
        rows = self.adapter.reconciliation_history(operation_id=ids["op"])
        self.assertEqual([(r["outcome"], r["detail"]) for r in rows], [("REFUSED", "UNKNOWN_STRATEGY")])


class Crash(BaseException):
    """Simula una caída del proceso (no es Exception: nadie puede capturarla por accidente)."""


class AtomicityGap(ReconBase):
    """DTOS escribe el estado del intento y el ledger en dos transacciones: una caída entre ambas deja el intento terminal sin fila en
    `effects`. El adaptador no puede modificar DTOS (D-05); detecta con la bitácora y repara."""

    def gap_after_state_change(self):
        """Reproduce la brecha de DTOS en cualquier versión (con o sin reconcile atómico): el intento pasa a terminal y el proceso cae antes del ledger."""
        dtos = self.dtos

        def faulty(attempt_id, strategy, result, evidence, operation_id, provider, idempotency_key):
            dtos.commit_effect_attempt_transition(attempt_id, result, dtos.get_effect_attempt(attempt_id)["version"])
            raise Crash()
        self.bridge.effects.reconcile = faulty
        self.addCleanup(lambda: self.bridge.effects.__dict__.pop("reconcile", None))

    def crash_in(self, obj, name):
        def boom(*a, **k):
            raise Crash()
        setattr(obj, name, boom)
        self.addCleanup(lambda: obj.__dict__.pop(name, None))

    def restart(self):
        for name, obj in (("insert_effect", self.dtos), ("reconcile", self.bridge.effects), ("log_reconciliation", self.adapter)):
            obj.__dict__.pop(name, None)

    def test_crash_between_state_change_and_ledger_is_repaired(self):
        ids = self.uncertain()
        self.gap_after_state_change()
        with self.assertRaises(Crash):
            self.rec.apply(self.rmsg(ids, evidence_refs=["ev:7", "ev:8"]), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual((self.attempt_status(ids), self.ledger()), ("CONFIRMED", []))  # el hueco, tal como lo deja DTOS
        self.assertEqual(len(self.adapter.unfinished_reconciliations()), 1)
        self.restart()
        self.assertEqual(self.rec.recover_after_restart(), {"aborted_before_apply": 0, "ledger_repaired": 1, "log_completed": 0, "inconsistent": 0, "unproven": 0})
        self.assertEqual(self.ledger(), [(ids["op"], "CONFIRMED")])
        row = self.dtos._conn.execute("SELECT provider, idempotency_key, attempt_id FROM effects").fetchone()
        self.assertEqual(tuple(row), ("prov-A", ids["idem"], ids["attempt"]))
        self.assertEqual([r["outcome"] for r in self.adapter.reconciliation_history(operation_id=ids["op"])], ["PENDING", "APPLIED"])
        self.assertEqual(self.adapter.unfinished_reconciliations(), [])
        self.assertEqual(sum(self.rec.recover_after_restart().values()), 0)  # idempotente

    def test_rejected_is_repaired_too(self):
        ids = self.uncertain()
        self.gap_after_state_change()
        with self.assertRaises(Crash):
            self.rec.apply(self.rmsg(ids, result="REJECTED", evidence_refs=["human:1"]), strategy="HUMAN_CONFIRMATION", reconciler_id="test-reconciler")
        self.restart()
        self.assertEqual(self.rec.recover_after_restart()["ledger_repaired"], 1)
        self.assertEqual(self.ledger(), [(ids["op"], "REJECTED")])

    def test_crash_after_ledger_before_final_log_only_completes_the_log(self):
        ids = self.uncertain()
        original = self.adapter.log_reconciliation

        def log_or_crash(**kw):
            if kw["outcome"] == "APPLIED":
                raise Crash()
            return original(**kw)
        self.adapter.log_reconciliation = log_or_crash  # type: ignore[method-assign]
        with self.assertRaises(Crash):
            self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.restart()
        self.assertEqual(self.ledger(), [(ids["op"], "CONFIRMED")])
        counts = self.rec.recover_after_restart()
        self.assertEqual((counts["log_completed"], counts["ledger_repaired"]), (1, 0))
        self.assertEqual(self.ledger(), [(ids["op"], "CONFIRMED")])  # sin duplicar
        self.assertEqual(self.adapter.reconciliation_history(operation_id=ids["op"])[-1]["outcome"], "APPLIED")

    def test_crash_before_state_change_is_aborted_and_can_be_retried(self):
        ids = self.uncertain()
        self.crash_in(self.bridge.effects, "reconcile")
        with self.assertRaises(Crash):
            self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.restart()
        self.assertEqual(self.rec.recover_after_restart()["aborted_before_apply"], 1)
        self.assertEqual((self.attempt_status(ids), self.ledger()), ("RECONCILIATION_REQUIRED", []))
        self.assertEqual(self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler").dtos_status, "CONFIRMED")
        self.assertEqual(self.ledger(), [(ids["op"], "CONFIRMED")])

    def collide_on_idempotency_key(self, ids):
        """Otro efecto (otro intento y otra operación) ya ocupa (provider, idempotency_key): la fila que falta no puede escribirse."""
        self.dtos.insert_effect(effect_id="e-ajeno", operation_id="op-ajena", intent_id="at-ajeno", attempt_id="at-ajeno",
                                provider="prov-A", idempotency_key=ids["idem"], status="CONFIRMED")

    def test_idempotency_key_collision_is_recorded_as_inconsistent_not_hidden(self):
        """Hallazgo 11: si la fila del ledger no se puede escribir por una colisión REAL de clave, el intento sigue huérfano. No debe cerrarse
        como APPLIED / LOG_COMPLETED: se cuenta y se registra INCONSISTENT, con el efecto que ocupa la clave en el detalle."""
        ids = self.uncertain()
        self.gap_after_state_change()
        with self.assertRaises(Crash):
            self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.restart()
        self.collide_on_idempotency_key(ids)
        self.assertEqual(self.attempt_status(ids), "CONFIRMED")
        counts = self.rec.recover_after_restart()
        self.assertEqual(counts, {"aborted_before_apply": 0, "ledger_repaired": 0, "log_completed": 0, "inconsistent": 1, "unproven": 0})
        last = self.adapter.reconciliation_history(operation_id=ids["op"])[-1]
        self.assertEqual(last["outcome"], "INCONSISTENT")
        self.assertIn("e-ajeno", last["detail"])  # quién ocupa la clave
        self.assertEqual(self.dtos.get_effects_by_attempt(ids["attempt"]), [])  # no se inventó ninguna fila para el intento huérfano
        self.assertEqual(self.adapter.unfinished_reconciliations(), [])  # la bitácora queda cerrada: no se reprocesa en bucle

    def test_benign_race_on_the_ledger_row_is_still_log_completed(self):
        """Si el ConflictError se debe a que la fila DE ESTE intento apareció entre la comprobación y la escritura, sigue siendo LOG_COMPLETED."""
        from dtos.store import ConflictError
        ids = self.uncertain()
        self.gap_after_state_change()
        with self.assertRaises(Crash):
            self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.restart()
        original = self.dtos.insert_effect

        def racing(**kw):
            original(**kw)                       # otro proceso escribió la fila de ESTE intento justo antes...
            raise ConflictError("carrera benigna")  # ...y nuestro INSERT choca con ella
        self.dtos.insert_effect = racing  # type: ignore[method-assign]
        self.addCleanup(lambda: self.dtos.__dict__.pop("insert_effect", None))
        counts = self.rec.recover_after_restart()
        self.assertEqual((counts["log_completed"], counts["ledger_repaired"], counts["inconsistent"]), (1, 0, 0))
        self.assertEqual(self.ledger(), [(ids["op"], "CONFIRMED")])

    def test_unproven_terminal_attempt_is_reported_not_repaired(self):
        ids = self.uncertain()  # alguien resuelve el intento por otra vía, sin dejar rastro en la bitácora y sin ledger
        self.dtos.commit_effect_attempt_transition(ids["attempt"], "CONFIRMED", self.dtos.get_effect_attempt(ids["attempt"])["version"])
        self.assertEqual(self.rec.recover_after_restart()["unproven"], 1)
        self.assertEqual(self.ledger(), [])  # no se fabrica una fila sin prueba

    def test_evidence_is_persisted_in_the_adapter_log(self):
        import json
        ids = self.uncertain()
        self.rec.apply(self.rmsg(ids, evidence_refs=["ev:a", "ev:b"]), strategy="READ_AFTER_WRITE", reconciler_id="test-reconciler")
        pending = self.adapter.reconciliation_history(operation_id=ids["op"])[0]
        ev = json.loads(pending["evidence_json"])
        self.assertEqual((ev["evidence_refs"], ev["reported_result"]), (["ev:a", "ev:b"], "CONFIRMED"))

    def test_old_database_without_evidence_column_is_migrated(self):
        with closing(sqlite3.connect(self.adapter_db)) as c:
            c.executescript("DROP TRIGGER reconciliation_log_no_update; DROP TRIGGER reconciliation_log_no_delete; "
                            "ALTER TABLE reconciliation_log DROP COLUMN evidence_json;")
        from adapter import UNAEDTOSAdapter
        UNAEDTOSAdapter(self.adapter_db)
        with closing(sqlite3.connect(self.adapter_db)) as c:
            self.assertIn("evidence_json", [r[1] for r in c.execute("PRAGMA table_info(reconciliation_log)")])


class EvidenceInDtosLedger(ReconBase):
    """Parche 0003: la evidencia de una resolución terminal queda en el ledger de DTOS, idéntica a la de la bitácora del adaptador."""

    def test_ledger_row_carries_the_same_canonical_evidence_as_the_log(self):
        import json
        from dtos.canonical import canonical_json
        ids = self.uncertain()
        self.rec.apply(self.rmsg(ids, evidence_refs=["ev:b", "ev:a"]), strategy="PROVIDER_QUERY", reconciler_id="orq-1")
        in_log = self.adapter.reconciliation_history(operation_id=ids["op"])[0]["evidence_json"]
        in_ledger = self.bridge.dtos.get_effects_by_attempt(ids["attempt"])[0]["evidence"]
        self.assertEqual(in_ledger, in_log)
        self.assertEqual(sorted(json.loads(in_ledger)), ["declared_by", "evidence_refs", "message_id", "reported_result"])
        self.assertEqual(in_ledger, canonical_json(json.loads(in_ledger)))

    def test_inconclusive_and_partial_results_leave_no_evidence_in_dtos(self):
        a, b = self.uncertain(), self.uncertain()
        self.rec.apply(self.rmsg(a, result="NOT_FOUND"), strategy="PROVIDER_QUERY", reconciler_id="orq-1")
        self.rec.apply(self.rmsg(b, result="PARTIAL"), strategy="READ_AFTER_WRITE", reconciler_id="orq-1")
        self.assertEqual(self.ledger(), [])  # solo la bitácora del adaptador guarda esa evidencia

    def test_recovery_writes_the_logged_evidence_into_the_repaired_row(self):
        ids = self.uncertain()
        dtos = self.dtos

        def faulty(attempt_id, strategy, result, evidence, operation_id, provider, idempotency_key):
            dtos.commit_effect_attempt_transition(attempt_id, result, dtos.get_effect_attempt(attempt_id)["version"])
            raise Crash()
        self.bridge.effects.reconcile = faulty
        with self.assertRaises(Crash):
            self.rec.apply(self.rmsg(ids, evidence_refs=["ev:9"]), strategy="PROVIDER_QUERY", reconciler_id="orq-2")
        self.bridge.effects.__dict__.pop("reconcile")
        self.assertEqual(self.rec.recover_after_restart()["ledger_repaired"], 1)
        in_log = self.adapter.reconciliation_history(operation_id=ids["op"])[0]["evidence_json"]
        self.assertEqual(self.bridge.dtos.get_effects_by_attempt(ids["attempt"])[0]["evidence"], in_log)


class LedgerConflictPath(ReconBase):
    """Vía SIN caída hacia la misma inconsistencia: el ledger ya contiene la clave y la inserción falla. Con el DTOS original el intento ya cambió
    de estado (DTOS_PARTIAL_APPLY / INCONSISTENT); con el DTOS parcheado (reconcile atómico) el intento queda intacto (DTOS_REFUSED / REFUSED)."""

    def test_ledger_conflict_is_never_hidden(self):
        ids = self.uncertain()
        self.dtos.insert_effect(effect_id="e0", operation_id="op-ajena", intent_id="i0", attempt_id="otro-intento", provider="prov-A",
                                idempotency_key=ids["idem"], status="CONFIRMED")
        with self.assertRaises(ReconcileRejected) as cm:
            self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        status, last = self.attempt_status(ids), self.adapter.reconciliation_history(operation_id=ids["op"])[-1]["outcome"]
        if status == "RECONCILIATION_REQUIRED":      # DTOS atómico
            self.assertEqual((cm.exception.reason, last), ("DTOS_REFUSED", "REFUSED"))
        else:                                        # DTOS original: estado cambiado sin ledger
            self.assertEqual((status, cm.exception.reason, last), ("CONFIRMED", "DTOS_PARTIAL_APPLY", "INCONSISTENT"))


class PartialEvidenceAndSizeCap(ReconBase):
    """D-46/D-47: la evidencia de PARTIAL queda también en DTOS (historial) y la evidencia está acotada en bytes."""

    def test_partial_evidence_is_in_the_dtos_history_and_matches_the_adapter_log(self):
        ids = self.uncertain()
        self.rec.apply(self.rmsg(ids, result="PARTIAL", evidence_refs=["ev:a", "ev:b"]), strategy="READ_AFTER_WRITE", reconciler_id="test-reconciler")
        (hist,) = self.dtos.get_effect_reconciliations(ids["attempt"])
        pending = self.adapter.reconciliation_history(operation_id=ids["op"])[0]
        self.assertEqual((hist["from_status"], hist["to_status"], hist["strategy"]), ("RECONCILIATION_REQUIRED", "PARTIAL", "READ_AFTER_WRITE"))
        self.assertEqual(hist["evidence"], pending["evidence_json"])  # misma canonicalización en el adaptador y en DTOS
        self.assertEqual(self.ledger(), [])

    MID = "00000000-0000-4000-8000-000000000000"

    def refs_for(self, extra):
        """A-2 (r10): una referencia ya no puede medir más de MAX_EVIDENCE_REF_BYTES, así que la evidencia grande se reparte en 64 referencias.
        Devuelve (ids, refs) para que la evidencia serializada mida exactamente MAX_EVIDENCE_SIZE_BYTES + extra bytes."""
        from adapter.adapter import canonical_json, MAX_EVIDENCE_REFS, MAX_EVIDENCE_REF_BYTES
        ids = self.uncertain()
        head = [f"{i:03d}" + "x" * (MAX_EVIDENCE_REF_BYTES - 3) for i in range(MAX_EVIDENCE_REFS - 1)]
        probe = {"evidence_refs": head + ["z"], "message_id": self.MID, "reported_result": "CONFIRMED", "declared_by": "test-reconciler"}
        last = 1 + MAX_EVIDENCE_SIZE_BYTES + extra - len(canonical_json(probe).encode("utf-8"))
        self.assertTrue(1 <= last <= MAX_EVIDENCE_REF_BYTES, last)
        return ids, head + ["z" * last]

    def test_evidence_at_the_cap_is_accepted(self):
        ids, refs = self.refs_for(0)
        m = self.rmsg(ids, evidence_refs=refs, message_id=self.MID)
        self.assertEqual(self.rec.apply(m, strategy="PROVIDER_QUERY", reconciler_id="test-reconciler").status, "APPLIED")

    def test_evidence_above_the_cap_is_refused_before_writing_anything(self):
        ids, refs = self.refs_for(1)
        m = self.rmsg(ids, evidence_refs=refs, message_id=self.MID)
        with self.assertRaises(ReconcileRejected) as cm:
            self.rec.apply(m, strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual(cm.exception.reason, "EVIDENCE_TOO_LARGE")
        self.assertEqual((self.attempt_status(ids), self.ledger(), self.dtos.get_effect_reconciliations(ids["attempt"])), ("RECONCILIATION_REQUIRED", [], []))
        self.assertEqual([(r["outcome"], r["detail"]) for r in self.adapter.reconciliation_history(operation_id=ids["op"])], [("REFUSED", "EVIDENCE_TOO_LARGE")])

    def test_the_cap_counts_bytes_not_characters(self):
        ids = self.uncertain()
        refs = [f"{i:02d}" + "ñ" * 510 for i in range(64)]  # 64 x 1022 bytes pero solo ~32 700 caracteres
        self.assertLess(sum(map(len, refs)), MAX_EVIDENCE_SIZE_BYTES)
        m = self.rmsg(ids, evidence_refs=refs)
        with self.assertRaises(ReconcileRejected) as cm:
            self.rec.apply(m, strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual(cm.exception.reason, "EVIDENCE_TOO_LARGE")

    def test_the_adapter_and_dtos_caps_are_the_same(self):
        from dtos.store import MAX_EVIDENCE_SIZE_BYTES as DTOS_CAP
        self.assertEqual(MAX_EVIDENCE_SIZE_BYTES, DTOS_CAP)


class RejectedPolicy(ReconBase):
    """D-42: REJECTED («no ocurrió») solo lo concluyen las estrategias autorizadas; por defecto, solo la confirmación humana."""

    def reject(self, strategy, rec=None):
        ids = self.uncertain()
        return ids, (rec or self.rec).apply(self.rmsg(ids, result="REJECTED", evidence_refs=["ev:r"]), strategy=strategy, reconciler_id="test-reconciler")

    def test_default_policy_is_human_confirmation_only(self):
        self.assertEqual(DEFAULT_REJECTED_STRATEGIES, frozenset({"HUMAN_CONFIRMATION"}))
        self.assertEqual(self.rec.rejected_strategies, DEFAULT_REJECTED_STRATEGIES)

    def test_automatic_strategies_cannot_conclude_rejected(self):
        for i, strategy in enumerate(s for s in STRATEGIES if s != "HUMAN_CONFIRMATION"):
            with self.subTest(strategy=strategy):
                self.build(name=f"r{i}")
                self.rec = Reconciler(self.bridge)
                ids = self.uncertain()
                with self.assertRaises(ReconcileRejected) as cm:
                    self.rec.apply(self.rmsg(ids, result="REJECTED", evidence_refs=["ev:r"]), strategy=strategy, reconciler_id="test-reconciler")
                self.assertEqual(cm.exception.reason, "REJECTED_NOT_ALLOWED")
                self.assertEqual((self.attempt_status(ids), self.ledger(), self.status(ids)), ("RECONCILIATION_REQUIRED", [], "UNKNOWN"))
                self.assertEqual([(r["outcome"], r["detail"]) for r in self.adapter.reconciliation_history(operation_id=ids["op"])],
                                 [("REFUSED", "REJECTED_NOT_ALLOWED")])

    def test_human_confirmation_can_conclude_rejected(self):
        ids, out = self.reject("HUMAN_CONFIRMATION")
        self.assertEqual((out.status, out.dtos_status), ("APPLIED", "REJECTED"))
        self.assertEqual(self.ledger(), [(ids["op"], "REJECTED")])

    def test_the_policy_only_restricts_rejected(self):
        for result, expected in (("CONFIRMED", "CONFIRMED"), ("PARTIAL", "PARTIAL")):
            ids = self.uncertain()
            out = self.rec.apply(self.rmsg(ids, result=result), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
            self.assertEqual(out.dtos_status, expected)
        ids = self.uncertain()
        out = self.rec.apply(self.rmsg(ids, result="NOT_FOUND"), strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual(out.status, "INCONCLUSIVE")

    def test_the_policy_is_extensible_and_can_be_empty(self):
        wide = Reconciler(self.bridge, {"HUMAN_CONFIRMATION", "EVENT_CONFIRMATION"})
        self.assertEqual(self.reject("EVENT_CONFIRMATION", wide)[1].dtos_status, "REJECTED")
        self.build(name="never")
        never = Reconciler(self.bridge, frozenset())
        ids = self.uncertain()
        with self.assertRaises(ReconcileRejected) as cm:
            never.apply(self.rmsg(ids, result="REJECTED", evidence_refs=["ev:r"]), strategy="HUMAN_CONFIRMATION", reconciler_id="x")
        self.assertEqual(cm.exception.reason, "REJECTED_NOT_ALLOWED")

    def test_invalid_policies_fail_at_construction(self):
        for bad in ("HUMAN_CONFIRMATION", {"GUESS"}, {"human_confirmation"}, None, 5, {"HUMAN_CONFIRMATION", 7}):
            with self.subTest(v=repr(bad)), self.assertRaises(ValueError):
                Reconciler(self.bridge, bad)

    def test_the_policy_cannot_be_mutated_after_construction(self):
        with self.assertRaises(AttributeError):
            self.rec.rejected_strategies.add("PROVIDER_QUERY")  # frozenset


class ReconcilerIdentity(ReconBase):
    """D-32: quien declara la estrategia debe identificarse; queda en la bitácora, en la evidencia y en la recuperación."""

    def test_reconciler_id_is_mandatory_without_default(self):
        ids = self.uncertain()
        with self.assertRaises(TypeError):
            self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY")  # type: ignore[call-arg]
        for bad in ("", "   ", None, 5, ["x"]):
            with self.subTest(v=bad), self.assertRaises(ProtocolError):
                self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id=bad)  # type: ignore[arg-type]
        self.assertEqual((self.attempt_status(ids), self.ledger(), self.adapter.reconciliation_history(operation_id=ids["op"])),
                         ("RECONCILIATION_REQUIRED", [], []))

    def test_recorded_in_every_log_row_and_in_the_evidence(self):
        import json
        a, b, c = self.uncertain(), self.uncertain(), self.uncertain()
        self.rec.apply(self.rmsg(a), strategy="PROVIDER_QUERY", reconciler_id="orquestador-1")
        self.rec.apply(self.rmsg(b, result="NOT_FOUND"), strategy="PROVIDER_QUERY", reconciler_id="orquestador-2")
        with self.assertRaises(ReconcileRejected):
            self.rec.apply(self.rmsg(c), strategy="GUESS", reconciler_id="orquestador-3")
        for ids, who in ((a, "orquestador-1"), (b, "orquestador-2"), (c, "orquestador-3")):
            rows = self.adapter.reconciliation_history(operation_id=ids["op"])
            self.assertTrue(rows and all(r["reconciler_id"] == who for r in rows), (who, rows))
        self.assertEqual(json.loads(self.adapter.reconciliation_history(operation_id=a["op"])[0]["evidence_json"])["declared_by"], "orquestador-1")

    def test_recovery_keeps_the_original_declarant(self):
        ids = self.uncertain()
        dtos = self.dtos

        def faulty(attempt_id, strategy, result, evidence, operation_id, provider, idempotency_key):
            dtos.commit_effect_attempt_transition(attempt_id, result, dtos.get_effect_attempt(attempt_id)["version"])
            raise Crash()
        self.bridge.effects.reconcile = faulty
        with self.assertRaises(Crash):
            self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="orquestador-9")
        self.bridge.effects.__dict__.pop("reconcile")
        self.rec.recover_after_restart()
        self.assertEqual({r["reconciler_id"] for r in self.adapter.reconciliation_history(operation_id=ids["op"])}, {"orquestador-9"})

    def test_old_database_without_the_new_columns_is_migrated(self):
        with closing(sqlite3.connect(self.adapter_db)) as c:
            c.executescript("DROP TRIGGER reconciliation_log_no_update; DROP TRIGGER reconciliation_log_no_delete; "
                            "ALTER TABLE reconciliation_log DROP COLUMN evidence_json; ALTER TABLE reconciliation_log DROP COLUMN reconciler_id;")
        from adapter import UNAEDTOSAdapter
        UNAEDTOSAdapter(self.adapter_db)
        with closing(sqlite3.connect(self.adapter_db)) as c:
            cols = [r[1] for r in c.execute("PRAGMA table_info(reconciliation_log)")]
        self.assertIn("evidence_json", cols)
        self.assertIn("reconciler_id", cols)


if __name__ == "__main__":
    unittest.main()


class A3_ReusedMessageIdIsDetected(ReconBase):
    """A-3 (informe g0.8 r9): `apply` debe pasar por el inbox y no mentir sobre un message_id reutilizado con otro contenido."""

    def inbox_rows(self):
        with closing(sqlite3.connect(self.adapter_db)) as c:
            return c.execute("SELECT message_id,status FROM adapter_inbox").fetchall()

    def test_same_message_id_with_other_result_is_quarantined_not_reported_as_processed(self):
        ids = self.uncertain()
        first = self.rmsg(ids, result="UNKNOWN", message_id="mid-reused", evidence_refs=["ev:a"])
        self.assertEqual(self.rec.apply(first, strategy="PROVIDER_QUERY", reconciler_id="r").status, "INCONCLUSIVE")
        second = self.rmsg(ids, result="CONFIRMED", message_id="mid-reused", evidence_refs=["ev:b"])
        with self.assertRaises(MessageConflict):
            self.rec.apply(second, strategy="PROVIDER_QUERY", reconciler_id="r")
        self.assertEqual(self.quarantine_reasons(), ["MESSAGE_ID_PAYLOAD_CONFLICT"])
        self.assertEqual((self.attempt_status(ids), self.ledger()), ("RECONCILIATION_REQUIRED", []))  # nada se aplicó

    def test_same_message_id_with_other_evidence_but_same_result_is_also_a_conflict(self):
        ids = self.uncertain()
        self.rec.apply(self.rmsg(ids, result="NOT_FOUND", message_id="mid-ev", evidence_refs=["ev:a"]), strategy="PROVIDER_QUERY", reconciler_id="r")
        with self.assertRaises(MessageConflict):
            self.rec.apply(self.rmsg(ids, result="NOT_FOUND", message_id="mid-ev", evidence_refs=["ev:other"]), strategy="PROVIDER_QUERY", reconciler_id="r")
        self.assertEqual(self.quarantine_reasons(), ["MESSAGE_ID_PAYLOAD_CONFLICT"])

    def test_applied_message_cannot_be_flipped_by_reusing_its_id(self):
        ids = self.uncertain()
        self.rec.apply(self.rmsg(ids, result="CONFIRMED", message_id="mid-flip"), strategy="PROVIDER_QUERY", reconciler_id="r")
        with self.assertRaises(MessageConflict):
            self.rec.apply(self.rmsg(ids, result="REJECTED", message_id="mid-flip", evidence_refs=["ev:9"]), strategy="HUMAN_CONFIRMATION", reconciler_id="r")
        self.assertEqual((self.attempt_status(ids), self.ledger()), ("CONFIRMED", [(ids["op"], "CONFIRMED")]))

    def test_identical_redelivery_returns_the_stored_result(self):
        ids = self.uncertain()
        m = self.rmsg(ids, result="UNKNOWN", message_id="mid-same")
        self.rec.apply(m, strategy="PROVIDER_QUERY", reconciler_id="r")
        again = self.rec.apply(dict(m), strategy="PROVIDER_QUERY", reconciler_id="r")
        self.assertEqual((again.status, again.result, again.dtos_status), ("ALREADY_PROCESSED", "UNKNOWN", "RECONCILIATION_REQUIRED"))
        self.assertEqual(self.quarantine_reasons(), [])

    def test_apply_records_the_message_in_the_inbox_once(self):
        ids = self.uncertain()
        m = self.rmsg(ids, message_id="mid-inbox")
        self.rec.apply(m, strategy="PROVIDER_QUERY", reconciler_id="r")
        self.rec.apply(dict(m), strategy="PROVIDER_QUERY", reconciler_id="r")
        self.assertEqual(self.inbox_rows(), [("mid-inbox", "ACCEPTED")])

    def test_log_written_before_the_inbox_existed_still_detects_a_different_result(self):
        """Bitácora de r9 (sin fila en el inbox): el resultado almacenado sigue mandando."""
        ids = self.uncertain()
        self.bridge.adapter.log_reconciliation(operation_id=ids["op"], effect_attempt_id=ids["attempt"], message_id="mid-legacy", strategy="PROVIDER_QUERY",
                                               reported_result="UNKNOWN", outcome="INCONCLUSIVE", reconciler_id="r")
        with self.assertRaises(MessageConflict):
            self.rec.apply(self.rmsg(ids, result="CONFIRMED", message_id="mid-legacy"), strategy="PROVIDER_QUERY", reconciler_id="r")
        self.assertEqual(self.attempt_status(ids), "RECONCILIATION_REQUIRED")
        self.assertEqual(self.quarantine_reasons(), ["MESSAGE_ID_PAYLOAD_CONFLICT"])

    def test_a_refused_message_can_be_retried_with_the_same_id_once_the_cause_is_gone(self):
        ids = self.new_op(state="UNKNOWN")  # intento aún INTENDED: se rechaza
        m = self.rmsg(ids, message_id="mid-retry")
        with self.assertRaises(ReconcileRejected):
            self.rec.apply(m, strategy="PROVIDER_QUERY", reconciler_id="r")
        self.bridge.effects.dispatch(ids["attempt"]); self.bridge.effects.mark_unknown(ids["attempt"], "test")
        self.assertEqual(self.rec.apply(dict(m), strategy="PROVIDER_QUERY", reconciler_id="r").status, "APPLIED")


class L1_L4_RecoveryAndReconcilerId(ReconBase):
    """L-1 y L-4 del informe g0.8 r9 (D-62)."""

    def test_recovery_survives_an_attempt_that_does_not_exist_in_dtos(self):
        real = self.uncertain()
        self.bridge.adapter.log_reconciliation(operation_id=real["op"], effect_attempt_id=real["attempt"], message_id="m-real", strategy="PROVIDER_QUERY",
                                               reported_result="CONFIRMED", outcome="PENDING", evidence={"evidence_refs": ["e"]}, reconciler_id="r")
        self.bridge.adapter.log_reconciliation(operation_id="op-ghost", effect_attempt_id="att-ghost", message_id="m-ghost", strategy="PROVIDER_QUERY",
                                               reported_result="CONFIRMED", outcome="PENDING", evidence={"evidence_refs": ["e"]}, reconciler_id="r")
        counts = self.rec.recover_after_restart()  # antes: TypeError sobre None y toda la recuperación abortaba
        self.assertEqual((counts["inconsistent"], counts["aborted_before_apply"]), (1, 1))
        self.assertEqual(self.adapter.unfinished_reconciliations(), [])
        ghost = [r for r in self.adapter.reconciliation_history(message_id="m-ghost")][-1]
        self.assertEqual(ghost["outcome"], "INCONSISTENT")

    def test_terminal_attempt_without_ledger_row_and_without_correlation_is_reported_not_fatal(self):
        from test_bridge import uid
        att, intent, op, idem = uid(), uid(), uid(), "idem-0123456789abcdef-orphan"
        self.bridge.effects.create(att, intent, "prov-A", idem)
        self.bridge.effects.dispatch(att)
        self.bridge.effects.mark_unknown(att, "test")
        self.bridge.effects.reconcile(att, "PROVIDER_QUERY", "CONFIRMED", {"evidence_refs": ["e"]}, op, "prov-A", idem)
        self.bridge._ledger_operation_ids = lambda attempt_id: set()  # simula el ledger sin fila (caída entre las dos transacciones de DTOS)
        self.bridge.adapter.log_reconciliation(operation_id=op, effect_attempt_id=att, message_id="m-orphan", strategy="PROVIDER_QUERY",
                                               reported_result="CONFIRMED", outcome="PENDING", evidence={"evidence_refs": ["e"]}, reconciler_id="r")
        counts = self.rec.recover_after_restart()  # antes: StopIteration
        self.assertEqual(counts["inconsistent"], 1)
        last = self.adapter.reconciliation_history(message_id="m-orphan")[-1]
        self.assertEqual(last["outcome"], "INCONSISTENT")
        self.assertIn("correlación durable", last["detail"])

    def test_reconciler_id_is_a_bounded_identifier(self):
        ids = self.uncertain()
        for bad in ("x" * 100_000, "x" * 129, " r", "r ", "a\x00b", "a\nb", "e\u0301"):  # la última no está en NFC
            with self.subTest(v=repr(bad)[:20]), self.assertRaises(ProtocolError):
                self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id=bad)
        self.assertEqual((self.attempt_status(ids), self.adapter.reconciliation_history(operation_id=ids["op"])), ("RECONCILIATION_REQUIRED", []))
        self.assertEqual(self.rec.apply(self.rmsg(ids), strategy="PROVIDER_QUERY", reconciler_id="x" * 128).status, "APPLIED")
