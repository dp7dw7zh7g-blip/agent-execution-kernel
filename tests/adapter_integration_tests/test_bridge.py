"""Pruebas de INTEGRACIÓN del puente: adaptador + Store UNAE real + SQLiteStore DTOS real (AT-16: se reportan aparte).
Cubren AT-05, AT-06, AT-07 y el registro de vías A/B. No cubren AT-01, 08-14 (despacho, reconciliador, caída)."""
import sqlite3
import tempfile
import unittest
import uuid
from contextlib import closing
from pathlib import Path

from adapter import (CorrelationMismatch, CorrelationUnverifiable, ProtocolError, StoreBridge, UNAEDTOSAdapter,
                     from_dtos_token, to_dtos_token, MAX_FENCING_TOKEN, MAX_SQLITE_INTEGER)
from dtos.store import SQLiteStore
from unae import Store, sign_receipt

KEY_A = b"secret-A" * 4
IDEM = "idem-0123456789abcdef"


def uid():
    return str(uuid.uuid4())


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.build()

    def build(self, *, name="", keyring=None, store_require_signed=False, bridge_require_signed=True):
        """Monta una pila completa (UNAE + DTOS + adaptador + puente). Se puede llamar de nuevo con otra configuración."""
        d = Path(self._tmp.name)
        self.unae = Store(str(d / f"u{name}.sqlite"), keyring=keyring if keyring is not None else {"prov-A": KEY_A},
                          require_signed_receipts=store_require_signed)
        self.unae.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 4)
        self.unae.configure_account("task-1", 1000, 100000, 10_000_000, 4)
        self.unae.create_task("task-1", {"n": 0})
        self.dtos = SQLiteStore(d / f"d{name}.sqlite")
        self.adapter_db = str(d / f"a{name}.sqlite")
        self.adapter = UNAEDTOSAdapter(self.adapter_db)
        self.bridge = StoreBridge(self.adapter, self.unae, self.dtos, require_signed_receipts=bridge_require_signed)
        self.addCleanup(self.dtos.close)
        self.addCleanup(self.unae.close)
        self.n = 0

    def new_op(self, *, state="RUNNING", token=1, names=None, bound=True):
        """Crea una operación en UNAE y registra su intención en el puente. state: RESERVED | RUNNING | UNKNOWN."""
        self.n += 1
        ids = dict(op=uid(), intent=uid(), attempt=uid(), res=f"res-{self.n}", idem=f"{IDEM}-{self.n}")
        ids.update(names or {})
        self.assertEqual(self.unae.reserve_and_register(task_id="task-1", operation_id=ids["op"], reservation_id=ids["res"],
                         idempotency_key=ids["idem"], fencing_token=token, compute_units=10, time_ms=10, memory_bytes=10), "RESERVED")
        if state in ("RUNNING", "UNKNOWN"):
            self.assertTrue(self.unae.mark_running(ids["op"], token, f"p-{self.n}", **({"provider_id": "prov-A"} if bound else {})))
        if state == "UNKNOWN":
            self.assertTrue(self.unae.mark_unknown(ids["op"]))
        self.bridge.register_intent(operation_id=ids["op"], effect_intent_id=ids["intent"], effect_attempt_id=ids["attempt"],
                                    provider="prov-A", idempotency_key=ids["idem"], fencing_token=token, payload={"n": self.n})
        return ids

    def msg(self, ids, kind="EFFECT_RESULT", **over):
        m = {"contract_version": "1.1", "schema_version": "1", "message_id": uid(), "message_type": kind,
             "correlation_id": "corr-1", "evidence_refs": ["ev:1"], "created_at": "2026-10-25T10:00:00Z",
             "operation_id": ids["op"], "effect_intent_id": ids["intent"], "effect_attempt_id": ids["attempt"],
             "idempotency_key": ids["idem"], "result": "UNKNOWN"}
        if kind == "EFFECT_DISPATCHED":
            m.update(fencing_token=1); m.pop("result"); m["evidence_refs"] = []
        if kind == "RECONCILIATION_RESULT":
            m.pop("effect_intent_id"); m.pop("idempotency_key")
        m.update(over)
        return m

    def receipt(self, ids, rid=None, *, key=KEY_A, provider="prov-A", **over):
        rid = rid or f"r-{ids['res']}"
        kw = dict(receipt_id=rid, operation_id=ids["op"], reservation_id=ids["res"], outcome="SUCCEEDED", actual_compute=5,
                  actual_time_ms=5, peak_memory_bytes=5, provider_operation_id=f"p-{ids['res'][4:]}")
        kw.update(over)
        sig = sign_receipt(key, provider_id=provider, task_id="task-1", **kw)
        return {**kw, "provider_id": provider, "task_id": "task-1", "signature": sig}

    def status(self, ids):
        return self.bridge.unae_operation(ids["op"])["status"]

    def quarantine_reasons(self):
        with closing(sqlite3.connect(self.adapter_db)) as c:
            return [r[0] for r in c.execute("SELECT reason FROM adapter_quarantine ORDER BY quarantine_id")]


class AT05_FencingTokenConversion(Base):
    def test_to_dtos_is_canonical_decimal(self):
        for n, s in ((0, "0"), (7, "7"), (10, "10"), (MAX_FENCING_TOKEN, str(MAX_FENCING_TOKEN))):
            self.assertEqual(to_dtos_token(n), s)
            self.assertEqual(from_dtos_token(s), n)

    def test_to_dtos_rejects_non_integers(self):
        for bad in (True, False, -1, 1.0, "7", None, MAX_FENCING_TOKEN + 1, MAX_SQLITE_INTEGER):
            with self.subTest(v=bad), self.assertRaises(ProtocolError):
                to_dtos_token(bad)

    def test_from_dtos_rejects_non_canonical(self):
        for bad in ("", "007", "+7", "-1", "7.0", " 7", "7 ", "7\n", "٣", "9223372036854775808", "9007199254740992", 7, None, "1" * 5000):
            with self.subTest(v=repr(bad)[:20]), self.assertRaises(ProtocolError):
                from_dtos_token(bad)

    def test_intent_carries_text_while_unae_and_correlation_keep_integer(self):
        ids = dict(op=uid(), intent=uid(), attempt=uid())
        intent = self.bridge.register_intent(operation_id=ids["op"], effect_intent_id=ids["intent"], effect_attempt_id=ids["attempt"],
                                             provider="prov-A", idempotency_key=IDEM, fencing_token=7, payload={"x": 1})
        self.assertEqual(intent.fencing_token, "7")
        self.assertIsInstance(intent.fencing_token, str)
        corr = self.adapter.get_correlation(ids["op"])
        self.assertEqual(corr["fencing_token"], 7)
        self.assertIsInstance(corr["fencing_token"], int)

    def test_string_token_never_reaches_unae(self):
        ids = self.new_op(state="UNKNOWN")
        for bad in ("1", 1.0, True, -1):
            with self.subTest(v=bad), self.assertRaises(ProtocolError):
                self.bridge.begin_reconciliation(self.msg(ids), bad)
        self.assertEqual(self.status(ids), "UNKNOWN")
        self.assertEqual(self.adapter.route_log(ids["op"]), [])


class AT09_SignedReceipts(Base):
    """AT-09: un recibo firmado y verificable desde UNKNOWN se registra por record_receipt; el commit sigue siendo aparte.
    Las claves viven solo en el keyring en memoria de UNAE; el adaptador nunca las ve."""

    def test_valid_signed_receipt_from_unknown_is_recorded_without_commit(self):
        ids = self.new_op(state="UNKNOWN")
        self.assertEqual(self.bridge.record_receipt(self.msg(ids, result="SUCCEEDED"), self.receipt(ids)), "RECORDED")
        self.assertEqual(self.status(ids), "SUCCEEDED")
        self.assertNotIn(self.status(ids), ("RECONCILED",))
        self.assertEqual(self.dtos.get_effect_attempt(ids["attempt"])["status"], "INTENDED")  # el puente no resuelve el efecto DTOS (AT-10)

    def test_key_rotation_accepts_any_current_key_and_rejects_retired_ones(self):
        old, new = b"old-secret" * 4, b"new-secret" * 4
        self.build(name="rot", keyring={"prov-A": (old, new)})
        a, b = self.new_op(state="UNKNOWN"), self.new_op(state="UNKNOWN")
        self.assertEqual(self.bridge.record_receipt(self.msg(a), self.receipt(a, key=old)), "RECORDED")
        self.assertEqual(self.bridge.record_receipt(self.msg(b), self.receipt(b, key=new)), "RECORDED")
        self.build(name="ret", keyring={"prov-A": (new,)})  # se retira la clave vieja
        c = self.new_op(state="UNKNOWN")
        self.assertEqual(self.bridge.record_receipt(self.msg(c), self.receipt(c, key=old)), "BAD_SIGNATURE")
        self.assertEqual(self.status(c), "UNKNOWN")

    def test_unknown_provider_and_foreign_provider_are_not_accepted(self):
        self.build(name="two", keyring={"prov-A": KEY_A, "prov-B": b"secret-B" * 4})
        ids = self.new_op(state="UNKNOWN")
        # proveedor sin clave en el keyring
        self.assertEqual(self.bridge.record_receipt(self.msg(ids), self.receipt(ids, rid="r1", key=b"x" * 32, provider="prov-C")), "BAD_SIGNATURE")
        # firma válida de OTRO proveedor conocido: la operación está ligada a prov-A
        self.assertEqual(self.bridge.record_receipt(self.msg(ids), self.receipt(ids, rid="r2", key=b"secret-B" * 4, provider="prov-B")), "INVALID_RECEIPT")
        self.assertEqual(self.status(ids), "UNKNOWN")

    def test_tampered_receipt_is_rejected(self):
        ids = self.new_op(state="UNKNOWN")
        r = self.receipt(ids)
        self.assertEqual(self.bridge.record_receipt(self.msg(ids), {**r, "outcome": "FAILED"}), "BAD_SIGNATURE")
        self.assertEqual(self.bridge.record_receipt(self.msg(ids), {**r, "actual_compute": 6}), "BAD_SIGNATURE")
        self.assertEqual(self.status(ids), "UNKNOWN")

    def test_replay_is_idempotent_and_conflicts_do_not_mutate(self):
        ids = self.new_op(state="UNKNOWN")
        r = self.receipt(ids)
        self.assertEqual(self.bridge.record_receipt(self.msg(ids, result="SUCCEEDED"), r), "RECORDED")
        self.assertEqual(self.bridge.record_receipt(self.msg(ids, result="SUCCEEDED"), r), "ALREADY_RECORDED")  # p. ej. tras una caída antes de registrar la ruta
        self.assertEqual(self.bridge.record_receipt(self.msg(ids), self.receipt(ids, outcome="FAILED")), "RECEIPT_CONFLICT")
        self.assertEqual(self.status(ids), "SUCCEEDED")
        self.assertEqual([r["route"] for r in self.adapter.route_log(ids["op"])], ["B", "B", "N", "N", "N", "N"])

    def test_bridge_requires_signature_by_default_even_if_store_does_not(self):
        ids = self.new_op(state="UNKNOWN", bound=False)  # operación sin proveedor ligado: el Store aceptaría un recibo sin firma
        unsigned = {k: v for k, v in self.receipt(ids).items() if k not in ("provider_id", "task_id", "signature")}
        self.assertEqual(self.bridge.record_receipt(self.msg(ids), unsigned), "UNSIGNED_REJECTED_BY_BRIDGE")
        self.assertEqual(self.status(ids), "UNKNOWN")

    def test_configuration_matrix_when_bridge_does_not_require_signatures(self):
        self.build(name="lenient", bridge_require_signed=False)
        free = self.new_op(state="UNKNOWN", bound=False)
        unsigned = {k: v for k, v in self.receipt(free).items() if k not in ("provider_id", "task_id", "signature")}
        self.assertEqual(self.bridge.record_receipt(self.msg(free), unsigned), "RECORDED")        # Store laxo + operación sin proveedor
        bound = self.new_op(state="UNKNOWN", bound=True)
        unsigned_b = {k: v for k, v in self.receipt(bound).items() if k not in ("provider_id", "task_id", "signature")}
        self.assertEqual(self.bridge.record_receipt(self.msg(bound), unsigned_b), "UNSIGNED_REJECTED")  # operación ligada: UNAE exige firma
        self.build(name="strict", store_require_signed=True, bridge_require_signed=False)
        free2 = self.new_op(state="UNKNOWN", bound=False)
        unsigned_c = {k: v for k, v in self.receipt(free2).items() if k not in ("provider_id", "task_id", "signature")}
        self.assertEqual(self.bridge.record_receipt(self.msg(free2), unsigned_c), "UNSIGNED_REJECTED")  # Store estricto
        self.assertEqual(self.status(free2), "UNKNOWN")


class OpaqueIdsEndToEnd(Base):
    def test_non_uuid_ids_work_through_both_stores(self):
        ids = dict(op="op-A1", intent="int-A1", attempt="att-A1", res="res-A1", idem=IDEM + "-A1")
        self.assertEqual(self.unae.reserve_and_register(task_id="task-1", operation_id=ids["op"], reservation_id=ids["res"],
                         idempotency_key=ids["idem"], fencing_token=1, compute_units=10, time_ms=10, memory_bytes=10), "RESERVED")
        self.assertTrue(self.unae.mark_running(ids["op"], 1, "p-A1", provider_id="prov-A"))
        self.assertTrue(self.unae.mark_unknown(ids["op"]))
        self.bridge.register_intent(operation_id=ids["op"], effect_intent_id=ids["intent"], effect_attempt_id=ids["attempt"],
                                    provider="prov-A", idempotency_key=ids["idem"], fencing_token=1, payload={})
        m = self.msg(ids, message_id="msg-A1")
        self.assertTrue(self.bridge.begin_reconciliation(m, 1))
        self.assertEqual(self.bridge.record_receipt(self.msg(ids, message_id="msg-A2", result="SUCCEEDED"),
                         self.receipt(ids, provider_operation_id="p-A1")), "RECORDED")
        self.assertEqual(self.status(ids), "SUCCEEDED")
        # el identificador es sensible a mayúsculas también en los puentes: "OP-A1" no es "op-A1"
        with self.assertRaises(CorrelationUnverifiable):
            self.bridge.verify_message(self.msg({**ids, "op": "OP-A1"}))


class RegisterIntent(Base):
    def test_idempotent_and_recovers_from_half_done_registration(self):
        ids = dict(op=uid(), intent=uid(), attempt=uid())
        kw = dict(operation_id=ids["op"], effect_intent_id=ids["intent"], effect_attempt_id=ids["attempt"], provider="prov-A",
                  idempotency_key=IDEM, fencing_token=1, payload={"x": 1})
        # caída simulada: correlación guardada pero sin intento en DTOS
        self.adapter.bind_correlation(operation_id=ids["op"], effect_intent_id=ids["intent"], effect_attempt_id=ids["attempt"],
                                      provider="prov-A", idempotency_key=IDEM, fencing_token=1, intent_payload={"x": 1})
        self.assertIsNone(self.dtos.get_effect_attempt(ids["attempt"]))
        self.bridge.register_intent(**kw)
        self.bridge.register_intent(**kw)
        row = self.dtos.get_effect_attempt(ids["attempt"])
        self.assertEqual((row["intent_id"], row["status"], row["version"]), (ids["intent"], "INTENDED", 1))

    def test_conflicting_registration_rejected(self):
        ids = self.new_op()
        with self.assertRaises(Exception):
            self.bridge.register_intent(operation_id=ids["op"], effect_intent_id=uid(), effect_attempt_id=uid(), provider="prov-A",
                                        idempotency_key=ids["idem"], fencing_token=1, payload={})
        self.assertEqual(self.adapter.get_correlation(ids["op"])["effect_intent_id"], ids["intent"])


class AT06_OperationDoesNotMatchIntent(Base):
    def test_message_op_with_foreign_intent_is_quarantined(self):
        a, b = self.new_op(), self.new_op()
        before = self.dtos.get_effect_attempt(a["attempt"])
        bad = self.msg(a, operation_id=b["op"])  # operation de B, intención/intento de A
        with self.assertRaises(CorrelationMismatch) as cm:
            self.bridge.verify_message(bad)
        self.assertEqual(cm.exception.reason, "OPERATION_INTENT_MISMATCH")
        self.assertEqual(self.quarantine_reasons(), ["OPERATION_INTENT_MISMATCH"])
        self.assertEqual(self.dtos.get_effect_attempt(a["attempt"]), before)

    def test_uncorrelated_operation_is_unverifiable(self):
        ids = dict(op=uid(), intent=uid(), attempt=uid(), idem=IDEM)
        with self.assertRaises(CorrelationUnverifiable) as cm:
            self.bridge.verify_message(self.msg(ids))
        self.assertEqual(cm.exception.reason, "OPERATION_NOT_CORRELATED")
        self.assertEqual(self.quarantine_reasons(), ["OPERATION_NOT_CORRELATED"])

    def test_ledger_bound_to_another_operation(self):
        ids = self.new_op()
        self.dtos.insert_effect(effect_id=uid(), operation_id=uid(), intent_id=ids["attempt"], attempt_id=ids["attempt"],
                                provider="prov-A", idempotency_key=ids["idem"], status="CONFIRMED")
        with self.assertRaises(CorrelationMismatch) as cm:
            self.bridge.verify_message(self.msg(ids))
        self.assertEqual(cm.exception.reason, "LEDGER_OPERATION_MISMATCH")

    def test_operation_missing_in_unae(self):
        ids = dict(op=uid(), intent=uid(), attempt=uid())
        self.bridge.register_intent(operation_id=ids["op"], effect_intent_id=ids["intent"], effect_attempt_id=ids["attempt"],
                                    provider="prov-A", idempotency_key=IDEM, fencing_token=1, payload={})
        ids["idem"] = IDEM
        with self.assertRaises(CorrelationUnverifiable) as cm:
            self.bridge.verify_message(self.msg(ids))
        self.assertEqual(cm.exception.reason, "UNAE_OPERATION_MISSING")


class AT07_AttemptDoesNotBelongToIntent(Base):
    def test_foreign_attempt_is_quarantined(self):
        a, b = self.new_op(), self.new_op()
        bad = self.msg(a, effect_attempt_id=b["attempt"])
        with self.assertRaises(CorrelationMismatch) as cm:
            self.bridge.verify_message(bad)
        self.assertEqual(cm.exception.reason, "ATTEMPT_INTENT_MISMATCH")
        self.assertEqual(self.quarantine_reasons(), ["ATTEMPT_INTENT_MISMATCH"])

    def test_dtos_side_tampering_is_detected(self):
        ids = self.new_op()
        self.dtos._conn.execute("UPDATE effect_attempts SET intent_id=? WHERE attempt_id=?", (uid(), ids["attempt"]))
        with self.assertRaises(CorrelationMismatch) as cm:
            self.bridge.verify_message(self.msg(ids))
        self.assertEqual(cm.exception.reason, "DTOS_ATTEMPT_MISMATCH")

    def test_missing_dtos_attempt_blocks_results_but_not_dispatch(self):
        ids = self.new_op()
        self.dtos._conn.execute("DELETE FROM effect_attempts WHERE attempt_id=?", (ids["attempt"],))
        with self.assertRaises(CorrelationUnverifiable) as cm:
            self.bridge.verify_message(self.msg(ids))
        self.assertEqual(cm.exception.reason, "DTOS_ATTEMPT_MISSING")
        self.bridge.verify_message(self.msg(ids, kind="EFFECT_DISPATCHED"))  # el intento aún puede no existir al despachar

    def test_reconciliation_result_checks_attempt_and_derives_intent(self):
        ids = self.new_op()
        self.assertEqual(self.bridge.verify_message(self.msg(ids, kind="RECONCILIATION_RESULT", result="NOT_FOUND"))["effect_intent_id"], ids["intent"])
        with self.assertRaises(CorrelationMismatch):
            self.bridge.verify_message(self.msg(self.new_op(), kind="RECONCILIATION_RESULT", effect_attempt_id=ids["attempt"], result="NOT_FOUND"))

    def test_idempotency_key_and_fencing_token_must_match(self):
        ids = self.new_op()
        with self.assertRaises(CorrelationMismatch) as cm:
            self.bridge.verify_message(self.msg(ids, idempotency_key="otra-clave-0123456789"))
        self.assertEqual(cm.exception.reason, "IDEMPOTENCY_KEY_MISMATCH")
        with self.assertRaises(CorrelationMismatch) as cm:
            self.bridge.verify_message(self.msg(ids, kind="EFFECT_DISPATCHED", fencing_token=2))
        self.assertEqual(cm.exception.reason, "FENCING_TOKEN_MISMATCH")


class RouteA_BeginReconciliation(Base):
    def test_unknown_to_reconciling_then_receipt_is_logged_as_route_a(self):
        ids = self.new_op(state="UNKNOWN")
        self.assertTrue(self.bridge.begin_reconciliation(self.msg(ids), 1))
        self.assertEqual(self.status(ids), "RECONCILING")
        self.assertEqual(self.bridge.record_receipt(self.msg(ids, result="SUCCEEDED"), self.receipt(ids)), "RECORDED")
        self.assertEqual(self.status(ids), "SUCCEEDED")  # recibo aceptado: ni commit ni RECONCILED (I-02, I-08)
        log = [(r["route"], r["step"], r["status_before"], r["outcome"]) for r in self.adapter.route_log(ids["op"])]
        self.assertEqual(log, [("A", "BEGIN_RECONCILIATION", "UNKNOWN", "PENDING"), ("A", "BEGIN_RECONCILIATION", "UNKNOWN", "RECONCILING"),
                               ("A", "RECORD_RECEIPT", "RECONCILING", "PENDING"), ("A", "RECORD_RECEIPT", "RECONCILING", "RECORDED")])

    def test_stale_token_is_refused_and_logged(self):
        ids = self.new_op(state="UNKNOWN")
        self.assertFalse(self.bridge.begin_reconciliation(self.msg(ids), 99))
        self.assertEqual(self.status(ids), "UNKNOWN")
        self.assertEqual([r["outcome"] for r in self.adapter.route_log(ids["op"])], ["PENDING", "REFUSED"])

    def test_unverifiable_message_never_reaches_unae(self):
        a, b = self.new_op(state="UNKNOWN"), self.new_op(state="UNKNOWN")
        with self.assertRaises(CorrelationMismatch):
            self.bridge.begin_reconciliation(self.msg(a, effect_attempt_id=b["attempt"]), 1)
        self.assertEqual((self.status(a), self.status(b)), ("UNKNOWN", "UNKNOWN"))
        self.assertEqual(self.adapter.route_log(a["op"]), [])


class RouteB_DirectReceipt(Base):
    def test_signed_receipt_from_unknown_is_route_b(self):
        ids = self.new_op(state="UNKNOWN")
        self.assertEqual(self.bridge.record_receipt(self.msg(ids, result="SUCCEEDED"), self.receipt(ids)), "RECORDED")
        self.assertEqual(self.status(ids), "SUCCEEDED")
        log = [(r["route"], r["step"], r["outcome"]) for r in self.adapter.route_log(ids["op"])]
        self.assertEqual(log, [("B", "RECORD_RECEIPT", "PENDING"), ("B", "RECORD_RECEIPT", "RECORDED")])

    def test_bad_signature_and_unsigned_leave_unknown_and_are_logged(self):
        ids = self.new_op(state="UNKNOWN")
        forged = {**self.receipt(ids), "signature": "0" * 64}
        self.assertEqual(self.bridge.record_receipt(self.msg(ids), forged), "BAD_SIGNATURE")
        unsigned = {k: v for k, v in self.receipt(ids, rid="r2").items() if k not in ("provider_id", "task_id", "signature")}
        self.assertEqual(self.bridge.record_receipt(self.msg(ids), unsigned), "UNSIGNED_REJECTED_BY_BRIDGE")
        self.assertEqual(self.status(ids), "UNKNOWN")
        self.assertEqual([r["outcome"] for r in self.adapter.route_log(ids["op"])], ["PENDING", "BAD_SIGNATURE", "UNSIGNED_REJECTED_BY_BRIDGE"])

    def test_receipt_of_another_operation_is_quarantined(self):
        a, b = self.new_op(state="UNKNOWN"), self.new_op(state="UNKNOWN")
        with self.assertRaises(CorrelationMismatch) as cm:
            self.bridge.record_receipt(self.msg(a), self.receipt(b))
        self.assertEqual(cm.exception.reason, "RECEIPT_OPERATION_MISMATCH")
        self.assertEqual((self.status(a), self.status(b)), ("UNKNOWN", "UNKNOWN"))

    def test_normal_receipt_from_running_is_route_n(self):
        ids = self.new_op(state="RUNNING")
        self.assertEqual(self.bridge.record_receipt(self.msg(ids, result="SUCCEEDED"), self.receipt(ids)), "RECORDED")
        self.assertEqual({r["route"] for r in self.adapter.route_log(ids["op"])}, {"N"})

    def test_receipt_not_applicable_in_reserved_state(self):
        ids = self.new_op(state="RESERVED")
        with self.assertRaises(ProtocolError):
            self.bridge.record_receipt(self.msg(ids, result="SUCCEEDED"), self.receipt(ids))
        self.assertEqual(self.status(ids), "RESERVED")
        self.assertEqual(self.adapter.route_log(ids["op"]), [])


class LedgerReadPath(Base):
    """El puente usa la lectura pública del ledger de DTOS si existe (P-3) y filtra siempre por intento."""

    def test_uses_public_read_when_available_and_filters_by_attempt(self):
        ids = self.new_op()
        seen = []

        def public(attempt_id):
            seen.append(attempt_id)
            return [{"operation_id": uid(), "attempt_id": attempt_id}]  # el ledger liga el intento a OTRA operación
        self.dtos.get_effects_by_attempt = public
        self.addCleanup(lambda: self.dtos.__dict__.pop("get_effects_by_attempt", None))
        with self.assertRaises(CorrelationMismatch) as cm:
            self.bridge.verify_message(self.msg(ids))
        self.assertEqual((cm.exception.reason, seen), ("LEDGER_OPERATION_MISMATCH", [ids["attempt"]]))

    def test_bridge_refuses_a_dtos_without_the_public_reads(self):
        """D-63: ya no hay respaldo con la conexión privada `_conn`; un DTOS sin la lectura pública falla AL CONSTRUIR el puente."""
        for missing in ("get_effects_by_attempt", "get_effect_by_provider_key"):
            with self.subTest(missing=missing):
                class Incomplete:
                    pass
                fake = Incomplete()
                for name in ("get_effects_by_attempt", "get_effect_by_provider_key", "get_effect_attempt"):
                    if name != missing:
                        setattr(fake, name, lambda *a, **k: None)
                with self.assertRaises(TypeError) as cm:
                    StoreBridge(self.adapter, self.unae, fake)
                self.assertIn(missing, str(cm.exception))

    def test_patched_dtos_provides_the_public_read_by_default(self):
        self.assertTrue(callable(getattr(self.dtos, "get_effects_by_attempt", None)))


class UnaeReadPath(Base):
    """El puente lee UNAE por `Store.get_operation` (parche 0004), sin SQL propio; con respaldo si el Store no la ofrece."""

    def test_uses_the_public_read_and_no_direct_sql(self):
        ids = self.new_op(state="UNKNOWN")
        calls = []

        def fake(op):  # no toca la base: si el puente abriera su propia conexión, no_sql lo delataría
            calls.append(op)
            return {"operation_id": op, "task_id": "task-1", "reservation_id": "x", "status": "UNKNOWN", "fencing_token": 1}

        def no_sql():
            raise AssertionError("el puente no debe abrir conexiones propias a UNAE")
        self.unae.get_operation = fake
        self.unae.connect = no_sql
        self.addCleanup(lambda: [self.unae.__dict__.pop(n, None) for n in ("get_operation", "connect")])
        self.assertEqual(self.bridge.unae_operation(ids["op"])["status"], "UNKNOWN")
        self.bridge.verify_message(self.msg(ids))
        self.assertEqual(calls, [ids["op"], ids["op"]])

    def test_bridge_refuses_a_unae_without_get_operation(self):
        """D-63: ya no hay respaldo con SQL propio sobre `execution_journal`; un UNAE sin `get_operation` falla AL CONSTRUIR el puente."""
        class Incomplete:
            pass
        with self.assertRaises(TypeError) as cm:
            StoreBridge(self.adapter, Incomplete(), self.dtos)
        self.assertIn("get_operation", str(cm.exception))

    def test_unae_read_returns_none_for_an_unknown_operation(self):
        self.assertIsNone(self.bridge.unae_operation("no-existe"))

    def test_patched_unae_provides_it_by_default(self):
        self.assertTrue(callable(getattr(self.unae, "get_operation", None)))


class RouteLog(Base):
    def test_route_log_is_append_only(self):
        ids = self.new_op(state="UNKNOWN")
        self.bridge.begin_reconciliation(self.msg(ids), 1)
        with closing(sqlite3.connect(self.adapter_db)) as c:
            with self.assertRaises(sqlite3.DatabaseError):
                c.execute("UPDATE reconciliation_route_log SET route='B'")
            with self.assertRaises(sqlite3.DatabaseError):
                c.execute("DELETE FROM reconciliation_route_log")


if __name__ == "__main__":
    unittest.main()
