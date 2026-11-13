"""INTEGRACIÓN del despacho (AT-16: se reporta aparte): AT-01, AT-08, AT-12, AT-13 contra Stores reales."""
import threading
import time
import unittest
from pathlib import Path

from adapter import (FencingGate, StaleFencingToken, ConfigurationError, CorrelationMismatch, DispatchAuthorization, DispatchBlocked, Dispatcher, ProtocolError, StoreBridge,
                     UNAEDTOSAdapter, TransportConfig)
from dtos.store import SQLiteStore
from test_bridge import Base, KEY_A
from unae import Store


class Crash(BaseException):
    """Simula una caída del proceso: no es Exception, así que el despachador no puede capturarla."""


class DispatchBase(Base):
    def setUp(self):
        super().setUp()
        self.dispatcher = Dispatcher(self.bridge)
        self.calls = []

    def auth(self, ids, **over):
        kw = dict(operation_id=ids["op"], effect_intent_id=ids["intent"], effect_attempt_id=ids["attempt"], fencing_token=1,
                  provider_id="prov-A", provider_operation_id=f"p-{ids['res'][4:]}", authorized_by="test-authority")
        kw.update(over)
        return DispatchAuthorization(**kw)

    def provider_ok(self, ids):
        def call(request):
            self.calls.append(request)
            return self.receipt(ids)
        return call

    def attempt_status(self, ids):
        return self.dtos.get_effect_attempt(ids["attempt"])["status"]


class AT01_NoAuthorizationNoDispatch(DispatchBase):
    def test_only_an_explicit_authorization_dispatches(self):
        ids = self.new_op(state="RESERVED")
        for not_auth in (None, {"cycle": "COMPLETED"}, "COMPLETED", ids, self.msg(ids)):
            with self.subTest(v=type(not_auth).__name__), self.assertRaises(DispatchBlocked) as cm:
                self.dispatcher.dispatch(not_auth, self.provider_ok(ids), timeout_s=1)
            self.assertEqual(cm.exception.reason, "NO_AUTHORIZATION")
        self.assertEqual(self.calls, [])
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("RESERVED", "INTENDED"))

    def test_blank_authorization_fields_are_blocked(self):
        ids = self.new_op(state="RESERVED")
        with self.assertRaises(DispatchBlocked):
            self.dispatcher.dispatch(self.auth(ids, authorized_by="  "), self.provider_ok(ids), timeout_s=1)
        self.assertEqual(self.calls, [])

    def test_registering_the_intent_never_calls_the_provider(self):
        self.new_op(state="RESERVED")
        self.assertEqual(self.calls, [])

    def test_timeout_is_mandatory_and_validated(self):
        ids = self.new_op(state="RESERVED")
        with self.assertRaises(ValueError):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids))  # sin timeout y sin configuración: no hay valor por defecto
        for bad in (0, -1, float("nan"), float("inf"), True, "5", None):
            with self.subTest(v=bad), self.assertRaises(ValueError):
                self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=bad)
        self.assertEqual(self.calls, [])


class HappyPath(DispatchBase):
    def test_receipt_recorded_without_commit_or_effect_resolution(self):
        ids = self.new_op(state="RESERVED")
        r = self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual((r.status, r.reason), ("RECEIPT_RECORDED", "RECORDED"))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0]["idempotency_key"], ids["idem"])
        self.assertEqual(self.status(ids), "SUCCEEDED")            # ni RECONCILED (I-02)
        self.assertEqual(self.attempt_status(ids), "DISPATCHED")   # el efecto de DTOS lo resuelve el reconciliador (AT-14)

    def test_same_attempt_cannot_be_dispatched_twice(self):
        ids = self.new_op(state="RESERVED")
        self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        with self.assertRaises(DispatchBlocked) as cm:
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(cm.exception.reason, "ATTEMPT_NOT_INTENDED")
        self.assertEqual(len(self.calls), 1)


class AT12_InvalidFencingBlocksDispatch(DispatchBase):
    def test_token_different_from_registered_is_quarantined(self):
        ids = self.new_op(state="RESERVED")
        with self.assertRaises(CorrelationMismatch) as cm:
            self.dispatcher.dispatch(self.auth(ids, fencing_token=5), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(cm.exception.reason, "FENCING_TOKEN_MISMATCH")
        self.assertEqual(self.calls, [])
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("RESERVED", "INTENDED"))

    def test_new_authority_blocks_the_old_owner(self):
        ids = self.new_op(state="RESERVED")
        self.assertEqual(self.unae.acquire_authority("task-1", "other-owner"), 2)  # UNAE cancela las RESERVED del dueño anterior
        with self.assertRaises(DispatchBlocked) as cm:
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertTrue(cm.exception.reason.startswith("OPERATION_"))
        self.assertEqual(self.calls, [])
        self.assertEqual(self.attempt_status(ids), "INTENDED")

    def test_unknown_token_values_are_rejected_before_anything(self):
        ids = self.new_op(state="RESERVED")
        for bad in (-1, 1.5, "1", True):
            with self.subTest(v=bad), self.assertRaises(ProtocolError):
                self.dispatcher.dispatch(self.auth(ids, fencing_token=bad), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(self.calls, [])


class AT08_UnknownNeverRetries(DispatchBase):
    def assert_unknown_and_blocked(self, ids, reason_prefix):
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"))  # DTOS: UNKNOWN -> RECONCILIATION_REQUIRED
        n = len(self.calls)
        with self.assertRaises(DispatchBlocked):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(len(self.calls), n)  # el proveedor no se vuelve a llamar

    def test_provider_error_leaves_unknown_without_retry(self):
        ids = self.new_op(state="RESERVED")

        def boom(request):
            self.calls.append(request)
            raise ConnectionError("se cortó la conexión")
        r = self.dispatcher.dispatch(self.auth(ids), boom, timeout_s=5)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "PROVIDER_ERROR:ConnectionError"))
        self.assertEqual(len(self.calls), 1)
        self.assert_unknown_and_blocked(ids, "PROVIDER_ERROR")

    def test_timeout_leaves_unknown_without_retry(self):
        ids = self.new_op(state="RESERVED")
        release = threading.Event()
        self.addCleanup(release.set)

        def slow(request):
            self.calls.append(request)
            release.wait(5)
            return self.receipt(ids)
        t0 = time.monotonic()
        r = self.dispatcher.dispatch(self.auth(ids), slow, timeout_s=0.1)
        self.assertLess(time.monotonic() - t0, 2)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "TIMEOUT"))
        self.assert_unknown_and_blocked(ids, "TIMEOUT")

    def test_invalid_receipt_leaves_unknown(self):
        ids = self.new_op(state="RESERVED")
        r = self.dispatcher.dispatch(self.auth(ids), lambda req: {**self.receipt(ids), "signature": "0" * 64}, timeout_s=5)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "RECEIPT_BAD_SIGNATURE"))
        self.assert_unknown_and_blocked(ids, "RECEIPT")

    def test_receipt_of_another_operation_leaves_unknown_and_raises(self):
        a, b = self.new_op(state="RESERVED"), self.new_op(state="RESERVED")
        with self.assertRaises(CorrelationMismatch):
            self.dispatcher.dispatch(self.auth(a), lambda req: self.receipt(b), timeout_s=5)
        self.assertEqual((self.status(a), self.status(b)), ("UNKNOWN", "RESERVED"))

    def test_no_substitute_operation_with_the_same_idempotency_key(self):
        ids = self.new_op(state="RESERVED")
        self.dispatcher.dispatch(self.auth(ids), lambda req: (_ for _ in ()).throw(RuntimeError("x")), timeout_s=5)
        again = self.unae.reserve_and_register(task_id="task-1", operation_id="otra-operacion", reservation_id="otra-reserva",
                                               idempotency_key=ids["idem"], fencing_token=1, compute_units=10, time_ms=10, memory_bytes=10)
        self.assertEqual(again, "KEY_CONFLICT")
        self.assertIsNone(self.bridge.unae_operation("otra-operacion"))


class AT13_CrashBetweenDispatchAndReceipt(DispatchBase):
    def test_restart_never_repeats_the_dispatch(self):
        ids = self.new_op(state="RESERVED")

        def crash(request):
            self.calls.append(request)
            raise Crash()
        with self.assertRaises(Crash):
            self.dispatcher.dispatch(self.auth(ids), crash, timeout_s=5)
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("RUNNING", "DISPATCHED"))  # estado tal como quedaría tras una caída
        # --- reinicio: nuevas instancias sobre los mismos archivos ---
        d = Path(self._tmp.name)
        unae2 = Store(str(d / "u.sqlite"), keyring={"prov-A": KEY_A})
        dtos2 = SQLiteStore(d / "d.sqlite")
        self.addCleanup(unae2.close)
        self.addCleanup(dtos2.close)
        dispatcher2 = Dispatcher(StoreBridge(UNAEDTOSAdapter(str(d / "a.sqlite")), unae2, dtos2))
        self.assertEqual(dispatcher2.recover_after_restart(), {"unae_operations_to_unknown": 1, "dtos_attempts_to_unknown": 1, "dtos_undispatched_to_unknown": 0, "routes_closed": 0})
        self.assertEqual(dispatcher2.bridge.unae_operation(ids["op"])["status"], "UNKNOWN")
        self.assertEqual(dtos2.get_effect_attempt(ids["attempt"])["status"], "RECONCILIATION_REQUIRED")
        with self.assertRaises(DispatchBlocked):
            dispatcher2.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(len(self.calls), 1)  # una sola llamada en toda la historia
        self.assertEqual(dispatcher2.recover_after_restart(), {"unae_operations_to_unknown": 0, "dtos_attempts_to_unknown": 0, "dtos_undispatched_to_unknown": 0, "routes_closed": 0})  # idempotente

    def test_crash_before_provider_call_is_also_unknown_not_retried(self):
        ids = self.new_op(state="RESERVED")
        self.unae.mark_running(ids["op"], 1, "p-x", provider_id="prov-A")  # UNAE RUNNING, DTOS aún INTENDED (caída entre ambos pasos)
        rec = self.dispatcher.recover_after_restart()
        self.assertEqual((rec["unae_operations_to_unknown"], rec["dtos_undispatched_to_unknown"]), (1, 1))  # C-1 (r10): ya no queda INTENDED
        self.assertEqual(self.attempt_status(ids), "RECONCILIATION_REQUIRED")
        with self.assertRaises(DispatchBlocked):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(self.calls, [])


class ConfiguredTimeouts(DispatchBase):
    """El plazo sale de la TransportConfig por provider_id; sin configuración para el proveedor no se toca nada."""

    def cfg(self, **providers):
        return TransportConfig.from_dict({"providers": providers})

    def test_total_timeout_comes_from_the_configuration_and_timeouts_reach_the_provider(self):
        ids = self.new_op(state="RESERVED")
        d = Dispatcher(self.bridge, self.cfg(**{"prov-A": {"connect_timeout_s": 0.5, "read_timeout_s": 1.0, "total_timeout_s": 3.0}}))
        r = d.dispatch(self.auth(ids), self.provider_ok(ids))
        self.assertEqual(r.status, "RECEIPT_RECORDED")
        self.assertEqual(self.calls[0]["timeouts"], {"connect_timeout_s": 0.5, "read_timeout_s": 1.0, "total_timeout_s": 3.0})

    def test_configured_total_timeout_is_enforced(self):
        ids = self.new_op(state="RESERVED")
        release = threading.Event()
        self.addCleanup(release.set)

        def slow(request):
            self.calls.append(request)
            release.wait(5)
            return self.receipt(ids)
        d = Dispatcher(self.bridge, self.cfg(**{"prov-A": {"connect_timeout_s": 0.05, "read_timeout_s": 0.1, "total_timeout_s": 0.2}}))
        t0 = time.monotonic()
        r = d.dispatch(self.auth(ids), slow)
        self.assertLess(time.monotonic() - t0, 2)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "TIMEOUT"))
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"))

    def test_unconfigured_provider_stops_before_touching_anything(self):
        ids = self.new_op(state="RESERVED")
        for transport in (self.cfg(), self.cfg(**{"prov-B": {"connect_timeout_s": 1, "read_timeout_s": 1, "total_timeout_s": 2}})):
            with self.assertRaises(ConfigurationError):
                Dispatcher(self.bridge, transport).dispatch(self.auth(ids), self.provider_ok(ids))
        self.assertEqual(self.calls, [])
        self.assertEqual((self.status(ids), self.attempt_status(ids), self.quarantine_reasons()), ("RESERVED", "INTENDED", []))

    def test_explicit_timeout_cannot_bypass_the_configuration(self):
        ids = self.new_op(state="RESERVED")
        d = Dispatcher(self.bridge, self.cfg(**{"prov-A": {"connect_timeout_s": 1, "read_timeout_s": 1, "total_timeout_s": 2}}))
        with self.assertRaises(ValueError):
            d.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=999)
        self.assertEqual((self.calls, self.status(ids)), ([], "RESERVED"))

    def test_authorization_is_checked_before_the_configuration(self):
        ids = self.new_op(state="RESERVED")
        with self.assertRaises(DispatchBlocked) as cm:
            Dispatcher(self.bridge, self.cfg()).dispatch(self.auth(ids, provider_id="  "), self.provider_ok(ids))
        self.assertEqual(cm.exception.reason, "NO_AUTHORIZATION")

    def test_without_configuration_the_explicit_timeout_still_works(self):
        ids = self.new_op(state="RESERVED")
        self.assertEqual(Dispatcher(self.bridge).dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5).status, "RECEIPT_RECORDED")
        self.assertNotIn("timeouts", self.calls[0])


class CooperativeCancellation(DispatchBase):
    """Hallazgo 5 (D-55): al vencer el plazo el proveedor recibe la señal de cancelación; un recibo tardío nunca se registra."""

    def assert_unknown_and_blocked(self, ids):
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"))
        n = len(self.calls)
        with self.assertRaises(DispatchBlocked):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(len(self.calls), n)  # el proveedor no se vuelve a llamar

    def run_late_provider(self, ids, *, honours_cancel):
        """Proveedor lento. Devuelve (state, thread-finished Event). `state['applied']` = llegó a «aplicar el efecto»."""
        state = {"applied": False, "saw_cancel": None}
        finished = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        self.addCleanup(finished.wait, 5)

        def provider(request):
            self.calls.append(request)
            try:
                if honours_cancel:
                    state["saw_cancel"] = request["cancel"].wait(5)   # espera ACTIVA a la señal, no un sleep ciego
                    if request["cancel"].is_set():
                        return None                                     # paso irreversible omitido
                else:
                    release.wait(0.5)                                   # ignora la señal
                state["applied"] = True                                 # el «efecto» externo
                return self.receipt(ids)
            finally:
                finished.set()
        state["release"] = release
        return provider, state, finished

    def test_provider_receives_a_cancel_event_and_an_absolute_deadline(self):
        ids = self.new_op(state="RESERVED")
        seen = {}

        def provider(request):
            seen["cancel_type"] = type(request["cancel"])
            seen["set_during_call"] = request["cancel"].is_set()
            seen["left"] = request["deadline"] - time.monotonic()
            return self.receipt(ids)
        self.assertEqual(self.dispatcher.dispatch(self.auth(ids), provider, timeout_s=5).status, "RECEIPT_RECORDED")
        self.assertIs(seen["cancel_type"], threading.Event)
        self.assertFalse(seen["set_during_call"])           # mientras hay plazo, la señal NO está activada
        self.assertTrue(0 < seen["left"] <= 5)

    def test_configured_transport_also_gets_cancel_and_deadline(self):
        ids = self.new_op(state="RESERVED")
        cfg = TransportConfig.from_dict({"providers": {"prov-A": {"connect_timeout_s": 0.5, "read_timeout_s": 1.0, "total_timeout_s": 3.0}}})
        Dispatcher(self.bridge, cfg).dispatch(self.auth(ids), self.provider_ok(ids))
        self.assertIsInstance(self.calls[0]["cancel"], threading.Event)
        self.assertIn("deadline", self.calls[0])

    def test_timeout_signals_the_provider_before_marking_unknown_and_a_cooperative_provider_applies_nothing(self):
        ids = self.new_op(state="RESERVED")
        provider, state, finished = self.run_late_provider(ids, honours_cancel=True)
        order = []
        real_unknown = self.dispatcher._unknown

        def spy(a, message, reason):
            order.append(self.calls[0]["cancel"].is_set())  # ¿la señal ya estaba activada cuando se marca UNKNOWN?
            return real_unknown(a, message, reason)
        self.dispatcher._unknown = spy  # type: ignore[method-assign]
        r = self.dispatcher.dispatch(self.auth(ids), provider, timeout_s=0.1)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "TIMEOUT"))
        self.assertEqual(order, [True], "la señal debe activarse ANTES de marcar UNKNOWN")
        self.assertTrue(finished.wait(2), "el hilo del proveedor debe terminar al recibir la señal, no quedar colgado")
        self.assertEqual((state["saw_cancel"], state["applied"]), (True, False))  # no hay efecto zombie
        self.assert_unknown_and_blocked(ids)

    def test_signal_is_set_on_provider_error_and_after_success(self):
        for outcome in ("error", "ok"):
            ids = self.new_op(state="RESERVED")
            holder = {}

            def provider(request, outcome=outcome, ids=ids):
                holder["cancel"] = request["cancel"]
                if outcome == "error":
                    raise RuntimeError("boom")
                return self.receipt(ids)
            self.dispatcher.dispatch(self.auth(ids), provider, timeout_s=5)
            self.assertTrue(holder["cancel"].is_set(), f"la señal debe quedar activada tras terminar la llamada ({outcome})")

    def test_a_provider_that_ignores_the_signal_can_still_apply_late_but_the_late_receipt_is_never_recorded(self):
        """LÍMITE DOCUMENTADO: la cancelación es cooperativa. Un proveedor que la ignora aplica el efecto tarde; el sistema lo trata como
        incierto (UNKNOWN) y solo la reconciliación puede resolverlo. Esta prueba fija ese comportamiento para que no pase por silencio."""
        ids = self.new_op(state="RESERVED")
        provider, state, finished = self.run_late_provider(ids, honours_cancel=False)
        r = self.dispatcher.dispatch(self.auth(ids), provider, timeout_s=0.1)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "TIMEOUT"))
        self.assertTrue(finished.wait(3))
        self.assertTrue(state["applied"])                   # el efecto externo SÍ ocurrió (zombie de un proveedor no cooperativo)...
        self.assert_unknown_and_blocked(ids)     # ...pero el motor lo mantiene UNKNOWN y bloqueado: nada registró el recibo tardío
        self.assertEqual(self.attempt_status(ids), "RECONCILIATION_REQUIRED")


class FencingToTheProvider(DispatchBase):
    """Hallazgo 4 (D-56): el token llega al proveedor, éste puede rechazar líderes reemplazados y devolver el token en el recibo."""

    def test_request_carries_the_token_and_the_task_scope(self):
        ids = self.new_op(state="RESERVED")
        self.dispatcher.dispatch(self.auth(ids, fencing_token=1), self.provider_ok(ids), timeout_s=5)
        self.assertEqual((self.calls[0]["fencing_token"], self.calls[0]["fencing_scope"]), (1, "task-1"))
        self.assertIs(type(self.calls[0]["fencing_token"]), int)

    def fenced_provider(self, ids, gate, applied):
        def provider(request):
            self.calls.append(request)
            gate.admit(request)               # el proveedor rechaza ANTES de aplicar
            applied.append(request["fencing_token"])
            return self.receipt(ids)
        return provider

    def test_replaced_leader_is_rejected_by_the_provider_and_nothing_is_applied(self):
        """El líder viejo (token 1) pasó la comprobación de UNAE y salió hacia el proveedor, pero el nuevo líder (token 2) ya llegó antes."""
        ids = self.new_op(state="RESERVED")
        gate, applied = FencingGate(), []
        gate.admit({"fencing_token": 2, "fencing_scope": "task-1"})  # el líder nuevo ya actuó sobre esta Task
        r = self.dispatcher.dispatch(self.auth(ids, fencing_token=1), self.fenced_provider(ids, gate, applied), timeout_s=5)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "PROVIDER_ERROR:StaleFencingToken"))
        self.assertEqual(applied, [])                                 # el efecto NO se aplicó
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"))
        self.assertEqual(len(self.calls), 1)                          # y no hubo reintento

    def test_current_leader_with_an_equal_token_is_accepted(self):
        ids = self.new_op(state="RESERVED")
        gate, applied = FencingGate(), []
        gate.admit({"fencing_token": 1, "fencing_scope": "task-1"})   # otra operación de la misma autoridad ya pasó con el mismo token
        r = self.dispatcher.dispatch(self.auth(ids, fencing_token=1), self.fenced_provider(ids, gate, applied), timeout_s=5)
        self.assertEqual((r.status, applied), ("RECEIPT_RECORDED", [1]))

    def test_matching_echo_is_accepted_and_removed_before_unae_sees_the_receipt(self):
        ids = self.new_op(state="RESERVED")

        def provider(request):
            return {**self.receipt(ids), "fencing_token": request["fencing_token"]}  # UNAE no acepta ese campo: el despachador lo retira
        r = Dispatcher(self.bridge, require_fencing_echo=True).dispatch(self.auth(ids), provider, timeout_s=5)
        self.assertEqual(r.status, "RECEIPT_RECORDED")

    def test_wrong_echo_leaves_unknown_and_records_no_receipt(self):
        for i, bad in enumerate((2, 0, True, "1", 1.0, None)):
            self.build(name=f"-echo{i}")  # pila nueva por valor: las operaciones UNKNOWN retienen reservas y la cuenta admite 4 a la vez
            self.dispatcher = Dispatcher(self.bridge)
            ids = self.new_op(state="RESERVED")

            def provider(request, bad=bad, ids=ids):
                return {**self.receipt(ids), "fencing_token": bad}
            r = self.dispatcher.dispatch(self.auth(ids), provider, timeout_s=5)
            self.assertEqual((r.status, r.reason), ("UNKNOWN", "RECEIPT_FENCING_MISMATCH"), repr(bad))
            self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"), repr(bad))

    def test_missing_echo_is_optional_by_default_and_required_on_demand(self):
        ids = self.new_op(state="RESERVED")
        self.assertEqual(self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5).status, "RECEIPT_RECORDED")  # retrocompatible
        ids2 = self.new_op(state="RESERVED")
        r = Dispatcher(self.bridge, require_fencing_echo=True).dispatch(self.auth(ids2), self.provider_ok(ids2), timeout_s=5)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "RECEIPT_FENCING_MISSING"))

    def test_provider_that_ignores_fencing_is_not_protected_documented_limit(self):
        """LÍMITE DOCUMENTADO: el token solo protege si el proveedor lo comprueba. Uno que lo ignora aplica el efecto de un líder reemplazado."""
        ids = self.new_op(state="RESERVED")
        applied = []

        def careless(request):
            applied.append(request["fencing_token"])  # no consulta ninguna puerta
            return self.receipt(ids)
        r = self.dispatcher.dispatch(self.auth(ids, fencing_token=1), careless, timeout_s=5)
        self.assertEqual((r.status, applied), ("RECEIPT_RECORDED", [1]))


if __name__ == "__main__":
    unittest.main()


class C1_FailureBetweenMarkRunningAndDtosDispatch(DispatchBase):
    """C-1 (informe g0.8 r9): si el registro del despacho en DTOS falla DESPUÉS de `mark_running`, la operación debe poder salir sola."""

    def break_dtos_dispatch(self, exc=RuntimeError("database is locked"), times=None):
        original = self.bridge.effects.dispatch
        state = {"left": times}

        def boom(attempt_id):
            if state["left"] is None or state["left"] > 0:
                if state["left"] is not None:
                    state["left"] -= 1
                raise exc
            return original(attempt_id)
        self.bridge.effects.dispatch = boom
        self.addCleanup(lambda: setattr(self.bridge.effects, "dispatch", original))
        return original

    def test_transient_failure_ends_in_reconciliation_required(self):
        ids = self.new_op(state="RESERVED")
        self.break_dtos_dispatch(times=1)  # falla solo la primera vez: el despachador puede cerrar el intento en el acto
        with self.assertRaises(DispatchBlocked) as cm:
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(cm.exception.reason, "DISPATCH_RECORD_FAILED")
        self.assertEqual(self.calls, [])  # el proveedor nunca se llamó
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"))
        self.assertEqual(self.dispatcher.recover_after_restart(), {"unae_operations_to_unknown": 0, "dtos_attempts_to_unknown": 0, "dtos_undispatched_to_unknown": 0, "routes_closed": 0})

    def test_persistent_failure_is_finished_by_recovery(self):
        ids = self.new_op(state="RESERVED")
        original = self.break_dtos_dispatch()  # DTOS sigue fallando: el intento queda INTENDED
        with self.assertRaises(DispatchBlocked) as cm:
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(cm.exception.reason, "DISPATCH_RECORD_FAILED")
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "INTENDED"))
        self.bridge.effects.dispatch = original  # DTOS se recupera
        rec = self.dispatcher.recover_after_restart()
        self.assertEqual(rec["dtos_undispatched_to_unknown"], 1)
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"))
        self.assertEqual(self.dispatcher.recover_after_restart()["dtos_undispatched_to_unknown"], 0)  # idempotente
        self.assertEqual(self.calls, [])

    def test_recovery_leaves_the_attempt_alone_while_dtos_keeps_failing(self):
        ids = self.new_op(state="RESERVED")
        self.break_dtos_dispatch()
        with self.assertRaises(DispatchBlocked):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(self.dispatcher.recover_after_restart()["dtos_undispatched_to_unknown"], 0)  # no lanza ni inventa estados
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "INTENDED"))

    def test_unae_mark_unknown_failing_too_is_recovered_after_restart(self):
        ids = self.new_op(state="RESERVED")
        self.break_dtos_dispatch()
        original_unknown = self.unae.mark_unknown
        self.unae.mark_unknown = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("database is locked"))
        try:
            with self.assertRaises(DispatchBlocked):
                self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        finally:
            self.unae.mark_unknown = original_unknown
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("RUNNING", "INTENDED"))
        self.bridge.effects.dispatch = type(self.bridge.effects).dispatch.__get__(self.bridge.effects)
        rec = self.dispatcher.recover_after_restart()
        self.assertEqual((rec["unae_operations_to_unknown"], rec["dtos_undispatched_to_unknown"]), (1, 1))
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"))

    def test_crash_in_dtos_dispatch_is_recovered_like_any_crash(self):
        ids = self.new_op(state="RESERVED")
        self.break_dtos_dispatch(exc=Crash())  # BaseException: no se captura, como una caída real
        with self.assertRaises(Crash):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("RUNNING", "INTENDED"))
        del self.bridge.effects.dispatch
        rec = self.dispatcher.recover_after_restart()
        self.assertEqual((rec["unae_operations_to_unknown"], rec["dtos_undispatched_to_unknown"]), (1, 1))
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"))

    def test_the_recovered_operation_enters_the_reconciliation_path_and_is_never_redispatched(self):
        from adapter import Reconciler
        ids = self.new_op(state="RESERVED")
        self.break_dtos_dispatch(times=1)
        with self.assertRaises(DispatchBlocked):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        with self.assertRaises(DispatchBlocked) as cm:
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(cm.exception.reason, "ATTEMPT_NOT_INTENDED")
        out = Reconciler(self.bridge).apply(self.msg(ids, kind="RECONCILIATION_RESULT", result="NOT_FOUND", evidence_refs=["ev:q"]),
                                            strategy="PROVIDER_QUERY", reconciler_id="test-reconciler")
        self.assertEqual((out.status, out.dtos_status), ("INCONCLUSIVE", "RECONCILIATION_REQUIRED"))  # el reconciliador ya puede actuar
        self.assertEqual(self.calls, [])


class A1_MalformedReceiptsAndProviderTimeoutError(DispatchBase):
    """A-1 (informe g0.8 r9): un recibo inutilizable es un resultado UNKNOWN tipificado, no una excepción. Y un TimeoutError del PROVEEDOR
    no se confunde con el vencimiento del plazo (desde Python 3.11 `concurrent.futures.TimeoutError` es el `TimeoutError` incorporado)."""

    def run_with(self, provider):
        ids = self.new_op(state="RESERVED")
        r = self.dispatcher.dispatch(self.auth(ids), provider, timeout_s=5)
        return ids, r

    def assert_uncertain(self, ids):
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("UNKNOWN", "RECONCILIATION_REQUIRED"))

    def test_malformed_receipts_return_unknown_without_raising(self):
        extra = lambda ids: {**self.receipt(ids), "zzz": 1}
        missing = lambda ids: {k: v for k, v in self.receipt(ids).items() if k != "outcome"}
        for label, make in (("None", lambda ids: None), ("str", lambda ids: "ok"), ("list", lambda ids: [1]), ("int", lambda ids: 7),
                            ("empty_dict", lambda ids: {}), ("extra_key", extra), ("missing_key", missing),
                            ("non_str_key", lambda ids: {**self.receipt(ids), 1: 2})):
            with self.subTest(receipt=label):
                self.build(name=f"-{label}")
                self.dispatcher = Dispatcher(self.bridge)
                ids = self.new_op(state="RESERVED")
                r = self.dispatcher.dispatch(self.auth(ids), lambda request, ids=ids, make=make: make(ids), timeout_s=5)
                self.assertEqual((r.status, r.reason), ("UNKNOWN", "RECEIPT_MALFORMED"))
                self.assert_uncertain(ids)
                with self.assertRaises(DispatchBlocked):
                    self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)

    def test_a_valid_receipt_is_still_recorded(self):
        ids = self.new_op(state="RESERVED")
        r = self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual((r.status, r.reason), ("RECEIPT_RECORDED", "RECORDED"))

    def test_a_malformed_receipt_with_fencing_echo_is_also_malformed(self):
        self.dispatcher = Dispatcher(self.bridge, require_fencing_echo=True)
        ids = self.new_op(state="RESERVED")
        r = self.dispatcher.dispatch(self.auth(ids), lambda request: "ok", timeout_s=5)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "RECEIPT_MALFORMED"))

    def test_provider_timeout_error_is_a_provider_error_not_the_deadline(self):
        import socket
        for n, exc in enumerate((TimeoutError("boom"), socket.timeout("read timed out"))):  # socket.timeout es un alias de TimeoutError
            with self.subTest(exc=type(exc).__name__, n=n):
                self.build(name=f"-te{n}")
                self.dispatcher = Dispatcher(self.bridge)

                def provider(request, exc=exc):
                    raise exc
                ids = self.new_op(state="RESERVED")
                r = self.dispatcher.dispatch(self.auth(ids), provider, timeout_s=5)
                self.assertEqual((r.status, r.reason), ("UNKNOWN", "PROVIDER_ERROR:TimeoutError"))
                self.assert_uncertain(ids)

    def test_the_real_deadline_is_still_reported_as_timeout(self):
        ids = self.new_op(state="RESERVED")
        release = threading.Event()
        self.addCleanup(release.set)

        def slow(request):
            release.wait(2)
            return self.receipt(ids)
        r = self.dispatcher.dispatch(self.auth(ids), slow, timeout_s=0.2)
        self.assertEqual((r.status, r.reason), ("UNKNOWN", "TIMEOUT"))
        self.assert_uncertain(ids)

    def test_a_base_exception_from_the_provider_still_propagates_like_a_crash(self):
        ids = self.new_op(state="RESERVED")

        def crash(request):
            raise Crash()
        with self.assertRaises(Crash):
            self.dispatcher.dispatch(self.auth(ids), crash, timeout_s=5)
        self.assertEqual((self.status(ids), self.attempt_status(ids)), ("RUNNING", "DISPATCHED"))


class C_Hygiene_ProviderThreads(DispatchBase):
    """M-3 (informe g0.8 r9, D-62): hilos del proveedor daemon y acotados por proveedor."""

    def wait_idle(self, dispatcher, provider_id="prov-A", limit=3.0):
        end = time.monotonic() + limit
        while dispatcher.inflight(provider_id) and time.monotonic() < end:
            time.sleep(0.01)
        return dispatcher.inflight(provider_id)

    def test_provider_runs_in_a_daemon_thread(self):
        ids = self.new_op(state="RESERVED")
        seen = []

        def provider(request):
            seen.append((threading.current_thread().daemon, threading.current_thread().name))
            return self.receipt(ids)
        self.assertEqual(self.dispatcher.dispatch(self.auth(ids), provider, timeout_s=5).status, "RECEIPT_RECORDED")
        self.assertEqual(seen, [(True, "adapter-provider-prov-A")])

    def test_a_hung_provider_does_not_block_interpreter_exit(self):
        """Con el ThreadPoolExecutor de r11 este proceso se quedaba esperando al hilo colgado al salir."""
        import subprocess, sys, textwrap
        src = str(Path(__file__).resolve().parents[2] / "src")
        here = str(Path(__file__).resolve().parent)
        script = textwrap.dedent(f"""
            import sys, threading
            sys.path[:0] = [{src!r}, {here!r}]
            from test_dispatch import DispatchBase
            class P(DispatchBase):
                def runTest(self): pass
            p = P(); p.setUp()
            ids = p.new_op(state="RESERVED")
            r = p.dispatcher.dispatch(p.auth(ids), lambda request: threading.Event().wait(), timeout_s=0.2)
            print(r.status, r.reason, p.dispatcher.inflight("prov-A"), flush=True)
        """)
        t0 = time.monotonic()
        out = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr[-800:])
        self.assertIn("UNKNOWN TIMEOUT 1", out.stdout)
        self.assertLess(time.monotonic() - t0, 30)

    def test_in_flight_calls_are_capped_per_provider_before_touching_any_store(self):
        d = Dispatcher(self.bridge, max_inflight_per_provider=2)
        release = threading.Event()
        self.addCleanup(release.set)
        calls = []

        def hung(request):
            calls.append(1)
            release.wait(5)
            return self.receipt(ids3)
        ids1, ids2, ids3 = (self.new_op(state="RESERVED") for _ in range(3))
        for ids in (ids1, ids2):
            r = d.dispatch(self.auth(ids), hung, timeout_s=0.1)
            self.assertEqual((r.status, r.reason), ("UNKNOWN", "TIMEOUT"))
        self.assertEqual(d.inflight("prov-A"), 2)  # vencieron el plazo pero sus hilos siguen vivos
        with self.assertRaises(DispatchBlocked) as cm:
            d.dispatch(self.auth(ids3), hung, timeout_s=0.1)
        self.assertEqual(cm.exception.reason, "PROVIDER_SATURATED")
        self.assertEqual((self.status(ids3), self.attempt_status(ids3), len(calls)), ("RESERVED", "INTENDED", 2))  # nada se tocó, el proveedor no se llamó
        release.set()
        self.assertEqual(self.wait_idle(d), 0)
        ids3_receipt = self.provider_ok(ids3)
        self.assertEqual(d.dispatch(self.auth(ids3), ids3_receipt, timeout_s=5).status, "RECEIPT_RECORDED")  # la plaza se liberó al terminar el hilo

    def test_slot_is_released_after_every_kind_of_exit(self):
        d = Dispatcher(self.bridge, max_inflight_per_provider=1)
        ids = self.new_op(state="RESERVED")
        self.assertEqual(d.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5).status, "RECEIPT_RECORDED")
        self.assertEqual(self.wait_idle(d), 0)
        ids = self.new_op(state="RESERVED")

        def boom(request):
            raise ConnectionError("x")
        self.assertEqual(d.dispatch(self.auth(ids), boom, timeout_s=5).reason, "PROVIDER_ERROR:ConnectionError")
        self.assertEqual(self.wait_idle(d), 0)
        with self.assertRaises(ProtocolError):  # rechazo ANTES del proveedor (token distinto del registrado): la plaza tomada se devuelve
            d.dispatch(self.auth(self.new_op(state="RESERVED"), fencing_token=9), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(d.inflight("prov-A"), 0)
        ids = self.new_op(state="RESERVED")
        with self.assertRaises(Crash):
            d.dispatch(self.auth(ids), lambda request: (_ for _ in ()).throw(Crash()), timeout_s=5)
        self.assertEqual(self.wait_idle(d), 0)

    def test_the_cap_is_per_provider(self):
        d = Dispatcher(self.bridge, max_inflight_per_provider=1)
        d._inflight["otro-proveedor"] = 1  # otro proveedor saturado no afecta a prov-A
        ids = self.new_op(state="RESERVED")
        self.assertEqual(d.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5).status, "RECEIPT_RECORDED")

    def test_invalid_cap_is_rejected(self):
        for bad in (0, -1, True, 1.5, "8", None):
            with self.subTest(v=bad), self.assertRaises(ValueError):
                Dispatcher(self.bridge, max_inflight_per_provider=bad)

    def test_explicit_timeout_has_an_upper_bound(self):
        from adapter import MAX_PROVIDER_TIMEOUT_S
        ids = self.new_op(state="RESERVED")
        for bad in (MAX_PROVIDER_TIMEOUT_S + 1, 1e12, 1e300):
            with self.subTest(v=bad), self.assertRaises(ValueError):
                self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=bad)
        self.assertEqual((self.status(ids), self.calls), ("RESERVED", []))


class M4_PendingRoutesAreRecovered(DispatchBase):
    """M-4 (informe g0.8 r9, D-62): una fila PENDING de la bitácora de rutas no puede quedar abierta para siempre."""

    def crashing_unae(self, name, *, after):
        original = getattr(self.unae, name)

        def wrapper(*a, **k):
            result = original(*a, **k) if after else None
            raise Crash()
        setattr(self.unae, name, wrapper)
        self.addCleanup(lambda: setattr(self.unae, name, original))

    def outcomes(self, ids):
        return [(r["step"], r["outcome"]) for r in self.adapter.route_log(ids["op"])]

    def test_crash_before_unae_records_the_receipt_closes_with_no_state_change(self):
        ids = self.new_op(state="RESERVED")
        self.crashing_unae("record_receipt", after=False)
        with self.assertRaises(Crash):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(self.outcomes(ids), [("RECORD_RECEIPT", "PENDING")])
        self.assertEqual(len(self.adapter.unfinished_routes()), 1)
        rec = self.dispatcher.recover_after_restart()
        self.assertEqual(rec["routes_closed"], 1)
        self.assertEqual(self.outcomes(ids), [("RECORD_RECEIPT", "PENDING"), ("RECORD_RECEIPT", "RECOVERED:NO_STATE_CHANGE")])
        self.assertEqual((self.adapter.unfinished_routes(), self.dispatcher.recover_after_restart()["routes_closed"]), ([], 0))  # idempotente
        self.assertEqual(self.status(ids), "UNKNOWN")  # el RUNNING huérfano lo resolvió después `recover_interrupted`

    def test_crash_after_unae_recorded_the_receipt_closes_with_the_advanced_state(self):
        ids = self.new_op(state="RESERVED")
        self.crashing_unae("record_receipt", after=True)
        with self.assertRaises(Crash):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(self.status(ids), "SUCCEEDED")
        self.assertEqual(self.dispatcher.recover_after_restart()["routes_closed"], 1)
        self.assertEqual(self.outcomes(ids)[-1], ("RECORD_RECEIPT", "RECOVERED:STATE_ADVANCED:SUCCEEDED"))
        self.assertEqual(self.status(ids), "SUCCEEDED")  # recuperar la bitácora no muta UNAE

    def test_an_exception_from_unae_closes_the_route_inline(self):
        ids = self.new_op(state="RESERVED")
        original = self.unae.record_receipt
        self.unae.record_receipt = lambda **k: (_ for _ in ()).throw(RuntimeError("database is locked"))
        self.addCleanup(lambda: setattr(self.unae, "record_receipt", original))
        with self.assertRaises(RuntimeError):
            self.dispatcher.dispatch(self.auth(ids), self.provider_ok(ids), timeout_s=5)
        self.assertEqual(self.outcomes(ids), [("RECORD_RECEIPT", "PENDING"), ("RECORD_RECEIPT", "ERROR:RuntimeError")])
        self.assertEqual(self.adapter.unfinished_routes(), [])

    def test_begin_reconciliation_crash_is_recovered_too(self):
        ids = self.new_op(state="UNKNOWN")
        self.crashing_unae("begin_reconciliation", after=True)
        with self.assertRaises(Crash):
            self.bridge.begin_reconciliation(self.msg(ids, kind="RECONCILIATION_RESULT"), 1)
        self.assertEqual(self.dispatcher.recover_after_restart()["routes_closed"], 1)
        self.assertEqual(self.outcomes(ids)[-1], ("BEGIN_RECONCILIATION", "RECOVERED:STATE_ADVANCED:RECONCILING"))

    def test_a_closed_step_is_not_reopened_by_a_later_pending_of_another_message(self):
        a, b = self.new_op(state="RESERVED"), self.new_op(state="RESERVED")
        self.assertEqual(self.dispatcher.dispatch(self.auth(a), self.provider_ok(a), timeout_s=5).status, "RECEIPT_RECORDED")
        self.crashing_unae("record_receipt", after=False)
        with self.assertRaises(Crash):
            self.dispatcher.dispatch(self.auth(b), self.provider_ok(b), timeout_s=5)
        self.assertEqual([r["operation_id"] for r in self.adapter.unfinished_routes()], [b["op"]])
