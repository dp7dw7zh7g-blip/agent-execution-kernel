"""Despacho al proveedor (punto 3 del plan: AT-01, AT-08, AT-12, AT-13). Slice 0.3.

Reglas de diseño:
- Solo despacha con una `DispatchAuthorization` explícita. Un ciclo cognitivo COMPLETED, un mensaje o un dict NO son autorización (AT-01).
- Escribe antes de actuar (write-ahead): verifica identidad, pasa UNAE a RUNNING (comprueba la autoridad vigente) y el intento de DTOS a
  DISPATCHED; solo entonces llama al proveedor. Si la autoridad/fencing ya no es válida, el proveedor no se llama (AT-12).
- UNA sola llamada al proveedor por autorización. Sin reintentos, ni dentro ni fuera: cualquier cosa que no sea un recibo registrado
  (timeout, excepción, recibo inválido) deja la operación en UNKNOWN y bloquea el despacho siguiente (AT-08). Nunca se crea una
  operación sustituta.
- El timeout es obligatorio y sin valor por defecto: UNAE no tiene temporizador propio y el plazo correcto depende del proveedor. Sale de la
  `TransportConfig` por `provider_id` (si el despachador la tiene; entonces `timeout_s` explícito se rechaza para no saltarse la configuración y un
  proveedor sin configurar lanza `ConfigurationError` antes de tocar nada) o, sin configuración, del argumento `timeout_s` de cada llamada.
- Cancelación COOPERATIVA (Hallazgo 5, D-55): al vencer el plazo el despachador no puede matar el hilo del proveedor. Por eso a cada llamada le
  entrega `request["cancel"]` (un `threading.Event`) y `request["deadline"]` (instante absoluto de `time.monotonic()`). El `Event` se activa ANTES
  de marcar UNKNOWN y también al terminar la llamada por cualquier vía. Un proveedor correcto espera con `cancel.wait(...)` en lugar de dormir y
  comprueba `cancel.is_set()` / el deadline justo antes de cada paso irreversible. Esto REDUCE la ventana de efectos tardíos, no la cierra:
  un proveedor que ignore el Event, o que ya pasó su última comprobación, puede aplicar el efecto después; el recibo tardío se descarta, la
  operación sigue en UNKNOWN y solo la reconciliación resuelve el efecto (nunca se registra un recibo posterior al plazo).
- Fencing hasta el proveedor (Hallazgo 4, D-56): `request["fencing_token"]` (el entero con el que se despacha) y `request["fencing_scope"]` (la
  Task de UNAE) viajan al proveedor para que rechace tokens obsoletos (ver `fencing.FencingGate`: estrictamente menor se rechaza, igual se
  acepta). El proveedor puede devolver el token en el recibo (`receipt["fencing_token"]`); el despachador lo compara y lo RETIRA antes de
  entregar el recibo a UNAE, porque ni UNAE lo recibe ni su firma de recibos lo cubre. Es una afirmación sin autenticar del proveedor:
  detecta desincronización y errores, no falsificación. Con `require_fencing_echo=True` un recibo sin eco se trata como incierto; por
  defecto es opcional (retrocompatible con proveedores que no lo conocen).
- C-1 (g0.8 r10): si el registro de DTOS (`effects.dispatch`) falla DESPUÉS de `mark_running`, el despachador lleva la operación a UNKNOWN, intenta
  dejar el intento en RECONCILIATION_REQUIRED y lanza `DispatchBlocked("DISPATCH_RECORD_FAILED")` (proveedor NO llamado). Lo que no alcance a hacer
  lo completa `recover_after_restart` (UNAE UNKNOWN + DTOS INTENDED).
- g0.8 r10 (A-1): un recibo que no es un dict con las claves que acepta `unae.record_receipt` deja la operación en UNKNOWN con `RECEIPT_MALFORMED`
  (resultado, no excepción). La excepción del proveedor se captura dentro del hilo (`PROVIDER_ERROR:<tipo>`), de modo que un `TimeoutError` propio
  del proveedor no se confunde con el vencimiento del plazo (`TIMEOUT`), que desde Python 3.11 comparte clase con `concurrent.futures.TimeoutError`.
- Una excepción que no sea `Exception` (crash simulado, KeyboardInterrupt…) NO se captura: deja RUNNING, como una caída real. Tras reiniciar,
  `recover_after_restart()` lo pasa a UNKNOWN y no se repite el despacho (AT-13).
- En DTOS, `mark_unknown` deja el intento en RECONCILIATION_REQUIRED (así lo define su FSM); es el estado que espera al reconciliador.
- Aceptar un recibo no hace commit ni resuelve el efecto de DTOS (AT-14, I-02): el intento queda DISPATCHED hasta que lo resuelva el reconciliador.
"""
from __future__ import annotations

import inspect
import math
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from .adapter import ProtocolError, validate_envelope, validate_fencing_token
from .bridge import StoreBridge
from .transport import MAX_PROVIDER_TIMEOUT_S, TransportConfig


class DispatchBlocked(ProtocolError):
    """El despacho no se ejecutó (el proveedor NO fue llamado)."""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason


@dataclass(frozen=True)
class DispatchAuthorization:
    operation_id: str
    effect_intent_id: str
    effect_attempt_id: str
    fencing_token: int
    provider_id: str            # id del proveedor en UNAE (keyring)
    provider_operation_id: str  # id que el proveedor debe devolver en su recibo
    authorized_by: str


@dataclass(frozen=True)
class DispatchResult:
    status: str   # RECEIPT_RECORDED | UNKNOWN
    reason: str   # outcome de UNAE si RECEIPT_RECORDED; causa si UNKNOWN
    message_id: str


class Dispatcher:
    def __init__(self, bridge: StoreBridge, transport: TransportConfig | None = None, *, require_fencing_echo: bool = False,
                 max_inflight_per_provider: int = 8):
        self.bridge = bridge
        self.transport = transport
        self.require_fencing_echo = bool(require_fencing_echo)  # D-56: exige que el recibo devuelva el fencing_token despachado
        # M-3 (D-62): llamadas al proveedor todavía en curso (incluidas las que ya vencieron su plazo pero cuyo hilo no ha terminado), por provider_id.
        # 8 es un valor propuesto, no una medición. Con el tope alcanzado el despacho se bloquea ANTES de tocar ningún Store.
        if isinstance(max_inflight_per_provider, bool) or not isinstance(max_inflight_per_provider, int) or max_inflight_per_provider < 1:
            raise ValueError("max_inflight_per_provider debe ser un entero >= 1")
        self.max_inflight_per_provider = max_inflight_per_provider
        self._inflight: dict[str, int] = {}
        self._inflight_lock = threading.Lock()

    def inflight(self, provider_id: str) -> int:
        """Llamadas al proveedor cuyo hilo sigue vivo (un proveedor colgado mantiene su plaza hasta que su hilo termine)."""
        with self._inflight_lock:
            return self._inflight.get(provider_id, 0)

    def _acquire_slot(self, provider_id: str) -> None:
        with self._inflight_lock:
            if self._inflight.get(provider_id, 0) >= self.max_inflight_per_provider:
                raise DispatchBlocked("PROVIDER_SATURATED", f"hay {self._inflight[provider_id]} llamadas sin terminar al proveedor {provider_id!r} "
                                      f"(máximo {self.max_inflight_per_provider}): no se despacha y no se toca ningún Store")
            self._inflight[provider_id] = self._inflight.get(provider_id, 0) + 1

    def _release_slot(self, provider_id: str) -> None:
        with self._inflight_lock:
            left = self._inflight.get(provider_id, 0) - 1
            if left > 0:
                self._inflight[provider_id] = left
            else:
                self._inflight.pop(provider_id, None)

    def dispatch(self, authorization: DispatchAuthorization, provider_call: Callable[[dict[str, Any]], dict[str, Any]],
                 *, timeout_s: float | None = None) -> DispatchResult:
        if not isinstance(authorization, DispatchAuthorization):
            raise DispatchBlocked("NO_AUTHORIZATION", "solo una DispatchAuthorization explícita autoriza un despacho")
        for field in ("operation_id", "effect_intent_id", "effect_attempt_id", "provider_id", "provider_operation_id", "authorized_by"):
            if not isinstance(getattr(authorization, field), str) or not getattr(authorization, field).strip():
                raise DispatchBlocked("NO_AUTHORIZATION", f"{field} vacío")
        timeouts = None
        if self.transport is not None:
            if timeout_s is not None:
                raise ValueError("el despachador tiene TransportConfig: el plazo sale de la configuración, no de timeout_s")
            timeouts = self.transport.timeouts_for(authorization.provider_id)  # ConfigurationError antes de tocar nada
            timeout_s = timeouts.total_timeout_s
        elif isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)) or not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError("timeout_s debe ser un número finito mayor que 0 (no hay valor por defecto)")
        elif timeout_s > MAX_PROVIDER_TIMEOUT_S:  # L-3
            raise ValueError(f"timeout_s no puede superar {MAX_PROVIDER_TIMEOUT_S:g} s")
        self._acquire_slot(authorization.provider_id)  # M-3: antes de tocar nada; DispatchBlocked("PROVIDER_SATURATED") si el proveedor tiene demasiadas llamadas sin terminar
        handed_off = False
        try:
            token = validate_fencing_token(authorization.fencing_token)
            b, a = self.bridge, authorization
            corr = b.adapter.get_correlation(a.operation_id)
            if corr is None:
                raise DispatchBlocked("OPERATION_NOT_CORRELATED", "la intención no está registrada en el puente")
            message = {"contract_version": "1.1", "schema_version": "1", "message_id": str(uuid.uuid4()), "message_type": "EFFECT_DISPATCHED",
                       "correlation_id": f"dispatch:{a.operation_id}", "evidence_refs": [],
                       "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "operation_id": a.operation_id, "effect_intent_id": a.effect_intent_id, "effect_attempt_id": a.effect_attempt_id,
                       "idempotency_key": corr["idempotency_key"], "fencing_token": token, "authority_ref": a.authorized_by}
            validate_envelope(message)
            b.verify_message(message)  # cuarentena + excepción si no cuadra con lo persistido
            attempt = b.dtos.get_effect_attempt(a.effect_attempt_id)
            if attempt is None or attempt["status"] != "INTENDED":
                raise DispatchBlocked("ATTEMPT_NOT_INTENDED", "el intento de DTOS no está en INTENDED: no se despacha dos veces")
            unae_op = b.unae_operation(a.operation_id)
            status = unae_op["status"]  # type: ignore[index]
            scope = str(unae_op["task_id"])  # type: ignore[index]  # el token de UNAE es por Task
            if status != "RESERVED":
                raise DispatchBlocked(f"OPERATION_{status}", f"la operación UNAE está en {status}, no en RESERVED: no se despacha")
            # --- write-ahead: nada sale al proveedor hasta que ambos Stores lo hayan registrado ---
            if not b.unae.mark_running(a.operation_id, token, a.provider_operation_id, provider_id=a.provider_id):
                raise DispatchBlocked("AUTHORITY_INVALID", "UNAE rechazó mark_running: fencing/autoridad no vigente")
            try:
                b.effects.dispatch(a.effect_attempt_id)
            except Exception as exc:  # C-1: UNAE ya está en RUNNING; sin esto la operación quedaría sin salida automática
                self._abort_unrecorded(a)
                raise DispatchBlocked("DISPATCH_RECORD_FAILED", f"DTOS no registró el despacho ({type(exc).__name__}); el proveedor NO fue llamado y la operación queda en UNKNOWN") from exc
            request = {"operation_id": a.operation_id, "effect_attempt_id": a.effect_attempt_id,
                       "idempotency_key": corr["idempotency_key"], "provider_operation_id": a.provider_operation_id,
                       "fencing_token": token, "fencing_scope": scope}  # D-56: el proveedor puede rechazar tokens obsoletos
            if timeouts is not None:
                request["timeouts"] = timeouts.as_dict()  # connect/read los aplica el código del proveedor; el despachador impone el total
            cancel = threading.Event()  # D-55: señal cooperativa de «el plazo venció, no apliques nada más»
            request["cancel"] = cancel
            request["deadline"] = time.monotonic() + timeout_s  # instante absoluto (monotonic), el mismo plazo que impone el despachador
            # --- única llamada al proveedor, con plazo; sin reintentos ---
            # M-3 (D-62): hilo DAEMON propio (un proveedor colgado ya no impide cerrar el intérprete) y plaza por proveedor, que el propio hilo libera
            # al terminar. La excepción del proveedor se captura DENTRO del hilo (A-1): desde Python 3.11 `concurrent.futures.TimeoutError` es el
            # `TimeoutError` incorporado y un timeout de socket del proveedor se habría confundido con el vencimiento del plazo. Una BaseException
            # (caída simulada, KeyboardInterrupt) se guarda y se relanza en el hilo llamador, como una caída real.
            box: dict[str, Any] = {}
            done = threading.Event()

            def run_provider() -> None:
                try:
                    try:
                        box["ok"] = (True, provider_call(request))
                    except Exception as exc:
                        box["ok"] = (False, exc)
                    except BaseException as exc:
                        box["base"] = exc
                finally:
                    done.set()
                    self._release_slot(a.provider_id)
            thread = threading.Thread(target=run_provider, name=f"adapter-provider-{a.provider_id}", daemon=True)
            try:
                thread.start()
            except Exception:  # sin recursos para un hilo: el proveedor no se llamó, pero el write-ahead ya se hizo
                cancel.set()
                return self._unknown(a, message, "PROVIDER_THREAD_START_FAILED")
            handed_off = True  # desde aquí la plaza la libera el hilo
            try:
                if not done.wait(timeout_s):
                    cancel.set()  # primero la señal, luego el estado: cuanto antes lo sepa el proveedor, menos efecto tardío
                    return self._unknown(a, message, "TIMEOUT")
            finally:
                cancel.set()  # cualquier salida (recibo, error, plazo, caída simulada) deja la señal activada para un hilo rezagado
            if "base" in box:
                raise box["base"]
            ok, receipt = box["ok"]
            if not ok:
                return self._unknown(a, message, f"PROVIDER_ERROR:{type(receipt).__name__}")
            if isinstance(receipt, dict):
                receipt = dict(receipt)
                if "fencing_token" in receipt:
                    echoed = receipt.pop("fencing_token")  # UNAE no conoce este campo (ni su firma lo cubre): se valida aquí y no se le entrega
                    if isinstance(echoed, bool) or not isinstance(echoed, int) or echoed != token:
                        return self._unknown(a, message, "RECEIPT_FENCING_MISMATCH")
                elif self.require_fencing_echo:
                    return self._unknown(a, message, "RECEIPT_FENCING_MISSING")
            malformed = self._receipt_shape_error(receipt)
            if malformed is not None:  # A-1: un recibo que no es utilizable deja la operación en UNKNOWN, con un resultado tipificado y sin excepción
                return self._unknown(a, message, "RECEIPT_MALFORMED")
            try:
                outcome = b.record_receipt(message, receipt)
            except Exception:
                self._unknown(a, message, "RECEIPT_PROCESSING_ERROR")
                raise
            if outcome not in ("RECORDED", "ALREADY_RECORDED"):
                return self._unknown(a, message, f"RECEIPT_{outcome}")
            return DispatchResult("RECEIPT_RECORDED", outcome, message["message_id"])
        finally:
            if not handed_off:
                self._release_slot(authorization.provider_id)

    def _receipt_shape_error(self, receipt: Any) -> str | None:
        """A-1: el recibo debe ser un dict con claves de texto y exactamente los argumentos que acepta `unae.record_receipt` (ni de más ni de menos).
        La firma y el contenido los valida UNAE; aquí solo se evita que `record_receipt(**receipt)` falle con una excepción no tipificada."""
        if not isinstance(receipt, dict):
            return "el recibo no es un objeto"
        if any(not isinstance(k, str) for k in receipt):
            return "el recibo tiene claves que no son texto"
        try:
            params = inspect.signature(self.bridge.unae.record_receipt).parameters
        except (TypeError, ValueError):
            return None  # sin firma consultable no se puede comparar
        if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return None
        names = {n for n, p in params.items() if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)}
        required = {n for n in names if params[n].default is inspect.Parameter.empty}
        if set(receipt) - names:
            return f"claves desconocidas: {sorted(set(receipt) - names)}"
        if required - set(receipt):
            return f"faltan claves: {sorted(required - set(receipt))}"
        return None

    def _abort_unrecorded(self, a: DispatchAuthorization) -> None:
        """C-1: el write-ahead de DTOS falló después de `mark_running`. El proveedor no se llamó, pero UNAE no puede volver a RESERVED:
        se lleva a UNKNOWN (falla cerrada) y se intenta dejar el intento de DTOS en RECONCILIATION_REQUIRED. Cada paso es de mejor esfuerzo:
        si también falla (p. ej. el mismo bloqueo de SQLite), `recover_after_restart` termina el trabajo con la misma regla."""
        try:
            self.bridge.unae.mark_unknown(a.operation_id)
        except Exception:
            pass
        self._settle_undispatched(a.effect_attempt_id, "DISPATCH_RECORD_FAILED")

    def _settle_undispatched(self, attempt_id: str, reason: str) -> bool:
        """Lleva un intento de DTOS que quedó en INTENDED (o DISPATCHED sin cierre) a RECONCILIATION_REQUIRED por las transiciones que ya
        ofrece su FSM (INTENDED -> DISPATCHED -> UNKNOWN -> RECONCILIATION_REQUIRED). Devuelve True si el intento queda en RECONCILIATION_REQUIRED."""
        b = self.bridge
        try:
            row = b.dtos.get_effect_attempt(attempt_id)
            if row is not None and row["status"] == "INTENDED":
                b.effects.dispatch(attempt_id)
                row = b.dtos.get_effect_attempt(attempt_id)
            if row is not None and row["status"] == "DISPATCHED":
                b.effects.mark_unknown(attempt_id, reason)
            row = b.dtos.get_effect_attempt(attempt_id)
            return row is not None and row["status"] == "RECONCILIATION_REQUIRED"
        except Exception:
            return False

    def _unknown(self, a: DispatchAuthorization, message: dict[str, Any], reason: str) -> DispatchResult:
        self.bridge.unae.mark_unknown(a.operation_id)
        self.bridge.effects.mark_unknown(a.effect_attempt_id, reason)
        return DispatchResult("UNKNOWN", reason, message["message_id"])

    def recover_after_restart(self) -> dict[str, int]:
        """Tras reiniciar: RUNNING huérfano -> UNKNOWN en UNAE y, para esas operaciones, DISPATCHED -> (UNKNOWN) RECONCILIATION_REQUIRED en DTOS.
        C-1: UNAE en UNKNOWN con el intento de DTOS aún en INTENDED (caída o fallo entre `mark_running` y el registro en DTOS) sigue el mismo
        camino: UNKNOWN en UNAE implica que el write-ahead empezó, así que el efecto es incierto aunque el proveedor no conste como llamado.
        No llama a ningún proveedor ni reintenta nada."""
        b = self.bridge
        routes_closed = b.recover_routes()  # M-4: primero, mientras el estado de UNAE aún es el que dejó la caída (antes de `recover_interrupted`)
        unae_changed = b.unae.recover_interrupted()
        dtos_changed = 0
        undispatched = 0
        for corr in b.adapter.list_correlations():
            op = b.unae_operation(corr["operation_id"])
            attempt = b.dtos.get_effect_attempt(corr["effect_attempt_id"])
            if not (op and attempt and op["status"] == "UNKNOWN"):
                continue
            if attempt["status"] == "DISPATCHED":
                b.effects.mark_unknown(corr["effect_attempt_id"], "RESTART_RECOVERY")
                dtos_changed += 1
            elif attempt["status"] == "INTENDED":
                if self._settle_undispatched(corr["effect_attempt_id"], "DISPATCH_RECORD_FAILED"):
                    undispatched += 1
        return {"unae_operations_to_unknown": unae_changed, "dtos_attempts_to_unknown": dtos_changed, "dtos_undispatched_to_unknown": undispatched, "routes_closed": routes_closed}
