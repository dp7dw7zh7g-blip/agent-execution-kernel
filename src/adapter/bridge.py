"""Puente entre el adaptador y los dos Stores (UNAE 4.3S rev3 y DTOS). Slice 0.2, punto 1 del plan.

Responsabilidades:
- Verificar que (operation_id, effect_intent_id, effect_attempt_id) del mensaje coincide con lo persistido
  (correlación durable del adaptador, intento de DTOS, ledger de DTOS y diario de UNAE). Ante discrepancia: cuarentena y rechazo (AT-06, AT-07).
  Ante falta de evidencia: cuarentena y rechazo también (I-14, fail closed).
- Convertir el token entero de UNAE a texto decimal canónico para DTOS, solo aquí (AT-05, D-01).
- Registrar qué vía se usó para resolver una incertidumbre: A (begin_reconciliation), B (record_receipt desde UNKNOWN),
  N (recibo normal desde RUNNING) (C-03).

No hace: despacho al proveedor, reintentos, commit (coordinated_commit), ni resuelve efectos DTOS. Un recibo aceptado
no autoriza commit (I-02, I-08).

SUPUESTO A-01 (a revisar): el operation_id de UNAE equivale al de DTOS. La tabla `operations` de DTOS no se consulta;
la fuente de verdad de la relación es `operation_correlation` del adaptador (ver DECISIONES_v1_1_1.md).
"""
from __future__ import annotations

import re
from contextlib import closing
from typing import Any

from .adapter import ProtocolError, UNAEDTOSAdapter, validate_fencing_token

_DEC_RE = re.compile(r"(0|[1-9][0-9]*)")
OPERATION_BOUND = {"EFFECT_DISPATCHED", "EFFECT_RESULT", "RECONCILIATION_RESULT", "RECOVERY_NOTICE"}
_ROUTE_BY_STATUS = {"UNKNOWN": "B", "RECONCILING": "A", "RUNNING": "N"}
# Estados posteriores al recibo: un reenvío es inocuo (UNAE responde ALREADY_RECORDED / RECEIPT_CONFLICT sin mutar nada)
# y debe poder repetirse tras una caída entre record_receipt y el registro de la ruta (AT-13).
_REPLAY_STATUSES = frozenset({"SUCCEEDED", "FAILED", "RECONCILED"})


class CorrelationMismatch(ProtocolError):
    """Los identificadores del mensaje contradicen lo persistido."""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason


class CorrelationUnverifiable(ProtocolError):
    """No hay evidencia persistida suficiente para verificar el mensaje (fail closed)."""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason


def to_dtos_token(value: int) -> str:
    """Entero UNAE -> cadena decimal canónica de DTOS (sin signo, espacios ni ceros a la izquierda)."""
    return str(validate_fencing_token(value))


def from_dtos_token(value: str) -> int:
    """Cadena DTOS -> entero UNAE. Solo acepta ^(0|[1-9][0-9]*)$ y el rango de UNAE/SQLite."""
    if not isinstance(value, str) or len(value) > 19 or _DEC_RE.fullmatch(value) is None:
        raise ProtocolError("fencing_token de DTOS debe ser decimal canónico ^(0|[1-9][0-9]*)$")
    return validate_fencing_token(int(value))


class StoreBridge:
    def __init__(self, adapter: UNAEDTOSAdapter, unae_store: Any, dtos_store: Any, *, require_signed_receipts: bool = True):
        from dtos.effect_repository import EffectRepository  # import perezoso: el adaptador no exige DTOS para importarse
        # D-63: el puente solo habla con los Stores por su API pública. Sin el parche que la aporta, falla AL CONSTRUIR (antes caía en SQL propio
        # sobre `execution_journal` de UNAE o en la conexión privada `_conn` de DTOS; ya no hay ese respaldo).
        for store, name, patch in ((unae_store, "get_operation", "0004"), (dtos_store, "get_effects_by_attempt", "0001"),
                                   (dtos_store, "get_effect_by_provider_key", "0012")):
            if not callable(getattr(store, name, None)):
                raise TypeError(f"StoreBridge exige {type(store).__name__}.{name}() (parche {patch} de la serie); el puente ya no abre conexiones propias a los Stores")
        self.adapter, self.unae, self.dtos = adapter, unae_store, dtos_store
        # D-17: el puente exige recibos firmados por defecto, con independencia de la configuración del Store. Las claves viven
        # solo en el keyring en memoria de UNAE (el adaptador nunca las ve); la verificación la hace UNAE.
        self.require_signed_receipts = require_signed_receipts
        self.effects = EffectRepository(dtos_store)

    # ---------------------------------------------------------------- lecturas (solo lectura, sin escribir estado ajeno)
    def unae_operation(self, operation_id: str) -> dict[str, Any] | None:
        """Operación de UNAE por la lectura pública `Store.get_operation` (parche 0004); sin SQL propio."""
        return self.unae.get_operation(operation_id)

    def _ledger_operation_ids(self, attempt_id: str) -> set[str]:
        """operation_id de las filas del ledger de DTOS para este intento, por la lectura pública `get_effects_by_attempt` (parche 0001).
        Siempre se filtra por INTENTO, nunca por operación: filtrar por la operación del mensaje haría invisible un ledger que liga el
        intento a otra operación."""
        return {r["operation_id"] for r in self.dtos.get_effects_by_attempt(attempt_id)}

    # ---------------------------------------------------------------- registro de la intención (lado escritura, idempotente)
    def register_intent(self, *, operation_id: str, effect_intent_id: str, effect_attempt_id: str, provider: str,
                        idempotency_key: str, fencing_token: int, payload: dict[str, Any]):
        """Persiste la correlación durable y crea el intento en DTOS. Devuelve el EffectIntent de DTOS (token como texto)."""
        from dtos.contracts import EffectIntent
        token_text = to_dtos_token(fencing_token)  # AT-05: única conversión entero -> texto
        self.adapter.bind_correlation(operation_id=operation_id, effect_intent_id=effect_intent_id, effect_attempt_id=effect_attempt_id,
                                      provider=provider, idempotency_key=idempotency_key, fencing_token=fencing_token, intent_payload=payload)
        row = self.dtos.get_effect_attempt(effect_attempt_id)
        if row is None:
            self.effects.create(effect_attempt_id, effect_intent_id, provider, idempotency_key)
        elif (row["intent_id"], row["provider"], row["idempotency_key"]) != (effect_intent_id, provider, idempotency_key):
            raise CorrelationMismatch("DTOS_ATTEMPT_MISMATCH", "el intento ya existe en DTOS con otra intención/proveedor/clave")
        return EffectIntent(intent_id=effect_intent_id, operation_id=operation_id, provider=provider,
                            idempotency_key=idempotency_key, payload=payload, fencing_token=token_text)

    # ---------------------------------------------------------------- verificación (AT-06, AT-07)
    def _fail(self, message: dict[str, Any], exc: ProtocolError) -> None:
        self.adapter.quarantine(message, exc.reason)  # type: ignore[attr-defined]
        raise exc

    def verify_message(self, message: dict[str, Any]) -> dict[str, Any]:
        """Verifica un mensaje ya validado contra todo lo persistido. Devuelve la correlación; cuarentena y lanza si no cuadra."""
        kind = message["message_type"]
        if kind not in OPERATION_BOUND:
            raise ProtocolError(f"{kind} no está ligado a una operación verificable")
        op = message.get("operation_id")
        if op is None:
            raise ProtocolError(f"{kind} sin operation_id no se puede verificar contra los Stores")
        corr = self.adapter.get_correlation(op)
        if corr is None:
            self._fail(message, CorrelationUnverifiable("OPERATION_NOT_CORRELATED", f"operation_id {op} sin correlación durable"))
        intent, attempt = message.get("effect_intent_id"), message.get("effect_attempt_id")
        if intent is not None and intent != corr["effect_intent_id"]:
            self._fail(message, CorrelationMismatch("OPERATION_INTENT_MISMATCH", "la intención no pertenece a esa operación"))
        if attempt is not None and attempt != corr["effect_attempt_id"]:
            self._fail(message, CorrelationMismatch("ATTEMPT_INTENT_MISMATCH", "el intento no pertenece a esa operación/intención"))
        idem = message.get("idempotency_key")
        if idem is not None and idem != corr["idempotency_key"]:
            self._fail(message, CorrelationMismatch("IDEMPOTENCY_KEY_MISMATCH", "idempotency_key distinta de la persistida"))
        if kind == "EFFECT_DISPATCHED" and message["fencing_token"] != corr["fencing_token"]:
            self._fail(message, CorrelationMismatch("FENCING_TOKEN_MISMATCH", "fencing_token distinto del persistido al registrar la intención"))
        if attempt is not None:
            row = self.dtos.get_effect_attempt(attempt)
            if row is None:
                if kind != "EFFECT_DISPATCHED":
                    self._fail(message, CorrelationUnverifiable("DTOS_ATTEMPT_MISSING", "el intento no existe en DTOS"))
            elif (row["intent_id"], row["provider"], row["idempotency_key"]) != (corr["effect_intent_id"], corr["provider"], corr["idempotency_key"]):
                self._fail(message, CorrelationMismatch("DTOS_ATTEMPT_MISMATCH", "el intento de DTOS contradice la correlación"))
            if any(o != op for o in self._ledger_operation_ids(attempt)):
                self._fail(message, CorrelationMismatch("LEDGER_OPERATION_MISMATCH", "el ledger de DTOS liga este intento a otra operación"))
        if self.unae_operation(op) is None:
            self._fail(message, CorrelationUnverifiable("UNAE_OPERATION_MISSING", f"operation_id {op} no existe en UNAE"))
        return corr

    # ---------------------------------------------------------------- vías de resolución de UNKNOWN (C-03)
    def begin_reconciliation(self, message: dict[str, Any], fencing_token: int) -> bool:
        """Vía A: UNKNOWN -> RECONCILING bajo la autoridad vigente. El token entero pasa tal cual a UNAE."""
        token = validate_fencing_token(fencing_token)
        self.verify_message(message)
        op, mid = message["operation_id"], message["message_id"]
        before = self.unae_operation(op)["status"]  # type: ignore[index]
        self.adapter.log_route(operation_id=op, message_id=mid, route="A", step="BEGIN_RECONCILIATION", status_before=before,
                               outcome="PENDING", fencing_token=token)
        try:
            ok = bool(self.unae.begin_reconciliation(op, token))
        except Exception as exc:  # M-4: la fila PENDING no se deja abierta cuando UNAE lanza; una caída (BaseException) la cierra `recover_routes`
            self.adapter.log_route(operation_id=op, message_id=mid, route="A", step="BEGIN_RECONCILIATION", status_before=before,
                                   outcome=f"ERROR:{type(exc).__name__}", fencing_token=token)
            raise
        self.adapter.log_route(operation_id=op, message_id=mid, route="A", step="BEGIN_RECONCILIATION", status_before=before,
                               outcome="RECONCILING" if ok else "REFUSED", fencing_token=token)
        return ok

    def record_receipt(self, message: dict[str, Any], receipt: dict[str, Any]) -> str:
        """Registra un recibo del proveedor en UNAE. Vía B si la operación está UNKNOWN; A si ya está RECONCILING; N si RUNNING.
        No hace commit. La firma/verificación del recibo la decide UNAE según su configuración."""
        self.verify_message(message)
        op, mid = message["operation_id"], message["message_id"]
        if receipt.get("operation_id") != op:
            self._fail(message, CorrelationMismatch("RECEIPT_OPERATION_MISMATCH", "el recibo es de otra operación"))
        before = self.unae_operation(op)["status"]  # type: ignore[index]
        route = _ROUTE_BY_STATUS.get(before)
        if route is None and before in _REPLAY_STATUSES:
            route = "N"
        if route is None:
            raise ProtocolError(f"un recibo no es aplicable con la operación UNAE en estado {before}")
        if self.require_signed_receipts and (receipt.get("signature") is None or receipt.get("provider_id") is None):
            self.adapter.log_route(operation_id=op, message_id=mid, route=route, step="RECORD_RECEIPT", status_before=before,
                                   outcome="UNSIGNED_REJECTED_BY_BRIDGE")
            return "UNSIGNED_REJECTED_BY_BRIDGE"
        self.adapter.log_route(operation_id=op, message_id=mid, route=route, step="RECORD_RECEIPT", status_before=before, outcome="PENDING")
        try:
            outcome = self.unae.record_receipt(**receipt)
        except Exception as exc:  # M-4: ídem
            self.adapter.log_route(operation_id=op, message_id=mid, route=route, step="RECORD_RECEIPT", status_before=before, outcome=f"ERROR:{type(exc).__name__}")
            raise
        self.adapter.log_route(operation_id=op, message_id=mid, route=route, step="RECORD_RECEIPT", status_before=before, outcome=str(outcome))
        return str(outcome)

    # ---------------------------------------------------------------- recuperación de la bitácora de rutas (M-4)
    def recover_routes(self) -> int:
        """Cierra las filas PENDING de la bitácora de rutas que quedaron a medias por una caída, comparando con el estado REAL de la operación en UNAE.
        No llama a UNAE para mutar nada ni a ningún proveedor: solo añade una fila de cierre (la bitácora es append-only). Si el estado ya no es el
        previo, el paso avanzó (o lo avanzó otro actor); si es el mismo, UNAE no llegó a cambiar nada. Se asume un proceso en reposo, como en el resto
        de las recuperaciones. Devuelve cuántas filas cerró."""
        closed = 0
        for row in self.adapter.unfinished_routes():
            op, before = row["operation_id"], row["status_before"]
            current = self.unae_operation(op)
            if current is None:
                outcome = "RECOVERED:UNAE_OPERATION_MISSING"
            elif current["status"] == before:
                outcome = "RECOVERED:NO_STATE_CHANGE"
            else:
                outcome = f"RECOVERED:STATE_ADVANCED:{current['status']}"
            self.adapter.log_route(operation_id=op, message_id=row["message_id"], route=row["route"], step=row["step"], status_before=before,
                                   outcome=outcome, fencing_token=row["fencing_token"])
            closed += 1
        return closed
