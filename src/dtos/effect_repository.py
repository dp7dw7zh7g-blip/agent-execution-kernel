"""
effect_repository.py
======================
Mismo patrón que CognitiveCycleRepository (repository.py), aplicado a
EffectFSM. Cierra el hueco anotado en corrección 179: hasta ahora
`store.insert_effect()` podía escribirse directo sin pasar por
EffectFSM, así que nada impedía persistir, por ejemplo, un estado
"CONFIRMED" que nunca pasó por reconciliación real.

Dos tablas distintas, a propósito:
- `effect_attempts` (mutable, Commit Gate): el estado EN CURSO de un
  intento, mientras vive DISPATCHED -> UNKNOWN -> RECONCILIATION_REQUIRED
  -> ... Se actualiza con optimistic concurrency, igual que
  cognitive_cycles.
- `effects` (append-only, ya existía desde corrección 179/store.py): el
  ledger histórico, escrito UNA VEZ que el intento llega a un estado
  terminal (CONFIRMED/REJECTED). No se reescribe.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from .canonical import canonical_json
from .effect_fsm import EffectFSM
from .enums import EffectStatus as S
from .store import ConflictError, SQLiteStore, check_evidence_size

_TERMINAL = frozenset({S.CONFIRMED, S.REJECTED})


class AttemptNotFoundError(ValueError):
    pass


@dataclass(frozen=True)
class LoadedAttempt:
    fsm: EffectFSM
    version: int
    operation_id: str  # para el ledger final; no vive en effect_attempts


class EffectRepository:
    def __init__(self, store: SQLiteStore):
        self._store = store

    def create(self, attempt_id: str, intent_id: str, provider: str, idempotency_key: str) -> LoadedAttempt:
        fsm = EffectFSM()  # nace en INTENDED
        self._store.create_effect_attempt(
            attempt_id=attempt_id, intent_id=intent_id, provider=provider, idempotency_key=idempotency_key,
            status=fsm.state.value,
        )
        return LoadedAttempt(fsm=fsm, version=1, operation_id="")

    def load(self, attempt_id: str) -> LoadedAttempt:
        row = self._store.get_effect_attempt(attempt_id)
        if row is None:
            raise AttemptNotFoundError(f"effect_attempt {attempt_id!r} no existe")
        fsm = EffectFSM()
        target = S(row["status"])
        if target != S.INTENDED:
            fsm._state = target  # reconstrucción de snapshot persistido, no una transición nueva
        return LoadedAttempt(fsm=fsm, version=row["version"], operation_id="")

    def dispatch(self, attempt_id: str) -> LoadedAttempt:
        loaded = self.load(attempt_id)
        loaded.fsm.transition(S.DISPATCHED)
        result = self._store.commit_effect_attempt_transition(attempt_id, S.DISPATCHED.value, loaded.version)
        return LoadedAttempt(fsm=loaded.fsm, version=result.new_version, operation_id=loaded.operation_id)

    def mark_unknown(self, attempt_id: str, reason: str) -> LoadedAttempt:
        """DISPATCHED -> UNKNOWN -> RECONCILIATION_REQUIRED, validado en la FSM primero."""
        loaded = self.load(attempt_id)
        loaded.fsm.mark_unknown(reason=reason)  # valida las dos transiciones en memoria
        result = self._store.commit_effect_attempt_transition(
            attempt_id, S.RECONCILIATION_REQUIRED.value, loaded.version
        )
        return LoadedAttempt(fsm=loaded.fsm, version=result.new_version, operation_id=loaded.operation_id)

    def reconcile(
        self,
        attempt_id: str,
        strategy: str,
        result_status: str,
        evidence: dict,
        operation_id: str,
        provider: str,
        idempotency_key: str,
    ) -> LoadedAttempt:
        """
        Única vía para resolver un intento en RECONCILIATION_REQUIRED.
        Si el resultado es terminal (CONFIRMED/REJECTED), además de
        actualizar effect_attempts, escribe la fila permanente en el
        ledger `effects`; toda reconciliación aceptada (también PARTIAL) deja además
        una fila en el historial `effect_reconciliations` — en el mismo paso lógico, para que quien
        consulte el ledger nunca vea un estado que no pasó por
        reconcile() con evidencia real (I-005 cerrado de punta a punta).
        """
        loaded = self.load(attempt_id)
        # Evidencia en forma canónica (P-064) y acotada: se comprueba el tamaño ANTES de tocar la FSM o el Store (EvidenceTooLargeError).
        serialized_evidence = canonical_json(evidence)
        check_evidence_size(serialized_evidence, where="reconcile")
        from_status = loaded.fsm.state.value
        loaded.fsm.reconcile(strategy=strategy, result=result_status, evidence=evidence)  # valida + exige evidencia

        effect_row = None
        if loaded.fsm.state in _TERMINAL:
            effect_row = {
                "effect_id": str(uuid.uuid4()),
                "operation_id": operation_id,
                "intent_id": attempt_id,  # simplificado: en este esquema attempt_id referencia el intent
                "attempt_id": attempt_id,
                "provider": provider,
                "idempotency_key": idempotency_key,
                # La evidencia queda en el ledger append-only junto al estado que justifica.
                "evidence": serialized_evidence,
            }

        # Una sola transacción: estado del intento + fila del ledger (si es terminal) + historial de reconciliaciones (siempre, también PARTIAL).
        result = self._store.reconcile_effect_attempt_atomic(
            attempt_id, loaded.fsm.state.value, loaded.version, effect_row,
            {"from_status": from_status, "strategy": strategy, "evidence": serialized_evidence},
        )

        return LoadedAttempt(fsm=loaded.fsm, version=result.new_version, operation_id=operation_id)
