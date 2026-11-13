"""
effect_fsm.py
=============
FSM de EXTERNAL EFFECT (sección homónima del Registro Maestro).

INTENDED -> DISPATCHED -> {CONFIRMED, PARTIAL, REJECTED, UNKNOWN}

I-005 en código: "Uncertain execution attempt debe resolverse antes de
retry obligatorio" — aquí se traduce en que desde UNKNOWN NO se puede
saltar directo a CONFIRMED/REJECTED/PARTIAL vía transition(). Primero
hay que pasar por RECONCILIATION_REQUIRED, y de ahí solo se resuelve vía
`reconcile(...)`, que exige declarar qué estrategia de reconciliación se
usó (Provider Query, Idempotency Lookup, etc. — sección RECONCILIATION
del Registro Maestro) y evidencia.

NOT_FOUND no colapsa automáticamente a REJECTED (regla explícita del
documento): `reconcile()` lo expone como parámetro para que el llamador
decida explícitamente entre STILL_UNKNOWN/CONTRADICTORY, nunca por
default.
"""

from __future__ import annotations

from .enums import EffectStatus as S
from .fsm import FiniteStateMachine

RECONCILIATION_STRATEGIES = frozenset(
    {
        "PROVIDER_QUERY",
        "IDEMPOTENCY_LOOKUP",
        "EXTERNAL_REFERENCE_LOOKUP",
        "READ_AFTER_WRITE",
        "EVENT_CONFIRMATION",
        "INDEPENDENT_OBSERVATION",
        "HUMAN_CONFIRMATION",
    }
)

_TERMINAL = frozenset({S.CONFIRMED, S.REJECTED})

_TRANSITIONS = {
    S.INTENDED: frozenset({S.DISPATCHED}),
    S.DISPATCHED: frozenset({S.CONFIRMED, S.PARTIAL, S.REJECTED, S.UNKNOWN}),
    S.UNKNOWN: frozenset({S.RECONCILIATION_REQUIRED}),
    S.RECONCILIATION_REQUIRED: frozenset(
        {S.CONFIRMED, S.REJECTED, S.PARTIAL, S.UNKNOWN}  # UNKNOWN = STILL_UNKNOWN
    ),
    S.PARTIAL: frozenset(),  # requiere nueva EffectAttempt para completar, fuera de esta FSM
    S.CONFIRMED: frozenset(),
    S.REJECTED: frozenset(),
}


class EffectFSM(FiniteStateMachine):
    state_enum = S
    transitions = _TRANSITIONS

    # Todo lo que sale de RECONCILIATION_REQUIRED hacia una resolución
    # queda protegido: solo reconcile() puede materializarlo, porque
    # reconcile() exige declarar la estrategia usada.
    guarded_transitions = frozenset(
        {
            (S.RECONCILIATION_REQUIRED, S.CONFIRMED),
            (S.RECONCILIATION_REQUIRED, S.REJECTED),
            (S.RECONCILIATION_REQUIRED, S.PARTIAL),
            (S.RECONCILIATION_REQUIRED, S.UNKNOWN),
        }
    )

    def __init__(self) -> None:
        super().__init__(initial_state=S.INTENDED)

    def mark_unknown(self, reason: str) -> None:
        """DISPATCHED -> UNKNOWN. Transición libre, no requiere evidencia
        todavía (la incertidumbre en sí no es una afirmación, solo un
        hecho: no se pudo confirmar)."""
        self.transition(S.UNKNOWN, reason=reason)
        # UNKNOWN siempre avanza a RECONCILIATION_REQUIRED; no se permite
        # quedarse "flotando" en UNKNOWN sin marcar que requiere
        # reconciliación (I-005).
        self.transition(S.RECONCILIATION_REQUIRED, reason="UNKNOWN exige reconciliation (I-005)")

    def reconcile(self, strategy: str, result: str, evidence: dict) -> None:
        """
        Única vía para resolver RECONCILIATION_REQUIRED.
        `strategy` debe ser una de RECONCILIATION_STRATEGIES.
        `result` debe ser CONFIRMED | REJECTED | PARTIAL | UNKNOWN.
        `evidence` no puede estar vacío: una reconciliación sin evidencia
        no es reconciliación, es una suposición (lo que el documento
        prohíbe explícitamente para NOT_FOUND -> REJECTED automático).
        """
        if self._state != S.RECONCILIATION_REQUIRED:
            raise ValueError(
                f"reconcile() solo aplica desde RECONCILIATION_REQUIRED, "
                f"estado actual: {self._state.value}"
            )
        if strategy not in RECONCILIATION_STRATEGIES:
            raise ValueError(f"strategy desconocida: {strategy!r}")
        if not evidence:
            raise ValueError(
                "reconcile() requiere evidence no vacío (NOT_FOUND no puede "
                "colapsar a REJECTED sin evidencia explícita)."
            )
        try:
            target = S[result] if result in S.__members__ else S(result)
        except (KeyError, ValueError) as e:
            raise ValueError(f"result inválido: {result!r}") from e

        evidence_with_strategy = {**evidence, "_reconciliation_strategy": strategy}
        self._force_transition(target, reason=f"reconciled via {strategy}", evidence=evidence_with_strategy)
