"""
cognitive_cycle_fsm.py
=======================
FSM del CICLO COGNITIVO CANÓNICO (sección homónima del Registro Maestro).

Camino feliz:
CREATED -> PREPARING -> CONTEXT_ASSEMBLING -> REPRESENTING -> REASONING
-> VERIFYING -> RESULT_READY -> PLANNING_UPDATE -> PROJECTING -> CONTROL
-> READY_TO_COMMIT -> COMMITTING -> COMPLETED

Estados especiales, alcanzables desde cualquier estado "activo" (no desde
COMPLETED/FAILED/CANCELLED, que son terminales):
WAITING, NEEDS_INFORMATION, NEEDS_HUMAN, BLOCKED, FAILED, UNKNOWN,
CANCELLED.

Decisión de diseño explícita — P-013 aplicado como GUARD, no como
comentario:
UNKNOWN -> FAILED existe conceptualmente (un UNKNOWN puede resolverse en
un fallo real), pero NO se permite vía transition() directo. Solo se
permite a través de `resolve_unknown_as_failed(evidence=...)`, que exige
pasar evidencia — obligando a que quien haga esa transición documente por
qué, en vez de que un `if` cualquiera colapse UNKNOWN en FAILED por
comodidad (que es exactamente el bug que P-013 prohíbe).

Lo mismo aplica, más suave, a UNKNOWN -> COMPLETED: solo vía
`resolve_unknown_as_completed(evidence=...)`.
"""

from __future__ import annotations

from typing import Optional

from .enums import CognitiveCycleState as S
from .fsm import FiniteStateMachine


# Estados terminales: no tienen transiciones salientes.
_TERMINAL = frozenset({S.COMPLETED, S.FAILED, S.CANCELLED})

# Estados especiales, alcanzables desde cualquier estado activo.
_SPECIAL = frozenset({S.WAITING, S.NEEDS_INFORMATION, S.NEEDS_HUMAN, S.BLOCKED, S.UNKNOWN})

_HAPPY_PATH: dict = {
    S.CREATED: {S.PREPARING},
    S.PREPARING: {S.CONTEXT_ASSEMBLING},
    S.CONTEXT_ASSEMBLING: {S.REPRESENTING},
    S.REPRESENTING: {S.REASONING},
    S.REASONING: {S.VERIFYING},
    S.VERIFYING: {S.RESULT_READY},
    S.RESULT_READY: {S.PLANNING_UPDATE},
    S.PLANNING_UPDATE: {S.PROJECTING},
    S.PROJECTING: {S.CONTROL},
    S.CONTROL: {S.READY_TO_COMMIT},
    S.READY_TO_COMMIT: {S.COMMITTING},
    S.COMMITTING: {S.COMPLETED},
}

_ACTIVE_STATES = frozenset(_HAPPY_PATH.keys())


def _build_transitions() -> dict:
    table: dict = {}
    for state, targets in _HAPPY_PATH.items():
        # Desde cualquier estado activo se puede ir al happy-path target
        # Y también a cualquier estado especial (interrupción legítima).
        table[state] = frozenset(targets | _SPECIAL | {S.CANCELLED})

    # Desde estados especiales: pueden volver a REASONING (resume tras
    # resolución, ciclo WAIT->WAKE->OBSERVE->THINK del Registro Maestro),
    # a otro estado especial, o a CANCELLED. No saltan directo a COMPLETED
    # ni a FAILED sin pasar por el guard explícito (ver más abajo).
    resumable = {S.REASONING, S.CANCELLED} | (_SPECIAL - {S.UNKNOWN})
    for state in _SPECIAL:
        if state == S.UNKNOWN:
            # UNKNOWN es el más restringido: puede permanecer, escalar a
            # NEEDS_HUMAN, o resolverse. La resolución a FAILED/COMPLETED
            # queda en la tabla (para que _force_transition no la
            # rechace) pero BLOQUEADA para transition() vía guarded_transitions.
            table[state] = frozenset(
                {S.UNKNOWN, S.NEEDS_HUMAN, S.REASONING, S.CANCELLED, S.FAILED, S.COMPLETED}
            )
        else:
            table[state] = frozenset(resumable)

    # Terminales: sin salidas.
    for state in _TERMINAL:
        table[state] = frozenset()

    return table


class CognitiveCycleFSM(FiniteStateMachine):
    state_enum = S
    transitions = _build_transitions()

    # P-013 en código: estas dos transiciones EXISTEN en la tabla (para
    # que la resolución sea posible) pero no son alcanzables por
    # transition() directo. Solo por los métodos explícitos de abajo.
    guarded_transitions = frozenset(
        {
            (S.UNKNOWN, S.FAILED),
            (S.UNKNOWN, S.COMPLETED),
        }
    )

    def __init__(self) -> None:
        super().__init__(initial_state=S.CREATED)

    def resolve_unknown_as_failed(self, evidence: dict, reason: str) -> None:
        """
        Única vía legítima para UNKNOWN -> FAILED. Exige evidencia no
        vacía: una llamada sin evidencia es exactamente el patrón que
        P-013 prohíbe ("no hubo respuesta -> asumo que falló").
        """
        if self._state != S.UNKNOWN:
            raise ValueError(
                f"resolve_unknown_as_failed solo aplica desde UNKNOWN, "
                f"estado actual: {self._state.value}"
            )
        if not evidence:
            raise ValueError(
                "resolve_unknown_as_failed requiere evidence no vacío "
                "(P-013: UNKNOWN != FAILED no se resuelve por default)."
            )
        self._force_transition(S.FAILED, reason=reason, evidence=evidence)

    def resolve_unknown_as_completed(self, evidence: dict, reason: str) -> None:
        """Vía simétrica: UNKNOWN puede resolver en éxito, con evidencia."""
        if self._state != S.UNKNOWN:
            raise ValueError(
                f"resolve_unknown_as_completed solo aplica desde UNKNOWN, "
                f"estado actual: {self._state.value}"
            )
        if not evidence:
            raise ValueError("resolve_unknown_as_completed requiere evidence no vacío.")
        self._force_transition(S.COMPLETED, reason=reason, evidence=evidence)
