"""
recovery_bootstrap_fsm.py
==========================
FSM de RecoveryTrigger y de Authority Bootstrap (corrección 175 del
Registro Maestro).

RecoveryTriggerFSM: NONE -> SUSPECT -> DECLARED -> RECOVERY_STARTING.
P-061 (Failure Declaration requires standing authority) se aplica aquí
como guard: SUSPECT -> DECLARED solo es alcanzable vía `declare_failure`,
que exige pasar `declared_by` (una autoridad), nunca transition() plano.

BootstrapFSM: COLD_START -> CANDIDATE -> QUORUM_CHECK -> GENESIS_WRITTEN
-> ACTIVE, con las salidas BOOTSTRAP_INCOMPLETE y BOOTSTRAP_CONFLICT.
P-063 (Bootstrap conflict != Overwrite) se aplica como guard: una vez en
BOOTSTRAP_CONFLICT, no existe transición de vuelta a GENESIS_WRITTEN
dentro de esta misma FSM — requiere intervención externa (una nueva
instancia de FSM tras resolución manual, no una transición automática).
"""

from __future__ import annotations

from typing import Optional

from .enums import BootstrapState as B
from .enums import RecoveryTriggerState as R
from .fsm import FiniteStateMachine


# ---------------------------------------------------------------------------
# RecoveryTriggerFSM
# ---------------------------------------------------------------------------

_TRIGGER_TRANSITIONS = {
    R.NONE: frozenset({R.SUSPECT}),
    R.SUSPECT: frozenset({R.DECLARED, R.NONE}),  # NONE = suspicion retirada (heartbeat volvió)
    R.DECLARED: frozenset({R.RECOVERY_STARTING}),
    R.RECOVERY_STARTING: frozenset(),  # a partir de aquí gobierna RecoveryState, fuera de esta FSM
}


class RecoveryTriggerFSM(FiniteStateMachine):
    state_enum = R
    transitions = _TRIGGER_TRANSITIONS
    guarded_transitions = frozenset({(R.SUSPECT, R.DECLARED)})

    def __init__(self) -> None:
        super().__init__(initial_state=R.NONE)

    def suspect(self, reason: str) -> None:
        self.transition(R.SUSPECT, reason=reason)

    def clear_suspicion(self, reason: str) -> None:
        """El nodo volvió a dar heartbeat antes de ser declarado DEAD."""
        self.transition(R.NONE, reason=reason)

    def declare_failure(self, declared_by: str, evidence: dict) -> None:
        """
        Única vía para SUSPECT -> DECLARED. P-061: requiere una autoridad
        de liderazgo activa (`declared_by`), no un nodo cualquiera.
        Esta función no VERIFICA que declared_by sea válida (eso requiere
        consultar Authority, fuera del alcance de una FSM pura, P-001) —
        pero hace estructuralmente imposible declarar sin identificar
        quién lo hace.
        """
        if self._state != R.SUSPECT:
            raise ValueError(f"declare_failure solo aplica desde SUSPECT, actual: {self._state.value}")
        if not declared_by or not declared_by.strip():
            raise ValueError("declare_failure requiere declared_by no vacío (P-061).")
        if not evidence:
            raise ValueError("declare_failure requiere evidence no vacío.")
        self._force_transition(
            R.DECLARED, reason=f"declared by {declared_by}", evidence=evidence
        )

    def start_recovery(self, reason: str) -> None:
        self.transition(R.RECOVERY_STARTING, reason=reason)


# ---------------------------------------------------------------------------
# BootstrapFSM
# ---------------------------------------------------------------------------

_BOOTSTRAP_TRANSITIONS = {
    B.COLD_START: frozenset({B.CANDIDATE}),
    B.CANDIDATE: frozenset({B.QUORUM_CHECK}),
    B.QUORUM_CHECK: frozenset({B.GENESIS_WRITTEN, B.BOOTSTRAP_INCOMPLETE, B.BOOTSTRAP_CONFLICT}),
    B.GENESIS_WRITTEN: frozenset({B.ACTIVE}),
    B.ACTIVE: frozenset(),
    B.BOOTSTRAP_INCOMPLETE: frozenset(),  # requiere reinicio del proceso, no una transición
    B.BOOTSTRAP_CONFLICT: frozenset(),  # P-063: nunca se sobrescribe, requiere intervención externa
}


class BootstrapFSM(FiniteStateMachine):
    state_enum = B
    transitions = _BOOTSTRAP_TRANSITIONS
    # Sin guarded_transitions: aquí la protección real es que
    # BOOTSTRAP_CONFLICT y BOOTSTRAP_INCOMPLETE simplemente no tienen
    # salidas en la tabla, así que ni _force_transition podría sacarlos
    # de ese estado. Eso es P-063 impuesto por construcción, no por guard.

    def __init__(self) -> None:
        super().__init__(initial_state=B.COLD_START)

    def write_genesis(self, installation_id: str, existing_record_found: bool) -> None:
        """
        Desde QUORUM_CHECK. Si ya existe un GENESIS_AUTHORITY_RECORD para
        esta instalación (reinstalación/restore), va a BOOTSTRAP_CONFLICT,
        nunca sobrescribe (P-063). La detección de "existing_record_found"
        es responsabilidad de la capa de persistencia (FASE 2), no de esta
        FSM — aquí solo se modela la bifurcación.
        """
        if not installation_id or not installation_id.strip():
            raise ValueError("write_genesis requiere installation_id no vacío.")
        if existing_record_found:
            self.transition(B.BOOTSTRAP_CONFLICT, reason=f"genesis ya existe para {installation_id}")
        else:
            self.transition(B.GENESIS_WRITTEN, reason=f"genesis escrito para {installation_id}")

    def mark_incomplete(self, reason: str) -> None:
        self.transition(B.BOOTSTRAP_INCOMPLETE, reason=reason)

    def activate(self, reason: str = "genesis confirmado") -> None:
        self.transition(B.ACTIVE, reason=reason)
