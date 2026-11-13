"""
fsm.py
======
Motor genérico de máquina de estados finita. Este es el "paso 2" de
FASE 1: las FSM dejan de ser diagramas en el Registro Maestro y pasan a
ser código que puede rechazar una transición inválida en vez de
permitirla silenciosamente.

Diseño deliberado:
- La tabla de transiciones es la única fuente de verdad sobre qué
  transición es válida. No hay lógica de negocio aquí, solo forma.
- Toda transición exitosa queda en `history` (list de TransitionRecord),
  nunca se sobrescribe — coherente con P-051 (Historical Decision !=
  Rewritable Record) aplicado a nivel de FSM en memoria.
- `transition()` es síncrono y no hace I/O. Persistir la transición
  (SQLite, Commit Gate) es responsabilidad de la capa que envuelve esta
  FSM (FASE 2), no de este archivo.
- Una subclase puede declarar `guarded_transitions`: transiciones que
  existen en la tabla pero que NO deben poder hacerse por transition()
  directo — solo por un método explícito con nombre propio. Esto es lo
  que permite convertir un principio en prosa (ej. P-013) en algo que el
  código impide, no solo documenta.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, FrozenSet, Optional, Type


class InvalidTransitionError(ValueError):
    """La transición pedida no existe en la tabla de la FSM."""


class GuardedTransitionError(ValueError):
    """
    La transición existe en la tabla pero está bloqueada para
    transition() directo — requiere pasar por el método explícito que
    documenta la razón (ver guarded_transitions en la subclase).
    """


@dataclass(frozen=True)
class TransitionRecord:
    from_state: str
    to_state: str
    reason: Optional[str] = None
    evidence: Optional[dict] = None


class FiniteStateMachine:
    """
    Clase base. Una subclase concreta (ver cognitive_cycle_fsm.py,
    effect_fsm.py, etc.) define:
      - state_enum: el Enum de estados válidos
      - transitions: Dict[state_enum, FrozenSet[state_enum]]
      - guarded_transitions: FrozenSet[(from, to)] (opcional)
    """

    state_enum: Type[Enum]
    transitions: Dict[Enum, FrozenSet[Enum]] = {}
    guarded_transitions: FrozenSet = frozenset()

    def __init__(self, initial_state: Enum):
        if not isinstance(initial_state, self.state_enum):
            raise TypeError(
                f"initial_state debe ser {self.state_enum.__name__}, "
                f"recibido {type(initial_state).__name__}"
            )
        self._state = initial_state
        self._history: list[TransitionRecord] = []

    @property
    def state(self) -> Enum:
        return self._state

    @property
    def history(self) -> tuple:
        return tuple(self._history)

    def can_transition(self, to_state: Enum) -> bool:
        allowed = self.transitions.get(self._state, frozenset())
        return to_state in allowed

    def transition(self, to_state: Enum, reason: Optional[str] = None) -> None:
        if not isinstance(to_state, self.state_enum):
            raise TypeError(f"to_state debe ser {self.state_enum.__name__}")

        if (self._state, to_state) in self.guarded_transitions:
            raise GuardedTransitionError(
                f"{self._state.value} -> {to_state.value} está protegida: "
                f"usar el método explícito de la subclase, no transition() directo."
            )

        if not self.can_transition(to_state):
            allowed = sorted(s.value for s in self.transitions.get(self._state, frozenset()))
            raise InvalidTransitionError(
                f"Transición inválida: {self._state.value} -> {to_state.value}. "
                f"Permitidas desde {self._state.value}: {allowed}"
            )

        self._history.append(
            TransitionRecord(from_state=self._state.value, to_state=to_state.value, reason=reason)
        )
        self._state = to_state

    def _force_transition(self, to_state: Enum, reason: str, evidence: Optional[dict] = None) -> None:
        """
        Uso exclusivo de subclases, para materializar una transición
        `guarded` a través de un método con nombre propio (ej.
        `resolve_unknown_as_failed(evidence=...)`). No es de uso público.
        """
        if not self.can_transition(to_state):
            raise InvalidTransitionError(
                f"_force_transition: {self._state.value} -> {to_state.value} "
                f"no está en la tabla de transiciones en absoluto (ni siquiera guarded)."
            )
        self._history.append(
            TransitionRecord(
                from_state=self._state.value,
                to_state=to_state.value,
                reason=reason,
                evidence=evidence,
            )
        )
        self._state = to_state
