"""
repository.py
==============
Cierra el hueco entre FASE 1 (FSM en memoria) y FASE 2 (persistencia):
hasta ahora, `store.commit_cognitive_cycle_transition()` acepta
cualquier string como `new_state` y lo escribe si gana el Commit Gate —
nada le impide escribir una transición que la FSM rechazaría (ej.
UNKNOWN -> FAILED sin evidencia). Eso significa que las garantías de
fsm.py (P-013, I-005, P-061, P-063) protegían la memoria, pero no lo que
queda realmente en SQLite.

`CognitiveCycleRepository` es la única vía sancionada para mutar un
cognitive_cycle persistido:

    load -> reconstruir CognitiveCycleFSM en el estado real de la fila
    transition -> se valida PRIMERO en la FSM (guards incluidos);
                  solo si la FSM la acepta, se intenta el commit real
    commit -> Commit Gate de store.py con expected_version; si otro
              proceso ya avanzó la versión, ConflictError (hay que
              recargar y reintentar, no reescribir a ciegas)

Ningún código de aplicación debería llamar a
`store.commit_cognitive_cycle_transition()` directamente para mutar
estado de negocio; ese método queda como primitiva de bajo nivel, y este
repository es la interfaz pública.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .canonical import content_hash
from .cognitive_cycle_fsm import CognitiveCycleFSM
from .enums import CognitiveCycleState
from .fsm import GuardedTransitionError, InvalidTransitionError
from .store import ConflictError, SQLiteStore


class CycleNotFoundError(ValueError):
    pass


@dataclass(frozen=True)
class LoadedCycle:
    fsm: CognitiveCycleFSM
    version: int
    task_id: str


class CognitiveCycleRepository:
    """
    Envuelve un SQLiteStore. No reemplaza store.py (que sigue siendo la
    única parte del sistema que habla SQL, P-001) — lo que hace es forzar
    que toda mutación de un cognitive_cycle pase primero por la FSM.
    """

    def __init__(self, store: SQLiteStore):
        self._store = store

    def load(self, cycle_id: str) -> LoadedCycle:
        row = self._store.get_cognitive_cycle(cycle_id)
        if row is None:
            raise CycleNotFoundError(f"cognitive_cycle {cycle_id!r} no existe")

        fsm = CognitiveCycleFSM()
        # CognitiveCycleFSM nace en CREATED; hay que llevarla al estado
        # real de la fila SIN pasar por transition() (que exigiría
        # recorrer todo el camino y validaría de más). Esto es
        # reconstrucción de estado persistido, no una transición nueva:
        # se usa el mismo mecanismo interno que usan los guards
        # (_force_transition), pero solo si el estado destino existe en
        # la tabla de transiciones válidas desde CREATED-alcanzable, para
        # no reconstruir un estado imposible por corrupción de datos.
        target_state = CognitiveCycleState(row["state"])
        if target_state != CognitiveCycleState.CREATED:
            fsm._state = target_state  # reconstrucción de snapshot, no transición
        return LoadedCycle(fsm=fsm, version=row["cycle_version"], task_id=row["task_id"])

    def transition(
        self,
        cycle_id: str,
        to_state: CognitiveCycleState,
        reason: Optional[str] = None,
        domain_event_payload: Optional[dict] = None,
    ) -> LoadedCycle:
        """
        Atajo de conveniencia: carga el estado más reciente y transiciona
        de inmediato. Correcto para el caso simple (sin cómputo largo
        entre leer y escribir). Para el caso realista — cargar, hacer
        reasoning/tool calls, y SOLO ENTONCES intentar comitear — usar
        load() + apply_transition() por separado, para que un snapshot
        viejo pueda de verdad perder la carrera contra el Commit Gate
        (ver test_repository.py, caso 4).
        """
        loaded = self.load(cycle_id)
        return self.apply_transition(cycle_id, loaded, to_state, reason=reason, domain_event_payload=domain_event_payload)

    def apply_transition(
        self,
        cycle_id: str,
        loaded: LoadedCycle,
        to_state: CognitiveCycleState,
        reason: Optional[str] = None,
        domain_event_payload: Optional[dict] = None,
    ) -> LoadedCycle:
        """
        Valida en la FSM del snapshot `loaded` (puede lanzar
        InvalidTransitionError o GuardedTransitionError SIN tocar la
        base) y solo si es válida intenta el commit real usando
        `loaded.version` como expected_version — si alguien más ya
        avanzó la versión desde que se cargó `loaded`, ConflictError
        (hay que recargar y decidir si la transición sigue teniendo
        sentido, no reintentar a ciegas).
        """
        loaded.fsm.transition(to_state, reason=reason)  # valida legalidad de la FSM primero, sobre el snapshot

        new_hash = content_hash({"cycle_id": cycle_id, "state": to_state.value, "reason": reason})
        result = self._store.commit_cognitive_cycle_transition(
            cycle_id=cycle_id,
            new_state=to_state.value,
            expected_version=loaded.version,
            new_state_hash=new_hash,
            new_context_hash=new_hash,  # simplificado; context real vendría del caller en FASE 3
            domain_event_payload=domain_event_payload,
        )
        return LoadedCycle(fsm=loaded.fsm, version=result.new_version, task_id=loaded.task_id)

    def resolve_unknown_as_failed(
        self, cycle_id: str, evidence: dict, reason: str, domain_event_payload: Optional[dict] = None
    ) -> LoadedCycle:
        """
        Misma lógica que transition(), pero para el camino guarded
        (P-013). La FSM exige evidencia no vacía ANTES de tocar la base
        — si la evidencia falta, ni siquiera se intenta el commit.
        """
        loaded = self.load(cycle_id)
        loaded.fsm.resolve_unknown_as_failed(evidence=evidence, reason=reason)  # valida + exige evidencia

        new_hash = content_hash({"cycle_id": cycle_id, "state": "FAILED", "evidence": evidence})
        result = self._store.commit_cognitive_cycle_transition(
            cycle_id=cycle_id,
            new_state=CognitiveCycleState.FAILED.value,
            expected_version=loaded.version,
            new_state_hash=new_hash,
            new_context_hash=new_hash,
            domain_event_payload=domain_event_payload,
        )
        return LoadedCycle(fsm=loaded.fsm, version=result.new_version, task_id=loaded.task_id)
