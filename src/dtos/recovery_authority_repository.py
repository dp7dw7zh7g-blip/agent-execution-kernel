"""
recovery_authority_repository.py
==================================
Mismo patrón que repository.py y effect_repository.py, aplicado a
RecoveryTriggerFSM y BootstrapFSM. Cierra los dos huecos restantes
anotados en corrección 179.

RecoveryTriggerRepository: usa la tabla mutable `recovery_triggers`
(Commit Gate) — `declare_failure` exige `declared_by` ANTES de intentar
el commit, igual que los otros repositories.

AuthorityBootstrapRepository: BootstrapFSM no necesita una tabla mutable
propia — sus estados previos a GENESIS_WRITTEN (COLD_START, CANDIDATE,
QUORUM_CHECK) son puramente en memoria (decisiones de arranque, todavía
no hay nada que persistir), y GENESIS_WRITTEN/BOOTSTRAP_CONFLICT ya
están cubiertos por `store.write_genesis_authority()` (el UNIQUE INDEX
de schema.sql). Este repository conecta ambos: si el store dice que ya
existe genesis, la FSM se lleva a BOOTSTRAP_CONFLICT — nunca al revés
(nunca se le pide al store que sobrescriba porque la FSM "quiere"
GENESIS_WRITTEN).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Optional

from .enums import BootstrapState as B
from .enums import RecoveryTriggerState as R
from .recovery_bootstrap_fsm import BootstrapFSM, RecoveryTriggerFSM
from .store import BootstrapConflictError, SQLiteStore


class TriggerNotFoundError(ValueError):
    pass


@dataclass(frozen=True)
class LoadedTrigger:
    fsm: RecoveryTriggerFSM
    version: int


class RecoveryTriggerRepository:
    def __init__(self, store: SQLiteStore):
        self._store = store

    def create(self, trigger_id: str, suspected_node_id: str, incarnation_at_suspicion: int) -> LoadedTrigger:
        fsm = RecoveryTriggerFSM()  # nace en NONE
        self._store.create_recovery_trigger(trigger_id, suspected_node_id, incarnation_at_suspicion)
        return LoadedTrigger(fsm=fsm, version=1)

    def load(self, trigger_id: str) -> LoadedTrigger:
        row = self._store.get_recovery_trigger(trigger_id)
        if row is None:
            raise TriggerNotFoundError(f"recovery_trigger {trigger_id!r} no existe")
        fsm = RecoveryTriggerFSM()
        target = R(row["state"])
        if target != R.NONE:
            fsm._state = target
        return LoadedTrigger(fsm=fsm, version=row["version"])

    def suspect(self, trigger_id: str, reason: str) -> LoadedTrigger:
        loaded = self.load(trigger_id)
        loaded.fsm.suspect(reason=reason)
        result = self._store.commit_recovery_trigger_transition(trigger_id, R.SUSPECT.value, loaded.version)
        return LoadedTrigger(fsm=loaded.fsm, version=result.new_version)

    def declare_failure(self, trigger_id: str, declared_by: str, evidence: dict) -> LoadedTrigger:
        """
        P-061 de punta a punta: declare_failure() de la FSM exige
        declared_by no vacío y evidence no vacío ANTES de que este método
        intente tocar SQLite. Si la FSM lo rechaza, no hay commit.
        """
        loaded = self.load(trigger_id)
        loaded.fsm.declare_failure(declared_by=declared_by, evidence=evidence)  # valida primero
        result = self._store.commit_recovery_trigger_transition(
            trigger_id, R.DECLARED.value, loaded.version, declared_by=declared_by, evidence=evidence
        )
        return LoadedTrigger(fsm=loaded.fsm, version=result.new_version)

    def start_recovery(self, trigger_id: str, reason: str) -> LoadedTrigger:
        loaded = self.load(trigger_id)
        loaded.fsm.start_recovery(reason=reason)
        result = self._store.commit_recovery_trigger_transition(trigger_id, R.RECOVERY_STARTING.value, loaded.version)
        return LoadedTrigger(fsm=loaded.fsm, version=result.new_version)


class AuthorityBootstrapRepository:
    def __init__(self, store: SQLiteStore):
        self._store = store

    def bootstrap(self, installation_id: str, principal: str = "system") -> BootstrapFSM:
        """
        Recorre COLD_START -> CANDIDATE -> QUORUM_CHECK enteramente en
        memoria (P-001: decisiones de arranque, sin efecto persistido
        todavía), y solo al llegar a QUORUM_CHECK intenta el commit real.
        Si store.write_genesis_authority() rechaza por conflicto
        (P-063), la FSM se lleva a BOOTSTRAP_CONFLICT — el rechazo de
        SQLite es la fuente de verdad, la FSM solo lo refleja.
        """
        fsm = BootstrapFSM()
        fsm.transition(B.CANDIDATE, reason="single-node claim (simplificado; quorum real es FASE 8)")
        fsm.transition(B.QUORUM_CHECK, reason="verificación previa a genesis")

        try:
            self._store.write_genesis_authority(
                authority_id=str(uuid.uuid4()), installation_id=installation_id, principal=principal
            )
        except BootstrapConflictError:
            fsm.write_genesis(installation_id=installation_id, existing_record_found=True)
            return fsm

        fsm.write_genesis(installation_id=installation_id, existing_record_found=False)
        fsm.activate()
        return fsm
