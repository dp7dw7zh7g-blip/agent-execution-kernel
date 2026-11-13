"""
Paquete `dtos` — FASE 1 del Registro Maestro de Arquitectura.

Contiene:
- enums.py       Vocabulario de estados de todas las FSM (sin transiciones).
- validation.py  Validadores de forma compartidos.
- canonical.py   Serialización canónica (JCS-like) + hashing (P-064).
- contracts.py   DTOs centrales: Reasoning, Effect, RecoveryTrigger, Genesis.
- registries.py  DTOs de REG-001 a REG-016.

Deliberadamente NO incluido aquí (fuera de alcance de FASE 1 - DTOs):
- Lógica de transición de FSM (fase 1, siguiente paso).
- Persistencia SQLite (FASE 2).
- Cualquier llamada a Reasoning Provider real (depende de OPEN-001/002).
"""

from .audit import AuditChain, AuditVerificationResult, GENESIS_HASH
from .canonical import canonical_json, content_hash
from .effect_repository import AttemptNotFoundError, EffectRepository
from .inbox_outbox import InboxProcessor, OutboxPublisher, ReceiveResult, ReceiveStatus
from .recovery_authority_repository import AuthorityBootstrapRepository, RecoveryTriggerRepository, TriggerNotFoundError
from .cognitive_cycle_fsm import CognitiveCycleFSM
from .contracts import (
    ContractInconsistencyError,
    EffectAttempt,
    EffectIntent,
    GenesisAuthorityRecord,
    ReasoningRequest,
    ReasoningResult,
    RecoveryTrigger,
)
from .effect_fsm import EffectFSM
from .fsm import FiniteStateMachine, GuardedTransitionError, InvalidTransitionError
from .recovery_bootstrap_fsm import BootstrapFSM, RecoveryTriggerFSM
from .repository import CognitiveCycleRepository, CycleNotFoundError
from .store import BootstrapConflictError, ConflictError, SQLiteStore
from .validation import ContractValidationError

__all__ = [
    "canonical_json",
    "content_hash",
    "AuditChain",
    "AuditVerificationResult",
    "GENESIS_HASH",
    "InboxProcessor",
    "OutboxPublisher",
    "ReceiveResult",
    "ReceiveStatus",
    "AttemptNotFoundError",
    "AuthorityBootstrapRepository",
    "EffectRepository",
    "RecoveryTriggerRepository",
    "TriggerNotFoundError",
    "BootstrapConflictError",
    "BootstrapFSM",
    "CognitiveCycleFSM",
    "CognitiveCycleRepository",
    "ConflictError",
    "ContractInconsistencyError",
    "ContractValidationError",
    "CycleNotFoundError",
    "EffectAttempt",
    "EffectFSM",
    "EffectIntent",
    "FiniteStateMachine",
    "GenesisAuthorityRecord",
    "GuardedTransitionError",
    "InvalidTransitionError",
    "ReasoningRequest",
    "ReasoningResult",
    "RecoveryTrigger",
    "RecoveryTriggerFSM",
    "SQLiteStore",
]
