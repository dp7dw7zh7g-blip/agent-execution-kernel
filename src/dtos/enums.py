"""
enums.py
========
Estados formales de todas las FSM descritas en el Registro Maestro de
Arquitectura. Cada Enum referencia la sección del documento de la que
proviene, para trazabilidad (P-039: Artifact identity != Content identity
también aplica a "de dónde vino este contrato").

No contiene lógica de transición (eso vive en las FSM, fase 1 - paso 2).
Aquí solo se congela el vocabulario de estados válidos.
"""

from enum import Enum


class ReasoningResultStatus(str, Enum):
    """Sección REASONING GATEWAY. COMPLETED != acertó, solo != terminó."""
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    STOPPED = "STOPPED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    INVALID_OUTPUT = "INVALID_OUTPUT"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class CognitiveCycleState(str, Enum):
    """Sección COGNITIVE CYCLE FSM."""
    CREATED = "CREATED"
    PREPARING = "PREPARING"
    CONTEXT_ASSEMBLING = "CONTEXT_ASSEMBLING"
    REPRESENTING = "REPRESENTING"
    REASONING = "REASONING"
    VERIFYING = "VERIFYING"
    RESULT_READY = "RESULT_READY"
    PLANNING_UPDATE = "PLANNING_UPDATE"
    PROJECTING = "PROJECTING"
    CONTROL = "CONTROL"
    READY_TO_COMMIT = "READY_TO_COMMIT"
    COMMITTING = "COMMITTING"
    COMPLETED = "COMPLETED"
    # especiales
    WAITING = "WAITING"
    NEEDS_INFORMATION = "NEEDS_INFORMATION"
    NEEDS_HUMAN = "NEEDS_HUMAN"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    CANCELLED = "CANCELLED"


class FinalRevalidationResult(str, Enum):
    """Sección FINAL REVALIDATION."""
    VALID = "VALID"
    STATE_CHANGED = "STATE_CHANGED"
    DEPENDENCY_CHANGED = "DEPENDENCY_CHANGED"
    PLAN_CHANGED = "PLAN_CHANGED"
    POLICY_CHANGED = "POLICY_CHANGED"
    AUTHORITY_CHANGED = "AUTHORITY_CHANGED"
    LEASE_INVALID = "LEASE_INVALID"
    GRAPH_CHANGED = "GRAPH_CHANGED"
    CAPABILITY_CHANGED = "CAPABILITY_CHANGED"
    RESOURCE_CHANGED = "RESOURCE_CHANGED"
    UNKNOWN = "UNKNOWN"


class EffectStatus(str, Enum):
    """Sección EXTERNAL EFFECT."""
    INTENDED = "INTENDED"
    DISPATCHED = "DISPATCHED"
    CONFIRMED = "CONFIRMED"
    PARTIAL = "PARTIAL"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


class ReconciliationResult(str, Enum):
    """Sección RECONCILIATION."""
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    PARTIAL = "PARTIAL"
    STILL_UNKNOWN = "STILL_UNKNOWN"
    CONTRADICTORY = "CONTRADICTORY"


class OperationClosureState(str, Enum):
    """Sección OPERATION CLOSURE."""
    OPEN = "OPEN"
    RUNNING = "RUNNING"
    ASSESSING = "ASSESSING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    UNKNOWN = "UNKNOWN"
    RECOVERING = "RECOVERING"
    RECOVERED = "RECOVERED"
    CANCELLED = "CANCELLED"
    ABANDONED = "ABANDONED"
    REQUIRES_HUMAN = "REQUIRES_HUMAN"


class RecoveryState(str, Enum):
    """Sección RECOVERY."""
    STARTING = "STARTING"
    STORAGE_RECOVERY = "STORAGE_RECOVERY"
    SCANNING = "SCANNING"
    PLANNING = "PLANNING"
    RECOVERING = "RECOVERING"
    VERIFYING = "VERIFYING"
    DEGRADED = "DEGRADED"
    READY = "READY"
    BLOCKED = "BLOCKED"


class RecoveryLeadershipState(str, Enum):
    """Sección RECOVERY LEADERSHIP."""
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"
    FOLLOWER = "FOLLOWER"
    REVOKED = "REVOKED"
    EXPIRED = "EXPIRED"
    FENCED = "FENCED"
    UNKNOWN = "UNKNOWN"


class StaleLeaderState(str, Enum):
    """Sección STALE LEADER."""
    HEALTHY = "HEALTHY"
    SUSPECT = "SUSPECT"
    STALE = "STALE"
    FENCED = "FENCED"
    DRAINING = "DRAINING"
    STOPPED = "STOPPED"


class RecoveryTriggerState(str, Enum):
    """Sección RECOVERY TRIGGER (corrección 175). P-060, I-031, I-033."""
    NONE = "NONE"
    SUSPECT = "SUSPECT"
    DECLARED = "DECLARED"
    RECOVERY_STARTING = "RECOVERY_STARTING"


class BootstrapState(str, Enum):
    """Sección AUTHORITY BOOTSTRAP (corrección 175). P-062, P-063, I-032, I-034."""
    COLD_START = "COLD_START"
    CANDIDATE = "CANDIDATE"
    QUORUM_CHECK = "QUORUM_CHECK"
    GENESIS_WRITTEN = "GENESIS_WRITTEN"
    ACTIVE = "ACTIVE"
    BOOTSTRAP_INCOMPLETE = "BOOTSTRAP_INCOMPLETE"
    BOOTSTRAP_CONFLICT = "BOOTSTRAP_CONFLICT"


class RecoveryGraphState(str, Enum):
    """Sección RECOVERY GRAPH."""
    DRAFT = "DRAFT"
    VALIDATING = "VALIDATING"
    VERIFIED = "VERIFIED"
    ACTIVE = "ACTIVE"
    SUPERSEDED = "SUPERSEDED"
    INVALIDATED = "INVALIDATED"
    ABANDONED = "ABANDONED"


class ProjectionState(str, Enum):
    """Sección PROJECTIONS."""
    HEALTHY = "HEALTHY"
    LAGGING = "LAGGING"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


class AuditIntegrityState(str, Enum):
    """Sección AUDIT INTEGRITY."""
    VALID = "VALID"
    INVALID = "INVALID"
    BROKEN = "BROKEN"
    TRUNCATED = "TRUNCATED"
    FORKED = "FORKED"
    CORRUPTED = "CORRUPTED"
    MISSING = "MISSING"
    UNKNOWN = "UNKNOWN"


class ReservationState(str, Enum):
    """Sección RESOURCES."""
    REQUESTED = "REQUESTED"
    ACTIVE = "ACTIVE"
    CONSUMED = "CONSUMED"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class MandateStrength(str, Enum):
    """REG-017 Mandates (parche 0009, D-52): fuerza con la que se concedió el mandato a la Task."""
    EXPLICIT = "EXPLICIT"
    INFERRED = "INFERRED"


class MandateTriggerKind(str, Enum):
    """REG-017 Mandates (parche 0009, D-52): origen de la acción que motivó el mandato. SENSOR exige una regla de política."""
    USER = "USER"
    AGENT = "AGENT"
    SENSOR = "SENSOR"


class MandateEventType(str, Enum):
    """REG-017 mandate_events (parche 0009, D-52). Solo MANDATE_CREATED existe hoy; los eventos de revalidación llegan con el parche 0011."""
    MANDATE_CREATED = "MANDATE_CREATED"
