"""
registries.py
=============
DTOs de los Registros Maestros (REG-001 a REG-016, más REG-017 Mandates del parche 0009). Estos son las
"tablas" que luego mapean 1:1 al SQLite schema baseline (corrección 176).

Nota de diseño: varios campos "status"/"state" en el documento original
no especifican de qué Enum provienen exactamente (ej. REG-006 Operations
"status" podría ser OperationClosureState o algo más granular). Aquí se
tipan con el Enum más específico cuando el documento lo deja claro, y
como `str` documentado cuando no — para no inventar una decisión que el
Registro Maestro no tomó explícitamente (evitar el error de "no inventar
logits" aplicado aquí a "no inventar enums no especificados").
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Optional

from .canonical import canonical_json
from .enums import (
    CognitiveCycleState,
    EffectStatus,
    MandateStrength,
    MandateTriggerKind,
    OperationClosureState,
    ProjectionState,
    ReasoningResultStatus,
)
from .validation import ContractValidationError, require_nonempty_str, require_nonnegative_int


@dataclass(frozen=True)
class ComponentRecord:
    """REG-001 Components."""

    component_id: str
    name: str
    responsibility: str
    lifecycle: str
    dependencies: tuple  # tuple de component_id, inmutable por diseño
    criticality: str
    version: str

    def __post_init__(self) -> None:
        require_nonempty_str(self.component_id, "component_id")
        require_nonempty_str(self.name, "name")
        require_nonempty_str(self.responsibility, "responsibility")
        require_nonempty_str(self.lifecycle, "lifecycle")
        require_nonempty_str(self.criticality, "criticality")
        require_nonempty_str(self.version, "version")
        if not isinstance(self.dependencies, tuple):
            raise TypeError("dependencies debe ser tuple de component_id")


@dataclass(frozen=True)
class ContractRecord:
    """REG-002 Contracts."""

    contract_id: str
    version: str
    producer: str
    consumer: str
    schema_hash: str
    compatibility: str

    def __post_init__(self) -> None:
        for f, v in (
            ("contract_id", self.contract_id),
            ("version", self.version),
            ("producer", self.producer),
            ("consumer", self.consumer),
            ("schema_hash", self.schema_hash),
            ("compatibility", self.compatibility),
        ):
            require_nonempty_str(v, f)


@dataclass(frozen=True)
class ProviderRecord:
    """REG-003 Providers."""

    provider_id: str
    family: str
    version: str
    location: str  # "stationary" | "edge" | otro, no forzado a Enum aquí
    capabilities: dict
    resources: dict
    status: str

    def __post_init__(self) -> None:
        for f, v in (
            ("provider_id", self.provider_id),
            ("family", self.family),
            ("version", self.version),
            ("location", self.location),
            ("status", self.status),
        ):
            require_nonempty_str(v, f)
        if not isinstance(self.capabilities, dict):
            raise TypeError("capabilities debe ser dict")
        if not isinstance(self.resources, dict):
            raise TypeError("resources debe ser dict")


class PolicyContentError(ContractValidationError):
    """El contenido de una política no es JSON canónico válido o su hash no coincide (parche 0008, D-51)."""


def policy_content_hash(content: str) -> str:
    """SHA-256 (hex) de `canonical_json(json.loads(content))` en UTF-8, exigiendo que `content` YA sea ese texto canónico.

    `content` es el texto JSON canónico, no un objeto: así el texto guardado en la columna `policies.content` son exactamente
    los bytes hasheados. Se rechaza cualquier texto que no sea canónico (espacios, claves desordenadas, NaN, claves duplicadas),
    porque dos textos distintos con el mismo significado producirían hashes distintos."""
    if not isinstance(content, str) or not content:
        raise PolicyContentError("content: debe ser un texto JSON canónico no vacío")
    try:
        canonical = canonical_json(json.loads(content))
        if canonical != content:
            raise PolicyContentError("content: no está en forma canónica (use canonical_json del objeto)")
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    except PolicyContentError:
        raise
    except (ValueError, TypeError, RecursionError) as e:  # JSON inválido, NaN/Infinity, no serializable, UTF-8 inválido
        raise PolicyContentError(f"content: JSON no válido para forma canónica ({type(e).__name__}: {e})") from e


@dataclass(frozen=True)
class PolicyRecord:
    """REG-004 Policies.

    `content` (parche 0008) es opcional: None = política sin contenido (el caso de las filas anteriores al parche, que solo guardan
    el hash). Si se da, debe ser el texto JSON canónico y `hash` debe ser exactamente `policy_content_hash(content)`."""

    policy_id: str
    version: str
    scope: str
    effective_from: str  # ISO 8601, validado como string no vacío aquí
    hash: str
    content: Optional[str] = None

    def __post_init__(self) -> None:
        for f, v in (
            ("policy_id", self.policy_id),
            ("version", self.version),
            ("scope", self.scope),
            ("effective_from", self.effective_from),
            ("hash", self.hash),
        ):
            require_nonempty_str(v, f)
        if self.content is not None:
            expected = policy_content_hash(self.content)
            if self.hash != expected:
                raise PolicyContentError(
                    f"hash: no coincide con el SHA-256 del contenido canónico (esperado {expected}, recibido {self.hash})"
                )


@dataclass(frozen=True)
class AuthorityRecord:
    """REG-005 Authorities."""

    authority_id: str
    principal: str
    epoch: int
    capabilities: tuple
    expiry: Optional[str]
    fencing: int

    def __post_init__(self) -> None:
        require_nonempty_str(self.authority_id, "authority_id")
        require_nonempty_str(self.principal, "principal")
        require_nonnegative_int(self.epoch, "epoch")
        require_nonnegative_int(self.fencing, "fencing")
        if not isinstance(self.capabilities, tuple):
            raise TypeError("capabilities debe ser tuple")


@dataclass(frozen=True)
class OperationRecord:
    """REG-006 Operations."""

    operation_id: str
    semantic_fingerprint: str
    status: OperationClosureState
    goal_id: str
    task_id: str

    def __post_init__(self) -> None:
        require_nonempty_str(self.operation_id, "operation_id")
        require_nonempty_str(self.semantic_fingerprint, "semantic_fingerprint")
        require_nonempty_str(self.goal_id, "goal_id")
        require_nonempty_str(self.task_id, "task_id")
        if not isinstance(self.status, OperationClosureState):
            raise TypeError("status debe ser OperationClosureState")


@dataclass(frozen=True)
class TaskRecord:
    """REG-007 Tasks."""

    task_id: str
    goal_id: str
    status: str
    current_cycle_id: Optional[str]

    def __post_init__(self) -> None:
        require_nonempty_str(self.task_id, "task_id")
        require_nonempty_str(self.goal_id, "goal_id")
        require_nonempty_str(self.status, "status")


@dataclass(frozen=True)
class CognitiveCycleRecord:
    """REG-008 Cognitive Cycles."""

    cycle_id: str
    task_id: str
    state: CognitiveCycleState
    cycle_version: int
    state_hash: str
    context_hash: str

    def __post_init__(self) -> None:
        require_nonempty_str(self.cycle_id, "cycle_id")
        require_nonempty_str(self.task_id, "task_id")
        require_nonnegative_int(self.cycle_version, "cycle_version")
        require_nonempty_str(self.state_hash, "state_hash")
        require_nonempty_str(self.context_hash, "context_hash")
        if not isinstance(self.state, CognitiveCycleState):
            raise TypeError("state debe ser CognitiveCycleState")


@dataclass(frozen=True)
class ReasoningRunRecord:
    """REG-009 Reasoning Runs."""

    request_id: str
    provider_id: str
    provider_version: str
    input_hash: str
    output_hash: str
    status: ReasoningResultStatus
    iterations: int
    duration: int

    def __post_init__(self) -> None:
        require_nonempty_str(self.request_id, "request_id")
        require_nonempty_str(self.provider_id, "provider_id")
        require_nonempty_str(self.provider_version, "provider_version")
        require_nonempty_str(self.input_hash, "input_hash")
        require_nonempty_str(self.output_hash, "output_hash")
        require_nonnegative_int(self.iterations, "iterations")
        require_nonnegative_int(self.duration, "duration")
        if not isinstance(self.status, ReasoningResultStatus):
            raise TypeError("status debe ser ReasoningResultStatus")


@dataclass(frozen=True)
class ArtifactRecord:
    """REG-010 Artifacts. Identidad = artifact_id + version + content_hash (P-039)."""

    artifact_id: str
    version: int
    artifact_class: str  # "class" es palabra reservada en Python
    content_hash: str
    provenance: str
    status: str

    def __post_init__(self) -> None:
        require_nonempty_str(self.artifact_id, "artifact_id")
        require_nonnegative_int(self.version, "version")
        for f, v in (
            ("artifact_class", self.artifact_class),
            ("content_hash", self.content_hash),
            ("provenance", self.provenance),
            ("status", self.status),
        ):
            require_nonempty_str(v, f)


@dataclass(frozen=True)
class EvidenceRecord:
    """REG-011 Evidence."""

    evidence_id: str
    source: str
    observation_id: str
    freshness: str
    reliability: str
    contradictions: tuple

    def __post_init__(self) -> None:
        for f, v in (
            ("evidence_id", self.evidence_id),
            ("source", self.source),
            ("observation_id", self.observation_id),
            ("freshness", self.freshness),
            ("reliability", self.reliability),
        ):
            require_nonempty_str(v, f)
        if not isinstance(self.contradictions, tuple):
            raise TypeError("contradictions debe ser tuple")


@dataclass(frozen=True)
class EffectRecord:
    """REG-012 Effects."""

    effect_id: str
    operation_id: str
    intent_id: str
    attempt_id: str
    provider: str
    idempotency_key: str
    status: EffectStatus

    def __post_init__(self) -> None:
        for f, v in (
            ("effect_id", self.effect_id),
            ("operation_id", self.operation_id),
            ("intent_id", self.intent_id),
            ("attempt_id", self.attempt_id),
            ("provider", self.provider),
            ("idempotency_key", self.idempotency_key),
        ):
            require_nonempty_str(v, f)
        if not isinstance(self.status, EffectStatus):
            raise TypeError("status debe ser EffectStatus")


@dataclass(frozen=True)
class RecoveryRecord:
    """REG-013 Recovery."""

    recovery_run_id: str
    epoch: int
    incarnation: int
    leadership: str
    checkpoint: str
    status: str

    def __post_init__(self) -> None:
        require_nonempty_str(self.recovery_run_id, "recovery_run_id")
        require_nonnegative_int(self.epoch, "epoch")
        require_nonnegative_int(self.incarnation, "incarnation")
        for f, v in (
            ("leadership", self.leadership),
            ("checkpoint", self.checkpoint),
            ("status", self.status),
        ):
            require_nonempty_str(v, f)


@dataclass(frozen=True)
class InvariantRecord:
    """REG-014 Invariants. Une el catálogo P-xxx/I-xxx con su evaluador real."""

    invariant_id: str  # ej. "P-013" o "I-003"
    severity: str
    scope: str
    evaluator: str
    policy: str

    def __post_init__(self) -> None:
        for f, v in (
            ("invariant_id", self.invariant_id),
            ("severity", self.severity),
            ("scope", self.scope),
            ("evaluator", self.evaluator),
            ("policy", self.policy),
        ):
            require_nonempty_str(v, f)


@dataclass(frozen=True)
class ProjectionRecord:
    """REG-015 Projections."""

    projection_id: str
    version: int
    checkpoint: str
    status: ProjectionState
    hash: str

    def __post_init__(self) -> None:
        require_nonempty_str(self.projection_id, "projection_id")
        require_nonnegative_int(self.version, "version")
        require_nonempty_str(self.checkpoint, "checkpoint")
        require_nonempty_str(self.hash, "hash")
        if not isinstance(self.status, ProjectionState):
            raise TypeError("status debe ser ProjectionState")


@dataclass(frozen=True)
class RetentionRecord:
    """REG-016 Retention. Archive != Delete (P-040); esto nunca borra directamente."""

    artifact_id: str
    retention_class: str
    references: tuple
    deletion_eligibility: bool

    def __post_init__(self) -> None:
        require_nonempty_str(self.artifact_id, "artifact_id")
        require_nonempty_str(self.retention_class, "retention_class")
        if not isinstance(self.references, tuple):
            raise TypeError("references debe ser tuple")


# Combinaciones (strength, trigger_kind) admitidas. PROVISIONAL (parche 0009): hoy se admiten las seis; la matriz cerrada por el usuario
# no está registrada en el repositorio, así que no se restringe nada sin saber qué excluye. Cuando se aporte, basta cambiar esta constante
# (la restricción vive en Python, no en SQL, para no exigir una migración). Lo único que SQL y el DTO imponen ya es: trigger_kind SENSOR
# exige una regla de política.
ALLOWED_MANDATE_COMBINATIONS = frozenset((s, t) for s in MandateStrength for t in MandateTriggerKind)


# D-53: `rule_hash` es un SHA-256 en hex minúscula (64 caracteres), el mismo formato que `PolicyRecord.hash` (hashlib.hexdigest()).
RULE_HASH_PATTERN = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class MandateRecord:
    """REG-017 Mandates (parche 0009, D-52): UN mandato por Task (alcance y fuerza concedidos a un trabajo), inmutable una vez creado.

    `rule_id`, `policy_id`, `policy_version` y `rule_hash` van juntos o ninguno; si `trigger_kind` es SENSOR son obligatorios (toda
    acción de origen sensor exige una regla de política preexistente). `rule_hash` es el hash de la regla en el contenido de la política
    (`policies.content`, parche 0008) que revalidará el parche 0011; aquí se valida su FORMATO (D-53: SHA-256 hex) y que la política exista
    y tenga contenido; que sea el hash de esa regla concreta lo comprueba el 0011."""

    mandate_id: str
    task_id: str
    strength: MandateStrength
    trigger_kind: MandateTriggerKind
    created_at: str
    rule_id: Optional[str] = None
    policy_id: Optional[str] = None
    policy_version: Optional[str] = None
    rule_hash: Optional[str] = None

    def __post_init__(self) -> None:
        for f, v in (("mandate_id", self.mandate_id), ("task_id", self.task_id), ("created_at", self.created_at)):
            require_nonempty_str(v, f)
        if not isinstance(self.strength, MandateStrength):
            raise ContractValidationError("strength: debe ser un MandateStrength")
        if not isinstance(self.trigger_kind, MandateTriggerKind):
            raise ContractValidationError("trigger_kind: debe ser un MandateTriggerKind")
        if (self.strength, self.trigger_kind) not in ALLOWED_MANDATE_COMBINATIONS:
            raise ContractValidationError(f"combinación no admitida: strength={self.strength.value}, trigger_kind={self.trigger_kind.value}")
        rule = (("rule_id", self.rule_id), ("policy_id", self.policy_id), ("policy_version", self.policy_version), ("rule_hash", self.rule_hash))
        present = [name for name, v in rule if v is not None]
        if present and len(present) != len(rule):
            raise ContractValidationError("rule_id, policy_id, policy_version y rule_hash van juntos o ninguno (dados: " + ", ".join(present) + ")")
        for name, v in rule:
            if v is not None:
                require_nonempty_str(v, name)
        if self.rule_hash is not None and not RULE_HASH_PATTERN.fullmatch(self.rule_hash):
            raise ContractValidationError("rule_hash: debe ser un SHA-256 en hexadecimal minúscula (64 caracteres)")
        if self.trigger_kind is MandateTriggerKind.SENSOR and not present:
            raise ContractValidationError("trigger_kind SENSOR exige rule_id, policy_id, policy_version y rule_hash")

    def to_canonical_dict(self) -> dict:
        return {
            "mandate_id": self.mandate_id,
            "task_id": self.task_id,
            "strength": self.strength.value,
            "trigger_kind": self.trigger_kind.value,
            "rule_id": self.rule_id,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "rule_hash": self.rule_hash,
            "created_at": self.created_at,
        }
