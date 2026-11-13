"""
contracts.py
============
DTOs de los contratos centrales descritos en el Registro Maestro:
REASONING GATEWAY, EXTERNAL EFFECT, RECOVERY TRIGGER, AUTHORITY BOOTSTRAP.

Convenciones para TODOS los DTOs de este paquete:
- @dataclass(frozen=True): un DTO no se muta, se reemplaza (P-051:
  Historical Decision != Rewritable Record aplica también a nivel de dato).
- __post_init__ valida forma; falla rápido con ContractValidationError.
- to_canonical_dict(): forma serializable determinista, usada por
  canonical.content_hash(). NUNCA usar __dict__ directamente para hashing,
  porque no garantiza estabilidad de tipos (P-064).
- Ningún DTO de este archivo ejecuta I/O, red, ni SQLite (P-001, SECURITY
  BOUNDARIES: "el modelo recibe datos, no autoridad").
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .enums import BootstrapState, EffectStatus, ReasoningResultStatus, RecoveryTriggerState
from .validation import (
    require_hash_like,
    require_nonempty_str,
    require_nonnegative_int,
    require_optional_str,
    require_positive_int,
)


# ---------------------------------------------------------------------------
# REASONING GATEWAY
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReasoningRequest:
    """Cognitive Core -> Reasoning Gateway. Ver sección REASONING GATEWAY."""

    request_id: str
    task_id: str
    cycle_id: str
    problem_hash: str
    representation_version: str
    input_payload: dict
    max_iterations: int
    max_duration_ms: int
    allow_partial: bool
    allow_early_stop: bool
    provider_hint: Optional[str] = None

    def __post_init__(self) -> None:
        require_nonempty_str(self.request_id, "request_id")
        require_nonempty_str(self.task_id, "task_id")
        require_nonempty_str(self.cycle_id, "cycle_id")
        require_hash_like(self.problem_hash, "problem_hash")
        require_nonempty_str(self.representation_version, "representation_version")
        require_positive_int(self.max_iterations, "max_iterations")
        require_positive_int(self.max_duration_ms, "max_duration_ms")
        require_optional_str(self.provider_hint, "provider_hint")
        if not isinstance(self.input_payload, dict):
            raise TypeError("input_payload debe ser dict")

    def to_canonical_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "task_id": self.task_id,
            "cycle_id": self.cycle_id,
            "problem_hash": self.problem_hash,
            "representation_version": self.representation_version,
            "input_payload": self.input_payload,
            "max_iterations": self.max_iterations,
            "max_duration_ms": self.max_duration_ms,
            "allow_partial": self.allow_partial,
            "allow_early_stop": self.allow_early_stop,
            "provider_hint": self.provider_hint,
        }


@dataclass(frozen=True)
class ReasoningResult:
    """
    Reasoning Gateway -> Verification.
    COMPLETED significa que terminó el proveedor, no que acertó (ver
    documento). Ese hecho NO se valida aquí: el dato solo registra el
    status declarado; la interpretación es responsabilidad de Verification.
    """

    request_id: str
    provider_id: str
    provider_version: str
    status: ReasoningResultStatus
    output_payload: dict
    iterations_used: int
    duration_ms: int
    input_hash: str
    output_hash: str
    termination_reason: str

    def __post_init__(self) -> None:
        require_nonempty_str(self.request_id, "request_id")
        require_nonempty_str(self.provider_id, "provider_id")
        require_nonempty_str(self.provider_version, "provider_version")
        if not isinstance(self.status, ReasoningResultStatus):
            raise TypeError("status debe ser ReasoningResultStatus")
        require_nonnegative_int(self.iterations_used, "iterations_used")
        require_nonnegative_int(self.duration_ms, "duration_ms")
        require_hash_like(self.input_hash, "input_hash")
        require_hash_like(self.output_hash, "output_hash")
        require_nonempty_str(self.termination_reason, "termination_reason")
        if not isinstance(self.output_payload, dict):
            raise TypeError("output_payload debe ser dict")

    def to_canonical_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "status": self.status.value,
            "output_payload": self.output_payload,
            "iterations_used": self.iterations_used,
            "duration_ms": self.duration_ms,
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
            "termination_reason": self.termination_reason,
        }


# ---------------------------------------------------------------------------
# EXTERNAL EFFECT
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EffectIntent:
    """Intención de efecto externo, previa a cualquier dispatch."""

    intent_id: str
    operation_id: str
    provider: str
    idempotency_key: str
    payload: dict
    fencing_token: Optional[str] = None

    def __post_init__(self) -> None:
        require_nonempty_str(self.intent_id, "intent_id")
        require_nonempty_str(self.operation_id, "operation_id")
        require_nonempty_str(self.provider, "provider")
        require_nonempty_str(self.idempotency_key, "idempotency_key")
        require_optional_str(self.fencing_token, "fencing_token")
        if not isinstance(self.payload, dict):
            raise TypeError("payload debe ser dict")

    def to_canonical_dict(self) -> dict:
        return {
            "intent_id": self.intent_id,
            "operation_id": self.operation_id,
            "provider": self.provider,
            "idempotency_key": self.idempotency_key,
            "payload": self.payload,
            "fencing_token": self.fencing_token,
        }


@dataclass(frozen=True)
class EffectAttempt:
    """
    Un intento concreto de materializar un EffectIntent.
    status UNKNOWN es válido y esperado (I-005: uncertain attempt debe
    resolverse vía Reconciliation antes de retry obligatorio).
    """

    attempt_id: str
    intent_id: str
    status: EffectStatus
    receipt: Optional[dict] = None
    reconciliation_required: bool = False

    def __post_init__(self) -> None:
        require_nonempty_str(self.attempt_id, "attempt_id")
        require_nonempty_str(self.intent_id, "intent_id")
        if not isinstance(self.status, EffectStatus):
            raise TypeError("status debe ser EffectStatus")
        if self.receipt is not None and not isinstance(self.receipt, dict):
            raise TypeError("receipt debe ser dict o None")
        if self.status == EffectStatus.UNKNOWN and not self.reconciliation_required:
            raise ContractInconsistencyError(
                "EffectAttempt en UNKNOWN debe declarar reconciliation_required=True "
                "(I-005 / T-UNK-01: UNKNOWN fuerza Reconciliation, nunca retry directo)."
            )

    def to_canonical_dict(self) -> dict:
        return {
            "attempt_id": self.attempt_id,
            "intent_id": self.intent_id,
            "status": self.status.value,
            "receipt": self.receipt,
            "reconciliation_required": self.reconciliation_required,
        }


class ContractInconsistencyError(ValueError):
    """
    Error de invariante violado a nivel de dato (distinto de error de
    forma). Se usa cuando dos campos del mismo DTO se contradicen respecto
    a una regla explícita del Registro Maestro (ej. I-005).
    """


# ---------------------------------------------------------------------------
# RECOVERY TRIGGER (corrección 175)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RecoveryTrigger:
    """
    Artifact que dispara RECOVERY.STARTING. Ver P-060/P-061, I-031/I-033.
    declared_by debe ser una autoridad vigente: eso se valida en la capa
    de dominio (no aquí, que es solo forma), porque requiere consultar
    Authority — cosa que un DTO puro no debe hacer (P-001).
    """

    trigger_id: str
    suspected_node_id: str
    incarnation_at_suspicion: int
    evidence: dict
    declared_by: str
    state: RecoveryTriggerState = RecoveryTriggerState.SUSPECT

    def __post_init__(self) -> None:
        require_nonempty_str(self.trigger_id, "trigger_id")
        require_nonempty_str(self.suspected_node_id, "suspected_node_id")
        require_nonnegative_int(self.incarnation_at_suspicion, "incarnation_at_suspicion")
        require_nonempty_str(self.declared_by, "declared_by")
        if not isinstance(self.evidence, dict):
            raise TypeError("evidence debe ser dict")
        if not isinstance(self.state, RecoveryTriggerState):
            raise TypeError("state debe ser RecoveryTriggerState")

    def to_canonical_dict(self) -> dict:
        return {
            "trigger_id": self.trigger_id,
            "suspected_node_id": self.suspected_node_id,
            "incarnation_at_suspicion": self.incarnation_at_suspicion,
            "evidence": self.evidence,
            "declared_by": self.declared_by,
            "state": self.state.value,
        }


# ---------------------------------------------------------------------------
# AUTHORITY BOOTSTRAP (corrección 175)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GenesisAuthorityRecord:
    """
    epoch=0 -> epoch=1. Ver P-062/P-063, I-032/I-034.
    Unicidad real (constraint por installation_id) se impone en SQLite,
    no aquí — este DTO solo puede rechazar formas inválidas, no
    duplicados (eso requiere estado compartido, fuera del alcance de un DTO).
    """

    installation_id: str
    epoch: int
    fencing_token: int
    state: BootstrapState = BootstrapState.GENESIS_WRITTEN

    def __post_init__(self) -> None:
        require_nonempty_str(self.installation_id, "installation_id")
        if self.epoch != 1:
            raise ContractInconsistencyError(
                "GenesisAuthorityRecord siempre nace con epoch=1 (P-062: "
                "Genesis Authority != Re-derivable from timeout; no hay "
                "genesis 'parcial' con otro epoch)."
            )
        if self.fencing_token != 1:
            raise ContractInconsistencyError(
                "GenesisAuthorityRecord siempre nace con fencing_token=1."
            )
        if not isinstance(self.state, BootstrapState):
            raise TypeError("state debe ser BootstrapState")

    def to_canonical_dict(self) -> dict:
        return {
            "installation_id": self.installation_id,
            "epoch": self.epoch,
            "fencing_token": self.fencing_token,
            "state": self.state.value,
        }
