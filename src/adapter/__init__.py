"""Paquete `adapter` — frontera de protocolo UNAE 4.3S <-> DTOS (slice 0.2)."""
from .adapter import (
    MAX_CORRELATION_ID_BYTES,
    MAX_ENVELOPE_BYTES,
    MAX_EVIDENCE_REFS,
    MAX_EVIDENCE_REF_BYTES,
    MAX_EVIDENCE_SIZE_BYTES,
    MAX_TEXT_FIELD_BYTES,
    MAX_FENCING_TOKEN,
    MAX_SQLITE_INTEGER,
    MESSAGE_TYPES,
    LocalResult,
    LocalTx,
    MessageConflict,
    PayloadIntegrityError,
    ProtocolError,
    ReceiveResult,
    UNAEDTOSAdapter,
    payload_digest,
    validate_envelope,
    validate_fencing_token,
)
from .transport import MAX_PROVIDER_TIMEOUT_S, ConfigurationError, ProviderTimeouts, TransportConfig
from .reconcile import DEFAULT_REJECTED_STRATEGIES, ReconcileOutcome, ReconcileRejected, Reconciler
from .fencing import FencingGate, StaleFencingToken
from .dispatch import DispatchAuthorization, DispatchBlocked, DispatchResult, Dispatcher
from .bridge import (
    CorrelationMismatch,
    CorrelationUnverifiable,
    StoreBridge,
    from_dtos_token,
    to_dtos_token,
)

__all__ = [
    "MAX_CORRELATION_ID_BYTES", "MAX_ENVELOPE_BYTES", "MAX_EVIDENCE_REFS", "MAX_EVIDENCE_REF_BYTES", "MAX_EVIDENCE_SIZE_BYTES", "MAX_TEXT_FIELD_BYTES", "MAX_FENCING_TOKEN", "MAX_SQLITE_INTEGER", "MESSAGE_TYPES", "LocalResult", "LocalTx", "MessageConflict", "PayloadIntegrityError", "ProtocolError",
    "ReceiveResult", "UNAEDTOSAdapter", "payload_digest", "validate_envelope", "validate_fencing_token",
    "MAX_PROVIDER_TIMEOUT_S", "ConfigurationError", "ProviderTimeouts", "TransportConfig", "DEFAULT_REJECTED_STRATEGIES", "ReconcileOutcome", "ReconcileRejected", "Reconciler", "DispatchAuthorization", "DispatchBlocked", "DispatchResult", "Dispatcher", "FencingGate", "StaleFencingToken", "CorrelationMismatch", "CorrelationUnverifiable", "StoreBridge", "from_dtos_token", "to_dtos_token",
]
