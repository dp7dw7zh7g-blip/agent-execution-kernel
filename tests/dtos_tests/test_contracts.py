"""
test_contracts.py
==================
Contract tests de FASE 1. Distintos de los tests de sistema T-CONC/T-CRASH/
T-UNK/T-EDGE/T-PROP del Registro Maestro (esos prueban comportamiento end-
to-end; estos prueban que los DTOs en sí son correctos y deterministas).

Ejecutar: python3 -m pytest test_contracts.py -v
(o, sin pytest instalado, python3 test_contracts.py)
"""

from __future__ import annotations

import sys

from dtos.canonical import canonical_json, content_hash
from dtos.contracts import (
    ContractInconsistencyError,
    EffectAttempt,
    EffectIntent,
    GenesisAuthorityRecord,
    ReasoningRequest,
    ReasoningResult,
    RecoveryTrigger,
)
from dtos.enums import BootstrapState, EffectStatus, ReasoningResultStatus, RecoveryTriggerState
from dtos.registries import ArtifactRecord, CognitiveCycleRecord, OperationRecord
from dtos.validation import ContractValidationError

FAILURES: list[str] = []


def check(condition: bool, description: str) -> None:
    status = "OK " if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        FAILURES.append(description)


def expect_raises(exc_type, fn, description: str) -> None:
    try:
        fn()
        check(False, description + " (no lanzó excepción)")
    except exc_type:
        check(True, description)
    except Exception as e:  # noqa: BLE001
        check(False, f"{description} (lanzó {type(e).__name__} en vez de {exc_type.__name__})")


def main() -> int:
    # -- 1. Forma válida construye sin error -----------------------------
    req = ReasoningRequest(
        request_id="req-1",
        task_id="task-1",
        cycle_id="cycle-1",
        problem_hash="ab" * 32,
        representation_version="v1",
        input_payload={"goal": "test"},
        max_iterations=50,
        max_duration_ms=30000,
        allow_partial=False,
        allow_early_stop=True,
    )
    check(req.request_id == "req-1", "ReasoningRequest válido se construye")

    # -- 2. Forma inválida falla rápido -----------------------------------
    expect_raises(
        ContractValidationError,
        lambda: ReasoningRequest(
            request_id="",  # inválido
            task_id="t",
            cycle_id="c",
            problem_hash="ab" * 32,
            representation_version="v1",
            input_payload={},
            max_iterations=50,
            max_duration_ms=30000,
            allow_partial=False,
            allow_early_stop=True,
        ),
        "ReasoningRequest con request_id vacío falla en construcción",
    )

    expect_raises(
        ContractValidationError,
        lambda: ReasoningRequest(
            request_id="r",
            task_id="t",
            cycle_id="c",
            problem_hash="no-es-hex",  # inválido
            representation_version="v1",
            input_payload={},
            max_iterations=50,
            max_duration_ms=30000,
            allow_partial=False,
            allow_early_stop=True,
        ),
        "ReasoningRequest con problem_hash no-hexadecimal falla",
    )

    # -- 3. Hashing determinista (P-064) ----------------------------------
    a = req.to_canonical_dict()
    b = req.to_canonical_dict()
    check(canonical_json(a) == canonical_json(b), "canonical_json es determinista para el mismo DTO")
    check(content_hash(a) == content_hash(b), "content_hash es determinista para el mismo DTO")

    # orden de claves de entrada no debe afectar el hash
    unordered = {"z": 1, "a": 2, "m": {"y": 1, "b": 2}}
    reordered = {"a": 2, "m": {"b": 2, "y": 1}, "z": 1}
    check(
        content_hash(unordered) == content_hash(reordered),
        "content_hash es insensible al orden de claves de entrada (JCS-like)",
    )

    # -- 4. DTO distinto -> hash distinto ---------------------------------
    req2 = ReasoningRequest(
        request_id="req-2",  # único campo distinto
        task_id="task-1",
        cycle_id="cycle-1",
        problem_hash="ab" * 32,
        representation_version="v1",
        input_payload={"goal": "test"},
        max_iterations=50,
        max_duration_ms=30000,
        allow_partial=False,
        allow_early_stop=True,
    )
    check(
        content_hash(req.to_canonical_dict()) != content_hash(req2.to_canonical_dict()),
        "DTOs con contenido distinto producen hashes distintos",
    )

    # -- 5. ReasoningResult con status válido ------------------------------
    result = ReasoningResult(
        request_id="req-1",
        provider_id="provider-x",
        provider_version="0.1.0",
        status=ReasoningResultStatus.COMPLETED,
        output_payload={"answer": 42},
        iterations_used=5,
        duration_ms=1200,
        input_hash="ab" * 32,
        output_hash="cd" * 32,
        termination_reason="converged",
    )
    check(result.status == ReasoningResultStatus.COMPLETED, "ReasoningResult acepta status del Enum")

    expect_raises(
        TypeError,
        lambda: ReasoningResult(
            request_id="req-1",
            provider_id="provider-x",
            provider_version="0.1.0",
            status="COMPLETED",  # string plano, no Enum -> debe rechazarse
            output_payload={},
            iterations_used=5,
            duration_ms=1200,
            input_hash="ab" * 32,
            output_hash="cd" * 32,
            termination_reason="converged",
        ),
        "ReasoningResult rechaza status como string plano (exige Enum)",
    )

    # -- 6. EffectAttempt UNKNOWN debe forzar reconciliation (I-005/T-UNK-01)
    attempt_ok = EffectAttempt(
        attempt_id="a-1",
        intent_id="i-1",
        status=EffectStatus.UNKNOWN,
        reconciliation_required=True,
    )
    check(attempt_ok.status == EffectStatus.UNKNOWN, "EffectAttempt UNKNOWN + reconciliation_required=True es válido")

    expect_raises(
        ContractInconsistencyError,
        lambda: EffectAttempt(
            attempt_id="a-2",
            intent_id="i-1",
            status=EffectStatus.UNKNOWN,
            reconciliation_required=False,  # viola I-005
        ),
        "EffectAttempt UNKNOWN sin reconciliation_required viola I-005 y falla",
    )

    # -- 7. RecoveryTrigger básico (corrección 175) ------------------------
    trigger = RecoveryTrigger(
        trigger_id="rt-1",
        suspected_node_id="node-7",
        incarnation_at_suspicion=3,
        evidence={"missed_heartbeats": 4},
        declared_by="authority-leader-1",
    )
    check(trigger.state == RecoveryTriggerState.SUSPECT, "RecoveryTrigger nace en estado SUSPECT por default")

    # -- 8. GenesisAuthorityRecord solo puede nacer en epoch=1 (P-062) -----
    genesis = GenesisAuthorityRecord(
        installation_id="install-abc",
        epoch=1,
        fencing_token=1,
    )
    check(genesis.state == BootstrapState.GENESIS_WRITTEN, "GenesisAuthorityRecord válido con epoch=1")

    expect_raises(
        ContractInconsistencyError,
        lambda: GenesisAuthorityRecord(
            installation_id="install-abc",
            epoch=2,  # viola P-062
            fencing_token=1,
        ),
        "GenesisAuthorityRecord con epoch != 1 viola P-062 y falla",
    )

    # -- 9. Inmutabilidad (frozen=True) ------------------------------------
    def try_mutate():
        req.request_id = "otro"  # type: ignore[misc]

    expect_raises(
        Exception,  # FrozenInstanceError hereda de AttributeError/TypeError según versión
        try_mutate,
        "DTOs son inmutables (frozen=True) - reasignar un campo falla",
    )

    # -- 10. Registros (REG-xxx) validan Enums correctos --------------------
    from dtos.enums import CognitiveCycleState, OperationClosureState

    cycle = CognitiveCycleRecord(
        cycle_id="c-1",
        task_id="t-1",
        state=CognitiveCycleState.REASONING,
        cycle_version=1,
        state_hash="ab" * 32,
        context_hash="cd" * 32,
    )
    check(cycle.state == CognitiveCycleState.REASONING, "CognitiveCycleRecord acepta CognitiveCycleState")

    op = OperationRecord(
        operation_id="op-1",
        semantic_fingerprint="fp-1",
        status=OperationClosureState.RUNNING,
        goal_id="g-1",
        task_id="t-1",
    )
    check(op.status == OperationClosureState.RUNNING, "OperationRecord acepta OperationClosureState")

    artifact = ArtifactRecord(
        artifact_id="art-1",
        version=1,
        artifact_class="ReasoningResult",
        content_hash="ef" * 32,
        provenance="reasoning-gateway",
        status="ACTIVE",
    )
    check(artifact.version == 1, "ArtifactRecord válido (identidad artifact_id+version+content_hash, P-039)")

    # -- Resumen ------------------------------------------------------------
    print()
    if FAILURES:
        print(f"{len(FAILURES)} test(s) fallaron:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("Todos los contract tests pasaron.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
