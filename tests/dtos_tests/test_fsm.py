"""
test_fsm.py
===========
Verifica:
1. El happy path de cada FSM funciona.
2. Una transición fuera de tabla se rechaza (InvalidTransitionError).
3. Un atajo prohibido por invariante (ej. UNKNOWN -> FAILED directo) se
   rechaza aunque exista en la tabla, porque está `guarded`
   (GuardedTransitionError) — y solo el método explícito lo permite.

Ejecutar: python3 -m dtos.test_fsm
"""

from __future__ import annotations

import sys

from dtos.cognitive_cycle_fsm import CognitiveCycleFSM
from dtos.effect_fsm import EffectFSM
from dtos.enums import BootstrapState, CognitiveCycleState, EffectStatus, RecoveryTriggerState
from dtos.fsm import GuardedTransitionError, InvalidTransitionError
from dtos.recovery_bootstrap_fsm import BootstrapFSM, RecoveryTriggerFSM

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
    # ===================================================================
    # CognitiveCycleFSM
    # ===================================================================
    cc = CognitiveCycleFSM()
    check(cc.state == CognitiveCycleState.CREATED, "CognitiveCycleFSM nace en CREATED")

    for target in [
        CognitiveCycleState.PREPARING,
        CognitiveCycleState.CONTEXT_ASSEMBLING,
        CognitiveCycleState.REPRESENTING,
        CognitiveCycleState.REASONING,
        CognitiveCycleState.VERIFYING,
        CognitiveCycleState.RESULT_READY,
        CognitiveCycleState.PLANNING_UPDATE,
        CognitiveCycleState.PROJECTING,
        CognitiveCycleState.CONTROL,
        CognitiveCycleState.READY_TO_COMMIT,
        CognitiveCycleState.COMMITTING,
        CognitiveCycleState.COMPLETED,
    ]:
        cc.transition(target, reason="happy path")
    check(cc.state == CognitiveCycleState.COMPLETED, "CognitiveCycleFSM recorre el happy path completo")
    check(len(cc.history) == 12, "CognitiveCycleFSM registra las 12 transiciones en history")

    expect_raises(
        InvalidTransitionError,
        lambda: cc.transition(CognitiveCycleState.REASONING),
        "CognitiveCycleFSM: COMPLETED es terminal, no admite salidas",
    )

    cc2 = CognitiveCycleFSM()
    expect_raises(
        InvalidTransitionError,
        lambda: cc2.transition(CognitiveCycleState.COMPLETED),
        "CognitiveCycleFSM: CREATED -> COMPLETED directo es inválido (salta el ciclo)",
    )

    # -- P-013 como guard real, no como comentario --------------------------
    cc3 = CognitiveCycleFSM()
    cc3.transition(CognitiveCycleState.PREPARING)
    cc3.transition(CognitiveCycleState.UNKNOWN, reason="timeout de reasoning provider")
    check(cc3.state == CognitiveCycleState.UNKNOWN, "CognitiveCycleFSM puede entrar a UNKNOWN desde estado activo")

    expect_raises(
        GuardedTransitionError,
        lambda: cc3.transition(CognitiveCycleState.FAILED),
        "P-013 en código: UNKNOWN -> FAILED vía transition() directo está BLOQUEADO",
    )

    expect_raises(
        ValueError,
        lambda: cc3.resolve_unknown_as_failed(evidence={}, reason="sin evidencia"),
        "resolve_unknown_as_failed rechaza evidence vacío",
    )

    cc3.resolve_unknown_as_failed(evidence={"provider_confirmed_error": True}, reason="provider query confirmó error")
    check(cc3.state == CognitiveCycleState.FAILED, "resolve_unknown_as_failed con evidencia sí resuelve UNKNOWN -> FAILED")
    check(cc3.history[-1].evidence == {"provider_confirmed_error": True}, "La evidencia queda en el history, no se pierde")

    # ===================================================================
    # EffectFSM
    # ===================================================================
    ef = EffectFSM()
    check(ef.state == EffectStatus.INTENDED, "EffectFSM nace en INTENDED")
    ef.transition(EffectStatus.DISPATCHED)
    ef.transition(EffectStatus.CONFIRMED)
    check(ef.state == EffectStatus.CONFIRMED, "EffectFSM: INTENDED -> DISPATCHED -> CONFIRMED (happy path)")

    ef2 = EffectFSM()
    ef2.transition(EffectStatus.DISPATCHED)

    expect_raises(
        InvalidTransitionError,
        lambda: ef2.transition(EffectStatus.RECONCILIATION_REQUIRED),
        "EffectFSM: DISPATCHED -> RECONCILIATION_REQUIRED directo es inválido (debe pasar por UNKNOWN)",
    )

    ef2.mark_unknown(reason="sin respuesta del provider tras timeout")
    check(
        ef2.state == EffectStatus.RECONCILIATION_REQUIRED,
        "EffectFSM.mark_unknown() lleva automáticamente a RECONCILIATION_REQUIRED (I-005)",
    )

    # -- I-005 como guard real ------------------------------------------
    expect_raises(
        GuardedTransitionError,
        lambda: ef2.transition(EffectStatus.CONFIRMED),
        "I-005 en código: RECONCILIATION_REQUIRED -> CONFIRMED vía transition() directo está BLOQUEADO",
    )

    expect_raises(
        ValueError,
        lambda: ef2.reconcile(strategy="MAGIC_GUESS", result="CONFIRMED", evidence={"x": 1}),
        "reconcile() rechaza estrategias no reconocidas",
    )

    expect_raises(
        ValueError,
        lambda: ef2.reconcile(strategy="PROVIDER_QUERY", result="REJECTED", evidence={}),
        "reconcile() rechaza evidence vacío (NOT_FOUND no puede colapsar a REJECTED sin evidencia)",
    )

    ef2.reconcile(strategy="PROVIDER_QUERY", result="CONFIRMED", evidence={"provider_receipt": "rcpt-123"})
    check(ef2.state == EffectStatus.CONFIRMED, "reconcile() con estrategia + evidencia sí resuelve el efecto")

    # NOT_FOUND -> STILL_UNKNOWN explícito (no colapsa a REJECTED por default)
    ef3 = EffectFSM()
    ef3.transition(EffectStatus.DISPATCHED)
    ef3.mark_unknown(reason="conexión perdida")
    ef3.reconcile(strategy="EXTERNAL_REFERENCE_LOOKUP", result="UNKNOWN", evidence={"lookup": "NOT_FOUND"})
    check(
        ef3.state == EffectStatus.UNKNOWN,
        "reconcile() con NOT_FOUND puede resolver explícitamente a STILL_UNKNOWN, no a REJECTED automático",
    )

    # ===================================================================
    # RecoveryTriggerFSM
    # ===================================================================
    rt = RecoveryTriggerFSM()
    check(rt.state == RecoveryTriggerState.NONE, "RecoveryTriggerFSM nace en NONE")
    rt.suspect(reason="3 heartbeats perdidos")
    check(rt.state == RecoveryTriggerState.SUSPECT, "RecoveryTriggerFSM: NONE -> SUSPECT")

    expect_raises(
        GuardedTransitionError,
        lambda: rt.transition(RecoveryTriggerState.DECLARED),
        "P-061 en código: SUSPECT -> DECLARED vía transition() directo está BLOQUEADO",
    )

    expect_raises(
        ValueError,
        lambda: rt.declare_failure(declared_by="", evidence={"gap_s": 30}),
        "declare_failure rechaza declared_by vacío (P-061: requiere autoridad)",
    )

    rt.declare_failure(declared_by="authority-leader-1", evidence={"gap_s": 30})
    check(rt.state == RecoveryTriggerState.DECLARED, "declare_failure con declared_by sí resuelve SUSPECT -> DECLARED")
    rt.start_recovery(reason="disparando RECOVERY.STARTING")
    check(rt.state == RecoveryTriggerState.RECOVERY_STARTING, "RecoveryTriggerFSM llega a RECOVERY_STARTING")

    rt2 = RecoveryTriggerFSM()
    rt2.suspect(reason="heartbeat lento")
    rt2.clear_suspicion(reason="heartbeat volvió")
    check(rt2.state == RecoveryTriggerState.NONE, "RecoveryTriggerFSM: SUSPECT -> NONE cuando la sospecha se retira")

    # ===================================================================
    # BootstrapFSM
    # ===================================================================
    bs = BootstrapFSM()
    check(bs.state == BootstrapState.COLD_START, "BootstrapFSM nace en COLD_START")
    bs.transition(BootstrapState.CANDIDATE)
    bs.transition(BootstrapState.QUORUM_CHECK)
    bs.write_genesis(installation_id="install-1", existing_record_found=False)
    check(bs.state == BootstrapState.GENESIS_WRITTEN, "BootstrapFSM: write_genesis sin registro previo -> GENESIS_WRITTEN")
    bs.activate()
    check(bs.state == BootstrapState.ACTIVE, "BootstrapFSM: GENESIS_WRITTEN -> ACTIVE")

    expect_raises(
        InvalidTransitionError,
        lambda: bs.transition(BootstrapState.CANDIDATE),
        "BootstrapFSM: ACTIVE es terminal, no admite volver a CANDIDATE",
    )

    # -- P-063: conflicto nunca se sobrescribe --------------------------
    bs2 = BootstrapFSM()
    bs2.transition(BootstrapState.CANDIDATE)
    bs2.transition(BootstrapState.QUORUM_CHECK)
    bs2.write_genesis(installation_id="install-1", existing_record_found=True)
    check(bs2.state == BootstrapState.BOOTSTRAP_CONFLICT, "BootstrapFSM: genesis ya existente -> BOOTSTRAP_CONFLICT")

    expect_raises(
        InvalidTransitionError,
        lambda: bs2.transition(BootstrapState.GENESIS_WRITTEN),
        "P-063 en código: BOOTSTRAP_CONFLICT no tiene salida hacia GENESIS_WRITTEN (no hay sobrescritura posible)",
    )
    expect_raises(
        InvalidTransitionError,
        lambda: bs2.transition(BootstrapState.ACTIVE),
        "P-063 en código: BOOTSTRAP_CONFLICT no tiene salida hacia ACTIVE tampoco",
    )

    # -- Resumen ----------------------------------------------------------
    print()
    if FAILURES:
        print(f"{len(FAILURES)} test(s) fallaron:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("Todos los FSM tests pasaron.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
