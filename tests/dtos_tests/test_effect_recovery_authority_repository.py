"""
test_effect_recovery_authority_repository.py
================================================
Ejecutar: python3 -m dtos.test_effect_recovery_authority_repository
"""

from __future__ import annotations

import os
import sys
import tempfile

from dtos.effect_repository import AttemptNotFoundError, EffectRepository
from dtos.enums import EffectStatus as ES
from dtos.enums import RecoveryTriggerState as RS
from dtos.fsm import GuardedTransitionError, InvalidTransitionError
from dtos.recovery_authority_repository import AuthorityBootstrapRepository, RecoveryTriggerRepository
from dtos.store import BootstrapConflictError, ConflictError, SQLiteStore

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
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = tmp.name

    try:
        store = SQLiteStore(db_path)

        # ===================================================================
        # EffectRepository
        # ===================================================================
        eff_repo = EffectRepository(store)
        eff_repo.create(attempt_id="att-1", intent_id="int-1", provider="stripe", idempotency_key="k-1")

        row = store.get_effect_attempt("att-1")
        check(row["status"] == "INTENDED", "EffectRepository.create persiste el estado inicial INTENDED")

        eff_repo.dispatch("att-1")
        row = store.get_effect_attempt("att-1")
        check(row["status"] == "DISPATCHED", "dispatch() persiste DISPATCHED")

        # Atajo prohibido: nadie puede llamar commit_effect_attempt_transition
        # directo saltando a CONFIRMED sin reconciliar — pero eso se prueba
        # a nivel FSM (ya cubierto en test_fsm.py); aquí lo que se prueba es
        # que EL REPOSITORY no ofrece ningún método que permita ese salto.
        check(
            not hasattr(eff_repo, "force_confirm"),
            "EffectRepository no expone ningún método para forzar CONFIRMED sin pasar por reconcile()",
        )

        eff_repo.mark_unknown("att-1", reason="timeout del provider")
        row = store.get_effect_attempt("att-1")
        check(
            row["status"] == "RECONCILIATION_REQUIRED",
            "mark_unknown() persiste directo en RECONCILIATION_REQUIRED (pasa por UNKNOWN en memoria, "
            "pero solo se comitea el destino final de esa cadena)",
        )

        expect_raises(
            ValueError,
            lambda: eff_repo.reconcile(
                "att-1", strategy="PROVIDER_QUERY", result_status="CONFIRMED", evidence={},
                operation_id="op-1", provider="stripe", idempotency_key="k-1",
            ),
            "reconcile() sin evidencia falla en la FSM ANTES de tocar la base",
        )
        row_unchanged = store.get_effect_attempt("att-1")
        check(row_unchanged["status"] == "RECONCILIATION_REQUIRED", "La base no cambió tras el intento sin evidencia")

        eff_repo.reconcile(
            "att-1", strategy="PROVIDER_QUERY", result_status="CONFIRMED", evidence={"receipt": "r-1"},
            operation_id="op-1", provider="stripe", idempotency_key="k-1",
        )
        row = store.get_effect_attempt("att-1")
        check(row["status"] == "CONFIRMED", "reconcile() con evidencia persiste CONFIRMED en effect_attempts")

        ledger_row = store._conn.execute(
            "SELECT status FROM effects WHERE attempt_id = 'att-1'"
        ).fetchone()
        check(
            ledger_row is not None and ledger_row[0] == "CONFIRMED",
            "Al llegar a estado terminal, además se escribió la fila permanente en el ledger `effects`",
        )

        expect_raises(
            AttemptNotFoundError,
            lambda: eff_repo.load("att-nope"),
            "load() de un attempt_id inexistente falla con AttemptNotFoundError",
        )

        # ===================================================================
        # RecoveryTriggerRepository
        # ===================================================================
        rt_repo = RecoveryTriggerRepository(store)
        rt_repo.create(trigger_id="rt-1", suspected_node_id="node-9", incarnation_at_suspicion=2)

        expect_raises(
            ValueError,
            lambda: rt_repo.declare_failure("rt-1", declared_by="auth-1", evidence={"gap_s": 30}),
            "declare_failure() directo desde NONE (sin pasar por SUSPECT) es rechazado con ValueError explícito",
        )
        row = store.get_recovery_trigger("rt-1")
        check(row["state"] == "NONE", "La base sigue en NONE tras el intento inválido")

        rt_repo.suspect("rt-1", reason="3 heartbeats perdidos")
        row = store.get_recovery_trigger("rt-1")
        check(row["state"] == "SUSPECT", "suspect() persiste SUSPECT")

        expect_raises(
            ValueError,
            lambda: rt_repo.declare_failure("rt-1", declared_by="", evidence={"gap_s": 30}),
            "declare_failure() sin declared_by falla en la FSM (P-061) antes de tocar la base",
        )
        row_unchanged2 = store.get_recovery_trigger("rt-1")
        check(row_unchanged2["state"] == "SUSPECT", "La base sigue en SUSPECT tras el intento sin declared_by")

        rt_repo.declare_failure("rt-1", declared_by="authority-leader-1", evidence={"gap_s": 30})
        row = store.get_recovery_trigger("rt-1")
        check(
            row["state"] == "DECLARED" and row["declared_by"] == "authority-leader-1",
            "declare_failure() con declared_by persiste DECLARED y guarda quién lo declaró",
        )

        rt_repo.start_recovery("rt-1", reason="disparando recovery")
        row = store.get_recovery_trigger("rt-1")
        check(row["state"] == "RECOVERY_STARTING", "start_recovery() persiste RECOVERY_STARTING")

        # Concurrencia real: dos triggers "viendo" el mismo nodo sospechoso
        # compitiendo por avanzar la misma fila.
        rt_repo.create(trigger_id="rt-2", suspected_node_id="node-5", incarnation_at_suspicion=1)
        loaded_x = rt_repo.load("rt-2")
        loaded_y = rt_repo.load("rt-2")
        rt_repo.suspect("rt-2", reason="worker X detecta la sospecha primero")
        # worker Y todavía tiene la FSM en NONE (su loaded_y.fsm no fue
        # mutado por lo que hizo X) e intenta declarar directo -> además
        # de ser ilegal para su FSM, tampoco debe poder colarse en la base.
        expect_raises(
            (InvalidTransitionError, ConflictError),
            lambda: loaded_y.fsm.transition(RS.SUSPECT) or store.commit_recovery_trigger_transition(
                "rt-2", RS.SUSPECT.value, loaded_y.version
            ),
            "worker Y con snapshot obsoleto no logra un segundo SUSPECT válido sobre la misma fila",
        )

        # ===================================================================
        # AuthorityBootstrapRepository
        # ===================================================================
        boot_repo = AuthorityBootstrapRepository(store)
        fsm1 = boot_repo.bootstrap(installation_id="install-42")
        check(fsm1.state.value == "ACTIVE", "Primer bootstrap para install-42 llega a ACTIVE")

        genesis_row = store._conn.execute(
            "SELECT epoch, fencing, is_genesis FROM authorities WHERE installation_id = 'install-42'"
        ).fetchone()
        check(
            genesis_row == (1, 1, 1),
            "El GENESIS_AUTHORITY_RECORD real quedó en la tabla authorities con epoch=1, fencing=1",
        )

        fsm2 = boot_repo.bootstrap(installation_id="install-42")
        check(
            fsm2.state.value == "BOOTSTRAP_CONFLICT",
            "P-063 de punta a punta vía repository: un segundo bootstrap para la MISMA instalación "
            "no sobrescribe, la FSM refleja BOOTSTRAP_CONFLICT porque SQLite lo rechazó",
        )

        count = store._conn.execute(
            "SELECT COUNT(*) FROM authorities WHERE installation_id = 'install-42' AND is_genesis = 1"
        ).fetchone()[0]
        check(count == 1, "Sigue habiendo exactamente UNA fila genesis para install-42 tras el intento de conflicto")

        fsm3 = boot_repo.bootstrap(installation_id="install-99")
        check(fsm3.state.value == "ACTIVE", "Bootstrap para una instalación DISTINTA sí se permite y llega a ACTIVE")

        store.close()

        # -- Resumen ------------------------------------------------------
        print()
        if FAILURES:
            print(f"{len(FAILURES)} test(s) fallaron:")
            for f in FAILURES:
                print(f"  - {f}")
            return 1
        print("Todos los tests de Effect/Recovery/Authority repository pasaron.")
        return 0

    finally:
        os.unlink(db_path)


if __name__ == "__main__":
    sys.exit(main())
