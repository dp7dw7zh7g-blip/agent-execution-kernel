"""
test_store.py
==============
A diferencia de test_contracts.py y test_fsm.py, esto corre contra un
SQLite real (archivo temporal), porque lo que se está verificando es
justamente que las garantías viven en la base de datos, no en un mock
de Python que podría mentir.

Ejecutar: python3 -m dtos.test_store
"""

from __future__ import annotations

import os
import sys
import tempfile

from dtos.registries import ArtifactRecord
from dtos.store import (
    AppendOnlyViolationError,
    BootstrapConflictError,
    ConflictError,
    SQLiteStore,
)

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

        # ===============================================================
        # T-CONC-03 real: dos ReadyToCommit con mismo expected_version
        # ===============================================================
        store._conn.execute(
            "INSERT INTO tasks (task_id, goal_id, status) VALUES ('t-1', 'g-1', 'ACTIVE')"
        )
        store._conn.execute("COMMIT;") if False else None  # tasks no usa transaction() helper aquí, autocommit
        store.create_cognitive_cycle(
            cycle_id="c-1", task_id="t-1", state="REASONING", state_hash="a" * 64, context_hash="b" * 64
        )

        r1 = store.commit_cognitive_cycle_transition(
            cycle_id="c-1",
            new_state="VERIFYING",
            expected_version=1,
            new_state_hash="c" * 64,
            new_context_hash="d" * 64,
        )
        check(r1.rows_affected == 1, "Primer commit con expected_version correcto: rows_affected == 1")
        check(r1.new_version == 2, "Primer commit avanza cycle_version a 2")

        # Un segundo escritor que todavía cree que la versión es 1
        # (no se enteró del primer commit) debe perder la carrera:
        expect_raises(
            ConflictError,
            lambda: store.commit_cognitive_cycle_transition(
                cycle_id="c-1",
                new_state="VERIFYING",
                expected_version=1,  # ya está en 2 en la base real
                new_state_hash="e" * 64,
                new_context_hash="f" * 64,
            ),
            "T-CONC-03 real: segundo commit con expected_version obsoleto es rechazado por SQLite (rows_affected=0)",
        )

        current = store.get_cognitive_cycle("c-1")
        check(
            current["state"] == "VERIFYING" and current["cycle_version"] == 2,
            "El estado en la base refleja solo el commit ganador, no el perdedor",
        )

        # ===============================================================
        # P-062/P-063 real: unicidad de GENESIS_AUTHORITY_RECORD
        # ===============================================================
        store.write_genesis_authority(
            authority_id="auth-genesis-1", installation_id="install-abc", principal="system"
        )
        check(True, "Primer GENESIS_AUTHORITY_RECORD para install-abc se escribe sin error")

        expect_raises(
            BootstrapConflictError,
            lambda: store.write_genesis_authority(
                authority_id="auth-genesis-2", installation_id="install-abc", principal="system"
            ),
            "P-063 real: segundo genesis para la misma installation_id es rechazado por el UNIQUE INDEX de SQLite",
        )

        # Otra instalación sí puede tener su propio genesis (no es un
        # límite global, es por instalación):
        store.write_genesis_authority(
            authority_id="auth-genesis-3", installation_id="install-xyz", principal="system"
        )
        check(True, "Genesis para una installation_id DISTINTA sí se permite")

        # ===============================================================
        # P-039/P-065 real: artifacts identidad + append-only
        # ===============================================================
        art = ArtifactRecord(
            artifact_id="art-1",
            version=1,
            artifact_class="ReasoningResult",
            content_hash="ab" * 32,
            provenance="reasoning-gateway",
            status="ACTIVE",
        )
        store.insert_artifact(art)
        check(True, "insert_artifact con forma válida se escribe")

        expect_raises(
            AppendOnlyViolationError,
            lambda: store.update_artifact_status_forbidden("art-1", "ARCHIVED"),
            "P-065 real: UPDATE sobre artifacts es rechazado por el trigger de SQLite, no solo por convención",
        )

        # Una "corrección" real se modela como fila nueva con distinto
        # content_hash, nunca pisando la anterior:
        art_corrected = ArtifactRecord(
            artifact_id="art-1",
            version=2,
            artifact_class="ReasoningResult",
            content_hash="cd" * 32,
            provenance="reasoning-gateway",
            status="ACTIVE",
        )
        store.insert_artifact(art_corrected)
        rows = store._conn.execute(
            "SELECT version, content_hash FROM artifacts WHERE artifact_id = 'art-1' ORDER BY version"
        ).fetchall()
        check(
            len(rows) == 2 and rows[0][0] == 1 and rows[1][0] == 2,
            "Una corrección crea una fila nueva (version=2); la version=1 original sigue intacta",
        )

        # ===============================================================
        # Idempotencia real de Effects vía UNIQUE(provider, idempotency_key)
        # ===============================================================
        store.insert_effect(
            effect_id="eff-1",
            operation_id="op-1",
            intent_id="int-1",
            attempt_id="att-1",
            provider="stripe",
            idempotency_key="idem-key-1",
            status="DISPATCHED",
        )
        check(True, "Primer effect con idempotency_key nueva se inserta")

        expect_raises(
            ConflictError,
            lambda: store.insert_effect(
                effect_id="eff-2",  # distinto effect_id...
                operation_id="op-1",
                intent_id="int-1",
                attempt_id="att-2",
                provider="stripe",
                idempotency_key="idem-key-1",  # ...misma key + mismo provider
                status="DISPATCHED",
            ),
            "Regla de idempotencia real: misma (provider, idempotency_key) es rechazada por SQLite",
        )

        # ===============================================================
        # State + Outbox atómicos en la MISMA transacción (P-016)
        # ===============================================================
        store.create_cognitive_cycle(
            cycle_id="c-2", task_id="t-1", state="REASONING", state_hash="1" * 64, context_hash="2" * 64
        )
        store.commit_cognitive_cycle_transition(
            cycle_id="c-2",
            new_state="VERIFYING",
            expected_version=1,
            new_state_hash="3" * 64,
            new_context_hash="4" * 64,
            domain_event_payload={"cycle_id": "c-2", "dependency_epoch": 5},
        )
        outbox_rows = store._conn.execute(
            "SELECT event_type, dependency_epoch FROM outbox WHERE event_type = 'CognitiveCycleStateChanged'"
        ).fetchall()
        check(
            len(outbox_rows) == 1 and outbox_rows[0][1] == 5,
            "Commit con domain_event_payload escribe también en outbox, en la misma transacción",
        )

        store.close()

        # -- Resumen ------------------------------------------------------
        print()
        if FAILURES:
            print(f"{len(FAILURES)} test(s) fallaron:")
            for f in FAILURES:
                print(f"  - {f}")
            return 1
        print("Todos los store tests pasaron (contra SQLite real).")
        return 0

    finally:
        os.unlink(db_path)


if __name__ == "__main__":
    sys.exit(main())
