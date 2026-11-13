"""
test_repository.py
===================
Verifica específicamente lo que test_fsm.py y test_store.py no podían
verificar por separado: que una transición ilegal para la FSM NUNCA
llega a tocar SQLite (no solo que la FSM la rechaza en memoria), y que
el Commit Gate real sigue protegiendo contra condiciones de carrera
incluso cuando se pasa por el repository.

Ejecutar: python3 -m dtos.test_repository
"""

from __future__ import annotations

import os
import sys
import tempfile

from dtos.enums import CognitiveCycleState as S
from dtos.fsm import GuardedTransitionError, InvalidTransitionError
from dtos.repository import CognitiveCycleRepository
from dtos.store import ConflictError, SQLiteStore

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
        store._conn.execute("INSERT INTO tasks (task_id, goal_id, status) VALUES ('t-1', 'g-1', 'ACTIVE')")
        store.create_cognitive_cycle(
            cycle_id="c-1", task_id="t-1", state="CREATED", state_hash="0" * 64, context_hash="0" * 64
        )
        repo = CognitiveCycleRepository(store)

        # ===============================================================
        # 1. Transición ilegal para la FSM NUNCA toca la base
        # ===============================================================
        expect_raises(
            InvalidTransitionError,
            lambda: repo.transition("c-1", S.COMPLETED, reason="salto ilegal"),
            "repository.transition: CREATED -> COMPLETED directo es rechazado por la FSM",
        )
        row = store.get_cognitive_cycle("c-1")
        check(
            row["state"] == "CREATED" and row["cycle_version"] == 1,
            "La base NO cambió tras el intento rechazado: sigue en CREATED, version=1 "
            "(la FSM bloqueó ANTES de llegar a SQL, no después)",
        )

        # ===============================================================
        # 2. Camino feliz real, persistido
        # ===============================================================
        loaded = repo.transition("c-1", S.PREPARING, reason="avance normal")
        check(loaded.version == 2, "Transición legal avanza version a 2")
        row = store.get_cognitive_cycle("c-1")
        check(row["state"] == "PREPARING", "El estado persistido refleja la transición legal")

        repo.transition("c-1", S.CONTEXT_ASSEMBLING)
        repo.transition("c-1", S.REPRESENTING)
        repo.transition("c-1", S.REASONING)
        row = store.get_cognitive_cycle("c-1")
        check(row["state"] == "REASONING" and row["cycle_version"] == 5, "Cadena de transiciones legales se persiste en orden")

        # ===============================================================
        # 3. UNKNOWN -> FAILED guarded: bloqueado en memoria, base intacta
        # ===============================================================
        repo.transition("c-1", S.UNKNOWN, reason="timeout de provider")
        row_before = store.get_cognitive_cycle("c-1")

        expect_raises(
            GuardedTransitionError,
            lambda: repo.transition("c-1", S.FAILED, reason="atajo prohibido"),
            "P-013 en el repository: UNKNOWN -> FAILED vía transition() normal sigue BLOQUEADO",
        )
        row_after = store.get_cognitive_cycle("c-1")
        check(
            row_before == row_after,
            "El intento de atajo prohibido no modificó absolutamente nada en SQLite",
        )

        expect_raises(
            ValueError,
            lambda: repo.resolve_unknown_as_failed("c-1", evidence={}, reason="sin evidencia"),
            "resolve_unknown_as_failed sin evidencia falla ANTES de tocar la base (la FSM lo rechaza primero)",
        )
        row_after_empty = store.get_cognitive_cycle("c-1")
        check(row_after_empty == row_before, "El intento sin evidencia tampoco tocó la base")

        final = repo.resolve_unknown_as_failed(
            "c-1", evidence={"provider_confirmed_error": True}, reason="provider query confirmó fallo"
        )
        row_final = store.get_cognitive_cycle("c-1")
        check(
            row_final["state"] == "FAILED" and final.version == row_final["cycle_version"],
            "Con evidencia válida, la resolución SÍ se persiste como FAILED",
        )

        # ===============================================================
        # 4. Commit Gate real sigue protegiendo contra carreras vía repo
        # ===============================================================
        store.create_cognitive_cycle(
            cycle_id="c-2", task_id="t-1", state="CREATED", state_hash="1" * 64, context_hash="1" * 64
        )
        repo_a = CognitiveCycleRepository(store)
        repo_b = CognitiveCycleRepository(store)

        # Ambos "leen" el mismo estado inicial (simulando dos workers)
        loaded_a = repo_a.load("c-2")
        loaded_b = repo_b.load("c-2")
        check(loaded_a.version == loaded_b.version == 1, "Dos repositories cargan la misma version inicial")

        # A gana la carrera
        repo_a.transition("c-2", S.PREPARING, reason="worker A")
        row = store.get_cognitive_cycle("c-2")
        check(row["cycle_version"] == 2, "Worker A comete primero y avanza la version")

        # B todavía cree que está en version=1 (su `loaded_b` es stale) e
        # intenta transicionar sobre ESE snapshot -> debe perder, porque
        # apply_transition() usa loaded_b.version, no la versión actual.
        expect_raises(
            ConflictError,
            lambda: repo_b.apply_transition("c-2", loaded_b, S.PREPARING, reason="worker B, versión obsoleta"),
            "Commit Gate real: worker B con snapshot obsoleto pierde la carrera vía apply_transition "
            "(la FSM en memoria de B no sabe que ya es stale, pero SQLite sí lo detecta)",
        )

        # Contraste: si B usara el atajo transition() (que recarga por
        # dentro) en vez de su snapshot viejo, SÍ ganaría, porque
        # recargaría la version=2 real antes de intentar comitear.
        loaded_b2 = repo_b.transition("c-2", S.CONTEXT_ASSEMBLING, reason="worker B, usando atajo con recarga")
        check(
            loaded_b2.version == 3,
            "transition() (atajo con recarga) no puede quedar stale: siempre parte de la version real",
        )

        store.close()

        # -- Resumen ------------------------------------------------------
        print()
        if FAILURES:
            print(f"{len(FAILURES)} test(s) fallaron:")
            for f in FAILURES:
                print(f"  - {f}")
            return 1
        print("Todos los repository tests pasaron (FSM + Commit Gate integrados).")
        return 0

    finally:
        os.unlink(db_path)


if __name__ == "__main__":
    sys.exit(main())
