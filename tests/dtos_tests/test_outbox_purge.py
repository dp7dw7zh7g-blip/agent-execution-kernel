"""
test_outbox_purge.py (parche 0018, D-70)
========================================
Hallazgo de la auditoría de SQLite de g0.8 r24: `get_pending_outbox` hacía un recorrido completo de `outbox` (7,8 ms con 200 000 eventos ya
publicados, frente a 0,01 ms con índice) y el histórico publicado crecía sin tope porque nada lo purgaba.

Cubre: (1) el índice parcial `ix_outbox_pending` y que el plan de la consulta lo use, también en una base creada ANTES del parche;
(2) `SQLiteStore.purge_published_outbox`: qué borra y, sobre todo, qué NO borra; (3) la validación de argumentos.
Sin el parche 0018 estas pruebas FALLAN (no existe el índice ni `purge_published_outbox`).

Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import tempfile

from dtos.store import SQLiteStore

FAILURES: list[str] = []
OLD = "2020-01-01T00:00:00.000Z"       # muy anterior a cualquier corte razonable
RECENT = None                          # None = DEFAULT de la columna (ahora)


def check(condition: bool, description: str) -> None:
    print(f"[{'OK ' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def raises(exc, fn) -> bool:
    try:
        fn()
    except exc:
        return True
    except Exception:
        return False
    return False


def seed(s: SQLiteStore, published: int, recorded_at: str | None, n: int = 1) -> list[int]:
    ids = []
    with s.transaction() as conn:
        for _ in range(n):
            if recorded_at is None:
                cur = conn.execute("INSERT INTO outbox (event_type, payload, published) VALUES ('E', '{}', ?)", (published,))
            else:
                cur = conn.execute("INSERT INTO outbox (event_type, payload, published, recorded_at) VALUES ('E', '{}', ?, ?)", (published, recorded_at))
            ids.append(cur.lastrowid)
    return ids


def remaining(s: SQLiteStore) -> set[int]:
    return {r[0] for r in s._conn.execute("SELECT outbox_id FROM outbox")}


def plan(s: SQLiteStore) -> str:
    sql = ("SELECT outbox_id, event_type, payload FROM outbox WHERE published = 0 "
           "AND outbox_id NOT IN (SELECT outbox_id FROM outbox_failures WHERE quarantined = 1) ORDER BY outbox_id ASC LIMIT ?")
    return " | ".join(r[3] for r in s._conn.execute("EXPLAIN QUERY PLAN " + sql, (10,)))


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="outbox-purge-")
    try:
        path = os.path.join(tmp, "d.db")
        s = SQLiteStore(path)

        # --- (1) índice ---
        idx = s._conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='ix_outbox_pending'").fetchone()
        check(idx is not None and "WHERE published = 0" in idx[0], "existe el índice PARCIAL ix_outbox_pending (solo pendientes)")
        seed(s, 1, OLD, n=500)
        pend = seed(s, 0, RECENT, n=3)
        check("ix_outbox_pending" in plan(s), "el plan de get_pending_outbox usa ix_outbox_pending (no recorre toda la tabla)")
        check([r[0] for r in s.get_pending_outbox()] == pend, "get_pending_outbox sigue devolviendo solo los pendientes, en orden")
        s.close()

        # base creada antes del parche: sin el índice; al abrir se crea sola
        c = sqlite3.connect(path)
        c.execute("DROP INDEX ix_outbox_pending")
        c.close()
        s = SQLiteStore(path)
        check(s._conn.execute("SELECT 1 FROM sqlite_master WHERE name='ix_outbox_pending'").fetchone() is not None,
              "abrir una base sin el índice (anterior al parche) lo crea")
        check("ix_outbox_pending" in plan(s), "y el plan vuelve a usarlo")
        s.close()
        os.remove(path)
        for ext in ("-wal", "-shm"):
            if os.path.exists(path + ext):
                os.remove(path + ext)

        # --- (2) purga ---
        s = SQLiteStore(path)
        old_pub = seed(s, 1, OLD, n=5)
        recent_pub = seed(s, 1, RECENT, n=2)
        pending = seed(s, 0, OLD, n=2)                      # pendiente y viejo: NO se purga
        quarantined = seed(s, 0, OLD, n=1)                  # en cuarentena: NO se purga
        requeued = seed(s, 1, OLD, n=1)                     # publicado, viejo, con historial de reencolado: NO se purga
        with s.transaction() as conn:
            conn.execute("INSERT INTO outbox_failures (outbox_id, attempts, last_error, quarantined) VALUES (?, 5, 'x', 1)", (quarantined[0],))
            conn.execute("INSERT INTO outbox_failures (outbox_id, attempts, last_error, quarantined) VALUES (?, 1, 'transitorio', 0)", (old_pub[0],))
            conn.execute("INSERT INTO outbox_requeues (outbox_id, requeued_by, reason, previous_attempts, previous_error) VALUES (?, 'ops', 'r', 5, 'e')",
                         (requeued[0],))

        n = s.purge_published_outbox(older_than_days=30, limit=3)
        check(n == 3, "con limit=3 borra exactamente 3 (la llamada está acotada)")
        check(remaining(s) & set(old_pub[:3]) == set(), "borra primero los más antiguos (por outbox_id)")
        n = s.purge_published_outbox(older_than_days=30)
        check(n == 2, "la siguiente llamada borra los 2 publicados viejos que quedaban")
        check(s.purge_published_outbox(older_than_days=30) == 0, "y después devuelve 0 (se llama en bucle hasta 0)")
        left = remaining(s)
        check(set(recent_pub) <= left, "NO borra un evento publicado reciente (dentro del plazo)")
        check(set(pending) <= left, "NO borra eventos pendientes, aunque sean viejos")
        check(quarantined[0] in left, "NO borra un evento en cuarentena")
        check(requeued[0] in left, "NO borra un evento con historial en outbox_requeues (la bitácora append-only sigue apuntando a algo real)")
        check(s._conn.execute("SELECT COUNT(*) FROM outbox_failures WHERE outbox_id = ?", (old_pub[0],)).fetchone()[0] == 0,
              "borra también la fila de outbox_failures (fallo transitorio) del evento purgado")
        check(s._conn.execute("SELECT COUNT(*) FROM outbox_failures WHERE outbox_id = ?", (quarantined[0],)).fetchone()[0] == 1,
              "y conserva la de un evento en cuarentena")
        check(s._conn.execute("SELECT COUNT(*) FROM outbox_requeues").fetchone()[0] == 1, "la bitácora outbox_requeues queda intacta")
        new_id = seed(s, 0, RECENT)[0]
        check(new_id > max(old_pub + recent_pub + pending + quarantined + requeued), "los outbox_id purgados NO se reutilizan (AUTOINCREMENT)")
        s.close()
        s = SQLiteStore(os.path.join(tmp, "frac.db"))
        one = seed(s, 1, OLD)
        check(s.purge_published_outbox(older_than_days=0.000001) == 1 and one[0] not in remaining(s),
              "un plazo fraccionario muy pequeño (1e-6 días) es válido: sin notación exponencial en el modificador de SQLite")

        # --- (3) argumentos ---
        for bad in (0, -1, True, "30", float("nan"), float("inf"), 36500, None):
            check(raises(ValueError, lambda b=bad: s.purge_published_outbox(older_than_days=b)), f"older_than_days={bad!r} -> ValueError")
        for bad in (0, -5, True, 1.5, "10", 100_001, None):
            check(raises(ValueError, lambda b=bad: s.purge_published_outbox(older_than_days=30, limit=b)), f"limit={bad!r} -> ValueError")
        check(raises(TypeError, lambda: s.purge_published_outbox(30)), "older_than_days es solo por nombre (no admite posicional)")
        s.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{'TODO OK' if not FAILURES else str(len(FAILURES)) + ' FALLOS'}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
