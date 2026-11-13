"""
test_outbox_requeue.py (parche 0015, D-66)
==========================================
Hallazgo I-1 de la revisión adversarial de g0.8 r15: una caída larga del destino mandaba TODA la cola a cuarentena sin vuelta atrás
(solo se podía editar `outbox_failures` a mano). Ahora `SQLiteStore.requeue_quarantined` la revierte, con constancia en `outbox_requeues`.

Escenario: el sender falla `max_attempts` corridas seguidas (caída sostenida) -> cuarentena; el destino se recupera; el operador reencola;
la siguiente corrida publica. Sin el parche 0015 estas pruebas FALLAN (no existe `requeue_quarantined`).

Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile

from dtos.inbox_outbox import OutboxPublisher
from dtos.store import SQLiteStore

FAILURES: list[str] = []


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


def fresh() -> str:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    return tmp.name


def cleanup(path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            try:
                os.unlink(path + suffix)
            except OSError:
                pass


def seed(s: SQLiteStore, rows: list[tuple[str, str]]) -> None:
    with s.transaction() as c:
        for event_type, payload in rows:
            c.execute("INSERT INTO outbox (event_type, payload) VALUES (?, ?)", (event_type, payload))


def main() -> int:
    path = fresh()
    try:
        s = SQLiteStore(path)
        pub = OutboxPublisher(s)
        seed(s, [("A", '{"n": 1}'), ("B", '{"n": 2}')])

        # --- I-1: caída sostenida del destino -> cuarentena de toda la cola ---
        def down(event_type, payload):
            raise ConnectionError("destino caído")

        for _ in range(3):
            pub.publish_pending(down, max_attempts=3)
        quarantined = [row[0] for row in s.get_quarantined_outbox()]
        check(quarantined == [1, 2], "una caída sostenida (3 corridas = max_attempts) manda los dos eventos a cuarentena")
        check(s.get_pending_outbox() == [], "en cuarentena, get_pending_outbox no los devuelve")
        sent: list = []
        r = pub.publish_pending(lambda t, p: sent.append(t), max_attempts=3)
        check(r.published_count == 0 and sent == [], "con el destino ya recuperado, la cuarentena NO se reintenta sola (diseño D-65)")

        # --- el operador reencola uno ---
        check(s.requeue_quarantined(1, requeued_by="ops-ana", reason="destino recuperado tras la caída del 07/11") is True,
              "requeue_quarantined(1) devuelve True")
        check([row[0] for row in s.get_pending_outbox()] == [1], "el evento 1 vuelve a la cola y el 2 sigue en cuarentena")
        check([row[0] for row in s.get_quarantined_outbox()] == [2], "la cuarentena conserva solo el evento 2")
        r = pub.publish_pending(lambda t, p: sent.append((t, p)), max_attempts=3)
        check(sent == [("A", {"n": 1})] and r.published_count == 1, "la siguiente corrida publica el evento reencolado, con el contador reiniciado")

        # --- constancia (auditoría) ---
        log = s.get_outbox_requeues()
        check(len(log) == 1 and log[0][1:5] == (1, "ops-ana", "destino recuperado tras la caída del 07/11", 3),
              "outbox_requeues anota quién, por qué y los intentos previos")
        check("destino caído" in log[0][5], "outbox_requeues conserva el último error previo")
        check(s.get_outbox_requeues(outbox_id=2) == [], "get_outbox_requeues filtra por evento")

        # --- casos sin efecto ---
        check(s.requeue_quarantined(1, requeued_by="ops-ana", reason="otra vez") is False, "un evento ya publicado no se reencola (False)")
        check(s.requeue_quarantined(999, requeued_by="ops-ana", reason="no existe") is False, "un evento inexistente devuelve False")
        check(len(s.get_outbox_requeues()) == 1, "los reencolados sin efecto no dejan rastro falso")

        # --- validación de argumentos ---
        check(raises(ValueError, lambda: s.requeue_quarantined(2, requeued_by="", reason="x")), "requeued_by vacío -> ValueError")
        check(raises(ValueError, lambda: s.requeue_quarantined(2, requeued_by="ops", reason="   ")), "reason en blanco -> ValueError")
        check(raises(ValueError, lambda: s.requeue_quarantined(True, requeued_by="ops", reason="x")), "outbox_id bool -> ValueError")
        check(raises(ValueError, lambda: s.requeue_quarantined("2", requeued_by="ops", reason="x")), "outbox_id no entero -> ValueError")
        check([row[0] for row in s.get_quarantined_outbox()] == [2], "los argumentos inválidos no reencolan nada")

        # --- la bitácora es append-only ---
        check(raises(sqlite3.DatabaseError, lambda: s._conn.execute("UPDATE outbox_requeues SET reason = 'x'")), "outbox_requeues no admite UPDATE")
        check(raises(sqlite3.DatabaseError, lambda: s._conn.execute("DELETE FROM outbox_requeues")), "outbox_requeues no admite DELETE")

        # --- un payload ilegible reencolado vuelve a cuarentena (reintentar no lo arregla) ---
        seed(s, [("BAD", "esto no es json")])
        pub.publish_pending(lambda t, p: None, max_attempts=3)
        bad = [row[0] for row in s.get_quarantined_outbox() if row[1] == "BAD"]
        check(len(bad) == 1, "un payload no JSON queda en cuarentena en el acto")
        check(s.requeue_quarantined(bad[0], requeued_by="ops", reason="prueba") is True, "se puede reencolar un payload ilegible")
        pub.publish_pending(lambda t, p: None, max_attempts=3)
        check(bad[0] in [row[0] for row in s.get_quarantined_outbox()], "y vuelve a cuarentena en la siguiente corrida")
        s.close()
    finally:
        cleanup(path)

    print(f"\n{'TODO OK' if not FAILURES else str(len(FAILURES)) + ' FALLOS'}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
