"""
test_inbox_outbox.py
======================
Ejecutar: python3 -m dtos.test_inbox_outbox
"""

from __future__ import annotations

import os
import sys
import tempfile

from dtos.inbox_outbox import InboxProcessor, OutboxPublisher, ReceiveStatus
from dtos.store import SQLiteStore

FAILURES: list[str] = []


def check(condition: bool, description: str) -> None:
    status = "OK " if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        FAILURES.append(description)


def main() -> int:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = tmp.name

    try:
        store = SQLiteStore(db_path)
        inbox = InboxProcessor(store)
        outbox = OutboxPublisher(store)

        # ===============================================================
        # 1. Inbox: primera entrega procesa; segunda (mismo dedup_key) es
        #    idempotente — el processor_fn NO se vuelve a ejecutar.
        # ===============================================================
        store._conn.execute(
            "CREATE TABLE IF NOT EXISTS projection_counter (id INTEGER PRIMARY KEY, total INTEGER NOT NULL DEFAULT 0)"
        )
        store._conn.execute("INSERT INTO projection_counter (id, total) VALUES (1, 0)")
        store._conn.commit()

        call_count = {"n": 0}

        def bump_projection(conn, payload):
            call_count["n"] += 1
            conn.execute("UPDATE projection_counter SET total = total + ? WHERE id = 1", (payload["amount"],))

        r1 = inbox.receive("evt-1", "AmountAdded", {"amount": 10}, bump_projection)
        check(r1.status == ReceiveStatus.PROCESSED, "Primera entrega de evt-1 se procesa")

        r2 = inbox.receive("evt-1", "AmountAdded", {"amount": 10}, bump_projection)
        check(r2.status == ReceiveStatus.ALREADY_PROCESSED, "Redelivery de evt-1 (mismo dedup_key) es ALREADY_PROCESSED")
        check(call_count["n"] == 1, "processor_fn se ejecutó UNA sola vez pese a dos entregas (dedup real, T-PROP-09)")

        total = store._conn.execute("SELECT total FROM projection_counter WHERE id = 1").fetchone()[0]
        check(total == 10, "La proyección solo refleja el efecto una vez, no dos (10, no 20)")

        # ===============================================================
        # 2. Atomicidad: si processor_fn falla, el dedup NO queda marcado
        #    como visto — se puede reprocesar limpio en el siguiente intento.
        # ===============================================================
        def failing_processor(conn, payload):
            conn.execute("UPDATE projection_counter SET total = total + ? WHERE id = 1", (payload["amount"],))
            raise RuntimeError("fallo simulado a mitad de procesamiento")

        try:
            inbox.receive("evt-2", "AmountAdded", {"amount": 999}, failing_processor)
            check(False, "receive() debería haber propagado la excepción de processor_fn")
        except RuntimeError:
            check(True, "receive() propaga la excepción de processor_fn (no la traga silenciosamente)")

        total_after_failure = store._conn.execute("SELECT total FROM projection_counter WHERE id = 1").fetchone()[0]
        check(
            total_after_failure == 10,
            "El UPDATE parcial de failing_processor hizo rollback (sigue en 10, no 1009): "
            "el fallo no dejó la proyección a medio aplicar",
        )

        row = store._conn.execute("SELECT processed FROM inbox WHERE dedup_key = 'evt-2'").fetchone()
        check(
            row is None,
            "El INSERT en inbox para evt-2 también hizo rollback: no quedó 'visto' un evento que no se aplicó",
        )

        # Reintento limpio: ahora sí debe procesar de verdad.
        def working_processor(conn, payload):
            conn.execute("UPDATE projection_counter SET total = total + ? WHERE id = 1", (payload["amount"],))

        r3 = inbox.receive("evt-2", "AmountAdded", {"amount": 5}, working_processor)
        check(r3.status == ReceiveStatus.PROCESSED, "Reintento de evt-2 tras el fallo se procesa limpio")
        total_final = store._conn.execute("SELECT total FROM projection_counter WHERE id = 1").fetchone()[0]
        check(total_final == 15, "La proyección queda en 15 (10 + 5), consistente con solo las entregas exitosas")

        # ===============================================================
        # 3. Outbox: publish_pending envía y marca published, at-least-once
        # ===============================================================
        store._conn.execute("INSERT INTO tasks (task_id, goal_id, status) VALUES ('t-1', 'g-1', 'ACTIVE')")
        store._conn.commit()
        store.create_cognitive_cycle(
            cycle_id="c-1", task_id="t-1", state="CREATED", state_hash="a" * 64, context_hash="a" * 64
        )
        store.commit_cognitive_cycle_transition(
            cycle_id="c-1",
            new_state="PREPARING",
            expected_version=1,
            new_state_hash="b" * 64,
            new_context_hash="b" * 64,
            domain_event_payload={"cycle_id": "c-1", "dependency_epoch": 1},
        )
        store.commit_cognitive_cycle_transition(
            cycle_id="c-1",
            new_state="CONTEXT_ASSEMBLING",
            expected_version=2,
            new_state_hash="c" * 64,
            new_context_hash="c" * 64,
            domain_event_payload={"cycle_id": "c-1", "dependency_epoch": 2},
        )

        pending_before = store._conn.execute("SELECT COUNT(*) FROM outbox WHERE published = 0").fetchone()[0]
        check(pending_before == 2, "Hay 2 eventos pendientes en outbox tras los dos commits")

        sent = []

        def sender_ok(event_type, payload):
            sent.append((event_type, payload))

        result = outbox.publish_pending(sender_ok)
        check(result.published_count == 2 and result.failed_count == 0, "publish_pending envía y marca los 2 eventos pendientes")
        check(len(sent) == 2, "sender_fn fue invocado para cada evento pendiente")

        pending_after = store._conn.execute("SELECT COUNT(*) FROM outbox WHERE published = 0").fetchone()[0]
        check(pending_after == 0, "Tras publicar, no quedan eventos pendientes")

        # Un segundo publish_pending no reenvía lo ya publicado.
        sent.clear()
        result2 = outbox.publish_pending(sender_ok)
        check(result2.published_count == 0 and len(sent) == 0, "Eventos ya publicados no se reenvían en la siguiente corrida")

        # ===============================================================
        # 4. Outbox: sender_fn que falla deja el evento pendiente (no se
        #    marca published), demostrando el hueco at-least-once real.
        # ===============================================================
        store.commit_cognitive_cycle_transition(
            cycle_id="c-1",
            new_state="REPRESENTING",
            expected_version=3,
            new_state_hash="d" * 64,
            new_context_hash="d" * 64,
            domain_event_payload={"cycle_id": "c-1", "dependency_epoch": 3},
        )

        def sender_fails(event_type, payload):
            raise ConnectionError("transporte caído")

        result3 = outbox.publish_pending(sender_fails)
        check(
            result3.published_count == 0 and result3.failed_count == 1,
            "Si sender_fn falla, el evento NO se marca published (queda pendiente para reintento)",
        )
        pending_after_failure = store._conn.execute("SELECT COUNT(*) FROM outbox WHERE published = 0").fetchone()[0]
        check(pending_after_failure == 1, "El evento sigue pendiente tras el fallo de envío, listo para la próxima corrida")

        store.close()

        # -- Resumen ------------------------------------------------------
        print()
        if FAILURES:
            print(f"{len(FAILURES)} test(s) fallaron:")
            for f in FAILURES:
                print(f"  - {f}")
            return 1
        print("Todos los inbox/outbox tests pasaron.")
        return 0

    finally:
        os.unlink(db_path)


if __name__ == "__main__":
    sys.exit(main())
