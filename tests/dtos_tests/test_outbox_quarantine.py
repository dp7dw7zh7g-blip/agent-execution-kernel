"""
test_outbox_quarantine.py (parche 0014, D-65)
=============================================
Correcciones de la revisión adversarial de g0.8 r14:
  - C1: la exclusión mutua de `publish_pending` es un LEASE en SQLite (portable; el `flock` del 0013 no existe en Windows): no importa
        `fcntl`, funciona con bases en archivo y en memoria, caduca solo y se renueva antes de cada envío.
  - I1: `AuditChain.verify` devuelve CORRUPTED (no lanza) ante NaN, Infinity, un surrogate aislado, anidación extrema o un tipo no texto.
  - I2: un evento imposible de publicar (payload no JSON) o que agota sus intentos pasa a cuarentena y deja de bloquear la cola.
  - I4: `transaction()` captura BaseException: una KeyboardInterrupt dentro del bloque no deja la conexión inservible.

Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time

from dtos.audit import AuditChain
from dtos.enums import AuditIntegrityState
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


def drop_audit_triggers(s: SQLiteStore) -> None:
    for (name,) in s._conn.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='audit_log'").fetchall():
        s._conn.execute(f"DROP TRIGGER {name}")


def main() -> int:
    paths: list[str] = []
    try:
        # --- C1: el módulo no depende de fcntl ------------------------------------------------------------------------------
        import dtos.inbox_outbox as io
        check(not hasattr(io, "fcntl"), "C1: inbox_outbox ya no importa fcntl (portable: Windows incluido)")
        p = fresh(); paths.append(p); s = SQLiteStore(p)
        seed(s, [("A", "{}")])
        saved = sys.modules.get("fcntl")
        sys.modules["fcntl"] = None  # simula una plataforma sin fcntl: cualquier `import fcntl` fallaría
        try:
            res = OutboxPublisher(s).publish_pending(lambda t, pl: None)
        finally:
            if saved is not None:
                sys.modules["fcntl"] = saved
            else:
                del sys.modules["fcntl"]
        check(res.published_count == 1 and not res.skipped, "C1: con una base en archivo y sin fcntl, publish_pending publica")
        s.close()

        m = SQLiteStore(":memory:")
        seed(m, [("A", "{}"), ("B", "{}")])
        check(OutboxPublisher(m).publish_pending(lambda t, pl: None).published_count == 2, "C1: también funciona con una base en memoria")
        m.close()

        # --- lease: caducidad, renovación, pertenencia ------------------------------------------------------------------------
        p = fresh(); paths.append(p); s = SQLiteStore(p)
        check(s.acquire_publisher_lease("a", 10, now=100.0), "lease: libre -> lo toma a")
        check(not s.acquire_publisher_lease("b", 10, now=105.0), "lease: vigente de a -> b no lo toma")
        check(s.acquire_publisher_lease("a", 10, now=106.0), "lease: el mismo titular lo retoma")
        check(s.acquire_publisher_lease("b", 10, now=200.0), "lease: vencido (proceso muerto) -> b lo toma, no queda bloqueado para siempre")
        check(not s.renew_publisher_lease("a", 10, now=201.0), "lease: a ya no lo posee -> no puede renovarlo")
        check(s.renew_publisher_lease("b", 10, now=201.0), "lease: b sí lo renueva")
        check(not s.release_publisher_lease("a"), "lease: a no puede liberar el de b")
        check(s.release_publisher_lease("b"), "lease: b lo libera")
        for bad in (0, -1, True, "5", None, 86400):
            check(raises(ValueError, lambda b=bad: s.acquire_publisher_lease("a", b)), f"lease_s inválido {bad!r} -> ValueError")
        check(raises(ValueError, lambda: s.acquire_publisher_lease("  ", 5)), "holder vacío -> ValueError")

        # el lease se pierde a mitad de corrida: se detiene antes de enviar más
        seed(s, [(f"T{i}", "{}") for i in range(4)])
        sent: list[str] = []
        def steals(t, pl):
            sent.append(t)
            if len(sent) == 2:  # otro publicador roba el lease (el nuestro "venció")
                s._conn.execute("UPDATE outbox_publisher_lease SET holder='ladron', expires_at=? WHERE lease_name='outbox'", (time.time() + 60,))
        res = OutboxPublisher(s).publish_pending(steals)
        check(res.lease_lost is True and res.published_count == 2 and sent == ["T0", "T1"], "lease perdido: se detiene sin enviar más (lease_lost=True)")
        check([r[1] for r in s.get_pending_outbox()] == ["T2", "T3"], "lease perdido: los eventos no enviados siguen pendientes")
        s.release_publisher_lease("ladron")
        s.close()

        # --- I2: cuarentena ---------------------------------------------------------------------------------------------------------
        p = fresh(); paths.append(p); s = SQLiteStore(p)
        seed(s, [("BAD", "no-json")] * 100 + [("GOOD", "{}")])
        sent = []
        r1 = OutboxPublisher(s).publish_pending(lambda t, pl: sent.append(t), limit=100)
        check(r1.published_count == 0 and r1.quarantined_count == 100 and r1.failed_count == 100, "I2: 100 payloads no JSON a la cabeza: los 100 pasan a cuarentena")
        r2 = OutboxPublisher(s).publish_pending(lambda t, pl: sent.append(t), limit=100)
        check(sent == ["GOOD"] and r2.published_count == 1 and r2.failed_count == 0, "I2: la corrida siguiente ya llega al evento sano (antes: nunca)")
        check(len(s.get_quarantined_outbox()) == 100 and len(s.get_pending_outbox()) == 0, "I2: la cuarentena se puede consultar y no queda nada pendiente")
        check(s._conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 101, "I2: la cuarentena no borra ni modifica filas de `outbox`")
        check(raises(ValueError, lambda: s.get_quarantined_outbox(-1)), "get_quarantined_outbox(-1) -> ValueError")
        s.close()

        p = fresh(); paths.append(p); s = SQLiteStore(p)
        seed(s, [("FLAKY", "{}"), ("OK", "{}")])
        def flaky(t, pl):
            if t == "FLAKY":
                raise RuntimeError("proveedor caído")
        for i in range(2):
            r = OutboxPublisher(s).publish_pending(flaky, max_attempts=3)
        check([x[1] for x in s.get_pending_outbox()] == ["FLAKY"] and r.quarantined_count == 0, "I2: un fallo del sender reintenta (2 de 3 intentos, aún pendiente)")
        r = OutboxPublisher(s).publish_pending(flaky, max_attempts=3)
        check(r.quarantined_count == 1 and s.get_pending_outbox() == [], "I2: al agotar max_attempts pasa a cuarentena")
        q = s.get_quarantined_outbox()
        check(len(q) == 1 and q[0][2] == 3 and "proveedor caído" in q[0][3], "I2: la cuarentena guarda intentos y último error")
        check(raises(ValueError, lambda: OutboxPublisher(s).publish_pending(flaky, max_attempts=0)), "max_attempts=0 -> ValueError")
        check(raises(ValueError, lambda: s.record_outbox_failure(1, "x", max_attempts=True)), "record_outbox_failure(max_attempts=True) -> ValueError")
        s.close()

        # una cuarentena anotada por un proceso es vista por otro (mismo archivo)
        p = fresh(); paths.append(p); s = SQLiteStore(p); s2 = SQLiteStore(p)
        seed(s, [("BAD", "{x")])
        OutboxPublisher(s).publish_pending(lambda t, pl: None)
        check(s2.get_pending_outbox() == [] and len(s2.get_quarantined_outbox()) == 1, "I2: la cuarentena es visible desde otra conexión al mismo archivo")
        s.close(); s2.close()

        # --- I1: verify no lanza ------------------------------------------------------------------------------------------------------
        hostile = {
            "NaN": '{"a":NaN}', "Infinity": '{"a":Infinity}', "-Infinity": '{"a":-Infinity}', "surrogate aislado": '{"a":"\\ud800"}',
            "anidación extrema": "[" * 100000 + "]" * 100000, "entero gigante": '{"a":' + "9" * 5000 + "}",
            "no es JSON": "{rota", "vacío": "",
        }
        for label, raw in hostile.items():
            p = fresh(); paths.append(p); s = SQLiteStore(p); a = AuditChain(s)
            a.append({"k": 1}); a.append({"k": 2})
            drop_audit_triggers(s)
            s._conn.execute("UPDATE audit_log SET payload=? WHERE seq=2", (raw,))
            try:
                r = a.verify()
                ok = r.status == AuditIntegrityState.CORRUPTED and r.first_bad_seq == 2
            except BaseException as e:  # noqa: BLE001
                ok = False
                print(f"       lanzó {type(e).__name__}")
            check(ok, f"I1: verify() con payload «{label}»: CORRUPTED en seq=2, sin lanzar")
            s.close()
        p = fresh(); paths.append(p); s = SQLiteStore(p); a = AuditChain(s)
        a.append({"k": 1}); drop_audit_triggers(s)
        s._conn.execute("UPDATE audit_log SET payload=12345 WHERE seq=1")
        check(a.verify().status == AuditIntegrityState.CORRUPTED, "I1: payload almacenado como entero (tipo no texto): CORRUPTED, sin lanzar")
        s.close()

        # --- I4: transaction() y BaseException ----------------------------------------------------------------------------------------
        p = fresh(); paths.append(p); s = SQLiteStore(p)
        seed(s, [("A", "{}")])
        try:
            with s.transaction() as c:
                c.execute("UPDATE outbox SET published = 1")
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass
        check(not s._conn.in_transaction, "I4: tras una KeyboardInterrupt dentro de transaction() no queda ninguna transacción abierta")
        check(len(s.get_pending_outbox()) == 1, "I4: la interrupción revirtió los cambios del bloque")
        try:
            with s.transaction() as c:
                pass
            usable = True
        except Exception:
            usable = False
        check(usable, "I4: la conexión sigue siendo utilizable (antes: «cannot start a transaction within a transaction»)")
        try:
            with s.transaction() as c:
                raise SystemExit(3)
        except SystemExit:
            pass
        check(not s._conn.in_transaction, "I4: igual con SystemExit")
        try:
            with s.transaction() as c:
                raise ValueError("original")
        except ValueError as e:
            check(str(e) == "original", "I4: una excepción normal sigue propagándose intacta")
        s.close()
    finally:
        for p in paths:
            cleanup(p)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} comprobación(es) fallida(s):")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("Todas las comprobaciones pasaron.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
