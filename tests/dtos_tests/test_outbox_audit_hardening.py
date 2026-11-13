"""
test_outbox_audit_hardening.py (parche 0013, D-64)
==================================================
Endurece las lecturas públicas del 0012 tras su revisión adversarial:
  - `get_pending_outbox(limit)` valida `limit` (ValueError); antes -1 devolvía todo y None/1.5 daban `datatype mismatch`.
  - `publish_pending`: un payload que no es JSON se informa en `failures` y no bloquea los eventos siguientes.
  - `publish_pending`: exclusión mutua entre publicadores; un segundo publicador no duplica los envíos (D-65: lease en SQLite, antes flock).
  - `AuditChain.verify`: payload no JSON -> CORRUPTED (no lanza); la cadena debe empezar en seq=1 (cabeza borrada -> TRUNCATED).
  - Límite documentado: reescribir la cadena recalculando hashes y borrar las últimas entradas NO se detectan (se fija como hecho).

Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
from pathlib import Path

from dtos.audit import AuditChain
from dtos.canonical import canonical_json, content_hash
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
            os.unlink(path + suffix)


def seed_outbox(s: SQLiteStore, payloads: list[tuple[str, str]]) -> None:
    with s.transaction() as c:
        for event_type, payload in payloads:
            c.execute("INSERT INTO outbox (event_type, payload) VALUES (?, ?)", (event_type, payload))


def main() -> int:
    paths: list[str] = []
    try:
        # --- limit inválido ------------------------------------------------------------------------------------------------------
        p = fresh(); paths.append(p); s = SQLiteStore(p)
        seed_outbox(s, [(f"T{i}", "{}") for i in range(5)])
        for bad in (-1, None, 1.5, True, "2"):
            check(raises(ValueError, lambda b=bad: s.get_pending_outbox(b)), f"get_pending_outbox({bad!r}) lanza ValueError")
        check(len(s.get_pending_outbox(0)) == 0 and len(s.get_pending_outbox(2)) == 2 and len(s.get_pending_outbox()) == 5,
              "get_pending_outbox: 0, 2 y el valor por defecto siguen funcionando")
        s.close()

        # --- payload corrupto en medio del outbox ---------------------------------------------------------------------------------
        p = fresh(); paths.append(p); s = SQLiteStore(p)
        seed_outbox(s, [("A", '{"i":0}'), ("B", "no-json"), ("C", '{"i":2}')])
        sent: list[str] = []
        res = OutboxPublisher(s).publish_pending(lambda t, pl: sent.append(t))
        check(sent == ["A", "C"] and res.published_count == 2 and res.failed_count == 1 and res.failures[0][0] == 2,
              "payload corrupto: se envían A y C, y B queda en failures (con su outbox_id)")
        check(s.get_pending_outbox() == [] and [r[0] for r in s.get_quarantined_outbox()] == [2],
              "payload corrupto: B ya no queda pendiente, pasa a cuarentena (D-65; antes se reintentaba para siempre)")
        check(res.skipped is False, "una corrida normal no se marca skipped")
        s.close()

        # --- exclusión mutua --------------------------------------------------------------------------------------------------------
        p = fresh(); paths.append(p); s = SQLiteStore(p)
        seed_outbox(s, [(f"T{i}", "{}") for i in range(3)])
        check(s.acquire_publisher_lease("otro-proceso", 60), "«otro proceso» toma el lease del publicador")
        sent = []
        res = OutboxPublisher(s).publish_pending(lambda t, pl: sent.append(t))
        check(res.skipped is True and res.published_count == 0 and sent == [], "lease tomado por otro: se omite la corrida y no se envía nada")
        check(len(s.get_pending_outbox()) == 3, "lease tomado por otro: los eventos siguen pendientes")
        s.release_publisher_lease("otro-proceso")
        res = OutboxPublisher(s).publish_pending(lambda t, pl: sent.append(t))
        check(res.skipped is False and res.published_count == 3, "lease liberado: la corrida siguiente publica")

        seed_outbox(s, [("X", "{}")])
        def boom(t, pl):
            raise KeyboardInterrupt()
        check(raises(KeyboardInterrupt, lambda: OutboxPublisher(s).publish_pending(boom)), "una interrupción en el sender se propaga")
        res = OutboxPublisher(s).publish_pending(lambda t, pl: None)
        check(res.skipped is False and res.published_count == 1, "tras una interrupción el lease se libera (la corrida siguiente no se omite)")
        s.close()

        # Dos publicadores concurrentes (una conexión por hilo, mismo archivo): ningún evento sale dos veces.
        p = fresh(); paths.append(p); s = SQLiteStore(p)
        seed_outbox(s, [(f"T{i}", "{}") for i in range(20)])
        s.close()
        out: list[list[str]] = [[], []]
        def run(k: int) -> None:
            st = SQLiteStore(p)
            try:
                OutboxPublisher(st).publish_pending(lambda t, pl: (out[k].append(t), __import__("time").sleep(0.01)))
            finally:
                st.close()
        ts = [threading.Thread(target=run, args=(k,)) for k in range(2)]
        for t_ in ts: t_.start()
        for t_ in ts: t_.join()
        allsent = out[0] + out[1]
        check(len(allsent) == len(set(allsent)), "dos publicadores concurrentes: ningún evento se envía dos veces")

        # --- auditoría ----------------------------------------------------------------------------------------------------------------
        p = fresh(); paths.append(p); s = SQLiteStore(p); a = AuditChain(s)
        for i in range(4):
            a.append({"k": i})
        check(a.verify().status == AuditIntegrityState.VALID, "cadena intacta: VALID")
        s._conn.execute("DROP TRIGGER trg_audit_log_no_update")
        s._conn.execute("UPDATE audit_log SET payload = '{rota' WHERE seq = 2")
        r = a.verify()
        check(r.status == AuditIntegrityState.CORRUPTED and r.first_bad_seq == 2, "payload no JSON: CORRUPTED con first_bad_seq=2 (no lanza)")
        s.close()

        p = fresh(); paths.append(p); s = SQLiteStore(p); a = AuditChain(s)
        for i in range(4):
            a.append({"k": i})
        s._conn.execute("DROP TRIGGER trg_audit_log_no_delete")
        s._conn.execute("DELETE FROM audit_log WHERE seq = 1")
        r = a.verify()
        check(r.status == AuditIntegrityState.TRUNCATED and r.first_bad_seq == 1, "cabeza borrada (la cadena no empieza en seq=1): TRUNCATED")
        s.close()

        # Límites documentados (D-64): se fijan para que un cambio futuro sea deliberado.
        p = fresh(); paths.append(p); s = SQLiteStore(p); a = AuditChain(s)
        for i in range(4):
            a.append({"k": i})
        s._conn.execute("DROP TRIGGER trg_audit_log_no_delete")
        s._conn.execute("DELETE FROM audit_log WHERE seq IN (3, 4)")
        check(a.verify().status == AuditIntegrityState.VALID, "LÍMITE documentado: borrar las últimas entradas no se detecta")
        s2 = SQLiteStore(fresh()); paths.append(s2.db_path); a2 = AuditChain(s2)
        for i in range(3):
            a2.append({"k": i})
        s2._conn.execute("DROP TRIGGER trg_audit_log_no_update")
        rows = s2.read_audit_chain()
        prev = rows[0][1]
        for seq, _eh, _ph, pt in rows[1:]:
            pl = json.loads(pt)
            if seq == 2:
                pl = {"k": "falsificada"}
            h = content_hash({"prev_hash": prev, "payload": pl})
            s2._conn.execute("UPDATE audit_log SET entry_hash=?, prev_hash=?, payload=? WHERE seq=?", (h, prev, canonical_json(pl), seq))
            prev = h
        check(a2.verify().status == AuditIntegrityState.VALID, "LÍMITE documentado: una cadena reescrita con los hashes recalculados no se detecta")
        check("LÍMITE DE LA GARANTÍA" in (Path(__file__).resolve().parents[2] / "src" / "dtos" / "audit.py").read_text(encoding="utf-8"),
              "audit.py declara el límite de la garantía")
        s.close(); s2.close()
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
