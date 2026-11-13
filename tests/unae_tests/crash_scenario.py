"""Proceso hijo de la prueba de caídas. Uso: python tests/unae_tests/crash_scenario.py <db> <dry|N|migrate>

dry     -> ejecuta el escenario completo y imprime (JSON) los puntos de inyección encontrados.
N       -> el proceso se mata con SIGKILL en el N-ésimo punto de inyección (1-based).
migrate -> abre la base y se mata justo antes del commit de la migración de esquema.
"""
import json
import os
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from unae import Store, sign_receipt  # noqa: E402
from unae.store import CommitRequest  # noqa: E402

KEY = b"k" * 32


def receipt(s, op, outcome, compute=5):
    kw = dict(receipt_id=f"r-{op}", operation_id=op, reservation_id=f"res-{op}", outcome=outcome, actual_compute=compute,
              actual_time_ms=5, peak_memory_bytes=5, provider_operation_id=f"p-{op}")
    return s.record_receipt(provider_id="prov-A", signature=sign_receipt(KEY, provider_id="prov-A", task_id="task-1", **kw), task_id="task-1", **kw)


def reserve(s, op, token):
    return s.reserve_and_register(task_id="task-1", operation_id=op, reservation_id=f"res-{op}", idempotency_key=f"k-{op}",
                                  fencing_token=token, compute_units=10, time_ms=10, memory_bytes=10)


def start(s, op, token):
    reserve(s, op, token)
    s.mark_running(op, token, f"p-{op}", provider_id="prov-A")


def commit(s, op, token, version):
    return s.coordinated_commit(CommitRequest(f"c-{op}", "task-1", op, f"res-{op}", f"r-{op}", version, token, {"v": version + 1}, f"e-{op}"))


def run(s, hook):
    sender = lambda e: hook("dispatch:sender")  # noqa: E731
    start(s, "a", 1); receipt(s, "a", "SUCCEEDED"); commit(s, "a", 1, 0); s.dispatch_outbox(sender)
    reserve(s, "b", 1); s.release_reservation("b", 1)
    start(s, "c", 1); reserve(s, "d", 1); start(s, "e", 1); receipt(s, "e", "SUCCEEDED")
    t = s.acquire_authority("task-1", "w2")                       # c: UNKNOWN, d: cancelada, e: re-fenceada
    s.begin_reconciliation("c", t); receipt(s, "c", "FAILED"); commit(s, "c", t, 1)
    reserve(s, "f", t); s.expire_reservations(60, now=time.time() + 120)
    commit(s, "e", t, 2); s.dispatch_outbox(sender)
    start(s, "g", t); t2 = s.acquire_authority("task-1", "w3")      # g: UNKNOWN; su recibo llega tarde y pasa al dueño vigente
    receipt(s, "g", "SUCCEEDED"); commit(s, "g", t2, 3); s.dispatch_outbox(sender)


def main():
    db, mode = sys.argv[1], sys.argv[2]
    names = []
    if mode == "migrate":
        def hook(name):
            if name == "_migrate:before_commit":
                os.kill(os.getpid(), signal.SIGKILL)
        Store(db, fault_hook=hook)
        return
    target = None if mode == "dry" else int(mode)

    def hook(name):
        if name.startswith("_migrate"):
            return
        names.append(name)
        if target is not None and len(names) == target:
            os.kill(os.getpid(), signal.SIGKILL)

    run(Store(db, keyring={"prov-A": KEY}, fault_hook=hook), hook)
    if mode == "dry":
        print(json.dumps(names))


if __name__ == "__main__":
    main()
