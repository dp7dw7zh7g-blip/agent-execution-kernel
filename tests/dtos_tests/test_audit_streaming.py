"""
test_audit_streaming.py (parche 0019, D-71)
===========================================
Hallazgo de la auditoría de SQLite de g0.8 r24: `AuditChain.verify()` leía TODA la cadena con `read_audit_chain()` (`fetchall`) y la memoria
crecía con el número de entradas. Ahora lee por lotes (`SQLiteStore.iter_audit_chain`, paginación por `seq`).

Lo que más importa aquí NO es la memoria sino que el resultado no cambie: `verify()` por lotes debe devolver EXACTAMENTE lo mismo que la
verificación anterior (estado, total_entries, first_bad_seq y detail), para cada tipo de manipulación, en cada posición de la cadena y con
varios tamaños de lote. Por eso la prueba lleva una copia literal del algoritmo anterior como referencia.
Sin el parche 0019 estas pruebas FALLAN (no existen `iter_audit_chain`, `count_audit_entries` ni el parámetro `batch_size`).

Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import tracemalloc

from dtos.audit import AuditChain, GENESIS_HASH
from dtos.canonical import canonical_json, content_hash
from dtos.enums import AuditIntegrityState
from dtos.store import SQLiteStore

FAILURES: list[str] = []
N = 7
BATCHES = (1, 2, 3, 7, 1000)


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


def legacy_verify(rows: list[tuple]) -> tuple:
    """Copia LITERAL de `AuditChain.verify` de g0.8 r24 (lectura completa), devolviendo (status, total, first_bad_seq, detail)."""
    if not rows:
        return (AuditIntegrityState.VALID, 0, None, None)
    expected_seq = 1
    running_hash = GENESIS_HASH
    for seq, entry_hash, prev_hash, payload_text in rows:
        if seq != expected_seq:
            return (AuditIntegrityState.TRUNCATED, len(rows), expected_seq,
                    f"hueco en seq: se esperaba {expected_seq}, se encontró {seq} "
                    f"(una fila fue eliminada directamente, evadiendo el trigger append-only)")
        if prev_hash != running_hash:
            return (AuditIntegrityState.FORKED, len(rows), seq,
                    f"prev_hash de seq={seq} no coincide con el entry_hash real de la "
                    f"entrada anterior (la cadena fue reescrita o reordenada)")
        try:
            payload = json.loads(payload_text)
            recomputed = content_hash({"prev_hash": prev_hash, "payload": payload})
        except (TypeError, ValueError, RecursionError) as e:
            return (AuditIntegrityState.CORRUPTED, len(rows), seq,
                    f"payload de seq={seq} no es un JSON canonicalizable "
                    f"({type(e).__name__}); el payload fue modificado directamente en el archivo")
        if recomputed != entry_hash:
            return (AuditIntegrityState.CORRUPTED, len(rows), seq,
                    f"entry_hash de seq={seq} no coincide con el recálculo sobre el "
                    f"payload almacenado (el payload fue modificado directamente en el archivo)")
        running_hash = entry_hash
        expected_seq += 1
    return (AuditIntegrityState.VALID, len(rows), None, None)


def as_tuple(r) -> tuple:
    return (r.status, r.total_entries, r.first_bad_seq, r.detail)


def tamper(conn, kind: str, k: int) -> None:
    conn.execute("DROP TRIGGER trg_audit_log_no_update")
    conn.execute("DROP TRIGGER trg_audit_log_no_delete")
    if kind == "payload":
        conn.execute("UPDATE audit_log SET payload = '{\"event\":\"TAMPERED\"}' WHERE seq = ?", (k,))
    elif kind == "not_json":
        conn.execute("UPDATE audit_log SET payload = 'esto no es json' WHERE seq = ?", (k,))
    elif kind == "nan":
        conn.execute("UPDATE audit_log SET payload = '{\"x\": NaN}' WHERE seq = ?", (k,))
    elif kind == "entry_hash":
        conn.execute("UPDATE audit_log SET entry_hash = ? WHERE seq = ?", ("f" * 64, k))
    elif kind == "prev_hash":
        conn.execute("UPDATE audit_log SET prev_hash = ? WHERE seq = ?", ("e" * 64, k))
    elif kind == "delete":
        conn.execute("DELETE FROM audit_log WHERE seq = ?", (k,))
    else:
        raise AssertionError(kind)


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="audit-stream-")
    try:
        # plantilla: una cadena válida de N entradas, ya volcada al archivo principal
        template = os.path.join(tmp, "template.db")
        s = SQLiteStore(template)
        chain = AuditChain(s)
        for i in range(N):
            chain.append({"event": "e", "n": i})
        s._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        s.close()

        # --- cadena vacía y válida ---
        empty = SQLiteStore(os.path.join(tmp, "empty.db"))
        check(list(empty.iter_audit_chain()) == [] and empty.count_audit_entries() == 0, "cadena vacía: iter_audit_chain no cede lotes y el recuento es 0")
        check(as_tuple(AuditChain(empty).verify()) == legacy_verify([]), "cadena vacía: verify == referencia (VALID, 0)")
        empty.close()

        s = SQLiteStore(template)
        ref_rows = s.read_audit_chain()
        for b in BATCHES:
            batches = list(s.iter_audit_chain(batch_size=b))
            flat = [r for batch in batches for r in batch]
            check(flat == ref_rows and all(1 <= len(x) <= b for x in batches),
                  f"batch_size={b}: los lotes (<= {b} filas) concatenados == read_audit_chain() ({N} filas, en orden de seq)")
            check(as_tuple(AuditChain(s).verify(batch_size=b)) == legacy_verify(ref_rows) == (AuditIntegrityState.VALID, N, None, None),
                  f"batch_size={b}: cadena válida -> VALID con {N} entradas, igual que la referencia")
        check([len(x) for x in s.iter_audit_chain(batch_size=3)] == [3, 3, 1], "batch_size=3 sobre 7 filas -> lotes de 3, 3 y 1")
        s.close()

        # --- equivalencia con la verificación anterior: cada manipulación, en cada posición, con cada tamaño de lote ---
        mismatches: list[str] = []
        cases = 0
        for kind in ("payload", "not_json", "nan", "entry_hash", "prev_hash", "delete"):
            for k in range(1, N + 1):
                path = os.path.join(tmp, f"{kind}-{k}.db")
                shutil.copy(template, path)
                st = SQLiteStore(path)
                tamper(st._conn, kind, k)
                expected = legacy_verify(st.read_audit_chain())
                for b in BATCHES:
                    got = as_tuple(AuditChain(st).verify(batch_size=b))
                    cases += 1
                    if got != expected:
                        mismatches.append(f"{kind}@{k} batch={b}: {got} != {expected}")
                st.close()
        check(not mismatches, f"verify por lotes == verificación anterior en {cases} casos (6 manipulaciones x {N} posiciones x {len(BATCHES)} lotes)"
              + ("" if not mismatches else f": {mismatches[:3]}"))

        # los estados esperados siguen siendo los documentados (no solo «igual que antes»)
        def status_of(kind, k):
            path = os.path.join(tmp, f"{kind}-{k}.db")
            st = SQLiteStore(path)
            try:
                r = AuditChain(st).verify(batch_size=2)
                return r.status, r.first_bad_seq, r.total_entries
            finally:
                st.close()
        check(status_of("payload", 4) == (AuditIntegrityState.CORRUPTED, 4, N), "payload modificado en seq=4 -> CORRUPTED en 4, total 7 (fila con fallo en mitad de lote)")
        check(status_of("prev_hash", 5) == (AuditIntegrityState.FORKED, 5, N), "prev_hash alterado en seq=5 -> FORKED en 5")
        check(status_of("delete", 1) == (AuditIntegrityState.TRUNCATED, 1, N - 1), "cabeza borrada -> TRUNCATED en seq=1, total 6")
        check(status_of("delete", 4) == (AuditIntegrityState.TRUNCATED, 4, N - 1), "hueco en la mitad -> TRUNCATED en seq=4, total 6")
        check(status_of("delete", N) == (AuditIntegrityState.VALID, None, N - 1), "borrar las ÚLTIMAS entradas NO se detecta (límite documentado de D-64)")

        # --- argumentos ---
        s = SQLiteStore(template)
        for bad in (0, -1, True, 1.5, "10", None, 100_001):
            check(raises(ValueError, lambda b=bad: s.iter_audit_chain(batch_size=b)), f"iter_audit_chain(batch_size={bad!r}) -> ValueError al llamar (no al iterar)")
            check(raises(ValueError, lambda b=bad: AuditChain(s).verify(batch_size=b)), f"verify(batch_size={bad!r}) -> ValueError")
        check(raises(TypeError, lambda: s.iter_audit_chain(2)), "batch_size es solo por nombre")

        # --- entre lotes no queda ninguna lectura abierta y se puede escribir ---
        it = s.iter_audit_chain(batch_size=2)
        first = next(it)
        check(not s._conn.in_transaction, "entre lotes no hay transacción de lectura abierta (no se retiene el checkpoint de WAL)")
        AuditChain(s).append({"event": "escrito entre lotes"})
        rest = [r for batch in it for r in batch]
        check(len(first) == 2 and [r[0] for r in first + rest] == list(range(1, N + 2)), "una entrada añadida mientras se itera aparece al final del recorrido")

        # una entrada añadida a mitad de un verify() no lo rompe: la cadena sigue siendo VALID y cuenta las filas verificadas
        real_iter = s.iter_audit_chain
        appended = []

        def iter_and_append(*, batch_size):
            for i, batch in enumerate(real_iter(batch_size=batch_size)):
                yield batch
                if i == 0 and not appended:
                    appended.append(AuditChain(s).append({"event": "durante verify"}))
        s.iter_audit_chain = iter_and_append  # type: ignore[method-assign]
        r = AuditChain(s).verify(batch_size=2)
        check(r.status == AuditIntegrityState.VALID and r.total_entries == N + 2, f"append durante verify(): VALID y total = {N + 2} (las {N + 1} previas + la nueva)")
        del s.iter_audit_chain
        s.close()

        # --- memoria: O(batch_size) frente a O(n) ---
        big = SQLiteStore(os.path.join(tmp, "big.db"))
        rows, prev = [], GENESIS_HASH
        for i in range(20_000):
            payload = {"event": "e", "n": i}
            h = content_hash({"prev_hash": prev, "payload": payload})
            rows.append((h, prev, canonical_json(payload)))
            prev = h
        with big.transaction() as conn:
            conn.executemany("INSERT INTO audit_log (entry_hash, prev_hash, payload) VALUES (?, ?, ?)", rows)
        del rows
        tracemalloc.start()
        legacy_rows = big.read_audit_chain()
        legacy_peak = tracemalloc.get_traced_memory()[1]
        del legacy_rows
        tracemalloc.reset_peak()
        result = AuditChain(big).verify(batch_size=500)
        stream_peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        check(result.status == AuditIntegrityState.VALID and result.total_entries == 20_000, "cadena de 20 000 entradas: VALID por lotes")
        check(stream_peak < legacy_peak / 3, f"memoria pico por lotes ({stream_peak / 1e6:.2f} MB) < 1/3 de la lectura completa ({legacy_peak / 1e6:.2f} MB)")
        big.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{'TODO OK' if not FAILURES else str(len(FAILURES)) + ' FALLOS'}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
