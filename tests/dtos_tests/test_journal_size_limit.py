"""
test_journal_size_limit.py (parche 0021, D-73)
==============================================
`SQLiteStore(db_path, *, journal_size_limit_bytes=64 MiB)`: `PRAGMA journal_size_limit` aplicado y verificado por lectura en cada conexión.

Cubre: (1) valor por defecto, valores explícitos (-1, 0, otros), también en una base en memoria; (2) validación de argumentos (antes de abrir nada);
(3) EFECTO REAL y control negativo: con un lector abierto el WAL crece por encima del límite (el PRAGMA NO lo acota); al soltar el lector y reiniciarse el
WAL, con límite se recorta y con -1 se queda enorme; (4) el recorte lo hace la conexión que escribe: una conexión sin el PRAGMA no recorta;
(5) `backup_to` (parche 0020) sigue funcionando.
Sin el parche 0021 estas pruebas FALLAN (el parámetro no existe).

Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import tempfile

from dtos.audit import AuditChain
from dtos.store import DEFAULT_JOURNAL_SIZE_LIMIT_BYTES, SQLiteStore

FAILURES: list[str] = []
LIMIT = 1_000_000

# D-79: carga de las pruebas de WAL (una transacción con fsync por fila). Cada fila de auditoría deja ~17 KB en el WAL (medido).
# Completo = 8000 filas (~134 MB por base, la carga de siempre). Rápido (AEK_TESTS_FAST=1 o `run_tests.py --fast`) = 600 filas (~10,7 MB, 2,1x el umbral de 5 * LIMIT).
FILAS_COMPLETO = 8000
FILAS_RAPIDO = 600
RAPIDO = os.environ.get("AEK_TESTS_FAST") == "1"
FILAS = FILAS_RAPIDO if RAPIDO else FILAS_COMPLETO


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


def pragma(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA journal_size_limit").fetchone()[0]


def grow_wal_with_open_reader(path: str, writer: SQLiteStore, rows: int = FILAS):
    """Abre un lector (retiene su punto de lectura), escribe `rows` entradas de ~2 KB y devuelve (lector, tamaño del WAL)."""
    reader = sqlite3.connect(path, isolation_level=None)
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM audit_log").fetchall()
    a = AuditChain(writer)
    for i in range(rows):
        a.append({"i": i, "pad": "x" * 2000})
    return reader, os.path.getsize(path + "-wal")


def main() -> int:
    print(f"[carga] {'RÁPIDA' if RAPIDO else 'completa'}: {FILAS} filas por base (AEK_TESTS_FAST={'1' if RAPIDO else 'no definida'})")
    check(FILAS_RAPIDO * 17 * 1024 >= 2 * 5 * LIMIT, "el modo rápido conserva al menos 2x de margen sobre el umbral del WAL (5 * LIMIT, a ~17 KB por fila)")
    tmp = tempfile.mkdtemp(prefix="journal-limit-")
    try:
        # --- 1) valores ---------------------------------------------------------------------------------------------------------------------
        check(DEFAULT_JOURNAL_SIZE_LIMIT_BYTES == 64 * 1024 * 1024, "el valor por defecto es 64 MiB")
        s = SQLiteStore(os.path.join(tmp, "d.db"))
        check(s.journal_size_limit_bytes == 64 * 1024 * 1024 and pragma(s._conn) == 64 * 1024 * 1024, "por defecto: atributo y PRAGMA leído de vuelta = 64 MiB")
        s.close()
        for v in (-1, 0, 4096, LIMIT, 2 ** 40):
            x = SQLiteStore(os.path.join(tmp, f"v{v}.db"), journal_size_limit_bytes=v)
            check(pragma(x._conn) == v, f"journal_size_limit_bytes={v}: SQLite quedó en {v}")
            x.close()
        m = SQLiteStore(":memory:", journal_size_limit_bytes=12345)
        check(pragma(m._conn) == 12345, "base en memoria: también se aplica y se verifica")
        m.close()

        # --- 2) validación --------------------------------------------------------------------------------------------------------------------
        before = sorted(os.listdir(tmp))
        for bad in (True, False, None, "64", 1.5, -2, -1000, 2 ** 63, float("inf")):
            check(raises(ValueError, lambda b=bad: SQLiteStore(os.path.join(tmp, "bad.db"), journal_size_limit_bytes=b)), f"journal_size_limit_bytes={bad!r} -> ValueError")
        check(sorted(os.listdir(tmp)) == before, "un valor inválido no abre ni crea nada (se valida antes de abrir)")
        check(raises(TypeError, lambda: SQLiteStore(os.path.join(tmp, "bad.db"), 5000, 1)), "los parámetros de política son solo por nombre")

        # --- 3) efecto real + control negativo --------------------------------------------------------------------------------------------------
        p1 = os.path.join(tmp, "limite.db")  # D-78: «con.db» es un nombre de dispositivo reservado en Windows (CON): SQLite no lo abre
        w1 = SQLiteStore(p1, journal_size_limit_bytes=LIMIT)
        r1, wal_open = grow_wal_with_open_reader(p1, w1)
        check(wal_open > 5 * LIMIT, f"con un lector abierto el WAL SUPERA el límite ({wal_open / 1e6:.1f} MB > {LIMIT / 1e6:.0f} MB): el PRAGMA no lo acota")
        r1.execute("COMMIT")
        r1.close()
        w1._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        AuditChain(w1).append({"reinicia": "el WAL"})
        after1 = os.path.getsize(p1 + "-wal")
        check(after1 <= 2 * LIMIT, f"soltado el lector y reiniciado el WAL, con límite se recorta ({after1 / 1e6:.2f} MB)")
        check(AuditChain(w1).verify().status.name == "VALID", "los datos siguen íntegros tras el recorte")
        w1.close()

        p2 = os.path.join(tmp, "sin.db")
        w2 = SQLiteStore(p2, journal_size_limit_bytes=-1)
        r2, wal_open2 = grow_wal_with_open_reader(p2, w2)
        r2.execute("COMMIT")
        r2.close()
        w2._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        AuditChain(w2).append({"reinicia": "el WAL"})
        after2 = os.path.getsize(p2 + "-wal")
        check(after2 > 5 * LIMIT, f"CONTROL NEGATIVO: con -1 el WAL se queda grande tras el checkpoint ({after2 / 1e6:.1f} MB)")
        w2.close()

        # --- 4) el recorte lo hace la conexión que escribe --------------------------------------------------------------------------------------
        p3 = os.path.join(tmp, "otra.db")
        w3 = SQLiteStore(p3, journal_size_limit_bytes=LIMIT)
        r3, _ = grow_wal_with_open_reader(p3, w3)
        r3.execute("COMMIT")
        r3.close()
        raw = sqlite3.connect(p3, isolation_level=None)  # escribe SIN el PRAGMA
        raw.execute("PRAGMA wal_checkpoint(PASSIVE)")
        w3.close()
        raw.execute("PRAGMA busy_timeout=5000")
        raw.execute("CREATE TABLE IF NOT EXISTS aux(x)")
        raw.execute("INSERT INTO aux VALUES(1)")
        check(os.path.getsize(p3 + "-wal") > 5 * LIMIT, "una conexión que escribe sin el PRAGMA NO recorta (el límite es por conexión)")
        raw.close()

        # --- 5) backup_to sigue funcionando -----------------------------------------------------------------------------------------------------
        b = SQLiteStore(os.path.join(tmp, "b.db"), journal_size_limit_bytes=LIMIT)
        AuditChain(b).append({"a": 1})
        res = b.backup_to(os.path.join(tmp, "b-copia.db"))
        check(res.chain_verified and res.audit_entries == 1, "backup_to funciona con un límite explícito")
        b.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{'TODO OK' if not FAILURES else str(len(FAILURES)) + ' FALLOS'}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
