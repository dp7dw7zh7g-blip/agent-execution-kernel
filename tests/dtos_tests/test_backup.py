"""
test_backup.py (parche 0020, D-72)
==================================
Backup en caliente: `SQLiteStore.backup_to(target_path, *, verify_chain=True)` (VACUUM INTO + validación + os.replace).

Cubre: (1) la copia es válida, abrible y conserva cadena, `sqlite_sequence` y triggers; (2) NO sobrescribe un destino existente; (3) no deja
temporales ni -wal/-shm; (4) dentro de una transacción falla con RuntimeError (y el hecho de SQLite que lo motiva); (5) una copia que no valida
no deja nada (integrity_check y cadena alterada) y `verify_chain=False` permite respaldar igualmente una base alterada; (6) escritores
concurrentes durante el backup; (7) permisos 0600 (POSIX); (8) validación de argumentos; (9) la base de origen sigue usable.
Sin el parche 0020 estas pruebas FALLAN (no existe `backup_to`).

Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path

from dtos.audit import AuditChain
from dtos.store import BackupResult, SQLiteStore

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


def filled(path: str, n: int = 200) -> tuple[SQLiteStore, AuditChain]:
    s = SQLiteStore(path)
    a = AuditChain(s)
    for i in range(n):
        a.append({"i": i, "pad": "x" * 50})
    return s, a


def names(d: str) -> list[str]:
    return sorted(os.listdir(d))


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="backup-")
    try:
        # --- 1) copia válida --------------------------------------------------------------------------------------------------------------
        src = os.path.join(tmp, "src.db")
        s, a = filled(src)
        dst = os.path.join(tmp, "copia.db")
        r = s.backup_to(dst)
        check(isinstance(r, BackupResult) and r.path == str(Path(dst).absolute()) and r.audit_entries == 200 and r.chain_verified is True,
              "devuelve BackupResult (ruta, 200 entradas, cadena verificada)")
        check(r.size_bytes == os.path.getsize(dst) > 0, "size_bytes coincide con el archivo final")
        c = SQLiteStore(dst)
        v = AuditChain(c).verify()
        check(v.status.name == "VALID" and v.total_entries == 200, "la copia se abre con SQLiteStore y su cadena es VALID (200)")
        check(c.get_last_audit_hash() == s.get_last_audit_hash(), "el último hash de la copia coincide con el del origen")
        seq_src = s._conn.execute("SELECT name, seq FROM sqlite_sequence ORDER BY name").fetchall()
        seq_dst = c._conn.execute("SELECT name, seq FROM sqlite_sequence ORDER BY name").fetchall()
        check(seq_src == seq_dst and ("audit_log", 200) in [tuple(x) for x in seq_dst], "sqlite_sequence se conserva (los ids no se reutilizan)")
        trg = lambda conn: sorted(x[0] for x in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'"))
        check(trg(s._conn) == trg(c._conn) and "trg_audit_log_no_update" in trg(c._conn), "los triggers append-only se conservan")
        check(c._conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "integrity_check de la copia: ok")
        AuditChain(c).append({"despues": "de restaurar"})
        check(AuditChain(c).verify().status.name == "VALID", "la copia admite nuevas entradas y sigue VALID")
        c.close()

        # --- 3) sin temporales ni auxiliares ----------------------------------------------------------------------------------------------
        check(names(tmp)[:1] == ["copia.db"] and not [n for n in names(tmp) if n.endswith(".tmp") or n.startswith("copia.db-") or n.startswith(".")],
              f"no quedan temporales ni -wal/-shm de la copia: {names(tmp)}")

        # --- 2) no sobrescribe ------------------------------------------------------------------------------------------------------------
        before = Path(dst).read_bytes()
        check(raises(FileExistsError, lambda: s.backup_to(dst)), "destino existente -> FileExistsError")
        check(Path(dst).read_bytes() == before, "el destino existente no se tocó")
        check(raises(FileExistsError, lambda: s.backup_to(Path(dst))), "también con os.PathLike")
        n0 = names(tmp)
        check(raises(FileNotFoundError, lambda: s.backup_to(os.path.join(tmp, "no-existe", "x.db"))), "directorio de destino inexistente -> FileNotFoundError")
        check(names(tmp) == n0, "un destino inválido no deja archivos")

        # --- 4) dentro de una transacción -------------------------------------------------------------------------------------------------
        raw = sqlite3.connect(os.path.join(tmp, "raw.db"), isolation_level=None)
        raw.execute("CREATE TABLE t(x)")
        raw.execute("BEGIN IMMEDIATE")
        check(raises(sqlite3.OperationalError, lambda: raw.execute("VACUUM INTO ?", (os.path.join(tmp, "nope.db"),))),
              "hecho de SQLite: VACUUM INTO dentro de una transacción -> OperationalError")
        raw.execute("ROLLBACK")
        raw.close()
        tx_target = os.path.join(tmp, "tx.db")
        with s.transaction():
            ok = raises(RuntimeError, lambda: s.backup_to(tx_target))
        check(ok and not os.path.exists(tx_target), "backup_to dentro de transaction() -> RuntimeError y no crea nada")
        check(not s._conn.in_transaction, "la conexión queda fuera de transacción tras el rechazo")

        # --- 8) argumentos ----------------------------------------------------------------------------------------------------------------
        for bad in (None, 5, True, b"x.db", ["x"]):
            check(raises(TypeError, lambda b=bad: s.backup_to(b)), f"target_path={bad!r} -> TypeError")
        for bad in ("", "   "):
            check(raises(ValueError, lambda b=bad: s.backup_to(b)), f"target_path={bad!r} -> ValueError")
        for bad in (1, None, "si"):
            check(raises(ValueError, lambda b=bad: s.backup_to(os.path.join(tmp, "v.db"), verify_chain=b)), f"verify_chain={bad!r} -> ValueError")
        check(raises(TypeError, lambda: s.backup_to(os.path.join(tmp, "v.db"), True)), "verify_chain es solo por nombre")
        check(not os.path.exists(os.path.join(tmp, "v.db")), "los argumentos inválidos no crean nada")

        # --- 7) permisos ------------------------------------------------------------------------------------------------------------------
        if os.name == "posix":
            check((os.stat(dst).st_mode & 0o777) == 0o600, "la copia queda con permisos 0600")
        else:
            print("[SKIP] permisos 0600: solo se comprueban en POSIX")

        # --- 5) la validación rechaza una copia mala --------------------------------------------------------------------------------------
        n1 = names(tmp)
        orig = SQLiteStore._integrity_problems
        SQLiteStore._integrity_problems = staticmethod(lambda p: ["fila corrupta simulada"])
        try:
            bad_t = os.path.join(tmp, "malo.db")
            check(raises(RuntimeError, lambda: s.backup_to(bad_t)), "integrity_check con problemas -> RuntimeError")
        finally:
            SQLiteStore._integrity_problems = staticmethod(orig)
        check(not os.path.exists(os.path.join(tmp, "malo.db")) and names(tmp) == n1, "y no deja ni destino ni temporales")

        # cadena alterada en el ORIGEN (se evade el trigger como hace test_audit.py)
        tsrc = os.path.join(tmp, "alterada.db")
        t, ta = filled(tsrc, 20)
        t._conn.execute("DROP TRIGGER trg_audit_log_no_update")
        t._conn.execute("UPDATE audit_log SET payload = '{\"i\": 999}' WHERE seq = 10")
        n2 = names(tmp)
        check(raises(RuntimeError, lambda: t.backup_to(os.path.join(tmp, "alt-copia.db"))), "cadena alterada (CORRUPTED) -> RuntimeError por defecto")
        n3 = [x for x in names(tmp) if x not in n2]
        check(n3 == [], f"y no deja destino ni temporales: {n3}")
        r2 = t.backup_to(os.path.join(tmp, "alt-copia.db"), verify_chain=False)
        check(r2.chain_verified is False and r2.audit_entries == 20 and os.path.exists(os.path.join(tmp, "alt-copia.db")),
              "verify_chain=False respalda igualmente una base alterada y lo declara (chain_verified=False)")
        e = SQLiteStore(os.path.join(tmp, "alt-copia.db"))
        check(AuditChain(e).verify().status.name == "CORRUPTED", "la copia conserva la alteración (es prueba, no se 'arregla')")
        e.close()
        t.close()

        # --- 6) escritores concurrentes ---------------------------------------------------------------------------------------------------
        csrc = os.path.join(tmp, "conc.db")
        cs, _ = filled(csrc, 2000)
        stop = threading.Event()
        errors: list[str] = []
        written = [0]

        def writer():
            w = SQLiteStore(csrc)
            wa = AuditChain(w)
            try:
                while not stop.is_set():
                    wa.append({"w": written[0]})
                    written[0] += 1
            except Exception as ex:  # noqa: BLE001 - se informa en la prueba
                errors.append(repr(ex))
            finally:
                w.close()

        th = threading.Thread(target=writer)
        th.start()
        results = []
        try:
            for i in range(5):
                results.append(cs.backup_to(os.path.join(tmp, f"conc-{i}.db")))
        finally:
            stop.set()
            th.join()
        check(errors == [], f"el escritor concurrente no falló durante los backups: {errors}")
        check(all(x.chain_verified and x.audit_entries >= 2000 for x in results), "las 5 copias hechas con escritura concurrente son VALID")
        counts = [x.audit_entries for x in results]
        check(counts == sorted(counts) and counts[-1] <= cs.count_audit_entries(), f"cada copia es una instantánea coherente y no supera al origen: {counts}")
        check(not [n for n in names(tmp) if n.endswith(".tmp")], "sin temporales tras los backups concurrentes")

        # --- 9) el origen sigue usable ----------------------------------------------------------------------------------------------------
        before_n = cs.count_audit_entries()
        AuditChain(cs).append({"tras": "backup"})
        check(cs.count_audit_entries() == before_n + 1 and not cs._conn.in_transaction, "el origen sigue admitiendo escrituras tras el backup")
        cs.close()
        s.close()

        # base en memoria
        m = SQLiteStore(":memory:")
        AuditChain(m).append({"m": 1})
        rm = m.backup_to(os.path.join(tmp, "mem.db"))
        check(rm.audit_entries == 1 and rm.chain_verified, "una base en memoria también se respalda")
        m.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{'TODO OK' if not FAILURES else str(len(FAILURES)) + ' FALLOS'}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
