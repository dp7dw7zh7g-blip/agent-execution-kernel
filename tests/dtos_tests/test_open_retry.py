"""
test_open_retry.py (parche 0016, D-67)
======================================
Hallazgo de la ejecución en Windows 10 (Python 3.15.2) de g0.8 r16/r17: con 8 procesos abriendo a la vez una base NUEVA, uno falló con
«database is locked» a los 4 ms; es decir, SQLite devolvió BUSY SIN esperar el busy_timeout (la apertura tardaba de 4 a 10 s por proceso).
`SQLiteStore.__init__` ahora reintenta la configuración y la creación del esquema ante un bloqueo temporal, con pausas crecientes y jitter,
hasta agotar `busy_timeout_ms` en total.

Estas pruebas simulan el BUSY inmediato (en Linux la carrera real casi no se da) y comprueban el comportamiento del reintento; la carrera
real la cubre la sección C de test_concurrency.py. Sin el parche 0016 estas pruebas FALLAN (no existe `_is_lock_error` ni hay reintento).

Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import contextlib
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from dtos import store as store_mod
from dtos.store import SQLiteStore


class OpenRetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)  # registrado primero = se ejecuta al final, tras cerrar las conexiones (Windows no borra archivos abiertos)
        self.db = str(Path(self.tmp.name) / "a.db")
        self.conns: list[sqlite3.Connection] = []
        real_connect = sqlite3.connect

        def spy(*args, **kwargs):
            conn = real_connect(*args, **kwargs)
            self.conns.append(conn)
            return conn

        patcher = mock.patch.object(store_mod.sqlite3, "connect", spy)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self._close_all)

    def _close_all(self):
        for conn in self.conns:
            with contextlib.suppress(sqlite3.Error):
                conn.close()

    def make_failing(self, method: str, times: int, exc: BaseException | None = None):  # NO llamarlo `fail`: pisaría TestCase.fail (g0.8 r19)
        """Hace que SQLiteStore.<method> lance `exc` las primeras `times` veces y luego funcione. Devuelve (contexto, contador)."""
        original = getattr(SQLiteStore, method)
        calls = {"n": 0}

        def wrapper(inst):
            calls["n"] += 1
            if calls["n"] <= times:
                raise exc if exc is not None else sqlite3.OperationalError("database is locked")
            return original(inst)

        return mock.patch.object(SQLiteStore, method, wrapper), calls

    def open_with_fake_sleep(self, method: str, times: int, **kwargs):
        sleeps: list[float] = []
        ctx, calls = self.make_failing(method, times)
        with ctx, mock.patch.object(store_mod.time, "sleep", side_effect=sleeps.append):
            store = SQLiteStore(self.db, **kwargs)
        self.addCleanup(store.close)
        return store, calls, sleeps

    def test_helpers_do_not_shadow_the_unittest_api(self):
        self.assertIs(type(self).fail, unittest.TestCase.fail)  # regresión de r19: un método `fail` propio ocultaba el mensaje de los assert fallidos

    # -- reintento ante un bloqueo temporal -------------------------------------------------

    def test_transient_lock_while_creating_the_schema_is_retried(self):
        store, calls, sleeps = self.open_with_fake_sleep("_init_schema", 3)
        self.assertEqual(calls["n"], 4)
        self.assertEqual(len(sleeps), 3)
        self.assertTrue(all(0 < s <= store_mod.OPEN_RETRY_MAX_S for s in sleeps), sleeps)
        self.assertEqual(store._conn.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal")
        self.assertGreater(store._conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0], 10)

    def test_transient_lock_while_configuring_the_connection_is_retried(self):
        store, calls, sleeps = self.open_with_fake_sleep("_configure_connection", 2)
        self.assertEqual(calls["n"], 3)
        self.assertEqual(len(sleeps), 2)
        self.assertEqual(store._conn.execute("PRAGMA busy_timeout").fetchone()[0], store.busy_timeout_ms)

    def test_pauses_grow_and_are_capped(self):
        _, calls, sleeps = self.open_with_fake_sleep("_init_schema", 8)
        self.assertEqual(calls["n"], 9)
        self.assertLessEqual(max(sleeps), store_mod.OPEN_RETRY_MAX_S)
        self.assertLessEqual(sleeps[0], store_mod.OPEN_RETRY_FIRST_S)
        self.assertGreaterEqual(sleeps[-1], store_mod.OPEN_RETRY_MAX_S / 2)  # con jitter: entre el 50 % y el 100 % del tope
        self.assertGreater(sleeps[-1], sleeps[0])

    # -- límites del reintento --------------------------------------------------------------

    def test_a_permanent_lock_gives_up_at_the_configured_deadline_and_closes_the_connection(self):
        # Se simula el bloqueo en _configure_connection (no hace I/O): así el plazo de 200 ms no depende de cuánto tarde el disco en la primera
        # apertura real. Con _init_schema (r19) un disco lento (Windows 10, commit de ~45 ms) agotaba el plazo antes del primer fallo y no había reintento.
        ctx, calls = self.make_failing("_configure_connection", 10**9)
        t0 = time.monotonic()
        with ctx, self.assertRaises(sqlite3.OperationalError) as cm:
            SQLiteStore(self.db, busy_timeout_ms=200)
        elapsed = time.monotonic() - t0
        self.assertIn("locked", str(cm.exception).lower())
        self.assertGreaterEqual(calls["n"], 2, "no hubo reintentos")
        self.assertTrue(0.15 <= elapsed < 3.0, f"el error debe llegar al agotarse el plazo CONFIGURADO (200 ms): {elapsed:.2f} s")
        with self.assertRaises(sqlite3.ProgrammingError):  # la conexión de la apertura fallida quedó cerrada
            self.conns[-1].execute("SELECT 1")

    def test_zero_timeout_never_retries(self):
        ctx, calls = self.make_failing("_configure_connection", 10**9)
        with ctx, self.assertRaises(sqlite3.OperationalError):
            SQLiteStore(self.db, busy_timeout_ms=0)
        self.assertEqual(calls["n"], 1)

    def test_errors_that_are_not_locks_are_not_retried(self):
        for exc in (sqlite3.OperationalError("no such table: x"), sqlite3.DatabaseError("file is not a database"), RuntimeError("journal_mode")):
            with self.subTest(exc=repr(exc)):
                ctx, calls = self.make_failing("_init_schema", 10**9, exc)
                with ctx, self.assertRaises(type(exc)):
                    SQLiteStore(self.db)
                self.assertEqual(calls["n"], 1)

    # -- bloqueos reales de SQLite ----------------------------------------------------------

    def test_is_lock_error_recognises_real_sqlite_errors(self):
        holder = sqlite3.connect(self.db, isolation_level=None)
        holder.execute("BEGIN EXCLUSIVE")
        other = sqlite3.connect(self.db, timeout=0, isolation_level=None)
        with self.assertRaises(sqlite3.OperationalError) as locked:
            other.execute("BEGIN IMMEDIATE")
        holder.execute("ROLLBACK")
        with self.assertRaises(sqlite3.OperationalError) as missing:
            other.execute("SELECT * FROM tabla_que_no_existe")
        self.assertTrue(store_mod._is_lock_error(locked.exception))
        self.assertFalse(store_mod._is_lock_error(missing.exception))
        self.assertTrue(store_mod._is_lock_error(sqlite3.OperationalError("database table is locked")))
        self.assertFalse(store_mod._is_lock_error(sqlite3.OperationalError("disk I/O error")))
        self.assertFalse(store_mod._is_lock_error(RuntimeError("database is locked")))

    def test_a_real_temporary_lock_on_a_new_database_is_waited_out(self):
        holder = sqlite3.connect(self.db, isolation_level=None, check_same_thread=False)
        holder.execute("BEGIN EXCLUSIVE")
        timer = threading.Timer(0.3, lambda: holder.execute("ROLLBACK"))
        timer.start()
        self.addCleanup(timer.join)
        t0 = time.monotonic()
        store = SQLiteStore(self.db, busy_timeout_ms=5000)
        self.addCleanup(store.close)
        self.assertGreaterEqual(time.monotonic() - t0, 0.2, "la apertura no esperó a que se liberara el bloqueo")
        self.assertEqual(store._conn.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal")


def main() -> int:
    r = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(OpenRetryTests))
    return 0 if r.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
