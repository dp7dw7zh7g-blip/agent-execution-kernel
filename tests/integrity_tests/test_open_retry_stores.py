"""Reintento acotado de la APERTURA en UNAE, el adaptador y la capa proactiva (D-76, parche 0024 y g0.8 r31).

DTOS lo tiene desde D-67 (tests/dtos_tests/test_open_retry.py). SQLite puede devolver «database is locked» SIN esperar el busy_timeout cuando esperar podría causar
un bloqueo mutuo; los otros tres almacenes ahora repiten TODA la apertura ante un bloqueo temporal, con pausas crecientes y jitter, hasta agotar `busy_timeout_ms` en total.
Cada almacén conserva su propio código (D-63): aquí se vigila el COMPORTAMIENTO común.

Como en DTOS, el BUSY inmediato se SIMULA (medido en r30: en Linux, con un lector abierto, SQLite espera el busy_timeout en vez de fallar al instante, así que no se puede
provocar de forma fiable); además hay una prueba con un bloqueo real y temporal, y otra con apertura simultánea real.
Contra el código de r30 (sin `_open_with_retry` ni `_is_lock_error`) estas pruebas FALLAN.
"""
import sqlite3
from contextlib import closing
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from adapter import UNAEDTOSAdapter
from adapter import adapter as adapter_module
from dtos import store as dtos_module
from proactive import shadow as shadow_module
from proactive.shadow import ShadowLog
from unae import Store
from unae import store as unae_module


class Fabrica:
    """`once`: el método que hace UN intento de apertura (el que el reintento repite)."""
    def __init__(self, nombre, modulo, clase, once, abrir):
        self.nombre, self.modulo, self.clase, self.once, self.abrir = nombre, modulo, clase, once, abrir


FABRICAS = (
    Fabrica("unae", unae_module, Store, "_initialize", lambda p, **kw: Store(str(p), **kw)),
    Fabrica("adapter", adapter_module, UNAEDTOSAdapter, "_open_once", lambda p, **kw: UNAEDTOSAdapter(p, **kw)),
    Fabrica("proactive", shadow_module, ShadowLog, "_open_once", lambda p, **kw: ShadowLog(p, **kw)),
)


def cerrar(s):
    if hasattr(s, "close"):
        s.close()


def tablas(path):
    # D-78: `with sqlite3.connect(...)` confirma pero NO cierra; Windows no podría borrar el directorio temporal. `closing` cierra; el segundo `c` confirma.
    with closing(sqlite3.connect(str(path))) as c, c:
        return c.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0]


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)  # primero registrado = último en ejecutarse (Windows no borra archivos abiertos)
        self.d = Path(self._tmp.name)

    def fallar(self, f, veces, exc=None):
        """Hace que `f.clase.<once>` lance `exc` las primeras `veces` veces y luego funcione. Devuelve (contexto, contador)."""
        original = getattr(f.clase, f.once)
        calls = {"n": 0}

        def wrapper(inst):
            calls["n"] += 1
            if calls["n"] <= veces:
                raise exc if exc is not None else sqlite3.OperationalError("database is locked")
            return original(inst)

        return mock.patch.object(f.clase, f.once, wrapper), calls

    def abrir_sin_dormir(self, f, veces, **kw):
        sleeps = []
        ctx, calls = self.fallar(f, veces)
        with ctx, mock.patch.object(f.modulo.time, "sleep", side_effect=sleeps.append):
            s = f.abrir(self.d / f"{f.nombre}.sqlite", **kw)
        self.addCleanup(cerrar, s)
        return s, calls, sleeps


class ReintentoAnteBloqueoTemporal(Base):
    def test_transient_lock_is_retried_and_the_store_ends_up_usable(self):
        for f in FABRICAS:
            with self.subTest(f.nombre):
                s, calls, sleeps = self.abrir_sin_dormir(f, 3)
                self.assertEqual(calls["n"], 4)
                self.assertEqual(len(sleeps), 3)
                self.assertTrue(all(0 < x <= f.modulo.OPEN_RETRY_MAX_S for x in sleeps), sleeps)
                self.assertGreater(tablas(self.d / f"{f.nombre}.sqlite"), 3, "tras reintentar, el esquema debe estar creado")
                with closing(sqlite3.connect(str(self.d / f"{f.nombre}.sqlite"))) as c, c:
                    self.assertEqual(c.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal")

    def test_pauses_grow_and_are_capped(self):
        for f in FABRICAS:
            with self.subTest(f.nombre):
                _, calls, sleeps = self.abrir_sin_dormir(f, 8)
                self.assertEqual(calls["n"], 9)
                self.assertLessEqual(max(sleeps), f.modulo.OPEN_RETRY_MAX_S)
                self.assertLessEqual(sleeps[0], f.modulo.OPEN_RETRY_FIRST_S)
                self.assertGreaterEqual(sleeps[-1], f.modulo.OPEN_RETRY_MAX_S / 2)  # con jitter: entre el 50 % y el 100 % del tope
                self.assertGreater(sleeps[-1], sleeps[0])


class LimitesDelReintento(Base):
    def test_a_permanent_lock_gives_up_at_the_configured_deadline(self):
        for f in FABRICAS:
            with self.subTest(f.nombre):
                ctx, calls = self.fallar(f, 10 ** 9)
                t0 = time.monotonic()
                with ctx, self.assertRaises(sqlite3.OperationalError) as cm:
                    f.abrir(self.d / f"{f.nombre}-perm.sqlite", busy_timeout_ms=200)
                elapsed = time.monotonic() - t0
                self.assertIn("locked", str(cm.exception).lower())
                self.assertGreaterEqual(calls["n"], 2, "no hubo reintentos")
                self.assertTrue(0.15 <= elapsed < 3.0, f"el error debe llegar al agotarse el plazo CONFIGURADO (200 ms): {elapsed:.2f} s")

    def test_zero_timeout_never_retries(self):
        """`busy_timeout_ms=0` significa «sin espera»: el reintento está acotado por ese plazo (igual que en DTOS, D-67), así que no reintenta."""
        for f in FABRICAS:
            with self.subTest(f.nombre):
                ctx, calls = self.fallar(f, 10 ** 9)
                with ctx, self.assertRaises(sqlite3.OperationalError):
                    f.abrir(self.d / f"{f.nombre}-cero.sqlite", busy_timeout_ms=0)
                self.assertEqual(calls["n"], 1)

    def test_errors_that_are_not_locks_are_not_retried(self):
        for f in FABRICAS:
            for exc in (sqlite3.OperationalError("no such table: x"), sqlite3.DatabaseError("file is not a database"), RuntimeError("journal_mode")):
                with self.subTest(f.nombre, exc=repr(exc)):
                    ctx, calls = self.fallar(f, 10 ** 9, exc)
                    with ctx, self.assertRaises(type(exc)):
                        f.abrir(self.d / f"{f.nombre}-nolock.sqlite")
                    self.assertEqual(calls["n"], 1)

    def test_validation_errors_happen_before_any_attempt(self):
        for f in FABRICAS:
            with self.subTest(f.nombre):
                ctx, calls = self.fallar(f, 0)
                with ctx, self.assertRaises(ValueError):
                    f.abrir(self.d / "nuevo" / f"{f.nombre}.sqlite", busy_timeout_ms=-1)
                self.assertEqual(calls["n"], 0)


class BloqueosReales(Base):
    def test_is_lock_error_recognises_real_sqlite_errors(self):
        db = self.d / "real.sqlite"
        holder = sqlite3.connect(str(db), isolation_level=None)
        self.addCleanup(holder.close)
        holder.execute("BEGIN EXCLUSIVE")
        other = sqlite3.connect(str(db), timeout=0, isolation_level=None)
        self.addCleanup(other.close)
        with self.assertRaises(sqlite3.OperationalError) as locked:
            other.execute("BEGIN IMMEDIATE")
        holder.execute("ROLLBACK")
        with self.assertRaises(sqlite3.OperationalError) as missing:
            other.execute("SELECT * FROM tabla_que_no_existe")
        for f in FABRICAS:
            with self.subTest(f.nombre):
                self.assertTrue(f.modulo._is_lock_error(locked.exception))
                self.assertFalse(f.modulo._is_lock_error(missing.exception))
                self.assertTrue(f.modulo._is_lock_error(sqlite3.OperationalError("database table is locked")))
                self.assertFalse(f.modulo._is_lock_error(sqlite3.OperationalError("disk I/O error")))
                self.assertFalse(f.modulo._is_lock_error(RuntimeError("database is locked")))

    def test_a_real_temporary_lock_on_a_new_database_is_waited_out(self):
        for f in FABRICAS:
            with self.subTest(f.nombre):
                db = self.d / f"{f.nombre}-temp.sqlite"
                holder = sqlite3.connect(str(db), isolation_level=None, check_same_thread=False)
                holder.execute("BEGIN EXCLUSIVE")
                timer = threading.Timer(0.3, lambda h=holder: h.execute("ROLLBACK"))
                timer.start()
                t0 = time.monotonic()
                s = f.abrir(db, busy_timeout_ms=5000)
                timer.join()
                holder.close()
                self.addCleanup(cerrar, s)
                self.assertGreaterEqual(time.monotonic() - t0, 0.2, "la apertura no esperó a que se liberara el bloqueo")
                self.assertGreater(tablas(db), 3)

    def test_simultaneous_real_opening_of_a_new_database_does_not_fail(self):
        n = 8
        for f in FABRICAS:
            with self.subTest(f.nombre):
                db = self.d / f"{f.nombre}-race.sqlite"
                barrier, errores, abiertos = threading.Barrier(n), [], []

                def abrir():
                    barrier.wait()
                    try:
                        abiertos.append(f.abrir(db, busy_timeout_ms=10000))
                    except Exception as exc:  # noqa: BLE001 - se informa abajo
                        errores.append(f"{type(exc).__name__}: {exc}")

                hilos = [threading.Thread(target=abrir) for _ in range(n)]
                [h.start() for h in hilos]
                [h.join() for h in hilos]
                for s in abiertos:
                    cerrar(s)
                self.assertEqual(errores, [])
                self.assertEqual(len(abiertos), n)


class ParidadConDtos(unittest.TestCase):
    """D-63: sin código común; el mismo comportamiento y las mismas constantes que DTOS (D-67)."""

    def test_same_policy_constants_as_dtos(self):
        esperado = (dtos_module.OPEN_RETRY_FIRST_S, dtos_module.OPEN_RETRY_MAX_S)
        for f in FABRICAS:
            with self.subTest(f.nombre):
                self.assertEqual((f.modulo.OPEN_RETRY_FIRST_S, f.modulo.OPEN_RETRY_MAX_S), esperado)

    def test_same_lock_verdict_as_dtos(self):
        casos = (sqlite3.OperationalError("database is locked"), sqlite3.OperationalError("database table is locked"), sqlite3.OperationalError("disk I/O error"),
                 sqlite3.DatabaseError("database is locked"), RuntimeError("database is locked"), ValueError("x"))
        for f in FABRICAS:
            for exc in casos:
                with self.subTest(f.nombre, exc=repr(exc)):
                    self.assertEqual(f.modulo._is_lock_error(exc), dtos_module._is_lock_error(exc))


if __name__ == "__main__":
    unittest.main()
