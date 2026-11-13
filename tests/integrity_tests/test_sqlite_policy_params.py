"""Política de conexión PARAMETRIZABLE en los cuatro almacenes (D-74, parche 0022 y g0.8 r29).

`busy_timeout_ms` (DTOS desde D-57) y `journal_size_limit_bytes` (DTOS desde D-73) ya se podían configurar en DTOS; ahora también en UNAE, el adaptador y la
capa proactiva, solo por nombre y con la misma validación. Cada almacén conserva su propio código (D-63): aquí se vigila el COMPORTAMIENTO común.

Se mide con las conexiones reales de cada almacén (`Store.connect()`, `UNAEDTOSAdapter._connect()`, `ShadowLog._connect()`, `SQLiteStore._conn`).
Incluye el efecto del recorte del WAL con CONTROL NEGATIVO (-1) y la comprobación de que el valor se aplica en cada conexión por operación.
"""
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from adapter import UNAEDTOSAdapter
from dtos.store import SQLiteStore
from proactive.shadow import ShadowLog
from unae import Store

LIMIT = 256 * 1024

# D-79: carga del test de WAL. Cada fila ocupa una página de 4096 B en el WAL; el umbral del test es 8 * LIMIT (2 MiB).
# Completo = 2500 filas (~10 MB por almacén, la carga de siempre). Rápido (AEK_TESTS_FAST=1 o `run_tests.py --fast`) = 1024 filas (~4,2 MB, 2x el umbral).
PAD_BYTES = 4000
FILAS_COMPLETO = 2500
FILAS_RAPIDO = 1024
RAPIDO = os.environ.get("AEK_TESTS_FAST") == "1"
FILAS = FILAS_RAPIDO if RAPIDO else FILAS_COMPLETO


class Fabrica:
    """Cómo abrir cada almacén y obtener una conexión REAL suya."""
    def __init__(self, nombre, abrir, conexion):
        self.nombre, self.abrir, self.conexion = nombre, abrir, conexion


FABRICAS = (
    Fabrica("unae", lambda p, **kw: Store(str(p), **kw), lambda s: s.connect()),
    Fabrica("dtos", lambda p, **kw: SQLiteStore(p, **kw), lambda s: sqlite3.connect(s.db_path, isolation_level=None)),  # su conexión se mide aparte
    Fabrica("adapter", lambda p, **kw: UNAEDTOSAdapter(p, **kw), lambda s: s._connect()),
    Fabrica("proactive", lambda p, **kw: ShadowLog(p, **kw), lambda s: s._connect()),
)
PARAMETRIZADOS = tuple(f for f in FABRICAS if f.nombre != "dtos")


def pragma(conn, name):
    return conn.execute(f"PRAGMA {name}").fetchone()[0]


def cerrar(s):
    if hasattr(s, "close"):
        s.close()


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.d = Path(self._tmp.name)


class Parametros(Base):
    def test_explicit_values_reach_every_connection(self):
        for f in PARAMETRIZADOS:
            with self.subTest(f.nombre):
                s = f.abrir(self.d / f"{f.nombre}.sqlite", busy_timeout_ms=1234, journal_size_limit_bytes=4096)
                self.addCleanup(cerrar, s)
                self.assertEqual((s.busy_timeout_ms, s.journal_size_limit_bytes), (1234, 4096))
                with closing(f.conexion(s)) as c:
                    self.assertEqual(pragma(c, "busy_timeout"), 1234)
                    self.assertEqual(pragma(c, "journal_size_limit"), 4096)

    def test_unlimited_and_zero_are_accepted(self):
        for f in PARAMETRIZADOS:
            for v in (-1, 0):
                with self.subTest(f.nombre, v=v):
                    s = f.abrir(self.d / f"{f.nombre}{v}.sqlite", journal_size_limit_bytes=v, busy_timeout_ms=0)
                    self.addCleanup(cerrar, s)
                    with closing(f.conexion(s)) as c:
                        self.assertEqual(pragma(c, "journal_size_limit"), v)
                        self.assertEqual(pragma(c, "busy_timeout"), 0)

    def test_invalid_values_are_rejected_before_creating_anything(self):
        for f in PARAMETRIZADOS:
            for kw in ({"busy_timeout_ms": True}, {"busy_timeout_ms": -1}, {"busy_timeout_ms": "10"}, {"busy_timeout_ms": 1.5}, {"busy_timeout_ms": None},
                       {"journal_size_limit_bytes": True}, {"journal_size_limit_bytes": -2}, {"journal_size_limit_bytes": "64"},
                       {"journal_size_limit_bytes": 1.5}, {"journal_size_limit_bytes": None}, {"journal_size_limit_bytes": 2 ** 63}):
                with self.subTest(f.nombre, **kw):
                    target = self.d / "nuevo" / f"{f.nombre}.sqlite"
                    with self.assertRaises(ValueError):
                        f.abrir(target, **kw)
                    self.assertFalse(target.parent.exists(), "un valor inválido no debe crear ni el directorio")

    def test_policy_parameters_are_keyword_only(self):
        with self.assertRaises(TypeError):
            UNAEDTOSAdapter(self.d / "a.sqlite", 5000)
        with self.assertRaises(TypeError):
            ShadowLog(self.d / "s.sqlite", lambda: 0.0, 5000)
        with self.assertRaises(TypeError):
            Store(str(self.d / "u.sqlite"), None, False, None, "FULL", False, 5000)

    def test_defaults_are_unchanged(self):
        for f in PARAMETRIZADOS:
            with self.subTest(f.nombre):
                s = f.abrir(self.d / f"{f.nombre}.sqlite")
                self.addCleanup(cerrar, s)
                self.assertEqual((s.busy_timeout_ms, s.journal_size_limit_bytes), (10000, 64 * 1024 * 1024))


class EfectoSobreElWal(Base):
    """Con un lector abierto el WAL crece; al soltarlo y reiniciarse el WAL, con límite se recorta y con -1 se queda grande."""

    def _wal_tras_ciclo(self, f, limit):
        s = f.abrir(self.d / f"{f.nombre}{limit}.sqlite", journal_size_limit_bytes=limit)
        self.addCleanup(cerrar, s)
        path = s.path if hasattr(s, "path") else s.db_path
        reader = sqlite3.connect(path, isolation_level=None)
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM sqlite_master").fetchall()
        with closing(f.conexion(s)) as w:
            w.execute("CREATE TABLE IF NOT EXISTS aux_wal(x)")
            pad = "x" * PAD_BYTES
            w.execute("BEGIN")  # una sola transacción: 2500 commits con fsync cada uno tardaban ~1 min por prueba en Windows; el WAL crece igual (~10 MB)
            for _ in range(FILAS):
                w.execute("INSERT INTO aux_wal VALUES(?)", (pad,))
            w.execute("COMMIT")
            durante = os.path.getsize(path + "-wal")
            reader.execute("COMMIT")
            reader.close()
            w.execute("PRAGMA wal_checkpoint(PASSIVE)")
            w.execute("INSERT INTO aux_wal VALUES('reinicia')")  # la siguiente escritura reinicia el WAL
            despues = os.path.getsize(path + "-wal")
        return durante, despues

    def test_limit_trims_the_wal_after_checkpoint_in_every_store(self):
        for f in PARAMETRIZADOS:
            with self.subTest(f.nombre):
                durante, despues = self._wal_tras_ciclo(f, LIMIT)
                self.assertGreater(durante, 8 * LIMIT, "con un lector abierto el WAL supera el límite (el PRAGMA no lo acota)")
                self.assertLessEqual(despues, 2 * LIMIT, f"{f.nombre}: el WAL debía recortarse a ~{LIMIT} bytes y mide {despues}")

    def test_negative_control_without_limit_the_wal_stays_large(self):
        for f in PARAMETRIZADOS:
            with self.subTest(f.nombre):
                _, despues = self._wal_tras_ciclo(f, -1)
                self.assertGreater(despues, 8 * LIMIT, f"{f.nombre}: con -1 el WAL debe quedarse grande (control negativo)")


class CargaDelModoRapido(unittest.TestCase):
    """D-79: el modo rápido solo reduce la carga; no puede quedarse sin margen sobre el umbral que mide `EfectoSobreElWal`."""

    def test_fast_load_keeps_a_2x_margin_over_the_wal_threshold(self):
        self.assertGreaterEqual(FILAS_RAPIDO * 4096, 2 * 8 * LIMIT, "con tan pocas filas el WAL no supera 8 * LIMIT con margen")

    def test_full_load_is_not_smaller_than_the_fast_one(self):
        self.assertGreater(FILAS_COMPLETO, FILAS_RAPIDO)


if __name__ == "__main__":
    unittest.main()
