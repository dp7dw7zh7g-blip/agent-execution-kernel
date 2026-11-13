"""Límites y normalización de la política de conexión en los cuatro almacenes (D-75, parche 0023 y g0.8 r30).

Dos defectos de la auditoría de SQLite de g0.8 r29, corregidos con el mismo comportamiento en UNAE, DTOS, el adaptador y la capa proactiva
(cada almacén conserva su propia copia del código, D-63; aquí se vigila el COMPORTAMIENTO común):

1. `sqlite3_busy_timeout` recibe un `int` de C (32 bits con signo). `busy_timeout_ms >= 2**31` pasaba la validación, SQLite quedaba en 0 ms y el
   `RuntimeError` llegaba DESPUÉS de haber creado el archivo de base de datos. Ahora el tope es 2**31 - 1 y el rechazo (`ValueError`) ocurre antes de crear nada.
2. `isinstance(x, int)` deja pasar subclases de `int` que pueden redefinir `__format__`, `__int__` o las comparaciones, y el valor se interpola en una
   sentencia PRAGMA (no admite parámetros). Ahora la validación devuelve `int` EXACTOS y los almacenes solo usan esos.
"""
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from adapter import UNAEDTOSAdapter
from adapter import adapter as adapter_module
from dtos import store as dtos_module
from dtos.store import SQLiteStore
from proactive import shadow as shadow_module
from proactive.shadow import ShadowLog
from unae import Store
from unae import store as unae_module

MAX_BUSY = 2 ** 31 - 1


class Fabrica:
    def __init__(self, nombre, abrir, conexion, modulo):
        self.nombre, self.abrir, self.conexion, self.modulo = nombre, abrir, conexion, modulo


FABRICAS = (
    Fabrica("unae", lambda p, **kw: Store(str(p), **kw), lambda s: s.connect(), unae_module),
    Fabrica("dtos", lambda p, **kw: SQLiteStore(p, **kw), lambda s: s._conn, dtos_module),
    Fabrica("adapter", lambda p, **kw: UNAEDTOSAdapter(p, **kw), lambda s: s._connect(), adapter_module),
    Fabrica("proactive", lambda p, **kw: ShadowLog(p, **kw), lambda s: s._connect(), shadow_module),
)


def pragma(conn, name):
    return conn.execute(f"PRAGMA {name}").fetchone()[0]


def cerrar(s):
    if hasattr(s, "close"):
        s.close()


class Hostil(int):
    """Subclase de `int` con TODO redefinido: si algún almacén usara el objeto recibido en lugar del valor normalizado, estas pruebas lo delatarían."""
    def __format__(self, spec): return "1; DROP TABLE x"
    def __str__(self): return "1; DROP TABLE x"
    def __repr__(self): return "Hostil(…)"
    def __int__(self): return 999
    def __index__(self): return 999
    def __float__(self): return 999.0


class Mentirosa(int):
    """Valor FUERA de rango cuyas comparaciones dicen que está dentro."""
    def __lt__(self, o): return False
    def __gt__(self, o): return False
    def __le__(self, o): return True
    def __ge__(self, o): return True


class Tacana(int):
    """Valor DENTRO de rango cuyas comparaciones dicen que está fuera."""
    def __lt__(self, o): return True
    def __gt__(self, o): return True
    def __le__(self, o): return False
    def __ge__(self, o): return False


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.d = Path(self._tmp.name)


class TopeDeBusyTimeout(Base):
    def test_upper_bound_is_accepted_and_reaches_the_connection(self):
        for f in FABRICAS:
            with self.subTest(f.nombre):
                s = f.abrir(self.d / f"{f.nombre}.sqlite", busy_timeout_ms=MAX_BUSY)
                self.addCleanup(cerrar, s)
                self.assertEqual(s.busy_timeout_ms, MAX_BUSY)
                conn = f.conexion(s)
                try:
                    self.assertEqual(pragma(conn, "busy_timeout"), MAX_BUSY)
                finally:
                    if f.nombre != "dtos":
                        conn.close()

    def test_values_above_the_bound_are_rejected_before_creating_anything(self):
        for f in FABRICAS:
            for v in (2 ** 31, 2 ** 31 + 1, 2 ** 40, 2 ** 63, 10 ** 30):
                with self.subTest(f.nombre, v=v):
                    target = self.d / "nuevo" / f"{f.nombre}.sqlite"
                    with self.assertRaises(ValueError) as cm:
                        f.abrir(target, busy_timeout_ms=v)
                    self.assertIn(str(MAX_BUSY), str(cm.exception), "el mensaje debe decir el máximo permitido")
                    self.assertFalse(target.parent.exists(), "un valor fuera de rango no debe crear ni el directorio")

    def test_over_the_bound_does_not_leave_a_database_in_an_existing_directory(self):
        """Antes de D-75 el RuntimeError llegaba con el archivo YA creado; ahora el directorio queda vacío."""
        for f in FABRICAS:
            with self.subTest(f.nombre):
                d = self.d / f"vacio-{f.nombre}"
                d.mkdir()
                with self.assertRaises(ValueError):
                    f.abrir(d / "x.sqlite", busy_timeout_ms=2 ** 31)
                self.assertEqual(list(d.iterdir()), [])

    def test_motivation_sqlite_overflows_above_the_bound(self):
        """Hecho que justifica el tope (medido en SQLite 3.45.1: queda en 0 ms). Si otra versión no desborda, se omite: el tope sigue siendo el de la API de C."""
        with closing(sqlite3.connect(":memory:")) as c:
            c.execute(f"PRAGMA busy_timeout={2 ** 31}")
            if pragma(c, "busy_timeout") == 2 ** 31:
                self.skipTest(f"SQLite {sqlite3.sqlite_version} no desborda con 2**31; revisar si el tope sigue haciendo falta")
            self.assertNotEqual(pragma(c, "busy_timeout"), 2 ** 31)


class NormalizacionAIntExacto(Base):
    def test_validators_return_exact_ints(self):
        for f in FABRICAS:
            with self.subTest(f.nombre):
                busy, limit = f.modulo._validate_connection_policy(5000, 4096)
                self.assertEqual((busy, limit), (5000, 4096))
                self.assertIs(type(busy), int)
                self.assertIs(type(limit), int)

    def test_int_subclass_is_replaced_by_its_numeric_value_in_every_store(self):
        """Con `__int__`, `__index__`, `__format__` y `__str__` hostiles, el almacén usa el valor numérico REAL (no 999 ni el texto inyectado)."""
        for f in FABRICAS:
            with self.subTest(f.nombre):
                s = f.abrir(self.d / f"{f.nombre}.sqlite", busy_timeout_ms=Hostil(1234), journal_size_limit_bytes=Hostil(4096))
                self.addCleanup(cerrar, s)
                self.assertIs(type(s.busy_timeout_ms), int)
                self.assertIs(type(s.journal_size_limit_bytes), int)
                self.assertEqual((s.busy_timeout_ms, s.journal_size_limit_bytes), (1234, 4096))
                conn = f.conexion(s)
                try:
                    self.assertEqual(pragma(conn, "busy_timeout"), 1234)
                    self.assertEqual(pragma(conn, "journal_size_limit"), 4096)
                finally:
                    if f.nombre != "dtos":
                        conn.close()

    def test_range_is_checked_on_the_real_value_not_on_overridden_comparisons(self):
        for f in FABRICAS:
            casos = (
                ("fuera de rango con comparaciones que mienten (busy)", {"busy_timeout_ms": Mentirosa(2 ** 31)}, False),
                ("negativo con comparaciones que mienten (busy)", {"busy_timeout_ms": Mentirosa(-1)}, False),
                ("fuera de rango con comparaciones que mienten (limit)", {"journal_size_limit_bytes": Mentirosa(-2)}, False),
                ("en rango con comparaciones que niegan (busy)", {"busy_timeout_ms": Tacana(77)}, True),
                ("en rango con comparaciones que niegan (limit)", {"journal_size_limit_bytes": Tacana(0)}, True),
            )
            for n, (etiqueta, kw, valido) in enumerate(casos):
                with self.subTest(f.nombre, caso=etiqueta):
                    target = self.d / f"{f.nombre}-c{n}" / "x.sqlite"
                    if valido:
                        target.parent.mkdir()  # DTOS no crea directorios
                        s = f.abrir(target, **kw)
                        self.addCleanup(cerrar, s)
                        for k, v in kw.items():
                            self.assertEqual(getattr(s, k), int(v))
                            self.assertIs(type(getattr(s, k)), int)
                    else:
                        with self.assertRaises(ValueError):
                            f.abrir(target, **kw)
                        self.assertFalse(target.parent.exists())

    def test_bool_and_non_int_are_still_rejected(self):
        for f in FABRICAS:
            for kw in ({"busy_timeout_ms": True}, {"busy_timeout_ms": 1.5}, {"busy_timeout_ms": "10"}, {"busy_timeout_ms": None},
                       {"journal_size_limit_bytes": False}, {"journal_size_limit_bytes": 2.0}, {"journal_size_limit_bytes": "64"}):
                with self.subTest(f.nombre, **kw):
                    with self.assertRaises(ValueError):
                        f.modulo._validate_connection_policy(**{"busy_timeout_ms": 10, "journal_size_limit_bytes": 0, **kw})


class ParidadEntreLasCuatroCopias(unittest.TestCase):
    """D-63: no comparten código; sí deben dar EXACTAMENTE el mismo veredicto y los mismos valores para cada entrada."""

    ENTRADAS = (
        (0, -1), (0, 0), (1, 1), (10_000, 64 * 1024 * 1024), (MAX_BUSY, 2 ** 63 - 1),
        (MAX_BUSY + 1, 0), (2 ** 40, 0), (-1, 0), (10, -2), (10, 2 ** 63), (True, 0), (10, False), (1.5, 0), (10, "1"), (None, 0), (10, None),
        (Hostil(5), Hostil(6)), (Mentirosa(2 ** 31), 0), (Tacana(5), Tacana(7)),
    )

    @staticmethod
    def veredicto(fn, *args):
        try:
            busy, limit = fn(*args)
            return ("ok", busy, limit, type(busy), type(limit))
        except ValueError as exc:
            return ("ValueError", str(exc).split(":")[0])

    def test_same_verdict_and_values_in_every_copy(self):
        for args in self.ENTRADAS:
            veredictos = {f.nombre: self.veredicto(f.modulo._validate_connection_policy, *args) for f in FABRICAS}
            with self.subTest(args=[type(a).__name__ + ":" + int.__repr__(a) if isinstance(a, int) and not isinstance(a, bool) else repr(a) for a in args]):
                self.assertEqual(len(set(veredictos.values())), 1, f"las copias discrepan: {veredictos}")

    def test_same_constant_in_every_copy(self):
        self.assertEqual({f.nombre: f.modulo.MAX_BUSY_TIMEOUT_MS for f in FABRICAS}, {f.nombre: MAX_BUSY for f in FABRICAS})


if __name__ == "__main__":
    unittest.main()
