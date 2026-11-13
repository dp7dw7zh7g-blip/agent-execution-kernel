"""Endurecimiento de la apertura y de las transacciones (D-77, g0.8 r32): hallazgos 1, 2 y 3 de la auditoría de SQLite de r31.

1. Adaptador: `PRAGMA table_info` + `ALTER TABLE ADD COLUMN` no son atómicos entre procesos. Con varias aperturas simultáneas de una base ANTIGUA, las que perdían la
   carrera fallaban con «duplicate column name», que NO es un bloqueo y D-76 no reintenta. Ahora se vuelve a comprobar dentro de BEGIN IMMEDIATE (como DTOS, D-57).
2. Adaptador: `sqlite3.connect(timeout=ms/1000)` trunca el producto float a ms enteros; para valores como 1001 el motor quedaba en 1000 ms y la verificación de lectura
   lanzaba RuntimeError. Ahora la conexión fija `PRAGMA busy_timeout` de forma explícita, como UNAE, DTOS y la capa proactiva.
3. Capa proactiva: `ROLLBACK` sin guarda en tres sitios; si SQLite ya había revertido, «cannot rollback» tapaba el error original (UNAE lo resolvió en D-68).

Contra el código de r31 estas pruebas FALLAN (ver meta/MANIFEST.md, r32). Medido en Linux con Python 3.12 y SQLite 3.45.1; no se probó en Windows.
"""
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from adapter import UNAEDTOSAdapter
from dtos.store import SQLiteStore
from proactive.shadow import ShadowLog
from unae import Store

COLUMNAS_MIGRADAS = (("reconciliation_log", "evidence_json"), ("reconciliation_log", "reconciler_id"), ("adapter_inbox", "compacted_at"))


def columnas(path, tabla):
    with closing(sqlite3.connect(str(path))) as c:
        return {r[1] for r in c.execute(f"PRAGMA table_info({tabla})")}


def hacer_base_antigua(path):
    """Crea la base con el código actual y le quita las tres columnas que migra el adaptador (como una base de una revisión antigua de g0.4)."""
    UNAEDTOSAdapter(path)
    with closing(sqlite3.connect(str(path), isolation_level=None)) as c:
        for tabla, columna in COLUMNAS_MIGRADAS:
            c.execute(f"ALTER TABLE {tabla} DROP COLUMN {columna}")
    for tabla, columna in COLUMNAS_MIGRADAS:
        assert columna not in columnas(path, tabla)


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.d = Path(self._tmp.name)


class MigracionAtomicaDelAdaptador(Base):
    def test_an_old_database_gets_its_columns_and_a_second_opening_changes_nothing(self):
        db = self.d / "a.sqlite"
        hacer_base_antigua(db)
        UNAEDTOSAdapter(db)
        for tabla, columna in COLUMNAS_MIGRADAS:
            self.assertIn(columna, columnas(db, tabla))
        antes = {t: columnas(db, t) for t, _ in COLUMNAS_MIGRADAS}
        UNAEDTOSAdapter(db)
        self.assertEqual({t: columnas(db, t) for t, _ in COLUMNAS_MIGRADAS}, antes)

    def test_simultaneous_opening_of_an_old_database_does_not_fail(self):
        """8 hilos por ronda, 10 rondas, cada una sobre una base antigua nueva. Contra r31 falla en todas las rondas (`duplicate column name`)."""
        n = 8
        for ronda in range(10):
            with self.subTest(ronda=ronda):
                db = self.d / f"carrera-{ronda}.sqlite"
                hacer_base_antigua(db)
                barrera, errores = threading.Barrier(n), []

                def abrir():
                    barrera.wait()
                    try:
                        UNAEDTOSAdapter(db, busy_timeout_ms=10000)
                    except Exception as exc:  # noqa: BLE001 - se informa abajo
                        errores.append(f"{type(exc).__name__}: {exc}")

                hilos = [threading.Thread(target=abrir) for _ in range(n)]
                [h.start() for h in hilos]
                [h.join() for h in hilos]
                self.assertEqual(errores, [])
                for tabla, columna in COLUMNAS_MIGRADAS:
                    self.assertIn(columna, columnas(db, tabla))

    def test_a_column_added_by_someone_else_between_check_and_alter_is_tolerated(self):
        """Determinista: otro proceso añade la columna justo después de la comprobación sin bloqueo y antes de que se tome BEGIN IMMEDIATE."""
        db = self.d / "b.sqlite"
        hacer_base_antigua(db)
        a = UNAEDTOSAdapter.__new__(UNAEDTOSAdapter)
        a.busy_timeout_ms, a.journal_size_limit_bytes, a.db_path = 10000, 64 * 1024 * 1024, str(db)
        with closing(a._connect()) as conn:
            real_execute_calls = {"begin": 0}

            class Intermedio:
                """Delega en la conexión real; al ver el BEGIN IMMEDIATE, otro actor ya añadió la columna."""
                def __init__(self, c):
                    self._c = c

                def execute(self, sql, *args):
                    if sql.strip().upper().startswith("BEGIN IMMEDIATE") and not real_execute_calls["begin"]:
                        real_execute_calls["begin"] = 1
                        with closing(sqlite3.connect(str(db), isolation_level=None)) as otro:
                            otro.execute("ALTER TABLE adapter_inbox ADD COLUMN compacted_at TEXT")
                    return self._c.execute(sql, *args)

                @property
                def in_transaction(self):
                    return self._c.in_transaction

            UNAEDTOSAdapter._add_column_if_missing(Intermedio(conn), "adapter_inbox", "compacted_at")
        self.assertEqual(real_execute_calls["begin"], 1)
        self.assertIn("compacted_at", columnas(db, "adapter_inbox"))


class BusyTimeoutExactoEnElAdaptador(Base):
    def test_the_connection_applies_exactly_the_requested_busy_timeout_for_every_value(self):
        """Todos los valores de 0 a 3000 ms. Contra r31 fallan los que el producto float trunca (1001, 1003, 1005…)."""
        a = UNAEDTOSAdapter(self.d / "t.sqlite")
        malos = []
        for ms in range(0, 3001):
            a.busy_timeout_ms = ms
            with closing(a._connect()) as c:
                efectivo = c.execute("PRAGMA busy_timeout").fetchone()[0]
            if efectivo != ms:
                malos.append((ms, efectivo))
        self.assertEqual(malos, [])

    def test_opening_with_a_value_the_float_conversion_truncates_works_in_all_four_stores(self):
        self.assertNotEqual(int((1001 / 1000) * 1000), 1001, "el hecho que motiva la prueba ya no se da en este intérprete")
        aperturas = {
            "adapter": lambda p: UNAEDTOSAdapter(p, busy_timeout_ms=1001),
            "unae": lambda p: Store(p, busy_timeout_ms=1001),
            "proactive": lambda p: ShadowLog(p, busy_timeout_ms=1001),
            "dtos": lambda p: SQLiteStore(p, busy_timeout_ms=1001),
        }
        for nombre, abrir in aperturas.items():
            with self.subTest(nombre):
                s = abrir(self.d / f"{nombre}.sqlite")
                if hasattr(s, "close"):
                    self.addCleanup(s.close)
                self.assertEqual(s.busy_timeout_ms, 1001)


class _Intermedio:
    """Conexión que, ante la sentencia marcada, hace ROLLBACK por su cuenta (como si SQLite hubiera revertido solo) y lanza un error que NO es de bloqueo."""
    def __init__(self, c, prefijo):
        self._c, self._prefijo = c, prefijo

    def execute(self, sql, *args):
        if sql.lstrip().startswith(self._prefijo):
            self._c.execute("ROLLBACK")
            raise sqlite3.OperationalError("disk I/O error")
        return self._c.execute(sql, *args)

    def __getattr__(self, nombre):
        return getattr(self._c, nombre)


class RollbackSeguroEnLaCapaProactiva(Base):
    def test_rollback_helper_is_safe_with_and_without_a_transaction(self):
        with closing(sqlite3.connect(":memory:", isolation_level=None)) as c:
            ShadowLog._rollback(c)  # sin transacción: no lanza
            c.execute("CREATE TABLE t(x)")
            c.execute("BEGIN")
            c.execute("INSERT INTO t VALUES (1)")
            ShadowLog._rollback(c)
            self.assertFalse(c.in_transaction)
            self.assertEqual(c.execute("SELECT count(*) FROM t").fetchone()[0], 0)

    def _con_intermedio(self, log, prefijo, solo_desde=1):
        """Parchea `_connect` para que, desde la llamada número `solo_desde` (0 = la primera), devuelva una conexión que falla en `prefijo`."""
        original, llamadas = log._connect, {"n": 0}

        def conectar():
            c = original()
            llamadas["n"] += 1
            return _Intermedio(c, prefijo) if llamadas["n"] > solo_desde else c

        return mock.patch.object(log, "_connect", conectar)

    def test_record_surfaces_the_original_error_when_sqlite_already_rolled_back(self):
        log = ShadowLog(self.d / "r.sqlite")
        with self._con_intermedio(log, "INSERT INTO would_have_done(", solo_desde=0):
            with self.assertRaises(sqlite3.OperationalError) as cm:
                log.record(rule_id="r", policy_version="1", trigger_ref="t", kind="k", payload={}, reason="x")
        self.assertIn("disk I/O error", str(cm.exception))

    def test_review_surfaces_the_original_error_when_sqlite_already_rolled_back(self):
        log = ShadowLog(self.d / "v.sqlite")
        rec = log.record(rule_id="r", policy_version="1", trigger_ref="t", kind="k", payload={}, reason="x")
        with self._con_intermedio(log, "INSERT INTO would_have_done_review", solo_desde=0):
            with self.assertRaises(sqlite3.OperationalError) as cm:
                log.review(rec.would_id, "CORRECT")
        self.assertIn("disk I/O error", str(cm.exception))

    def test_migrate_surfaces_the_original_error_when_sqlite_already_rolled_back(self):
        """`_open_once` usa la primera conexión (PRAGMAs); `_migrate` la segunda, que es la que falla."""
        original, llamadas = ShadowLog._connect, {"n": 0}

        def conectar(self_):
            c = original(self_)
            llamadas["n"] += 1
            return _Intermedio(c, "INSERT INTO schema_version") if llamadas["n"] == 2 else c

        with mock.patch.object(ShadowLog, "_connect", conectar):
            with self.assertRaises(sqlite3.OperationalError) as cm:
                ShadowLog(self.d / "m.sqlite")
        self.assertIn("disk I/O error", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
