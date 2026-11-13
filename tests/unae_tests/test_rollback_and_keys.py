"""Parche 0017 (D-68), UNAE: (1) el ROLLBACK no oculta el error original; (2) `canonical_json` rechaza claves no-str.

Sin el parche: los tres métodos de la prueba 1 lanzaban `OperationalError: cannot rollback - no transaction is active` en lugar del error real,
y `canonical_json({1: "a"})` devolvía lo mismo que `canonical_json({"1": "a"})`.
"""
import ast
import sqlite3
from contextlib import closing
import tempfile
import time
import unittest
from enum import StrEnum
from pathlib import Path

from unae import Store
from unae import store as store_mod


class Boom(RuntimeError):
    pass


class RollbackDoesNotMaskTheRealError(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.armed = None
        self.path = str(Path(self.tmp.name) / "u.sqlite")
        self.s = Store(self.path, fault_hook=self.hook)
        self.addCleanup(self.s.close)

    def hook(self, point):
        if self.armed == point:
            raise Boom(point)

    def claimed_event(self):
        # D-78: `closing` cierra la conexión (el `with` solo confirma); si no, en Windows no se puede borrar el directorio temporal.
        with closing(sqlite3.connect(self.path)) as raw, raw:  # sin FK: solo hace falta una fila de outbox
            raw.execute("INSERT INTO outbox(event_id,commit_id,event_type,payload,status,attempts,created_at) VALUES('e1','c1','T','{}','PENDING',0,?)", (time.time(),))
        self.assertEqual(len(self.s.claim_events("w1")), 1)

    def check(self, method, call):
        self.claimed_event()
        self.armed = f"{method}:after_commit"  # el COMMIT ya se hizo; lo que falla viene después
        with self.assertRaises(Boom):
            call()

    def test_mark_event_sent(self):
        self.check("mark_event_sent", lambda: self.s.mark_event_sent("e1", "w1"))

    def test_mark_event_failed(self):
        self.check("mark_event_failed", lambda: self.s.mark_event_failed("e1", 5, "w1"))

    def test_release_events(self):
        self.check("release_events", lambda: self.s.release_events(["e1"], "w1"))

    def test_claim_and_renew_keep_working(self):
        self.claimed_event()
        self.armed = "renew_claim:after_commit"
        with self.assertRaises(Boom):
            self.s.renew_claim("e1", "w1")

    def test_rollback_helper_is_a_noop_without_transaction(self):
        with closing(sqlite3.connect(self.path, isolation_level=None)) as c, c:
            self.assertFalse(c.in_transaction)
            Store._rollback(c)  # no lanza

    def test_rollback_helper_rolls_back_an_open_transaction(self):
        c = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(c.close)
        c.execute("BEGIN IMMEDIATE")
        c.execute("INSERT INTO consumer_inbox VALUES('c','e',1.0)")
        Store._rollback(c)
        self.assertFalse(c.in_transaction)
        self.assertEqual(c.execute("SELECT COUNT(*) FROM consumer_inbox").fetchone()[0], 0)

    def test_only_the_helper_executes_a_raw_rollback(self):
        """Guarda estructural: ningún método vuelve a ejecutar `ROLLBACK` a pelo (esa era la fuente del defecto)."""
        tree = ast.parse(Path(store_mod.__file__).read_text(encoding="utf-8"))
        owners = []
        for fn in (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)):
            for call in (n for n in ast.walk(fn) if isinstance(n, ast.Call)):
                if call.args and isinstance(call.args[0], ast.Constant) and call.args[0].value == "ROLLBACK":
                    owners.append(fn.name)
        self.assertEqual(owners, ["_rollback"])


class CanonicalJsonRejectsNonStrKeys(unittest.TestCase):
    def test_int_and_bool_keys_are_rejected(self):
        for bad in ({1: "a"}, {True: 1}, {None: 1}, {1.5: 1}, {(1, 2): 1}):
            with self.assertRaises(ValueError, msg=repr(bad)):
                store_mod.canonical_json(bad)

    def test_nested_keys_are_checked(self):
        for bad in ({"a": {"b": {1: 2}}}, {"a": [{"b": 1}, {2: 3}]}, [[{3: 4}]], {"a": ({5: 6},)}):
            with self.assertRaises(ValueError, msg=repr(bad)):
                store_mod.canonical_json(bad)

    def test_no_more_silent_collision(self):
        self.assertEqual(store_mod.canonical_json({"1": "a"}), '{"1":"a"}')
        with self.assertRaises(ValueError):
            store_mod.canonical_json({1: "a"})

    def test_valid_payloads_are_unchanged(self):
        self.assertEqual(store_mod.canonical_json({"b": [1, 2.5, None, True], "a": {"z": "ñ"}}), '{"a":{"z":"\\u00f1"},"b":[1,2.5,null,true]}')
        self.assertEqual(store_mod.fingerprint({"a": 1}), store_mod.fingerprint({"a": 1}))

    def test_str_subclass_keys_are_accepted(self):
        class K(StrEnum):
            A = "a"
        self.assertEqual(store_mod.canonical_json({K.A: 1}), '{"a":1}')

    def test_other_errors_are_unchanged(self):
        with self.assertRaises(ValueError):
            store_mod.canonical_json({"x": float("nan")})
        with self.assertRaises(TypeError):
            store_mod.canonical_json({"x": object()})


if __name__ == "__main__":
    unittest.main()
