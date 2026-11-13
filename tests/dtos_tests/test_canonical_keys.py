"""
test_canonical_keys.py (parche 0017, D-68)
==========================================
`canonical.canonical_json` (la ÚNICA función autorizada para los hashes, P-064) convertía en silencio las claves no-str: {1: "a"} y {"1": "a"}
daban el mismo texto y el mismo hash, y {True: 1} salía como {"true":1}. Ahora se rechazan con ValueError.
Sin el parche estas pruebas FALLAN. Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import sys
import unittest
from enum import StrEnum

from dtos.canonical import canonical_json, content_hash


class CanonicalKeyTests(unittest.TestCase):
    def test_non_str_keys_are_rejected_at_any_depth(self):
        for bad in ({1: "a"}, {True: 1}, {None: 1}, {"a": {"b": {2: 3}}}, {"a": [{"b": 1}, {2: 3}]}, [[{3: 4}]], {"a": ({5: 6},)}):
            with self.assertRaises(ValueError, msg=repr(bad)):
                canonical_json(bad)

    def test_hash_of_int_key_no_longer_collides_with_str_key(self):
        with self.assertRaises(ValueError):
            content_hash({1: "a"})
        self.assertEqual(content_hash({"1": "a"}), content_hash({"1": "a"}))

    def test_str_keys_and_str_subclasses_are_accepted(self):
        class K(StrEnum):
            A = "a"
        self.assertEqual(canonical_json({K.A: 1, "b": [None, True, 1.5]}), '{"a":1,"b":[null,true,1.5]}')

    def test_existing_behaviour_is_unchanged(self):
        self.assertEqual(canonical_json({"b": 1, "a": "ñ"}), '{"a":"ñ","b":1}')
        with self.assertRaises(ValueError):
            canonical_json({"x": float("inf")})
        with self.assertRaises(TypeError):
            canonical_json({"x": object()})

    def test_persisted_payloads_round_trip_with_the_same_hash(self):
        """Lo que se lee de la base (json.loads) siempre tiene claves str: la auditoría histórica no se ve afectada."""
        import json
        payload = {"id": "x", "n": [1, {"k": "v"}]}
        self.assertEqual(canonical_json(json.loads(canonical_json(payload))), canonical_json(payload))


def main() -> int:
    r = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(CanonicalKeyTests))
    return 0 if r.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
