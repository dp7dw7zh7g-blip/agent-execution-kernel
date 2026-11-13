"""Parche 0017 (D-68), adaptador: `canonical_json` y `payload_digest` rechazan claves no-str (antes {1: "a"} y {"1": "a"} daban el mismo digest)."""
import unittest

from adapter.adapter import canonical_json, payload_digest, sha256_json


class AdapterCanonicalKeys(unittest.TestCase):
    def test_non_str_keys_are_rejected(self):
        for bad in ({1: "a"}, {True: 1}, {"a": {"b": [{2: 3}]}}):
            with self.assertRaises(ValueError, msg=repr(bad)):
                canonical_json(bad)
            with self.assertRaises(ValueError, msg=repr(bad)):
                sha256_json(bad)

    def test_payload_digest_of_a_message_with_int_key_is_rejected(self):
        with self.assertRaises(ValueError):
            payload_digest({"message_id": "m", "body": {7: "x"}})

    def test_valid_messages_keep_their_digest(self):
        self.assertEqual(canonical_json({"b": 1, "a": "ñ"}), '{"a":"ñ","b":1}')
        self.assertEqual(payload_digest({"a": 1, "payload_sha256": "ignorado"}), sha256_json({"a": 1}))


if __name__ == "__main__":
    unittest.main()
