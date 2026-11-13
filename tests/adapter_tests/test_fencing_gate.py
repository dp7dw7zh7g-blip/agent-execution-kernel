"""Unitarias de la puerta de fencing de referencia (Hallazgo 4, D-56). Sin Stores."""
import threading
import unittest

from adapter import FencingGate, ProtocolError, StaleFencingToken


def req(token, scope="task-1"):
    return {"fencing_token": token, "fencing_scope": scope}


class FencingGateRules(unittest.TestCase):
    def test_strictly_lower_token_is_rejected(self):
        g = FencingGate()
        g.admit(req(2))
        with self.assertRaises(StaleFencingToken) as cm:
            g.admit(req(1))
        self.assertEqual((cm.exception.token, cm.exception.highest), (1, 2))
        self.assertEqual(g.highest("task-1"), 2)  # el rechazo no baja ni cambia el máximo

    def test_equal_token_is_accepted_so_retries_and_sibling_operations_work(self):
        g = FencingGate()
        for _ in range(3):
            self.assertEqual(g.admit(req(5)), 5)  # «menor o igual» rompería la idempotencia: igual debe pasar

    def test_higher_token_is_accepted_and_fences_the_previous_leader(self):
        g = FencingGate()
        g.admit(req(1))
        g.admit(req(2))
        with self.assertRaises(StaleFencingToken):
            g.admit(req(1))

    def test_scopes_are_independent(self):
        g = FencingGate()
        g.admit(req(9, "task-A"))
        self.assertEqual(g.admit(req(1, "task-B")), 1)  # el token 1 de otra Task no compite con el 9 de ésta
        self.assertIsNone(g.highest("task-C"))

    def test_request_without_fencing_is_not_admitted_silently(self):
        g = FencingGate()
        for bad in ({}, {"fencing_token": 1}, {"fencing_scope": "t"}, {"fencing_token": 1, "fencing_scope": " "}):
            with self.assertRaises(ProtocolError):
                g.admit(bad)

    def test_invalid_token_types_are_rejected(self):
        g = FencingGate()
        for bad in (True, "1", 1.0, -1, None, 2 ** 53):
            with self.assertRaises(ProtocolError, msg=repr(bad)):
                g.admit(req(bad))
        self.assertIsNone(g.highest("task-1"))

    def test_concurrent_admissions_never_lower_the_maximum(self):
        g = FencingGate()
        errors = []

        def worker(t):
            try:
                g.admit(req(t))
            except StaleFencingToken:
                pass
            except Exception as e:  # noqa: BLE001
                errors.append(e)
        threads = [threading.Thread(target=worker, args=(t,)) for t in list(range(50)) + list(range(50))]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual((errors, g.highest("task-1")), ([], 49))


if __name__ == "__main__":
    unittest.main()
