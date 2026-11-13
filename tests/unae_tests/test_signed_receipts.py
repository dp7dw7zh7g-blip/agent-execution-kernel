import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from unae import Store, CommitStatus, sign_receipt
from unae.store import CommitRequest

KEY_A, KEY_B = b"secret-A" * 4, b"secret-B" * 4


class SignedReceiptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "u.sqlite")
        self._stores = []
        self.s = self.open()

    def tearDown(self):
        for s in self._stores:          # todos los Stores abiertos con open(): libera sus conexiones ancla (Windows)
            s.close()
        self.tmp.cleanup()

    def open(self, **kw):
        s = Store(self.db, keyring=kw.pop("keyring", {"prov-A": KEY_A, "prov-B": KEY_B}), **kw)
        self._stores.append(s)
        s.configure_account("__GLOBAL__", 1000, 100000, 10_000_000, 4)
        s.configure_account("task-1", 1000, 100000, 10_000_000, 4)
        try: s.create_task("task-1", {"n": 0})
        except Exception: pass
        return s

    def running(self, provider_id="prov-A", op="op-1", res="res-1", key="k1"):
        self.assertEqual(self.s.reserve_and_register(task_id="task-1", operation_id=op, reservation_id=res, idempotency_key=key,
            fencing_token=1, compute_units=10, time_ms=10, memory_bytes=10), "RESERVED")
        self.assertTrue(self.s.mark_running(op, 1, "p1", provider_id=provider_id))

    def receipt(self, secret=KEY_A, task="task-1", pid="prov-A", rid="r1", op="op-1", res="res-1", compute=5, sign_compute=None, store=None):
        kw = dict(receipt_id=rid, operation_id=op, reservation_id=res, outcome="SUCCEEDED", actual_compute=compute,
                  actual_time_ms=5, peak_memory_bytes=5, provider_operation_id="p1")
        sig = sign_receipt(secret, provider_id=pid, task_id=task, **{**kw, "actual_compute": sign_compute if sign_compute is not None else compute})
        return (store or self.s).record_receipt(provider_id=pid, signature=sig, task_id=task, **kw)

    def test_valid_signed_receipt_end_to_end(self):
        self.running()
        self.assertEqual(self.receipt(), "RECORDED")
        self.assertEqual(self.receipt(), "ALREADY_RECORDED")                     # reintento idéntico
        st = self.s.coordinated_commit(CommitRequest("c1", "task-1", "op-1", "res-1", "r1", 0, 1, {"n": 1}, "e1"))
        self.assertEqual(st, CommitStatus.COMMITTED)
        with closing(self.s.connect()) as c:
            row = c.execute("SELECT provider_id,signature FROM receipts WHERE receipt_id='r1'").fetchone()
        self.assertEqual(row["provider_id"], "prov-A"); self.assertEqual(len(row["signature"]), 64)

    def test_tampered_wrong_key_and_unknown_provider_are_rejected(self):
        self.running()
        self.assertEqual(self.receipt(compute=1, sign_compute=999), "BAD_SIGNATURE")      # contenido alterado
        self.assertEqual(self.receipt(secret=b"x" * 32), "BAD_SIGNATURE")                  # clave equivocada
        self.assertEqual(self.receipt(secret=b"x" * 32, pid="prov-Z"), "BAD_SIGNATURE")    # proveedor desconocido
        self.assertEqual(self.s.record_receipt(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED",
            actual_compute=5, actual_time_ms=5, peak_memory_bytes=5, provider_operation_id="p1",
            provider_id="prov-A", task_id="task-1", signature=12345), "BAD_SIGNATURE")                       # firma no-str
        with closing(self.s.connect()) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM receipts").fetchone()[0], 0)

    def test_receipt_from_other_valid_provider_is_not_accepted(self):
        self.running(provider_id="prov-A")
        self.assertEqual(self.receipt(secret=KEY_B, pid="prov-B"), "INVALID_RECEIPT")      # firma válida, proveedor ajeno

    def test_operation_bound_to_provider_rejects_unsigned(self):
        self.running()
        self.assertEqual(self.s.record_receipt(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED",
            actual_compute=5, actual_time_ms=5, peak_memory_bytes=5, provider_operation_id="p1"), "UNSIGNED_REJECTED")

    def test_require_signed_store_rejects_unsigned_and_unbound(self):
        strict = self.open(require_signed_receipts=True)
        self.running(provider_id=None)
        self.assertEqual(strict.record_receipt(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED",
            actual_compute=5, actual_time_ms=5, peak_memory_bytes=5, provider_operation_id="p1"), "UNSIGNED_REJECTED")
        self.assertEqual(self.receipt(store=strict), "INVALID_RECEIPT")                    # op sin proveedor declarado

    def test_same_receipt_id_with_different_valid_content_conflicts(self):
        self.running()
        self.assertEqual(self.receipt(compute=5), "RECORDED")
        self.assertEqual(self.receipt(compute=6), "RECEIPT_CONFLICT")

    def test_key_rotation_accepts_old_key_while_listed(self):
        self.running()
        rotated = self.open(keyring={"prov-A": (b"new-key" * 5, KEY_A)})
        self.assertEqual(self.receipt(secret=KEY_A, store=rotated), "RECORDED")
        self.running(op="op-2", res="res-2", key="k2")
        retired = self.open(keyring={"prov-A": (b"new-key" * 5,)})
        self.assertEqual(self.receipt(secret=KEY_A, rid="r2", op="op-2", res="res-2", store=retired), "BAD_SIGNATURE")

    def test_params_must_come_together_and_legacy_unsigned_still_works(self):
        self.running(provider_id=None)
        with self.assertRaises(ValueError):
            self.s.record_receipt(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED",
                actual_compute=5, actual_time_ms=5, peak_memory_bytes=5, provider_id="prov-A")
        self.assertEqual(self.s.record_receipt(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED",
            actual_compute=5, actual_time_ms=5, peak_memory_bytes=5, provider_operation_id="p1"), "RECORDED")

    def test_signature_is_bound_to_the_task(self):
        self.running()
        self.assertEqual(self.receipt(task="task-OTHER"), "INVALID_RECEIPT")            # firma válida para otra tarea, enviada como tal
        kw = dict(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED", actual_compute=5,
                  actual_time_ms=5, peak_memory_bytes=5, provider_operation_id="p1")
        other = sign_receipt(KEY_A, provider_id="prov-A", task_id="task-OTHER", **kw)
        self.assertEqual(self.s.record_receipt(provider_id="prov-A", task_id="task-1", signature=other, **kw), "BAD_SIGNATURE")  # reutilizada
        with self.assertRaises(ValueError):                                              # firma sin task_id
            self.s.record_receipt(provider_id="prov-A", signature=other, **kw)

    def test_provider_bound_operation_needs_provider_operation_id_and_receipt_must_echo_it(self):
        self.assertEqual(self.s.reserve_and_register(task_id="task-1", operation_id="op-1", reservation_id="res-1", idempotency_key="k1",
            fencing_token=1, compute_units=10, time_ms=10, memory_bytes=10), "RESERVED")
        with self.assertRaises(ValueError):
            self.s.mark_running("op-1", 1, None, provider_id="prov-A")
        self.assertTrue(self.s.mark_running("op-1", 1, "p-real", provider_id="prov-A"))
        kw = dict(receipt_id="r1", operation_id="op-1", reservation_id="res-1", outcome="SUCCEEDED", actual_compute=5,
                  actual_time_ms=5, peak_memory_bytes=5, provider_operation_id="p-OTRO")
        sig = sign_receipt(KEY_A, provider_id="prov-A", task_id="task-1", **kw)
        self.assertEqual(self.s.record_receipt(provider_id="prov-A", task_id="task-1", signature=sig, **kw), "INVALID_RECEIPT")


if __name__ == "__main__":
    unittest.main()
