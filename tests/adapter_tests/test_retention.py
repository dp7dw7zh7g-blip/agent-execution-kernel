"""Pruebas UNITARIAS de retención del inbox y revisión/purga de la cuarentena (D-43, D-44). Sin Stores: solo el adaptador."""
import sqlite3
import tempfile
import unittest
import uuid
from contextlib import closing
from pathlib import Path

from adapter import MessageConflict, PayloadIntegrityError, ProtocolError, UNAEDTOSAdapter

OLD = "2020-01-01T00:00:00.000Z"


def uid():
    return str(uuid.uuid4())


def msg(**over):
    m = {"contract_version": "1.1", "schema_version": "1", "message_id": uid(), "message_type": "OPERATION_REQUEST", "correlation_id": "c",
         "evidence_refs": [], "created_at": "2026-10-25T10:00:00Z", "operation_id": uid()}
    m.update(over)
    return m


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db = str(Path(self._tmp.name) / "a.db")
        self.a = UNAEDTOSAdapter(self.db)

    def sql(self, statement, args=()):
        with closing(sqlite3.connect(self.db, isolation_level=None)) as c:
            cur = c.execute(statement, args)
            return cur.fetchall()


class InboxCompaction(Base):
    def old_message(self):
        m = msg()
        self.a.receive(m)
        self.sql("UPDATE adapter_inbox SET received_at=? WHERE message_id=?", (OLD, m["message_id"]))
        return m

    def test_compacts_only_old_rows_and_keeps_identity(self):
        old, recent = self.old_message(), msg()
        self.a.receive(recent)
        self.assertEqual(self.a.compact_inbox(older_than_days=30), 1)
        rows = {r[0]: r for r in self.sql("SELECT message_id,payload_json,payload_sha256,status,compacted_at FROM adapter_inbox")}
        self.assertEqual(rows[old["message_id"]][1], "")
        self.assertIsNotNone(rows[old["message_id"]][4])
        self.assertTrue(rows[old["message_id"]][2] and rows[old["message_id"]][3] == "ACCEPTED")
        self.assertNotEqual(rows[recent["message_id"]][1], "")
        self.assertIsNone(rows[recent["message_id"]][4])
        self.assertEqual(len(rows), 2)  # no se borra ninguna fila

    def test_second_run_is_a_no_op(self):
        self.old_message()
        self.assertEqual(self.a.compact_inbox(older_than_days=30), 1)
        self.assertEqual(self.a.compact_inbox(older_than_days=30), 0)

    def test_deduplication_and_conflict_detection_survive_compaction(self):
        m = self.old_message()
        self.a.compact_inbox(older_than_days=30)
        self.assertEqual(self.a.receive(dict(m)).status, "DUPLICATE")  # AT-02: un redelivery tardío NO se acepta como nuevo
        with self.assertRaises(MessageConflict):
            self.a.receive({**m, "correlation_id": "otra"})  # AT-03
        self.assertEqual(self.a.quarantine_count(), 1)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM adapter_inbox")[0][0], 1)

    def test_older_than_days_is_mandatory_and_validated(self):
        with self.assertRaises(TypeError):
            self.a.compact_inbox()  # type: ignore[call-arg]
        for bad in (0, -1, float("nan"), float("inf"), True, "7", None):
            with self.subTest(v=repr(bad)), self.assertRaises(ProtocolError):
                self.a.compact_inbox(older_than_days=bad)

    def test_database_without_the_column_is_migrated(self):
        self.sql("ALTER TABLE adapter_inbox DROP COLUMN compacted_at")
        UNAEDTOSAdapter(self.db)
        self.assertIn("compacted_at", [r[1] for r in self.sql("PRAGMA table_info(adapter_inbox)")])


class QuarantineReview(Base):
    def quarantined(self, n=1):
        ids = []
        for _ in range(n):
            m = msg()
            self.a.receive(m)
            with self.assertRaises(MessageConflict):
                self.a.receive({**m, "correlation_id": "otra"})
        return [r["quarantine_id"] for r in self.a.list_quarantine()][-n:]

    def test_list_filters_and_shape(self):
        a, b = self.quarantined(2)
        self.a.review_quarantine(a, decision="DISMISSED", reviewer_id="ana", note="falso positivo")
        self.assertEqual([r["quarantine_id"] for r in self.a.list_quarantine()], [a, b])
        self.assertEqual([r["quarantine_id"] for r in self.a.list_quarantine(reviewed=True)], [a])
        self.assertEqual([r["quarantine_id"] for r in self.a.list_quarantine(reviewed=False)], [b])
        row = self.a.list_quarantine(reviewed=True)[0]
        self.assertEqual((row["decision"], row["reviewer_id"], row["note"], row["reason"]), ("DISMISSED", "ana", "falso positivo", "MESSAGE_ID_PAYLOAD_CONFLICT"))
        self.assertIn("payload_json", row)  # quien revisa puede leer el mensaje

    def test_review_is_validated_once_only_and_append_only(self):
        (q,) = self.quarantined()
        for kwargs in (dict(decision="PURGE", reviewer_id="x"), dict(decision="DISMISSED", reviewer_id="  "), dict(decision="DISMISSED", reviewer_id=5),
                       dict(decision="DISMISSED", reviewer_id="x", note=5)):
            with self.subTest(**{k: repr(v) for k, v in kwargs.items()}), self.assertRaises(ProtocolError):
                self.a.review_quarantine(q, **kwargs)
        for bad_id in (999, True, "1", None):
            with self.subTest(id=repr(bad_id)), self.assertRaises(ProtocolError):
                self.a.review_quarantine(bad_id, decision="DISMISSED", reviewer_id="x")  # type: ignore[arg-type]
        self.a.review_quarantine(q, decision="REPROCESSED", reviewer_id="ana")
        with self.assertRaises(ProtocolError):
            self.a.review_quarantine(q, decision="DISMISSED", reviewer_id="luis")
        with self.assertRaises(sqlite3.DatabaseError):
            self.sql("UPDATE quarantine_review SET decision='DISMISSED'")
        with self.assertRaises(sqlite3.DatabaseError):
            self.sql("DELETE FROM quarantine_review")
        self.assertEqual(self.a.list_quarantine(reviewed=True)[0]["decision"], "REPROCESSED")

    def test_review_never_deletes(self):
        (q,) = self.quarantined()
        self.a.review_quarantine(q, decision="DISMISSED", reviewer_id="ana")
        self.assertEqual(self.a.quarantine_count(), 1)

    def test_purge_only_removes_reviewed_rows_older_than_the_limit(self):
        unreviewed_old, reviewed_recent, reviewed_old = self.quarantined(3)
        self.sql("UPDATE adapter_quarantine SET created_at=?", (OLD,))  # todas antiguas: la antigüedad de creación no cuenta
        self.a.review_quarantine(reviewed_recent, decision="DISMISSED", reviewer_id="ana")
        self.sql("INSERT INTO quarantine_review(quarantine_id,decision,reviewer_id,note,reviewed_at) VALUES(?,?,?,?,?)", (reviewed_old, "REPROCESSED", "luis", "n", OLD))
        self.assertEqual(self.a.purge_reviewed_quarantine(older_than_days=30), 1)
        self.assertEqual(sorted(r["quarantine_id"] for r in self.a.list_quarantine()), sorted([unreviewed_old, reviewed_recent]))
        (tomb,) = self.a.quarantine_purge_history()
        self.assertEqual((tomb["quarantine_id"], tomb["decision"], tomb["reviewer_id"], tomb["reason"]), (reviewed_old, "REPROCESSED", "luis", "MESSAGE_ID_PAYLOAD_CONFLICT"))
        self.assertTrue(tomb["message_id"] and tomb["incoming_sha256"] and tomb["stored_sha256"])
        self.assertEqual(self.a.purge_reviewed_quarantine(older_than_days=30), 0)

    def test_unreviewed_rows_are_never_purged(self):
        (q,) = self.quarantined()
        self.sql("UPDATE adapter_quarantine SET created_at=?", (OLD,))
        self.assertEqual(self.a.purge_reviewed_quarantine(older_than_days=0.000001), 0)
        self.assertEqual(self.a.quarantine_count(), 1)

    def test_purge_log_is_append_only(self):
        (q,) = self.quarantined()
        self.sql("INSERT INTO quarantine_review(quarantine_id,decision,reviewer_id,reviewed_at) VALUES(?,?,?,?)", (q, "DISMISSED", "ana", OLD))
        self.a.purge_reviewed_quarantine(older_than_days=1)
        for statement in ("UPDATE quarantine_purge_log SET reason='x'", "DELETE FROM quarantine_purge_log"):
            with self.assertRaises(sqlite3.DatabaseError):
                self.sql(statement)

    def test_purge_requires_a_valid_limit(self):
        with self.assertRaises(TypeError):
            self.a.purge_reviewed_quarantine()  # type: ignore[call-arg]
        for bad in (0, -1, float("nan"), float("inf"), True, "7", None):
            with self.subTest(v=repr(bad)), self.assertRaises(ProtocolError):
                self.a.purge_reviewed_quarantine(older_than_days=bad)

    def test_backlog_counts_only_unreviewed(self):
        self.assertEqual(self.a.quarantine_backlog(), {"unreviewed": 0, "oldest_unreviewed_at": None})
        a, b = self.quarantined(2)
        self.a.review_quarantine(a, decision="DISMISSED", reviewer_id="ana")
        backlog = self.a.quarantine_backlog()
        self.assertEqual(backlog["unreviewed"], 1)
        self.assertIsNotNone(backlog["oldest_unreviewed_at"])

    def test_hash_mismatch_rows_are_listed_too(self):
        m = msg()
        with self.assertRaises(PayloadIntegrityError):
            self.a.receive({**m, "payload_sha256": "0" * 64})
        self.assertEqual(self.a.list_quarantine(reviewed=False)[0]["reason"], "PAYLOAD_SHA256_MISMATCH")

    def test_list_limit_is_validated(self):
        for bad in (0, 1001, True, "5", None):
            with self.subTest(v=repr(bad)), self.assertRaises(ProtocolError):
                self.a.list_quarantine(limit=bad)  # type: ignore[arg-type]
        with self.assertRaises(ProtocolError):
            self.a.list_quarantine(reviewed="si")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
