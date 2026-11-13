"""Pruebas de `proactive.shadow` (D-49): registro solo-añadir de lo que el agente habría hecho."""
import hashlib
import re
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path

from dtos.canonical import canonical_json
from proactive.shadow import MAX_PAYLOAD_BYTES, SCHEMA_VERSION, ShadowLog, Verdict, real_action_key

SRC = Path(__file__).resolve().parents[2] / "src" / "proactive"


class ShadowBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)        # registrado primero = se ejecuta al final (LIFO), después de cerrar las conexiones de cada prueba
        self.db = str(Path(self.tmp.name) / "shadow.sqlite")
        self.t = [1000.0]
        self.log = ShadowLog(self.db, clock=lambda: self.t[0])

    def rec(self, **kw):
        args = dict(rule_id="r1", policy_version="p1", trigger_ref="evt-1", kind="send_message",
                    payload={"to": "ana", "text": "hola"}, reason="la regla r1 lo permite")
        args.update(kw)
        return self.log.record(**args)


class RecordTests(ShadowBase):
    def test_record_stores_canonical_payload_and_hash(self):
        r = self.rec(payload={"b": 2, "a": {"z": 1, "y": "é"}}, kernel_refs={"task_id": "t-1"})
        self.assertTrue(r.created and r.payload_matches)
        row = self.log.get(r.would_id)
        self.assertEqual(row["payload"], '{"a":{"y":"é","z":1},"b":2}')
        self.assertEqual(row["payload_sha256"], hashlib.sha256(row["payload"].encode("utf-8")).hexdigest())
        self.assertEqual(row["payload_bytes"], len(row["payload"].encode("utf-8")))
        self.assertEqual(row["kernel_refs"], '{"task_id":"t-1"}')
        self.assertEqual((row["rule_id"], row["policy_version"], row["trigger_ref"], row["kind"], row["created_at"]),
                         ("r1", "p1", "evt-1", "send_message", 1000.0))

    def test_same_key_is_idempotent_and_keeps_the_first_row(self):
        first = self.rec()
        self.t[0] = 2000.0
        again = self.rec(reason="otra razón", policy_version="p2")
        self.assertEqual((again.would_id, again.created, again.payload_matches), (first.would_id, False, True))
        self.assertEqual(self.log.get(first.would_id)["created_at"], 1000.0)
        self.assertEqual(self.log.get(first.would_id)["policy_version"], "p1")
        self.assertEqual(self.log.summary()["total"], 1)

    def test_same_key_with_different_payload_is_flagged_not_overwritten(self):
        first = self.rec()
        again = self.rec(payload={"to": "ana", "text": "OTRO"})
        self.assertEqual((again.would_id, again.created, again.payload_matches), (first.would_id, False, False))
        self.assertEqual(self.log.get(first.would_id)["payload"], canonical_json({"to": "ana", "text": "hola"}))

    def test_different_rule_trigger_or_kind_makes_a_new_row(self):
        ids = {self.rec().would_id, self.rec(rule_id="r2").would_id, self.rec(trigger_ref="evt-2").would_id,
               self.rec(kind="create_task").would_id}
        self.assertEqual(len(ids), 4)
        self.assertEqual(self.log.summary()["total"], 4)

    def test_payload_limit_is_in_bytes_not_characters(self):
        exact = {"x": "a" * (MAX_PAYLOAD_BYTES - len(canonical_json({"x": ""})))}
        self.assertEqual(len(canonical_json(exact).encode("utf-8")), MAX_PAYLOAD_BYTES)
        self.assertTrue(self.rec(payload=exact).created)                       # justo en el tope: se acepta
        with self.assertRaisesRegex(ValueError, "too large"):
            self.rec(trigger_ref="evt-2", payload={"x": "a" * MAX_PAYLOAD_BYTES})
        with self.assertRaisesRegex(ValueError, "too large"):                  # 40 000 caracteres = 80 000 bytes
            self.rec(trigger_ref="evt-3", payload={"x": "é" * 40000})
        self.assertEqual(self.log.summary()["total"], 1)                       # nada de lo rechazado quedó guardado

    def test_invalid_input_is_rejected_and_stores_nothing(self):
        bad = [dict(rule_id=" "), dict(policy_version=""), dict(trigger_ref=None), dict(kind=" "), dict(reason=""),
               dict(reason="x" * 501), dict(payload=["no", "es", "objeto"]), dict(payload={"n": float("nan")}),
               dict(payload={"n": float("inf")}), dict(kernel_refs={"task_id": 7}), dict(kernel_refs=["t-1"]),
               dict(kernel_refs={"k": "v" * 5000})]
        for kw in bad:
            with self.subTest(kw=list(kw)), self.assertRaises(ValueError):
                self.rec(**kw)
        self.assertEqual(self.log.summary()["total"], 0)

    def test_concurrent_records_of_the_same_key_create_exactly_one_row(self):
        results, errors = [], []
        def work():
            try:
                results.append(ShadowLog(self.db).record(rule_id="r1", policy_version="p1", trigger_ref="evt-1", kind="k",
                                                         payload={"a": 1}, reason="x"))
            except Exception as e:                                             # pragma: no cover
                errors.append(e)
        threads = [threading.Thread(target=work) for _ in range(8)]
        [t.start() for t in threads]; [t.join() for t in threads]
        self.assertEqual(errors, [])
        self.assertEqual(sum(r.created for r in results), 1)
        self.assertEqual(len({r.would_id for r in results}), 1)
        self.assertEqual(self.log.summary()["total"], 1)


class AppendOnlyTests(ShadowBase):
    def raw(self):
        c = sqlite3.connect(self.db, isolation_level=None)
        self.addCleanup(c.close)
        return c

    def test_would_have_done_rejects_update_and_delete(self):
        self.rec()
        c = self.raw()
        for sql in ("UPDATE would_have_done SET reason='x'", "DELETE FROM would_have_done"):
            with self.subTest(sql=sql), self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                c.execute(sql)
        self.assertEqual(self.log.summary()["total"], 1)

    def test_review_rejects_update_and_delete(self):
        w = self.rec().would_id
        self.log.review(w, Verdict.CORRECT)
        c = self.raw()
        for sql in ("UPDATE would_have_done_review SET verdict='WRONG'", "DELETE FROM would_have_done_review"):
            with self.subTest(sql=sql), self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                c.execute(sql)
        self.assertEqual(self.log.summary()["CORRECT"], 1)

    def test_database_itself_enforces_the_payload_cap_and_the_verdict_set(self):
        w = self.rec().would_id
        c = self.raw(); c.execute("PRAGMA foreign_keys=ON")
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("INSERT INTO would_have_done_review(would_id,verdict,reviewed_at) VALUES(?,?,1)", (w, "MAYBE"))
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("INSERT INTO would_have_done_review(would_id,verdict,reviewed_at) VALUES('nope','CORRECT',1)")
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("""INSERT INTO would_have_done(would_id,created_at,rule_id,policy_version,trigger_ref,kind,payload,
                         payload_sha256,payload_bytes,reason) VALUES('w9',1,'r','p','t','k','{}',?,65537,'x')""", ("0" * 64,))


class ReviewTests(ShadowBase):
    def test_review_validates_verdict_and_would_id(self):
        w = self.rec().would_id
        with self.assertRaisesRegex(ValueError, "verdict"):
            self.log.review(w, "MAYBE")
        with self.assertRaises(KeyError):
            self.log.review("no-existe", Verdict.CORRECT)
        self.assertEqual(self.log.summary()["unreviewed"], 1)

    def test_latest_review_wins_and_history_is_kept(self):
        w = self.rec().would_id
        r1 = self.log.review(w, "WRONG", "me equivoqué")
        r2 = self.log.review(w, Verdict.CORRECT, "corregido")
        self.assertGreater(r2, r1)
        s = self.log.summary()
        self.assertEqual((s["CORRECT"], s["WRONG"], s["unreviewed"], s["total"]), (1, 0, 0, 1))
        with closing(sqlite3.connect(self.db)) as c:        # `with connect()` solo hace commit: NO cierra (Windows)
            self.assertEqual(c.execute("SELECT COUNT(*) FROM would_have_done_review").fetchone()[0], 2)

    def test_summary_precision_ignores_ignored_and_unreviewed_and_filters_by_rule(self):
        ids = [self.rec(trigger_ref=f"e{i}").would_id for i in range(5)]
        other = self.rec(rule_id="r2").would_id
        for w, v in zip(ids[:4], ("CORRECT", "CORRECT", "CORRECT", "WRONG")):
            self.log.review(w, v)
        self.log.review(other, "IGNORED")
        s = self.log.summary("r1")
        self.assertEqual((s["total"], s["CORRECT"], s["WRONG"], s["IGNORED"], s["unreviewed"], s["precision"]), (5, 3, 1, 0, 1, 0.75))
        s = self.log.summary()
        self.assertEqual((s["total"], s["IGNORED"], s["precision"]), (6, 1, 0.75))
        self.assertIsNone(self.log.summary("r2")["precision"])                 # solo IGNORED: sin decisión, sin precisión
        self.assertIsNone(self.log.summary("no-existe")["precision"])
        self.assertEqual(self.log.summary("no-existe")["total"], 0)


class SchemaAndLayeringTests(ShadowBase):
    def test_schema_version_persists_across_reopen_and_data_survives(self):
        w = self.rec().would_id
        again = ShadowLog(self.db)
        self.assertEqual(again.schema_version(), SCHEMA_VERSION)
        self.assertEqual(again.get(w)["rule_id"], "r1")
        with closing(sqlite3.connect(self.db)) as c:        # `with connect()` solo hace commit: NO cierra (Windows)
            self.assertEqual(c.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0], 1)

    def test_newer_database_is_refused_untouched(self):
        self.rec()
        with closing(sqlite3.connect(self.db, isolation_level=None)) as c:   # autocommit: closing() no hace commit
            c.execute("UPDATE schema_version SET version=99")
        with self.assertRaisesRegex(RuntimeError, "newer than this code"):
            ShadowLog(self.db)
        with closing(sqlite3.connect(self.db)) as c:        # `with connect()` solo hace commit: NO cierra (Windows)
            self.assertEqual(c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0], 99)

    def test_leaves_no_open_connections_or_side_files(self):
        w = self.rec().would_id
        self.log.review(w, "CORRECT"); self.log.summary(); self.log.get(w); self.log.schema_version()   # toda la API pública
        self.assertEqual(sorted(p.name for p in Path(self.tmp.name).iterdir()), ["shadow.sqlite"])   # sin -wal/-shm

    def test_real_action_key_is_deterministic_and_distinct(self):
        a, b = self.rec().would_id, self.rec(trigger_ref="evt-2").would_id
        self.assertEqual(real_action_key(a), real_action_key(a))
        self.assertNotEqual(real_action_key(a), real_action_key(b))
        self.assertTrue(real_action_key(a).startswith("shadow:"))
        for bad in ("", " ", None):
            with self.assertRaises(ValueError):
                real_action_key(bad)

    def test_layer_only_depends_on_the_canonical_helper_of_dtos(self):
        """La capa es cliente del kernel: no importa `unae` ni nada de `dtos` salvo canonical_json."""
        imports = set()
        for f in SRC.glob("*.py"):
            for line in f.read_text(encoding="utf-8").splitlines():
                m = re.match(r"\s*(?:from|import)\s+([\w.]+)", line)
                if m and m.group(1).split(".")[0] in ("unae", "dtos", "adapter"):
                    imports.add(line.strip())
        self.assertEqual(imports, {"from dtos.canonical import canonical_json"})


if __name__ == "__main__":
    unittest.main()
