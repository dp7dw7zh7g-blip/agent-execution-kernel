"""Auditoría de solo lectura de evidencia persistida (dtos.scripts.audit_historical_data): detecta infracciones y NO modifica la base."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from dtos.canonical import canonical_json
from dtos.scripts import audit_historical_data as A
from dtos.store import MAX_EVIDENCE_SIZE_BYTES

SCHEMA = Path(A.__file__).resolve().parents[1] / "schema.sql"
OK = canonical_json({"ref": "x", "n": 1})


def make_db(path: str) -> None:
    with contextlib.closing(sqlite3.connect(path)) as c:
        c.executescript(SCHEMA.read_text(encoding="utf-8"))
        c.commit()


def add_effect(c, n, evidence):
    c.execute("INSERT INTO effects (effect_id, operation_id, intent_id, attempt_id, provider, idempotency_key, status, evidence) VALUES (?,?,?,?,?,?,?,?)",
              (f"e{n}", "op", "in", f"a{n}", "p", f"k{n}", "CONFIRMED", evidence))


def add_recon(c, n, evidence):
    c.execute("INSERT INTO effect_reconciliations (attempt_id, from_status, to_status, strategy, evidence) VALUES (?,?,?,?,?)",
              (f"a{n}", "UNKNOWN", "CONFIRMED", "s", evidence))


def digest(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class AuditHistoricalData(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / "t.db")
        make_db(self.db)

    def seed(self, fn):
        with contextlib.closing(sqlite3.connect(self.db)) as c:
            fn(c)
            c.commit()

    def findings(self):
        rep = A.audit(self.db)
        return rep, {(f["tabla"], f["gravedad"], f["motivo"].split(" (")[0]) for f in rep["findings"]}

    def test_clean_database_has_no_findings(self):
        def f(c):
            add_effect(c, 1, OK); add_effect(c, 2, None); add_recon(c, 1, OK)
            c.execute("INSERT INTO recovery_triggers (trigger_id, suspected_node_id, incarnation_at_suspicion, evidence, state) VALUES ('t','n',1,?,'SUSPECT')", (OK,))
        self.seed(f)
        rep, _ = self.findings()
        self.assertEqual((rep["violaciones"], rep["avisos"], rep["findings"]), (0, 0, []))
        self.assertEqual(rep["tables"]["effects.evidence"], {"filas": 2, "revisadas": 1, "nulas": 1})

    def test_non_finite_values_are_violations(self):
        def f(c):
            add_effect(c, 1, '{"a":NaN}'); add_recon(c, 1, '{"a":Infinity}'); add_effect(c, 2, '{"a":-Infinity}')
        self.seed(f)
        rep, found = self.findings()
        self.assertEqual(rep["violaciones"], 3)
        self.assertEqual({t for t, g, m in found}, {"effects", "effect_reconciliations"})

    def test_oversized_and_non_canonical_effect_evidence_are_violations(self):
        big = canonical_json({"blob": "x" * MAX_EVIDENCE_SIZE_BYTES})
        def f(c):
            add_effect(c, 1, big); add_recon(c, 1, '{"b": 1, "a": 2}')
        self.seed(f)
        _, found = self.findings()
        self.assertIn(("effects", "VIOLACION", f"tamaño {len(big.encode())} B > {MAX_EVIDENCE_SIZE_BYTES} B"), found)
        self.assertIn(("effect_reconciliations", "VIOLACION", "no está en forma canónica"), found)

    def test_size_counts_utf8_bytes_not_characters(self):
        text = canonical_json({"s": "é" * (MAX_EVIDENCE_SIZE_BYTES // 2)})  # < 65536 caracteres, > 65536 bytes
        self.assertLess(len(text), MAX_EVIDENCE_SIZE_BYTES)
        self.seed(lambda c: add_effect(c, 1, text))
        self.assertEqual(self.findings()[0]["violaciones"], 1)

    def test_invalid_json_and_null_in_not_null_column(self):
        def f(c):
            add_recon(c, 1, "{no json")
            c.execute("INSERT INTO recovery_triggers (trigger_id, suspected_node_id, incarnation_at_suspicion, evidence, state) VALUES ('t','n',1,'[1,]','SUSPECT')")
        self.seed(f)
        rep, found = self.findings()
        self.assertEqual({t for t, g, m in found}, {"effect_reconciliations", "recovery_triggers"})
        self.assertEqual(rep["violaciones"], 2)

    def test_recovery_trigger_size_and_form_are_only_warnings(self):
        big = json.dumps({"blob": "x" * MAX_EVIDENCE_SIZE_BYTES}, indent=1)
        self.seed(lambda c: c.execute("INSERT INTO recovery_triggers (trigger_id, suspected_node_id, incarnation_at_suspicion, evidence, state) VALUES ('t','n',1,?,'SUSPECT')", (big,)))
        rep, _ = self.findings()
        self.assertEqual((rep["violaciones"], rep["avisos"]), (0, 2))
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(A.main([self.db]), 0)

    def test_audit_never_modifies_the_database(self):
        def f(c):
            add_effect(c, 1, '{"a":NaN}'); add_recon(c, 1, OK)
        self.seed(f)
        before = digest(self.db)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(A.main([self.db]), 1)
            self.assertEqual(A.main([self.db, "--json"]), 1)
        self.assertEqual(digest(self.db), before)
        # Una base en modo WAL (schema.sql lo fija) hace que SQLite cree, incluso en modo lectura, los auxiliares -wal/-shm; el contenido de t.db no cambia.
        self.assertLessEqual({p.name for p in Path(self.tmp.name).iterdir()}, {"t.db", "t.db-wal", "t.db-shm"})
        with self.assertRaises(sqlite3.OperationalError), contextlib.closing(A.sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)) as ro:
            ro.execute("DELETE FROM effects")      # se cierra la conexión: en Windows un archivo abierto no se puede borrar al limpiar el directorio temporal

    def test_missing_database_is_exit_code_2(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(A.main([str(Path(self.tmp.name) / "no-existe.db")]), 2)
        self.assertFalse((Path(self.tmp.name) / "no-existe.db").exists(), "no debe crear la base")

    def test_database_before_patch_0003_without_evidence_column(self):
        with contextlib.closing(sqlite3.connect(self.db)) as c:
            c.executescript("DROP TRIGGER trg_effects_no_update; DROP TRIGGER trg_effects_no_delete; ALTER TABLE effects DROP COLUMN evidence;")
        rep = A.audit(self.db)
        self.assertEqual(rep["violaciones"], 0)
        self.assertIn("ausente", rep["tables"]["effects.evidence"]["estado"])


def main() -> int:
    r = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(AuditHistoricalData))
    return 0 if r.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
