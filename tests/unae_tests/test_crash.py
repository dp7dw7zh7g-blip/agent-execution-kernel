"""Prueba sistemática de caídas: SIGKILL en cada punto de commit del ciclo completo.

Después de cada caída se exige (1) que el estado persistido sea consistente tal cual quedó, y
(2) que un nuevo dueño pueda llevar el sistema a reposo: sin reservas retenidas ni eventos sin enviar.
Límite: mata procesos (SIGKILL), no simula pérdida de energía ni fallos de fsync del disco.
En Windows (sin signal.SIGKILL) toda la clase se OMITE con un mensaje explícito (g0.8 r20); no se ha ejecutado allí.
"""
import json
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path

from unae import Store, sign_receipt
from unae.store import SCHEMA_VERSION, CommitRequest

HERE = Path(__file__).resolve().parent
SCENARIO = str(HERE / "crash_scenario.py")
KEY = b"k" * 32


def child(db, mode):
    return subprocess.run([sys.executable, SCENARIO, db, mode], capture_output=True, text=True, timeout=60)


def setup_db(path):
    s = Store(path)
    for acct in ("__GLOBAL__", "task-1"):
        s.configure_account(acct, 10_000, 1_000_000, 10_000_000, 16)
    s.create_task("task-1", {"v": 0})


def quiesce(s):
    """Un dueño nuevo lleva todo a estado terminal usando solo la API pública."""
    t = s.acquire_authority("task-1", "drain")
    with closing(s.connect()) as c:
        ops = c.execute("SELECT operation_id,status,provider_id FROM execution_journal WHERE status NOT IN ('RECONCILED','CANCELLED')").fetchall()
    for op in ops:
        oid = op["operation_id"]
        if op["status"] == "UNKNOWN":
            assert s.begin_reconciliation(oid, t), oid
            kw = dict(receipt_id=f"dr-{oid}", operation_id=oid, reservation_id=f"res-{oid}", outcome="FAILED",
                      actual_compute=0, actual_time_ms=1, peak_memory_bytes=0, provider_operation_id=f"p-{oid}")
            sig = sign_receipt(KEY, provider_id=op["provider_id"], task_id="task-1", **kw) if op["provider_id"] else None
            assert s.record_receipt(provider_id=op["provider_id"], signature=sig, task_id="task-1" if sig else None, **kw) == "RECORDED", oid
        elif op["status"] == "RESERVED":
            assert s.release_reservation(oid, t), oid
            continue
        with closing(s.connect()) as c:
            rid = c.execute("SELECT receipt_id FROM receipts WHERE operation_id=?", (oid,)).fetchone()[0]
            version = c.execute("SELECT version FROM task_state WHERE task_id='task-1'").fetchone()[0]
        st = s.coordinated_commit(CommitRequest(f"dc-{oid}", "task-1", oid, f"res-{oid}", rid, version, t, {"v": version + 1}, f"de-{oid}"))
        assert st.value == "COMMITTED", (oid, st)
    for e in s.claim_events("drain", limit=1000, now=time.time() + 10_000):   # leases de despachadores muertos ya vencidos
        assert s.mark_event_sent(e["event_id"], "drain")


@unittest.skipUnless(hasattr(signal, "SIGKILL"),
                     "test_crash necesita SIGKILL (POSIX); en Windows no existe: el barrido de caídas solo está verificado en Linux/macOS")
class CrashTests(unittest.TestCase):
    def fresh(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        db = str(Path(tmp.name) / "u.sqlite"); setup_db(db)
        return db

    def test_clean_run_reaches_expected_final_state(self):
        db = self.fresh()
        r = child(db, "dry")
        self.assertEqual(r.returncode, 0, r.stderr)
        s = Store(db, keyring={"prov-A": KEY})
        self.assertEqual(s.verify_integrity(), [])
        with closing(s.connect()) as c:
            st = dict(c.execute("SELECT operation_id,status FROM execution_journal").fetchall())
            version = c.execute("SELECT version FROM task_state").fetchone()[0]
            held = c.execute("SELECT SUM(reserved_compute)+SUM(active_count) FROM budget_accounts").fetchone()[0]
            pending = c.execute("SELECT COUNT(*) FROM outbox WHERE status='PENDING'").fetchone()[0]
        self.assertEqual(st, {"a": "RECONCILED", "b": "CANCELLED", "c": "RECONCILED", "d": "CANCELLED", "e": "RECONCILED", "f": "CANCELLED", "g": "RECONCILED"})
        self.assertEqual((version, held, pending), (4, 0, 0))

    def test_sigkill_at_every_injection_point(self):
        points = json.loads(child(self.fresh(), "dry").stdout)
        self.assertGreater(len(points), 40)
        for i, name in enumerate(points, start=1):
            with self.subTest(point=i, name=name):
                db = self.fresh()
                r = child(db, str(i))
                self.assertEqual(r.returncode, -signal.SIGKILL, r.stderr)
                s = Store(db, keyring={"prov-A": KEY})
                self.assertEqual(s.verify_integrity(), [], "estado inconsistente justo después de la caída")
                quiesce(s)
                self.assertEqual(s.verify_integrity(), [], "inconsistente tras reposo")
                with closing(s.connect()) as c:
                    held = c.execute("SELECT SUM(reserved_compute)+SUM(reserved_time_ms)+SUM(active_memory_bytes)+SUM(active_count) FROM budget_accounts").fetchone()[0]
                    live = c.execute("SELECT COUNT(*) FROM execution_journal WHERE status NOT IN ('RECONCILED','CANCELLED')").fetchone()[0]
                    pending = c.execute("SELECT COUNT(*) FROM outbox WHERE status<>'SENT'").fetchone()[0]
                self.assertEqual((held, live, pending), (0, 0, 0), "recursos u operaciones retenidos tras la caída")

    def test_sigkill_during_schema_migration_is_atomic(self):
        db = self.fresh()
        s = Store(db, keyring={"prov-A": KEY})
        with closing(s.connect()) as c:                    # base como la dejaba 4.3N (v1)
            c.execute("ALTER TABLE outbox DROP COLUMN claimed_by"); c.execute("ALTER TABLE outbox DROP COLUMN claim_expires_at")
            c.execute("ALTER TABLE execution_journal DROP COLUMN provider_id")
            c.execute("ALTER TABLE receipts DROP COLUMN provider_id"); c.execute("ALTER TABLE receipts DROP COLUMN signature")
            for col in ("outcome", "attestation", "via_reconciliation"):
                c.execute(f"ALTER TABLE commit_log DROP COLUMN {col}")
            c.execute("DROP TRIGGER trg_journal_fsm"); c.execute("DROP TRIGGER trg_reservation_fsm")
            c.execute("UPDATE schema_version SET version=1")
        r = child(db, "migrate")
        self.assertEqual(r.returncode, -signal.SIGKILL, r.stderr)
        with closing(sqlite_raw(db)) as c:
            self.assertEqual(c.execute("SELECT version FROM schema_version").fetchone()[0], 1)
            cols = [x[1] for x in c.execute("PRAGMA table_info(outbox)").fetchall()]
            self.assertNotIn("claimed_by", cols)            # nada quedó a medias
        s2 = Store(db)                                      # y la siguiente apertura completa la migración
        self.assertEqual(s2.schema_version(), SCHEMA_VERSION)
        self.assertEqual(s2.verify_integrity(), [])


def sqlite_raw(path):
    import sqlite3
    return sqlite3.connect(path)


if __name__ == "__main__":
    unittest.main()
