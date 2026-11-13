"""Pruebas de semántica: qué significa RECONCILED, qué estados retienen recursos, quién puede mover una operación
y qué NO garantizan outbox/inbox."""
import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path

from unae import Store, CommitStatus, sign_receipt
from unae.store import CommitRequest, fsm_table

KEY = b"k" * 32


class SemanticsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = Store(str(Path(self.tmp.name) / "u.sqlite"), keyring={"prov-A": KEY})
        for a in ("__GLOBAL__", "task-1"):
            self.s.configure_account(a, 1000, 100000, 10_000_000, 8)
        self.s.create_task("task-1", {"v": 0})
        self.version = 0

    def tearDown(self):
        self.s.close(); self.tmp.cleanup()

    # ---- helpers ----
    def start(self, op, token=1, provider=True):
        self.assertEqual(self.s.reserve_and_register(task_id="task-1", operation_id=op, reservation_id=f"res-{op}", idempotency_key=f"k-{op}",
            fencing_token=token, compute_units=10, time_ms=100, memory_bytes=1024), "RESERVED")
        self.assertTrue(self.s.mark_running(op, token, f"p-{op}", provider_id="prov-A" if provider else None))

    def receipt(self, op, signed=True, outcome="SUCCEEDED", compute=7):
        kw = dict(receipt_id=f"r-{op}", operation_id=op, reservation_id=f"res-{op}", outcome=outcome, actual_compute=compute,
                  actual_time_ms=5, peak_memory_bytes=5, provider_operation_id=f"p-{op}")
        if signed:
            return self.s.record_receipt(provider_id="prov-A", task_id="task-1", signature=sign_receipt(KEY, provider_id="prov-A", task_id="task-1", **kw), **kw)
        return self.s.record_receipt(**kw)

    def commit(self, op, token=1):
        st = self.s.coordinated_commit(CommitRequest(f"c-{op}", "task-1", op, f"res-{op}", f"r-{op}", self.version, token, {"v": self.version + 1}, f"e-{op}"))
        self.assertEqual(st, CommitStatus.COMMITTED); self.version += 1

    def acct(self):
        with closing(self.s.connect()) as c:
            return tuple(c.execute("SELECT reserved_compute,reserved_time_ms,active_memory_bytes,active_count,compute_used FROM budget_accounts WHERE account_id='__GLOBAL__'").fetchone())

    # ---- 1. RECONCILED y niveles de evidencia ----
    def test_reconciled_records_how_strong_the_evidence_is(self):
        self.start("a"); self.receipt("a"); self.commit("a")                                   # firmado, directo
        self.start("b", provider=False); self.receipt("b", signed=False); self.commit("b")     # sin firma
        self.start("c")
        t = self.s.acquire_authority("task-1", "w2")                                           # c -> UNKNOWN
        self.assertTrue(self.s.begin_reconciliation("c", t)); self.receipt("c", outcome="FAILED", compute=0); self.commit("c", t)
        with closing(self.s.connect()) as cx:
            rows = {r["operation_id"]: (r["outcome"], r["attestation"], r["via_reconciliation"]) for r in
                    cx.execute("SELECT operation_id,outcome,attestation,via_reconciliation FROM commit_log")}
        self.assertEqual(rows, {"a": ("SUCCEEDED", "PROVIDER_SIGNED", 0), "b": ("SUCCEEDED", "UNATTESTED", 0), "c": ("FAILED", "PROVIDER_SIGNED", 1)})
        self.assertEqual(sorted(x["operation_id"] for x in self.s.list_unverified_settlements()), ["b", "c"])   # 'a' es la única con evidencia directa
        self.assertEqual(self.s.verify_integrity(), [])

    # ---- 2. contabilidad: qué estados retienen y cuáles consumen ----
    def test_which_states_hold_resources_and_which_consume(self):
        self.assertEqual(self.acct(), (0, 0, 0, 0, 0))
        self.start("x")                                                                          # RUNNING retiene
        self.assertEqual(self.acct(), (10, 100, 1024, 1, 0))
        t = self.s.acquire_authority("task-1", "w2")                                             # UNKNOWN sigue reteniendo (cota superior)
        self.assertEqual(self.acct(), (10, 100, 1024, 1, 0))
        self.receipt("x"); self.assertEqual(self.acct(), (10, 100, 1024, 1, 0))                  # SUCCEEDED sin commit sigue reteniendo
        self.commit("x", t)                                                                      # RECONCILED: libera y consume lo REAL
        self.assertEqual(self.acct(), (0, 0, 0, 0, 7))
        self.start("y", token=t)
        self.assertTrue(self.s.release_reservation("y", t) is False)                             # RUNNING no se libera sin reconciliar
        self.assertEqual(self.acct(), (10, 100, 1024, 1, 7))
        self.s.reserve_and_register(task_id="task-1", operation_id="z", reservation_id="res-z", idempotency_key="k-z", fencing_token=t,
                                    compute_units=3, time_ms=1, memory_bytes=1)
        self.assertTrue(self.s.release_reservation("z", t))                                      # CANCELLED libera y NO consume
        self.assertEqual(self.acct(), (10, 100, 1024, 1, 7))
        self.assertEqual(self.s.verify_integrity(), [])

    # ---- 3. la FSM la hace cumplir la base, no solo el código ----
    def test_database_enforces_the_single_state_machine(self):
        self.start("a")
        illegal = [("execution_journal", "RECONCILED"), ("execution_journal", "CANCELLED"), ("reservations", "RESERVED")]
        for table, new in illegal:
            with self.subTest(table=table, new=new), closing(self.s.connect()) as c:
                with self.assertRaises(sqlite3.DatabaseError):
                    c.execute(f"UPDATE {table} SET status=? WHERE {'operation_id' if table == 'execution_journal' else 'reservation_id'}=?",
                              (new, "a" if table == "execution_journal" else "res-a"))
        self.assertEqual(fsm_table()["CANCELLED"], [])                                           # terminal
        self.assertEqual(fsm_table()["RESERVED"], ["CANCELLED", "RUNNING"])
        with closing(self.s.connect()) as c:                                                     # manipular el trigger se detecta
            c.execute("DROP TRIGGER trg_journal_fsm")
        self.assertTrue(any("trg_journal_fsm" in p for p in self.s.verify_integrity()))

    # ---- 4. outbox/inbox: propiedades distintas, ninguna evita duplicados en sistemas externos ----
    def test_outbox_and_inbox_do_not_deduplicate_external_effects(self):
        self.start("a", provider=False); self.receipt("a", signed=False); self.commit("a")
        sink = []                                                                                # sistema externo SIN idempotencia
        def slow(event):
            sink.append(event["event_id"])
            self.s.claim_events("w2", now=time.time() + 100)                                     # w1 pierde el lease durante el envío
        self.assertEqual(self.s.dispatch_outbox(slow, worker_id="w1", lease_s=30)["lost"], 1)
        for e in self.s.claim_events("w2", now=time.time() + 200):                                # w2 reenvía el mismo evento
            sink.append(e["event_id"]); self.assertTrue(self.s.mark_event_sent(e["event_id"], "w2"))
        self.assertEqual(sink, ["e-a", "e-a"])                                                   # el efecto externo ocurrió DOS veces
        self.assertEqual(len(set(sink)), 1)                                                      # deduplicable solo si el destino usa event_id como clave
        with closing(self.s.connect()) as c:
            c.execute("CREATE TABLE local_effect(event_id TEXT)"); c.execute("PRAGMA foreign_keys=ON")
        apply = lambda conn, eid: conn.execute("INSERT INTO local_effect VALUES(?)", (eid,))      # noqa: E731
        self.assertTrue(self.s.consume_once("consumer-1", "e-a", apply))
        self.assertFalse(self.s.consume_once("consumer-1", "e-a", apply))                        # el inbox sí evita el duplicado LOCAL
        with closing(self.s.connect()) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM local_effect").fetchone()[0], 1)

    # ---- 5. FSM: contrato canónico obligatorio y coherencia en proceso con DTOS y el adaptador (D-80) ----
    def test_fsm_matches_canonical_contract_and_status_enum(self):
        import unae
        canonical_path = Path(unae.__file__).resolve().parent / "fsm.json"
        with canonical_path.open("r", encoding="utf-8") as handle:
            canonical = json.load(handle)
        canonical = {key: sorted(value) for key, value in canonical.items()}
        self.assertEqual(canonical, fsm_table())
        from unae.store import Status
        self.assertEqual(set(canonical), {status.value for status in Status})
        for state, targets in canonical.items():
            self.assertEqual(len(targets), len(set(targets)), f"transiciones duplicadas en {state}")
            self.assertTrue(set(targets).issubset(canonical), f"destino desconocido en {state}")

    # D-80: la «paridad con dtos/» se omitía siempre. El contrato (I-01, I-04) declara las FSM separadas y sin estados equiparados: la tabla de UNAE
    # (RESERVED…RECONCILED) no puede ser igual a ninguna de DTOS (EffectStatus: INTENDED…RECONCILIATION_REQUIRED). Lo que SÍ se puede vigilar en proceso es
    # que cada lado sea coherente consigo mismo y que el adaptador solo nombre estados que UNAE tiene.
    def test_dtos_effect_fsm_is_self_consistent_and_not_equated_with_unae(self):
        from dtos.effect_fsm import EffectFSM
        from dtos.enums import EffectStatus
        table = {k.value: sorted(x.value for x in v) for k, v in EffectFSM.transitions.items()}
        self.assertEqual(set(table), {st.value for st in EffectStatus}, "EffectFSM debe cubrir exactamente EffectStatus")
        for state, targets in table.items():
            self.assertTrue(set(targets).issubset(table), f"destino desconocido en DTOS {state}")
        self.assertNotEqual(table, fsm_table(), "I-01: las FSM de UNAE y DTOS no se fusionan ni se equiparan")

    def test_adapter_only_names_unae_states_that_exist(self):
        from adapter import bridge
        known = set(fsm_table())
        self.assertTrue(set(bridge._ROUTE_BY_STATUS) <= known, "el puente enruta por un estado que UNAE no tiene")
        self.assertTrue(set(bridge._REPLAY_STATUSES) <= known, "el puente reenvía recibos de un estado que UNAE no tiene")
        self.assertIn("RESERVED", known)   # dispatch.py solo despacha desde RESERVED


if __name__ == "__main__":
    unittest.main()
