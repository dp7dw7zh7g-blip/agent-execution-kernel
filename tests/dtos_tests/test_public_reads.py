"""
test_public_reads.py (parche 0012, D-63)
========================================
Lecturas públicas de SQLiteStore que reemplazan el acceso a `store._conn` desde fuera del Store:
`get_effect_by_provider_key`, `read_audit_chain`, `get_last_audit_hash` y `get_pending_outbox`.
También vigila que `audit.py` e `inbox_outbox.py` ya no toquen la conexión privada.

Ejecutar: python run_tests.py dtos
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

from dtos.audit import GENESIS_HASH, AuditChain
from dtos.enums import AuditIntegrityState
from dtos.inbox_outbox import InboxProcessor, OutboxPublisher
from dtos.store import SQLiteStore

FAILURES: list[str] = []
DTOS_SRC = Path(__file__).resolve().parents[2] / "src" / "dtos"


def check(condition: bool, description: str) -> None:
    print(f"[{'OK ' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def main() -> int:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    try:
        s = SQLiteStore(tmp.name)

        # --- get_effect_by_provider_key ---------------------------------------------------------------------------------------------
        check(s.get_effect_by_provider_key("prov-A", "idem-1") is None, "get_effect_by_provider_key: None si no hay fila")
        s.insert_effect(effect_id="e1", operation_id="op1", intent_id="at1", attempt_id="at1", provider="prov-A",
                        idempotency_key="idem-1", status="CONFIRMED")
        row = s.get_effect_by_provider_key("prov-A", "idem-1")
        check(row is not None and row["effect_id"] == "e1" and row["operation_id"] == "op1" and row["attempt_id"] == "at1"
              and row["status"] == "CONFIRMED", "get_effect_by_provider_key: devuelve la fila como dict con sus columnas")
        check(s.get_effect_by_provider_key("prov-B", "idem-1") is None and s.get_effect_by_provider_key("prov-A", "idem-2") is None,
              "get_effect_by_provider_key: la clave es el PAR (provider, idempotency_key)")
        row["status"] = "MUTADO"
        check(s.get_effect_by_provider_key("prov-A", "idem-1")["status"] == "CONFIRMED", "get_effect_by_provider_key: devuelve una copia")

        # --- auditoría ---------------------------------------------------------------------------------------------------------------
        chain = AuditChain(s)
        check(s.read_audit_chain() == [] and s.get_last_audit_hash() is None, "cadena vacía: read_audit_chain() == [] y último hash None")
        check(chain.last_hash() == GENESIS_HASH, "cadena vacía: AuditChain.last_hash() sigue siendo GENESIS_HASH")
        h1 = chain.append({"k": 1})
        h2 = chain.append({"k": 2})
        rows = s.read_audit_chain()
        check([r[0] for r in rows] == sorted(r[0] for r in rows) and len(rows) == 2, "read_audit_chain: en orden de seq")
        check(rows[0][1] == h1 and rows[1][1] == h2 and rows[1][2] == h1 and rows[0][2] == GENESIS_HASH,
              "read_audit_chain: (seq, entry_hash, prev_hash, payload) encadenados")
        check(isinstance(rows[0], tuple) and isinstance(rows[0][3], str), "read_audit_chain: tuplas con el payload como texto")
        check(s.get_last_audit_hash() == h2 and chain.last_hash() == h2, "get_last_audit_hash == último entry_hash == AuditChain.last_hash()")
        check(chain.verify().status == AuditIntegrityState.VALID and chain.verify().total_entries == 2, "AuditChain.verify() sigue validando la cadena")

        # --- outbox ------------------------------------------------------------------------------------------------------------------
        check(s.get_pending_outbox() == [], "outbox vacío: get_pending_outbox() == []")
        with s.transaction() as c:
            for i in range(3):
                c.execute("INSERT INTO outbox (event_type, payload) VALUES (?, ?)", (f"T{i}", f'{{"i":{i}}}'))
        pend = s.get_pending_outbox()
        check([p[1] for p in pend] == ["T0", "T1", "T2"] and all(isinstance(p, tuple) and len(p) == 3 for p in pend),
              "get_pending_outbox: (outbox_id, event_type, payload) en orden de outbox_id")
        check(len(s.get_pending_outbox(2)) == 2 and s.get_pending_outbox(2)[0][1] == "T0", "get_pending_outbox: respeta el límite")
        sent: list[str] = []
        res = OutboxPublisher(s).publish_pending(lambda t, p: sent.append(t))
        check(res.published_count == 3 and sent == ["T0", "T1", "T2"], "OutboxPublisher.publish_pending sigue publicando por la lectura pública")
        check(s.get_pending_outbox() == [], "tras publicar no queda nada pendiente")
        check(InboxProcessor(s) is not None, "InboxProcessor se construye sin cambios")
        s.close()

        # --- ya no hay acceso a la conexión privada desde audit.py ni inbox_outbox.py ------------------------------------------------
        for name in ("audit.py", "inbox_outbox.py"):
            code = (DTOS_SRC / name).read_text(encoding="utf-8")
            check(re.search(r"\._conn\b", code) is None, f"{name}: no accede a `_conn` del Store")
    finally:
        for suffix in ("", "-wal", "-shm"):
            if os.path.exists(tmp.name + suffix):
                os.unlink(tmp.name + suffix)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} comprobación(es) fallida(s):")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("Todas las comprobaciones pasaron.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
