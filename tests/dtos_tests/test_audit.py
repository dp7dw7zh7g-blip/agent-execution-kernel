"""
test_audit.py
=============
Verifica que AuditChain.verify() detecta manipulación incluso cuando el
atacante evade los triggers append-only de SQLite (DROP TRIGGER + editar
directo). Eso es justamente el caso que los triggers por sí solos NO
cubren: alguien con acceso al archivo .db fuera de este proceso.

Ejecutar: python3 -m dtos.test_audit
"""

from __future__ import annotations

import os
import sys
import tempfile

from dtos.audit import AuditChain, GENESIS_HASH
from dtos.canonical import content_hash
from dtos.enums import AuditIntegrityState
from dtos.store import SQLiteStore

FAILURES: list[str] = []


def check(condition: bool, description: str) -> None:
    status = "OK " if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        FAILURES.append(description)


def main() -> int:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = tmp.name

    try:
        store = SQLiteStore(db_path)
        chain = AuditChain(store)

        # ===============================================================
        # 1. Cadena vacía es VALID trivialmente
        # ===============================================================
        result = chain.verify()
        check(
            result.status == AuditIntegrityState.VALID and result.total_entries == 0,
            "Cadena vacía se verifica como VALID con 0 entradas",
        )

        # ===============================================================
        # 2. Cadena normal, sin manipular, es VALID
        # ===============================================================
        h1 = chain.append({"event": "genesis_authority_written", "installation_id": "install-1"})
        h2 = chain.append({"event": "cognitive_cycle_created", "cycle_id": "c-1"})
        h3 = chain.append({"event": "cognitive_cycle_completed", "cycle_id": "c-1"})

        check(h1 != h2 != h3, "Cada entry_hash es distinto (depende del payload y del prev_hash)")

        result = chain.verify()
        check(
            result.status == AuditIntegrityState.VALID and result.total_entries == 3,
            "Cadena de 3 entradas sin manipular se verifica como VALID",
        )
        check(chain.last_hash() == h3, "last_hash() devuelve el hash de la entrada más reciente")

        # Recalcular manualmente el primer hash para confirmar que
        # AuditChain no está inventando la forma del hash internamente:
        expected_h1 = content_hash(
            {"prev_hash": GENESIS_HASH, "payload": {"event": "genesis_authority_written", "installation_id": "install-1"}}
        )
        check(h1 == expected_h1, "El primer entry_hash es exactamente content_hash({GENESIS_HASH, payload})")

        # ===============================================================
        # 3. CORRUPTED: payload modificado directo, evadiendo el trigger
        # ===============================================================
        store._conn.execute("DROP TRIGGER trg_audit_log_no_update;")
        store._conn.execute(
            "UPDATE audit_log SET payload = '{\"event\":\"TAMPERED\"}' WHERE seq = 2"
        )
        store._conn.commit()

        result = chain.verify()
        check(
            result.status == AuditIntegrityState.CORRUPTED and result.first_bad_seq == 2,
            "Payload modificado directo (con trigger removido a propósito) es detectado como CORRUPTED en seq=2",
        )

        # Restaurar para los siguientes casos: recrear DB limpia en vez
        # de intentar deshacer la manipulación (más simple y más honesto
        # sobre qué se está probando en cada bloque).
        store.close()
        os.unlink(db_path)
        tmp2 = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp2.close()
        db_path = tmp2.name
        store = SQLiteStore(db_path)
        chain = AuditChain(store)

        # ===============================================================
        # 4. TRUNCATED: una fila eliminada directo, evadiendo el trigger
        # ===============================================================
        chain.append({"event": "a"})
        chain.append({"event": "b"})
        chain.append({"event": "c"})

        store._conn.execute("DROP TRIGGER trg_audit_log_no_delete;")
        store._conn.execute("DELETE FROM audit_log WHERE seq = 2")
        store._conn.commit()

        result = chain.verify()
        check(
            result.status == AuditIntegrityState.TRUNCATED and result.first_bad_seq == 2,
            "Fila eliminada directo (con trigger removido a propósito) es detectada como TRUNCATED en seq=2",
        )

        store.close()
        os.unlink(db_path)
        tmp3 = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp3.close()
        db_path = tmp3.name
        store = SQLiteStore(db_path)
        chain = AuditChain(store)

        # ===============================================================
        # 5. FORKED: una fila insertada con prev_hash que no es el real
        # ===============================================================
        chain.append({"event": "a"})
        chain.append({"event": "b"})
        # INSERT no está bloqueado por los triggers (solo UPDATE/DELETE);
        # esto simula una inserción maliciosa que intenta "reescribir"
        # la cadena apuntando a un prev_hash viejo/inventado en vez del
        # último real.
        fake_prev = "f" * 64
        fake_payload = {"event": "forged"}
        fake_hash = content_hash({"prev_hash": fake_prev, "payload": fake_payload})
        store._conn.execute(
            "INSERT INTO audit_log (entry_hash, prev_hash, payload) VALUES (?, ?, ?)",
            (fake_hash, fake_prev, '{"event": "forged"}'),
        )
        store._conn.commit()

        result = chain.verify()
        check(
            result.status == AuditIntegrityState.FORKED and result.first_bad_seq == 3,
            "Fila insertada con prev_hash falso es detectada como FORKED en seq=3, "
            "aunque su propio entry_hash sea internamente consistente con SU prev_hash falso",
        )

        store.close()

        # -- Resumen ------------------------------------------------------
        print()
        if FAILURES:
            print(f"{len(FAILURES)} test(s) fallaron:")
            for f in FAILURES:
                print(f"  - {f}")
            return 1
        print("Todos los audit tests pasaron.")
        return 0

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


if __name__ == "__main__":
    sys.exit(main())
