"""
test_mandates.py
================
Parche 0009 (D-52): REG-017 Mandates (`mandates`, `mandate_events`) y `policies` inmutable.

Corre contra un SQLite real (archivo temporal): las garantías de SQL (CHECK, UNIQUE, claves foráneas, triggers append-only) son lo
que se verifica, no un mock.

Ejecutar: python run_tests.py dtos
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import tempfile

from dtos.canonical import canonical_json
from dtos.enums import MandateEventType, MandateStrength, MandateTriggerKind
from dtos.registries import ALLOWED_MANDATE_COMBINATIONS, MandateRecord, PolicyRecord, policy_content_hash
from dtos.store import SQLiteStore
from dtos.validation import ContractValidationError

FAILURES: list[str] = []
S, T = MandateStrength, MandateTriggerKind


def check(condition: bool, description: str) -> None:
    print(f"[{'OK ' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def expect_raises(exc_type, fn, description: str, contains: str | None = None) -> None:
    try:
        fn()
        check(False, description + " (no lanzó excepción)")
    except exc_type as e:
        check(contains is None or contains in str(e), description + ("" if contains is None else f" [mensaje con «{contains}»]"))
    except Exception as e:
        check(False, f"{description} (lanzó {type(e).__name__}: {e})")


def mandate(mid="m-1", task="t-1", strength=S.EXPLICIT, trigger=T.USER, **rule) -> MandateRecord:
    return MandateRecord(mandate_id=mid, task_id=task, strength=strength, trigger_kind=trigger, created_at="2026-11-02T10:00:00Z", **rule)


RULE_HASH = hashlib.sha256(b"r-1").hexdigest()
RULE = dict(rule_id="r-1", policy_id="pol-1", policy_version="1", rule_hash=RULE_HASH)


def count(conn, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def main() -> int:
    # ---------------------------------------------------------------
    # MandateRecord (DTO)
    # ---------------------------------------------------------------
    m_user = mandate()
    check(m_user.rule_id is None and m_user.strength is S.EXPLICIT, "mandato USER sin regla se construye")
    m_sensor = mandate("m-s", "t-s", S.INFERRED, T.SENSOR, **RULE)
    check(m_sensor.rule_hash == RULE_HASH, "mandato SENSOR con regla completa se construye")
    expect_raises(ContractValidationError, lambda: mandate(trigger=T.SENSOR), "SENSOR sin regla -> error")
    for missing in ("rule_id", "policy_id", "policy_version", "rule_hash"):
        partial = {k: v for k, v in RULE.items() if k != missing}
        expect_raises(ContractValidationError, lambda p=partial: mandate(trigger=T.SENSOR, **p), f"regla sin {missing} -> error (van juntos o ninguno)")
    expect_raises(ContractValidationError, lambda: mandate(rule_id="r-1"), "regla parcial en un mandato no SENSOR -> error")
    expect_raises(ContractValidationError, lambda: mandate(strength="EXPLICIT"), "strength como str (no enum) -> error")
    expect_raises(ContractValidationError, lambda: mandate(trigger="USER"), "trigger_kind como str (no enum) -> error")
    expect_raises(ContractValidationError, lambda: mandate(mid=" "), "mandate_id vacío -> error")
    expect_raises(ContractValidationError, lambda: mandate(task=""), "task_id vacío -> error")
    expect_raises(ContractValidationError, lambda: mandate("m", "t", S.EXPLICIT, T.USER, rule_id=" ", policy_id="p", policy_version="1", rule_hash="h"),
                  "campo de regla en blanco -> error")
    check(len(ALLOWED_MANDATE_COMBINATIONS) == 6, "PROVISIONAL: la matriz admite hoy las seis combinaciones (la cerrada por el usuario no está registrada)")
    check(set(MandateEventType) == {MandateEventType.MANDATE_CREATED}, "solo existe el evento MANDATE_CREATED (los de revalidación llegan con el 0011)")
    check(json.loads(canonical_json(m_sensor.to_canonical_dict()))["trigger_kind"] == "SENSOR", "to_canonical_dict serializa los enums como texto")

    # ---------------------------------------------------------------
    # Store contra SQLite real
    # ---------------------------------------------------------------
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db = tmp.name
    try:
        s = SQLiteStore(db)
        c = s._conn
        for tid in ("t-1", "t-2", "t-s", "t-x"):
            c.execute("INSERT INTO tasks (task_id, goal_id, status) VALUES (?, 'g-1', 'ACTIVE')", (tid,))
        content = canonical_json({"rules": [{"rule_id": "r-1"}]})
        s.insert_policy(PolicyRecord("pol-1", "1", "kernel", "2026-11-01T00:00:00Z", policy_content_hash(content), content))

        # --- creación atómica: mandato + evento
        s.create_mandate(m_user, "ev-1")
        check(s.get_mandate("m-1") == m_user, "create_mandate + get_mandate: ida y vuelta idéntica")
        check(s.get_mandate_for_task("t-1") == m_user, "get_mandate_for_task devuelve el mandato de la Task")
        evs = s.get_mandate_events("m-1")
        check(len(evs) == 1 and evs[0]["event_type"] == "MANDATE_CREATED" and evs[0]["event_id"] == "ev-1", "queda exactamente un evento MANDATE_CREATED")
        check(evs[0]["payload"] == canonical_json(m_user.to_canonical_dict()), "el payload del evento es el JSON canónico del mandato")
        s.create_mandate(m_sensor, "ev-2")
        got = s.get_mandate("m-s")
        check(got == m_sensor and got.trigger_kind is T.SENSOR and got.rule_hash == RULE_HASH, "mandato SENSOR con regla: ida y vuelta idéntica")
        check(s.get_mandate("no-existe") is None and s.get_mandate_for_task("no-existe") is None, "mandato inexistente -> None")
        check(s.get_mandate_events("no-existe") == [], "eventos de un mandato inexistente -> lista vacía")
        expect_raises(ContractValidationError, lambda: s.create_mandate(mandate("m-9", "t-2"), " "), "event_id vacío -> error y no se escribe nada")
        check(count(c, "mandates") == 2 and count(c, "mandate_events") == 2, "el event_id inválido no dejó filas")

        # --- unicidad y claves foráneas, sin filas huérfanas
        before = (count(c, "mandates"), count(c, "mandate_events"))
        expect_raises(sqlite3.IntegrityError, lambda: s.create_mandate(mandate("m-otro", "t-1"), "ev-3"), "segundo mandato para la misma Task -> IntegrityError")
        expect_raises(sqlite3.IntegrityError, lambda: s.create_mandate(mandate("m-1", "t-2"), "ev-4"), "mandate_id repetido -> IntegrityError")
        expect_raises(sqlite3.IntegrityError, lambda: s.create_mandate(mandate("m-3", "t-2"), "ev-1"), "event_id repetido -> IntegrityError")
        expect_raises(sqlite3.IntegrityError, lambda: s.create_mandate(mandate("m-3", "t-inexistente"), "ev-5"), "Task inexistente -> IntegrityError (clave foránea)")
        expect_raises(sqlite3.IntegrityError,
                      lambda: s.create_mandate(mandate("m-3", "t-2", S.EXPLICIT, T.SENSOR, **{**RULE, "policy_id": "pol-no"}), "ev-6"),
                      "política inexistente -> IntegrityError (clave foránea)")
        expect_raises(sqlite3.IntegrityError,
                      lambda: s.create_mandate(mandate("m-3", "t-2", S.EXPLICIT, T.SENSOR, **{**RULE, "policy_version": "9"}), "ev-6"),
                      "versión de política inexistente -> IntegrityError (clave foránea)")
        check((count(c, "mandates"), count(c, "mandate_events")) == before, "ningún intento fallido dejó filas (ni mandato ni evento huérfano)")

        # --- los CHECK de SQL, saltándose el DTO
        raw_cols = "(mandate_id, task_id, strength, trigger_kind, created_at, rule_id, policy_id, policy_version, rule_hash)"

        def raw_insert(*vals):
            c.execute(f"INSERT INTO mandates {raw_cols} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", vals)

        expect_raises(sqlite3.IntegrityError, lambda: raw_insert("r1", "t-x", "EXPLICIT", "SENSOR", "x", None, None, None, None), "SQL: SENSOR sin regla se rechaza")
        expect_raises(sqlite3.IntegrityError, lambda: raw_insert("r2", "t-x", "FUERTE", "USER", "x", None, None, None, None), "SQL: strength fuera del enum se rechaza")
        expect_raises(sqlite3.IntegrityError, lambda: raw_insert("r3", "t-x", "EXPLICIT", "ROBOT", "x", None, None, None, None), "SQL: trigger_kind fuera del enum se rechaza")
        expect_raises(sqlite3.IntegrityError, lambda: raw_insert("r4", "t-x", "EXPLICIT", "USER", "x", "r-1", None, None, None), "SQL: regla parcial se rechaza")
        check(count(c, "mandates") == 2, "los rechazos de SQL no dejaron filas")

        # --- append-only: mandates, mandate_events y policies
        for sql, what in (
            ("UPDATE mandates SET strength = 'INFERRED' WHERE mandate_id = 'm-1'", "UPDATE en mandates"),
            ("DELETE FROM mandates WHERE mandate_id = 'm-1'", "DELETE en mandates"),
            ("UPDATE mandate_events SET payload = '{}' WHERE event_id = 'ev-1'", "UPDATE en mandate_events"),
            ("DELETE FROM mandate_events WHERE event_id = 'ev-1'", "DELETE en mandate_events"),
            ("UPDATE policies SET content = NULL WHERE policy_id = 'pol-1'", "UPDATE en policies"),
            ("UPDATE policies SET hash = 'otro' WHERE policy_id = 'pol-1'", "UPDATE del hash en policies"),
            ("DELETE FROM policies WHERE policy_id = 'pol-1'", "DELETE en policies"),
        ):
            expect_raises(sqlite3.DatabaseError, lambda q=sql: c.execute(q), f"{what} -> rechazado por trigger", contains="append-only")
        check(s.get_mandate("m-1") == m_user and len(s.get_mandate_events("m-1")) == 1, "los intentos de modificar no cambiaron nada")
        check(s.get_policy("pol-1", "1") is not None and s.get_policy("pol-1", "1").content == content, "la política sigue intacta")
        s.insert_policy(PolicyRecord("pol-1", "2", "kernel", "2026-11-02T00:00:00Z", "hash-v2"))
        check(s.get_policy("pol-1", "2") is not None and s.get_policy("pol-1", "1").content == content, "un cambio de política es una versión nueva: la anterior no se toca")
        s.close()

        # --- base anterior al parche: sin tablas ni triggers -> se crean al abrir (CREATE ... IF NOT EXISTS)
        raw = sqlite3.connect(db, isolation_level=None)
        for trg in ("trg_policies_no_update", "trg_policies_no_delete"):
            raw.execute(f"DROP TRIGGER {trg}")
        raw.execute("DROP TABLE mandate_events")
        raw.execute("DROP TABLE mandates")
        names = {r[0] for r in raw.execute("SELECT name FROM sqlite_master")}
        raw.close()
        check("mandates" not in names and "trg_policies_no_update" not in names, "preparación: la base simulada no tiene REG-017 ni los triggers de policies")
        s1 = SQLiteStore(db)
        names = {r[0] for r in s1._conn.execute("SELECT name FROM sqlite_master")}
        check({"mandates", "mandate_events", "trg_policies_no_update", "trg_policies_no_delete", "trg_mandates_no_update"} <= names,
              "migración: al abrirse una base anterior se crean las tablas REG-017 y los triggers")
        expect_raises(sqlite3.DatabaseError, lambda: s1._conn.execute("UPDATE policies SET scope = 'x'"), "migración: policies queda inmutable en la base migrada", contains="append-only")
        s1.create_mandate(mandate("m-n", "t-2"), "ev-n")
        check(s1.get_mandate("m-n") is not None, "migración: tras migrar se pueden crear mandatos")
        s1.close()
        s2 = SQLiteStore(db)  # idempotente
        check(s2.get_mandate("m-n") is not None, "migración idempotente: una segunda apertura no falla ni pierde datos")
        s2.close()
    finally:
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(db + suffix)
            except OSError:
                pass

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FALLO(S):")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("test_mandates: todo OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
