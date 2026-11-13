"""
test_mandate_rule_hash.py
=========================
D-53: validación estricta de la referencia a la regla en `mandates` (rule_hash, policy_id, policy_version).

Un mandato con regla (obligatorio si trigger_kind = SENSOR) no puede quedar con (1) un rule_hash que no sea SHA-256 hex minúscula de
64 caracteres, (2) una política inexistente o (3) una política sin contenido. Se comprueba en TRES capas independientes: el DTO
(MandateRecord), `SQLiteStore.create_mandate` y los triggers de SQLite (una conexión que se salte Python, o con foreign_keys = OFF).

Límite documentado (NO es un defecto de esta prueba): que rule_hash sea el hash de la regla `rule_id` DENTRO de `content` lo comprueba el
parche 0011, porque el formato de las reglas aún no está definido; aquí se verifica que la prueba lo declara explícitamente.

Corre contra un SQLite real. Falla contra un schema.sql sin los triggers `trg_mandates_*` de D-53.
Ejecutar: python run_tests.py dtos
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import sys
import tempfile

from dtos.canonical import canonical_json
from dtos.enums import MandateStrength as S, MandateTriggerKind as T
from dtos.registries import MandateRecord, PolicyRecord, policy_content_hash
from dtos.store import SQLiteStore
from dtos.validation import ContractValidationError

FAILURES: list[str] = []
GUARDS = ("trg_mandates_rule_hash_format", "trg_mandates_policy_must_exist", "trg_mandates_policy_must_have_content")
GOOD = hashlib.sha256(b"regla").hexdigest()
COLS = "(mandate_id, task_id, strength, trigger_kind, created_at, rule_id, policy_id, policy_version, rule_hash)"


def check(cond: bool, desc: str) -> None:
    print(f"[{'OK ' if cond else 'FAIL'}] {desc}")
    if not cond:
        FAILURES.append(desc)


def raises(exc, fn, desc: str, contains: str | None = None) -> None:
    try:
        fn()
    except exc as e:
        check(contains is None or contains in str(e), desc + ("" if contains is None else f" [«{contains}»]"))
    except Exception as e:  # noqa: BLE001
        check(False, f"{desc} (lanzó {type(e).__name__}: {e})")
    else:
        check(False, desc + " (no lanzó excepción)")


def rec(mid, task, trigger=T.SENSOR, **over) -> MandateRecord:
    rule = dict(rule_id="r-1", policy_id="pol-ok", policy_version="1", rule_hash=GOOD)
    rule.update(over)
    return MandateRecord(mid, task, S.EXPLICIT, trigger, "2026-11-05T00:00:00Z", **rule)


def raw(c, mid, task, trigger="SENSOR", rule_id="r-1", pid="pol-ok", pv="1", rh=GOOD):
    c.execute(f"INSERT INTO mandates {COLS} VALUES (?, ?, 'EXPLICIT', ?, 'x', ?, ?, ?, ?)", (mid, task, trigger, rule_id, pid, pv, rh))


def n(c, table):
    return c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def main() -> int:
    d = tempfile.mkdtemp(prefix="mandate-rule-hash-")
    db = os.path.join(d, "t.db")
    try:
        s = SQLiteStore(db)
        c = s._conn
        check(set(GUARDS) <= {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}, "existen los tres triggers D-53 en una base nueva")
        for i in range(1, 25):
            c.execute("INSERT INTO tasks (task_id, goal_id, status, version) VALUES (?, 'g', 'OPEN', 1)", (f"t-{i}",))
        content = canonical_json({"rules": [{"rule_id": "r-1"}]})
        s.insert_policy(PolicyRecord("pol-ok", "1", "kernel", "2026-11-05T00:00:00Z", policy_content_hash(content), content))
        s.insert_policy(PolicyRecord("pol-vacia", "1", "kernel", "2026-11-05T00:00:00Z", "h-sin-contenido"))  # content NULL

        # --- capa 1: DTO
        for bad, why in (("no-es-un-hash", "texto libre"), ("A" * 64, "mayúsculas"), (GOOD[:63], "63 caracteres"), (GOOD + "0", "65 caracteres"),
                         ("g" * 64, "caracteres no hex"), (" " + GOOD[1:], "espacio"), ("", "vacío")):
            raises(ContractValidationError, lambda b=bad: rec("m-x", "t-1", rule_hash=b), f"DTO: rule_hash {why} se rechaza")
        raises(ContractValidationError, lambda: rec("m-x", "t-1", T.USER, rule_hash="no-es-un-hash"), "DTO: el formato también se exige a un mandato NO sensor que traiga regla")
        raises(ContractValidationError, lambda: MandateRecord("m-x", "t-1", S.EXPLICIT, T.SENSOR, "x"), "DTO: SENSOR sin regla se rechaza")

        # --- capa 2: create_mandate (política inexistente / sin contenido) sin dejar filas
        before = (n(c, "mandates"), n(c, "mandate_events"))
        raises(sqlite3.IntegrityError, lambda: s.create_mandate(rec("m-1", "t-1", policy_id="pol-no"), "e-1"), "create_mandate: política inexistente", "no existe")
        raises(sqlite3.IntegrityError, lambda: s.create_mandate(rec("m-2", "t-2", policy_version="9"), "e-2"), "create_mandate: versión de política inexistente", "no existe")
        raises(sqlite3.IntegrityError, lambda: s.create_mandate(rec("m-3", "t-3", policy_id="pol-vacia"), "e-3"), "create_mandate: política sin contenido", "no tiene contenido")
        check((n(c, "mandates"), n(c, "mandate_events")) == before, "los rechazos no dejaron mandato ni evento huérfano")

        # --- capa 3: SQL directo (se salta el DTO)
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-1", "t-4", rh="no-es-un-hash"), "SQL: rule_hash «no-es-un-hash»", "D-53")
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-2", "t-5", rh="A" * 64), "SQL: rule_hash en mayúsculas", "D-53")
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-3", "t-6", rh=GOOD[:63]), "SQL: rule_hash de 63 caracteres", "D-53")
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-4", "t-7", rh=GOOD + "0"), "SQL: rule_hash de 65 caracteres", "D-53")
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-5", "t-8", rh="g" * 64), "SQL: rule_hash con caracteres no hex", "D-53")
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-6", "t-9", pid="pol-no"), "SQL: política inexistente", "no existe")
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-7", "t-10", pid="pol-vacia"), "SQL: política sin contenido", "no tiene contenido")
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-8", "t-11", trigger="USER", rh="no-es-un-hash"), "SQL: también para un mandato USER que traiga regla", "D-53")
        c.execute("PRAGMA foreign_keys = OFF")
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-9", "t-12", pid="pol-no"), "SQL con foreign_keys = OFF: la política inexistente se sigue rechazando (trigger, no solo FK)", "no existe")
        c.execute("PRAGMA foreign_keys = ON")
        check(n(c, "mandates") == 0, "ningún intento inválido dejó filas en mandates")

        # --- lo legítimo sigue funcionando
        s.create_mandate(rec("m-ok", "t-13"), "e-ok")
        check(s.get_mandate("m-ok") is not None and s.get_mandate("m-ok").rule_hash == GOOD, "mandato SENSOR con hash válido y política con contenido: aceptado")
        s.create_mandate(MandateRecord("m-user", "t-14", S.EXPLICIT, T.USER, "x"), "e-user")
        check(s.get_mandate("m-user") is not None, "mandato USER sin regla: sin cambios, aceptado")
        # límite documentado: un hash BIEN FORMADO que no es el de la regla se acepta hasta el parche 0011
        s.create_mandate(rec("m-lim", "t-15", rule_hash=hashlib.sha256(b"otra cosa").hexdigest()), "e-lim")
        check(s.get_mandate("m-lim") is not None, "LÍMITE DOCUMENTADO: un SHA-256 bien formado que no corresponde a la regla se acepta (la comprobación semántica es del parche 0011)")
        s.close()

        # --- control negativo: sin los triggers el defecto reaparece
        s = SQLiteStore(db); c = s._conn
        for g in GUARDS:
            c.execute(f"DROP TRIGGER {g}")
        c.execute("PRAGMA foreign_keys = OFF")
        raw(c, "r-neg", "t-16", rh="no-es-un-hash", pid="pol-no")
        check(n(c, "mandates") == 4, "control negativo: SIN los triggers (y sin FK) un hash basura y una política inexistente sí entran")
        s.close()

        # --- migración: al abrir una base sin los triggers se recrean, e idempotente
        s = SQLiteStore(db); c = s._conn
        check(set(GUARDS) <= {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}, "migración: al abrir la base sin triggers se recrean")
        raises(sqlite3.IntegrityError, lambda: raw(c, "r-mig", "t-17", rh="no-es-un-hash"), "migración: la base migrada ya rechaza el hash basura", "D-53")
        s.close()
        s = SQLiteStore(db)
        check(True, "migración idempotente: una segunda apertura no falla")
        s.close()
    finally:
        for name in os.listdir(d):
            try:
                os.unlink(os.path.join(d, name))
            except OSError:
                pass
        try:
            os.rmdir(d)
        except OSError:
            pass
    print()
    if FAILURES:
        print(f"{len(FAILURES)} FALLO(S):")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("test_mandate_rule_hash: todo OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
