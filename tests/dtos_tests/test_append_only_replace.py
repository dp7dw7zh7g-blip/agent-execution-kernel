"""
test_append_only_replace.py
===========================
Parche 0009 r3 (D-52c): `INSERT OR REPLACE` no debe poder sobrescribir filas de las tablas append-only del parche 0009
(`policies`, `mandates`, `mandate_events`).

Con `recursive_triggers = 0` (valor por defecto de SQLite) un REPLACE borra la fila en conflicto SIN disparar los triggers de DELETE;
en g0.8 r2 eso permitía alterar en silencio una política, un mandato o un evento «inmutables». La guarda es un trigger BEFORE INSERT
por tabla (no se activa `recursive_triggers`, para no cambiar el comportamiento de los demás triggers de la conexión).

Corre contra un SQLite real (archivo temporal). Falla contra el `schema.sql` de g0.8 r2 (sin los triggers `*_no_replace`).

Ejecutar: python run_tests.py dtos
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile

from dtos.store import SQLiteStore

FAILURES: list[str] = []
GUARDS = ("trg_policies_no_replace", "trg_mandates_no_replace", "trg_mandate_events_no_replace")
GUARDS_D52D = ("trg_artifacts_no_replace", "trg_audit_log_no_replace", "trg_effects_no_replace",
               "trg_effect_reconciliations_no_replace", "trg_reasoning_runs_no_replace")


def check(condition: bool, description: str) -> None:
    print(f"[{'OK ' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def rejected(conn, sql: str, params: tuple = ()) -> bool:
    """True si SQLite rechaza la sentencia con IntegrityError y la mencionada guarda (mensaje «append-only»)."""
    try:
        conn.execute(sql, params)
    except sqlite3.IntegrityError as e:
        return "append-only" in str(e)
    return False


def seed(conn) -> None:
    conn.execute("INSERT INTO tasks (task_id, goal_id, status, version) VALUES ('t-1', 'g-1', 'OPEN', 1)")
    conn.execute("INSERT INTO policies (policy_id, version, scope, effective_from, hash, content) VALUES ('pol-1', '1', 'original', '2026-11-04', 'h-orig', NULL)")
    conn.execute("INSERT INTO mandates (mandate_id, task_id, strength, trigger_kind, created_at) VALUES ('m-1', 't-1', 'EXPLICIT', 'USER', 'orig')")
    conn.execute("INSERT INTO mandate_events (event_id, mandate_id, event_type, payload, created_at) VALUES ('ev-1', 'm-1', 'MANDATE_CREATED', '{\"orig\":1}', 'orig')")


def snapshot(conn) -> tuple:
    return (
        conn.execute("SELECT * FROM policies ORDER BY policy_id, version").fetchall(),
        conn.execute("SELECT * FROM mandates ORDER BY mandate_id").fetchall(),
        conn.execute("SELECT * FROM mandate_events ORDER BY event_id").fetchall(),
    )


def triggers(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'")}


def main() -> int:
    d = tempfile.mkdtemp(prefix="append-only-replace-")
    db = os.path.join(d, "t.db")
    try:
        s = SQLiteStore(db)
        c = s._conn
        check(set(GUARDS) <= triggers(c), "los tres triggers *_no_replace existen tras abrir una base nueva")
        check(c.execute("PRAGMA recursive_triggers").fetchone()[0] == 0, "no se activó recursive_triggers (la guarda no cambia el comportamiento de los demás triggers)")
        seed(c)
        before = snapshot(c)

        # --- policies
        check(rejected(c, "INSERT OR REPLACE INTO policies (policy_id, version, scope, effective_from, hash, content) VALUES ('pol-1', '1', 'ALTERADA', 'x', 'h-alt', NULL)"),
              "policies: INSERT OR REPLACE sobre una (policy_id, version) existente se rechaza")
        check(rejected(c, "REPLACE INTO policies (policy_id, version, scope, effective_from, hash, content) VALUES ('pol-1', '1', 'ALTERADA', 'x', 'h-alt', NULL)"),
              "policies: REPLACE INTO se rechaza")
        check(rejected(c, "INSERT OR IGNORE INTO policies (policy_id, version, scope, effective_from, hash, content) VALUES ('pol-1', '1', 'ALTERADA', 'x', 'h-alt', NULL)"),
              "policies: INSERT OR IGNORE sobre una clave existente se rechaza (no se ignora en silencio)")
        check(rejected(c, "INSERT INTO policies (policy_id, version, scope, effective_from, hash, content) VALUES ('pol-1', '1', 'ALTERADA', 'x', 'h-alt', NULL) "
                          "ON CONFLICT (policy_id, version) DO UPDATE SET hash = 'h-alt'"),
              "policies: upsert (ON CONFLICT DO UPDATE) se rechaza")

        # --- mandates: REPLACE resuelve conflictos en CUALQUIER clave única (mandate_id y task_id)
        check(rejected(c, "INSERT OR REPLACE INTO mandates (mandate_id, task_id, strength, trigger_kind, created_at) VALUES ('m-1', 't-1', 'INFERRED', 'AGENT', 'alt')"),
              "mandates: REPLACE con el mismo mandate_id se rechaza")
        check(rejected(c, "INSERT OR REPLACE INTO mandates (mandate_id, task_id, strength, trigger_kind, created_at) VALUES ('m-otro', 't-1', 'INFERRED', 'AGENT', 'alt')"),
              "mandates: REPLACE con OTRO mandate_id pero la misma Task (conflicto en UNIQUE task_id) se rechaza")

        # --- mandate_events
        check(rejected(c, "INSERT OR REPLACE INTO mandate_events (event_id, mandate_id, event_type, payload, created_at) VALUES ('ev-1', 'm-1', 'MANDATE_CREATED', '{\"alt\":1}', 'alt')"),
              "mandate_events: INSERT OR REPLACE sobre un event_id existente se rechaza")

        check(snapshot(c) == before, "tras todos los intentos, las tres tablas quedaron EXACTAMENTE como estaban (sin sobrescritura ni borrado)")

        # --- lo legítimo sigue funcionando
        c.execute("INSERT INTO policies (policy_id, version, scope, effective_from, hash, content) VALUES ('pol-1', '2', 'nueva versión', 'x', 'h-2', NULL)")
        c.execute("INSERT INTO tasks (task_id, goal_id, status, version) VALUES ('t-2', 'g-1', 'OPEN', 1)")
        c.execute("INSERT INTO mandates (mandate_id, task_id, strength, trigger_kind, created_at) VALUES ('m-2', 't-2', 'EXPLICIT', 'USER', 'x')")
        c.execute("INSERT INTO mandate_events (event_id, mandate_id, event_type, payload, created_at) VALUES ('ev-2', 'm-2', 'MANDATE_CREATED', '{}', 'x')")
        check(c.execute("SELECT COUNT(*) FROM policies").fetchone()[0] == 2 and c.execute("SELECT COUNT(*) FROM mandates").fetchone()[0] == 2,
              "un INSERT normal con claves nuevas (otra versión de política, otro mandato, otro evento) sigue permitido")
        for stmt in ("UPDATE policies SET scope = 'x'", "DELETE FROM policies", "UPDATE mandates SET strength = 'INFERRED'", "DELETE FROM mandates",
                     "UPDATE mandate_events SET payload = '{}'", "DELETE FROM mandate_events"):
            check(rejected(c, stmt), f"siguen bloqueados los UPDATE/DELETE directos: {stmt}")

        # --- D-52d: las otras cinco tablas históricas
        check(set(GUARDS_D52D) <= triggers(c), "D-52d: existen los cinco triggers *_no_replace de artifacts/audit_log/effects/effect_reconciliations/reasoning_runs")
        c.execute("INSERT INTO artifacts (artifact_id, version, artifact_class, content_hash, provenance, status) VALUES ('a-1', 1, 'C', 'h1', 'orig', 'ACTIVE')")
        c.execute("INSERT INTO audit_log (entry_hash, prev_hash, payload) VALUES ('eh-1', 'GENESIS', 'orig')")
        c.execute("INSERT INTO effects (effect_id, operation_id, intent_id, attempt_id, provider, idempotency_key, status) VALUES ('e-1', 'op-1', 'i-1', 'at-1', 'prov', 'k-1', 'CONFIRMED')")
        c.execute("INSERT INTO effect_reconciliations (attempt_id, from_status, to_status, strategy, evidence) VALUES ('at-1', 'A', 'B', 's', '{}')")
        c.execute("INSERT INTO reasoning_runs (request_id, provider_id, provider_version, input_hash, output_hash, status, iterations, duration) VALUES ('r-1', 'p', '1', 'i', 'o', 'OK', 1, 1)")
        before2 = tuple(c.execute(f"SELECT * FROM {t}").fetchall() for t in ("artifacts", "audit_log", "effects", "effect_reconciliations", "reasoning_runs"))
        check(rejected(c, "INSERT OR REPLACE INTO artifacts (artifact_id, version, artifact_class, content_hash, provenance, status) VALUES ('a-1', 1, 'C', 'h1', 'ALTERADA', 'X')"),
              "artifacts: INSERT OR REPLACE sobre (artifact_id, version, content_hash) existente se rechaza")
        check(rejected(c, "INSERT OR REPLACE INTO audit_log (seq, entry_hash, prev_hash, payload) VALUES (1, 'x', 'x', 'ALTERADA')"),
              "audit_log: INSERT OR REPLACE con un seq existente se rechaza")
        check(rejected(c, "INSERT OR REPLACE INTO effects (effect_id, operation_id, intent_id, attempt_id, provider, idempotency_key, status) VALUES ('e-1', 'x', 'x', 'x', 'p2', 'k2', 'REJECTED')"),
              "effects: REPLACE con el mismo effect_id se rechaza")
        check(rejected(c, "INSERT OR REPLACE INTO effects (effect_id, operation_id, intent_id, attempt_id, provider, idempotency_key, status) VALUES ('e-otro', 'x', 'x', 'x', 'prov', 'k-1', 'REJECTED')"),
              "effects: REPLACE con OTRO effect_id pero la misma (provider, idempotency_key) se rechaza")
        check(rejected(c, "INSERT OR REPLACE INTO effect_reconciliations (reconciliation_id, attempt_id, from_status, to_status, strategy, evidence) VALUES (1, 'x', 'x', 'x', 'x', 'ALTERADA')"),
              "effect_reconciliations: REPLACE con un reconciliation_id existente se rechaza")
        check(rejected(c, "INSERT OR REPLACE INTO reasoning_runs (request_id, provider_id, provider_version, input_hash, output_hash, status, iterations, duration) VALUES ('r-1', 'x', 'x', 'x', 'x', 'ALTERADA', 9, 9)"),
              "reasoning_runs: REPLACE con un request_id existente se rechaza")
        check(rejected(c, "INSERT OR IGNORE INTO effects (effect_id, operation_id, intent_id, attempt_id, provider, idempotency_key, status) VALUES ('e-1', 'x', 'x', 'x', 'p', 'k', 'X')"),
              "effects: INSERT OR IGNORE sobre clave existente se rechaza (no se ignora en silencio)")
        after2 = tuple(c.execute(f"SELECT * FROM {t}").fetchall() for t in ("artifacts", "audit_log", "effects", "effect_reconciliations", "reasoning_runs"))
        check(after2 == before2, "D-52d: tras los intentos, las cinco tablas quedaron EXACTAMENTE como estaban")
        c.execute("INSERT INTO artifacts (artifact_id, version, artifact_class, content_hash, provenance, status) VALUES ('a-1', 2, 'C', 'h2', 'nueva', 'ACTIVE')")
        c.execute("INSERT INTO audit_log (entry_hash, prev_hash, payload) VALUES ('eh-2', 'eh-1', 'n')")
        c.execute("INSERT INTO effect_reconciliations (attempt_id, from_status, to_status, strategy, evidence) VALUES ('at-1', 'B', 'C', 's', '{}')")
        c.execute("INSERT INTO effects (effect_id, operation_id, intent_id, attempt_id, provider, idempotency_key, status) VALUES ('e-2', 'op-2', 'i-2', 'at-2', 'prov', 'k-2', 'CONFIRMED')")
        check(c.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0] == 2 and c.execute("SELECT COUNT(*) FROM effect_reconciliations").fetchone()[0] == 2,
              "D-52d: los INSERT normales (seq/reconciliation_id autoasignados, claves nuevas) siguen permitidos")
        s.close()

        # --- control negativo: sin las guardas el defecto reaparece (la prueba detecta el problema que dice detectar)
        s = SQLiteStore(db)
        c = s._conn
        for g in GUARDS + GUARDS_D52D:
            c.execute(f"DROP TRIGGER {g}")
        c.execute("INSERT OR REPLACE INTO policies (policy_id, version, scope, effective_from, hash, content) VALUES ('pol-1', '1', 'ALTERADA', 'x', 'h-alt', NULL)")
        c.execute("INSERT OR REPLACE INTO effects (effect_id, operation_id, intent_id, attempt_id, provider, idempotency_key, status) VALUES ('e-1', 'x', 'x', 'x', 'p2', 'k2', 'REJECTED')")
        check(c.execute("SELECT status FROM effects WHERE effect_id = 'e-1'").fetchone()[0] == "REJECTED",
              "control negativo: SIN las guardas el REPLACE sí sobrescribe el ledger de efectos (D-52d)")
        check(c.execute("SELECT hash FROM policies WHERE policy_id = 'pol-1' AND version = '1'").fetchone()[0] == "h-alt",
              "control negativo: SIN las guardas el REPLACE sí sobrescribe la política (reproduce el defecto de g0.8 r2)")
        s.close()

        # --- migración: una base creada con el esquema de g0.8 r2 (sin las guardas) las recibe al abrirse, y la fila alterada de arriba queda como dato previo
        s = SQLiteStore(db)
        c = s._conn
        check(set(GUARDS + GUARDS_D52D) <= triggers(c), "migración: al abrir una base de g0.8 r2 se crean los triggers *_no_replace")
        check(rejected(c, "INSERT OR REPLACE INTO policies (policy_id, version, scope, effective_from, hash, content) VALUES ('pol-1', '2', 'x', 'x', 'x', NULL)"),
              "migración: la base migrada ya rechaza el REPLACE")
        s.close()
        s = SQLiteStore(db)  # idempotente
        check(set(GUARDS) <= triggers(s._conn), "migración idempotente: una segunda apertura no falla")
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
    print("test_append_only_replace: todo OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
