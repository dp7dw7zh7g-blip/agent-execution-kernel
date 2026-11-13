"""
test_effect_reconcile_atomic.py
===============================
Parches 0001 y 0002 adoptados en DTOS (2026-10-25, D-34): `EffectRepository.reconcile`
escribe el estado del intento y la fila del ledger `effects` en UNA transacción, existe
la lectura pública `get_effects_by_attempt`, y `canonical_json` rechaza NaN/Infinity.

Corre contra un SQLite real (archivo temporal), como test_store.py.
Ejecutar: python3 tests/dtos_tests/test_effect_reconcile_atomic.py  (con PYTHONPATH=src)
"""

from __future__ import annotations

import os
import sys
import tempfile

import sqlite3

from dtos.canonical import canonical_json
from dtos.effect_repository import EffectRepository
from dtos.store import MAX_EVIDENCE_SIZE_BYTES, ConflictError, EvidenceTooLargeError, SQLiteStore

FAILURES: list[str] = []


def check(condition: bool, description: str) -> None:
    status = "OK " if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        FAILURES.append(description)


def uncertain(repo: EffectRepository, n: int) -> str:
    attempt = f"att-{n}"
    repo.create(attempt, f"int-{n}", "prov", f"key-{n}")
    repo.dispatch(attempt)
    repo.mark_unknown(attempt, "test")
    return attempt


def main() -> int:
    fd, db_path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    try:
        store = SQLiteStore(db_path)
        repo = EffectRepository(store)

        a = uncertain(repo, 1)
        repo.reconcile(a, "PROVIDER_QUERY", "CONFIRMED", {"ref": "ev"}, "op-1", "prov", "key-1")
        rows = store.get_effects_by_attempt(a)
        check(store.get_effect_attempt(a)["status"] == "CONFIRMED", "reconcile terminal: el intento queda CONFIRMED")
        check([(r["operation_id"], r["status"], r["provider"], r["idempotency_key"]) for r in rows] == [("op-1", "CONFIRMED", "prov", "key-1")],
              "reconcile terminal: la fila del ledger se escribe con operation_id, proveedor y clave")

        b = uncertain(repo, 2)
        repo.reconcile(b, "HUMAN_CONFIRMATION", "REJECTED", {"ref": "ev"}, "op-2", "prov", "key-2")
        check([r["status"] for r in store.get_effects_by_attempt(b)] == ["REJECTED"], "reconcile REJECTED: fila REJECTED en el ledger")

        c = uncertain(repo, 3)
        repo.reconcile(c, "READ_AFTER_WRITE", "PARTIAL", {"ref": "ev"}, "op-3", "prov", "key-3")
        check(store.get_effect_attempt(c)["status"] == "PARTIAL" and store.get_effects_by_attempt(c) == [],
              "reconcile PARTIAL (no terminal): cambia el estado y NO escribe el ledger")

        # el ledger rechaza la inserción: el intento NO debe cambiar (antes quedaba terminal sin fila)
        d = uncertain(repo, 4)
        before = store.get_effect_attempt(d)
        store.insert_effect(effect_id="e-ajeno", operation_id="op-x", intent_id="i-x", attempt_id="otro", provider="prov",
                            idempotency_key="key-4", status="CONFIRMED")
        raised = False
        try:
            repo.reconcile(d, "PROVIDER_QUERY", "CONFIRMED", {"ref": "ev"}, "op-4", "prov", "key-4")
        except ConflictError:
            raised = True
        after = store.get_effect_attempt(d)
        check(raised, "ledger con la clave ya usada: reconcile lanza ConflictError (mismo tipo que insert_effect)")
        check(after["status"] == "RECONCILIATION_REQUIRED" and after["version"] == before["version"],
              "ledger con la clave ya usada: el intento queda intacto (estado y versión), sin brecha")
        check(store.get_effects_by_attempt(d) == [], "ledger con la clave ya usada: no se escribe fila para el intento")

        # versión obsoleta: ni estado ni ledger
        e = uncertain(repo, 5)
        raised = False
        try:
            store.reconcile_effect_attempt_atomic(e, "CONFIRMED", 99, {"effect_id": "e5", "operation_id": "op-5", "intent_id": e,
                                                                       "attempt_id": e, "provider": "prov", "idempotency_key": "key-5"})
        except ConflictError:
            raised = True
        check(raised and store.get_effect_attempt(e)["status"] == "RECONCILIATION_REQUIRED" and store.get_effects_by_attempt(e) == [],
              "versión obsoleta: ConflictError, sin cambio de estado y sin fila en el ledger")
        check(store.get_effects_by_attempt("no-existe") == [], "get_effects_by_attempt de un intento sin ledger devuelve []")

        # canonical_json
        for bad, label in ((float("nan"), "NaN"), (float("inf"), "Infinity"), (float("-inf"), "-Infinity")):
            try:
                canonical_json({"x": bad})
                ok = False
            except ValueError:
                ok = True
            check(ok, f"canonical_json rechaza {label} con ValueError")
        check(canonical_json({"b": [1, 2.5, None], "a": "ñ"}) == '{"a":"ñ","b":[1,2.5,null]}', "canonical_json sigue produciendo el mismo texto para valores finitos")
        try:
            canonical_json({"x": object()})
            ok = False
        except TypeError as exc:
            ok = "to_canonical_dict" in str(exc)
        check(ok, "canonical_json conserva el TypeError descriptivo de _json_default")

        # ---- evidencia persistida en el ledger (parche 0003) ----
        ev = {"z": 1, "a": [2, 1], "ñ": {"k": None}}
        f = uncertain(repo, 6)
        repo.reconcile(f, "PROVIDER_QUERY", "CONFIRMED", ev, "op-6", "prov", "key-6")
        stored = store.get_effects_by_attempt(f)[0]["evidence"]
        check(stored == canonical_json(ev) == '{"a":[2,1],"z":1,"ñ":{"k":null}}',
              "evidencia: la columna effects.evidence contiene EXACTAMENTE el JSON canónico de la evidencia aportada")
        g = uncertain(repo, 7)
        repo.reconcile(g, "HUMAN_CONFIRMATION", "REJECTED", ev, "op-7", "prov", "key-7")
        check(store.get_effects_by_attempt(g)[0]["evidence"] == canonical_json(ev), "evidencia: también se guarda en un resultado REJECTED")
        check(store.get_effects_by_attempt(a)[0]["evidence"] == canonical_json({"ref": "ev"}), "evidencia: la primera reconciliación del test guardó su evidencia")
        check(store.get_effects_by_attempt(c) == [], "evidencia: un resultado PARTIAL no escribe ledger y por tanto no persiste evidencia en DTOS")

        store.insert_effect(effect_id="e-sin", operation_id="op-8", intent_id="i8", attempt_id="att-8", provider="prov",
                            idempotency_key="key-8", status="CONFIRMED")
        check(store.get_effects_by_attempt("att-8")[0]["evidence"] is None, "insert_effect sin evidencia: columna NULL (compatible hacia atrás)")
        store.insert_effect(effect_id="e-con", operation_id="op-9", intent_id="i9", attempt_id="att-9", provider="prov",
                            idempotency_key="key-9", status="CONFIRMED", evidence='{"x":1}')
        check(store.get_effects_by_attempt("att-9")[0]["evidence"] == '{"x":1}', "insert_effect con evidencia: se guarda tal cual")

        immutable = False
        try:
            store._conn.execute("UPDATE effects SET evidence = '{}' WHERE attempt_id = ?", (f,))
        except sqlite3.DatabaseError:
            immutable = True
        check(immutable, "la evidencia es inmutable: effects sigue siendo append-only (UPDATE prohibido)")

        # evidencia no serializable: falla ANTES de tocar nada
        h = uncertain(repo, 10)
        failed = False
        try:
            repo.reconcile(h, "PROVIDER_QUERY", "CONFIRMED", {"x": float("nan")}, "op-10", "prov", "key-10")
        except ValueError:
            failed = True
        check(failed and store.get_effect_attempt(h)["status"] == "RECONCILIATION_REQUIRED" and store.get_effects_by_attempt(h) == [],
              "evidencia con NaN: ValueError y el intento queda intacto")

        # ---- historial de reconciliaciones (parche 0005): también PARTIAL, mismo contenido canónico, append-only
        p1 = uncertain(repo, 106)
        repo.reconcile(p1, "READ_AFTER_WRITE", "PARTIAL", {"observed": "mitad", "z": 1, "a": ["x"]}, "op-106", "prov", "key-106")
        hist = store.get_effect_reconciliations(p1)
        check(len(hist) == 1 and (hist[0]["from_status"], hist[0]["to_status"], hist[0]["strategy"]) == ("RECONCILIATION_REQUIRED", "PARTIAL", "READ_AFTER_WRITE"),
              "PARTIAL: deja una fila en effect_reconciliations con estado previo, estado nuevo y estrategia")
        check(hist[0]["evidence"] == canonical_json({"observed": "mitad", "z": 1, "a": ["x"]}), "PARTIAL: la evidencia guardada es exactamente el JSON canónico")
        check(store.get_effects_by_attempt(p1) == [], "PARTIAL: sigue sin escribir fila en el ledger effects")
        hist_c = store.get_effect_reconciliations(a)
        check(len(hist_c) == 1 and hist_c[0]["to_status"] == "CONFIRMED" and hist_c[0]["evidence"] == canonical_json({"ref": "ev"}),
              "CONFIRMED: el historial también registra la reconciliación terminal")
        for statement, label in (("UPDATE effect_reconciliations SET strategy='x'", "UPDATE"), ("DELETE FROM effect_reconciliations", "DELETE")):
            try:
                store._conn.execute(statement)
                blocked = False
            except Exception:
                blocked = True
            check(blocked, f"effect_reconciliations es append-only: {label} prohibido")

        # ---- por qué PARTIAL NO va al ledger: un intento posterior con la MISMA clave completa el efecto y necesita su fila terminal
        q1 = uncertain(repo, 107)
        repo.reconcile(q1, "READ_AFTER_WRITE", "PARTIAL", {"ref": "mitad"}, "op-107", "prov", "key-107")
        repo.create("att-107b", "int-107b", "prov", "key-107")  # nuevo intento, misma idempotency_key (effect_attempts no la hace única)
        repo.dispatch("att-107b")
        repo.mark_unknown("att-107b", "t")
        repo.reconcile("att-107b", "PROVIDER_QUERY", "CONFIRMED", {"ref": "completo"}, "op-107", "prov", "key-107")
        check([r["status"] for r in store.get_effects_by_attempt("att-107b")] == ["CONFIRMED"], "el intento que completa un PARTIAL con la misma clave sí obtiene su fila terminal")
        check([h["to_status"] for h in store.get_effect_reconciliations(q1)] + [h["to_status"] for h in store.get_effect_reconciliations("att-107b")] == ["PARTIAL", "CONFIRMED"],
              "el historial conserva ambas reconciliaciones, una por intento")
        store.insert_effect(effect_id="e-p", operation_id="op-108", intent_id="i-108", attempt_id="att-108", provider="prov", idempotency_key="key-108", status="PARTIAL")
        raised = False
        try:
            store.insert_effect(effect_id="e-c", operation_id="op-108", intent_id="i-108b", attempt_id="att-108b", provider="prov", idempotency_key="key-108", status="CONFIRMED")
        except ConflictError:
            raised = True
        check(raised, "documentación: una fila PARTIAL en effects bloquearía la fila terminal de la misma clave (uq_effects_idempotency)")

        # ---- tope de tamaño de la evidencia (bytes UTF-8)
        base = len(canonical_json({"d": ""}).encode("utf-8"))
        at_cap = {"d": "x" * (MAX_EVIDENCE_SIZE_BYTES - base)}
        check(len(canonical_json(at_cap).encode("utf-8")) == MAX_EVIDENCE_SIZE_BYTES, "preparación: evidencia de exactamente MAX_EVIDENCE_SIZE_BYTES")
        r1 = uncertain(repo, 109)
        repo.reconcile(r1, "PROVIDER_QUERY", "CONFIRMED", at_cap, "op-109", "prov", "key-109")
        check(store.get_effect_attempt(r1)["status"] == "CONFIRMED", "evidencia en el límite exacto: se acepta")
        over = {"d": "x" * (MAX_EVIDENCE_SIZE_BYTES - base + 1)}
        r2 = uncertain(repo, 110)
        before = store.get_effect_attempt(r2)
        raised = False
        try:
            repo.reconcile(r2, "PROVIDER_QUERY", "CONFIRMED", over, "op-110", "prov", "key-110")
        except EvidenceTooLargeError:
            raised = True
        after = store.get_effect_attempt(r2)
        check(raised, "evidencia un byte por encima del límite: EvidenceTooLargeError")
        check(after == before and store.get_effects_by_attempt(r2) == [] and store.get_effect_reconciliations(r2) == [],
              "evidencia demasiado grande: intento, ledger e historial intactos")
        multibyte = {"d": "ñ" * (MAX_EVIDENCE_SIZE_BYTES // 2)}  # menos de 65536 caracteres, más de 65536 bytes
        r3 = uncertain(repo, 111)
        raised = False
        try:
            repo.reconcile(r3, "PROVIDER_QUERY", "CONFIRMED", multibyte, "op-111", "prov", "key-111")
        except EvidenceTooLargeError:
            raised = True
        check(raised and len(multibyte["d"]) < MAX_EVIDENCE_SIZE_BYTES, "el tope se mide en bytes UTF-8, no en caracteres")
        raised = False
        try:
            store.insert_effect(effect_id="e-big", operation_id="op-112", intent_id="i-112", attempt_id="att-112", provider="prov", idempotency_key="key-112",
                                status="CONFIRMED", evidence="x" * (MAX_EVIDENCE_SIZE_BYTES + 1))
        except EvidenceTooLargeError:
            raised = True
        check(raised and store.get_effects_by_attempt("att-112") == [], "insert_effect directo también rechaza evidencia por encima del límite")
        r4 = uncertain(repo, 113)
        raised = False
        try:
            store.reconcile_effect_attempt_atomic(r4, "PARTIAL", store.get_effect_attempt(r4)["version"], None,
                                                  {"from_status": "RECONCILIATION_REQUIRED", "strategy": "PROVIDER_QUERY", "evidence": "x" * (MAX_EVIDENCE_SIZE_BYTES + 1)})
        except EvidenceTooLargeError:
            raised = True
        check(raised and store.get_effect_attempt(r4)["status"] == "RECONCILIATION_REQUIRED", "la función atómica rechaza evidencia grande sin cambiar el estado")

        store.close()

        # ---- migración: una base creada ANTES de la columna se actualiza al abrirla ----
        fd2, old_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd2)
        try:
            s0 = SQLiteStore(old_path)
            s0.close()
            raw = sqlite3.connect(old_path)
            raw.execute("ALTER TABLE effects DROP COLUMN evidence")
            raw.execute("INSERT INTO effects (effect_id, operation_id, intent_id, attempt_id, provider, idempotency_key, status) "
                        "VALUES ('legacy', 'op-l', 'i-l', 'att-l', 'prov', 'key-l', 'CONFIRMED')")
            raw.commit()
            cols_before = [r[1] for r in raw.execute("PRAGMA table_info(effects)")]
            raw.close()
            s1 = SQLiteStore(old_path)
            cols_after = [r[1] for r in s1._conn.execute("PRAGMA table_info(effects)")]
            legacy = s1.get_effects_by_attempt("att-l")[0]
            s1.insert_effect(effect_id="nuevo", operation_id="op-n", intent_id="i-n", attempt_id="att-n", provider="prov",
                             idempotency_key="key-n", status="CONFIRMED", evidence='{"ok":true}')
            check("evidence" not in cols_before and cols_after[-1] == "evidence", "migración: una base anterior recibe la columna evidence al abrirse")
            check(legacy["evidence"] is None and legacy["operation_id"] == "op-l", "migración: las filas anteriores se conservan con evidence NULL")
            check(s1.get_effects_by_attempt("att-n")[0]["evidence"] == '{"ok":true}', "migración: las filas nuevas guardan evidencia")
            s1.close()
        finally:
            os.unlink(old_path)

        # ---- migración: una base creada ANTES de effect_reconciliations recibe la tabla al abrirse (CREATE TABLE IF NOT EXISTS)
        fd3, old_path2 = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd3)
        try:
            s0 = SQLiteStore(old_path2)
            s0.close()
            raw = sqlite3.connect(old_path2)
            raw.execute("DROP TABLE effect_reconciliations")
            raw.commit()
            tables_before = {r[0] for r in raw.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            raw.close()
            s2 = SQLiteStore(old_path2)
            repo2 = EffectRepository(s2)
            m = uncertain(repo2, 1)
            repo2.reconcile(m, "READ_AFTER_WRITE", "PARTIAL", {"ref": "ev"}, "op-m", "prov", "key-m")
            check("effect_reconciliations" not in tables_before and len(s2.get_effect_reconciliations(m)) == 1,
                  "migración: una base anterior sin effect_reconciliations recibe la tabla al abrirse y registra la reconciliación")
            s2.close()
        finally:
            os.unlink(old_path2)

        if FAILURES:
            print(f"\n{len(FAILURES)} test(s) FALLARON:")
            for f in FAILURES:
                print(f"  - {f}")
            return 1
        print("\nTodos los effect reconcile atomic tests pasaron.")
        return 0
    finally:
        os.unlink(db_path)


if __name__ == "__main__":
    sys.exit(main())
