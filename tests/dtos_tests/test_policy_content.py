"""
test_policy_content.py
======================
Parche 0008 (D-51): `policies.content` y la invariante hash == SHA-256(JSON canónico del contenido).

Corre contra un SQLite real (archivo temporal). La prueba de migración simula una base anterior al parche quitando la columna
(`ALTER TABLE ... DROP COLUMN`, SQLite >= 3.35, el mismo requisito que ya tiene test_effect_reconcile_atomic.py).

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
from dtos.registries import PolicyContentError, PolicyRecord, policy_content_hash
from dtos.store import SQLiteStore

FAILURES: list[str] = []


def check(condition: bool, description: str) -> None:
    print(f"[{'OK ' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def expect_raises(exc_type, fn, description: str) -> None:
    try:
        fn()
        check(False, description + " (no lanzó excepción)")
    except exc_type:
        check(True, description)
    except Exception as e:  # excepción de otro tipo: también es un fallo
        check(False, f"{description} (lanzó {type(e).__name__}: {e})")


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def policy(content, hash_, pid="pol-1", version="1") -> PolicyRecord:
    return PolicyRecord(policy_id=pid, version=version, scope="kernel", effective_from="2026-11-02T00:00:00Z", hash=hash_, content=content)


def table_columns(conn, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def main() -> int:
    # ---------------------------------------------------------------
    # La fórmula: SHA-256 de canonical_json(objeto) en UTF-8
    # ---------------------------------------------------------------
    # Vector fijo calculado con hashlib sobre los bytes literales, sin pasar por canonical_json.
    check(policy_content_hash('{"a":1}') == "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862",
          "vector fijo: SHA-256 de {\"a\":1} coincide con el calculado fuera del código")
    obj = {"rules": [{"rule_id": "r-1", "when": "señal ñ", "n": 2}], "z": None, "a": True}
    text = canonical_json(obj)
    check(policy_content_hash(text) == sha(text), "el hash es el SHA-256 del texto canónico (con caracteres no ASCII)")
    independent = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    check(text == independent, "el texto canónico coincide con json.dumps(sort_keys, separators mínimos, ensure_ascii=False)")

    # ---------------------------------------------------------------
    # PolicyRecord
    # ---------------------------------------------------------------
    ok = policy(text, sha(text))
    check(ok.content == text and ok.hash == sha(text), "PolicyRecord con content y hash coherentes se construye")
    sin = policy(None, "cualquier-hash-previo")
    check(sin.content is None, "sin content no se verifica nada (filas existentes, content NULL)")
    expect_raises(PolicyContentError, lambda: policy(text, "0" * 64), "hash que no corresponde al content -> PolicyContentError")
    expect_raises(PolicyContentError, lambda: policy(text, sha(text).upper()), "el hash se compara exacto (hex en mayúsculas no vale)")
    spaced = '{"a": 1}'
    expect_raises(PolicyContentError, lambda: policy(spaced, sha(spaced)), "content con espacios (no canónico) se rechaza aunque su hash sea el de ese texto")
    unordered = '{"b":1,"a":2}'
    expect_raises(PolicyContentError, lambda: policy(unordered, sha(unordered)), "content con claves desordenadas se rechaza")
    dup = '{"a":1,"a":2}'
    expect_raises(PolicyContentError, lambda: policy(dup, sha(dup)), "content con claves duplicadas se rechaza")
    for bad, why in (("", "vacío"), ("   ", "solo espacios"), ("{no es json", "JSON inválido"), ('{"x":NaN}', "NaN"), ('{"x":Infinity}', "Infinity")):
        expect_raises(PolicyContentError, lambda b=bad: policy(b, sha(b)), f"content {why} se rechaza")
    expect_raises(PolicyContentError, lambda: policy(123, "x"), "content que no es str se rechaza")
    expect_raises(PolicyContentError, lambda: policy({"a": 1}, "x"), "content como dict (no texto) se rechaza")
    check(issubclass(PolicyContentError, ValueError), "PolicyContentError es un ValueError (como el resto de errores de DTO)")

    # ---------------------------------------------------------------
    # Store: escritura, lectura, migración
    # ---------------------------------------------------------------
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db = tmp.name
    try:
        s = SQLiteStore(db)
        check("content" in table_columns(s._conn, "policies"), "una base nueva tiene policies.content")

        s.insert_policy(ok)
        s.insert_policy(policy(None, "hash-previo", pid="pol-viejo"))
        got = s.get_policy("pol-1", "1")
        check(got == ok, "insert_policy + get_policy: ida y vuelta idéntica (con content)")
        check(s._conn.execute("SELECT content FROM policies WHERE policy_id='pol-1'").fetchone()[0] == text,
              "la columna guarda exactamente el texto canónico hasheado")
        got_old = s.get_policy("pol-viejo", "1")
        check(got_old is not None and got_old.content is None and got_old.hash == "hash-previo", "una política sin content se guarda y se lee con content None")
        check(s._conn.execute("SELECT content FROM policies WHERE policy_id='pol-viejo'").fetchone()[0] is None, "sin content queda NULL en la base")
        check(s.get_policy("no-existe", "1") is None, "get_policy de una política inexistente devuelve None")
        expect_raises(sqlite3.IntegrityError, lambda: s.insert_policy(ok), "(policy_id, version) repetida -> IntegrityError")
        check(s._conn.execute("SELECT COUNT(*) FROM policies").fetchone()[0] == 2, "el intento repetido no dejó filas de más")

        # Alteración fuera del módulo: el hash ya no corresponde al content -> la lectura lo detecta.
        # (Desde el parche 0009 `policies` es append-only: para simular la alteración hay que quitar antes los triggers.)
        s._conn.execute("DROP TRIGGER trg_policies_no_update")
        s._conn.execute("DROP TRIGGER trg_policies_no_delete")
        s._conn.execute("UPDATE policies SET content = ? WHERE policy_id = 'pol-1'", ('{"rules":[]}',))
        expect_raises(PolicyContentError, lambda: s.get_policy("pol-1", "1"), "una fila alterada (content no coincide con hash) no se devuelve como válida")
        s.close()

        # Migración: base anterior al parche (sin la columna), con una fila ya escrita.
        raw = sqlite3.connect(db, isolation_level=None)
        raw.execute("DROP TRIGGER IF EXISTS trg_policies_no_update")  # (parche 0009: policies es append-only)
        raw.execute("DROP TRIGGER IF EXISTS trg_policies_no_delete")
        raw.execute("DELETE FROM policies")
        for trg in ("trg_mandates_rule_hash_format", "trg_mandates_policy_must_exist", "trg_mandates_policy_must_have_content"):
            raw.execute(f"DROP TRIGGER IF EXISTS {trg}")  # (D-53: una base anterior al parche 0008 no tiene triggers que lean policies.content)
        raw.execute("ALTER TABLE policies DROP COLUMN content")
        raw.execute("INSERT INTO policies (policy_id, version, scope, effective_from, hash) VALUES ('legacy', '1', 'kernel', '2026-10-01T00:00:00Z', 'h-legacy')")
        before = table_columns(raw, "policies")
        raw.close()
        check("content" not in before, "preparación: la base simulada no tiene la columna content")

        s1 = SQLiteStore(db)
        after = table_columns(s1._conn, "policies")
        check(after[-1] == "content" and after[:-1] == before, "migración: al abrirse una base anterior recibe la columna content (sin tocar las demás)")
        legacy = s1.get_policy("legacy", "1")
        check(legacy is not None and legacy.content is None and legacy.hash == "h-legacy", "migración: la fila anterior se conserva con content NULL")
        s1.insert_policy(ok)
        check(s1.get_policy("pol-1", "1") == ok, "migración: tras migrar se pueden guardar políticas con content")
        s1.close()

        s2 = SQLiteStore(db)  # idempotente: abrir de nuevo no falla ni duplica la columna
        check(table_columns(s2._conn, "policies") == after, "migración idempotente: una segunda apertura no cambia las columnas")
        check(s2.get_policy("pol-1", "1") == ok and s2.get_policy("legacy", "1") is not None, "migración idempotente: los datos siguen intactos")
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
    print("test_policy_content: todo OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
