"""
audit.py
========
Hash chain sobre la tabla `audit_log` (sección AUDIT INTEGRITY del
Registro Maestro). Los triggers de schema.sql ya impiden UPDATE/DELETE
por el camino normal (P-065) — esto cubre un caso más duro: una fila
modificada o eliminada directamente en el archivo .db (corrupción de E/S,
un error lógico, una edición manual que ignora los triggers).
`verify()` lo detecta porque el hash de cada entrada es una función
determinista de (prev_hash, payload).

LÍMITE DE LA GARANTÍA (D-64): el hash NO lleva clave ni hay un ancla fuera
del archivo. El modelo de amenaza asume que el sistema de archivos local es
de confianza. Por tanto la cadena NO protege frente a quien tenga acceso de
escritura al .db y reescriba la cadena recalculando los hashes siguientes
(`verify()` devolvería VALID), ni detecta que se hayan borrado las ÚLTIMAS
entradas. Si el modelo de amenaza cambia, hace falta un HMAC con clave
externa o un ancla periódica (seq, último hash) fuera de la base.

GENESIS_HASH es el prev_hash de la primera entrada — un valor fijo y
conocido, nunca "null" ni "vacío", para que la primera entrada también
sea verificable (un encadenamiento que empieza en null es indistinguible
de un encadenamiento truncado que perdió su primera fila).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from .canonical import canonical_json, content_hash
from .enums import AuditIntegrityState
from .store import SQLiteStore

GENESIS_HASH = "0" * 64


@dataclass(frozen=True)
class AuditVerificationResult:
    status: AuditIntegrityState
    total_entries: int
    first_bad_seq: Optional[int] = None
    detail: Optional[str] = None


class AuditChain:
    def __init__(self, store: SQLiteStore):
        self._store = store

    def append(self, payload: dict) -> str:
        """
        Añade una entrada encadenada al hash de la última. Devuelve el
        entry_hash resultante. Se hace dentro de una transacción para
        que leer "cuál es la última entrada" y escribir la nueva sean
        atómicos entre sí (si dos llamadas concurrentes calcularan
        prev_hash sobre la misma última fila, una de las dos rompería la
        cadena — BEGIN IMMEDIATE en store.transaction() serializa esto).
        """
        payload_canonical = canonical_json(payload)
        with self._store.transaction() as conn:
            cur = conn.execute("SELECT entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1")
            row = cur.fetchone()
            prev_hash = row[0] if row is not None else GENESIS_HASH

            entry_hash = content_hash({"prev_hash": prev_hash, "payload": payload})

            conn.execute(
                "INSERT INTO audit_log (entry_hash, prev_hash, payload) VALUES (?, ?, ?)",
                (entry_hash, prev_hash, payload_canonical),
            )
            return entry_hash

    def verify(self, *, batch_size: int = SQLiteStore.AUDIT_BATCH_DEFAULT) -> AuditVerificationResult:
        """
        Recorre toda la cadena en orden y para cada entrada recalcula
        entry_hash = content_hash({prev_hash, payload}) a partir del
        payload realmente almacenado. Si no coincide con el entry_hash
        guardado, alguien modificó esa fila directamente (CORRUPTED). Si
        el prev_hash guardado en la fila no coincide con el entry_hash
        real de la fila anterior, la cadena fue reescrita/reordenada
        (FORKED). Un hueco en `seq`, o una cadena que no empieza en
        seq=1, indica que una fila fue eliminada directamente en el
        archivo, evadiendo el trigger (TRUNCATED). Un payload que no es
        JSON válido, o que no se puede canonicalizar (NaN, Infinity, surrogate
        aislado, anidación excesiva), se informa como CORRUPTED (no lanza; D-65). Ver el LÍMITE
        DE LA GARANTÍA en el docstring del módulo: no detecta una cadena
        reescrita con los hashes recalculados ni el borrado de las
        últimas entradas.

        D-71: la cadena se lee por lotes de `batch_size` filas (`SQLiteStore.iter_audit_chain`), de modo que la memoria es O(batch_size) y no O(n).
        El resultado es el mismo que con la lectura completa: `total_entries` es el total de filas de la tabla también cuando se detecta un fallo
        (en ese caso se cuenta con `count_audit_entries`, sin cargarlas); si la cadena es VALID, es el número de filas verificadas.
        """
        total = 0
        expected_seq = 1  # AUTOINCREMENT empieza en 1; sin esto, borrar la cabeza se vería como FORKED y no como TRUNCATED
        running_hash = GENESIS_HASH

        for batch in self._store.iter_audit_chain(batch_size=batch_size):
            for seq, entry_hash, prev_hash, payload_text in batch:
                if seq != expected_seq:
                    return AuditVerificationResult(
                        status=AuditIntegrityState.TRUNCATED,
                        total_entries=self._store.count_audit_entries(),
                        first_bad_seq=expected_seq,
                        detail=f"hueco en seq: se esperaba {expected_seq}, se encontró {seq} "
                        f"(una fila fue eliminada directamente, evadiendo el trigger append-only)",
                    )

                if prev_hash != running_hash:
                    return AuditVerificationResult(
                        status=AuditIntegrityState.FORKED,
                        total_entries=self._store.count_audit_entries(),
                        first_bad_seq=seq,
                        detail=f"prev_hash de seq={seq} no coincide con el entry_hash real de la "
                        f"entrada anterior (la cadena fue reescrita o reordenada)",
                    )

                try:
                    payload = json.loads(payload_text)
                    recomputed = content_hash({"prev_hash": prev_hash, "payload": payload})
                except (TypeError, ValueError, RecursionError) as e:
                    # D-65: no solo un texto que no es JSON: también JSON que `json.loads` acepta y la forma canónica rechaza (NaN, Infinity,
                    # un surrogate aislado -> UnicodeEncodeError, que es un ValueError) o anidado hasta agotar la pila (RecursionError).
                    # Una fila que no se puede recalcular es una fila alterada: se informa, no se propaga.
                    return AuditVerificationResult(
                        status=AuditIntegrityState.CORRUPTED,
                        total_entries=self._store.count_audit_entries(),
                        first_bad_seq=seq,
                        detail=f"payload de seq={seq} no es un JSON canonicalizable "
                        f"({type(e).__name__}); el payload fue modificado directamente en el archivo",
                    )
                if recomputed != entry_hash:
                    return AuditVerificationResult(
                        status=AuditIntegrityState.CORRUPTED,
                        total_entries=self._store.count_audit_entries(),
                        first_bad_seq=seq,
                        detail=f"entry_hash de seq={seq} no coincide con el recálculo sobre el "
                        f"payload almacenado (el payload fue modificado directamente en el archivo)",
                    )

                running_hash = entry_hash
                expected_seq += 1
                total += 1

        return AuditVerificationResult(status=AuditIntegrityState.VALID, total_entries=total)

    def last_hash(self) -> str:
        last = self._store.get_last_audit_hash()
        return last if last is not None else GENESIS_HASH
