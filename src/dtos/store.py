"""
store.py
========
Capa de persistencia FASE 2, sobre el schema baseline (schema.sql).

Principio de diseño (P-001, SECURITY BOUNDARIES): este módulo es la
ÚNICA parte del sistema que abre una conexión SQLite y ejecuta SQL. Las
FSM (fsm.py y derivados) y los DTOs (contracts.py, registries.py) no
saben que SQLite existe — así, cambiar de motor de persistencia más
adelante no obliga a tocar Cognitive Core (mismo principio que aplica a
Reasoning Provider, aplicado aquí a storage).

Implementa tres mecanismos que en el Registro Maestro son prosa y aquí
son código que puede fallar de verdad:

1. COMMIT GATE (optimistic concurrency) — sección STATE UPDATE -> COMMIT.
   `commit_cognitive_cycle_transition` exige expected_version en el
   WHERE; si rows_affected != 1, lanza ConflictError. No hay forma de
   hacer un UPDATE "a ciegas" a través de este módulo.

2. GENESIS AUTHORITY BOOTSTRAP (P-062/P-063) — la unicidad de la
   autoridad génesis por instalación está en un índice único parcial de
   SQLite (ver schema.sql), no en un `if` de Python. Un segundo intento
   de escribir genesis para la misma instalación siempre falla, incluso
   si dos procesos corren en paralelo.

3. STATE + DOMAIN EVENT + OUTBOX ATÓMICOS (P-016, sección STATE UPDATE
   -> COMMIT -> DOMAIN EVENT -> OUTBOX -> PROJECTION) — una sola
   transacción SQLite. Si falla escribir el evento de dominio, el cambio
   de estado tampoco queda — no puede haber un commit "a medias".
"""

from __future__ import annotations

import json
import os
import random
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from .canonical import canonical_json, content_hash
from .enums import AuditIntegrityState, MandateEventType, MandateStrength, MandateTriggerKind
from .registries import ArtifactRecord, MandateRecord, PolicyRecord
from .validation import require_nonempty_str


class ConflictError(ValueError):
    """
    Commit Gate rechazó la escritura: expected_version no coincidía
    (otro escritor ganó la carrera) o la fila no existe.
    """


# Tope de la evidencia serializada (JSON canónico, medido en BYTES UTF-8) que se persiste en `effects.evidence` y en `effect_reconciliations.evidence`.
# 65536 es el valor propuesto por el usuario; no sale de mediciones. Es un límite de política: subirlo o bajarlo es decisión del operador.
MAX_EVIDENCE_SIZE_BYTES = 65536


class EvidenceTooLargeError(ValueError):
    """La evidencia serializada supera MAX_EVIDENCE_SIZE_BYTES: no se persiste y no cambia ningún estado."""


def check_evidence_size(serialized: Optional[str], *, where: str) -> None:
    if serialized is None:
        return
    size = len(serialized.encode("utf-8"))
    if size > MAX_EVIDENCE_SIZE_BYTES:
        raise EvidenceTooLargeError(f"{where}: la evidencia ocupa {size} bytes y el máximo es {MAX_EVIDENCE_SIZE_BYTES}")


class BootstrapConflictError(ValueError):
    """
    P-063: ya existe un GENESIS_AUTHORITY_RECORD para esta instalación.
    Nunca se sobrescribe — esto es el UNIQUE INDEX de schema.sql
    manifestándose como excepción de Python.
    """


class AppendOnlyViolationError(ValueError):
    """
    Se intentó UPDATE/DELETE sobre una tabla append-only. En la práctica
    esto lo lanza SQLite (los triggers RAISE(ABORT,...) de schema.sql);
    esta clase envuelve ese sqlite3.IntegrityError/OperationalError con
    un mensaje más claro para quien llama.
    """


@dataclass(frozen=True)
class CommitResult:
    rows_affected: int
    new_version: int


@dataclass(frozen=True)
class BackupResult:
    """D-72: resultado de `SQLiteStore.backup_to`. `chain_verified` es True solo si se ejecutó `AuditChain.verify()` sobre la copia y dio VALID
    (con `verify_chain=False` es False y `audit_entries` es solo el número de filas de la copia)."""
    path: str
    size_bytes: int
    audit_entries: int
    chain_verified: bool


# Espera máxima (ms) ante un bloqueo temporal de la base antes de lanzar sqlite3.OperationalError «database is locked». Mismo valor y misma
# estrategia que UNAE (unae/store.py: timeout=10 + PRAGMA busy_timeout=10000). Antes de D-57 DTOS dependía del `timeout=5.0` implícito de
# sqlite3.connect (5000 ms). Es un valor de política, no sale de mediciones: el operador puede cambiarlo con `busy_timeout_ms`.
DEFAULT_BUSY_TIMEOUT_MS = 10_000

# D-73 (parche 0021): valor por defecto de `PRAGMA journal_size_limit` (bytes). 64 MiB es un valor de política propuesto y ratificado por el usuario, no sale de
# mediciones. QUÉ HACE: tras un checkpoint completo, cuando el WAL se reinicia con la siguiente escritura, SQLite lo recorta a este tamaño en vez de dejarlo
# del tamaño máximo alcanzado (por defecto de SQLite, -1, el archivo -wal se queda así hasta cerrar la última conexión). QUÉ NO HACE: no limita el
# crecimiento del WAL mientras una lectura abierta (p. ej. `backup_to`) impide el checkpoint. -1 = sin límite (comportamiento nativo); 0 = recortar al mínimo.
DEFAULT_JOURNAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024

# D-75 (parche 0023 / g0.8 r30): `sqlite3_busy_timeout` recibe un `int` de C (32 bits con signo). Con un valor >= 2**31 el PRAGMA se desborda y SQLite queda en 0 ms
# (medido en SQLite 3.45.1): la validación lo dejaba pasar y el error llegaba DESPUÉS de crear el archivo. 2**31 - 1 ms son ~24,8 días; es un límite del motor, no de política.
MAX_BUSY_TIMEOUT_MS = 2 ** 31 - 1


def _validate_connection_policy(busy_timeout_ms, journal_size_limit_bytes) -> tuple[int, int]:
    """Valida y NORMALIZA la política de conexión: devuelve `(busy_timeout_ms, journal_size_limit_bytes)` como `int` EXACTOS (D-75). El llamante usa los
    valores devueltos, nunca los recibidos: una subclase de `int` puede redefinir `__format__`, `__int__` o las comparaciones, y esos valores acaban dentro de
    una sentencia PRAGMA (no admite parámetros). `int.__int__` (sin pasar por la subclase) copia el valor numérico real; el rango se comprueba sobre esa copia."""
    busy = None if isinstance(busy_timeout_ms, bool) or not isinstance(busy_timeout_ms, int) else int.__int__(busy_timeout_ms)
    if busy is None or not (0 <= busy <= MAX_BUSY_TIMEOUT_MS):
        raise ValueError(f"busy_timeout_ms: debe ser un entero entre 0 y {MAX_BUSY_TIMEOUT_MS} (milisegundos), recibido {busy_timeout_ms!r}")
    limit = None if isinstance(journal_size_limit_bytes, bool) or not isinstance(journal_size_limit_bytes, int) else int.__int__(journal_size_limit_bytes)
    if limit is None or not (-1 <= limit < 2 ** 63):
        raise ValueError(f"journal_size_limit_bytes: debe ser un entero >= -1 (-1 = sin límite, 0 = recorte al mínimo), recibido {journal_size_limit_bytes!r}")
    return busy, limit

# D-67 (parche 0016): reintento acotado de la APERTURA. SQLite puede devolver «database is locked» SIN esperar el busy_timeout cuando esperar
# podría causar un bloqueo mutuo (lo documenta SQLite). Medido en Windows 10 (g0.8 r17): con 8 procesos abriendo a la vez una base nueva, uno
# falló a los 4 ms (la apertura tardaba de 4 a 10 s por proceso porque el esquema son ~60 sentencias en autocommit con un fsync cada una).
# Pausas crecientes con jitter para que procesos idénticos no choquen otra vez al unísono. Valores de política, no salen de mediciones.
OPEN_RETRY_FIRST_S = 0.01
OPEN_RETRY_MAX_S = 0.25


def _is_lock_error(exc: BaseException) -> bool:
    """True si `exc` es un bloqueo temporal de SQLite (SQLITE_BUSY o SQLITE_LOCKED, con o sin código extendido). Sin código (Python < 3.11 o
    errores construidos a mano) se decide por el texto, igual que las pruebas existentes."""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int):
        return (code & 0xFF) in (5, 6)
    return "locked" in str(exc).lower()


class SQLiteStore:
    """Persistencia de DTOS sobre un archivo SQLite compartido por varios procesos.

    Política de concurrencia (D-57), explícita y verificada al abrir cada conexión:
      - `PRAGMA busy_timeout = busy_timeout_ms`: el driver reintenta solo ante un bloqueo temporal en lugar de fallar al instante.
      - `PRAGMA journal_mode = WAL`: las lecturas no bloquean a la escritura ni al revés. Si la base no puede quedar en WAL (p. ej. un
        sistema de archivos de red) se lanza RuntimeError al abrir; no se degrada en silencio a un modo sin esa garantía.
      - `PRAGMA journal_size_limit = journal_size_limit_bytes` (D-73, 64 MiB por defecto; -1 sin límite, 0 recorte al mínimo): recorta el `-wal` tras un
        checkpoint completo. Es por CONEXIÓN y lo aplica la conexión que escribe cuando el WAL se reinicia: una conexión que escribe sin este PRAGMA no
        recorta. NO acota el crecimiento del WAL mientras una lectura abierta frena el checkpoint (p. ej. durante `backup_to`).
      - Toda escritura pasa por `transaction()` (BEGIN IMMEDIATE): los escritores se serializan en el bloqueo de escritura y nunca hay una
        promoción lectura->escritura que pueda fallar sin reintento. Excepción: la creación del esquema (`executescript`), que son
        sentencias idempotentes (IF NOT EXISTS) y reintentan por el busy_timeout; las migraciones de columnas sí van en transacción.
      - La APERTURA (PRAGMAs + esquema) se reintenta ante un bloqueo temporal hasta agotar `busy_timeout_ms` en total (D-67): SQLite a veces
        devuelve BUSY sin esperar. Con `busy_timeout_ms=0` no se reintenta. Los errores que no son de bloqueo no se reintentan.
    El timeout NO elimina los bloqueos: si otro proceso mantiene el bloqueo de escritura más de `busy_timeout_ms`, la operación lanza
    sqlite3.OperationalError (no se reintenta aquí; la transacción no deja nada escrito)."""

    def __init__(self, db_path: str | Path, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
                 journal_size_limit_bytes: int = DEFAULT_JOURNAL_SIZE_LIMIT_BYTES):
        busy_timeout_ms, journal_size_limit_bytes = _validate_connection_policy(busy_timeout_ms, journal_size_limit_bytes)  # D-75: valores normalizados
        self.db_path = str(db_path)
        self.busy_timeout_ms = busy_timeout_ms
        self.journal_size_limit_bytes = journal_size_limit_bytes
        self._conn = sqlite3.connect(self.db_path, timeout=busy_timeout_ms / 1000, isolation_level=None)  # autocommit off, control manual
        try:
            self._open_with_retry()
        except BaseException:
            self._conn.close()
            raise

    def _open_with_retry(self) -> None:
        """D-67: configura la conexión y crea/migra el esquema; ante un bloqueo temporal repite TODO el paso (es idempotente) con pausas crecientes
        hasta que se agota `busy_timeout_ms` contado desde el inicio de la apertura. Pasado el plazo se relanza el error original."""
        deadline = time.monotonic() + self.busy_timeout_ms / 1000
        delay = OPEN_RETRY_FIRST_S
        while True:
            try:
                self._configure_connection()
                self._init_schema()
                return
            except sqlite3.OperationalError as exc:
                if not _is_lock_error(exc):
                    raise
                if self._conn.in_transaction:  # un fallo a medias no debe dejar una transacción abierta en el siguiente intento
                    try:
                        self._conn.execute("ROLLBACK;")
                    except sqlite3.Error:
                        pass
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(delay * (0.5 + random.random() / 2), remaining))
                delay = min(delay * 2, OPEN_RETRY_MAX_S)

    def _configure_connection(self) -> None:
        """PRAGMAs de concurrencia, ANTES de tocar el esquema (así el cambio a WAL y la creación de tablas ya reintentan ante bloqueos)."""
        self._conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms};")  # int validado en __init__: no hay entrada externa en la sentencia
        effective = self._conn.execute("PRAGMA busy_timeout;").fetchone()[0]
        if effective != self.busy_timeout_ms:
            raise RuntimeError(f"busy_timeout: pedido {self.busy_timeout_ms} ms y SQLite quedó en {effective} ms")
        mode = str(self._conn.execute("PRAGMA journal_mode = WAL;").fetchone()[0]).lower()
        in_memory = self.db_path in (":memory:", "")
        if mode != "wal" and not (in_memory and mode == "memory"):
            raise RuntimeError(
                f"journal_mode: se pidió WAL y SQLite devolvió {mode!r} para {self.db_path!r} (¿sistema de archivos de red o sin soporte de "
                f"memoria compartida?). DTOS exige WAL para desacoplar lecturas y escrituras entre procesos."
            )
        self._conn.execute(f"PRAGMA journal_size_limit = {self.journal_size_limit_bytes};")  # int validado en __init__: no hay entrada externa en la sentencia
        limit = self._conn.execute("PRAGMA journal_size_limit;").fetchone()[0]
        if limit != self.journal_size_limit_bytes:
            raise RuntimeError(f"journal_size_limit: pedido {self.journal_size_limit_bytes} bytes y SQLite quedó en {limit}")
        self._conn.execute("PRAGMA foreign_keys = ON;")

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        schema_path = Path(__file__).parent / "schema.sql"
        with open(schema_path, "r", encoding="utf-8") as f:
            self._conn.executescript(f.read())
        # Migración: bases creadas antes de que `effects` llevara `evidence` (CREATE TABLE IF NOT EXISTS no altera tablas existentes).
        self._add_column_if_missing("effects", "evidence", "TEXT")
        # Migración (parche 0008): bases creadas antes de que `policies` llevara `content`; las filas anteriores quedan con content NULL.
        self._add_column_if_missing("policies", "content", "TEXT")

    def _add_column_if_missing(self, table: str, column: str, declaration: str) -> None:
        """Migración segura entre procesos (D-57): sin esto, dos procesos que abren a la vez una base antigua ven los dos «falta la columna» y
        el segundo ALTER falla con «duplicate column name». Se comprueba sin bloqueo (camino habitual: no hay nada que migrar) y, si falta,
        se vuelve a comprobar DENTRO de BEGIN IMMEDIATE antes del ALTER. Los tres argumentos son constantes del propio módulo."""
        def has_column(conn: sqlite3.Connection) -> bool:
            return column in {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}

        if has_column(self._conn):
            return
        with self.transaction() as conn:
            if not has_column(conn):
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """
        Envoltorio explícito de transacción. Nunca se hace I/O externo
        (red, tools, reasoning provider) dentro de este bloque — esa
        regla no la puede imponer el código, pero el nombre y el docstring
        están para que quien la use la respete (ver P-016, sección
        COMMIT BOUNDARY: 'no mantener la transacción abierta durante
        inferencia, APIs o herramientas').
        """
        self._conn.execute("BEGIN IMMEDIATE;")
        try:
            yield self._conn
            self._conn.execute("COMMIT;")
        except BaseException:  # D-65: también KeyboardInterrupt/SystemExit; si no, la transacción quedaba abierta y la conexión inservible
            if self._conn.in_transaction:
                try:
                    self._conn.execute("ROLLBACK;")
                except sqlite3.Error:
                    pass  # no se oculta la excepción original con un fallo del propio ROLLBACK
            raise

    # -----------------------------------------------------------------
    # COGNITIVE CYCLES — Commit Gate de referencia
    # -----------------------------------------------------------------

    def create_cognitive_cycle(
        self, cycle_id: str, task_id: str, state: str, state_hash: str, context_hash: str
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO cognitive_cycles
                   (cycle_id, task_id, state, cycle_version, state_hash, context_hash)
                   VALUES (?, ?, ?, 1, ?, ?)""",
                (cycle_id, task_id, state, state_hash, context_hash),
            )

    def commit_cognitive_cycle_transition(
        self,
        cycle_id: str,
        new_state: str,
        expected_version: int,
        new_state_hash: str,
        new_context_hash: str,
        domain_event_payload: Optional[dict] = None,
    ) -> CommitResult:
        """
        Commit Gate: UPDATE solo tiene efecto si cycle_version ==
        expected_version. Si otro escritor ya avanzó la versión,
        rows_affected será 0 -> ConflictError (T-CONC-03 del Registro
        Maestro, verificado abajo en test_store.py).

        Si domain_event_payload se provee, el DomainEvent + Outbox se
        escriben en la MISMA transacción que el UPDATE del estado
        (P-016): ambos caben o ninguno cabe.
        """
        with self.transaction() as conn:
            cur = conn.execute(
                """UPDATE cognitive_cycles
                   SET state = ?, cycle_version = cycle_version + 1,
                       state_hash = ?, context_hash = ?
                   WHERE cycle_id = ? AND cycle_version = ?""",
                (new_state, new_state_hash, new_context_hash, cycle_id, expected_version),
            )
            if cur.rowcount != 1:
                raise ConflictError(
                    f"commit_cognitive_cycle_transition: expected_version="
                    f"{expected_version} no coincide (o cycle_id no existe) para "
                    f"{cycle_id}. rows_affected={cur.rowcount}."
                )

            if domain_event_payload is not None:
                conn.execute(
                    """INSERT INTO outbox (event_type, payload, dependency_epoch)
                       VALUES (?, ?, ?)""",
                    (
                        "CognitiveCycleStateChanged",
                        canonical_json(domain_event_payload),
                        domain_event_payload.get("dependency_epoch", 0),
                    ),
                )

            return CommitResult(rows_affected=cur.rowcount, new_version=expected_version + 1)

    def get_cognitive_cycle(self, cycle_id: str) -> Optional[dict]:
        cur = self._conn.execute(
            "SELECT cycle_id, task_id, state, cycle_version, state_hash, context_hash "
            "FROM cognitive_cycles WHERE cycle_id = ?",
            (cycle_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = ["cycle_id", "task_id", "state", "cycle_version", "state_hash", "context_hash"]
        return dict(zip(cols, row))

    # -----------------------------------------------------------------
    # AUTHORITY BOOTSTRAP — unicidad real de GENESIS_AUTHORITY_RECORD
    # -----------------------------------------------------------------

    def write_genesis_authority(
        self, authority_id: str, installation_id: str, principal: str
    ) -> None:
        """
        P-062: epoch=1, fencing=1, siempre. P-063: si ya existe genesis
        para esta installation_id, el UNIQUE INDEX de schema.sql rechaza
        el INSERT y esto se traduce en BootstrapConflictError — nunca en
        un UPDATE silencioso que sobrescriba.
        """
        try:
            with self.transaction() as conn:
                conn.execute(
                    """INSERT INTO authorities
                       (authority_id, installation_id, principal, epoch,
                        capabilities, expiry, fencing, is_genesis)
                       VALUES (?, ?, ?, 1, '[]', NULL, 1, 1)""",
                    (authority_id, installation_id, principal),
                )
        except sqlite3.IntegrityError as e:
            raise BootstrapConflictError(
                f"Ya existe GENESIS_AUTHORITY_RECORD para installation_id="
                f"{installation_id!r} (P-063: bootstrap conflict != overwrite)."
            ) from e

    # -----------------------------------------------------------------
    # ARTIFACTS — append-only, identidad artifact_id+version+content_hash
    # -----------------------------------------------------------------

    def insert_artifact(self, record: ArtifactRecord) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO artifacts
                   (artifact_id, version, artifact_class, content_hash,
                    provenance, status, corrects, supersedes)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record.artifact_id,
                    record.version,
                    record.artifact_class,
                    record.content_hash,
                    record.provenance,
                    record.status,
                    None,
                    None,
                ),
            )

    def update_artifact_status_forbidden(self, artifact_id: str, new_status: str) -> None:
        """
        Existe deliberadamente para que test_store.py demuestre que un
        UPDATE sobre `artifacts` es rechazado por el trigger de SQLite
        (P-065), no solo por convención de la aplicación.
        """
        try:
            with self.transaction() as conn:
                conn.execute(
                    "UPDATE artifacts SET status = ? WHERE artifact_id = ?",
                    (new_status, artifact_id),
                )
        except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
            raise AppendOnlyViolationError(str(e)) from e

    # -----------------------------------------------------------------
    # POLICIES (REG-004) — parche 0008: contenido canónico verificado por hash
    # -----------------------------------------------------------------

    def insert_policy(self, record: PolicyRecord) -> None:
        """Inserta una política. La coherencia hash/content ya la garantiza PolicyRecord al construirse. Una (policy_id, version)
        repetida lanza sqlite3.IntegrityError (clave primaria), igual que insert_artifact."""
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO policies (policy_id, version, scope, effective_from, hash, content) VALUES (?, ?, ?, ?, ?, ?)",
                (record.policy_id, record.version, record.scope, record.effective_from, record.hash, record.content),
            )

    def get_policy(self, policy_id: str, version: str) -> Optional[PolicyRecord]:
        """Lee una política. Reconstruye el PolicyRecord, así que una fila con content y hash incoherentes (alterada fuera de este
        módulo) lanza PolicyContentError en lugar de devolverse como válida."""
        row = self._conn.execute(
            "SELECT policy_id, version, scope, effective_from, hash, content FROM policies WHERE policy_id = ? AND version = ?",
            (policy_id, version),
        ).fetchone()
        return None if row is None else PolicyRecord(*row)

    # -----------------------------------------------------------------
    # MANDATES (REG-017) — parche 0009: un mandato por Task, inmutable, con su evento de creación
    # -----------------------------------------------------------------

    _MANDATE_COLUMNS = "mandate_id, task_id, strength, trigger_kind, created_at, rule_id, policy_id, policy_version, rule_hash"

    def create_mandate(self, record: MandateRecord, event_id: str) -> None:
        """Inserta el mandato y su evento MANDATE_CREATED (payload = JSON canónico del mandato) en UNA transacción: o quedan los dos o ninguno.
        `event_id` lo aporta quien llama, como el resto de identificadores de dominio. Un segundo mandato para la misma Task, un
        mandate_id repetido, una Task o política inexistentes (clave foránea) lanzan sqlite3.IntegrityError y no dejan filas."""
        require_nonempty_str(event_id, "event_id")
        with self.transaction() as conn:
            conn.execute(
                f"INSERT INTO mandates ({self._MANDATE_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (record.mandate_id, record.task_id, record.strength.value, record.trigger_kind.value, record.created_at,
                 record.rule_id, record.policy_id, record.policy_version, record.rule_hash),
            )
            conn.execute(
                "INSERT INTO mandate_events (event_id, mandate_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                (event_id, record.mandate_id, MandateEventType.MANDATE_CREATED.value, canonical_json(record.to_canonical_dict()), record.created_at),
            )

    @staticmethod
    def _mandate_from_row(row) -> MandateRecord:
        return MandateRecord(
            mandate_id=row[0], task_id=row[1], strength=MandateStrength(row[2]), trigger_kind=MandateTriggerKind(row[3]),
            created_at=row[4], rule_id=row[5], policy_id=row[6], policy_version=row[7], rule_hash=row[8],
        )

    def get_mandate(self, mandate_id: str) -> Optional[MandateRecord]:
        row = self._conn.execute(f"SELECT {self._MANDATE_COLUMNS} FROM mandates WHERE mandate_id = ?", (mandate_id,)).fetchone()
        return None if row is None else self._mandate_from_row(row)

    def get_mandate_for_task(self, task_id: str) -> Optional[MandateRecord]:
        row = self._conn.execute(f"SELECT {self._MANDATE_COLUMNS} FROM mandates WHERE task_id = ?", (task_id,)).fetchone()
        return None if row is None else self._mandate_from_row(row)

    def get_mandate_events(self, mandate_id: str) -> list[dict]:
        """Eventos del mandato en orden de registro (created_at y, a igualdad, rowid)."""
        cur = self._conn.execute(
            "SELECT event_id, mandate_id, event_type, payload, created_at FROM mandate_events WHERE mandate_id = ? ORDER BY created_at, rowid",
            (mandate_id,),
        )
        cols = ["event_id", "mandate_id", "event_type", "payload", "created_at"]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # -----------------------------------------------------------------
    # EFFECTS — idempotencia real vía UNIQUE(provider, idempotency_key)
    # -----------------------------------------------------------------

    def insert_effect(
        self,
        effect_id: str,
        operation_id: str,
        intent_id: str,
        attempt_id: str,
        provider: str,
        idempotency_key: str,
        status: str,
        evidence: Optional[str] = None,
    ) -> None:
        check_evidence_size(evidence, where="insert_effect")
        try:
            with self.transaction() as conn:
                conn.execute(
                    """INSERT INTO effects
                       (effect_id, operation_id, intent_id, attempt_id,
                        provider, idempotency_key, status, evidence)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (effect_id, operation_id, intent_id, attempt_id, provider, idempotency_key, status, evidence),
                )
        except sqlite3.IntegrityError as e:
            raise ConflictError(
                f"insert_effect: ya existe un efecto con provider={provider!r} "
                f"idempotency_key={idempotency_key!r} (misma key = misma "
                f"operación semántica; si el fingerprint difiere, es un "
                f"conflicto real, no un duplicado benigno)."
            ) from e

    # -----------------------------------------------------------------
    # EFFECT ATTEMPTS — estado mutable en curso (Commit Gate), distinto
    # del ledger append-only `effects` de arriba.
    # -----------------------------------------------------------------

    def create_effect_attempt(
        self, attempt_id: str, intent_id: str, provider: str, idempotency_key: str, status: str
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO effect_attempts
                   (attempt_id, intent_id, provider, idempotency_key, status, version)
                   VALUES (?, ?, ?, ?, ?, 1)""",
                (attempt_id, intent_id, provider, idempotency_key, status),
            )

    def commit_effect_attempt_transition(
        self, attempt_id: str, new_status: str, expected_version: int
    ) -> CommitResult:
        with self.transaction() as conn:
            cur = conn.execute(
                """UPDATE effect_attempts SET status = ?, version = version + 1
                   WHERE attempt_id = ? AND version = ?""",
                (new_status, attempt_id, expected_version),
            )
            if cur.rowcount != 1:
                raise ConflictError(
                    f"commit_effect_attempt_transition: expected_version="
                    f"{expected_version} no coincide (o attempt_id no existe) para "
                    f"{attempt_id}. rows_affected={cur.rowcount}."
                )
            return CommitResult(rows_affected=cur.rowcount, new_version=expected_version + 1)


    def reconcile_effect_attempt_atomic(
        self,
        attempt_id: str,
        new_status: str,
        expected_version: int,
        effect_row: Optional[dict] = None,
        reconciliation: Optional[dict] = None,
    ) -> CommitResult:
        """
        Transición del intento (optimistic concurrency), fila del ledger
        `effects` si es terminal y fila del historial `effect_reconciliations`,
        todo en UNA sola transacción: o quedan todas o no queda ninguna.
        `effect_row` lleva effect_id, operation_id, intent_id, attempt_id,
        provider, idempotency_key y, opcional, evidence (JSON canónico).
        `reconciliation` lleva from_status, strategy y evidence (JSON canónico).
        Una evidencia que supera MAX_EVIDENCE_SIZE_BYTES se rechaza ANTES de
        abrir la transacción (EvidenceTooLargeError).
        """
        check_evidence_size(effect_row.get("evidence") if effect_row else None, where="reconcile_effect_attempt_atomic(effects)")
        check_evidence_size(reconciliation["evidence"] if reconciliation else None, where="reconcile_effect_attempt_atomic(effect_reconciliations)")
        try:
            with self.transaction() as conn:
                cur = conn.execute(
                    """UPDATE effect_attempts SET status = ?, version = version + 1
                       WHERE attempt_id = ? AND version = ?""",
                    (new_status, attempt_id, expected_version),
                )
                if cur.rowcount != 1:
                    raise ConflictError(
                        f"reconcile_effect_attempt_atomic: expected_version="
                        f"{expected_version} no coincide (o attempt_id no existe) para "
                        f"{attempt_id}. rows_affected={cur.rowcount}."
                    )
                if effect_row is not None:
                    conn.execute(
                        """INSERT INTO effects
                           (effect_id, operation_id, intent_id, attempt_id,
                            provider, idempotency_key, status, evidence)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (effect_row["effect_id"], effect_row["operation_id"], effect_row["intent_id"],
                         effect_row["attempt_id"], effect_row["provider"], effect_row["idempotency_key"], new_status,
                         effect_row.get("evidence")),
                    )
                if reconciliation is not None:
                    conn.execute(
                        """INSERT INTO effect_reconciliations
                           (attempt_id, from_status, to_status, strategy, evidence)
                           VALUES (?, ?, ?, ?, ?)""",
                        (attempt_id, reconciliation["from_status"], new_status, reconciliation["strategy"], reconciliation["evidence"]),
                    )
                return CommitResult(rows_affected=cur.rowcount, new_version=expected_version + 1)
        except sqlite3.IntegrityError as e:
            if effect_row is not None:
                raise ConflictError(
                    f"reconcile_effect_attempt_atomic: ya existe un efecto con provider="
                    f"{effect_row['provider']!r} idempotency_key={effect_row['idempotency_key']!r} "
                    f"(misma key = misma operación); no se resolvió el intento."
                ) from e
            raise ConflictError(f"reconcile_effect_attempt_atomic: restricción de integridad violada: {e}") from e

    def get_effects_by_attempt(self, attempt_id: str) -> list[dict]:
        """Lectura pública (solo lectura) del ledger append-only `effects` para un intento."""
        cur = self._conn.execute("SELECT * FROM effects WHERE attempt_id = ?", (attempt_id,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_effect_by_provider_key(self, provider: str, idempotency_key: str) -> Optional[dict]:
        """Lectura pública (solo lectura) del ledger `effects` por la clave única (provider, idempotency_key); None si no existe.
        Existe para que quien diagnostica una colisión de esa clave no abra la conexión privada del Store."""
        cur = self._conn.execute("SELECT * FROM effects WHERE provider = ? AND idempotency_key = ?", (provider, idempotency_key))
        row = cur.fetchone()
        if row is None:
            return None
        return dict(zip([d[0] for d in cur.description], row))

    def read_audit_chain(self) -> list[tuple]:
        """Lectura pública (solo lectura) de TODA la cadena de auditoría, en orden de `seq`: tuplas (seq, entry_hash, prev_hash, payload_texto).
        La verificación de la cadena (AuditLog.verify) trabaja sobre esto; no valida nada por sí misma."""
        return [tuple(r) for r in self._conn.execute("SELECT seq, entry_hash, prev_hash, payload FROM audit_log ORDER BY seq ASC").fetchall()]

    AUDIT_BATCH_DEFAULT = 1000
    AUDIT_BATCH_MAX = 100_000

    def iter_audit_chain(self, *, batch_size: int = AUDIT_BATCH_DEFAULT) -> Iterator[list[tuple]]:
        """D-71: lectura pública (solo lectura) de la cadena de auditoría POR LOTES, en orden de `seq`: cada elemento es una lista de hasta `batch_size`
        tuplas (seq, entry_hash, prev_hash, payload_texto), las mismas que `read_audit_chain()`. Paginación por clave (`seq > último seq leído`), no por
        OFFSET: cada lote cuesta lo mismo aunque la cadena sea enorme y la memoria es O(batch_size), no O(n). Cada lote es una consulta completa y su
        cursor se cierra antes de ceder el control: entre lotes NO queda abierta ninguna lectura (no se retiene el checkpoint de WAL) y el llamador puede
        tardar lo que quiera. Consecuencia: no es una instantánea; una entrada añadida mientras se itera aparece al final, como cualquier otra, y una fila
        borrada a mano en un tramo ya leído no se ve (es el mismo límite de D-64: la cadena no protege frente a quien escribe en el archivo).
        `batch_size` debe ser un entero entre 1 y `AUDIT_BATCH_MAX` (no bool)."""
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or not (1 <= batch_size <= self.AUDIT_BATCH_MAX):
            raise ValueError(f"batch_size: debe ser un entero entre 1 y {self.AUDIT_BATCH_MAX}, recibido {batch_size!r}")  # al llamar, no al primer next()
        return self._audit_batches(batch_size)

    def _audit_batches(self, batch_size: int) -> Iterator[list[tuple]]:
        after = -(2 ** 63)  # menor que cualquier seq posible: el primer lote lee desde el principio, incluso con un seq <= 0 escrito a mano
        while True:
            rows = [tuple(r) for r in self._conn.execute(
                "SELECT seq, entry_hash, prev_hash, payload FROM audit_log WHERE seq > ? ORDER BY seq ASC LIMIT ?", (after, batch_size)).fetchall()]
            if not rows:
                return
            yield rows
            after = rows[-1][0]

    def count_audit_entries(self) -> int:
        """D-71: lectura pública: número de filas de `audit_log` (sin cargarlas)."""
        return self._conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]

    def get_last_audit_hash(self) -> Optional[str]:
        """Lectura pública: `entry_hash` de la última entrada de la cadena de auditoría; None si la cadena está vacía."""
        row = self._conn.execute("SELECT entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
        return row[0] if row is not None else None

    def get_pending_outbox(self, limit: int = 100) -> list[tuple]:
        """Lectura pública (solo lectura) del outbox: tuplas (outbox_id, event_type, payload_texto) de los eventos con published = 0,
en orden de `outbox_id`, hasta `limit`, SIN los eventos en cuarentena (D-65). No reclama ni marca nada. `limit` debe ser un entero >= 0 (no bool): SQLite trata un LIMIT
        negativo como «sin tope» y rechaza un no entero con un `datatype mismatch` poco claro, así que se valida aquí (ValueError)."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError(f"limit: debe ser un entero >= 0, recibido {limit!r}")
        return [tuple(r) for r in self._conn.execute(
            "SELECT outbox_id, event_type, payload FROM outbox WHERE published = 0 "
            "AND outbox_id NOT IN (SELECT outbox_id FROM outbox_failures WHERE quarantined = 1) ORDER BY outbox_id ASC LIMIT ?", (limit,)).fetchall()]

    # -----------------------------------------------------------------
    # OUTBOX: retención del histórico publicado (D-70, parche 0018)
    # -----------------------------------------------------------------

    PURGE_MAX_LIMIT = 100_000

    def purge_published_outbox(self, *, older_than_days: float, limit: int = 1000) -> int:
        """D-70: borra del outbox eventos YA PUBLICADOS (`published = 1`) con más de `older_than_days` días, como máximo `limit` por llamada, los más
        antiguos primero. Devuelve cuántos borró; para vaciar un histórico grande se llama en bucle hasta que devuelva 0 (cada llamada es una
        transacción corta: no retiene el bloqueo de escritura más que `limit` filas). Nunca toca: un evento pendiente o en cuarentena, ni uno con
        historial en `outbox_requeues` (esa bitácora es append-only y debe seguir apuntando a un evento que existe). Borra también la fila de
        `outbox_failures` del evento purgado (fallos transitorios previos a su publicación). Los `outbox_id` no se reutilizan (AUTOINCREMENT).
        No se llama sola: la retención la decide quien opera el kernel. NO deja constancia en `audit_log`; el outbox es una cola de entrega, no el
        libro de hechos. `older_than_days` debe ser un número finito > 0 (no bool) y `limit` un entero entre 1 y `PURGE_MAX_LIMIT`."""
        if isinstance(older_than_days, bool) or not isinstance(older_than_days, (int, float)) or not (0 < older_than_days < 36500):
            raise ValueError(f"older_than_days: debe ser un número en (0, 36500) días, recibido {older_than_days!r}")
        if isinstance(limit, bool) or not isinstance(limit, int) or not (1 <= limit <= self.PURGE_MAX_LIMIT):
            raise ValueError(f"limit: debe ser un entero entre 1 y {self.PURGE_MAX_LIMIT}, recibido {limit!r}")
        modifier = f"-{older_than_days:.6f} days"  # nunca notación exponencial: SQLite no la acepta en el modificador
        with self.transaction() as conn:
            ids = [row[0] for row in conn.execute(
                "SELECT o.outbox_id FROM outbox o WHERE o.published = 1 AND o.recorded_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?) "
                "AND NOT EXISTS (SELECT 1 FROM outbox_requeues r WHERE r.outbox_id = o.outbox_id) ORDER BY o.outbox_id ASC LIMIT ?",
                (modifier, limit)).fetchall()]
            conn.executemany("DELETE FROM outbox_failures WHERE outbox_id = ?", [(i,) for i in ids])
            conn.executemany("DELETE FROM outbox WHERE outbox_id = ? AND published = 1", [(i,) for i in ids])
        return len(ids)

    # -----------------------------------------------------------------
    # BACKUP EN CALIENTE (D-72, parche 0020)
    # -----------------------------------------------------------------

    def backup_to(self, target_path: str | os.PathLike, *, verify_chain: bool = True) -> BackupResult:
        """D-72: copia de seguridad consistente de la base EN CALIENTE (con otras conexiones escribiendo) mediante `VACUUM INTO`, validada antes de
        oficializarla. Pasos: (1) comprueba argumentos, que esta conexión NO esté en una transacción (`VACUUM` no puede ejecutarse dentro de una:
        SQLite lanza «cannot VACUUM from within a transaction»; por eso no usa `transaction()` y corre en autocommit) y que el destino NO exista
        (`FileExistsError`; nunca sobrescribe); (2) escribe la copia en un archivo temporal OCULTO del MISMO directorio (`.<nombre>.<uuid>.tmp`; el
        directorio debe existir) con permisos 0600; (3) valida la copia: `PRAGMA integrity_check` y, con `verify_chain=True` (por defecto),
        `AuditChain.verify()` abriéndola con `SQLiteStore`; cualquier fallo lanza `RuntimeError` y no deja ni destino ni temporales; (4) `fsync`
        del archivo, vuelve a comprobar que el destino no apareció y lo mueve con `os.replace` (atómico en el mismo volumen); (5) `fsync` del
        directorio donde el sistema operativo lo permite (POSIX). Devuelve `BackupResult`.
        Qué garantiza: la copia es UNA instantánea coherente (una sola transacción de lectura), también si otro proceso escribe durante la copia;
        conserva `sqlite_sequence` (los `outbox_id` y `seq` purgados no se reutilizan) y los triggers; queda compactada. En WAL no bloquea a los
        escritores, pero mantiene una lectura abierta mientras dura: el checkpoint no puede avanzar y el `-wal` crece hasta que termina.
        `verify_chain=False` existe para poder respaldar una base cuya cadena YA está alterada (conservar la prueba): sigue exigiendo
        `integrity_check`, y el resultado lo declara (`chain_verified=False`).
        Qué NO hace: no decide cuándo ni dónde respaldar, no rota ni purga copias (no se llama sola); no protege la copia frente a quien escribe en
        el archivo (mismo límite de D-64); la validación cuesta O(n) en la cadena; la copia queda en modo WAL tras la validación (al abrirla con
        `SQLiteStore`); entre la última comprobación y `os.replace` queda una ventana mínima en la que otro proceso podría crear el destino y
        `os.replace` lo sobrescribiría (no hay creación exclusiva portable del destino)."""
        try:
            raw = os.fspath(target_path)
        except TypeError:
            raise TypeError(f"target_path: debe ser str o os.PathLike, recibido {type(target_path).__name__}") from None
        if isinstance(target_path, bool) or not isinstance(raw, str):
            raise TypeError(f"target_path: debe ser str o os.PathLike (texto), recibido {type(target_path).__name__}")
        if not raw.strip():
            raise ValueError("target_path: no puede estar vacío")
        if not isinstance(verify_chain, bool):
            raise ValueError(f"verify_chain: debe ser bool, recibido {verify_chain!r}")
        if self._conn.in_transaction:
            raise RuntimeError("backup_to: no puede ejecutarse dentro de una transacción (VACUUM INTO exige autocommit)")
        target = Path(raw).absolute()
        if not target.parent.is_dir():
            raise FileNotFoundError(f"backup_to: el directorio de destino no existe: {str(target.parent)!r}")
        if os.path.lexists(target):
            raise FileExistsError(f"backup_to: el destino ya existe y no se sobrescribe: {str(target)!r}")
        tmp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            self._conn.execute("VACUUM INTO ?", (str(tmp),))
            os.chmod(tmp, 0o600)
            entries = self._verify_backup_copy(tmp, verify_chain)
            size = tmp.stat().st_size
            self._fsync_path(tmp)
            if os.path.lexists(target):
                raise FileExistsError(f"backup_to: el destino apareció mientras se copiaba y no se sobrescribe: {str(target)!r}")
            os.replace(tmp, target)
            self._fsync_dir(target.parent)
        finally:
            self._discard_backup_temp(tmp)
        return BackupResult(path=str(target), size_bytes=size, audit_entries=entries, chain_verified=verify_chain)

    def _verify_backup_copy(self, copy_path: Path, verify_chain: bool) -> int:
        """Valida la copia temporal; devuelve el número de entradas de `audit_log`. RuntimeError si algo no cuadra."""
        problems = self._integrity_problems(copy_path)
        if problems:
            raise RuntimeError(f"backup_to: la copia no pasa PRAGMA integrity_check: {problems[:5]!r}")
        from .audit import AuditChain  # import tardío: audit.py importa este módulo
        copy = SQLiteStore(copy_path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            entries = copy.count_audit_entries()
            if verify_chain:
                verdict = AuditChain(copy).verify()
                if verdict.status != AuditIntegrityState.VALID:
                    raise RuntimeError(
                        f"backup_to: la cadena de auditoría de la copia no es VALID ({verdict.status.name}, first_bad_seq={verdict.first_bad_seq}): "
                        f"{verdict.detail}. Con verify_chain=False se puede respaldar igualmente una base ya alterada")
        finally:
            copy.close()
        wal = Path(str(copy_path) + "-wal")
        if wal.exists() and wal.stat().st_size > 0:
            raise RuntimeError("backup_to: la copia validada dejó páginas sin integrar en su -wal; se descarta")
        return entries

    @staticmethod
    def _integrity_problems(path: Path) -> list[str]:
        """`PRAGMA integrity_check` sobre un archivo: lista vacía si SQLite responde «ok»."""
        conn = sqlite3.connect(str(path))
        try:
            rows = [str(r[0]) for r in conn.execute("PRAGMA integrity_check").fetchall()]
        finally:
            conn.close()
        return [] if rows == ["ok"] else rows

    @staticmethod
    def _fsync_path(path: Path) -> None:
        fd = os.open(path, os.O_RDWR)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        if os.name != "posix":  # Windows no permite abrir un directorio para fsync
            return
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _discard_backup_temp(tmp: Path) -> None:
        """Borra el temporal y sus archivos auxiliares si quedaron (tras un `os.replace` correcto ya no existen). Limpieza best-effort: un fallo
        aquí no debe ocultar la excepción original."""
        for suffix in ("", "-wal", "-shm", "-journal"):
            try:
                os.remove(str(tmp) + suffix)
            except OSError:
                pass

    # -----------------------------------------------------------------
    # OUTBOX: cuarentena y lease del publicador (D-65, parche 0014)
    # -----------------------------------------------------------------

    def record_outbox_failure(self, outbox_id: int, error: str, *, max_attempts: int = 5, permanent: bool = False) -> bool:
        """Suma un intento fallido de publicación del evento. Pasa a CUARENTENA (deja de salir en `get_pending_outbox`; la fila de `outbox`
        no se modifica) si `permanent` (p. ej. payload que no es JSON: reintentar no lo arregla) o si llega a `max_attempts`. Devuelve True
        si el evento queda en cuarentena. Un evento ya en cuarentena sigue en cuarentena."""
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
            raise ValueError(f"max_attempts: debe ser un entero >= 1, recibido {max_attempts!r}")
        text = str(error)[:500]
        with self.transaction() as conn:
            row = conn.execute("SELECT attempts, quarantined FROM outbox_failures WHERE outbox_id = ?", (outbox_id,)).fetchone()
            attempts = (row[0] if row else 0) + 1
            quarantined = 1 if (permanent or attempts >= max_attempts or (row is not None and row[1] == 1)) else 0
            conn.execute(
                "INSERT INTO outbox_failures (outbox_id, attempts, last_error, quarantined) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(outbox_id) DO UPDATE SET attempts = excluded.attempts, last_error = excluded.last_error, "
                "quarantined = excluded.quarantined, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
                (outbox_id, attempts, text, quarantined))
        return bool(quarantined)

    def get_quarantined_outbox(self, limit: int = 100) -> list[tuple]:
        """Lectura pública (solo lectura) de la cuarentena: tuplas (outbox_id, event_type, attempts, last_error), en orden de `outbox_id`."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError(f"limit: debe ser un entero >= 0, recibido {limit!r}")
        return [tuple(r) for r in self._conn.execute(
            "SELECT o.outbox_id, o.event_type, f.attempts, f.last_error FROM outbox_failures f JOIN outbox o ON o.outbox_id = f.outbox_id "
            "WHERE f.quarantined = 1 AND o.published = 0 ORDER BY o.outbox_id ASC LIMIT ?", (limit,)).fetchall()]

    def requeue_quarantined(self, outbox_id: int, *, requeued_by: str, reason: str) -> bool:
        """D-66: devuelve a la cola un evento en cuarentena (p. ej. tras una caída larga del destino) y deja constancia en `outbox_requeues`.
        En UNA transacción: anota (outbox_id, requeued_by, reason, intentos y último error previos) y borra su fila de `outbox_failures`, de modo que
        el contador vuelve a cero y `get_pending_outbox` lo devuelve de nuevo. La fila de `outbox` no se toca. True si lo reencoló; False (sin
        efecto) si el evento no está en cuarentena, no existe o ya se publicó. Un payload que no es JSON volverá a cuarentena en la siguiente
        corrida (reintentar no lo arregla): reencolarlo no lo corrige. `requeued_by` y `reason` son obligatorios (texto no vacío; `reason` se
        recorta a 500 caracteres). No hay versión masiva a propósito: se itera sobre `get_quarantined_outbox()` y cada reencolado queda anotado."""
        if isinstance(outbox_id, bool) or not isinstance(outbox_id, int):
            raise ValueError(f"outbox_id: debe ser un entero, recibido {outbox_id!r}")
        for name, value in (("requeued_by", requeued_by), ("reason", reason)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name}: texto no vacío")
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT f.attempts, f.last_error FROM outbox_failures f JOIN outbox o ON o.outbox_id = f.outbox_id "
                "WHERE f.outbox_id = ? AND f.quarantined = 1 AND o.published = 0", (outbox_id,)).fetchone()
            if row is None:
                return False
            conn.execute(
                "INSERT INTO outbox_requeues (outbox_id, requeued_by, reason, previous_attempts, previous_error) VALUES (?, ?, ?, ?, ?)",
                (outbox_id, requeued_by.strip(), reason.strip()[:500], row[0], row[1]))
            conn.execute("DELETE FROM outbox_failures WHERE outbox_id = ?", (outbox_id,))
        return True

    def get_outbox_requeues(self, outbox_id: Optional[int] = None, limit: int = 100) -> list[tuple]:
        """Lectura pública (solo lectura) de la bitácora de reencolados (D-66): tuplas (requeue_id, outbox_id, requeued_by, reason,
        previous_attempts, previous_error, requeued_at) en orden de `requeue_id`; con `outbox_id`, solo las de ese evento."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError(f"limit: debe ser un entero >= 0, recibido {limit!r}")
        if outbox_id is not None and (isinstance(outbox_id, bool) or not isinstance(outbox_id, int)):
            raise ValueError(f"outbox_id: debe ser un entero o None, recibido {outbox_id!r}")
        sql = ("SELECT requeue_id, outbox_id, requeued_by, reason, previous_attempts, previous_error, requeued_at FROM outbox_requeues "
               + ("WHERE outbox_id = ? " if outbox_id is not None else "") + "ORDER BY requeue_id ASC LIMIT ?")
        params = ((outbox_id, limit) if outbox_id is not None else (limit,))
        return [tuple(r) for r in self._conn.execute(sql, params).fetchall()]

    def acquire_publisher_lease(self, holder: str, lease_s: float, now: Optional[float] = None) -> bool:
        """Toma el lease único del publicador del outbox si está libre, vencido o ya es de `holder`. Portable (solo SQLite) y válido entre
        procesos. False si otro lo tiene vigente. El lease caduca solo (`lease_s`), así que un proceso muerto no bloquea para siempre."""
        self._check_lease_args(holder, lease_s)
        now = time.time() if now is None else now
        with self.transaction() as conn:
            row = conn.execute("SELECT holder, expires_at FROM outbox_publisher_lease WHERE lease_name = 'outbox'").fetchone()
            if row is not None and row[0] != holder and row[1] > now:
                return False
            conn.execute("INSERT INTO outbox_publisher_lease (lease_name, holder, expires_at) VALUES ('outbox', ?, ?) "
                         "ON CONFLICT(lease_name) DO UPDATE SET holder = excluded.holder, expires_at = excluded.expires_at",
                         (holder, now + lease_s))
        return True

    def renew_publisher_lease(self, holder: str, lease_s: float, now: Optional[float] = None) -> bool:
        """Extiende el lease SOLO si `holder` aún lo posee (si venció y otro lo tomó, False: hay que dejar de publicar)."""
        self._check_lease_args(holder, lease_s)
        now = time.time() if now is None else now
        with self.transaction() as conn:
            n = conn.execute("UPDATE outbox_publisher_lease SET expires_at = ? WHERE lease_name = 'outbox' AND holder = ?",
                             (now + lease_s, holder)).rowcount
        return n == 1

    def release_publisher_lease(self, holder: str) -> bool:
        """Libera el lease si `holder` lo posee; ignora el de otro. True si lo liberó."""
        if not isinstance(holder, str) or not holder.strip():
            raise ValueError("holder: texto no vacío")
        with self.transaction() as conn:
            n = conn.execute("DELETE FROM outbox_publisher_lease WHERE lease_name = 'outbox' AND holder = ?", (holder,)).rowcount
        return n == 1

    @staticmethod
    def _check_lease_args(holder: str, lease_s: float) -> None:
        if not isinstance(holder, str) or not holder.strip():
            raise ValueError("holder: texto no vacío")
        if isinstance(lease_s, bool) or not isinstance(lease_s, (int, float)) or not (0 < lease_s < 86400):
            raise ValueError(f"lease_s: debe ser un número en (0, 86400) segundos, recibido {lease_s!r}")

    def get_effect_reconciliations(self, attempt_id: str) -> list[dict]:
        """Lectura pública (solo lectura) del historial append-only de reconciliaciones de un intento, en orden cronológico."""
        cur = self._conn.execute("SELECT * FROM effect_reconciliations WHERE attempt_id = ? ORDER BY reconciliation_id", (attempt_id,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_effect_attempt(self, attempt_id: str) -> Optional[dict]:
        cur = self._conn.execute(
            "SELECT attempt_id, intent_id, provider, idempotency_key, status, version "
            "FROM effect_attempts WHERE attempt_id = ?",
            (attempt_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = ["attempt_id", "intent_id", "provider", "idempotency_key", "status", "version"]
        return dict(zip(cols, row))

    # -----------------------------------------------------------------
    # RECOVERY TRIGGERS — estado mutable en curso (Commit Gate).
    # -----------------------------------------------------------------

    def create_recovery_trigger(
        self, trigger_id: str, suspected_node_id: str, incarnation_at_suspicion: int
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO recovery_triggers
                   (trigger_id, suspected_node_id, incarnation_at_suspicion,
                    evidence, declared_by, state, version)
                   VALUES (?, ?, ?, '{}', NULL, 'NONE', 1)""",
                (trigger_id, suspected_node_id, incarnation_at_suspicion),
            )

    def commit_recovery_trigger_transition(
        self,
        trigger_id: str,
        new_state: str,
        expected_version: int,
        declared_by: Optional[str] = None,
        evidence: Optional[dict] = None,
    ) -> CommitResult:
        with self.transaction() as conn:
            if declared_by is not None or evidence is not None:
                cur = conn.execute(
                    """UPDATE recovery_triggers
                       SET state = ?, declared_by = COALESCE(?, declared_by),
                           evidence = COALESCE(?, evidence), version = version + 1
                       WHERE trigger_id = ? AND version = ?""",
                    (
                        new_state,
                        declared_by,
                        json.dumps(evidence) if evidence is not None else None,
                        trigger_id,
                        expected_version,
                    ),
                )
            else:
                cur = conn.execute(
                    """UPDATE recovery_triggers SET state = ?, version = version + 1
                       WHERE trigger_id = ? AND version = ?""",
                    (new_state, trigger_id, expected_version),
                )
            if cur.rowcount != 1:
                raise ConflictError(
                    f"commit_recovery_trigger_transition: expected_version="
                    f"{expected_version} no coincide (o trigger_id no existe) para "
                    f"{trigger_id}. rows_affected={cur.rowcount}."
                )
            return CommitResult(rows_affected=cur.rowcount, new_version=expected_version + 1)

    def get_recovery_trigger(self, trigger_id: str) -> Optional[dict]:
        cur = self._conn.execute(
            "SELECT trigger_id, suspected_node_id, incarnation_at_suspicion, "
            "evidence, declared_by, state, version FROM recovery_triggers WHERE trigger_id = ?",
            (trigger_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = ["trigger_id", "suspected_node_id", "incarnation_at_suspicion", "evidence", "declared_by", "state", "version"]
        return dict(zip(cols, row))
