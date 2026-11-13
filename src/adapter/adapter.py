"""Durable protocol boundary for UNAE 4.3S <-> DTOS.

This integration slice does not mutate either upstream package. It validates protocol
envelopes (contract v1.1, schema rev. 1.1.1), records inbox delivery identity, and stores
durable operation/intent/attempt correlation. External effects must be executed by the
caller only after an explicit authorization path; this module never retries effects.

Revision notes (rev. 0.2):
- Identifiers (message_id, operation_id, effect_intent_id, effect_attempt_id) are OPAQUE, case-sensitive text
  (1-128 chars, no edge whitespace, no control chars, NFC), exactly as UNAE and DTOS treat them. They are not
  required to be UUIDs: neither Store generates them (callers do).
- `payload_sha256` is defined as SHA-256 of the canonical JSON of the message WITHOUT the
  `payload_sha256` field; a declared value that does not match is quarantined.
- Envelope validation now enforces the schema: no unknown fields, `result` enum,
  unique `evidence_refs`, non-empty `evidence_refs` for EFFECT_RESULT /
  RECONCILIATION_RESULT / RECOVERY_NOTICE, RFC 3339 `created_at` with offset,
  `operation_id` optional in RECOVERY_NOTICE.
- Connections are closed after every transaction.
"""
from __future__ import annotations

import hashlib
import json
import math
import random
import re
import sqlite3
import time
import unicodedata
from contextlib import closing, contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator

MAX_SQLITE_INTEGER = (1 << 63) - 1
# Tope del fencing_token en el protocolo: mayor entero que un double (JavaScript, JCS/RFC 8785) representa exactamente. UNAE admite hasta 2^63-1,
# pero un token se incrementa de uno en uno y llegar a 2^53 es inalcanzable en la práctica: el tope no cuesta nada y evita la pérdida de precisión (D-41).
MAX_FENCING_TOKEN = (1 << 53) - 1
# Tope de la evidencia de una reconciliación (JSON canónico, en BYTES UTF-8). Debe coincidir con `dtos.store.MAX_EVIDENCE_SIZE_BYTES` (hay una prueba que lo comprueba).
# 65536 es el valor propuesto por el usuario, no una medición (D-47).
MAX_EVIDENCE_SIZE_BYTES = 65536
MESSAGE_TYPES = {
    "OPERATION_REQUEST", "EFFECT_DISPATCHED", "EFFECT_RESULT",
    "RECONCILIATION_RESULT", "RECOVERY_NOTICE",
}
RESULT_VALUES = frozenset({
    "REQUESTED", "DISPATCHED", "CONFIRMED", "REJECTED", "PARTIAL",
    "UNKNOWN", "SUCCEEDED", "FAILED", "NOT_FOUND", "RECOVERY_REQUIRED",
})
ENVELOPE_FIELDS = frozenset({
    "contract_version", "schema_version", "message_id", "message_type", "correlation_id",
    "cognitive_cycle_id", "operation_id", "effect_intent_id", "effect_attempt_id",
    "idempotency_key", "authority_ref", "fencing_token", "result", "evidence_refs",
    "created_at", "payload_sha256",
})
BASE_REQUIRED = frozenset({
    "contract_version", "schema_version", "message_id", "message_type",
    "correlation_id", "evidence_refs", "created_at",
})
PER_TYPE_REQUIRED = {
    "OPERATION_REQUEST": ("operation_id",),
    "EFFECT_DISPATCHED": ("operation_id", "effect_intent_id", "effect_attempt_id", "idempotency_key", "fencing_token"),
    "EFFECT_RESULT": ("operation_id", "effect_intent_id", "effect_attempt_id", "idempotency_key", "result", "evidence_refs"),
    "RECONCILIATION_RESULT": ("operation_id", "effect_attempt_id", "result", "evidence_refs"),
    "RECOVERY_NOTICE": ("result", "evidence_refs"),
}
EVIDENCE_NONEMPTY = frozenset({"EFFECT_RESULT", "RECONCILIATION_RESULT", "RECOVERY_NOTICE"})
ID_FIELDS = ("message_id", "operation_id", "effect_intent_id", "effect_attempt_id")
TEXT_FIELDS = ("correlation_id", "cognitive_cycle_id", "idempotency_key", "authority_ref")

MAX_ID_LENGTH = 128
# A-2 (g0.8 r10, D-60): cotas de la frontera de entrada. Valores propuestos y ratificados por el usuario, no mediciones. Todo se mide en BYTES UTF-8.
MAX_ENVELOPE_BYTES = 131072        # 128 KiB de JSON canónico por mensaje (2 x MAX_EVIDENCE_SIZE_BYTES: la evidencia más margen para el resto de campos)
MAX_CORRELATION_ID_BYTES = 256
MAX_TEXT_FIELD_BYTES = MAX_CORRELATION_ID_BYTES  # D-62: el mismo tope para los demás campos de texto (idempotency_key, authority_ref, cognitive_cycle_id)
MAX_EVIDENCE_REFS = 64
MAX_EVIDENCE_REF_BYTES = 1024
_SHA256_RE = re.compile(r"[a-fA-F0-9]{64}")
_RFC3339_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})")


class ProtocolError(ValueError):
    pass


class MessageConflict(ProtocolError):
    """Same message_id was redelivered with different immutable content."""


class PayloadIntegrityError(ProtocolError):
    """Declared payload_sha256 does not match the canonical payload."""

    def __init__(self, message_id: str, computed: str, declared: str):
        super().__init__(f"payload_sha256 declarado no coincide con el contenido canónico (message_id {message_id})")
        self.message_id, self.computed, self.declared = message_id, computed, declared


@dataclass(frozen=True)
class ReceiveResult:
    status: str  # ACCEPTED | DUPLICATE
    message_id: str
    payload_sha256: str


class LocalResult:
    """Resultado de una sentencia ejecutada por un `processor` (D-63). Envuelve el cursor para NO exponer `cursor.connection`, que daría
    acceso a la conexión real (y con ella a `commit`, `rollback` y `close`)."""
    __slots__ = ("_cur",)

    def __init__(self, cur: sqlite3.Cursor):
        self._cur = cur

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount

    @property
    def lastrowid(self) -> int | None:
        return self._cur.lastrowid

    def fetchone(self) -> Any:
        return self._cur.fetchone()

    def fetchall(self) -> list[Any]:
        return self._cur.fetchall()

    def __iter__(self) -> Iterator[Any]:
        return iter(self._cur)


class LocalTx:
    """Vista restringida de la transacción local del adaptador que recibe un `processor` en `UNAEDTOSAdapter.receive` (D-63).

    Antes el processor recibía el `sqlite3.Connection` crudo: podía hacer `commit()` (dejando la fila del inbox confirmada aunque después
    fallara), `rollback()`, `close()` o abrir otra transacción, y con eso romper la atomicidad «inbox + efecto local» que `receive` promete.
    Ahora solo ofrece `execute` y `executemany`, y mientras el processor corre un autorizador de SQLite deniega el control de transacciones
    (BEGIN/COMMIT/ROLLBACK/SAVEPOINT/RELEASE), ATTACH/DETACH y PRAGMA, incluso escritos con comentarios o mayúsculas.
    Límite: es una protección contra errores, no un sandbox frente a código hostil (Python no puede ocultar `_conn`)."""
    __slots__ = ("_conn",)

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def execute(self, sql: str, params: Any = ()) -> LocalResult:
        return LocalResult(self._conn.execute(sql, params))

    def executemany(self, sql: str, seq_of_params: Any) -> LocalResult:
        return LocalResult(self._conn.executemany(sql, seq_of_params))


_PROCESSOR_DENIED_ACTIONS = frozenset({sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT, sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH,
                                       sqlite3.SQLITE_PRAGMA})


def _processor_authorizer(action: int, *_ignored: Any) -> int:
    return sqlite3.SQLITE_DENY if action in _PROCESSOR_DENIED_ACTIONS else sqlite3.SQLITE_OK


def _require_str_keys(value: Any) -> None:
    """D-68: `json.dumps` convierte en silencio las claves no-str (1 -> "1", True -> "true"), de modo que {1: "a"} y {"1": "a"} daban el mismo
    texto y el mismo hash. Se rechazan (ValueError) en vez de colapsarlas. Recorrido iterativo: no añade profundidad de recursión."""
    stack = [value]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            for k, v in o.items():
                if not isinstance(k, str):
                    raise ValueError(f"canonical_json: las claves de objeto deben ser str (llegó {type(k).__name__}: {k!r})")
                stack.append(v)
        elif isinstance(o, (list, tuple)):
            stack.extend(o)


def canonical_json(value: Any) -> str:
    _require_str_keys(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def payload_digest(message: dict[str, Any]) -> str:
    """SHA-256 of the canonical message without the `payload_sha256` field itself."""
    return sha256_json({k: v for k, v in message.items() if k != "payload_sha256"})


def validate_fencing_token(value: Any) -> int:
    # bool is a subclass of int in Python; reject it explicitly.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolError("fencing_token debe ser un entero JSON, no booleano ni string")
    if value < 0:
        raise ProtocolError("fencing_token no puede ser negativo")
    if value > MAX_FENCING_TOKEN:
        raise ProtocolError("fencing_token excede el máximo entero seguro (2^53-1): se perdería precisión en consumidores JavaScript/JCS")
    return value


def _utf8_len(value: str, field: str) -> int:
    """Longitud en bytes UTF-8; un texto que no se codifica (surrogate aislado) es un error de protocolo, no un `UnicodeEncodeError`."""
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ProtocolError(f"{field} no es texto UTF-8 válido (surrogate o carácter no codificable)") from exc


def _ident(value: Any, field: str) -> None:
    """Identificador OPACO y sensible a mayúsculas, como lo tratan UNAE y DTOS (TEXT sin formato impuesto).

    Se compara siempre por igualdad exacta: "op-1" y "OP-1" son identidades distintas en los tres almacenes. Se exige:
    texto de 1 a 128 caracteres, sin espacios al inicio/final, sin caracteres de control y en forma NFC."""
    if isinstance(value, str) and len(value) <= MAX_ID_LENGTH:
        _utf8_len(value, field)
    if (not isinstance(value, str) or not 1 <= len(value) <= MAX_ID_LENGTH or value != value.strip()
            or any(ord(c) < 32 or ord(c) == 127 for c in value) or not unicodedata.is_normalized("NFC", value)):
        raise ProtocolError(f"{field} debe ser un identificador de 1 a {MAX_ID_LENGTH} caracteres, sin espacios en los extremos, "
                            "sin caracteres de control y en forma NFC")


def _text(value: Any, field: str) -> None:
    """Campo de texto del sobre (correlation_id, cognitive_cycle_id, idempotency_key, authority_ref): no vacío, UTF-8 válido, como máximo
    MAX_TEXT_FIELD_BYTES bytes y sin caracteres de control (D-60, D-62). `idempotency_key` no admite además espacios en los extremos (L-2)."""
    if not isinstance(value, str) or not value.strip():
        raise ProtocolError(f"{field} debe ser texto no vacío")
    if len(value) > MAX_TEXT_FIELD_BYTES:  # los bytes UTF-8 nunca son menos que los caracteres: se rechaza sin codificar
        raise ProtocolError(f"{field} excede {MAX_TEXT_FIELD_BYTES} bytes UTF-8")
    if _utf8_len(value, field) > MAX_TEXT_FIELD_BYTES:
        raise ProtocolError(f"{field} excede {MAX_TEXT_FIELD_BYTES} bytes UTF-8")
    if any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ProtocolError(f"{field} no admite caracteres de control")
    if field == "idempotency_key" and value != value.strip():
        raise ProtocolError(f"{field} no admite espacios al inicio ni al final")


def validate_envelope(message: dict[str, Any]) -> str:
    """Validate the v1.1 envelope; return the payload digest (see `payload_digest`)."""
    if not isinstance(message, dict):
        raise ProtocolError("el mensaje debe ser un objeto JSON")
    unknown = set(message) - ENVELOPE_FIELDS
    if unknown:
        raise ProtocolError(f"campos no permitidos: {', '.join(sorted(map(str, unknown)))}")
    missing = BASE_REQUIRED - message.keys()
    if missing:
        raise ProtocolError(f"faltan campos obligatorios: {', '.join(sorted(missing))}")
    if message["contract_version"] != "1.1" or message["schema_version"] != "1":
        raise ProtocolError("versión de contrato/esquema no soportada")
    _ident(message["message_id"], "message_id")
    kind = message["message_type"]
    if not isinstance(kind, str) or kind not in MESSAGE_TYPES:
        raise ProtocolError("message_type no reconocido")
    for field in TEXT_FIELDS:
        if field == "correlation_id" or message.get(field) is not None:
            _text(message[field], field)
    for field in ID_FIELDS[1:]:
        if message.get(field) is not None:
            _ident(message[field], field)
    if message.get("fencing_token") is not None:
        validate_fencing_token(message["fencing_token"])
    result = message.get("result")
    if result is not None and (not isinstance(result, str) or result not in RESULT_VALUES):
        raise ProtocolError("result fuera del conjunto permitido")
    refs = message["evidence_refs"]
    if not isinstance(refs, list):
        raise ProtocolError("evidence_refs debe ser una lista de textos no vacíos")
    if len(refs) > MAX_EVIDENCE_REFS:  # antes de recorrer la lista
        raise ProtocolError(f"evidence_refs admite como máximo {MAX_EVIDENCE_REFS} referencias")
    for x in refs:
        if not isinstance(x, str) or not x:
            raise ProtocolError("evidence_refs debe ser una lista de textos no vacíos")
        if len(x) > MAX_EVIDENCE_REF_BYTES or _utf8_len(x, "evidence_refs") > MAX_EVIDENCE_REF_BYTES:
            raise ProtocolError(f"cada referencia de evidence_refs admite como máximo {MAX_EVIDENCE_REF_BYTES} bytes UTF-8")
        if any(ord(c) < 32 or ord(c) == 127 for c in x):
            raise ProtocolError("evidence_refs no admite caracteres de control")
    if len(set(refs)) != len(refs):
        raise ProtocolError("evidence_refs no admite elementos repetidos")
    created = message["created_at"]
    if not isinstance(created, str) or _RFC3339_RE.fullmatch(created) is None:
        raise ProtocolError("created_at debe ser fecha/hora RFC 3339 con zona (Z o ±hh:mm)")
    try:
        datetime.fromisoformat(created.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProtocolError("created_at no es una fecha/hora válida") from exc
    declared = message.get("payload_sha256")
    if declared is not None and (not isinstance(declared, str) or _SHA256_RE.fullmatch(declared) is None):
        raise ProtocolError("payload_sha256 debe ser SHA-256 hexadecimal de 64 caracteres")
    for field in PER_TYPE_REQUIRED[kind]:
        if field not in message or message[field] is None or (isinstance(message[field], str) and not message[field].strip()):
            raise ProtocolError(f"{kind} requiere {field}")
    if kind in EVIDENCE_NONEMPTY and not refs:
        raise ProtocolError(f"{kind} requiere al menos una referencia de evidencia")
    try:
        envelope_size = len(canonical_json(message).encode("utf-8"))
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ProtocolError("el mensaje no se puede serializar como JSON canónico UTF-8") from exc
    if envelope_size > MAX_ENVELOPE_BYTES:
        raise ProtocolError(f"el mensaje ocupa {envelope_size} bytes y el máximo es {MAX_ENVELOPE_BYTES}")
    digest = payload_digest(message)
    if declared is not None and declared.lower() != digest:
        raise PayloadIntegrityError(message["message_id"], digest, declared)
    return digest


# D-74 (parche 0022 / g0.8 r29): política de conexión parametrizable, MISMA que DTOS (D-57, D-73) pero con código propio (D-63: los almacenes no comparten código).
# Son valores de política (no salen de mediciones). `journal_size_limit` solo recorta el `-wal` tras un checkpoint completo, cuando la siguiente escritura
# reinicia el WAL; NO acota su crecimiento mientras una lectura abierta frena el checkpoint, y es por conexión (lo aplica la conexión que escribe).
DEFAULT_BUSY_TIMEOUT_MS = 10_000
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


# D-76 (parche 0024 / g0.8 r31): reintento acotado de la APERTURA, MISMA estrategia que DTOS (D-67) pero con código propio (D-63). SQLite puede devolver «database is locked» SIN esperar el
# busy_timeout cuando esperar podría causar un bloqueo mutuo (lo documenta SQLite); con varios procesos abriendo a la vez una base nueva (cambio a WAL + esquema) uno puede fallar.
# Se repite TODA la apertura (es idempotente: IF NOT EXISTS y migraciones atómicas) con pausas crecientes y jitter, hasta agotar `busy_timeout_ms` contado desde el inicio.
# Con `busy_timeout_ms=0` no se reintenta. Valores de política, no salen de mediciones.
OPEN_RETRY_FIRST_S = 0.01
OPEN_RETRY_MAX_S = 0.25


def _is_lock_error(exc: BaseException) -> bool:
    """True si `exc` es un bloqueo temporal de SQLite (SQLITE_BUSY o SQLITE_LOCKED, con o sin código extendido). Sin código (Python < 3.11 o errores
    construidos a mano) se decide por el texto."""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int):
        return (code & 0xFF) in (5, 6)
    return "locked" in str(exc).lower()


class UNAEDTOSAdapter:
    """Protocol inbox + append-only correlation registry in a dedicated SQLite DB."""

    def __init__(self, db_path: str | Path, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
                 journal_size_limit_bytes: int = DEFAULT_JOURNAL_SIZE_LIMIT_BYTES):
        busy_timeout_ms, journal_size_limit_bytes = _validate_connection_policy(busy_timeout_ms, journal_size_limit_bytes)  # D-74: antes de crear nada; D-75: normalizados
        self.busy_timeout_ms = busy_timeout_ms
        self.journal_size_limit_bytes = journal_size_limit_bytes
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._open_with_retry()

    def _open_with_retry(self) -> None:
        """D-76: ejecuta `_open_once` y, ante un bloqueo temporal, repite TODO el paso con pausas crecientes y jitter hasta agotar `busy_timeout_ms` desde el
        inicio; pasado el plazo relanza el error original. Los errores que no son de bloqueo (y el RuntimeError de WAL) no se reintentan. Cada intento abre
        y cierra sus propias conexiones, así que un fallo a medias no deja transacciones abiertas."""
        deadline = time.monotonic() + self.busy_timeout_ms / 1000
        delay = OPEN_RETRY_FIRST_S
        while True:
            try:
                self._open_once()
                return
            except sqlite3.OperationalError as exc:
                if not _is_lock_error(exc):
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(delay * (0.5 + random.random() / 2), remaining))
                delay = min(delay * 2, OPEN_RETRY_MAX_S)

    def _open_once(self) -> None:
        """Un intento de apertura: WAL, verificación de la política, esquema y migración de columnas (idempotente)."""
        with closing(self._connect()) as conn:
            mode = str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
            if mode != "wal":  # D-70: igual que DTOS (D-57) y UNAE; antes se degradaba en silencio a otro modo
                raise RuntimeError(f"journal_mode: se pidió WAL y SQLite devolvió {mode!r} para {self.db_path!r} (¿sistema de archivos de red, sin "
                                   f"soporte de memoria compartida o base en memoria?). El adaptador exige WAL.")
            effective = conn.execute("PRAGMA busy_timeout").fetchone()[0]
            if effective != self.busy_timeout_ms:
                raise RuntimeError(f"busy_timeout: pedido {self.busy_timeout_ms} ms y SQLite quedó en {effective} ms")
            limit = conn.execute("PRAGMA journal_size_limit").fetchone()[0]
            if limit != self.journal_size_limit_bytes:
                raise RuntimeError(f"journal_size_limit: pedido {self.journal_size_limit_bytes} bytes y SQLite quedó en {limit}")
            conn.executescript("""
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS adapter_inbox (
              message_id TEXT PRIMARY KEY,
              message_type TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              payload_sha256 TEXT NOT NULL,
              status TEXT NOT NULL CHECK(status IN ('ACCEPTED','QUARANTINED')),
              received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              compacted_at TEXT
            );
            CREATE TABLE IF NOT EXISTS operation_correlation (
              operation_id TEXT PRIMARY KEY,
              effect_intent_id TEXT NOT NULL UNIQUE,
              effect_attempt_id TEXT NOT NULL UNIQUE,
              provider TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              fencing_token INTEGER NOT NULL CHECK(fencing_token >= 0),
              intent_payload_sha256 TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TABLE IF NOT EXISTS adapter_quarantine (
              quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
              message_id TEXT NOT NULL,
              incoming_sha256 TEXT NOT NULL,
              stored_sha256 TEXT,
              reason TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TABLE IF NOT EXISTS reconciliation_route_log (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              operation_id TEXT NOT NULL,
              message_id TEXT NOT NULL,
              route TEXT NOT NULL CHECK(route IN ('A','B','N')),
              step TEXT NOT NULL CHECK(step IN ('BEGIN_RECONCILIATION','RECORD_RECEIPT')),
              status_before TEXT NOT NULL,
              outcome TEXT NOT NULL,
              fencing_token INTEGER,
              created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TRIGGER IF NOT EXISTS route_log_no_update BEFORE UPDATE ON reconciliation_route_log
              BEGIN SELECT RAISE(ABORT, 'reconciliation_route_log es append-only'); END;
            CREATE TRIGGER IF NOT EXISTS route_log_no_delete BEFORE DELETE ON reconciliation_route_log
              BEGIN SELECT RAISE(ABORT, 'reconciliation_route_log es append-only'); END;
            CREATE TABLE IF NOT EXISTS reconciliation_log (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              operation_id TEXT NOT NULL,
              effect_attempt_id TEXT NOT NULL,
              message_id TEXT NOT NULL,
              strategy TEXT,
              reported_result TEXT NOT NULL,
              outcome TEXT NOT NULL,
              detail TEXT,
              evidence_json TEXT,
              reconciler_id TEXT,
              created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TRIGGER IF NOT EXISTS reconciliation_log_no_update BEFORE UPDATE ON reconciliation_log
              BEGIN SELECT RAISE(ABORT, 'reconciliation_log es append-only'); END;
            CREATE TRIGGER IF NOT EXISTS reconciliation_log_no_delete BEFORE DELETE ON reconciliation_log
              BEGIN SELECT RAISE(ABORT, 'reconciliation_log es append-only'); END;
            CREATE TABLE IF NOT EXISTS quarantine_review (
              review_id INTEGER PRIMARY KEY AUTOINCREMENT,
              quarantine_id INTEGER NOT NULL UNIQUE,
              decision TEXT NOT NULL CHECK(decision IN ('DISMISSED','REPROCESSED')),
              reviewer_id TEXT NOT NULL,
              note TEXT,
              reviewed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TRIGGER IF NOT EXISTS quarantine_review_no_update BEFORE UPDATE ON quarantine_review
              BEGIN SELECT RAISE(ABORT, 'quarantine_review es append-only'); END;
            CREATE TRIGGER IF NOT EXISTS quarantine_review_no_delete BEFORE DELETE ON quarantine_review
              BEGIN SELECT RAISE(ABORT, 'quarantine_review es append-only'); END;
            CREATE TABLE IF NOT EXISTS quarantine_purge_log (
              purge_id INTEGER PRIMARY KEY AUTOINCREMENT,
              quarantine_id INTEGER NOT NULL,
              message_id TEXT NOT NULL,
              reason TEXT NOT NULL,
              incoming_sha256 TEXT NOT NULL,
              stored_sha256 TEXT,
              decision TEXT NOT NULL,
              reviewer_id TEXT NOT NULL,
              reviewed_at TEXT NOT NULL,
              purged_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE TRIGGER IF NOT EXISTS quarantine_purge_no_update BEFORE UPDATE ON quarantine_purge_log
              BEGIN SELECT RAISE(ABORT, 'quarantine_purge_log es append-only'); END;
            CREATE TRIGGER IF NOT EXISTS quarantine_purge_no_delete BEFORE DELETE ON quarantine_purge_log
              BEGIN SELECT RAISE(ABORT, 'quarantine_purge_log es append-only'); END;
            CREATE TRIGGER IF NOT EXISTS correlation_no_update BEFORE UPDATE ON operation_correlation
              BEGIN SELECT RAISE(ABORT, 'operation_correlation es append-only'); END;
            CREATE TRIGGER IF NOT EXISTS correlation_no_delete BEFORE DELETE ON operation_correlation
              BEGIN SELECT RAISE(ABORT, 'operation_correlation es append-only'); END;
            """)

        with closing(self._connect()) as conn:  # migración de bases creadas con revisiones anteriores de g0.4
            for table, column in (("reconciliation_log", "evidence_json"), ("reconciliation_log", "reconciler_id"), ("adapter_inbox", "compacted_at")):
                self._add_column_if_missing(conn, table, column)

    @staticmethod
    def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str) -> None:
        """D-77: migración de columnas segura entre procesos (mismo patrón que `SQLiteStore._add_column_if_missing`, D-57). `PRAGMA table_info` + `ALTER TABLE` no son
        atómicos: dos procesos que abrían a la vez una base antigua veían los dos «falta la columna» y el segundo ALTER fallaba con «duplicate column name», que NO
        es un bloqueo y el reintento de la apertura (D-76) no repite. Se comprueba sin bloqueo (camino habitual: no hay nada que migrar) y, si falta, se vuelve a
        comprobar DENTRO de BEGIN IMMEDIATE antes del ALTER. Los argumentos son constantes del propio módulo (los identificadores no admiten parámetros)."""
        def has_column() -> bool:
            return column in {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}

        if has_column():
            return
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not has_column():
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT")
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                with suppress(sqlite3.Error):
                    conn.execute("ROLLBACK")
            raise

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None: autocommit; transactions are explicit (see _tx).
        conn = sqlite3.connect(self.db_path, timeout=self.busy_timeout_ms / 1000, isolation_level=None)  # el PRAGMA busy_timeout posterior fija el valor exacto (D-77)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")  # D-77: explícito, como en los otros tres almacenes; `timeout=` (float en segundos) trunca y dejaba p. ej. 1001 ms en 1000 ms
        conn.execute(f"PRAGMA journal_size_limit={self.journal_size_limit_bytes}")  # D-74: int validado en __init__
        return conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """BEGIN IMMEDIATE ... COMMIT; ROLLBACK on any exception; connection always closed."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                with suppress(sqlite3.Error):
                    conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")
        finally:
            conn.close()

    def _quarantine(self, message_id: str, incoming: str, stored: str | None, reason: str, message: dict[str, Any]) -> None:
        with self._tx() as conn:
            conn.execute("INSERT INTO adapter_quarantine(message_id,incoming_sha256,stored_sha256,reason,payload_json) VALUES(?,?,?,?,?)",
                         (message_id, incoming, stored, reason, canonical_json(message)))

    def receive(self, message: dict[str, Any], processor: Callable[[LocalTx, dict[str, Any]], None] | None = None) -> ReceiveResult:
        try:
            digest = validate_envelope(message)
        except PayloadIntegrityError as exc:
            self._quarantine(exc.message_id, exc.computed, exc.declared, "PAYLOAD_SHA256_MISMATCH", message)
            raise
        message_id = message["message_id"]
        body = canonical_json(message)
        stored_digest = None
        with self._tx() as conn:
            row = conn.execute("SELECT payload_sha256 FROM adapter_inbox WHERE message_id=?", (message_id,)).fetchone()
            if row:
                if row["payload_sha256"] == digest:
                    return ReceiveResult("DUPLICATE", message_id, digest)
                stored_digest = row["payload_sha256"]
                conn.execute("INSERT INTO adapter_quarantine(message_id,incoming_sha256,stored_sha256,reason,payload_json) VALUES(?,?,?,?,?)",
                             (message_id, digest, stored_digest, "MESSAGE_ID_PAYLOAD_CONFLICT", body))
            else:
                conn.execute("INSERT INTO adapter_inbox(message_id,message_type,payload_json,payload_sha256,status) VALUES(?,?,?,?, 'ACCEPTED')",
                             (message_id, message["message_type"], body, digest))
                if processor is not None:
                    # Processor is intentionally restricted to local writes on this adapter DB connection (LocalTx: sin commit/rollback/close
                    # y con el control de transacciones denegado por el autorizador de SQLite, D-63).
                    # An exception here rolls back the inbox row too (atomic with the local effect).
                    conn.set_authorizer(_processor_authorizer)
                    try:
                        processor(LocalTx(conn), message)
                    finally:
                        conn.set_authorizer(None)
        if stored_digest is not None:
            raise MessageConflict(f"message_id {message_id} reutilizado con contenido diferente; mensaje puesto en cuarentena")
        return ReceiveResult("ACCEPTED", message_id, digest)

    def bind_correlation(self, *, operation_id: str, effect_intent_id: str, effect_attempt_id: str,
                         provider: str, idempotency_key: str, fencing_token: int, intent_payload: dict[str, Any]) -> None:
        for value, field in ((operation_id, "operation_id"), (effect_intent_id, "effect_intent_id"), (effect_attempt_id, "effect_attempt_id")):
            _ident(value, field)
        if not isinstance(provider, str) or not provider.strip():
            raise ProtocolError("provider debe ser texto no vacío")
        if not isinstance(idempotency_key, str) or len(idempotency_key) < 16:
            raise ProtocolError("idempotency_key debe tener al menos 16 caracteres")
        _text(idempotency_key, "idempotency_key")  # L-2, D-62: mismas reglas que en el sobre; si no, el despacho rechazaría después lo que aquí se aceptó
        token = validate_fencing_token(fencing_token)
        payload_hash = sha256_json(intent_payload)
        with self._tx() as conn:
            existing = conn.execute("SELECT * FROM operation_correlation WHERE operation_id=? OR effect_intent_id=? OR effect_attempt_id=?",
                                    (operation_id, effect_intent_id, effect_attempt_id)).fetchone()
            if existing:
                expected = (operation_id, effect_intent_id, effect_attempt_id, provider, idempotency_key, token, payload_hash)
                actual = tuple(existing[k] for k in ("operation_id", "effect_intent_id", "effect_attempt_id", "provider", "idempotency_key", "fencing_token", "intent_payload_sha256"))
                if actual == expected:
                    return
                raise MessageConflict("correlación existente incompatible; no se sobrescribe el registro durable")
            conn.execute("INSERT INTO operation_correlation(operation_id,effect_intent_id,effect_attempt_id,provider,idempotency_key,fencing_token,intent_payload_sha256) VALUES(?,?,?,?,?,?,?)",
                         (operation_id, effect_intent_id, effect_attempt_id, provider, idempotency_key, token, payload_hash))

    def get_correlation(self, operation_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM operation_correlation WHERE operation_id=?", (operation_id,)).fetchone()
            return dict(row) if row else None

    def quarantine_count(self) -> int:
        with closing(self._connect()) as conn:
            return int(conn.execute("SELECT COUNT(*) FROM adapter_quarantine").fetchone()[0])

    def quarantine(self, message: dict[str, Any], reason: str) -> None:
        """Quarantine an already validated message (e.g. identity verification failed)."""
        self._quarantine(message["message_id"], payload_digest(message), None, reason, message)

    def log_route(self, *, operation_id: str, message_id: str, route: str, step: str, status_before: str,
                  outcome: str, fencing_token: int | None = None) -> None:
        """Append-only record of which UNKNOWN-resolution route was used (contract C-03: A, B; N = normal receipt)."""
        with self._tx() as conn:
            conn.execute("INSERT INTO reconciliation_route_log(operation_id,message_id,route,step,status_before,outcome,fencing_token) VALUES(?,?,?,?,?,?,?)",
                         (operation_id, message_id, route, step, status_before, outcome, fencing_token))

    def unfinished_routes(self) -> list[dict[str, Any]]:
        """M-4: filas PENDING de la bitácora de rutas sin cierre posterior para el mismo (operación, mensaje, paso): una llamada a UNAE que pudo
        quedar a medias por una caída. La fuente de verdad sigue siendo el estado de UNAE; esto solo señala qué pasos hay que cerrar."""
        with closing(self._connect()) as conn:
            rows = conn.execute("""SELECT * FROM reconciliation_route_log r WHERE outcome='PENDING' AND NOT EXISTS (
                                     SELECT 1 FROM reconciliation_route_log c WHERE c.operation_id=r.operation_id AND c.message_id=r.message_id
                                     AND c.step=r.step AND c.seq>r.seq AND c.outcome<>'PENDING') ORDER BY seq""").fetchall()
            return [dict(r) for r in rows]

    def route_log(self, operation_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM reconciliation_route_log WHERE operation_id=? ORDER BY seq", (operation_id,)).fetchall()
            return [dict(r) for r in rows]

    def list_correlations(self) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM operation_correlation ORDER BY created_at, operation_id").fetchall()]

    def log_reconciliation(self, *, operation_id: str, effect_attempt_id: str, message_id: str, strategy: str | None,
                           reported_result: str, outcome: str, detail: str | None = None, evidence: dict[str, Any] | None = None,
                           reconciler_id: str | None = None) -> None:
        with self._tx() as conn:
            conn.execute("INSERT INTO reconciliation_log(operation_id,effect_attempt_id,message_id,strategy,reported_result,outcome,detail,evidence_json,reconciler_id) VALUES(?,?,?,?,?,?,?,?,?)",
                         (operation_id, effect_attempt_id, message_id, strategy if isinstance(strategy, str) else repr(strategy), reported_result,
                          outcome, detail, canonical_json(evidence) if evidence is not None else None, reconciler_id))

    def unfinished_reconciliations(self) -> list[dict[str, Any]]:
        """Filas PENDING sin cierre posterior para el mismo mensaje: una reconciliación que pudo quedar a medias por una caída."""
        with closing(self._connect()) as conn:
            rows = conn.execute("""SELECT * FROM reconciliation_log r WHERE outcome='PENDING' AND NOT EXISTS (
                                     SELECT 1 FROM reconciliation_log c WHERE c.message_id=r.message_id AND c.seq>r.seq
                                     AND c.outcome IN ('APPLIED','REFUSED','ABORTED_BEFORE_APPLY','INCONSISTENT')) ORDER BY seq""").fetchall()
            return [dict(r) for r in rows]

    def reconciliation_history(self, *, message_id: str | None = None, operation_id: str | None = None) -> list[dict[str, Any]]:
        q, args = "SELECT * FROM reconciliation_log WHERE 1=1", []
        if message_id is not None:
            q += " AND message_id=?"; args.append(message_id)
        if operation_id is not None:
            q += " AND operation_id=?"; args.append(operation_id)
        with closing(self._connect()) as conn:
            return [dict(r) for r in conn.execute(q + " ORDER BY seq", args).fetchall()]

    # ------------------------------------------------------------------ retención y cuarentena (D-43, D-44)
    # Ningún plazo tiene valor por defecto: los decide el operador. Nada se borra sin que lo pida una llamada explícita.
    @staticmethod
    def _days_modifier(value: Any, name: str) -> str:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ProtocolError(f"{name} debe ser un número finito mayor que 0 (días)")
        return f"-{float(value):.6f} days"

    def compact_inbox(self, *, older_than_days: float) -> int:
        """Vacía `payload_json` de las filas del inbox recibidas hace más de N días y conserva `message_id`, `payload_sha256` y estado.
        NO purga filas: la deduplicación y la detección de conflictos siguen funcionando indefinidamente (borrar la fila permitiría que un
        redelivery tardío se aceptara como mensaje nuevo, AT-02). Devuelve cuántas filas compactó."""
        modifier = self._days_modifier(older_than_days, "older_than_days")
        with self._tx() as conn:
            cur = conn.execute("UPDATE adapter_inbox SET payload_json='', compacted_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                               "WHERE compacted_at IS NULL AND received_at < strftime('%Y-%m-%dT%H:%M:%fZ','now', ?)", (modifier,))
            return cur.rowcount

    def list_quarantine(self, *, reviewed: bool | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """Filas de `adapter_quarantine` con su revisión (si la hay). reviewed=None: todas; True: revisadas; False: pendientes de revisar."""
        if reviewed is not None and not isinstance(reviewed, bool):
            raise ProtocolError("reviewed debe ser True, False o None")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ProtocolError("limit debe ser un entero entre 1 y 1000")
        where = "" if reviewed is None else (" WHERE r.review_id IS NOT NULL" if reviewed else " WHERE r.review_id IS NULL")
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT q.*, r.decision, r.reviewer_id, r.note, r.reviewed_at FROM adapter_quarantine q "
                                "LEFT JOIN quarantine_review r ON r.quarantine_id=q.quarantine_id" + where + " ORDER BY q.quarantine_id LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]

    def review_quarantine(self, quarantine_id: int, *, decision: str, reviewer_id: str, note: str | None = None) -> None:
        """Registra, una sola vez y de forma append-only, que una persona/componente revisó la fila: DISMISSED (analizada, sin acción) o
        REPROCESSED (se reprocesó fuera del adaptador). El adaptador no reinyecta nada por sí mismo."""
        if isinstance(quarantine_id, bool) or not isinstance(quarantine_id, int):
            raise ProtocolError("quarantine_id debe ser un entero")
        if decision not in ("DISMISSED", "REPROCESSED"):
            raise ProtocolError("decision debe ser DISMISSED o REPROCESSED")
        _text(reviewer_id, "reviewer_id")
        if note is not None and not isinstance(note, str):
            raise ProtocolError("note debe ser texto o None")
        with self._tx() as conn:
            if conn.execute("SELECT 1 FROM adapter_quarantine WHERE quarantine_id=?", (quarantine_id,)).fetchone() is None:
                raise ProtocolError(f"no existe la fila de cuarentena {quarantine_id}")
            try:
                conn.execute("INSERT INTO quarantine_review(quarantine_id,decision,reviewer_id,note) VALUES(?,?,?,?)", (quarantine_id, decision, reviewer_id, note))
            except sqlite3.IntegrityError:
                raise ProtocolError(f"la fila de cuarentena {quarantine_id} ya fue revisada") from None

    def purge_reviewed_quarantine(self, *, older_than_days: float) -> int:
        """Borra las filas de cuarentena REVISADAS hace más de N días, dejando una lápida (`quarantine_purge_log`) con identificadores, motivo, hashes,
        decisión y revisor. Una fila sin revisar no se borra nunca, por antigua que sea. Devuelve cuántas purgó."""
        modifier = self._days_modifier(older_than_days, "older_than_days")
        with self._tx() as conn:
            rows = conn.execute("SELECT q.quarantine_id,q.message_id,q.reason,q.incoming_sha256,q.stored_sha256,r.decision,r.reviewer_id,r.reviewed_at "
                                "FROM adapter_quarantine q JOIN quarantine_review r ON r.quarantine_id=q.quarantine_id "
                                "WHERE r.reviewed_at < strftime('%Y-%m-%dT%H:%M:%fZ','now', ?)", (modifier,)).fetchall()
            for r in rows:
                conn.execute("INSERT INTO quarantine_purge_log(quarantine_id,message_id,reason,incoming_sha256,stored_sha256,decision,reviewer_id,reviewed_at) "
                             "VALUES(?,?,?,?,?,?,?,?)", tuple(r))
                conn.execute("DELETE FROM adapter_quarantine WHERE quarantine_id=?", (r["quarantine_id"],))
            return len(rows)

    def quarantine_backlog(self) -> dict[str, Any]:
        """Cuántas filas siguen sin revisar y desde cuándo (para vigilar que la cuarentena no se acumule)."""
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT COUNT(*), MIN(q.created_at) FROM adapter_quarantine q LEFT JOIN quarantine_review r "
                               "ON r.quarantine_id=q.quarantine_id WHERE r.review_id IS NULL").fetchone()
            return {"unreviewed": int(row[0]), "oldest_unreviewed_at": row[1]}

    def quarantine_purge_history(self) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM quarantine_purge_log ORDER BY purge_id").fetchall()]
