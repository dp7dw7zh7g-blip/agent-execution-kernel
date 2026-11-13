from __future__ import annotations

import hashlib
import hmac
from contextlib import closing
import json
import random
import sqlite3
import time
import uuid
import weakref
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


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
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


class Status(StrEnum):
    RESERVED = "RESERVED"
    RUNNING = "RUNNING"
    UNKNOWN = "UNKNOWN"
    RECONCILING = "RECONCILING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RECONCILED = "RECONCILED"


class CommitStatus(StrEnum):
    COMMITTED = "COMMITTED"
    ALREADY_COMMITTED = "ALREADY_COMMITTED"
    KEY_CONFLICT = "KEY_CONFLICT"
    STALE_AUTHORITY = "STALE_AUTHORITY"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    INVALID_OPERATION = "INVALID_OPERATION"
    INVALID_RECEIPT = "INVALID_RECEIPT"
    INVALID_TRANSITION = "INVALID_TRANSITION"


# ÚNICA tabla de transiciones. La base la hace cumplir con triggers generados desde ella (migración v4) y verify_integrity()
# comprueba que el trigger guardado coincide con esta tabla.
_ALLOWED = {
    Status.RESERVED: {Status.RUNNING, Status.CANCELLED},                       # CANCELLED solo desde RESERVED: nunca ejecutó
    Status.RUNNING: {Status.UNKNOWN, Status.SUCCEEDED, Status.FAILED},
    Status.UNKNOWN: {Status.RECONCILING, Status.SUCCEEDED, Status.FAILED},     # un recibo tardío del proveedor también resuelve
    Status.RECONCILING: {Status.UNKNOWN, Status.SUCCEEDED, Status.FAILED},
    Status.SUCCEEDED: {Status.RECONCILED},
    Status.FAILED: {Status.RECONCILED},
    Status.CANCELLED: set(),                                                   # terminal
    Status.RECONCILED: set(),                                                  # terminal
}


def fsm_table() -> dict[str, list[str]]:
    """La tabla de transiciones como datos puros (para compararla con otros paquetes, p. ej. dtos/)."""
    return {k.value: sorted(v.value for v in vs) for k, vs in _ALLOWED.items()}


def _fsm_trigger_sql(table: str, name: str) -> str:
    legal = " OR ".join(f"(OLD.status='{k}' AND NEW.status IN ({','.join(repr(x) for x in vs)}))"
                        for k, vs in fsm_table().items() if vs)
    return (f"CREATE TRIGGER {name} BEFORE UPDATE OF status ON {table} "
            f"WHEN OLD.status<>NEW.status AND NOT ({legal}) "
            f"BEGIN SELECT RAISE(ABORT,'illegal state transition'); END")


SCHEMA_VERSION = 4
# Versión 1 = esquema base de 4.3N (creado por _initialize). Cada entrada migra de (v-1) a v dentro de una transacción.
_MIGRATIONS: dict[int, tuple[str, ...]] = {
    2: ("ALTER TABLE outbox ADD COLUMN claimed_by TEXT",
        "ALTER TABLE outbox ADD COLUMN claim_expires_at REAL"),
    3: ("ALTER TABLE execution_journal ADD COLUMN provider_id TEXT",
        "ALTER TABLE receipts ADD COLUMN provider_id TEXT",
        "ALTER TABLE receipts ADD COLUMN signature TEXT"),
    4: ("ALTER TABLE commit_log ADD COLUMN outcome TEXT",
        "ALTER TABLE commit_log ADD COLUMN attestation TEXT",
        "ALTER TABLE commit_log ADD COLUMN via_reconciliation INTEGER",
        "@journal_fsm", "@reservation_fsm"),
}


def sign_receipt(secret: bytes, *, provider_id: str, task_id: str, receipt_id: str, operation_id: str, reservation_id: str,
                 outcome: str, actual_compute: int, actual_time_ms: int, peak_memory_bytes: int,
                 provider_operation_id: str | None = None) -> str:
    """Firma HMAC-SHA256 (hex) de un recibo. La usa el proveedor/adaptador; el Store la verifica con su keyring."""
    body = {"provider_id": provider_id, "task_id": task_id, "receipt_id": receipt_id, "operation_id": operation_id,
            "reservation_id": reservation_id, "outcome": outcome, "actual_compute": actual_compute,
            "actual_time_ms": actual_time_ms, "peak_memory_bytes": peak_memory_bytes,
            "provider_operation_id": provider_operation_id}
    return hmac.new(secret, b"unae-receipt-v1\n" + canonical_json(body).encode("utf-8"), hashlib.sha256).hexdigest()


@dataclass(frozen=True)
class CommitRequest:
    commit_id: str
    task_id: str
    operation_id: str
    reservation_id: str
    receipt_id: str
    expected_version: int
    fencing_token: int
    state_payload: dict[str, Any]
    event_id: str


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


class Store:
    """Single-file SQLite reference store. External calls must never run inside its transactions."""

    def __init__(self, path: str | Path, keyring: dict[str, bytes | tuple[bytes, ...]] | None = None,
                 require_signed_receipts: bool = False, fault_hook=None, synchronous: str = "FULL", keep_open: bool = True, *,
                 busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS, journal_size_limit_bytes: int = DEFAULT_JOURNAL_SIZE_LIMIT_BYTES):
        """keyring: provider_id -> secreto (o tupla de secretos para rotación). Los secretos viven en memoria, nunca en la base.
        require_signed_receipts: si es True, record_receipt rechaza todo recibo sin firma válida.
        synchronous: "FULL" (por defecto, fsync en cada commit) o "NORMAL" (más rápido; con WAL un corte de energía puede perder
        los últimos commits pero no corrompe la base; un SIGKILL del proceso no pierde nada).
        keep_open: mantiene una conexión ancla abierta mientras viva el Store. Sin ella, cada operación cierra la última conexión
        a la base y SQLite hace checkpoint y borra el -wal en cada llamada (~30 % del tiempo medido). Llamar close() o usar `with`
        para liberarla; no usar el Store a través de fork().
        fault_hook: solo para pruebas de caída. Se invoca como hook("<método>:before_commit") y hook("<método>:after_commit").
        busy_timeout_ms / journal_size_limit_bytes (D-74, solo por nombre): espera ante un bloqueo (10000 ms por defecto; 0 a 2147483647, D-75) y recorte del -wal tras un checkpoint
        completo (64 MiB por defecto; -1 sin límite). Se validan antes de abrir nada y se verifican leyéndolos de vuelta al abrir.
        La apertura se reintenta ante un bloqueo temporal hasta agotar busy_timeout_ms en total (D-76; con 0 no se reintenta)."""
        busy_timeout_ms, journal_size_limit_bytes = _validate_connection_policy(busy_timeout_ms, journal_size_limit_bytes)  # D-75: valores normalizados
        self.path = str(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.journal_size_limit_bytes = journal_size_limit_bytes
        self._keyring = dict(keyring or {})
        self.require_signed_receipts = require_signed_receipts
        self._fault_hook = fault_hook
        if synchronous not in ("FULL", "NORMAL"):
            raise ValueError("synchronous must be 'FULL' or 'NORMAL'")
        self._synchronous = synchronous
        self._finalizer = None
        self._open_with_retry()
        if keep_open:
            anchor = sqlite3.connect(self.path, timeout=self.busy_timeout_ms / 1000, isolation_level=None, check_same_thread=False)
            anchor.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            anchor.execute("SELECT 1 FROM schema_version").fetchone()      # se engancha al WAL; no se usa para nada más
            self._finalizer = weakref.finalize(self, anchor.close)

    def close(self) -> None:
        """Libera la conexión ancla (idempotente). Las operaciones siguen funcionando, solo pierden la optimización."""
        if self._finalizer is not None:
            self._finalizer()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @staticmethod
    def _rollback(c: sqlite3.Connection) -> None:
        """D-68: ROLLBACK que nunca oculta el error original. Si el COMMIT ya se hizo (fallo posterior) o SQLite ya revirtió la transacción por
        sí mismo, `ROLLBACK` lanza «cannot rollback - no transaction is active» y ese error tapaba al verdadero. Es el mismo criterio que
        `dtos.store.SQLiteStore.transaction()`: solo se revierte si hay transacción y un fallo del propio ROLLBACK no se propaga."""
        if c.in_transaction:
            try:
                c.execute("ROLLBACK")
            except sqlite3.Error:
                pass

    def _commit(self, c: sqlite3.Connection, name: str) -> None:
        if self._fault_hook:
            self._fault_hook(f"{name}:before_commit")
        c.execute("COMMIT")
        if self._fault_hook:
            self._fault_hook(f"{name}:after_commit")

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=self.busy_timeout_ms / 1000, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")  # int validado en __init__: no hay entrada externa en la sentencia
        conn.execute(f"PRAGMA journal_size_limit={self.journal_size_limit_bytes}")  # D-74
        conn.execute(f"PRAGMA synchronous={self._synchronous}")
        return conn

    def _open_with_retry(self) -> None:
        """D-76: ejecuta `_initialize` y, ante un bloqueo temporal, repite TODO el paso con pausas crecientes y jitter hasta agotar `busy_timeout_ms` desde el
        inicio; pasado el plazo relanza el error original. Los errores que no son de bloqueo (y el RuntimeError de WAL) no se reintentan. Cada intento abre
        y cierra sus propias conexiones, así que un fallo a medias no deja transacciones abiertas."""
        deadline = time.monotonic() + self.busy_timeout_ms / 1000
        delay = OPEN_RETRY_FIRST_S
        while True:
            try:
                self._initialize()
                return
            except sqlite3.OperationalError as exc:
                if not _is_lock_error(exc):
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(delay * (0.5 + random.random() / 2), remaining))
                delay = min(delay * 2, OPEN_RETRY_MAX_S)

    def _initialize(self) -> None:
        with closing(self.connect()) as c:
            mode = str(c.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
            if mode != "wal":  # D-70: igual que DTOS (D-57); antes se degradaba en silencio a otro modo
                raise RuntimeError(
                    f"journal_mode: se pidió WAL y SQLite devolvió {mode!r} para {self.path!r} (¿sistema de archivos de red, sin soporte de memoria "
                    f"compartida o base en memoria?). UNAE exige WAL: sus operaciones abren una conexión por llamada y dependen de que lectores y "
                    f"escritor no se bloqueen.")
            effective = c.execute("PRAGMA busy_timeout").fetchone()[0]
            if effective != self.busy_timeout_ms:
                raise RuntimeError(f"busy_timeout: pedido {self.busy_timeout_ms} ms y SQLite quedó en {effective} ms")
            limit = c.execute("PRAGMA journal_size_limit").fetchone()[0]
            if limit != self.journal_size_limit_bytes:
                raise RuntimeError(f"journal_size_limit: pedido {self.journal_size_limit_bytes} bytes y SQLite quedó en {limit}")
            c.executescript("""
            CREATE TABLE IF NOT EXISTS schema_version(version INTEGER NOT NULL);
            INSERT INTO schema_version(version) SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM schema_version);
            CREATE TABLE IF NOT EXISTS task_state(
              task_id TEXT PRIMARY KEY, version INTEGER NOT NULL CHECK(version >= 0),
              payload TEXT NOT NULL, fingerprint TEXT NOT NULL, updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS ownership(
              task_id TEXT PRIMARY KEY REFERENCES task_state(task_id), fencing_token INTEGER NOT NULL,
              owner_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS budget_accounts(
              account_id TEXT PRIMARY KEY, compute_limit INTEGER NOT NULL, time_limit_ms INTEGER NOT NULL,
              memory_limit_bytes INTEGER NOT NULL, max_concurrency INTEGER NOT NULL,
              compute_used INTEGER NOT NULL DEFAULT 0, time_used_ms INTEGER NOT NULL DEFAULT 0,
              reserved_compute INTEGER NOT NULL DEFAULT 0, reserved_time_ms INTEGER NOT NULL DEFAULT 0,
              active_memory_bytes INTEGER NOT NULL DEFAULT 0, active_count INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS execution_journal(
              operation_id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES task_state(task_id),
              reservation_id TEXT NOT NULL UNIQUE, idempotency_key TEXT NOT NULL UNIQUE,
              request_fingerprint TEXT NOT NULL, fencing_token INTEGER NOT NULL,
              status TEXT NOT NULL, provider_operation_id TEXT, recovery_count INTEGER NOT NULL DEFAULT 0,
              created_at REAL NOT NULL, updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS reservations(
              reservation_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL UNIQUE REFERENCES execution_journal(operation_id),
              compute_units INTEGER NOT NULL, time_ms INTEGER NOT NULL, memory_bytes INTEGER NOT NULL,
              status TEXT NOT NULL, actual_compute INTEGER, actual_time_ms INTEGER, peak_memory_bytes INTEGER,
              receipt_fingerprint TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS reservation_allocations(
              reservation_id TEXT NOT NULL REFERENCES reservations(reservation_id),
              account_id TEXT NOT NULL REFERENCES budget_accounts(account_id),
              PRIMARY KEY(reservation_id, account_id));
            CREATE TABLE IF NOT EXISTS receipts(
              receipt_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL REFERENCES execution_journal(operation_id),
              reservation_id TEXT NOT NULL REFERENCES reservations(reservation_id), fingerprint TEXT NOT NULL,
              outcome TEXT NOT NULL, actual_compute INTEGER NOT NULL, actual_time_ms INTEGER NOT NULL,
              peak_memory_bytes INTEGER NOT NULL, provider_operation_id TEXT, received_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS commit_log(
              commit_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, operation_id TEXT NOT NULL,
              receipt_id TEXT NOT NULL, expected_version INTEGER NOT NULL, committed_version INTEGER NOT NULL,
              fencing_token INTEGER NOT NULL, fingerprint TEXT NOT NULL, committed_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox(
              event_id TEXT PRIMARY KEY, commit_id TEXT NOT NULL REFERENCES commit_log(commit_id),
              event_type TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING',
              attempts INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, sent_at REAL);
            CREATE TABLE IF NOT EXISTS consumer_inbox(
              consumer_id TEXT NOT NULL, event_id TEXT NOT NULL, processed_at REAL NOT NULL,
              PRIMARY KEY(consumer_id, event_id));
            CREATE TABLE IF NOT EXISTS resource_overruns(
              operation_id TEXT PRIMARY KEY, compute_overrun INTEGER NOT NULL, time_overrun_ms INTEGER NOT NULL,
              memory_overrun_bytes INTEGER NOT NULL, created_at REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_journal_status ON execution_journal(status);
            CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status);
            """)
        self._migrate()

    def schema_version(self) -> int:
        with closing(self.connect()) as c:
            return c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]

    def _migrate(self) -> None:
        """Aplica migraciones pendientes de forma atómica. Releer la versión bajo el lock hace seguro el arranque concurrente."""
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                current = c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
                if current > SCHEMA_VERSION:
                    raise RuntimeError(f"database schema v{current} is newer than this code (v{SCHEMA_VERSION})")
                for v in range(current + 1, SCHEMA_VERSION + 1):
                    for stmt in _MIGRATIONS[v]:
                        if stmt == "@journal_fsm":
                            stmt = _fsm_trigger_sql("execution_journal", "trg_journal_fsm")
                        elif stmt == "@reservation_fsm":
                            stmt = _fsm_trigger_sql("reservations", "trg_reservation_fsm")
                        c.execute(stmt)
                    c.execute("UPDATE schema_version SET version=?", (v,))
                self._commit(c, "_migrate")
            except Exception:
                self._rollback(c)
                raise

    def configure_account(self, account_id: str, compute_limit: int, time_limit_ms: int,
                          memory_limit_bytes: int, max_concurrency: int) -> None:
        vals = (compute_limit, time_limit_ms, memory_limit_bytes, max_concurrency)
        if not account_id.strip() or any(v < 0 for v in vals) or max_concurrency < 1:
            raise ValueError("invalid account configuration")
        with closing(self.connect()) as c:
            c.execute("""INSERT INTO budget_accounts(account_id,compute_limit,time_limit_ms,memory_limit_bytes,max_concurrency)
                         VALUES(?,?,?,?,?) ON CONFLICT(account_id) DO UPDATE SET
                         compute_limit=excluded.compute_limit,time_limit_ms=excluded.time_limit_ms,
                         memory_limit_bytes=excluded.memory_limit_bytes,max_concurrency=excluded.max_concurrency""",
                      (account_id, *vals))

    def create_task(self, task_id: str, initial_state: dict[str, Any], owner_id: str = "bootstrap") -> None:
        if not task_id.strip() or not owner_id.strip():
            raise ValueError("task_id and owner_id are required")
        payload = canonical_json(initial_state)
        fp = fingerprint(initial_state)
        now = time.time()
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT version,payload,fingerprint FROM task_state WHERE task_id=?", (task_id,)).fetchone()
                if row:
                    if row["payload"] != payload or row["fingerprint"] != fp:
                        raise ValueError("task already exists with a different initial state")
                    self._commit(c, "create_task")
                    return
                c.execute("INSERT INTO task_state VALUES(?,?,?,?,?)", (task_id, 0, payload, fp, now))
                c.execute("INSERT INTO ownership VALUES(?,?,?)", (task_id, 1, owner_id))
                self._commit(c, "create_task")
            except Exception:
                self._rollback(c)
                raise

    def acquire_authority(self, task_id: str, owner_id: str) -> int:
        if not owner_id.strip():
            raise ValueError("owner_id required")
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT fencing_token FROM ownership WHERE task_id=?", (task_id,)).fetchone()
                if row is None:
                    raise KeyError(task_id)
                token = row[0] + 1
                c.execute("UPDATE ownership SET fencing_token=?,owner_id=? WHERE task_id=?", (token, owner_id, task_id))
                self._fence_previous_owner(c, task_id, token)
                self._commit(c, "acquire_authority")
                return token
            except Exception:
                self._rollback(c)
                raise

    def _release_reservation_counters(self, c: sqlite3.Connection, reservation_id: str) -> None:
        """Devuelve a las cuentas lo reservado. Falla si los contadores son inconsistentes."""
        r = c.execute("SELECT compute_units,time_ms,memory_bytes FROM reservations WHERE reservation_id=?", (reservation_id,)).fetchone()
        if r is None:
            raise KeyError(reservation_id)
        for (aid,) in c.execute("SELECT account_id FROM reservation_allocations WHERE reservation_id=?", (reservation_id,)).fetchall():
            a = c.execute("SELECT * FROM budget_accounts WHERE account_id=?", (aid,)).fetchone()
            if (not a or a["reserved_compute"] < r["compute_units"] or a["reserved_time_ms"] < r["time_ms"]
                    or a["active_memory_bytes"] < r["memory_bytes"] or a["active_count"] < 1):
                raise RuntimeError("ledger counters inconsistent; refusing silent repair")
            c.execute("UPDATE budget_accounts SET reserved_compute=reserved_compute-?,reserved_time_ms=reserved_time_ms-?,"
                      "active_memory_bytes=active_memory_bytes-?,active_count=active_count-1 WHERE account_id=?",
                      (r["compute_units"], r["time_ms"], r["memory_bytes"], aid))

    def _cancel_reserved(self, c: sqlite3.Connection, operation_id: str, reservation_id: str, now: float) -> None:
        self._release_reservation_counters(c, reservation_id)
        c.execute("UPDATE execution_journal SET status=?,updated_at=? WHERE operation_id=?", (Status.CANCELLED.value, now, operation_id))
        c.execute("UPDATE reservations SET status=?,updated_at=? WHERE reservation_id=?", (Status.CANCELLED.value, now, reservation_id))

    def _fence_previous_owner(self, c: sqlite3.Connection, task_id: str, new_token: int) -> None:
        """Operaciones del dueño anterior: RESERVED nunca ejecutó (se libera); RUNNING pudo tener efecto externo
        (pasa a UNKNOWN y conserva recursos hasta reconciliar; lo mismo RECONCILING, cuyo reconciliador murió);
        SUCCEEDED/FAILED ya tienen recibo y solo falta el commit
        (se re-fencean al nuevo dueño)."""
        now = time.time()
        rows = c.execute("SELECT operation_id,reservation_id,status FROM execution_journal WHERE task_id=? AND fencing_token<? AND status IN (?,?,?,?,?)",
                         (task_id, new_token, Status.RESERVED.value, Status.RUNNING.value, Status.RECONCILING.value,
                          Status.SUCCEEDED.value, Status.FAILED.value)).fetchall()
        for row in rows:
            if row["status"] == Status.RESERVED.value:
                self._cancel_reserved(c, row["operation_id"], row["reservation_id"], now)
            elif row["status"] in (Status.SUCCEEDED.value, Status.FAILED.value):
                # El recibo ya está registrado y es válido: solo falta el commit. Pasa al nuevo dueño en vez de quedar huérfano.
                c.execute("UPDATE execution_journal SET fencing_token=?,updated_at=? WHERE operation_id=?", (new_token, now, row["operation_id"]))
            else:
                c.execute("UPDATE execution_journal SET status=?,recovery_count=recovery_count+1,updated_at=? WHERE operation_id=?",
                          (Status.UNKNOWN.value, now, row["operation_id"]))
                c.execute("UPDATE reservations SET status=?,updated_at=? WHERE reservation_id=?", (Status.UNKNOWN.value, now, row["reservation_id"]))

    def release_reservation(self, operation_id: str, fencing_token: int) -> bool:
        """Cancela una reserva que nunca empezó a ejecutarse (solo RESERVED) y libera su presupuesto."""
        now = time.time()
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT task_id,reservation_id,status,fencing_token FROM execution_journal WHERE operation_id=?", (operation_id,)).fetchone()
                owner = c.execute("SELECT fencing_token FROM ownership WHERE task_id=?", (row["task_id"],)).fetchone() if row else None
                if (not row or row["status"] != Status.RESERVED.value or not owner
                        or owner[0] != fencing_token or row["fencing_token"] != fencing_token):
                    self._rollback(c)
                    return False
                self._cancel_reserved(c, operation_id, row["reservation_id"], now)
                self._commit(c, "release_reservation")
                return True
            except Exception:
                self._rollback(c)
                raise

    def expire_reservations(self, max_age_s: float, now: float | None = None) -> int:
        """Cancela reservas RESERVED (nunca ejecutadas) con más de max_age_s de antigüedad."""
        if max_age_s <= 0:
            raise ValueError("max_age_s must be positive")
        now = time.time() if now is None else now
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                rows = c.execute("SELECT operation_id,reservation_id FROM execution_journal WHERE status=? AND created_at<=?",
                                 (Status.RESERVED.value, now - max_age_s)).fetchall()
                for row in rows:
                    self._cancel_reserved(c, row["operation_id"], row["reservation_id"], now)
                self._commit(c, "expire_reservations")
                return len(rows)
            except Exception:
                self._rollback(c)
                raise

    def get_operation(self, operation_id: str) -> dict[str, Any] | None:
        """Lectura pública (solo lectura) de una operación del diario de ejecución; None si no existe.
        No modifica nada. Devuelve una copia: cambiarla no afecta al Store."""
        if not isinstance(operation_id, str):
            raise TypeError("operation_id debe ser str")
        with closing(self.connect()) as c:
            row = c.execute("SELECT operation_id,task_id,reservation_id,idempotency_key,status,fencing_token,provider_operation_id,recovery_count "
                            "FROM execution_journal WHERE operation_id=?", (operation_id,)).fetchone()
        return dict(row) if row else None

    def list_overruns(self, operation_id: str | None = None) -> list[dict[str, Any]]:
        """Sobreconsumos registrados al confirmar (consumo real mayor que lo reservado)."""
        q = "SELECT operation_id,compute_overrun,time_overrun_ms,memory_overrun_bytes,created_at FROM resource_overruns"
        args: tuple = ()
        if operation_id is not None:
            q += " WHERE operation_id=?"; args = (operation_id,)
        with closing(self.connect()) as c:
            return [dict(r) for r in c.execute(q + " ORDER BY created_at", args).fetchall()]

    def account_headroom(self, account_id: str) -> dict[str, int]:
        """Margen restante (puede ser negativo si hubo sobreconsumo). Útil para decidir antes de reservar."""
        with closing(self.connect()) as c:
            a = c.execute("SELECT * FROM budget_accounts WHERE account_id=?", (account_id,)).fetchone()
        if a is None:
            raise KeyError(account_id)
        return {"compute": a["compute_limit"] - a["compute_used"] - a["reserved_compute"],
                "time_ms": a["time_limit_ms"] - a["time_used_ms"] - a["reserved_time_ms"]}

    # ---- Outbox / inbox (entrega al menos una vez + deduplicación en el consumidor) ----

    def pending_events(self, limit: int = 100) -> list[dict[str, Any]]:
        with closing(self.connect()) as c:
            rows = c.execute("SELECT event_id,commit_id,event_type,payload,attempts FROM outbox WHERE status='PENDING' "
                             "ORDER BY created_at,event_id LIMIT ?", (limit,)).fetchall()
        return [{**dict(r), "payload": json.loads(r["payload"])} for r in rows]

    def claim_events(self, worker_id: str, limit: int = 100, lease_s: float = 30.0, now: float | None = None,
                     *, max_attempts: int = 5) -> list[dict[str, Any]]:
        """Reserva eventos PENDING sin dueño (o con lease vencido) para un despachador. Evita envíos dobles entre workers vivos.
        D-65: un evento cuyo payload no es JSON NO se reclama: pasa a DEAD (revisión manual, `dead_events()`) en la misma transacción, para
        que una sola fila corrupta no bloquee la cola; el JSON se valida ANTES del commit, así un fallo no deja a todo el lote reclamado.
        D-66: reclamar un evento cuyo claim VENCIÓ sin que su dueño lo confirmara ni lo marcara fallido cuenta como un intento (el dueño pudo morir a
        mitad del envío: OOM, os._exit, fallo de una librería nativa; ahí no hay `except Exception` que cuente). Al llegar a `max_attempts` el evento
        pasa a DEAD en vez de reclamarse otra vez: un evento tóxico ya no produce un bucle de caídas indefinido. Un dueño lento pero sano también suma
        un intento por cada vencimiento (sigue siendo at-least-once: el consumidor deduplica con consume_once). `dispatch_outbox` pasa su max_attempts."""
        if not worker_id.strip() or lease_s <= 0:
            raise ValueError("worker_id and positive lease_s required")
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
            raise ValueError("max_attempts: entero >= 1")
        now = time.time() if now is None else now
        claimed: list[dict[str, Any]] = []
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                rows = c.execute("SELECT event_id,commit_id,event_type,payload,attempts,claimed_by FROM outbox WHERE status='PENDING' "
                                 "AND (claimed_by IS NULL OR claim_expires_at<=?) ORDER BY created_at,event_id LIMIT ?",
                                 (now, limit)).fetchall()
                for r in rows:
                    try:
                        payload = json.loads(r["payload"])
                    except (ValueError, RecursionError):
                        c.execute("UPDATE outbox SET status='DEAD',claimed_by=NULL,claim_expires_at=NULL WHERE event_id=?", (r["event_id"],))
                        continue
                    attempts = r["attempts"]
                    if r["claimed_by"] is not None:  # D-66: claim vencido y abandonado (la consulta solo lo devuelve si claim_expires_at<=now)
                        attempts += 1
                        if attempts >= max_attempts:
                            c.execute("UPDATE outbox SET status='DEAD',attempts=?,claimed_by=NULL,claim_expires_at=NULL WHERE event_id=?",
                                      (attempts, r["event_id"]))
                            continue
                    c.execute("UPDATE outbox SET claimed_by=?,claim_expires_at=?,attempts=? WHERE event_id=?",
                              (worker_id, now + lease_s, attempts, r["event_id"]))
                    event = {k: r[k] for k in ("event_id", "commit_id", "event_type", "attempts")}  # misma forma que antes: sin claimed_by
                    claimed.append({**event, "payload": payload, "attempts": attempts})
                self._commit(c, "claim_events")
            except Exception:
                self._rollback(c)   # D-65: si el COMMIT ya se hizo (fallo posterior) no hay nada que revertir ni se oculta el error real
                raise
        return claimed

    def renew_claim(self, event_id: str, worker_id: str, lease_s: float = 30.0, now: float | None = None) -> bool:
        """D-65: extiende el lease de UN evento que `worker_id` aún posee (PENDING y reclamado por él, aunque el plazo ya haya vencido
        mientras nadie más lo tomó). False si ya no es suyo: otro worker lo reclamó o dejó de estar PENDING; entonces NO debe enviarlo."""
        if not isinstance(worker_id, str) or not worker_id.strip() or lease_s <= 0:
            raise ValueError("worker_id and positive lease_s required")
        now = time.time() if now is None else now
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                n = c.execute("UPDATE outbox SET claim_expires_at=? WHERE event_id=? AND status='PENDING' AND claimed_by=?",
                              (now + lease_s, event_id, worker_id)).rowcount
                self._commit(c, "renew_claim")
                return n == 1
            except Exception:
                self._rollback(c)
                raise

    def dead_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """Lectura pública (solo lectura) de los eventos DEAD (revisión manual). El payload se devuelve como TEXTO: puede no ser JSON."""
        with closing(self.connect()) as c:
            rows = c.execute("SELECT event_id,commit_id,event_type,payload,attempts FROM outbox WHERE status='DEAD' "
                             "ORDER BY created_at,event_id LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def mark_event_sent(self, event_id: str, worker_id: str | None = None) -> bool:
        """Con worker_id solo tiene efecto si ese worker aún posee el claim (si perdió el lease devuelve False)."""
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                n = c.execute("UPDATE outbox SET status='SENT',sent_at=?,attempts=attempts+1,claimed_by=NULL,claim_expires_at=NULL "
                              "WHERE event_id=? AND status='PENDING' AND (? IS NULL OR claimed_by=?)",
                              (time.time(), event_id, worker_id, worker_id)).rowcount
                self._commit(c, "mark_event_sent")
                return n == 1
            except Exception:
                self._rollback(c)
                raise

    def mark_event_failed(self, event_id: str, max_attempts: int = 5, worker_id: str | None = None) -> str | None:
        """Suma un intento fallido y libera el claim; al llegar a max_attempts pasa a DEAD (revisión manual)."""
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT attempts FROM outbox WHERE event_id=? AND status='PENDING' AND (? IS NULL OR claimed_by=?)",
                                (event_id, worker_id, worker_id)).fetchone()
                if row is None:
                    self._rollback(c)
                    return None
                attempts = row[0] + 1
                status = "DEAD" if attempts >= max_attempts else "PENDING"
                c.execute("UPDATE outbox SET attempts=?,status=?,claimed_by=NULL,claim_expires_at=NULL WHERE event_id=?", (attempts, status, event_id))
                self._commit(c, "mark_event_failed")
                return status
            except Exception:
                self._rollback(c)
                raise

    def release_events(self, event_ids: Iterable[str], worker_id: str) -> int:
        """Devuelve a la cola, SIN contar intento, los eventos PENDING que este worker aún tiene reclamados (pausa, no fallo).
        Ignora los que ya no son suyos (lease vencido y retomado por otro) o ya no están PENDING. Devuelve cuántos liberó."""
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id required")
        ids = list(event_ids)
        if not ids:
            return 0
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                n = 0
                for event_id in ids:
                    n += c.execute("UPDATE outbox SET claimed_by=NULL,claim_expires_at=NULL "
                                   "WHERE event_id=? AND status='PENDING' AND claimed_by=?", (event_id, worker_id)).rowcount
                self._commit(c, "release_events")
                return n
            except Exception:
                self._rollback(c)
                raise

    def dispatch_outbox(self, sender, limit: int = 100, max_attempts: int = 5,
                        worker_id: str | None = None, lease_s: float = 30.0,
                        paused: Callable[[], bool] | None = None) -> dict[str, int]:
        """Reclama eventos con lease y llama a sender(event) FUERA de toda transacción.
        'lost' cuenta eventos cuyo lease venció antes de confirmar: otro worker pudo reenviarlos (al menos una vez),
        por eso el consumidor debe deduplicar con consume_once. Elegir lease_s mayor que el peor tiempo de envío.
        paused: función opcional sin argumentos; si devuelve True el despacho se detiene SIN reclamar y SIN contar intentos
        (fuente apagada, sin cuota…: una pausa lógica no es un fallo). Se consulta antes de reclamar y antes de cada envío;
        lo ya reclamado y no enviado se libera de inmediato. El resultado conserva sus cuatro claves (una pausa no añade ninguna).
        D-65: el lease se RENUEVA antes de cada envío (`renew_claim`); si ya no es de este worker (venció y otro lo reclamó) el evento NO se
        envía y cuenta como 'lost'. Por eso `lease_s` solo debe superar el peor tiempo de UN envío, no el del lote entero."""
        if paused is not None and not callable(paused):
            raise TypeError("paused debe ser una función sin argumentos o None")
        worker_id = worker_id or f"w-{uuid.uuid4().hex[:8]}"
        result = {"sent": 0, "failed": 0, "dead": 0, "lost": 0}
        if paused is not None and paused():
            return result
        events = self.claim_events(worker_id, limit, lease_s, max_attempts=max_attempts)
        for i, event in enumerate(events):
            if paused is not None:
                try:
                    halt = paused()
                except BaseException:
                    self.release_events([e["event_id"] for e in events[i:]], worker_id)
                    raise
                if halt:
                    self.release_events([e["event_id"] for e in events[i:]], worker_id)
                    break
            if not self.renew_claim(event["event_id"], worker_id, lease_s):
                result["lost"] += 1
                continue
            try:
                sender(event)
            except Exception:
                status = self.mark_event_failed(event["event_id"], max_attempts, worker_id)
                result["dead" if status == "DEAD" else "failed" if status else "lost"] += 1
                continue
            result["sent" if self.mark_event_sent(event["event_id"], worker_id) else "lost"] += 1
        return result

    def consume_once(self, consumer_id: str, event_id: str, handler) -> bool:
        """Ejecuta handler(conn, event_id) y registra el evento en consumer_inbox en UNA transacción.
        El handler solo debe tocar esta base (sin llamadas externas). True = procesado; False = ya estaba procesado.
        Si el handler lanza, no queda marca en el inbox y el evento puede reintentarse."""
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                if c.execute("SELECT 1 FROM consumer_inbox WHERE consumer_id=? AND event_id=?", (consumer_id, event_id)).fetchone():
                    self._rollback(c)
                    return False
                handler(c, event_id)
                c.execute("INSERT INTO consumer_inbox VALUES(?,?,?)", (consumer_id, event_id, time.time()))
                self._commit(c, "consume_once")
                return True
            except Exception:
                self._rollback(c)
                raise

    def verify_integrity(self) -> list[str]:
        """Revisa invariantes del libro mayor. Devuelve la lista de violaciones (vacía = consistente). Solo lectura."""
        problems: list[str] = []
        with closing(self.connect()) as c:
            for a in c.execute("SELECT * FROM budget_accounts").fetchall():
                aid = a["account_id"]
                live = c.execute("SELECT COALESCE(SUM(r.compute_units),0) cu,COALESCE(SUM(r.time_ms),0) tm,COALESCE(SUM(r.memory_bytes),0) mb,COUNT(*) n "
                                 "FROM reservations r JOIN reservation_allocations al ON al.reservation_id=r.reservation_id "
                                 "WHERE al.account_id=? AND r.status NOT IN ('CANCELLED','RECONCILED')", (aid,)).fetchone()
                done = c.execute("SELECT COALESCE(SUM(r.actual_compute),0) cu,COALESCE(SUM(r.actual_time_ms),0) tm "
                                 "FROM reservations r JOIN reservation_allocations al ON al.reservation_id=r.reservation_id "
                                 "WHERE al.account_id=? AND r.status='RECONCILED'", (aid,)).fetchone()
                for label, have, want in (("reserved_compute", a["reserved_compute"], live["cu"]), ("reserved_time_ms", a["reserved_time_ms"], live["tm"]),
                                          ("active_memory_bytes", a["active_memory_bytes"], live["mb"]), ("active_count", a["active_count"], live["n"]),
                                          ("compute_used", a["compute_used"], done["cu"]), ("time_used_ms", a["time_used_ms"], done["tm"])):
                    if have != want:
                        problems.append(f"account {aid}: {label}={have} but reservations imply {want}")
            for r in c.execute("SELECT j.operation_id,j.status js,r.status rs FROM execution_journal j JOIN reservations r ON r.operation_id=j.operation_id WHERE j.status<>r.status").fetchall():
                problems.append(f"operation {r['operation_id']}: journal {r['js']} != reservation {r['rs']}")
            for t in c.execute("SELECT task_id,version FROM task_state").fetchall():
                n, top = c.execute("SELECT COUNT(*),COALESCE(MAX(committed_version),0) FROM commit_log WHERE task_id=?", (t["task_id"],)).fetchone()
                if not (t["version"] == n == top):
                    problems.append(f"task {t['task_id']}: version={t['version']} commits={n} max_committed={top}")
                tok = c.execute("SELECT fencing_token FROM ownership WHERE task_id=?", (t["task_id"],)).fetchone()
                high = c.execute("SELECT COALESCE(MAX(fencing_token),0) FROM execution_journal WHERE task_id=?", (t["task_id"],)).fetchone()[0]
                if tok and high > tok[0]:
                    problems.append(f"task {t['task_id']}: journal token {high} above ownership token {tok[0]}")
            for (oid,) in c.execute("SELECT j.operation_id FROM execution_journal j WHERE j.status='RECONCILED' AND (SELECT COUNT(*) FROM receipts x WHERE x.operation_id=j.operation_id)<>1").fetchall():
                problems.append(f"operation {oid}: RECONCILED without exactly one receipt")
            for (oid,) in c.execute("SELECT j.operation_id FROM execution_journal j WHERE j.status='CANCELLED' AND EXISTS (SELECT 1 FROM receipts x WHERE x.operation_id=j.operation_id)").fetchall():
                problems.append(f"operation {oid}: CANCELLED but has a receipt")
            for (rid,) in c.execute("SELECT r.reservation_id FROM reservations r JOIN receipts x ON x.operation_id=r.operation_id WHERE r.status='RECONCILED' "
                                    "AND (r.actual_compute IS NOT x.actual_compute OR r.actual_time_ms IS NOT x.actual_time_ms)").fetchall():
                problems.append(f"reservation {rid}: settled amounts differ from its receipt")
            for (cid,) in c.execute("SELECT l.commit_id FROM commit_log l JOIN receipts x ON x.receipt_id=l.receipt_id WHERE "
                                    "(l.attestation='PROVIDER_SIGNED' AND x.signature IS NULL) OR (l.outcome IS NOT NULL AND l.outcome<>x.outcome)").fetchall():
                problems.append(f"commit {cid}: recorded evidence does not match its receipt")
            for table, name in (("execution_journal", "trg_journal_fsm"), ("reservations", "trg_reservation_fsm")):
                row = c.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (name,)).fetchone()
                if row is None or " ".join(row[0].split()) != " ".join(_fsm_trigger_sql(table, name).split()):
                    problems.append(f"{name}: stored state-machine trigger is missing or differs from the code table")
            for (cid,) in c.execute("SELECT commit_id FROM commit_log WHERE (SELECT COUNT(*) FROM outbox o WHERE o.commit_id=commit_log.commit_id)<>1").fetchall():
                problems.append(f"commit {cid}: expected exactly one outbox event")
            for (oid,) in c.execute("SELECT j.operation_id FROM execution_journal j WHERE j.status='RECONCILED' AND NOT EXISTS (SELECT 1 FROM commit_log l WHERE l.operation_id=j.operation_id)").fetchall():
                problems.append(f"operation {oid}: RECONCILED without commit")
            for (oid,) in c.execute("SELECT l.operation_id FROM commit_log l JOIN execution_journal j ON j.operation_id=l.operation_id WHERE j.status<>'RECONCILED'").fetchall():
                problems.append(f"operation {oid}: committed but not RECONCILED")
        return problems

    def list_unverified_settlements(self) -> list[dict[str, Any]]:
        """RECONCILED significa 'liquidado en el libro y estado confirmado a partir de UN recibo'; NO prueba por sí solo que el
        efecto externo terminó bien. Esta lista devuelve las liquidaciones sin esa garantía de procedencia: recibo sin firma
        del proveedor (UNATTESTED o fila anterior a v4) o que pasó por UNKNOWN (via_reconciliation=1)."""
        with closing(self.connect()) as c:
            rows = c.execute("SELECT commit_id,operation_id,outcome,attestation,via_reconciliation FROM commit_log "
                             "WHERE attestation IS NULL OR attestation<>'PROVIDER_SIGNED' OR via_reconciliation IS NULL OR via_reconciliation=1 "
                             "ORDER BY committed_at").fetchall()
        return [dict(r) for r in rows]

    def begin_reconciliation(self, operation_id: str, fencing_token: int) -> bool:
        """UNKNOWN -> RECONCILING bajo la autoridad vigente; re-fencea la operación para poder cerrarla."""
        now = time.time()
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT task_id,reservation_id,status FROM execution_journal WHERE operation_id=?", (operation_id,)).fetchone()
                owner = c.execute("SELECT fencing_token FROM ownership WHERE task_id=?", (row["task_id"],)).fetchone() if row else None
                if not row or row["status"] != Status.UNKNOWN.value or not owner or owner[0] != fencing_token:
                    self._rollback(c)
                    return False
                c.execute("UPDATE execution_journal SET status=?,fencing_token=?,updated_at=? WHERE operation_id=?",
                          (Status.RECONCILING.value, fencing_token, now, operation_id))
                c.execute("UPDATE reservations SET status=?,updated_at=? WHERE reservation_id=?", (Status.RECONCILING.value, now, row["reservation_id"]))
                self._commit(c, "begin_reconciliation")
                return True
            except Exception:
                self._rollback(c)
                raise

    def reserve_and_register(self, *, task_id: str, operation_id: str, reservation_id: str,
                             idempotency_key: str, fencing_token: int, compute_units: int,
                             time_ms: int, memory_bytes: int, account_ids: tuple[str, ...] = ("__GLOBAL__",)) -> str:
        if not all(x.strip() for x in (task_id, operation_id, reservation_id, idempotency_key)):
            raise ValueError("identifiers are required")
        if min(compute_units, time_ms, memory_bytes) < 0 or (compute_units == 0 and time_ms == 0):
            raise ValueError("invalid resource request")
        now = time.time()
        # The idempotency fingerprint describes the complete immutable logical request.
        # Reusing a key for a different operation/reservation must never look identical.
        accounts = sorted(set(account_ids) | {task_id})
        # El fencing_token es metadato de autoridad, no parte de la solicitud lógica: un reintento tras rotar el token
        # debe reconocerse como la misma solicitud. legacy_fp conserva la huella de 4.3N (con token) para filas ya guardadas.
        logical = {
            "task_id": task_id,
            "operation_id": operation_id,
            "reservation_id": reservation_id,
            "idempotency_key": idempotency_key,
            "compute_units": compute_units,
            "time_ms": time_ms,
            "memory_bytes": memory_bytes,
            "account_ids": accounts,
        }
        req_fp = fingerprint(logical)
        legacy_fp = fingerprint({**logical, "fencing_token": fencing_token})
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                old = c.execute("SELECT request_fingerprint,status FROM execution_journal WHERE idempotency_key=?", (idempotency_key,)).fetchone()
                if old:
                    if old["request_fingerprint"] not in (req_fp, legacy_fp):
                        self._rollback(c)
                        return "KEY_CONFLICT"
                    if old["status"] == Status.CANCELLED.value:       # la solicitud ya no existe: usar otra clave y otro operation_id
                        self._rollback(c)
                        return "CANCELLED"
                    owner = c.execute("SELECT fencing_token FROM ownership WHERE task_id=?", (task_id,)).fetchone()
                    if not owner or owner[0] != fencing_token:         # quien reintenta ya no tiene la autoridad
                        self._rollback(c)
                        return "STALE_AUTHORITY"
                    self._commit(c, "reserve_and_register")
                    return "ALREADY_RESERVED"
                if c.execute("SELECT 1 FROM execution_journal WHERE operation_id=? OR reservation_id=?", (operation_id, reservation_id)).fetchone():
                    self._rollback(c)
                    return "OPERATION_CONFLICT"
                owner = c.execute("SELECT fencing_token FROM ownership WHERE task_id=?", (task_id,)).fetchone()
                state = c.execute("SELECT 1 FROM task_state WHERE task_id=?", (task_id,)).fetchone()
                if not owner or not state or owner[0] != fencing_token:
                    self._rollback(c)
                    return "STALE_AUTHORITY"
                for aid in accounts:
                    a = c.execute("SELECT * FROM budget_accounts WHERE account_id=?", (aid,)).fetchone()
                    if not a:
                        raise ValueError(f"account not configured: {aid}")
                    ok = (a["compute_used"] + a["reserved_compute"] + compute_units <= a["compute_limit"] and
                          a["time_used_ms"] + a["reserved_time_ms"] + time_ms <= a["time_limit_ms"] and
                          a["active_memory_bytes"] + memory_bytes <= a["memory_limit_bytes"] and
                          a["active_count"] + 1 <= a["max_concurrency"])
                    if not ok:
                        self._rollback(c)
                        return "REJECTED_BUDGET"
                c.execute("INSERT INTO execution_journal(operation_id,task_id,reservation_id,idempotency_key,request_fingerprint,fencing_token,status,provider_operation_id,recovery_count,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                          (operation_id, task_id, reservation_id, idempotency_key, req_fp, fencing_token,
                           Status.RESERVED.value, None, 0, now, now))
                c.execute("INSERT INTO reservations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                          (reservation_id, operation_id, compute_units, time_ms, memory_bytes, "RESERVED",
                           None, None, None, None, now, now))
                for aid in accounts:
                    c.execute("INSERT INTO reservation_allocations VALUES(?,?)", (reservation_id, aid))
                    c.execute("UPDATE budget_accounts SET reserved_compute=reserved_compute+?, reserved_time_ms=reserved_time_ms+?, active_memory_bytes=active_memory_bytes+?, active_count=active_count+1 WHERE account_id=?",
                              (compute_units, time_ms, memory_bytes, aid))
                self._commit(c, "reserve_and_register")
                return "RESERVED"
            except Exception:
                self._rollback(c)
                raise

    def mark_running(self, operation_id: str, fencing_token: int, provider_operation_id: str | None = None,
                     provider_id: str | None = None) -> bool:
        if provider_id is not None and not provider_operation_id:
            raise ValueError("a provider-bound operation needs provider_operation_id (the id the provider must echo in its receipt)")
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT task_id,status,fencing_token FROM execution_journal WHERE operation_id=?", (operation_id,)).fetchone()
                if not row or row["status"] != Status.RESERVED.value or row["fencing_token"] != fencing_token:
                    self._rollback(c)
                    return False
                owner = c.execute("SELECT fencing_token FROM ownership WHERE task_id=?", (row["task_id"],)).fetchone()
                if not owner or owner[0] != fencing_token:
                    self._rollback(c)
                    return False
                now = time.time()
                c.execute("UPDATE execution_journal SET status=?,provider_operation_id=?,provider_id=?,updated_at=? WHERE operation_id=?",
                          (Status.RUNNING.value, provider_operation_id, provider_id, now, operation_id))
                c.execute("UPDATE reservations SET status='RUNNING',updated_at=? WHERE reservation_id=(SELECT reservation_id FROM execution_journal WHERE operation_id=?)", (now, operation_id))
                self._commit(c, "mark_running")
                return True
            except Exception:
                self._rollback(c)
                raise

    def recover_interrupted(self) -> int:
        """RUNNING becomes UNKNOWN; no external operation is retried here."""
        now = time.time()
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                rows = c.execute("SELECT operation_id,reservation_id FROM execution_journal WHERE status=?", (Status.RUNNING.value,)).fetchall()
                for row in rows:
                    c.execute("UPDATE execution_journal SET status=?,recovery_count=recovery_count+1,updated_at=? WHERE operation_id=?",
                              (Status.UNKNOWN.value, now, row["operation_id"]))
                    c.execute("UPDATE reservations SET status='UNKNOWN',updated_at=? WHERE reservation_id=?", (now, row["reservation_id"]))
                self._commit(c, "recover_interrupted")
                return len(rows)
            except Exception:
                self._rollback(c)
                raise

    def _verify_receipt(self, provider_id: str, task_id: str, signature: str, receipt_id: str, operation_id: str, reservation_id: str,
                        outcome: str, actual_compute: int, actual_time_ms: int, peak_memory_bytes: int,
                        provider_operation_id: str | None) -> bool:
        secrets = self._keyring.get(provider_id)
        if secrets is None or not isinstance(signature, str):
            return False
        if isinstance(secrets, bytes):
            secrets = (secrets,)
        ok = False
        for secret in secrets:                      # tupla = rotación de claves: vale cualquiera de las vigentes
            expected = sign_receipt(secret, provider_id=provider_id, task_id=task_id, receipt_id=receipt_id, operation_id=operation_id,
                                    reservation_id=reservation_id, outcome=outcome, actual_compute=actual_compute,
                                    actual_time_ms=actual_time_ms, peak_memory_bytes=peak_memory_bytes,
                                    provider_operation_id=provider_operation_id)
            ok |= hmac.compare_digest(expected.encode(), signature.encode("utf-8", "replace"))
        return ok

    def record_receipt(self, *, receipt_id: str, operation_id: str, reservation_id: str,
                       outcome: str, actual_compute: int, actual_time_ms: int,
                       peak_memory_bytes: int, provider_operation_id: str | None = None,
                       provider_id: str | None = None, signature: str | None = None, task_id: str | None = None) -> str:
        """Retornos: RECORDED, ALREADY_RECORDED, RECEIPT_CONFLICT, INVALID_OPERATION, INVALID_RECEIPT,
        BAD_SIGNATURE (firma inválida o proveedor desconocido), UNSIGNED_REJECTED (la operación o el Store exigen firma)."""
        if outcome not in ("SUCCEEDED", "FAILED") or min(actual_compute, actual_time_ms, peak_memory_bytes) < 0:
            raise ValueError("invalid receipt")
        if (provider_id is None) != (signature is None) or (signature is not None and not task_id):
            raise ValueError("provider_id, task_id and signature must be given together")
        if signature is None and self.require_signed_receipts:
            return "UNSIGNED_REJECTED"
        if signature is not None and not self._verify_receipt(provider_id, task_id, signature, receipt_id, operation_id, reservation_id,
                                                              outcome, actual_compute, actual_time_ms, peak_memory_bytes,
                                                              provider_operation_id):
            return "BAD_SIGNATURE"
        body = {"receipt_id": receipt_id, "operation_id": operation_id, "reservation_id": reservation_id,
                "outcome": outcome, "actual_compute": actual_compute, "actual_time_ms": actual_time_ms,
                "peak_memory_bytes": peak_memory_bytes, "provider_operation_id": provider_operation_id}
        if provider_id is not None:
            body["provider_id"] = provider_id
            body["task_id"] = task_id
        fp = fingerprint(body)
        now = time.time()
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                existing = c.execute("SELECT fingerprint FROM receipts WHERE receipt_id=?", (receipt_id,)).fetchone()
                if existing:
                    self._rollback(c)
                    return "ALREADY_RECORDED" if existing[0] == fp else "RECEIPT_CONFLICT"
                op = c.execute("SELECT reservation_id,status,provider_operation_id,provider_id,task_id FROM execution_journal WHERE operation_id=?", (operation_id,)).fetchone()
                if not op or op["reservation_id"] != reservation_id or op["status"] not in ("RUNNING", "UNKNOWN", "RECONCILING"):
                    self._rollback(c)
                    return "INVALID_OPERATION"
                if signature is None and op["provider_id"] is not None:      # operación ligada a un proveedor: exige firma
                    self._rollback(c)
                    return "UNSIGNED_REJECTED"
                if signature is not None and (op["provider_id"] != provider_id or op["task_id"] != task_id):  # proveedor o tarea ajenos
                    self._rollback(c)
                    return "INVALID_RECEIPT"
                # If execution was registered with a provider operation ID, the receipt
                # must carry the same ID. Omitting it is not equivalent to a match.
                expected_provider_id = op["provider_operation_id"]
                if expected_provider_id is not None and provider_operation_id != expected_provider_id:
                    self._rollback(c)
                    return "INVALID_RECEIPT"
                c.execute("INSERT INTO receipts(receipt_id,operation_id,reservation_id,fingerprint,outcome,actual_compute,actual_time_ms,"
                          "peak_memory_bytes,provider_operation_id,received_at,provider_id,signature) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                          (receipt_id, operation_id, reservation_id, fp, outcome, actual_compute, actual_time_ms,
                           peak_memory_bytes, provider_operation_id, now, provider_id, signature))
                # Un recibo es evidencia del proveedor, no un acto de autoridad: se acepta bajo cualquier token. Desde ese momento la
                # operación pertenece a la autoridad VIGENTE (solo ella puede hacer el commit); si no, una operación UNKNOWN del dueño
                # anterior quedaría SUCCEEDED/FAILED con token viejo, retenida para siempre.
                c.execute("UPDATE execution_journal SET status=?,fencing_token=(SELECT fencing_token FROM ownership WHERE task_id=execution_journal.task_id),"
                          "updated_at=? WHERE operation_id=?", (outcome, now, operation_id))
                c.execute("UPDATE reservations SET status=?,actual_compute=?,actual_time_ms=?,peak_memory_bytes=?,receipt_fingerprint=?,updated_at=? WHERE reservation_id=?",
                          (outcome, actual_compute, actual_time_ms, peak_memory_bytes, fp, now, reservation_id))
                self._commit(c, "record_receipt")
                return "RECORDED"
            except Exception:
                self._rollback(c)
                raise

    def coordinated_commit(self, request: CommitRequest) -> CommitStatus:
        if not all(x.strip() for x in (request.commit_id, request.task_id, request.operation_id,
                                       request.reservation_id, request.receipt_id, request.event_id)):
            raise ValueError("commit identifiers are required")
        if request.expected_version < 0 or request.fencing_token < 0:
            raise ValueError("version and token must be non-negative")
        state_json = canonical_json(request.state_payload)
        state_fp = fingerprint(request.state_payload)
        commit_fp = fingerprint({"task_id": request.task_id, "operation_id": request.operation_id,
                                 "reservation_id": request.reservation_id, "receipt_id": request.receipt_id,
                                 "expected_version": request.expected_version, "fencing_token": request.fencing_token,
                                 "state_fp": state_fp, "event_id": request.event_id})
        now = time.time()
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                old = c.execute("SELECT fingerprint FROM commit_log WHERE commit_id=?", (request.commit_id,)).fetchone()
                if old:
                    self._rollback(c)
                    return CommitStatus.ALREADY_COMMITTED if old[0] == commit_fp else CommitStatus.KEY_CONFLICT
                owner = c.execute("SELECT fencing_token FROM ownership WHERE task_id=?", (request.task_id,)).fetchone()
                if not owner or owner[0] != request.fencing_token:
                    self._rollback(c)
                    return CommitStatus.STALE_AUTHORITY
                state = c.execute("SELECT version FROM task_state WHERE task_id=?", (request.task_id,)).fetchone()
                if not state or state[0] != request.expected_version:
                    self._rollback(c)
                    return CommitStatus.VERSION_CONFLICT
                op = c.execute("SELECT * FROM execution_journal WHERE operation_id=?", (request.operation_id,)).fetchone()
                reservation = c.execute("SELECT * FROM reservations WHERE reservation_id=?", (request.reservation_id,)).fetchone()
                receipt = c.execute("SELECT * FROM receipts WHERE receipt_id=?", (request.receipt_id,)).fetchone()
                if (not op or not reservation or not receipt or op["task_id"] != request.task_id or
                    op["reservation_id"] != request.reservation_id or op["fencing_token"] != request.fencing_token or
                    reservation["operation_id"] != request.operation_id or receipt["operation_id"] != request.operation_id or
                    receipt["reservation_id"] != request.reservation_id or op["status"] not in ("SUCCEEDED", "FAILED") or
                    receipt["outcome"] != op["status"]):
                    self._rollback(c)
                    return CommitStatus.INVALID_RECEIPT
                if reservation["status"] == Status.RECONCILED.value:
                    self._rollback(c)
                    return CommitStatus.INVALID_TRANSITION
                if Status.RECONCILED not in _ALLOWED.get(Status(op["status"]), set()):
                    self._rollback(c)
                    return CommitStatus.INVALID_TRANSITION
                allocations = c.execute("SELECT account_id FROM reservation_allocations WHERE reservation_id=? ORDER BY account_id", (request.reservation_id,)).fetchall()
                for allocation in allocations:
                    aid = allocation[0]
                    a = c.execute("SELECT * FROM budget_accounts WHERE account_id=?", (aid,)).fetchone()
                    if not a or a["reserved_compute"] < reservation["compute_units"] or a["reserved_time_ms"] < reservation["time_ms"] or a["active_memory_bytes"] < reservation["memory_bytes"] or a["active_count"] < 1:
                        raise RuntimeError("ledger counters inconsistent; refusing silent repair")
                for allocation in allocations:
                    aid = allocation[0]
                    c.execute("UPDATE budget_accounts SET reserved_compute=reserved_compute-?,reserved_time_ms=reserved_time_ms-?,compute_used=compute_used+?,time_used_ms=time_used_ms+?,active_memory_bytes=active_memory_bytes-?,active_count=active_count-1 WHERE account_id=?",
                              (reservation["compute_units"], reservation["time_ms"], receipt["actual_compute"], receipt["actual_time_ms"], reservation["memory_bytes"], aid))
                over_compute = max(0, receipt["actual_compute"] - reservation["compute_units"])
                over_time = max(0, receipt["actual_time_ms"] - reservation["time_ms"])
                over_memory = max(0, receipt["peak_memory_bytes"] - reservation["memory_bytes"])
                if over_compute or over_time or over_memory:
                    c.execute("INSERT INTO resource_overruns VALUES(?,?,?,?,?)", (request.operation_id, over_compute, over_time, over_memory, now))
                new_version = request.expected_version + 1
                updated = c.execute("UPDATE task_state SET version=?,payload=?,fingerprint=?,updated_at=? WHERE task_id=? AND version=?",
                                    (new_version, state_json, state_fp, now, request.task_id, request.expected_version)).rowcount
                if updated != 1:
                    raise RuntimeError("compare-and-swap failed inside transaction")
                recovery = c.execute("SELECT recovery_count FROM execution_journal WHERE operation_id=?", (request.operation_id,)).fetchone()[0]
                attestation = "PROVIDER_SIGNED" if receipt["signature"] else "UNATTESTED"
                c.execute("INSERT INTO commit_log(commit_id,task_id,operation_id,receipt_id,expected_version,committed_version,fencing_token,"
                          "fingerprint,committed_at,outcome,attestation,via_reconciliation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                          (request.commit_id, request.task_id, request.operation_id, request.receipt_id,
                           request.expected_version, new_version, request.fencing_token, commit_fp, now,
                           receipt["outcome"], attestation, 1 if recovery > 0 else 0))
                c.execute("UPDATE execution_journal SET status=?,updated_at=? WHERE operation_id=?", (Status.RECONCILED.value, now, request.operation_id))
                c.execute("UPDATE reservations SET status=?,updated_at=? WHERE reservation_id=?", (Status.RECONCILED.value, now, request.reservation_id))
                c.execute("INSERT INTO outbox(event_id,commit_id,event_type,payload,status,attempts,created_at) VALUES(?,?,?,?,?,?,?)",
                          (request.event_id, request.commit_id, "TASK_STATE_COMMITTED", canonical_json({"task_id": request.task_id, "version": new_version}), "PENDING", 0, now))
                self._commit(c, "coordinated_commit")
                return CommitStatus.COMMITTED
            except Exception:
                self._rollback(c)
                raise

    def mark_unknown(self, operation_id: str) -> bool:
        now = time.time()
        with closing(self.connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                row = c.execute("SELECT reservation_id,status FROM execution_journal WHERE operation_id=?", (operation_id,)).fetchone()
                if not row or row["status"] not in (Status.RUNNING.value, Status.RECONCILING.value):
                    self._rollback(c)
                    return False
                c.execute("UPDATE execution_journal SET status=?,updated_at=? WHERE operation_id=?", (Status.UNKNOWN.value, now, operation_id))
                c.execute("UPDATE reservations SET status=?,updated_at=? WHERE reservation_id=?", (Status.UNKNOWN.value, now, row["reservation_id"]))
                self._commit(c, "mark_unknown")
                return True
            except Exception:
                self._rollback(c)
                raise
