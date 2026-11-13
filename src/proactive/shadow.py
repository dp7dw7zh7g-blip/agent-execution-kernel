"""Modo «solo registrar» (shadow) de la capa proactiva: `would_have_done` y `would_have_done_review` (D-49).

Principios (ver DECISIONES, D-49):
- Base SQLite PROPIA, separada de la de UNAE/DTOS. Sin claves foráneas hacia el kernel: `trigger_ref` y `kernel_refs` son
  referencias opacas que el kernel no valida, y por eso no existe atomicidad cruzada con él (se asume a propósito).
- Este módulo NO ejecuta nada ni toca el kernel. Registrar «lo que habría hecho» no tiene efectos fuera de este archivo.
- Ambas tablas son solo-añadir (triggers que abortan UPDATE y DELETE, como el ledger de DTOS). Un veredicto se corrige
  añadiendo otro: vale el último.
- Idempotente por (rule_id, trigger_ref, kind): reintentar o reiniciar no duplica el rastro.
- Si una acción pasa a real, debe salir por `reserve_and_register` de UNAE con `real_action_key(would_id)` como clave
  idempotente. Ese paso NO está implementado aquí.
"""
from __future__ import annotations

import hashlib
import random
import sqlite3
import time
import uuid
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from dtos.canonical import canonical_json

SCHEMA_VERSION = 1
MAX_PAYLOAD_BYTES = 65536        # mismo tope que MAX_EVIDENCE_SIZE_BYTES de DTOS (bytes UTF-8 del JSON canónico)
MAX_KERNEL_REFS_BYTES = 4096
MAX_REASON_CHARS = 500


class Verdict(StrEnum):
    CORRECT = "CORRECT"      # lo que habría hecho era lo correcto
    WRONG = "WRONG"          # habría sido un error
    IGNORED = "IGNORED"      # no aplica o no importa; no cuenta ni a favor ni en contra


@dataclass(frozen=True)
class Recorded:
    would_id: str
    created: bool            # False: ya existía una fila con la misma (rule_id, trigger_ref, kind)
    payload_matches: bool    # True si el payload recibido es idéntico al ya guardado (siempre True si created)


_MIGRATIONS: dict[int, list[str]] = {
    1: [
        """CREATE TABLE would_have_done(
             would_id TEXT PRIMARY KEY CHECK(length(trim(would_id)) > 0),
             created_at REAL NOT NULL,
             rule_id TEXT NOT NULL CHECK(length(trim(rule_id)) > 0),
             policy_version TEXT NOT NULL CHECK(length(trim(policy_version)) > 0),
             trigger_ref TEXT NOT NULL CHECK(length(trim(trigger_ref)) > 0),
             kind TEXT NOT NULL CHECK(length(trim(kind)) > 0),
             payload TEXT NOT NULL,
             payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64),
             payload_bytes INTEGER NOT NULL CHECK(payload_bytes <= 65536),
             kernel_refs TEXT,
             reason TEXT NOT NULL CHECK(length(trim(reason)) > 0),
             UNIQUE(rule_id, trigger_ref, kind))""",
        """CREATE TABLE would_have_done_review(
             review_id INTEGER PRIMARY KEY AUTOINCREMENT,
             would_id TEXT NOT NULL REFERENCES would_have_done(would_id),
             verdict TEXT NOT NULL CHECK(verdict IN ('CORRECT','WRONG','IGNORED')),
             reviewed_at REAL NOT NULL,
             note TEXT NOT NULL DEFAULT '')""",
        "CREATE INDEX idx_review_would ON would_have_done_review(would_id, review_id)",
        """CREATE TRIGGER trg_wouldhavedone_no_update BEFORE UPDATE ON would_have_done
           BEGIN SELECT RAISE(ABORT, 'would_have_done es append-only: UPDATE prohibido'); END""",
        """CREATE TRIGGER trg_wouldhavedone_no_delete BEFORE DELETE ON would_have_done
           BEGIN SELECT RAISE(ABORT, 'would_have_done es append-only: DELETE prohibido'); END""",
        """CREATE TRIGGER trg_review_no_update BEFORE UPDATE ON would_have_done_review
           BEGIN SELECT RAISE(ABORT, 'would_have_done_review es append-only: UPDATE prohibido'); END""",
        """CREATE TRIGGER trg_review_no_delete BEFORE DELETE ON would_have_done_review
           BEGIN SELECT RAISE(ABORT, 'would_have_done_review es append-only: DELETE prohibido'); END""",
    ],
}


def real_action_key(would_id: str) -> str:
    """Clave idempotente para `reserve_and_register` si ESTE rastro se promueve a una acción real.
    Determinista: promover dos veces el mismo would_id da la misma clave, así que UNAE no duplica la ejecución."""
    if not isinstance(would_id, str) or not would_id.strip():
        raise ValueError("would_id required")
    return f"shadow:{would_id}"


def _require_text(name: str, value: Any, max_chars: int | None = None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} required (non-empty string)")
    if max_chars is not None and len(value) > max_chars:
        raise ValueError(f"{name} too long ({len(value)} > {max_chars} chars)")
    return value


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


class ShadowLog:
    """Registro solo-añadir de lo que el agente habría hecho. Abre una conexión por operación y la cierra al terminar:
    no mantiene conexiones abiertas, así que el archivo se puede borrar o mover (también en Windows) entre llamadas."""

    def __init__(self, path: str | Path, clock: Callable[[], float] = time.time, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
                 journal_size_limit_bytes: int = DEFAULT_JOURNAL_SIZE_LIMIT_BYTES):
        busy_timeout_ms, journal_size_limit_bytes = _validate_connection_policy(busy_timeout_ms, journal_size_limit_bytes)  # D-74: antes de abrir nada; D-75: normalizados
        self.busy_timeout_ms = busy_timeout_ms
        self.journal_size_limit_bytes = journal_size_limit_bytes
        self.path = str(path)
        self._clock = clock
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
        """Un intento de apertura: WAL, verificación de la política y migración del esquema (idempotente)."""
        with closing(self._connect()) as c:
            mode = str(c.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
            if mode != "wal":  # D-70: igual que DTOS (D-57), UNAE y el adaptador; antes se degradaba en silencio a otro modo
                raise RuntimeError(f"journal_mode: se pidió WAL y SQLite devolvió {mode!r} para {self.path!r} (¿sistema de archivos de red, sin "
                                   f"soporte de memoria compartida o base en memoria?). La capa proactiva exige WAL.")
            effective = c.execute("PRAGMA busy_timeout").fetchone()[0]
            if effective != self.busy_timeout_ms:
                raise RuntimeError(f"busy_timeout: pedido {self.busy_timeout_ms} ms y SQLite quedó en {effective} ms")
            limit = c.execute("PRAGMA journal_size_limit").fetchone()[0]
            if limit != self.journal_size_limit_bytes:
                raise RuntimeError(f"journal_size_limit: pedido {self.journal_size_limit_bytes} bytes y SQLite quedó en {limit}")
        self._migrate()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=self.busy_timeout_ms / 1000, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")  # int validado en __init__: no hay entrada externa en la sentencia
        conn.execute(f"PRAGMA journal_size_limit={self.journal_size_limit_bytes}")  # D-74
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @staticmethod
    def _rollback(c: sqlite3.Connection) -> None:
        """D-77: ROLLBACK que nunca oculta el error original (mismo criterio que `unae.Store._rollback`, D-68, y `SQLiteStore.transaction()`). Si SQLite ya
        revirtió la transacción por sí mismo, `ROLLBACK` lanza «cannot rollback - no transaction is active» y ese error tapaba al verdadero (y, al no ser
        un bloqueo, impedía además que el reintento de la apertura, D-76, reconociera uno). Solo se revierte si hay transacción y un fallo del propio ROLLBACK no se propaga."""
        if c.in_transaction:
            try:
                c.execute("ROLLBACK")
            except sqlite3.Error:
                pass

    def _migrate(self) -> None:
        """Aplica migraciones pendientes de forma atómica; se niega a arrancar con una base más nueva que este código."""
        with closing(self._connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                c.execute("CREATE TABLE IF NOT EXISTS schema_version(version INTEGER NOT NULL)")
                c.execute("INSERT INTO schema_version(version) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM schema_version)")
                current = c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
                if current > SCHEMA_VERSION:
                    raise RuntimeError(f"database schema v{current} is newer than this code (v{SCHEMA_VERSION})")
                for v in range(current + 1, SCHEMA_VERSION + 1):
                    for stmt in _MIGRATIONS[v]:
                        c.execute(stmt)
                    c.execute("UPDATE schema_version SET version=?", (v,))
                c.execute("COMMIT")
            except Exception:
                self._rollback(c)
                raise

    def schema_version(self) -> int:
        with closing(self._connect()) as c:
            return c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]

    def record(self, *, rule_id: str, policy_version: str, trigger_ref: str, kind: str, payload: dict[str, Any],
               reason: str, kernel_refs: dict[str, str] | None = None) -> Recorded:
        """Registra lo que habría hecho. Idempotente por (rule_id, trigger_ref, kind): si ya existe, no escribe nada y
        devuelve la fila existente con `created=False` (y `payload_matches` indica si el payload nuevo coincide)."""
        _require_text("rule_id", rule_id); _require_text("policy_version", policy_version)
        _require_text("trigger_ref", trigger_ref); _require_text("kind", kind)
        _require_text("reason", reason, MAX_REASON_CHARS)
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object (dict)")
        body = canonical_json(payload)                      # rechaza NaN/Infinity
        size = len(body.encode("utf-8"))
        if size > MAX_PAYLOAD_BYTES:
            raise ValueError(f"payload too large ({size} > {MAX_PAYLOAD_BYTES} bytes)")
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        refs = None
        if kernel_refs is not None:
            if not isinstance(kernel_refs, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in kernel_refs.items()):
                raise ValueError("kernel_refs must be a dict[str, str]")
            refs = canonical_json(kernel_refs)
            if len(refs.encode("utf-8")) > MAX_KERNEL_REFS_BYTES:
                raise ValueError(f"kernel_refs too large (> {MAX_KERNEL_REFS_BYTES} bytes)")
        would_id = uuid.uuid4().hex
        with closing(self._connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                n = c.execute("""INSERT INTO would_have_done(would_id,created_at,rule_id,policy_version,trigger_ref,kind,
                                 payload,payload_sha256,payload_bytes,kernel_refs,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                                 ON CONFLICT(rule_id, trigger_ref, kind) DO NOTHING""",
                              (would_id, self._clock(), rule_id, policy_version, trigger_ref, kind, body, digest, size,
                               refs, reason)).rowcount
                if n == 1:
                    c.execute("COMMIT")
                    return Recorded(would_id, True, True)
                row = c.execute("SELECT would_id, payload_sha256 FROM would_have_done WHERE rule_id=? AND trigger_ref=? AND kind=?",
                                (rule_id, trigger_ref, kind)).fetchone()
                c.execute("COMMIT")
                return Recorded(row["would_id"], False, row["payload_sha256"] == digest)
            except Exception:
                self._rollback(c)
                raise

    def get(self, would_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as c:
            row = c.execute("SELECT * FROM would_have_done WHERE would_id=?", (would_id,)).fetchone()
            return dict(row) if row else None

    def review(self, would_id: str, verdict: Verdict | str, note: str = "") -> int:
        """Añade un veredicto (nunca modifica uno anterior). Vale el último de cada would_id. Devuelve review_id."""
        try:
            verdict = Verdict(verdict)
        except ValueError:
            raise ValueError(f"verdict must be one of {[v.value for v in Verdict]}") from None
        if not isinstance(note, str):
            raise ValueError("note must be a string")
        with closing(self._connect()) as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                if c.execute("SELECT 1 FROM would_have_done WHERE would_id=?", (would_id,)).fetchone() is None:
                    raise KeyError(f"unknown would_id: {would_id}")
                cur = c.execute("INSERT INTO would_have_done_review(would_id,verdict,reviewed_at,note) VALUES(?,?,?,?)",
                                (would_id, verdict.value, self._clock(), note))
                c.execute("COMMIT")
                return cur.lastrowid
            except Exception:
                self._rollback(c)
                raise

    def summary(self, rule_id: str | None = None) -> dict[str, Any]:
        """Conteo por veredicto vigente (el último de cada would_id) y `unreviewed`. `precision` = CORRECT/(CORRECT+WRONG),
        o None si aún no hay ninguno de los dos. Es la métrica para decidir si ampliar permisos; no decide nada por sí sola."""
        sql = """SELECT COALESCE(r.verdict, 'unreviewed') AS v, COUNT(*) AS n FROM would_have_done w
                 LEFT JOIN would_have_done_review r ON r.would_id = w.would_id
                   AND r.review_id = (SELECT MAX(review_id) FROM would_have_done_review WHERE would_id = w.would_id)"""
        args: tuple = ()
        if rule_id is not None:
            sql += " WHERE w.rule_id = ?"; args = (rule_id,)
        with closing(self._connect()) as c:
            counts = {row["v"]: row["n"] for row in c.execute(sql + " GROUP BY v", args)}
        out = {"total": sum(counts.values()), "CORRECT": counts.get("CORRECT", 0), "WRONG": counts.get("WRONG", 0),
               "IGNORED": counts.get("IGNORED", 0), "unreviewed": counts.get("unreviewed", 0)}
        decided = out["CORRECT"] + out["WRONG"]
        out["precision"] = out["CORRECT"] / decided if decided else None
        return out
