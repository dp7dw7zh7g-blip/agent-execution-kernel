"""
inbox_outbox.py
================
Cierra la sección FASE 1.7 / EVENT ORDERING del Registro Maestro: hasta
ahora `store.py` sabía escribir en `outbox` (atómico con el cambio de
estado, ver test_store.py), pero no existía el otro lado: publicar lo
pendiente, ni el lado de consumo (`Inbox: deduplicar -> procesar ->
actualizar proyección/estado local -> ACK`).

Dos piezas, cada una con su propia garantía real (no solo declarada):

OutboxPublisher
---------------
"At-least-once + consumidores idempotentes. No se presupone
exactly-once global" (documento). Esto es estructural: el envío externo
(`sender_fn`) y el marcado `published=1` NO pueden ser la misma
transacción SQLite — el envío es I/O externo, y P-016 prohíbe mantener
una transacción SQLite abierta durante llamadas externas. Eso significa
que SIEMPRE existe una ventana entre "se envió" y "se marcó publicado"
donde un crash produce un reenvío duplicado en la próxima corrida. Esto
no es un bug: es la razón por la que Inbox tiene que deduplicar.

InboxProcessor
--------------
"deduplicar -> procesar -> actualizar proyección/estado local -> ACK"
en una misma unidad atómica LOCAL (a diferencia de Outbox, aquí sí
podemos meter dedup + processor_fn + ACK en una sola transacción SQLite,
porque processor_fn no debe hacer I/O externo — solo debe escribir en
SQLite. Si processor_fn falla a mitad de camino, la transacción entera
hace rollback, incluyendo el INSERT de dedup: así, la próxima entrega
del mismo evento (con el mismo dedup_key) se reprocesa desde cero, en
vez de quedar marcada como "vista" pero nunca aplicada.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional

from .canonical import canonical_json
from .store import SQLiteStore


class ReceiveStatus(str, Enum):
    PROCESSED = "PROCESSED"
    ALREADY_PROCESSED = "ALREADY_PROCESSED"  # dedup: no-op, idempotente


@dataclass(frozen=True)
class ReceiveResult:
    status: ReceiveStatus
    dedup_key: str


@dataclass(frozen=True)
class PublishResult:
    published_count: int
    failed_count: int
    failures: tuple  # tuple de (outbox_id, error_message)
    skipped: bool = False  # True: otro proceso ya está publicando (D-64); esta corrida no hizo nada
    quarantined_count: int = 0  # D-65: eventos que en ESTA corrida pasaron a cuarentena (payload no JSON o intentos agotados)
    lease_lost: bool = False  # D-65: el lease venció y otro publicador lo tomó a mitad de la corrida; se detuvo sin enviar más


DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_LEASE_S = 60.0


class OutboxPublisher:
    def __init__(self, store: SQLiteStore):
        self._store = store

    def publish_pending(
        self, sender_fn: Callable[[str, dict], None], limit: int = 100, *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS, lease_s: float = DEFAULT_LEASE_S,
    ) -> PublishResult:
        """
        Envía cada evento pendiente vía `sender_fn(event_type, payload)`
        y marca published=1 SOLO si sender_fn no lanzó excepción. El
        marcado es su propia transacción, separada del envío (no se
        puede evitar: el envío es I/O externo, P-016). Un crash entre
        "sender_fn devolvió éxito" y "se marcó published=1" deja el
        evento pendiente de nuevo -> se reenvía en la próxima corrida
        -> el consumidor lo ve como redelivery y lo deduplica por su
        cuenta (esto es exactamente el "at-least-once" del documento).

        D-65 (sustituye al `flock` del 0013, que no existe en Windows): solo un publicador a la vez, con un LEASE en SQLite
        (`outbox_publisher_lease`), válido entre procesos y en cualquier plataforma. Si otro lo tiene vigente devuelve
        `PublishResult(0, 0, (), skipped=True)` sin leer ni enviar nada. El lease dura `lease_s`, se renueva antes de CADA envío y
        caduca solo si el proceso muere. Elegir `lease_s` mayor que el peor tiempo de UN envío. Si aun así se pierde (lo tomó otro),
        la corrida se detiene antes de enviar más y lo informa en `lease_lost`; el envío en curso puede duplicarse (at-least-once).

        D-65 (cuarentena): un payload que no es JSON es un fallo permanente y pasa a cuarentena en el acto; un fallo del sender cuenta un
        intento y, al llegar a `max_attempts`, también. Los eventos en cuarentena dejan de ocupar la cabeza de la cola (no bloquean a los
        siguientes) y se consultan con `SQLiteStore.get_quarantined_outbox()`; no se borran ni se reintentan solos.

        D-66 (parche 0015): una cuarentena por un fallo TRANSITORIO (p. ej. el destino estuvo caído más de `max_attempts` corridas) se revierte
        con `SQLiteStore.requeue_quarantined(outbox_id, requeued_by=..., reason=...)`, que lo anota en `outbox_requeues` y reinicia su contador.
        Orden: el outbox NO garantiza el orden de entrega. Un fallo o una cuarentena no detiene a los eventos siguientes (decisión de
        disponibilidad sobre orden estricto): un consumidor que dependa del orden debe ordenar o deduplicar por su cuenta.
        """
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
            raise ValueError(f"max_attempts: debe ser un entero >= 1, recibido {max_attempts!r}")
        holder = f"{os.getpid()}-{uuid.uuid4().hex}"
        if not self._store.acquire_publisher_lease(holder, lease_s):
            return PublishResult(published_count=0, failed_count=0, failures=(), skipped=True)
        try:
            return self._publish_locked(sender_fn, limit, max_attempts, lease_s, holder)
        finally:
            try:
                self._store.release_publisher_lease(holder)
            except sqlite3.Error:
                pass  # no se oculta el resultado ni la excepción de la corrida; el lease caduca solo

    def _publish_locked(self, sender_fn: Callable[[str, dict], None], limit: int, max_attempts: int, lease_s: float, holder: str) -> PublishResult:
        rows = self._store.get_pending_outbox(limit)

        published = 0
        quarantined = 0
        failures = []

        for outbox_id, event_type, payload_text in rows:
            if not self._store.renew_publisher_lease(holder, lease_s):
                return PublishResult(published_count=published, failed_count=len(failures), failures=tuple(failures),
                                     quarantined_count=quarantined, lease_lost=True)
            permanent = False
            try:
                try:
                    payload = json.loads(payload_text)
                except (ValueError, RecursionError) as e:  # un payload ilegible no se arregla reintentando
                    permanent = True
                    raise ValueError(f"payload no es JSON válido: {e}") from e
                sender_fn(event_type, payload)
            except Exception as e:  # noqa: BLE001 - el envío es de terceros (y el payload puede estar corrupto): cualquier fallo cuenta
                failures.append((outbox_id, str(e)))
                try:
                    if self._store.record_outbox_failure(outbox_id, str(e), max_attempts=max_attempts, permanent=permanent):
                        quarantined += 1
                except sqlite3.Error:
                    pass  # no se pudo anotar el fallo: el evento sigue pendiente y se reintenta en la próxima corrida
                continue  # no se marca published; queda pendiente (o en cuarentena) para la próxima corrida

            # Marcado como su propia transacción local, después del envío.
            with self._store.transaction() as conn:
                conn.execute("UPDATE outbox SET published = 1 WHERE outbox_id = ?", (outbox_id,))
            published += 1

        return PublishResult(published_count=published, failed_count=len(failures), failures=tuple(failures), quarantined_count=quarantined)


class InboxProcessor:
    def __init__(self, store: SQLiteStore):
        self._store = store

    def receive(
        self,
        dedup_key: str,
        event_type: str,
        payload: dict,
        processor_fn: Callable[[sqlite3.Connection, dict], None],
    ) -> ReceiveResult:
        """
        `processor_fn(conn, payload)` debe limitarse a escrituras SQLite
        sobre `conn` (la misma conexión/transacción) — actualizar
        proyección o estado local. NO debe hacer red, llamar tools, ni
        invocar un Reasoning Provider (P-016: nada de eso dentro de una
        transacción SQLite). Si necesita disparar algo externo como
        consecuencia de procesar el evento, eso va a Outbox en la MISMA
        transacción (ver test_store.py, "State + Outbox atómicos"), no
        se ejecuta aquí directamente.

        Dedup real: si dedup_key ya existe y está processed=1, no se
        reprocesa (idempotencia real, T-PROP-03/T-PROP-09). Si existe
        pero processed=0 (crash a mitad de un intento anterior), se
        reintenta el processor_fn desde cero sobre el mismo registro.
        """
        with self._store.transaction() as conn:
            row = conn.execute(
                "SELECT processed FROM inbox WHERE dedup_key = ?", (dedup_key,)
            ).fetchone()

            if row is not None and row[0] == 1:
                return ReceiveResult(status=ReceiveStatus.ALREADY_PROCESSED, dedup_key=dedup_key)

            if row is None:
                conn.execute(
                    "INSERT INTO inbox (dedup_key, event_type, payload, processed) VALUES (?, ?, ?, 0)",
                    (dedup_key, event_type, canonical_json(payload)),
                )

            # procesar (escritura local) + ACK, misma transacción: si
            # processor_fn lanza, TODO este bloque (incluido el INSERT
            # de arriba si era nuevo) hace rollback vía store.transaction().
            processor_fn(conn, payload)

            conn.execute("UPDATE inbox SET processed = 1 WHERE dedup_key = ?", (dedup_key,))

        return ReceiveResult(status=ReceiveStatus.PROCESSED, dedup_key=dedup_key)
