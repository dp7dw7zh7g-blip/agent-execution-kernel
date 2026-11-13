"""Reconciliador del lado DTOS (punto 4 del plan: AT-10, AT-11, AT-14). Slice 0.4.

Aplica un RECONCILIATION_RESULT ya verificado contra los Stores a un efecto DTOS en RECONCILIATION_REQUIRED, solo mediante
`EffectRepository.reconcile` (la única vía que valida estrategia y evidencia y escribe el ledger). Reglas:
- La estrategia debe ser una de las reconocidas por DTOS (`RECONCILIATION_STRATEGIES`) y se pasa EXPLÍCITA: el sobre v1.1 no la lleva
  (D-29: la resuelve un componente local; quien llama a `apply` responde de la estrategia que declara
  y debe identificarse con `reconciler_id`, obligatorio y sin valor por defecto: queda en la bitácora y en la evidencia, D-32).
- La evidencia no puede estar vacía. Sin evidencia o con estrategia desconocida no cambia nada (AT-10).
- REJECTED solo lo pueden concluir las estrategias de `rejected_strategies` (por defecto HUMAN_CONFIRMATION): D-42.
- NOT_FOUND y UNKNOWN son inconcluyentes: el efecto sigue en RECONCILIATION_REQUIRED. NOT_FOUND nunca se convierte en REJECTED
  automáticamente; REJECTED solo se aplica si llega como REJECTED con evidencia (AT-11).
- Resolver el efecto en DTOS NO toca UNAE: ni SUCCEEDED, ni RECONCILED, ni commit (AT-14, I-02). UNAE solo cambia por su propio recibo.
- La evidencia serializada no puede superar `MAX_EVIDENCE_SIZE_BYTES` (EVIDENCE_TOO_LARGE; DTOS repite la comprobación como defensa en profundidad).
- Idempotente por message_id (A-3, g0.8 r10: pasa por el inbox; un message_id reutilizado con otro contenido se pone en cuarentena y lanza
  `MessageConflict`; el reenvío idéntico devuelve el resultado ALMACENADO) y con bitácora append-only (fila PENDING antes, con la evidencia; resultado después).
- DTOS no es atómico aquí: `EffectRepository.reconcile` cambia el estado del intento y escribe el ledger en dos transacciones distintas
  (`SQLiteStore.transaction()` no es reentrante, así que el adaptador no puede envolverlas). Una caída entre ambas deja el intento terminal
  sin fila en `effects`. `recover_after_restart()` lo detecta con la fila PENDING y repara el ledger; sin esa prueba no inventa filas.
"""
from __future__ import annotations

from dataclasses import dataclass
import uuid
from typing import Any

from .adapter import MAX_EVIDENCE_SIZE_BYTES, MessageConflict, ProtocolError, _ident, canonical_json, validate_envelope
from .bridge import StoreBridge

APPLICABLE_RESULTS = frozenset({"CONFIRMED", "REJECTED", "PARTIAL"})
INCONCLUSIVE_RESULTS = frozenset({"UNKNOWN", "NOT_FOUND"})


# Estrategias autorizadas a concluir REJECTED («el efecto NO ocurrió», que puede habilitar un reintento documentado, C-03 D). Por defecto solo la
# confirmación humana: una consulta automática que no encuentra nada (PROVIDER_QUERY, IDEMPOTENCY_LOOKUP, READ_AFTER_WRITE…) puede deberse a retardo o
# a un error del proveedor y es exactamente el NOT_FOUND que I-07 prohíbe convertir en REJECTED. Ampliable con el parámetro `rejected_strategies` (D-42).
DEFAULT_REJECTED_STRATEGIES = frozenset({"HUMAN_CONFIRMATION"})


class ReconcileRejected(ProtocolError):
    """La reconciliación no se aplicó; el efecto DTOS queda como estaba."""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason


@dataclass(frozen=True)
class ReconcileOutcome:
    status: str       # APPLIED | INCONCLUSIVE | ALREADY_PROCESSED
    result: str       # resultado reportado en el mensaje
    dtos_status: str  # estado del intento en DTOS tras la llamada


class LedgerCollision(Exception):
    """Interna (Hallazgo 11): la fila del ledger de un intento terminal no pudo escribirse porque otro efecto ocupa la clave de idempotencia."""


class Reconciler:
    def __init__(self, bridge: StoreBridge, rejected_strategies: Any = DEFAULT_REJECTED_STRATEGIES):
        from dtos.effect_fsm import RECONCILIATION_STRATEGIES  # import perezoso
        if not isinstance(rejected_strategies, (set, frozenset, list, tuple)):
            raise ValueError("rejected_strategies debe ser un conjunto de nombres de estrategia (no un texto)")
        policy = frozenset(rejected_strategies)
        unknown = policy - set(RECONCILIATION_STRATEGIES)
        if unknown:
            raise ValueError(f"estrategias desconocidas en rejected_strategies: {sorted(map(str, unknown))}")
        self.bridge = bridge
        self.rejected_strategies = policy

    def apply(self, message: dict[str, Any], *, strategy: str, reconciler_id: str) -> ReconcileOutcome:
        from dtos.effect_fsm import RECONCILIATION_STRATEGIES  # import perezoso
        b = self.bridge
        if not isinstance(message, dict) or message.get("message_type") != "RECONCILIATION_RESULT":
            raise ProtocolError("el reconciliador solo acepta mensajes RECONCILIATION_RESULT")
        if not isinstance(reconciler_id, str) or not reconciler_id.strip():  # sin valor por defecto: quien declara la estrategia debe identificarse (D-32)
            raise ProtocolError("reconciler_id es obligatorio: identifica al componente que declara la estrategia")
        _ident(reconciler_id, "reconciler_id")  # L-4: identificador acotado (<= 128 caracteres, sin controles ni espacios en los extremos, NFC)
        validate_envelope(message)                    # evidencia vacía, resultado fuera del enum, etc. se rechazan aquí
        mid, op, attempt_id, result = message["message_id"], message["operation_id"], message["effect_attempt_id"], message["result"]
        corr = b.verify_message(message)               # cuarentena + excepción si no cuadra con lo persistido
        # A-3: el mensaje pasa por el inbox como cualquier otro. Un message_id reutilizado con OTRO contenido se pone en cuarentena
        # (MESSAGE_ID_PAYLOAD_CONFLICT) y lanza MessageConflict; un reenvío idéntico devuelve DUPLICATE y sigue por la bitácora.
        b.adapter.receive(message)
        for row in reversed(b.adapter.reconciliation_history(message_id=mid)):
            if row["outcome"] in ("APPLIED", "INCONCLUSIVE"):  # reenvío: se devuelve lo ALMACENADO, nunca lo que dice el mensaje entrante
                if row["reported_result"] != result:  # bitácora anterior al inbox (r9): el mismo message_id con otro resultado es un conflicto
                    b.adapter.quarantine(message, "MESSAGE_ID_PAYLOAD_CONFLICT")
                    raise MessageConflict(f"message_id {mid} reutilizado con otro resultado ({result} frente a {row['reported_result']}); puesto en cuarentena")
                return ReconcileOutcome("ALREADY_PROCESSED", row["reported_result"], b.dtos.get_effect_attempt(attempt_id)["status"])

        def refuse(reason: str, detail: str) -> None:
            b.adapter.log_reconciliation(operation_id=op, effect_attempt_id=attempt_id, message_id=mid, strategy=strategy,
                                         reported_result=result, outcome="REFUSED", detail=reason, reconciler_id=reconciler_id)
            raise ReconcileRejected(reason, detail)

        status = b.dtos.get_effect_attempt(attempt_id)["status"]
        if status != "RECONCILIATION_REQUIRED":
            refuse("NOT_IN_RECONCILIATION", f"el intento de DTOS está en {status}")
        if not isinstance(strategy, str) or strategy not in RECONCILIATION_STRATEGIES:
            refuse("UNKNOWN_STRATEGY", f"estrategia no reconocida por DTOS: {strategy!r}")
        if result in INCONCLUSIVE_RESULTS:
            b.adapter.log_reconciliation(operation_id=op, effect_attempt_id=attempt_id, message_id=mid, strategy=strategy,
                                         reported_result=result, outcome="INCONCLUSIVE", detail="el efecto sigue en RECONCILIATION_REQUIRED", reconciler_id=reconciler_id)
            return ReconcileOutcome("INCONCLUSIVE", result, status)
        if result not in APPLICABLE_RESULTS:
            refuse("RESULT_NOT_APPLICABLE", f"{result} no es un resultado de reconciliación de efecto")
        if result == "REJECTED" and strategy not in self.rejected_strategies:
            refuse("REJECTED_NOT_ALLOWED", f"la estrategia {strategy} no puede concluir REJECTED; permitidas: {sorted(self.rejected_strategies) or 'ninguna'}")
        evidence = {"evidence_refs": list(message["evidence_refs"]), "message_id": mid, "reported_result": result, "declared_by": reconciler_id}
        size = len(canonical_json(evidence).encode("utf-8"))
        if size > MAX_EVIDENCE_SIZE_BYTES:  # antes de escribir nada, ni siquiera la bitácora (D-47)
            refuse("EVIDENCE_TOO_LARGE", f"la evidencia ocupa {size} bytes y el máximo es {MAX_EVIDENCE_SIZE_BYTES}")
        b.adapter.log_reconciliation(operation_id=op, effect_attempt_id=attempt_id, message_id=mid, strategy=strategy,
                                     reported_result=result, outcome="PENDING", evidence=evidence, reconciler_id=reconciler_id)  # DTOS no persiste la evidencia: solo queda aquí
        try:
            loaded = b.effects.reconcile(attempt_id, strategy, result, evidence, op, corr["provider"], corr["idempotency_key"])
        except ValueError as exc:  # la FSM de DTOS rechazó (estrategia, evidencia o transición) o falló el ledger
            now = b.dtos.get_effect_attempt(attempt_id)["status"]
            if now != "RECONCILIATION_REQUIRED":
                # DTOS original: el estado ya cambió y la fila del ledger falló (dos transacciones). Hay que atenderlo a mano: no se oculta como un simple rechazo.
                b.adapter.log_reconciliation(operation_id=op, effect_attempt_id=attempt_id, message_id=mid, strategy=strategy, reported_result=result,
                                             outcome="INCONSISTENT", detail=f"el intento quedó en {now} pero el ledger falló: {exc}", reconciler_id=reconciler_id)
                raise ReconcileRejected("DTOS_PARTIAL_APPLY", f"el intento está en {now} pero el ledger falló: {exc}") from exc
            refuse("DTOS_REFUSED", str(exc))
        applied = loaded.fsm.state.value  # type: ignore[union-attr]
        b.adapter.log_reconciliation(operation_id=op, effect_attempt_id=attempt_id, message_id=mid, strategy=strategy,
                                     reported_result=result, outcome="APPLIED", detail=applied, reconciler_id=reconciler_id)
        return ReconcileOutcome("APPLIED", result, applied)

    # ------------------------------------------------------------------ recuperación tras una caída
    def recover_after_restart(self) -> dict[str, int]:
        """Cierra reconciliaciones que quedaron a medias. No llama a proveedores ni cambia nada en UNAE.
        - PENDING y el intento sigue en RECONCILIATION_REQUIRED: no llegó a aplicarse -> ABORTED_BEFORE_APPLY (se puede reintentar `apply`).
        - PENDING y el intento ya es terminal sin fila en el ledger: se escribe la fila que faltaba (misma forma que `EffectRepository.reconcile`).
        - PENDING y el intento ya es terminal con fila (o PARTIAL): solo se cierra la bitácora.
        - Terminal sin fila en el ledger y SIN rastro en la bitácora: se informa como `unproven` y no se toca (no se inventan filas)."""
        b = self.bridge
        counts = {"aborted_before_apply": 0, "ledger_repaired": 0, "log_completed": 0, "inconsistent": 0, "unproven": 0}
        handled: set[str] = set()
        for row in b.adapter.unfinished_reconciliations():
            attempt_id, op, mid = row["effect_attempt_id"], row["operation_id"], row["message_id"]
            handled.add(attempt_id)
            attempt_row = b.dtos.get_effect_attempt(attempt_id)
            log = dict(operation_id=op, effect_attempt_id=attempt_id, message_id=mid, strategy=row["strategy"], reported_result=row["reported_result"],
                       reconciler_id=row["reconciler_id"])
            if attempt_row is None:  # L-1: el intento ni siquiera existe en DTOS; no aborta la recuperación de los demás
                b.adapter.log_reconciliation(**log, outcome="INCONSISTENT", detail=f"el intento {attempt_id} no existe en DTOS")
                counts["inconsistent"] += 1
                continue
            status = attempt_row["status"]
            if status == "RECONCILIATION_REQUIRED":
                b.adapter.log_reconciliation(**log, outcome="ABORTED_BEFORE_APPLY", detail="la caída ocurrió antes de aplicar")
                counts["aborted_before_apply"] += 1
            elif status in ("CONFIRMED", "REJECTED"):
                try:
                    repaired = self._ensure_ledger(attempt_id, status, row["evidence_json"])
                except LedgerCollision as exc:  # intento huérfano con la clave ocupada por otro efecto: requiere atención manual (Hallazgo 11)
                    b.adapter.log_reconciliation(**log, outcome="INCONSISTENT", detail=str(exc))
                    counts["inconsistent"] += 1
                    continue
                b.adapter.log_reconciliation(**log, outcome="APPLIED", detail=f"{status} (RECOVERED:{'LEDGER_REPAIRED' if repaired else 'LOG_COMPLETED'})")
                counts["ledger_repaired" if repaired else "log_completed"] += 1
            elif status == "PARTIAL":
                b.adapter.log_reconciliation(**log, outcome="APPLIED", detail="PARTIAL (RECOVERED:LOG_COMPLETED)")
                counts["log_completed"] += 1
            else:
                b.adapter.log_reconciliation(**log, outcome="INCONSISTENT", detail=f"el intento está en {status}")
                counts["inconsistent"] += 1
        for corr in b.adapter.list_correlations():
            aid = corr["effect_attempt_id"]
            att = b.dtos.get_effect_attempt(aid)
            if (att and att["status"] in ("CONFIRMED", "REJECTED") and aid not in handled and not b._ledger_operation_ids(aid)
                    and not b.adapter.reconciliation_history(operation_id=corr["operation_id"])):
                counts["unproven"] += 1
        return counts

    def _ensure_ledger(self, attempt_id: str, status: str, evidence_json: str | None = None) -> bool:
        """Escribe la fila del ledger si falta (True) o confirma que ya existe (False). La evidencia es el texto canónico guardado en la
        bitácora (igual al que escribe `EffectRepository.reconcile`); se pasa solo si el Store de DTOS acepta `evidence` (parche 0003)."""
        import inspect
        from dtos.store import ConflictError
        b = self.bridge
        if b._ledger_operation_ids(attempt_id):
            return False
        corr = next((c for c in b.adapter.list_correlations() if c["effect_attempt_id"] == attempt_id), None)
        if corr is None:  # L-1: sin correlación durable no hay de dónde sacar operación, proveedor ni clave: se informa, no se aborta toda la recuperación
            raise LedgerCollision(f"el intento {attempt_id} está en {status} sin fila en el ledger y no tiene correlación durable en el adaptador")
        extra = {"evidence": evidence_json} if "evidence" in inspect.signature(b.dtos.insert_effect).parameters else {}
        try:
            b.dtos.insert_effect(effect_id=str(uuid.uuid4()), operation_id=corr["operation_id"], intent_id=attempt_id,  # intent_id=attempt_id: igual que DTOS (H-1)
                                 attempt_id=attempt_id, provider=corr["provider"], idempotency_key=corr["idempotency_key"], status=status, **extra)
        except ConflictError as exc:
            # Hallazgo 11: un ConflictError NO implica «ya estaba». Solo es benigno si la fila de ESTE intento existe ahora (carrera con otro
            # proceso). Si no, otro efecto ocupa (provider, idempotency_key) y el intento sigue huérfano: no se oculta, se informa.
            if b._ledger_operation_ids(attempt_id):
                return False
            owner = b.dtos.get_effect_by_provider_key(corr["provider"], corr["idempotency_key"])
            who = (f"effect_id={owner['effect_id']} operation_id={owner['operation_id']} attempt_id={owner['attempt_id']} status={owner['status']}"
                   if owner else "sin fila visible (¿otra restricción?)")
            raise LedgerCollision(f"el intento {attempt_id} está en {status} sin fila en el ledger y la clave (provider={corr['provider']!r}, "
                                  f"idempotency_key={corr['idempotency_key']!r}) está ocupada: {who}; {exc}") from exc
        return True
