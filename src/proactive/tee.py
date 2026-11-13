"""Observador «tee» (D-50): registra en `ShadowLog` lo que el agente habría hecho ante cada evento que UNAE entrega.

Se acopla al `sender` que ya recibe `Store.dispatch_outbox`; no necesita ningún cambio en UNAE y esta capa no importa `unae`
(el `sender` lo compone quien despacha). Garantías:
- El envío real corre PRIMERO y su resultado no cambia: si lanza, la excepción se propaga igual y NO se observa nada
  (el evento se reintentará y se observará cuando salga).
- Cualquier excepción (`Exception`) del observador se traga y se reporta por `on_error`: nunca hace fallar ni frena la
  entrega. No se capturan `KeyboardInterrupt` ni `SystemExit`.
- El observador recibe una COPIA del evento: no puede alterar lo que el despachador marca como enviado.
- UNAE entrega «al menos una vez»: un reenvío vuelve a observarse y la unicidad (rule_id, trigger_ref=event_id, kind) de
  `ShadowLog` lo absorbe, sin tocar `consumer_inbox`.
- Si `paused()` detiene el despacho no se envía nada y por tanto no se observa nada.
"""
from __future__ import annotations

import copy
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from proactive.shadow import ShadowLog

TASK_STATE_COMMITTED = "TASK_STATE_COMMITTED"          # único tipo de evento que UNAE emite hoy
MAX_RECENT_ERRORS = 100


@dataclass(frozen=True)
class Proposal:
    """Lo que una regla dice que habría hecho. Se traduce a una fila de `would_have_done`."""
    kind: str
    payload: dict[str, Any]
    reason: str
    kernel_refs: dict[str, str] | None = None


@dataclass(frozen=True)
class Rule:
    rule_id: str
    policy_version: str
    matches: Callable[[dict[str, Any]], bool]
    build: Callable[[dict[str, Any]], Proposal]

    def __post_init__(self):
        for name in ("rule_id", "policy_version"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"{name} required (non-empty string)")
        if not callable(self.matches) or not callable(self.build):
            raise TypeError("matches and build must be callables")


def task_state_committed_review_rule(policy_version: str = "p1") -> Rule:
    """Primera regla, de pura tubería: ante cada estado de tarea confirmado, el agente revisaría esa versión.
    El payload del evento solo trae {task_id, version}: la regla no puede juzgar más que eso."""
    def matches(event: dict[str, Any]) -> bool:
        return event.get("event_type") == TASK_STATE_COMMITTED

    def build(event: dict[str, Any]) -> Proposal:
        p = event["payload"]
        return Proposal(kind="review_task_state",
                        payload={"task_id": p["task_id"], "version": p["version"]},
                        reason="UNAE confirmó un nuevo estado de la tarea; el agente lo revisaría",
                        kernel_refs={"task_id": p["task_id"], "commit_id": event["commit_id"]})
    return Rule("r-task-committed-review", policy_version, matches, build)


class ShadowObserver:
    """Aplica un conjunto de reglas a cada evento y registra las propuestas en un `ShadowLog`.
    `observe` NO lanza `Exception`: un fallo de una regla se reporta y las demás reglas siguen."""

    def __init__(self, shadow: ShadowLog, rules: Sequence[Rule],
                 on_error: Callable[[str, dict[str, Any], BaseException], None] | None = None):
        ids = [r.rule_id for r in rules]
        if len(set(ids)) != len(ids):
            raise ValueError(f"duplicate rule_id in rules: {sorted(i for i in set(ids) if ids.count(i) > 1)}")
        self.shadow, self.rules, self._on_error = shadow, tuple(rules), on_error
        self.errors: deque[tuple[str, str, str]] = deque(maxlen=MAX_RECENT_ERRORS)   # (rule_id, event_id, repr(error))

    def _report(self, rule_id: str, event: dict[str, Any], exc: BaseException) -> None:
        self.errors.append((rule_id, str(event.get("event_id")), repr(exc)))
        if self._on_error is not None:
            try:
                self._on_error(rule_id, event, exc)
            except Exception:
                pass

    def observe(self, event: dict[str, Any]) -> int:
        """Devuelve cuántas filas NUEVAS creó (0 si ninguna regla aplica o todo era repetido)."""
        created = 0
        for rule in self.rules:
            try:
                if not rule.matches(event):
                    continue
                prop = rule.build(event)
                rec = self.shadow.record(rule_id=rule.rule_id, policy_version=rule.policy_version,
                                         trigger_ref=event["event_id"], kind=prop.kind, payload=prop.payload,
                                         reason=prop.reason, kernel_refs=prop.kernel_refs)
                created += rec.created
            except Exception as exc:
                self._report(rule.rule_id, event, exc)
        return created


def tee_sender(real_sender: Callable[[dict[str, Any]], Any], observe: Callable[[dict[str, Any]], Any],
               on_error: Callable[[dict[str, Any], BaseException], None] | None = None) -> Callable[[dict[str, Any]], None]:
    """Devuelve un `sender` para `dispatch_outbox`: primero `real_sender(event)`; si no lanza, `observe(copia del evento)`."""
    if not callable(real_sender) or not callable(observe):
        raise TypeError("real_sender and observe must be callables")

    def sender(event: dict[str, Any]) -> None:
        real_sender(event)                                  # si lanza, se propaga y no se observa
        try:
            observe(copy.deepcopy(event))
        except Exception as exc:                            # el observador jamás puede romper la entrega real
            if on_error is not None:
                try:
                    on_error(event, exc)
                except Exception:
                    pass
    return sender
