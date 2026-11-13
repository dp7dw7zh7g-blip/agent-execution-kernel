"""Paquete `proactive` — capa proactiva por encima de UNAE/DTOS (g0.6 r13–r14).

Es un CLIENTE del kernel: no importa `unae` ni escribe en sus bases. Su almacenamiento es un archivo SQLite propio.
- `ShadowLog` (shadow.py): modo «solo registrar»: lo que el agente HABRÍA hecho, sin efectos reales.
- `tee_sender` / `ShadowObserver` / `Rule` (tee.py): lo conectan a los eventos que entrega `Store.dispatch_outbox`.
"""
from proactive.shadow import MAX_PAYLOAD_BYTES, SCHEMA_VERSION, Recorded, ShadowLog, Verdict, real_action_key
from proactive.tee import Proposal, Rule, ShadowObserver, task_state_committed_review_rule, tee_sender

__all__ = ["MAX_PAYLOAD_BYTES", "SCHEMA_VERSION", "Recorded", "ShadowLog", "Verdict", "real_action_key",
           "Proposal", "Rule", "ShadowObserver", "task_state_committed_review_rule", "tee_sender"]
