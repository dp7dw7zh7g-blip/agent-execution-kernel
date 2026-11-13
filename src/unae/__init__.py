"""Paquete `unae` — UNAE 4.3S rev3 (referencia local Python + SQLite).

NOTA DE PROCEDENCIA: este __init__.py fue reconstruido al reorganizar el proyecto, porque el
original no estaba entre los archivos subidos (el __init__.py del proyecto es el de `dtos`).
Solo reexporta los nombres que usan las pruebas (`from unae import Store, CommitStatus, sign_receipt`).
"""
from .store import (
    SCHEMA_VERSION,
    CommitRequest,
    CommitStatus,
    Status,
    Store,
    fsm_table,
    sign_receipt,
)

__all__ = ["SCHEMA_VERSION", "CommitRequest", "CommitStatus", "Status", "Store", "fsm_table", "sign_receipt"]
