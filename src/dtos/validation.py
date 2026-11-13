"""
validation.py
==============
Validadores de forma compartidos. Un DTO inválido debe fallar en
construcción (fail-fast), no propagarse silenciosamente hasta el hash o
la persistencia. Esto es lo que hace que "Contracts" en FASE 1 sea más
que un diccionario tipado: es la primera línea de defensa de I-020
(Model output cannot bypass verification) aplicada a nivel de dato.
"""

from __future__ import annotations

from typing import Optional


class ContractValidationError(ValueError):
    """Error de forma en un DTO. Distinto de un error de negocio."""


def require_nonempty_str(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractValidationError(f"{field}: debe ser string no vacío")
    return value


def require_positive_int(value: int, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ContractValidationError(f"{field}: debe ser entero positivo")
    return value


def require_nonnegative_int(value: int, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ContractValidationError(f"{field}: debe ser entero >= 0")
    return value


def require_optional_str(value: Optional[str], field: str) -> Optional[str]:
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise ContractValidationError(
            f"{field}: si se provee, debe ser string no vacío"
        )
    return value


def require_hash_like(value: str, field: str) -> str:
    """
    No se impone un algoritmo específico aquí (eso es política de
    canonical.py), solo forma mínima: string hexadecimal no vacío.
    """
    require_nonempty_str(value, field)
    if not all(c in "0123456789abcdefABCDEF" for c in value):
        raise ContractValidationError(f"{field}: no tiene forma de hash hexadecimal")
    return value
