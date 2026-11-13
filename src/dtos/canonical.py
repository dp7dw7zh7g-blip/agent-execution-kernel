"""
canonical.py
============
Serialización canónica para cálculo de hashes (P-064: Hash input must be
canonical form, never transport form). Ver sección "SERIALIZACIÓN
CANÓNICA (OPEN-006 resuelto)" del Registro Maestro.

Aproximación a RFC 8785 (JCS) usando solo stdlib:
- claves ordenadas alfabéticamente
- sin espacios
- UTF-8
- sin NaN/Infinity
- separators mínimos

No es una implementación JCS 100% conforme al spec en el borde de floats
extremos, pero es determinista y suficiente para problem_hash/content_hash
en este proyecto. Si más adelante se requiere conformidad estricta con
RFC 8785, sustituir _dumps por una librería JCS dedicada sin cambiar la
interfaz pública (canonical_json / content_hash).
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any


def _reject_non_finite(obj: Any) -> Any:
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            raise ValueError(
                "canonical_json: NaN/Infinity no son serializables de forma "
                "canónica (violaría reproducibilidad del hash)."
            )
    return obj


def _require_str_keys(obj: Any) -> None:
    """D-68: `json.dumps` convierte en silencio las claves no-str (1 -> "1", True -> "true"), de modo que {1: "a"} y {"1": "a"} daban el mismo
    texto y el mismo hash. Se rechazan (ValueError) en vez de colapsarlas. Recorrido iterativo: no añade profundidad de recursión."""
    stack = [obj]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            for k, v in o.items():
                if not isinstance(k, str):
                    raise ValueError(f"canonical_json: las claves de objeto deben ser str (llegó {type(k).__name__}: {k!r})")
                stack.append(v)
        elif isinstance(o, (list, tuple)):
            stack.extend(o)


def canonical_json(obj: Any) -> str:
    """
    Serializa `obj` a JSON canónico: claves ordenadas, sin espacios,
    UTF-8, determinista entre procesos y lenguajes.

    Esta es la ÚNICA función autorizada para producir el texto sobre el
    que se calculan problem_hash, input_hash, output_hash, content_hash,
    state_hash y context_hash (P-064).
    """
    _require_str_keys(obj)
    return json.dumps(
        obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,  # NaN/Infinity -> ValueError (antes se serializaban; _reject_non_finite no se invocaba)
        default=_json_default,
    )


def _json_default(obj: Any) -> Any:
    # Punto de extensión único para tipos no nativos (Enum, dataclass, etc.)
    # Los DTOs de este paquete implementan to_canonical_dict() y no deberían
    # llegar aquí, pero se deja como resguardo explícito en vez de fallar
    # silenciosamente con una representación no determinista.
    raise TypeError(
        f"canonical_json: tipo no serializable sin conversión explícita: "
        f"{type(obj).__name__}. Implementar to_canonical_dict() en el DTO."
    )


def content_hash(obj: Any, algorithm: str = "sha256") -> str:
    """
    Hash determinista de la forma canónica de `obj`.
    `obj` debe ser ya un dict/list/primitivo "canonicalizable" (usar
    to_canonical_dict() del DTO antes de llamar esto), nunca el DTO en sí.
    """
    text = canonical_json(obj)
    h = hashlib.new(algorithm)
    h.update(text.encode("utf-8"))
    return h.hexdigest()
