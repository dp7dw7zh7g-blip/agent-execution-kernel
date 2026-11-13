"""Fencing en el lado del proveedor (Hallazgo 4, D-56): implementación de REFERENCIA de la puerta que un proveedor debe aplicar.

El despachador entrega en `request` el `fencing_token` con el que se despacha y su alcance (`fencing_scope`: la Task de UNAE, porque la
autoridad y su token son por Task). Un proveedor que respete el fencing guarda, por alcance, el token MÁS ALTO que ha visto y rechaza
cualquier petición con un token ESTRICTAMENTE menor: es de un líder que ya fue reemplazado.

Reglas:
- Estrictamente menor se rechaza; IGUAL se acepta. Un reintento de la misma operación, o una segunda operación bajo la misma autoridad,
  llegan con el mismo token y no pueden rechazarse (rechazar «menor o igual» rompería la idempotencia).
- Un token mayor se acepta y ELEVA el máximo: desde ese momento el líder anterior queda rechazado.
- Los alcances son independientes (el token 1 de otra Task no compite con el token 2 de ésta).

LÍMITES (es una referencia, no una garantía): vive en memoria y se pierde al reiniciar; un proveedor real debe persistir el máximo y hacer la
comprobación y la aplicación del efecto de forma ATÓMICA (en la misma transacción o con un compare-and-set en su sistema), porque comprobar
y luego aplicar por separado reabre la ventana. Que un proveedor real lo haga no se puede imponer desde este repositorio.
"""
from __future__ import annotations

import threading
from typing import Any, Mapping

from .adapter import ProtocolError, validate_fencing_token


class StaleFencingToken(ProtocolError):
    """El token de la petición es menor que el más alto visto para ese alcance: líder reemplazado. El efecto NO debe aplicarse."""

    def __init__(self, scope: str, token: int, highest: int):
        super().__init__(f"fencing_token {token} obsoleto para {scope!r}: el proveedor ya vio {highest}")
        self.scope, self.token, self.highest = scope, token, highest


class FencingGate:
    def __init__(self) -> None:
        self._highest: dict[str, int] = {}
        self._lock = threading.Lock()

    def admit(self, request: Mapping[str, Any]) -> int:
        """Admite o rechaza una petición. Devuelve el token admitido; lanza `StaleFencingToken` si es obsoleto y `ProtocolError` si la
        petición no trae un `fencing_token` entero ni un `fencing_scope` no vacío (una petición sin fencing no se admite en silencio)."""
        scope = request.get("fencing_scope")
        if not isinstance(scope, str) or not scope.strip():
            raise ProtocolError("la petición no trae fencing_scope: no se puede aplicar el fencing")
        if "fencing_token" not in request:
            raise ProtocolError("la petición no trae fencing_token: no se puede aplicar el fencing")
        token = validate_fencing_token(request["fencing_token"])
        with self._lock:
            highest = self._highest.get(scope, -1)
            if token < highest:
                raise StaleFencingToken(scope, token, highest)
            self._highest[scope] = token
        return token

    def highest(self, scope: str) -> int | None:
        with self._lock:
            return self._highest.get(scope)
