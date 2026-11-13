"""Configuración de transporte por proveedor (punto 7 del plan: timeouts). Slice 0.5.

Los plazos de espera NO viven en el código: los declara quien conoce cada proveedor en un archivo JSON externo (`config/transport_config.example.json`
muestra la forma; sus valores son ilustrativos) y se cargan UNA vez al arrancar el proceso.

- Validación estricta: solo la clave `providers`; cada proveedor con exactamente `connect_timeout_s`, `read_timeout_s` y `total_timeout_s`; números
  finitos y > 0 (ni bool ni texto); `total_timeout_s` >= max(connect, read); claves JSON duplicadas, NaN e Infinity rechazados.
- Sin valores por defecto: un proveedor que no figura en la configuración provoca `ConfigurationError` ANTES de tocar ningún Store ni llamar al proveedor.
- Inmutable una vez cargada. Para pruebas se construye con `TransportConfig.from_dict({...})`, incluido `{"providers": {}}`.
- El despachador solo IMPONE `total_timeout_s` (su propio plazo sobre la llamada). `connect_timeout_s` y `read_timeout_s` se entregan al código del
  proveedor en `request["timeouts"]`; aplicarlos a sus sockets es cosa de quien implementa la llamada.
- La clave de cada proveedor es el `provider_id` de UNAE (el de `DispatchAuthorization`), no el nombre de proveedor de DTOS.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from .adapter import ProtocolError, _ident

TIMEOUT_FIELDS = ("connect_timeout_s", "read_timeout_s", "total_timeout_s")
# L-3 (g0.8 r12, D-62): tope superior de cualquier plazo. 3600 s (1 h) es un valor propuesto, no una medición: un plazo mayor deja un hilo del proveedor
# y una operación incierta abiertos demasiado tiempo. Un despliegue que de verdad necesite más debe cambiar esta constante a propósito.
MAX_PROVIDER_TIMEOUT_S = 3600.0


class ConfigurationError(ValueError):
    """La configuración de transporte falta o es inválida. Es un error de despliegue, no de protocolo."""


@dataclass(frozen=True)
class ProviderTimeouts:
    connect_timeout_s: float
    read_timeout_s: float
    total_timeout_s: float

    def __post_init__(self) -> None:
        for name in TIMEOUT_FIELDS:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ConfigurationError(f"{name} debe ser un número finito mayor que 0, no {value!r}")
            if value > MAX_PROVIDER_TIMEOUT_S:
                raise ConfigurationError(f"{name} no puede superar {MAX_PROVIDER_TIMEOUT_S:g} s (MAX_PROVIDER_TIMEOUT_S), no {value!r}")
            object.__setattr__(self, name, float(value))
        if self.total_timeout_s < max(self.connect_timeout_s, self.read_timeout_s):
            raise ConfigurationError("total_timeout_s no puede ser menor que connect_timeout_s ni que read_timeout_s (alguno sería inalcanzable)")

    def as_dict(self) -> dict[str, float]:
        return {name: getattr(self, name) for name in TIMEOUT_FIELDS}


@dataclass(frozen=True)
class TransportConfig:
    providers: Mapping[str, ProviderTimeouts]

    def __post_init__(self) -> None:
        clean: dict[str, ProviderTimeouts] = {}
        for provider_id, timeouts in dict(self.providers).items():
            try:
                _ident(provider_id, "provider_id")
            except ProtocolError as exc:
                raise ConfigurationError(f"identificador de proveedor inválido {provider_id!r}: {exc}") from exc
            if not isinstance(timeouts, ProviderTimeouts):
                raise ConfigurationError(f"{provider_id}: se esperaba ProviderTimeouts")
            clean[provider_id] = timeouts
        object.__setattr__(self, "providers", MappingProxyType(clean))

    @classmethod
    def from_dict(cls, data: Any) -> "TransportConfig":
        if not isinstance(data, dict) or set(data) != {"providers"} or not isinstance(data["providers"], dict):
            raise ConfigurationError('la configuración debe ser un objeto con la única clave "providers" (un objeto)')
        providers: dict[str, ProviderTimeouts] = {}
        for provider_id, entry in data["providers"].items():
            if not isinstance(entry, dict):
                raise ConfigurationError(f"{provider_id}: se esperaba un objeto con {', '.join(TIMEOUT_FIELDS)}")
            missing, extra = set(TIMEOUT_FIELDS) - set(entry), set(entry) - set(TIMEOUT_FIELDS)
            if missing or extra:
                raise ConfigurationError(f"{provider_id}: faltan {sorted(missing)} / no permitidas {sorted(map(str, extra))}")
            providers[provider_id] = ProviderTimeouts(**entry)
        return cls(providers)

    @classmethod
    def from_file(cls, path: str | Path) -> "TransportConfig":
        try:
            text = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigurationError(f"no se pudo leer la configuración de transporte {path}: {exc}") from exc

        def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            out: dict[str, Any] = {}
            for key, value in pairs:
                if key in out:
                    raise ConfigurationError(f"clave JSON duplicada: {key!r}")
                out[key] = value
            return out

        def reject_constant(name: str) -> Any:
            raise ConfigurationError(f"valor no finito en el JSON: {name}")
        try:
            data = json.loads(text, object_pairs_hook=no_duplicates, parse_constant=reject_constant)
        except json.JSONDecodeError as exc:
            raise ConfigurationError(f"JSON inválido en {path}: {exc}") from exc
        return cls.from_dict(data)

    def timeouts_for(self, provider_id: str) -> ProviderTimeouts:
        try:
            return self.providers[provider_id]
        except (KeyError, TypeError):
            raise ConfigurationError(f"el proveedor {provider_id!r} no figura en la configuración de transporte: no hay plazo declarado") from None
