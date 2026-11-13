# CLAUDE.md — agent-execution-kernel

Kernel de gobernanza de ejecución para un compañero digital IA (antes `ia-personal`, D-69).
Piezas: **UNAE** (diario de ejecución, reservas, recibos firmados, outbox), **DTOS** (ledger, auditoría, FSM, mandatos), **adapter** (frontera de protocolo UNAE↔DTOS) y **proactive** (modo «solo registrar»).
Todo es Python ≥ 3.11 sobre SQLite (WAL). Release v1.1.1, generación g0.8 (ver `meta/VERSION.md`).

## Estructura

- `src/unae/`, `src/dtos/`, `src/adapter/`, `src/proactive/`
- `tests/{unae,dtos,proactive,adapter,adapter_integration,integrity,patch}_tests/` — los directorios de pruebas no se llaman `unae/` ni `dtos/` a propósito (no ocultar paquetes).
- `patches/` — serie 0001…0010 y 0012…0024 (el 0011 está reservado). `patches/README.md` lleva el SHA-256 de cada parche.
- `docs/decisiones/DECISIONES_v1_1_1.md` (decisiones D-NN), `docs/contratos/`, `meta/` (VERSION, MANIFEST, SHA256SUMS), `schemas/`, `config/`.

## Comandos (desde la raíz)

```sh
python run_tests.py unae --skip-crash   # omite solo test_crash (barrido SIGKILL, lento)
python run_tests.py dtos                # scripts con main(), NO son unittest
python run_tests.py integrity           # manifiesto SHA-256, esquemas, política de conexión, portabilidad Windows
python run_tests.py patches             # requiere GNU `patch`
python run_tests.py all [--strict] [--fast]
```

- `--fast` (o `AEK_TESTS_FAST=1`, D-79): carga reducida en las pruebas de WAL. Para el ciclo diario; **la corrida de release es la completa (sin `--fast`)**.
- `--strict` falla con omisiones no permitidas (p. ej. `SchemaConformance` sin `jsonschema`).
- CI: `.github/workflows/ci.yml` (GitHub Actions) corre `python run_tests.py all --strict` en Linux (3.11–3.13) y Windows (3.12), sin `--fast` (D-81). No se ha ejecutado aún en GitHub: la primera corrida es su verificación.
- Un comando o flag desconocido termina con código 2 (fail closed): no lo relajes.

## Reglas de trabajo

- **unittest, no pytest.** No añadas dependencias nuevas.
- Cada cambio de comportamiento lleva pruebas que **fallan sin el cambio** (y, si procede, un control negativo). Verifica eso antes de darlo por bueno.
- **Parches (`patches/`)** solo para los archivos que son originales parchados: de DTOS `store.py`, `effect_repository.py`, `canonical.py`, `schema.sql`, `registries.py`, `enums.py`, `audit.py`, `inbox_outbox.py`; de UNAE `store.py`. `adapter/adapter.py` y `proactive/shadow.py` **no** están en la serie (llevan el mismo cambio con código propio, D-63).
- Un parche nuevo se genera sobre el árbol con los anteriores aplicados, se registra en `patches/README.md` (fila, hash, «Qué NO se verificó») y en `tests/patch_tests/test_patch_series.py`.
- **Manifiesto**: tras cambiar algo en `src`, `config`, `schemas` o `tests`, regenéralo (el runner y `run_tests.py` quedan fuera de su alcance):
  ```sh
  find src config schemas tests -type f -not -path '*/__pycache__/*' -not -name '*.pyc' -exec sha256sum {} + | LC_ALL=C sort -k 2 > meta/SHA256SUMS_v1_1_1.txt
  cp meta/SHA256SUMS_v1_1_1.txt meta/SHA256SUMS.txt
  ```
  Borra `__pycache__` antes. En Windows no hay `find`/`sha256sum`: usa WSL/Git Bash o un script equivalente.
- **Cada entrega** (revisión rNN): decisión D-NN en `DECISIONES_v1_1_1.md`, entrada en `meta/MANIFEST.md`, `meta/VERSION.md`, `README.md` si cambia el uso, zip `agent-execution-kernel_g0_8_rNN_AAAA-MM-DD.zip` con raíz `agent-execution-kernel/`. Entrega también el diff respecto a la revisión anterior y compruébalo aplicándolo sobre una copia limpia.
- No modifiques `docs/contratos/*_v1_1.*` ni `docs/contratos/unae_dtos_message.schema.v1_1_original.json`: son la línea base inmutable (hash fijado en `test_manifest.py`).

## Decisiones del usuario que no se deben revertir

- **No** se cambia `canonical_json` a JCS (rompería hashes persistidos); `fencing_token` es entero con tope 2^53−1. JCS solo en un versionado mayor de DTOS.
- **Sin scripts que muten o trunquen historial**: solo auditoría de solo lectura (`audit_historical_data.py`). No hay despliegue con datos previos a los parches.
- Evidencia autocontenida en `effects` y `effect_reconciliations`: no hay tabla de evidencias por hash.
- Esquema canónico = `schemas/unae_dtos_message.schema.v1_1_1.json`; `schema.json` es copia idéntica (sin wrapper `$ref`).
- Ante discrepancia, **manda el `.md` del contrato sobre el `.docx`**.
- **Las FSM de UNAE y DTOS son separadas** (contrato I-01, I-04): no equiparar estados ni comparar sus tablas (D-80).
- Cada almacén tiene su propio código de conexión (D-63): se estandariza por comportamiento y pruebas cruzadas, no con un módulo común.
- `UNKNOWN` se resuelve solo con evidencia y autoridad explícita (P-013, P-061); el outbox es «al menos una vez» y no deduplica efectos externos.

## Entorno del usuario (condiciona las pruebas)

- Windows 10, Python 3.15.2, portátil de 2 GB de RAM; disco interno HDD y una USB Kingston. Las pruebas son lentas por fsync (`synchronous=FULL`), no por CPU ni RAM: reduce commits, no cómputo.
- Por tanto: cierra siempre las conexiones SQLite (`with closing(sqlite3.connect(...)) as c, c:`; el `with` solo no cierra), no uses nombres reservados de Windows (`CON`, `PRN`, `AUX`, `NUL`, `COM1`…`LPT9`) en archivos de prueba (lo vigila `test_windows_portability.py`), y no dependas de `fork`, `flock` ni `SIGKILL`.
- `test_crash` (barrido SIGKILL) solo se ejecuta en Linux/macOS; en Windows se omite. Los tiempos de `adapter-integration` en Windows (~67 min con r32) son un pendiente conocido.
- Pruebas sensibles al tiempo: usa reloj simulado (`mock.patch("time.time")`) en vez de `sleep` reales.

## Cómo informar

- En español. Distingue siempre **lo medido** de **lo inferido** y di qué **no** se ejecutó (Windows, `--strict`, `jsonschema`, crash tests).
- No afirmes una mejora de tiempo sin haberla medido en el equipo del usuario; en Linux el fsync es casi gratis y no sirve de cronómetro.
- Si una propuesta anterior resulta equivocada, corrígela de forma explícita.
