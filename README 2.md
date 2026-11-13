# agent-execution-kernel — UNAE 4.3S rev3 ↔ DTOS

Kernel local-first (Python + SQLite) para gobernar la ejecución de agentes de IA: reservas de recursos, autoridad con fencing, efectos idempotentes, reconciliación de resultados inciertos, outbox/inbox y auditoría encadenada por hash. No contiene ningún modelo ni LLM (el Reasoning Provider real sigue pendiente). Referencia experimental, no certificada para producción.

> Nombre anterior: `ia-personal` (hasta g0.8 r23). Se renombró porque el proyecto no es una IA personal sino un kernel de gobernanza de ejecución (D-69).


Estructura `src/` que separa los dos paquetes (que compartían nombres como `store.py`, `test_store.py`,
`README.md`, `SHA256SUMS.txt` y el esquema JSON) y el adaptador de integración.

```
src/
  unae/      UNAE 4.3S rev3: store.py, fsm.json, __init__.py
  dtos/      DTOS: enums, FSM, contratos, store, repositories, inbox/outbox, audit, schema.sql
  adapter/   Adaptador UNAE–DTOS (slice 0.1, rev. 0.2): adapter.py
  proactive/ Capa proactiva (g0.6 r13–r14): shadow.py (modo «solo registrar», base SQLite propia) y tee.py (observador de los eventos de UNAE); cliente del kernel, no lo modifica
schemas/     unae_dtos_message.schema.v1_1_1.json (CANÓNICO) y unae_dtos_message.schema.json (alias idéntico; el v1.1 original está en docs/contratos/)
tests/
  unae_tests/     unittest (incluye test_crash.py y crash_scenario.py)
  dtos_tests/     scripts con main() (no son unittest); incluye test_mandates.py (D-52) y test_append_only_replace.py (D-52c, D-52d) y test_mandate_rule_hash.py (D-53); test_open_retry.py (D-67: reintento acotado de la apertura)
  proactive_tests/  unittest: test_shadow.py (D-49), test_tee.py (D-50; incluye pruebas contra un Store real de UNAE)
  adapter_integration_tests/  integración: test_bridge.py (AT-05, 06, 07, vías A/B) contra Stores reales
  adapter_tests/  unittest: test_envelope.py (AT-02, 03, 04, 15 + regresiones) y test_decoupling.py (D-63: frontera con los Stores y LocalTx)
  integrity_tests/  manifiesto SHA-256 del release, gobernanza de esquemas y política de conexión SQLite medida en los cuatro almacenes (D-63) y selección de `--skip-crash` del runner (g0.8 r23); test_open_retry_stores.py (D-76) y test_open_hardening_stores.py (D-77: migración atómica del adaptador, busy_timeout exacto, rollback seguro de la capa proactiva); test_windows_portability.py (D-78: guardia que lee el código de las pruebas y falla si vuelve `with sqlite3.connect(...)` sin cerrar o un nombre de archivo reservado de Windows)
  patch_tests/    el código == base + parches 0001…0010, 0012, 0013, 0014, 0015, 0016, 0017, 0018, 0019, 0020, 0021, 0022, 0023 y 0024 en orden (el 0011 está reservado)
docs/        contratos (v1.0, v1.1), auditoría, decisiones, README originales
meta/        resultados de pruebas y checksums originales; MANIFEST.md; SHA256SUMS_v1_1_1.txt (release; SHA256SUMS.txt es copia)
run_tests.py
```

Los directorios de pruebas no se llaman `unae/` ni `dtos/` a propósito: así no ocultan a los paquetes al importar.

## Ejecutar

```sh
python run_tests.py unae            # todo, incluido el barrido SIGKILL (lento)
python run_tests.py unae --skip-crash   # en Windows test_crash se omite solo (necesita SIGKILL, que no existe allí)
python run_tests.py dtos
python run_tests.py integrity       # manifiesto y esquemas
python run_tests.py patches         # serie de parches (requiere `patch`)
python run_tests.py all
python run_tests.py integrity --fast   # carga reducida en las pruebas de WAL (D-79); sin --fast, la completa (la del release)
```
También se activa con la variable `AEK_TESTS_FAST=1` (en Windows: `set AEK_TESTS_FAST=1`).

CI: `.github/workflows/ci.yml` corre `python run_tests.py all --strict` en Linux (3.11, 3.12, 3.13) y Windows (3.12), sin `--fast` (D-81).
Auditoría de solo lectura de la evidencia de una base DTOS existente (no modifica nada; código de salida 1 si hay violaciones):
```sh
PYTHONPATH=src python -m dtos.scripts.audit_historical_data RUTA.db [--json]
```
Requiere Python ≥ 3.11 (`StrEnum`) y SQLite ≥ 3.35 (`DROP COLUMN` en las pruebas de migración).

Lee `meta/MANIFEST.md` antes de confiar en este árbol: indica qué archivos son originales,
cuáles fueron transcritos, cuáles reconstruidos y qué cosas no coinciden con los documentos.
