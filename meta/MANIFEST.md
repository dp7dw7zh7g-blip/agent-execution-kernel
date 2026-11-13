# Manifiesto de procedencia

## Copiados tal cual del proyecto (sin cambios de contenido)
- `src/dtos/*.py` salvo `schema.sql` (enums, validation, canonical, contracts, registries, fsm, effect_fsm,
  cognitive_cycle_fsm, recovery_bootstrap_fsm, store, repository, effect_repository,
  recovery_authority_repository, inbox_outbox, audit, `__init__`)
- `src/adapter/adapter.py` (original del slice 0.1; ver «Modificados» abajo)
- `schemas/unae_dtos_message.schema.json` (contenido idéntico al `unae_dtos_message_schema.json` del proyecto;
  solo cambia el nombre al que cita el contrato y `SHA256SUMS`)
- `docs/**` y `meta/RESULTADOS_PRUEBAS_UNAE_4_3N.txt`, `meta/SHA256SUMS_contrato_v1_1_original.txt`
- Pruebas de UNAE: concurrency, idempotent_retry, migrations, outbox, perf_options, release, signed_receipts

## Con cambios mínimos de rutas/imports
- `tests/dtos_tests/*.py`: `from .x import` → `from dtos.x import` (antes se ejecutaban con `python -m dtos.test_x`).
- `tests/unae_tests/crash_scenario.py`: la ruta de `sys.path` ahora apunta a `src/`.
- `tests/unae_tests/test_semantics.py`: `fsm.json` se localiza desde el paquete `unae`, no desde `tests/..`.

## Transcritos del texto del proyecto (NO estaban en disco; en disco solo había la versión homónima de DTOS)
- `src/unae/store.py`
- `tests/unae_tests/test_store.py`
- `docs/readme/README_adaptador_slice_0_1.md`
`src/unae/store.py` coincide byte a byte (SHA-256 `3af567b5…`, 58 513 bytes) con el `store.py` aportado por el usuario el 2026-10-25; el resto de la transcripción sigue sin hash de referencia, con las 51 pruebas de UNAE como única evidencia.

## Reconstruidos (no existían entre los archivos)
- `src/unae/__init__.py` — solo reexporta lo que importan las pruebas.
- `src/unae/fsm.json` — generado desde `fsm_table()` de `store.py`; coincide con la FSM del contrato v1.0 §6.
- `src/adapter/__init__.py`, `run_tests.py`, `README.md`, este manifiesto.

## Discrepancias detectadas (no corregidas: no modifiqué contenido de los originales)
1. `SHA256SUMS` lista `Contrato_..._v1.1.docx` con hash `e2e609ea…`, pero el .docx del proyecto tiene
   `b7adaf1f…`. El .md, DECISIONES y el esquema JSON sí coinciden. El docx v1.1 del proyecto no es el que se checksumeó.
2. Adaptador vs. esquema JSON no están alineados: el adaptador exige UUIDv4 en `message_id` y en los IDs, exige
   `effect_attempt_id` en RECONCILIATION_RESULT, `operation_id` y `result` en RECOVERY_NOTICE; no valida el enum
   de `result`, ni `additionalProperties:false`, ni `uniqueItems`. El esquema solo pide `minLength: 1` en los IDs y
   exige `evidence_refs` no vacío en RECOVERY_NOTICE, cosa que el adaptador no comprueba.
3. El README del adaptador manda ejecutar `tests/`, pero no había pruebas del adaptador en el proyecto.
4. El README de UNAE (4.3N) está desactualizado respecto al código (menciona SCHEMA_VERSION=2 y 55 pruebas en
   distintos párrafos); el código es v4.

## Modificados / añadidos tras el análisis (2026-10-25)
- `src/adapter/adapter.py` rev. 0.2 y `src/adapter/__init__.py`: identificadores opacos (ver D-06 revisada), `payload_sha256` definido sobre el mensaje sin ese campo,
  validación estricta del sobre, conexiones cerradas. Ya no es copia del original.
- `schemas/unae_dtos_message.schema.v1_1_1.json`: nuevo; el esquema v1.1 original se conserva intacto.
- `docs/decisiones/DECISIONES_v1_1_1.md`: nuevo (D-06…D-12 y abiertos).
- `tests/adapter_tests/test_envelope.py`: nuevo (25 pruebas; 1 omitida sin `jsonschema`).
- `meta/SHA256SUMS.txt`: regenerado para este árbol.
- Sin cambios: `src/unae/`, `src/dtos/` y el resto de originales. Sigue pendiente recuperar `unae/store.py` y `dtos/schema.sql` originales.
- Conocido sin corregir: `run_tests.py unae --skip-crash` también omite `test_concurrency.py` (patrón `test_[!c]*.py`).

## Actualización: schema.sql real de DTOS (2026-10-25)
- `src/dtos/schema.sql` ahora es el aportado por el usuario (14 576 bytes, SHA-256 `9b88a721…`, 31 sentencias CREATE: 21 tablas, 8 triggers, 2 índices únicos). Sustituye al reconstruido; no es copia del reconstruido.
  Se descartó otro `schema.sql` de 660 bytes (cabecera «ANDAMIAJE DE PRUEBAS»: solo `effect_attempts` y `effects`).
- Con el schema real: 7 módulos de pruebas DTOS pasan (124 comprobaciones), adaptador 25 OK (1 omitida), UNAE 51 OK (sin crash/concurrency).
- Diferencias con el reconstruido: faltaban 11 tablas (components, contracts, providers, policies, operations, reasoning_runs, evidence, recovery_runs, invariants, projections, retention);
  `effects` es append-only por trigger; `effect_attempts.version` no lleva `CHECK (version >= 1)`.

## Puente a los Stores (2026-10-25)
- `src/adapter/bridge.py` (nuevo) y cambios en `adapter.py` (`reconciliation_route_log`, `quarantine`, `log_route`, `route_log`).
- `tests/adapter_integration_tests/test_bridge.py` (nuevo): 25 pruebas contra Store UNAE y SQLiteStore DTOS reales, reportadas aparte de las unitarias (AT-16).
- `run_tests.py`: nueva suite `adapter-integration`; `all` las ejecuta por separado.

## Identificadores opacos (2026-10-25)
- `adapter.py`: `_ident` sustituye a la validación UUIDv4; `schemas/unae_dtos_message.schema.v1_1_1.json` y `DECISIONES_v1_1_1.md` (D-06 revisada) actualizados.
- Pruebas: `Regression_OpaqueIds` (unitarias) y `OpaqueIdsEndToEnd` (integración con IDs «op-A1»). Unitarias 26 (1 omitida), integración 26.

## Recibos firmados (2026-10-25)
- `bridge.py`: `require_signed_receipts` (por defecto True) y reenvío idempotente de recibos en estados posteriores (D-17, D-18).
- `tests/adapter_integration_tests/test_bridge.py`: clase `AT09_SignedReceipts` (7 pruebas); total integración 33.

## Despacho (2026-10-25)
- `src/adapter/dispatch.py` (nuevo): `Dispatcher`, `DispatchAuthorization`, `DispatchBlocked`, `DispatchResult`; `adapter.py`: `list_correlations`.
- `meta/VERSION.md` (nuevo): generación/revisión y convención del nombre del zip.
- `tests/adapter_integration_tests/test_dispatch.py` (nuevo, 16 pruebas): AT-01, AT-08, AT-12, AT-13. Total integración 49.

## Reconciliador (2026-10-25, g0.4 r1)
- `src/adapter/reconcile.py` (nuevo): `Reconciler`, `ReconcileRejected`, `ReconcileOutcome`; `adapter.py`: tabla `reconciliation_log`, `log_reconciliation`, `reconciliation_history`.
- `tests/adapter_integration_tests/test_reconcile.py` (nuevo, 16 pruebas): AT-10, AT-11, AT-14. Total integración 65.
- `meta/VERSION.md`: g0.4 r1.

## g0.4 r2 (2026-10-25)
- `docs/decisiones/DECISIONES_v1_1_1.md`: D-29 (decisión del usuario sobre la estrategia de reconciliación); `reconcile.py`: solo docstring; `meta/VERSION.md`: r2.
- Pendiente de mantenimiento (sin tocar): regenerar el .docx v1.1 con el hash correcto y actualizar el README de UNAE.

## g0.4 r3 (2026-10-25)
- `reconcile.py`: `recover_after_restart` y `_ensure_ledger`; `adapter.py`: `reconciliation_log.evidence_json` (con migración), `unfinished_reconciliations`.
- `test_reconcile.py`: clase `AtomicityGap` (7 pruebas); `test_envelope.py`: clase `CanonicalParity` (2 pruebas). Totales: unitarias 28 (1 omitida), integración 72.
- `docs/propuestas/PROPUESTAS_UPSTREAM.md` (nuevo): cinco propuestas para los originales, sin aplicar.

## g0.4 r4 (2026-10-25)
- `reconcile.py`: `reconciler_id` obligatorio (cambio de API), `DTOS_PARTIAL_APPLY`; `adapter.py`: columna `reconciler_id` con migración; `bridge.py`: usa `get_effects_by_attempt` si existe, con respaldo a `_conn`.
- `patches/` (nuevo): 0001 y 0002 para DTOS (NO aplicados), `test_dtos_atomic_reconcile.py`, README. `docs/propuestas/PROPUESTAS_UPSTREAM.md` reescrito.
- Pruebas: unitarias 28 (1 omitida), integración 79; con los parches aplicados el resultado es el mismo.

## g0.4 r5 (2026-10-25): parches adoptados en DTOS (D-34)
Archivos de DTOS que YA NO son copia del original (SHA-256, primeros 16 caracteres):

| Archivo | Original | Actual |
|---|---|---|
| `src/dtos/store.py` | `cc355b3405e182a8` | `159638de83cd0a96` |
| `src/dtos/effect_repository.py` | `8016df2bdf47093a` | `57cc87f63044aa2f` |
| `src/dtos/canonical.py` | `7cf9f2344353bf20` | `90ce6b07e7c0fe32` |

- Registro exacto de los cambios en `patches/` (README incluido). `tests/dtos_tests/test_effect_reconcile_atomic.py` (nuevo) entra en `run_tests.py dtos`: ahora son 8 módulos.
- El resto de `src/dtos/` y todo `src/unae/` siguen siendo copia de los originales.

## g0.4 r6 (2026-10-26): evidencia en el ledger de DTOS (D-35, parche 0003)
Archivos de DTOS que YA NO son copia del original (SHA-256, primeros 16 caracteres; «original» = el aportado por el usuario):

| Archivo | Original | r5 | Actual (r6) |
|---|---|---|---|
| `src/dtos/schema.sql` | `9b88a721264ebed1` | `9b88a721264ebed1` | `9b886cff5366a455` |
| `src/dtos/store.py` | `cc355b3405e182a8` | `159638de83cd0a96` | `91a0dd14c8975775` |
| `src/dtos/effect_repository.py` | `8016df2bdf47093a` | `57cc87f63044aa2f` | `bad4b28e402baa79` |
| `src/dtos/canonical.py` | `7cf9f2344353bf20` | `90ce6b07e7c0fe32` | `90ce6b07e7c0fe32` |

- `patches/0003-dtos-evidencia-en-el-ledger.patch` (nuevo; aplica limpio sobre r5 y reproduce el árbol actual byte a byte).
- `adapter/reconcile.py`: la reparación del ledger escribe la evidencia de la bitácora. Pruebas: DTOS 8 módulos, unitarias 28 (1 omitida), integración 83, UNAE 56 (1 omitida).
- `schema.sql` era copia byte a byte del original hasta r5; con r6 deja de serlo.

## g0.4 r7 (2026-10-26): lectura pública en UNAE (D-36, parche 0004)
- `src/unae/store.py` YA NO es copia del original: SHA-256 `3af567b53a88f11e` (original aportado) → `9b08606fdc50e714`. Cambio: un método (`get_operation`), 10 líneas.
- `patches/0004-unae-get-operation.patch` (aplica limpio sobre r6 y reproduce el árbol actual byte a byte); `tests/unae_tests/test_get_operation.py` (nuevo, 6 pruebas; fallan sin el parche); `bridge.py` usa `get_operation` con respaldo.
- Pruebas: UNAE 62 (1 omitida), unitarias 28 (1 omitida), integración 86, DTOS 8 módulos. Con el cambio de r7, la restricción «originales inmutables» (D-05) queda levantada en 5 archivos: 4 de DTOS y 1 de UNAE.

## g0.5 r1 (2026-10-26): configuración de transporte (D-37…D-40)
- `src/adapter/transport.py` (nuevo); `dispatch.py`: `Dispatcher(bridge, transport=None)`, `timeout_s` opcional (con configuración se rechaza); `config/` (nuevo): `transport_config.example.json` y README.
- Pruebas: `tests/adapter_tests/test_transport_config.py` (nuevo, 13 pruebas unitarias) y clase `ConfiguredTimeouts` en `test_dispatch.py` (6 de integración). Totales: UNAE 62 (1 omitida), unitarias 41 (1 omitida), integración 92, DTOS 8 módulos.
- Sin cambios en DTOS ni en UNAE respecto a r7 de g0.4 (los 5 archivos modificados son los mismos).

## g0.6 r1 (2026-10-26): política REJECTED, tope del token, retención y documentación (D-41…D-45)
- `adapter.py`: `MAX_FENCING_TOKEN` (2^53-1), `compact_inbox`, `list_quarantine`, `review_quarantine`, `purge_reviewed_quarantine`, `quarantine_backlog`, tablas `quarantine_review` y `quarantine_purge_log`, columna `adapter_inbox.compacted_at` (con migración). `reconcile.py`: `rejected_strategies`. Esquema 1.1.1: `maximum` del `fencing_token`.
- `tests/adapter_tests/test_retention.py` (nuevo, 21 pruebas); clases `JcsCompliance` y ajustes en `test_envelope.py`; clase `RejectedPolicy` en `test_reconcile.py`. Totales: UNAE 62 (1 omitida), unitarias 62 (1 omitida), integración 99, DTOS 8 módulos.
- `docs/contratos/Contrato_Interoperabilidad_UNAE_DTOS_v1_1.docx`: REGENERADO como documento Word real a partir del `.md`. Antes era texto plano con extensión .docx (SHA-256 `b7adaf1f4f79644e`); ahora `879689bccee8468c`. El hash que figuraba en el SHA256SUMS original (`e2e609ea…`) no se puede reproducir. **[CORREGIDO en r7: el original `e2e609ea…` apareció y está restaurado; el regenerado `879689bc…` fue retirado.]** `Contrato_..._v1_0.docx` y `Auditoria_..._v1_0.docx` siguen siendo texto plano.
- `docs/readme/README_UNAE_4_3N.md` actualizado (estado actual, archivo de UNAE modificado, versión de esquema 4, 62 pruebas).
- Sin cambios en DTOS ni en UNAE respecto a g0.4 r7: los 5 archivos modificados y sus hashes son los mismos.

## g0.6 r2 (2026-10-26): parte 1 de 4 — historial de reconciliaciones en DTOS (D-46, parche 0005)
Archivos de DTOS que ya no son copia del original (SHA-256, primeros 16 caracteres; sustituye a las tablas anteriores):

| Archivo | Original | Actual |
|---|---|---|
| `src/dtos/schema.sql` | `9b88a721264ebed1` | `7252b482a1391844` |
| `src/dtos/store.py` | `cc355b3405e182a8` | `6a05c4221eafd47e` |
| `src/dtos/effect_repository.py` | `8016df2bdf47093a` | `5b027aee4b615c25` |
| `src/dtos/canonical.py` | `7cf9f2344353bf20` | `90ce6b07e7c0fe32` |
| `src/unae/store.py` | `3af567b53a88f11e` | `9b08606fdc50e714` |

- `patches/0005-dtos-historial-de-reconciliaciones.patch` (nuevo; aplica limpio sobre g0.6 r1 y reproduce el árbol actual). Pruebas: ampliada `tests/dtos_tests/test_effect_reconcile_atomic.py` y clase `PartialEvidenceInDtos` en `test_reconcile.py`.
- Totales: UNAE 62 (1 omitida), unitarias 62 (1 omitida), integración 100, DTOS 8 módulos.
- Entrega en partes: pendientes la 2 (tope de evidencia), la 3 (JCS, a decidir) y la 4 (integridad frente a los originales).

## g0.6 r3 (2026-10-26): parte 2 de 4 — tope de tamaño de la evidencia (D-47, parche 0006)
Archivos que ya no son copia del original (SHA-256, primeros 16 caracteres; sustituye a la tabla anterior):

| Archivo | Original | Actual |
|---|---|---|
| `src/dtos/schema.sql` | `9b88a721264ebed1` | `4ae43839d0405c26` |
| `src/dtos/store.py` | `cc355b3405e182a8` | `8fff4128df96ead6` |
| `src/dtos/effect_repository.py` | `8016df2bdf47093a` | `191181c4094aabe0` |
| `src/dtos/canonical.py` | `7cf9f2344353bf20` | `90ce6b07e7c0fe32` |
| `src/unae/store.py` | `3af567b53a88f11e` | `9b08606fdc50e714` |

- `patches/0006-dtos-tope-de-tamano-de-la-evidencia.patch` (nuevo; aplica limpio sobre g0.6 r2 y reproduce el árbol actual). `adapter.py`/`reconcile.py`: constante `MAX_EVIDENCE_SIZE_BYTES` y rechazo `EVIDENCE_TOO_LARGE`.
- Pruebas: ampliada `tests/dtos_tests/test_effect_reconcile_atomic.py` (166 comprobaciones en DTOS) y clase `PartialEvidenceAndSizeCap` en `test_reconcile.py`. Totales: UNAE 62 (1 omitida), unitarias 62 (1 omitida), integración 104, DTOS 8 módulos.
- Pendientes: parte 3 (JCS, a decidir) y parte 4 (integridad frente a los originales).

## g0.6 r4 (2026-10-27): parte 4 de 4 — integridad frente a los originales, trazabilidad de parches y esquema canónico (release v1.1.1)

### Esquemas activos
| Archivo | Papel | SHA-256 (16 primeros) |
|---|---|---|
| `schemas/unae_dtos_message.schema.v1_1_1.json` | **CANÓNICO** (única fuente de verdad del sobre v1.1.1; `$id` `urn:unae-dtos:message:1.1.1`) | `af282d8f351f4ee7` |
| `schemas/unae_dtos_message.schema.json` | alias de compatibilidad: copia byte a byte del canónico (vigilada por `tests/integrity_tests/test_schema_governance.py`) | `af282d8f351f4ee7` |
| `docs/contratos/unae_dtos_message.schema.v1_1_original.json` | esquema v1.1 original, archivado (no se carga en ningún sitio) | `f5045214b96984e7` |

- Antes de esta revisión `schemas/unae_dtos_message.schema.json` era el v1.1 original. Al convertirlo en alias se movió, sin cambios, a `docs/contratos/` con el nombre de arriba: conserva el hash de la línea base (`f5045214…`) y «el esquema v1.1 original se conserva intacto» sigue siendo cierto.
- `src/adapter/schema.py` (nuevo): `SCHEMA_PATH` apunta explícitamente al v1.1.1 y `load_canonical_schema()` lo carga comprobando `$id`. **La validación del sobre en ejecución (`adapter.validate_envelope`) sigue siendo código propio y no lee el JSON**; la deriva entre ambos la detectan `SchemaAlignment` (siempre) y `SchemaConformance` (solo con `jsonschema` instalado; en este entorno se omite).
- `src/adapter/transport.py` y `src/dtos/inbox_outbox.py` NO se modificaron: el primero solo carga la configuración de plazos y el segundo es la bandeja de eventos de DTOS (original sin parchear); ninguno interpreta el sobre de mensaje.

### Manifiestos y hashes vigentes
| Archivo | SHA-256 |
|---|---|
| `meta/SHA256SUMS_v1_1_1.txt` (vigente desde g0.6 r10: 65 archivos de `src`, `config`, `schemas`, `tests`; `SHA256SUMS.txt` es copia idéntica; r7 y r8: 64 archivos, hash `9e7d0b72…`) | `efa29ba45acf34ca3986f2b1b8d045bf26543ffb5f42ad4bcd45769adc2953e1` |
| `meta/SHA256SUMS_contrato_v1_1_original.txt` (línea base contractual v1.1; inmutable) | `c4b0decd68d72cb42046b7b4e561feeada6e4dfbc871bbf836d28b2dfe5d775e` |

Regenerar el manifiesto tras un cambio legítimo (desde la raíz; excluye `__pycache__`/`.pyc` y fija el orden con `LC_ALL=C`):
```sh
find src config schemas tests -type f -not -path '*/__pycache__/*' -not -name '*.pyc' -exec sha256sum {} + | LC_ALL=C sort -k 2 > meta/SHA256SUMS_v1_1_1.txt
cp meta/SHA256SUMS_v1_1_1.txt meta/SHA256SUMS.txt
```
**Alcance:** el manifiesto cubre solo esos cuatro directorios. `SHA256SUMS.txt` (79 líneas hasta r3) también cubría `docs/`, `patches/`, `run_tests.py`, `README.md` y `meta/`; esos archivos ya no figuran en él. Los parches se controlan por su hash en `patches/README.md` (verificado por `run_tests.py patches`); `docs/` y `meta/` no tienen ninguna comprobación automática.

Archivos de DTOS/UNAE que no son copia del original: los mismos cinco y con los mismos hashes que en g0.6 r3 (tabla de esa sección); esta revisión no los toca.

### Estado de la cadena de auditoría
- Línea base v1.1 sin tocar (hash fijado en `tests/integrity_tests/test_manifest.py`). Trazabilidad de sus 4 entradas: `Contrato_…_v1.1.md`, `DECISIONES_v1.1.md` y el esquema v1.1 coinciden con el hash de la línea base en sus ubicaciones actuales; el `.docx` v1.1 (`e2e609ea…`) sigue sin poder reproducirse (discrepancia 1 de este manifiesto; el `.docx` actual es el regenerado de g0.6 r1).
- Serie de parches 0001…0006: revertida sobre una copia reproduce los 5 originales (hash completo en `patches/README.md`); reaplicada en orden reproduce el árbol actual byte a byte.
- Pruebas nuevas: `tests/integrity_tests/` (11) y `tests/patch_tests/` (5, se omiten sin `patch`); `run_tests.py integrity|patches`, ambas incluidas en `all`. Totales: UNAE 62 (1 omitida), unitarias 62 (1 omitida), integración 104, DTOS 8 módulos, integridad 11, parches 5.
- Pendiente: parte 3 (JCS, a decidir); regenerar los `.docx` v1.0 y de auditoría (siguen siendo texto plano).

## g0.6 r5 (2026-10-27): auditoría de solo lectura de la evidencia persistida
- `src/dtos/scripts/audit_historical_data.py` (nuevo, con `__init__.py`): `PYTHONPATH=src python -m dtos.scripts.audit_historical_data RUTA.db [--json]`. Abre la base con `mode=ro`; no escribe en ella. Revisa `effects.evidence` y `effect_reconciliations.evidence` (JSON válido, sin NaN/Infinity, forma canónica y <= `MAX_EVIDENCE_SIZE_BYTES`: VIOLACIÓN) y `recovery_triggers.evidence` y `evidence.contradictions` (JSON válido y finito: VIOLACIÓN; forma canónica y tamaño: solo AVISO, porque el tope y la forma canónica se definieron para la evidencia de efectos). Salida 0 / 1 (violaciones) / 2 (no se pudo abrir).
- Una base en modo WAL puede dejar junto a ella los auxiliares vacíos `-wal`/`-shm` al auditarla; el `.db` no cambia ni un byte.
- Prueba: `tests/dtos_tests/test_audit_historical_data.py` (9; DTOS pasa a 9 módulos).
- Decisiones del usuario que NO se implementan (la guía «Punto 2»): script de saneamiento con `--apply` (choca con los triggers append-only y no hay datos previos a los parches); parche 0007 / tabla de evidencias por hash (se mantiene la evidencia autocontenida en `effects` y `effect_reconciliations`); wrapper `$ref` del esquema (se mantiene la copia idéntica). Sin cambios en parches, esquemas ni en los 5 archivos modificados de DTOS/UNAE (mismos hashes que en r3).
- Manifiesto regenerado (arriba). Verificado: `run_tests.py all` completo y manifiesto sin discrepancias.

## g0.6 r6 (2026-10-27): cierre documental — Parte 3 cerrada y limitaciones conocidas
Solo cambian `meta/VERSION.md` y este archivo. Ningún archivo de `src/`, `config/`, `schemas/`, `tests/` ni `patches/` cambia respecto a r5: `meta/SHA256SUMS_v1_1_1.txt` sigue siendo el de r5 (64 archivos, `2f6b636c…`).

### Parte 3 (esquema canónico JCS y tokens grandes): CERRADA, sin modificación (decisión del usuario)
- El serializador canónico actual (`dtos.canonical.canonical_json`) NO se cambia: sustituirlo por JCS alteraría los hashes calculados y persistidos y rompería la verificación de la cadena de auditoría en bases existentes.
- El contrato C-01 mantiene `fencing_token` como entero JSON dentro del sobre, con tope 2^53−1 (`MAX_FENCING_TOKEN`, `maximum` del esquema 1.1.1, vigilados por `SchemaAlignment`).
- Cualquier evolución hacia JCS exigiría un versionado mayor de DTOS en un ciclo futuro. Las menciones anteriores de «Parte 3 pendiente / a decidir» en las secciones de r3, r4 y r5 de este manifiesto quedan SUPERADAS por esta sección (se conservan como historia).

### Limitaciones y divergencias conocidas (registradas; no bloquean r6)
1. **Hash no reproducible del `.docx` v1.1.** La línea base lista `e2e609ea…`; el `.docx` v1.1 del proyecto tenía otro hash (`b7adaf1f…`, texto plano) y el actual (`879689bc…`) es un Word regenerado en g0.6 r1 desde el `.md`. La causa de la divergencia original no está determinada (no se conserva el archivo checksumeado). El `.md` y `DECISIONES_v1.1.md` sí coinciden con la línea base.
2. **`.docx` falsos.** `docs/contratos/Contrato_Interoperabilidad_UNAE_DTOS_v1_0.docx` y `docs/auditoria/Auditoria_Contrato_UNAE_DTOS_v1_0.docx` son texto plano con extensión `.docx`; no se abren como Word.
3. **`SchemaConformance` omitida.** Requiere `jsonschema`, no instalado en el entorno de desarrollo (sin red). La coherencia adaptador–esquema solo la cubre `SchemaAlignment`; la conformidad contra un validador JSON Schema real no se ha ejecutado.
4. **Sin cobertura automática de `docs/` ni `meta/`.** El manifiesto y `run_tests.py integrity` cubren únicamente `src`, `config`, `schemas` y `tests`; los parches se controlan por su hash en `patches/README.md`. El hash del manifiesto y de la línea base en este archivo y en `tests/integrity_tests/test_manifest.py` son la única referencia cruzada de `meta/`.

## g0.6 r7 (2026-10-27): `.docx` v1.1 original restaurado — limitación 1 RESUELTA
- El usuario aportó el archivo que generó la línea base: SHA-256 `e2e609eaf97b5bfc799369315918eebb0959f34f2b6714fa5ae48ed0d9bdc1ff` (Word real, 40 816 bytes), idéntico al de `meta/SHA256SUMS_contrato_v1_1_original.txt`. Sustituye a `docs/contratos/Contrato_Interoperabilidad_UNAE_DTOS_v1_1.docx` (el regenerado `879689bc…` ya no existe en el árbol).
- Con él, las 4 entradas de la línea base v1.1 se verifican en el árbol con su hash original, sin excepciones: `tests/integrity_tests/test_manifest.py::test_baseline_entries_still_traceable` ya no exime al `.docx`. Queda sin efecto la discrepancia 1 de este manifiesto (hash de `SHA256SUMS` frente al .docx del proyecto) y el punto 1 de «Limitaciones» de g0.6 r6, que pasa a **resuelto**. La causa de que el .docx del proyecto fuera otro (texto plano) sigue sin conocerse, pero ya no afecta al árbol.
- Texto del Word original vs `.md` del contrato: ~99,85 % de palabras coincidentes (comparación de texto, no byte a byte; no se revisó qué difiere).
- Siguen vigentes las limitaciones 2 (.docx v1.0 y de auditoría son texto plano), 3 (`SchemaConformance` omitida sin `jsonschema`) y 4 (sin cobertura automática de `docs/` y `meta/`: el .docx restaurado se protege solo por la prueba de la línea base).
- Cambio de código: solo `tests/integrity_tests/test_manifest.py` (manifiesto regenerado, arriba). Ningún cambio en `src/`, esquemas ni parches.

## g0.6 r8 (2026-10-27): precedencia documental (decisión del usuario)
- **Ante una discrepancia de contenido entre `docs/contratos/Contrato_Interoperabilidad_UNAE_DTOS_v1_1.md` y `…_v1_1.docx`, manda el `.md`**: es la fuente normativa documental. El `.docx` es el original de la línea base (`e2e609ea…`), conservado por compatibilidad y trazabilidad, no como referencia de contenido.
- Alcance: la precedencia es solo entre esos dos formatos del contrato. El propio contrato (§ del sobre) declara normativo el JSON Schema para la envoltura del mensaje; en el código, la referencia es `schemas/unae_dtos_message.schema.v1_1_1.json`.
- Esta regla es una decisión del usuario tomada en esta revisión; los documentos anteriores del paquete no la declaraban. Sigue sin revisarse la diferencia de ~0,15 % entre el texto del `.docx` y el `.md` (decisión del usuario: no es necesario).
- `docs/decisiones/DECISIONES_v1_1_1.md` (D-45, nota del `.docx`): añadida una corrección que remite a r7; el resto del texto se conserva como historia.
- Cambios de código: ninguno. `meta/SHA256SUMS_v1_1_1.txt` sin cambios (`9e7d0b72…`, 64 archivos). `docs/` y `meta/` siguen fuera del manifiesto automático (limitación 4).

## g0.6 r9
- Añadido `tests/integrity_tests/test_runner_exit_codes.py` (3 pruebas: `adaptr`, `adapter --bogus`, `unae extra` deben terminar con código 2).
- Manifiesto regenerado con el comando de arriba. Diferencia verificada frente a r8: una sola línea nueva (`5d0264f3…`).
- Sin cambios en `src/`, `config/`, `schemas/` ni en el resto de `tests/`.

## g0.6 r10
- `tests/adapter_tests/test_envelope.py`: importación explícita `import importlib.util`; el `skipUnless` usa `importlib.util.find_spec`.
- Diferencia del manifiesto frente a r9: una sola línea (`tests/adapter_tests/test_envelope.py`, nuevo hash `fc3be6fe…`).
- `run_tests.py` (raíz, fuera de alcance del manifiesto): `--strict` y comprobación previa de Python >= 3.11 (código 2).

## g0.6 r11 (2026-11-02): pausa lógica en `dispatch_outbox` de UNAE (D-48, parche 0007)
- `src/unae/store.py`: `dispatch_outbox(..., paused=None)` y método nuevo `release_events(event_ids, worker_id)`. Sin cambios de esquema, de contrato ni de ningún archivo de DTOS o del adaptador. Detalle y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-48).
- `patches/0007-unae-pausa-logica-en-dispatch-outbox.patch` (nuevo; aplica limpio sobre g0.6 r10 y reproduce el árbol actual; SHA-256 `0b6301dfeed9a04b…`, completo en `patches/README.md`). Serie ahora 0001…0007; `tests/patch_tests/test_patch_series.py`, `patches/README.md`, `README.md` y `run_tests.py` actualizados.
- Pruebas: 8 nuevas en `tests/unae_tests/test_outbox.py` (fallan sin el parche, pasan con él).

Archivos que ya no son copia del original (SHA-256, 16 primeros caracteres; solo cambia la última fila respecto a g0.6 r3/r4):

| Archivo | Original | Actual |
|---|---|---|
| `src/dtos/schema.sql` | `9b88a721264ebed1` | `4ae43839d0405c26` |
| `src/dtos/store.py` | `cc355b3405e182a8` | `8fff4128df96ead6` |
| `src/dtos/effect_repository.py` | `8016df2bdf47093a` | `191181c4094aabe0` |
| `src/dtos/canonical.py` | `7cf9f2344353bf20` | `90ce6b07e7c0fe32` |
| `src/unae/store.py` | `3af567b53a88f11e` | `9d48f6fa876536cf` |

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (65 archivos; `SHA256SUMS.txt` es copia idéntica), SHA-256 `c317e82a7894dd57016191f8761625640ba39b961f6d9986160cc472a4503b3c`. Respecto a g0.6 r10 difieren solo tres líneas: `src/unae/store.py`, `tests/unae_tests/test_outbox.py` y `tests/patch_tests/test_patch_series.py`. Mismo comando de regeneración que en g0.6 r4.

Verificación (Linux, Python 3.12.3, `patch` GNU disponible, `jsonschema` NO instalado): UNAE 70 pruebas (1 omitida permitida; incluye `test_crash` y `test_concurrency`), unitarias del adaptador 62 (1 omitida: `SchemaConformance`), integración 104, DTOS 9 módulos, integridad 14, parches 5; todas OK. NO se ejecutó `run_tests.py all --strict`: con `jsonschema` ausente la omisión de `SchemaConformance` no está permitida y el modo estricto fallaría. NO se probó en Windows (los errores de `tearDown` por `WinError 32` en `unae_tests` son anteriores y siguen sin corregir).
- Pendiente (la etiqueta g0.7 sigue reservada a esto): la condición de g0.7 registrada en DECISIONES (ejecutar `python run_tests.py adapter --strict` con `jsonschema` instalado) y el «Nivel 2» (migración 3 de UNAE, `not_before`).

## g0.6 r12 (2026-11-02): cierre de la conexión ancla en las pruebas de UNAE
- Solo pruebas: `tests/unae_tests/test_outbox.py`, `test_release.py`, `test_store.py` y `test_signed_receipts.py` (cierran el `Store` en `tearDown`). Nada en `src/`, `patches/` ni `schemas/`: los cinco archivos que no son copia del original conservan los hashes de g0.6 r11.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (65 archivos; `SHA256SUMS.txt` copia idéntica), SHA-256 `ffbad1ce3f09f81d7041287aa2ffdde30de39419d02cf16cc849989f85259ce7`. Respecto a r11 difieren solo esas cuatro líneas.
- Verificación (Linux, Python 3.12.3): UNAE 70 (1 omitida permitida), integridad y parches OK. Sin fuga de descriptores `.sqlite` tras cada prueba (ver DECISIONES). NO verificado en Windows.
- Pendiente: ejecutar `python run_tests.py unae` en Windows y confirmar 0 errores; condición de g0.7 (`jsonschema`); «Nivel 2» (migración 3, `not_before`).

## g0.6 r13 (2026-11-02): capa proactiva, modo «solo registrar» (D-49)
- Archivos nuevos (no existían en ninguna entrega anterior; no son copia de ningún original): `src/proactive/__init__.py`, `src/proactive/shadow.py`, `tests/proactive_tests/test_shadow.py` (18 pruebas). `run_tests.py` (comando `proactive`) y `README.md` actualizados.
- Sin cambios en `src/unae/`, `src/dtos/`, `src/adapter/`, `patches/`, `schemas/`: los cinco archivos que no son copia del original conservan los hashes de g0.6 r11.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (68 archivos, +3 respecto a r12; `SHA256SUMS.txt` copia idéntica), SHA-256 `6c6a48235e8d196d325938900b0393ced0c46c1357a56c075dea55fdc270bf50`.
- Dependencia nueva: `proactive.shadow` importa `dtos.canonical.canonical_json` (solo eso). Python ≥ 3.11 (StrEnum), igual que el resto.

## g0.6 r14 (2026-11-02): observador «tee» de la capa proactiva (D-50)
- Archivos nuevos: `src/proactive/tee.py`, `tests/proactive_tests/test_tee.py` (19 pruebas). Modificado respecto a r13: `src/proactive/__init__.py` (exporta la API nueva). `README.md`, `DECISIONES_v1_1_1.md` y `VERSION.md` actualizados.
- Sin cambios en `src/unae/`, `src/dtos/`, `src/adapter/`, `patches/`, `schemas/` ni `run_tests.py`: los cinco archivos que no son copia del original conservan los hashes de g0.6 r11.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (70 archivos, +2 respecto a r13; `SHA256SUMS.txt` copia idéntica), SHA-256 `b1990a419e4c048846182ac8fda2438bfabe439f4aedf3acb85248d47f371848`.
- Dependencias: `proactive.tee` importa solo `proactive.shadow`; `test_tee.py` importa `unae` (es una prueba de integración, la capa en sí no).
- Pendiente: ejecutar `python run_tests.py proactive` y `unae` en Windows; condición de g0.7 (`jsonschema`); conectar `tee_sender` a un despachador real; «Nivel 2» (migración 3, `not_before`); promoción a acción real.

## g0.8 r1 (2026-11-02): contenido de las políticas (D-51, parche 0008)
- `src/dtos/schema.sql`: columna `policies.content TEXT` (NULL permitido). `src/dtos/store.py`: migración idempotente con `PRAGMA table_info(policies)`, `insert_policy` y `get_policy`. `src/dtos/registries.py`: `PolicyRecord.content` opcional, `policy_content_hash` y `PolicyContentError`. Detalle, desvío respecto a la fórmula planteada y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-51).
- `patches/0008-dtos-contenido-de-politicas.patch` (nuevo; aplica limpio sobre g0.6 r14 y reproduce el árbol actual; no aplica sobre la base sin los parches previos; SHA-256 `e8843dbf93322e0b…`, completo en `patches/README.md`). Serie ahora 0001…0008; `tests/patch_tests/test_patch_series.py`, `patches/README.md`, `README.md`, `run_tests.py` y `meta/VERSION.md` actualizados. `registries.py` entra en la lista de archivos que ya no son copia del original.
- Archivos nuevos: `tests/dtos_tests/test_policy_content.py` (33 comprobaciones; falla contra el DTOS sin el parche).

Archivos que ya no son copia del original (SHA-256, 16 primeros caracteres):

| Archivo | Original | Actual |
|---|---|---|
| `src/dtos/schema.sql` | `9b88a721264ebed1` | `8763708c1c56b7f9` |
| `src/dtos/store.py` | `cc355b3405e182a8` | `c6f477ce05a89371` |
| `src/dtos/effect_repository.py` | `8016df2bdf47093a` | `191181c4094aabe0` |
| `src/dtos/canonical.py` | `7cf9f2344353bf20` | `90ce6b07e7c0fe32` |
| `src/unae/store.py` | `3af567b53a88f11e` | `9d48f6fa876536cf` |
| `src/dtos/registries.py` | `9fb44dd408ed92a8` | `94521211f2c9bab3` |

(El «original» de `registries.py` es su hash en g0.6 r14, declarado copia del original arriba; no hay un hash independiente aportado por el usuario.)

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (71 archivos, +1 respecto a r14; `SHA256SUMS.txt` copia idéntica), SHA-256 `caf51d0a11e2437f886685a61dd188ca9c8f7f1a78e7e4eac3d56bc01caa8e58`. Respecto a g0.6 r14 cambian cinco líneas (`src/dtos/schema.sql`, `src/dtos/store.py`, `src/dtos/registries.py`, `tests/patch_tests/test_patch_series.py`) más la nueva de `test_policy_content.py`. Mismo comando de regeneración que en g0.6 r4.

Verificación (Linux, Python 3.12.3, SQLite 3.45.1, `patch` GNU disponible, `jsonschema` NO instalado): `python run_tests.py all` con código de salida 0. UNAE 70 (1 omitida permitida; incluye crash y concurrencia), 9 módulos DTOS + `test_policy_content.py` sin FAIL, proactive 37, unitarias del adaptador 62 (1 omitida: `SchemaConformance`), integración 104, integridad 14, parches 5. NO se ejecutó `--strict` (la omisión de `SchemaConformance` lo haría fallar) ni nada en Windows.
- Pendiente: parches 0009 (REG-017), 0010 (`capability`), 0011 (revalidación por `rule_hash`), contrato y esquema v1.2 y adaptador; condición de g0.7 (`jsonschema`); «Nivel 2» (migración 3 de UNAE, `not_before`).

## g0.8 r2 (2026-11-02): REG-017 Mandates y políticas inmutables (D-52, parche 0009)
- `src/dtos/enums.py`: `MandateStrength`, `MandateTriggerKind`, `MandateEventType`. `src/dtos/registries.py`: `MandateRecord` y `ALLOWED_MANDATE_COMBINATIONS` (provisional). `src/dtos/schema.sql`: tablas `mandates` y `mandate_events`, triggers append-only también para `policies`. `src/dtos/store.py`: `create_mandate`, `get_mandate`, `get_mandate_for_task`, `get_mandate_events`. Detalle y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-52).
- `patches/0009-dtos-reg017-mandatos-y-politicas-inmutables.patch` (nuevo; aplica limpio sobre g0.8 r1 y reproduce el árbol actual; no aplica sobre la base sin los parches previos; SHA-256 `5d110a205a84be3e…`, completo en `patches/README.md`). Serie ahora 0001…0009. `tests/patch_tests/test_patch_series.py`, `patches/README.md`, `README.md`, `run_tests.py`, `meta/VERSION.md` y `DECISIONES_v1_1_1.md` actualizados. `enums.py` entra en la lista de archivos que ya no son copia del original.
- Archivos nuevos: `tests/dtos_tests/test_mandates.py` (52 comprobaciones; falla contra el DTOS sin el parche). Modificado: `tests/dtos_tests/test_policy_content.py` (simular una alteración de `policies` exige quitar antes los triggers).

Archivos que ya no son copia del original (SHA-256, 16 primeros caracteres):

| Archivo | Original | Actual |
|---|---|---|
| `src/dtos/schema.sql` | `9b88a721264ebed1` | `43e1b2cd6859631c` |
| `src/dtos/store.py` | `cc355b3405e182a8` | `0160104a934f5032` |
| `src/dtos/effect_repository.py` | `8016df2bdf47093a` | `191181c4094aabe0` |
| `src/dtos/canonical.py` | `7cf9f2344353bf20` | `90ce6b07e7c0fe32` |
| `src/unae/store.py` | `3af567b53a88f11e` | `9d48f6fa876536cf` |
| `src/dtos/registries.py` | `9fb44dd408ed92a8` | `7a25d460bc963ffe` |
| `src/dtos/enums.py` | `a7e9ffdba9c019fe` | `77fb447c6be3571b` |

(Los «originales» de `registries.py` y `enums.py` son sus hashes en g0.6 r14, declarados copia del original arriba; no hay un hash independiente aportado por el usuario.)

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (72 archivos, +1 respecto a g0.8 r1; `SHA256SUMS.txt` copia idéntica), SHA-256 `5676eda4651f869f18a6c4d8bfc14cae73da190cdb2bf091fa69ce3001939633`. Respecto a g0.8 r1 cambian `src/dtos/enums.py`, `schema.sql`, `registries.py`, `store.py`, `tests/dtos_tests/test_policy_content.py` y `tests/patch_tests/test_patch_series.py`, y se añade `tests/dtos_tests/test_mandates.py`. Mismo comando de regeneración que en g0.6 r4.

Verificación (Linux, Python 3.12.3, SQLite 3.45.1, `patch` GNU disponible, `jsonschema` NO instalado): `python run_tests.py all` con código de salida 0. UNAE 70 (1 omitida permitida), 11 módulos DTOS sin FAIL, proactive 37, unitarias del adaptador 62 (1 omitida: `SchemaConformance`), integración 104, integridad 14, parches 5. NO se ejecutó `--strict` ni nada en Windows.
- Pendiente: matriz (strength × trigger_kind) del usuario; parche 0010 (`capability`; antes listar los usos de `to_canonical_dict`), 0011 (revalidación por `rule_hash` y formato de las reglas), contrato y esquema v1.2 y adaptador; condición de g0.7 (`jsonschema`); «Nivel 2» (migración 3 de UNAE, `not_before`).

## g0.8 r3 (2026-11-04): corrección de la inmutabilidad frente a REPLACE (D-52c, parche 0009 revisado)
- `src/dtos/schema.sql`: tres triggers `BEFORE INSERT` (`trg_policies_no_replace`, `trg_mandates_no_replace` —claves `mandate_id` y `task_id`—, `trg_mandate_events_no_replace`) que impiden sobrescribir filas de las tablas append-only del parche 0009 con `INSERT OR REPLACE`. Único archivo de código modificado; `enums.py`, `registries.py` y `store.py` quedan idénticos a r2. Detalle, cambios de comportamiento y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-52c).
- `patches/0009-dtos-reg017-mandatos-y-politicas-inmutables.patch`: REGENERADO (aplica limpio sobre g0.8 r1 y reproduce el árbol actual; SHA-256 `817e2e24ff216404…`, completo en `patches/README.md`; el de r2 era `5d110a20…`). `patches/README.md`, `README.md`, `meta/VERSION.md` y `DECISIONES_v1_1_1.md` actualizados.
- Archivo nuevo: `tests/dtos_tests/test_append_only_replace.py` (falla contra el `schema.sql` de r2).

| Archivo | Original | Actual |
|---|---|---|
| `src/dtos/schema.sql` | `9b88a721264ebed1` | `a76fd4097a27dcd2` |

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (73 archivos, +1 respecto a g0.8 r2; `SHA256SUMS.txt` copia idéntica), SHA-256 `e3441228827662699eca96d1da932de7fd059f2afae242a54ed2f9e198135314`. Respecto a g0.8 r2 cambia una línea (`src/dtos/schema.sql`) y se añade otra (`tests/dtos_tests/test_append_only_replace.py`). Mismo comando de regeneración que en g0.6 r4.

Verificación (Linux, Python 3.12.3, SQLite 3.45.1, `patch` GNU disponible, `jsonschema` NO instalado): `python run_tests.py all` con código de salida 0, incluido el barrido de caída de UNAE. UNAE 70 (1 omitida permitida), 10 módulos DTOS sin FAIL (incluye el nuevo), proactive 37, unitarias del adaptador 62 (1 omitida: `SchemaConformance`), integración 104, integridad 14, parches 5. NO se ejecutó la traza g0.7 (sin `jsonschema`). Hallazgo abierto: las cinco tablas append-only originales (`artifacts`, `audit_log`, `effects`, `effect_reconciliations`, `reasoning_runs`) siguen sobrescribibles con `INSERT OR REPLACE` (comprobado; ver D-52c, límite 1).

## g0.8 r4 (2026-11-04): guardas anti-REPLACE en las cinco tablas históricas (D-52d, parche 0009 revisado)
- `src/dtos/schema.sql`: cinco triggers `BEFORE INSERT` (`trg_artifacts_no_replace`, `trg_reasoning_runs_no_replace`, `trg_effects_no_replace` —claves `effect_id` y `(provider, idempotency_key)`—, `trg_effect_reconciliations_no_replace`, `trg_audit_log_no_replace`) que impiden sobrescribir filas de `artifacts`, `audit_log`, `effects`, `effect_reconciliations` y `reasoning_runs` con `INSERT OR REPLACE`. Único archivo de código modificado además de la prueba; `store.py`, `registries.py` y `enums.py` quedan idénticos a r3. Detalle, cambios de comportamiento y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-52d). Cierra el «Hallazgo abierto» de r3.
- `patches/0009-dtos-reg017-mandatos-y-politicas-inmutables.patch`: REGENERADO (aplica limpio sobre g0.8 r1 y reproduce el árbol actual; SHA-256 `7e168824a49e6ced…`, completo en `patches/README.md`; el de r3 era `817e2e24…`). `patches/README.md`, `README.md`, `meta/VERSION.md` y `DECISIONES_v1_1_1.md` actualizados. No se crea un 0010: ese número sigue reservado a `capability`.
- `tests/dtos_tests/test_append_only_replace.py` ampliado (falla contra el `schema.sql` de r3).

| Archivo | Original | Actual |
|---|---|---|
| `src/dtos/schema.sql` | `9b88a721264ebed1` | `941f191b8b1765b8` |

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (73 archivos, igual cantidad que en r3; `SHA256SUMS.txt` copia idéntica), SHA-256 `e98500a3d726c675b50afd0c65fdae3e6c8e8ea020cb4b74a7b015fe2bff7d0c`. Respecto a g0.8 r3 cambian dos líneas (`src/dtos/schema.sql` y `tests/dtos_tests/test_append_only_replace.py`). Mismo comando de regeneración que en g0.6 r4.

Verificación (Linux, SQLite real, `patch` GNU disponible, `jsonschema` NO instalado): `python run_tests.py all` con código de salida 0 (UNAE 70 con 1 omitida permitida, DTOS sin FAIL, proactive 37, adaptador 62 con 1 omitida, integración 104, integridad 14, parches 5). NO se ejecutó la traza g0.7 (sin `jsonschema`).

## g0.8 r5 (2026-11-05): validación estricta de `rule_hash` en mandatos (D-53, parche 0009 revisado)
- `src/dtos/schema.sql`: tres triggers `BEFORE INSERT` en `mandates` (`trg_mandates_rule_hash_format`, `trg_mandates_policy_must_exist`, `trg_mandates_policy_must_have_content`). `src/dtos/registries.py`: `RULE_HASH_PATTERN` y su comprobación en `MandateRecord`. Detalle, alcance y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-53). Estos cambios de código ya estaban en el árbol recibido sin parche, manifiesto ni documentación; esta revisión los registra y los cubre con prueba.
- `patches/0009-dtos-reg017-mandatos-y-politicas-inmutables.patch`: REGENERADO (aplica limpio sobre g0.8 r1 y reproduce el árbol actual; SHA-256 `77b989559cdd0c28…`, completo en `patches/README.md`). `patches/README.md`, `README.md`, `meta/VERSION.md` y `DECISIONES_v1_1_1.md` actualizados.
- Archivo nuevo: `tests/dtos_tests/test_mandate_rule_hash.py` (falla contra el `schema.sql` sin los triggers D-53).

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (74 archivos, +1 respecto a r4; `SHA256SUMS.txt` copia idéntica), SHA-256 `8deae5f7c97a2fc945cc59da4dcff797d24d2b7e00b3e0f367f07a753c44bb2b`. Mismo comando de regeneración que en g0.6 r4.

## g0.8 r6 (2026-11-05): colisión de clave de idempotencia en `_ensure_ledger` (D-54, Hallazgo 11)
- `src/adapter/reconcile.py`: `LedgerCollision` (interna); `_ensure_ledger` distingue carrera benigna de colisión real; `recover_after_restart` registra `INCONSISTENT` y cuenta `inconsistent`. `tests/adapter_integration_tests/test_reconcile.py`: dos pruebas nuevas. Sin cambios en DTOS, UNAE, parches ni esquema. Detalle y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-54).

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (74 archivos, igual cantidad que r5; `SHA256SUMS.txt` copia idéntica), SHA-256 `bae783cf26b0f221f0ecef66c131c3e9a0285868ae718d985d9f0ce4f9863a5f`. Cambian dos líneas (`src/adapter/reconcile.py` y `tests/adapter_integration_tests/test_reconcile.py`).

## g0.8 r7 (2026-11-05): cancelación cooperativa de timeouts en el despacho (D-55, Hallazgo 5)
- `src/adapter/dispatch.py`: `request["cancel"]` (`threading.Event`) y `request["deadline"]` (monotonic) para el proveedor; la señal se activa antes de marcar UNKNOWN y al terminar la llamada. `tests/adapter_integration_tests/test_dispatch.py`: clase `CooperativeCancellation` (5 pruebas). Sin cambios en DTOS, UNAE, parches ni esquema. Detalle y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-55): reduce la ventana de efectos tardíos, no la cierra.

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (74 archivos, igual cantidad que r6; `SHA256SUMS.txt` copia idéntica), SHA-256 `8e0930452c1a30001e13094bf8bd4367c455404365db89691d8cf0a334831dd9`. Cambian dos líneas (`src/adapter/dispatch.py` y `tests/adapter_integration_tests/test_dispatch.py`).

## g0.8 r8 (2026-11-05): fencing hasta el proveedor (D-56, Hallazgo 4)
- `src/adapter/dispatch.py`: `request["fencing_token"]` y `request["fencing_scope"]`; validación y retirada del eco `receipt["fencing_token"]`; `require_fencing_echo`. `src/adapter/fencing.py` (nuevo): `FencingGate`, `StaleFencingToken`. `src/adapter/__init__.py`: exporta ambos. `tests/adapter_tests/test_fencing_gate.py` (nuevo, 7) y clase `FencingToTheProvider` en `tests/adapter_integration_tests/test_dispatch.py` (7). Sin cambios en DTOS, UNAE, parches, esquema ni contrato de mensajes. Detalle y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-56): el fencing solo protege si el proveedor lo aplica.

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (76 archivos, +2 respecto a r7; `SHA256SUMS.txt` copia idéntica), SHA-256 `6cb67fa8cdafcb97e9b7504186d696a8a398165f56e67c5e21ec84267d0653b5`.

## g0.8 r9 (2026-11-05): concurrencia explícita en la persistencia de DTOS (D-57, Hallazgo 8, parche 0010)
- `src/dtos/store.py`: `SQLiteStore(db_path, *, busy_timeout_ms=10000)` con `PRAGMA busy_timeout` y `PRAGMA journal_mode=WAL` explícitos y verificados antes de crear el esquema; validación de `busy_timeout_ms`; cierre de la conexión si la configuración falla; `_add_column_if_missing` (migración de columnas con doble comprobación dentro de `BEGIN IMMEDIATE`). Único archivo de `src/` modificado; `schema.sql`, UNAE y adaptador no cambian. Detalle, controles negativos y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-57).
- `patches/0010-dtos-concurrencia-busy-timeout-y-wal.patch` (nuevo; aplica limpio sobre g0.8 r8 y reproduce el árbol actual; no aplica sobre la base sin los parches previos; SHA-256 completo en `patches/README.md`). `patches/README.md`, `README.md`, `run_tests.py` (solo su docstring), `tests/patch_tests/test_patch_series.py`, `meta/VERSION.md` y `DECISIONES_v1_1_1.md` actualizados. La serie pasa a 0001…0010; los parches `capability` y revalidación por `rule_hash` pasan a 0011 y 0012 (pendiente de confirmar).
- `tests/dtos_tests/test_concurrency.py` (nuevo): seis secciones multiproceso con `spawn` (configuración, semántica del timeout y de WAL, apertura simultánea de una base nueva, escrituras simultáneas con lector, carreras sobre la misma clave, migración de una base antigua).

| Archivo | Original | Actual |
|---|---|---|
| `src/dtos/store.py` | `cc355b3405e182a8` | `f19d628182e60573` |

Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (77 archivos, +1 respecto a r8; `SHA256SUMS.txt` copia idéntica), SHA-256 `a9d0ff7fadc5525de63b48b1ce030a4717fa7a8077bbca778bfa508e60871614`. Respecto a r8 cambian `src/dtos/store.py` y `tests/patch_tests/test_patch_series.py`, y se añade `tests/dtos_tests/test_concurrency.py`. Mismo comando de regeneración que en g0.6 r4.

Verificación (Linux, Python 3.12.3, SQLite 3.45.1, `patch` GNU disponible, `jsonschema` NO instalado): `python run_tests.py all` con código de salida 0. UNAE 70 (1 omitida permitida), 14 módulos DTOS sin FAIL (incluye el nuevo), proactive 37, unitarias del adaptador 69 (1 omitida: `SchemaConformance`), integración 118, integridad 14, parches 5. `test_concurrency.py` además se repitió 14 veces (6 secuenciales y 8 en paralelo, para cargar la máquina) sin ningún fallo. NO se ejecutó: `--strict` (fallaría por la omisión de `SchemaConformance` sin `jsonschema`), la traza g0.7, nada en Windows ni con Python 3.10/3.15 (laptop del usuario), ni sobre sistemas de archivos de red.

## g0.8 r10 (2026-11-05): Tanda A del informe de adaptadores (C-1, A-3; D-58, D-59)
- Cambian `src/adapter/dispatch.py`, `src/adapter/reconcile.py`, `tests/adapter_integration_tests/test_dispatch.py` y `tests/adapter_integration_tests/test_reconcile.py`. Sin cambios en `src/dtos/`, `src/unae/` ni parches (la serie 0001…0010 sigue reproduciendo el árbol).
- Pruebas: integración 131 (+13). Unitarias 69 (1 omitida), integridad 14 y proactiva 37 sin cambios.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (77 archivos; `SHA256SUMS.txt` copia idéntica). Mismo comando de regeneración que en g0.6 r4.

## g0.8 r11 (2026-11-05): Tanda B del informe de adaptadores (A-2, A-1, TimeoutError; D-60, D-61)
- Cambian `src/adapter/adapter.py`, `src/adapter/dispatch.py`, `src/adapter/__init__.py`, `tests/adapter_tests/test_envelope.py`, `tests/adapter_integration_tests/test_dispatch.py` y `tests/adapter_integration_tests/test_reconcile.py`. Sin cambios en `src/dtos/`, `src/unae/`, parches ni esquemas.
- Pruebas: unitarias 77 (1 omitida), integración 137, integridad 14, proactiva 37, parches 5.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (77 archivos; `SHA256SUMS.txt` copia idéntica). Mismo comando de regeneración que en g0.6 r4.

## g0.8 r12 (2026-11-05): Tanda C del informe de adaptadores (campos de texto, M-3, M-4, L-1 a L-4; D-62)
- Cambian `src/adapter/{adapter,bridge,dispatch,reconcile,transport,__init__}.py` y `tests/adapter_tests/{test_envelope,test_transport_config}.py`, `tests/adapter_integration_tests/{test_dispatch,test_reconcile}.py`. Sin cambios en `src/dtos/`, `src/unae/`, parches ni esquemas.
- Pruebas: unitarias 85 (1 omitida), integración 152, integridad 14, proactiva 37, parches 5.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (77 archivos; `SHA256SUMS.txt` copia idéntica). Mismo comando de regeneración que en g0.6 r4.


## g0.8 r13 (2026-11-05): desacoplamiento del almacenamiento (D-63, parche 0012)
- Cambian `src/dtos/store.py`, `src/dtos/audit.py` y `src/dtos/inbox_outbox.py` (parche `0012`; `audit.py` e `inbox_outbox.py` dejan de ser copia del original: base `d9477957…` y `7ccafebd…`, hashes de g0.8 r12), `src/adapter/{adapter,bridge,reconcile,__init__}.py`, `run_tests.py`, `README.md` y `tests/patch_tests/test_patch_series.py`, `tests/adapter_integration_tests/test_bridge.py`.
- Archivos nuevos (+3): `tests/dtos_tests/test_public_reads.py`, `tests/adapter_tests/test_decoupling.py`, `tests/integrity_tests/test_connection_policy.py`. Parche nuevo (fuera del alcance del manifiesto): `patches/0012-…patch`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (80 archivos, +3; `SHA256SUMS.txt` copia idéntica). Mismo comando de regeneración que en g0.6 r4.

## g0.8 r14 (2026-11-06): endurecimiento de outbox y auditoría (D-64, parche 0013)
- Cambian `src/dtos/store.py`, `src/dtos/audit.py` y `src/dtos/inbox_outbox.py` (parche `0013`), `run_tests.py` (solo su docstring), `tests/patch_tests/test_patch_series.py` y `tests/dtos_tests/test_public_reads.py` (limpia también `.publish.lock`).
- Archivo nuevo (+1): `tests/dtos_tests/test_outbox_audit_hardening.py`. Parche nuevo (fuera del alcance del manifiesto): `patches/0013-…patch`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (81 archivos, +1; `SHA256SUMS.txt` copia idéntica). Mismo comando de regeneración que en g0.6 r4.

## g0.8 r15 (2026-11-06): lease portable, cuarentena del outbox y endurecimiento (D-65, parche 0014)
- Cambian `src/dtos/schema.sql`, `src/dtos/store.py`, `src/dtos/audit.py`, `src/dtos/inbox_outbox.py` y `src/unae/store.py` (parche `0014`), `run_tests.py` (solo su docstring), `README.md`, `tests/patch_tests/test_patch_series.py`, `tests/dtos_tests/test_outbox_audit_hardening.py` (el `flock` pasa a lease; un payload no JSON ahora queda en cuarentena), `tests/dtos_tests/test_public_reads.py` (ya no limpia `.publish.lock`) y `tests/unae_tests/test_outbox.py` (+5 pruebas).
- Archivo nuevo (+1): `tests/dtos_tests/test_outbox_quarantine.py`. Parche nuevo (fuera del alcance del manifiesto): `patches/0014-…patch`.
- Hallazgos de la revisión de g0.8 r14 resueltos: C1, I1, I2, I3, I4 e I5. Pospuestos: M1 (tope de `note` en `ShadowLog.review`), M2 (`verify` en streaming), M3 (fallo del marcado tras enviar), M4 (`skipped` vs vacío), M5 (`limit` > 2^63), M6 (el tee copia el evento antes del envío real), M7 (conflicto de idempotencia silencioso en `ShadowLog.record`).
- Cambios de comportamiento: `publish_pending` ya no depende de `fcntl` y acepta `max_attempts` y `lease_s` (solo por nombre); un payload no JSON se informa una vez y pasa a cuarentena (antes se reintentaba siempre); `dispatch_outbox` no envía un evento cuyo lease ya es de otro worker (cuenta `lost`); `claim_events` marca DEAD un evento con payload ilegible. `pending_events` sigue lanzando ante un payload ilegible (no se tocó).
- Verificado en Linux (Python 3.12.3): unae 75 (1 omitida), dtos 17 scripts sin fallos, proactiva 37, adaptador 93 (1 omitida) e integración 153, integridad 19, parches 5. NO se ejecutó en Windows.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (82 archivos, +1; `SHA256SUMS.txt` copia idéntica). Mismo comando de regeneración que en g0.6 r4.

## g0.8 r16 (2026-11-07): reencolado de la cuarentena y abandono de claims (D-66, parche 0015)
- Cambian `src/dtos/schema.sql`, `src/dtos/store.py`, `src/dtos/inbox_outbox.py` (solo docstring) y `src/unae/store.py` (parche `0015`), `run_tests.py` (solo su docstring), `README.md` y `tests/patch_tests/test_patch_series.py`.
- Archivos nuevos (+2): `tests/dtos_tests/test_outbox_requeue.py` y `tests/unae_tests/test_outbox_abandoned_claims.py`. Parche nuevo (fuera del alcance del manifiesto): `patches/0015-…patch`.
- Hallazgos de la revisión adversarial del 0014 resueltos: I-1 e I-2, y M-a (documentado). Pospuestos: backoff por tiempo en DTOS (exigiría una columna nueva en `outbox_failures` y cambiar la firma de `get_pending_outbox`; no evita la cuarentena en una caída larga, que ya se revierte con `requeue_quarantined`), M-b, M-c y M-d.
- Cambios de comportamiento: en UNAE, `attempts` también sube cuando se reclama un claim vencido sin confirmar (un dueño lento pero sano suma un intento por vencimiento); `claim_events` acepta `max_attempts` solo por nombre (por defecto 5, igual que `dispatch_outbox`).
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (84 archivos, +2; `SHA256SUMS.txt` copia idéntica). Mismo comando de regeneración que en g0.6 r4.


## g0.8 r17 (2026-11-07): pruebas compatibles con Windows (solo `tests/`)
- Cambian solo `tests/proactive_tests/test_shadow.py` (la limpieza del directorio temporal pasa de `tearDown` a `addCleanup` registrado en `setUp`, así corre después de cerrar la conexión `raw()` de `AppendOnlyTests`) y `tests/dtos_tests/test_audit_historical_data.py` (la conexión `mode=ro` de `test_audit_never_modifies_the_database` se cierra con `contextlib.closing`). `src/`, `config/`, `schemas/` y los parches no cambian.
- Origen: ejecución en Windows 10 con Python 3.15.2 de g0.8 r16 (`unae --skip-crash` 76 OK, `adapter` 93 OK, `adapter-integration` 153 OK, `dtos` 16/18 archivos OK, `proactive` 34/37, `integrity` 18/19): los 3 errores de `proactive` y el de `test_audit_historical_data` eran `PermissionError [WinError 32]` por conexiones abiertas al limpiar; el fallo de `integrity` fue por 7 archivos de salida ajenos al release dentro de `tests/`.
- Sin resolver: `dtos/test_concurrency` falla en Windows en las secciones C y D con `database is locked` bajo contención multiproceso; aún no se sabe si es del código o del entorno (laptop lenta). NO se ejecutaron en Windows `unae` sin `--skip-crash` ni la serie `patches`. Los arreglos de esta revisión solo se verificaron en Linux; su efecto en Windows está por confirmar.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (84 archivos, sin cambios de lista; solo cambian los hashes de los dos archivos de prueba; `SHA256SUMS.txt` copia idéntica).

## g0.8 r18 (2026-11-07): apertura robusta ante un BUSY inmediato (D-67, parche 0016)
- **Origen:** ejecución en Windows 10 + Python 3.15.2 de r16/r17. `dtos/test_concurrency` fallaba (secciones C y D) con `database is locked`; el diagnóstico `diag_bloqueos.py` (fuera del release) midió que abrir una base nueva con 8 procesos tarda de 4 a 10 s por proceso (el esquema son ~60 sentencias en autocommit, ~45 ms de fsync cada una) y que un proceso falló a los 4 ms: SQLite devolvió BUSY SIN esperar el `busy_timeout`.
- **Cambio:** solo `src/dtos/store.py` (parche `0016-dtos-reintento-acotado-de-la-apertura.patch`): `SQLiteStore.__init__` llama a `_open_with_retry`, que repite `_configure_connection` + `_init_schema` (idempotentes) ante un bloqueo temporal, con pausas de 10 ms hasta 250 ms con jitter, hasta agotar `busy_timeout_ms` en total; con 0 no reintenta; los errores que no son de bloqueo y los RuntimeError (p. ej. WAL imposible) se propagan al instante; la conexión de una apertura fallida se cierra. `_is_lock_error` decide por `sqlite_errorcode` (BUSY/LOCKED) o, sin él, por el texto.
- **Cambio de comportamiento:** abrir bajo bloqueo puede tardar hasta `busy_timeout_ms` antes de fallar (antes fallaba en cuanto SQLite devolvía BUSY). Lo demás no cambia: `transaction()` sigue sin reintentos propios.
- **Pruebas nuevas:** `tests/dtos_tests/test_open_retry.py` (8; parte de `run_tests.py dtos`); 5 de ellas fallan sin el parche. Se actualizó `tests/patch_tests/test_patch_series.py` (serie con el 0016). Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (85 archivos: +`test_open_retry.py`; cambian los hashes de `src/dtos/store.py`, `test_patch_series.py`; `SHA256SUMS.txt` copia idéntica).
- **NO cubierto (por qué):** (1) los errores de la sección D de `test_concurrency` (`task: database is locked` en la ronda 0, en `transaction()` = `BEGIN IMMEDIATE`) NO se tocan: no se midió cuánto tardan en llegar, así que no se sabe si tienen la misma causa; si persisten en Windows con r18, el siguiente paso es medirlos antes de cambiar `transaction()`. (2) El efecto en Windows de este parche está por confirmar: solo se verificó en Linux, donde la carrera real casi no se da. (3) Siguen sin ejecutarse en Windows `unae` sin `--skip-crash` y la serie `patches`.
## g0.8 r19 (2026-11-07): la sección D de `test_concurrency` deja de depender de la latencia del disco (solo pruebas)
- **Origen:** en Windows 10 + Python 3.15.2 con r18, la sección C pasa (el parche 0016 cumplió su objetivo) pero la D falla en 2 de 6 escritores con `database is locked`. El diagnóstico `diag_concurrencia_d.py` (copia de la prueba con tiempos, fuera del release; 2 corridas) midió que los errores llegan a los 10,9 y 11,0 s: se agota el busy_timeout de 10 s; NO es un BUSY inmediato. Las primeras inserciones terminaron en escalones de ~3,3 s (3,8 / 7,1 / 10,4 s en la corrida 1); es probable (inferencia, no medido) que cada escritor que consigue el bloqueo encadene toda su carga y los demás esperen en cola.
- **Causa:** SQLite no reparte el turno de escritura con equidad, y el trabajo total en cola (6 escritores x 12 rondas x 5 transacciones x ~45 ms por commit con `synchronous=FULL` en ese disco) ronda los 20 s, más que los 10 s de producción. En Linux, con commits de <1 ms, toda la prueba dura ~1 s y nadie espera. No se perdió ni se corrompió nada: cadena de auditoría válida (72/72), ledger 72/72, Commit Gate 72 avances, `integrity_check` y `foreign_key_check` limpios, append-only intacto; los 2 mandatos que faltaban (70/72) eran los de las 2 tareas que no se pudieron insertar.
- **Cambio (solo `tests/dtos_tests/test_concurrency.py`):** constante `STRESS_BUSY_TIMEOUT_MS = 60_000` (3 veces lo medido) aplicada ÚNICAMENTE a los escritores de la sección D (`_scn_stress`); la sección D comprueba que los escritores usan ese valor. El valor de producción (`DEFAULT_BUSY_TIMEOUT_MS = 10_000`), la sección B (que el error llega al agotarse el plazo CONFIGURADO) y las secciones A y C no cambian. `src/` y los parches no cambian: solo cambian los hashes de `test_concurrency.py`, `VERSION.md` y `MANIFEST.md`.
- **Límite conocido (documentado, no corregido):** con varios procesos escribiendo de forma continua y commits lentos (~45 ms), el último de la cola puede recibir `database is locked` pasados `busy_timeout_ms` (10 s por defecto), porque la espera de SQLite no es equitativa. Si la carga real de producción es así, hay que subir `busy_timeout_ms` (parámetro del constructor) o espaciar las escrituras; una política distinta (`DEFAULT_BUSY_TIMEOUT_MS` o `synchronous=NORMAL`) es una decisión del operador, no se tomó aquí.
- **Sin verificar:** el efecto de este ajuste en Windows (en Linux la prueba ya pasaba sin él; solo se comprobó que el valor llega a los escritores). Siguen sin ejecutarse en Windows `unae` sin `--skip-crash` y la serie `patches`.

## g0.8 r20 (2026-11-07): dos ajustes de pruebas tras la pasada completa en Windows (solo pruebas; `src/` no cambia)
- **Origen:** pasada completa de r19 en Windows 10 + Python 3.15.2: `adapter` 93 OK; `dtos` 18 de 19 scripts OK; `unae --skip-crash` 75 de 76 OK; `adapter-integration` sin resultado (salida cortada).
- **1. `tests/dtos_tests/test_open_retry.py` (defecto de la propia prueba, introducido en r18):** (a) el método auxiliar se llamaba `fail` y pisaba `unittest.TestCase.fail`, de modo que un `assert` fallido producía `TypeError: fail() missing 1 required positional argument` en lugar de su mensaje; ahora se llama `make_failing` y una prueba nueva (`test_helpers_do_not_shadow_the_unittest_api`) lo vigila; (b) la prueba del plazo agotado simulaba el bloqueo en `_init_schema`, de modo que la primera apertura REAL (con I/O) corría antes del primer fallo y en un disco lento podía consumir los 200 ms: sin reintento, `assertGreaterEqual(calls, 2)` fallaba. Ahora el bloqueo se simula en `_configure_connection`, que no hace I/O. Reproducido en Linux inyectando 0,3 s de latencia por sentencia: r19 falla con el mismo `TypeError` observado en Windows y r20 pasa. La suite pasa de 8 a 9 pruebas.
- **2. `tests/unae_tests/test_crash.py`:** la clase `CrashTests` se omite con `skipUnless(hasattr(signal, "SIGKILL"))` y un mensaje explícito, porque `signal.SIGKILL` no existe en Windows (las pruebas lo usan para matar el proceso y comprobar `-signal.SIGKILL`). Verificado en Linux simulando la ausencia de SIGKILL: 3 omitidas, 0 errores. **En Windows no se ha ejecutado** y el barrido de caídas sigue verificado solo en Linux/macOS. `run_tests.py` (fuera del alcance del manifiesto) permite esas 3 omisiones bajo `--strict` únicamente cuando no hay SIGKILL; con SIGKILL siguen siendo un fallo.
- **Sin cambios y sin resolver:** `unae/test_outbox.py::test_event_claimed_by_another_worker_is_not_sent_twice` falló 1 vez en Windows con r19 (`e1` enviado por A y por B; pasaba en r16). No se reprodujo en Linux ni inyectando latencia (0,05 a 0,2 s) en BEGIN/COMMIT o en todas las sentencias. Hipótesis sin confirmar: margen de 0,1 s en la prueba (lease 0,6 s frente a un envío de 0,5 s). No se modifica hasta medir su frecuencia (20 repeticiones en Windows). Tampoco hay resultado de `adapter-integration` con r19 en Windows.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (85 archivos; cambian los hashes de `test_open_retry.py` y `test_crash.py`; `SHA256SUMS.txt` copia idéntica).

## g0.8 r21 (2026-11-08): `test_event_claimed_by_another_worker_is_not_sent_twice` con reloj simulado (solo pruebas; `src/` no cambia)
- **Origen:** en Windows con r20 la prueba falló 5 de 20 veces (`e1` enviado por A y por B). Reproducido en Linux retrasando ≥0,25 s el acuse del primer evento (0 fallos con ≤0,18 s).
- **Causa:** defecto de margen de la prueba, no del código. Usaba `sleep` reales con solo 0,1 s de holgura entre el envío (0,5 s) y el lease (0,6 s); si el acuse de `e1` llega después de vencer el lease, B lo retoma legítimamente (contrato «al menos una vez», D-65).
- **Cambio (solo `tests/unae_tests/test_outbox.py`):** (1) la prueba usa un reloj simulado (`mock.patch("time.time")`), sin hilos ni `sleep`: A reclama 4 eventos con lease de 0,6 s, B entra a los 0,7 s y se verifica `a == [e1, e2]`, `b == [e3, e4]` y resultado `{sent: 2, lost: 2}`; (2) prueba nueva `test_send_longer_than_lease_may_be_resent_at_least_once` que fija como contrato que un envío más largo que `lease_s` puede reenviarse y A lo cuenta como `lost`.
- **Verificación:** con `renew_claim` anulado en `dispatch_outbox` la prueba reescrita FALLA (detecta la regresión que protege); 5 corridas seguidas de `test_outbox` en Linux/Python 3.12: 22/22 OK. No ejecutado en Windows.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (85 archivos; cambia solo el hash de `test_outbox.py`; `SHA256SUMS.txt` copia idéntica).

## g0.8 r22 (2026-11-09): parche 0017 (D-68) — `ROLLBACK` seguro en UNAE y claves str en `canonical_json` (cambia `src/`)
- **Origen:** análisis estático de r21. Detalle, causa y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-68) y en `patches/README.md` (fila 16).
- **Código:** `src/unae/store.py` (`Store._rollback`, usado por los 42 sitios; `canonical_json` valida claves), `src/dtos/canonical.py` y `src/adapter/adapter.py` (`canonical_json` valida claves). Los dos primeros están en la serie de parches (`patches/0017-…patch`); `adapter.py` no es un original parchado y no figura en ella.
- **Pruebas nuevas:** `tests/unae_tests/test_rollback_and_keys.py`, `tests/dtos_tests/test_canonical_keys.py`, `tests/adapter_tests/test_canonical_keys.py`. `tests/patch_tests/test_patch_series.py` incorpora el 0017 a la serie.
- **Documentación:** `patches/README.md`, `README.md`, `run_tests.py` (solo el texto de ayuda), `meta/VERSION.md`.
- **Hashes (abreviados):** `src/unae/store.py` `32af8676a44aeeb6`, `src/dtos/canonical.py` `316ef64181c4d095`, `src/adapter/adapter.py` `6812d06dba992fff`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (88 archivos; `SHA256SUMS.txt` copia idéntica), SHA-256 `23c15e81e432ad9b75fc9a27c57f1a1b7173495fa36142e09d0ea4ceb8eb3a6c`.

## g0.8 r23 (2026-11-09): `--skip-crash` omite solo `test_crash` y se redactan D-64 a D-67 (solo runner, pruebas y documentación; `src/` no cambia)
- **Origen:** el análisis de g0.8 r21 detectó que `run_tests.py --skip-crash` usaba el patrón `test_[!c]*.py`, que además de `test_crash.py` excluía `test_concurrency.py` (y cualquier archivo de prueba cuyo nombre empezara por «c»), y que `DECISIONES_v1_1_1.md` terminaba en D-63 mientras el código citaba hasta D-67.
- **Medido:** en r22, `unae --skip-crash` ejecutaba 90 pruebas con el patrón anterior; con el arreglo ejecuta 92 (las 2 de `test_concurrency`); el total con `test_crash` es 95. Las corridas de `--skip-crash` de r16 a r22 (incluidas las citadas en este documento) no cubrían `test_concurrency` de UNAE.
- **Cambio en `run_tests.py`:** `collect_unittest(start, skip_crash)` descubre `test_*.py` y quita solo los tests del módulo `test_crash`, por nombre exacto; `_run_suite` ejecuta la suite en un subproceso (modo normal y estricto por el mismo camino). Los fallos de importación nunca se filtran. Se conserva el `import importlib.util` provisional (pendiente de retirar, ver backlog).
- **Prueba nueva:** `tests/integrity_tests/test_runner_selection.py` (4 pruebas; 3 fallan con el patrón anterior, comprobado en una copia).
- **Documentación:** `docs/decisiones/DECISIONES_v1_1_1.md` incorpora D-64 (parche 0013), D-65 (0014), D-66 (0015) y D-67 (0016), redactadas a partir del texto de los parches, del README de parches y de este MANIFEST (cada una declara su procedencia), y corrige la nota de trazabilidad de D-68. `meta/VERSION.md`, `README.md`.
- **Verificado en Linux (Python 3.12.3), `run_tests.py all` con barrido SIGKILL:** unae 95 (1 omitida), dtos 20 scripts sin fallos, proactiva 37, adaptador 96 (1 omitida: `jsonschema` no instalado), integración 153, integridad 23, parches 5. NO se ejecutó en Windows ni con `--strict` global.
- **Backlog técnico (pendiente, sin tocar en r23):** (1) trazabilidad estructurada (`logging`) en las excepciones silenciosas de `adapter/dispatch.py` (`_abort_unrecorded`) y `proactive/tee.py` (dos sitios). (2) Validación explícita de `limit` en `claim_events` y `dead_events` de UNAE (SQLite trata un LIMIT negativo como sin tope); criterio por decidir: `>= 0`, como en `get_pending_outbox` de DTOS, o `> 0`. (3) Retirar 5 imports sin uso (`closing` en `adapter/bridge.py`; `Optional` en `cognitive_cycle_fsm.py`, `recovery_authority_repository.py` y `recovery_bootstrap_fsm.py`; `ConflictError` en `effect_repository.py`) y el `import importlib.util` provisional de `run_tests.py` (`test_envelope` ya lo importa). (4) Evaluar unificar las tres copias de `canonical_json` (`dtos/canonical.py`, `unae/store.py`, `adapter/adapter.py`), que difieren en `ensure_ascii`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (89 archivos: +`test_runner_selection.py`; cambian los hashes de `run_tests.py`; `SHA256SUMS.txt` copia idéntica), SHA-256 `4ce2d15b4419c0998589639b30afba7ad5fe702c2db741760313d3b657487060`.

## g0.8 r24 (2026-11-10): renombrado a `agent-execution-kernel` (solo documentación; `src/`, `config/`, `schemas/` y `tests/` no cambian)
- **Cambio:** el proyecto deja de llamarse `ia-personal` (D-69). Se actualizan `README.md` (título, descripción técnica y nota del nombre anterior), `meta/VERSION.md` (revisión, fecha y patrón del zip), `docs/readme/README_UNAE_4_3N.md` (encabezado) y `docs/decisiones/DECISIONES_v1_1_1.md` (D-69).
- **Código:** ninguno. Nombre del zip: `agent-execution-kernel_g0_8_r24_2026-11-10.zip`; carpeta raíz: `agent-execution-kernel/`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (89 archivos; sin cambios; `SHA256SUMS.txt` copia idéntica), SHA-256 `4ce2d15b4419c0998589639b30afba7ad5fe702c2db741760313d3b657487060`.

## g0.8 r25 (2026-11-10): parche 0018 (D-70) — índice parcial y purga del outbox, WAL verificado en los cuatro almacenes (cambia `src/`)
- **Origen:** auditoría de SQLite de r24. Detalle, medidas y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-70) y en `patches/README.md` (fila 17).
- **Código:** `src/dtos/schema.sql` (`ix_outbox_pending`), `src/dtos/store.py` (`purge_published_outbox`), `src/unae/store.py` (WAL verificado), `src/adapter/adapter.py` y `src/proactive/shadow.py` (WAL verificado). Los tres primeros están en la serie (`patches/0018-…patch`); `adapter.py` y `shadow.py` no son originales parchados y no figuran en ella.
- **Pruebas nuevas:** `tests/dtos_tests/test_outbox_purge.py`, `tests/integrity_tests/test_wal_policy.py`. `tests/patch_tests/test_patch_series.py` incorpora el 0018 a la serie.
- **Documentación:** `patches/README.md`, `README.md`, `run_tests.py` (solo el texto de ayuda), `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (91 archivos; `SHA256SUMS.txt` copia idéntica), SHA-256 `86710553f4f28dc58254e65a4fdc832a4effe3b6b0059853617551d907225874`.
- **Corrección documental de r25 (2026-11-10):** `docs/decisiones/DECISIONES_v1_1_1.md` (D-70) pasa de «decisiones abiertas» a ratificadas (se mantiene D-63; purga sin constancia en `audit_log`) y deja abierto solo el plazo por defecto de la purga. Sin cambios en `src/`, `config/`, `schemas/` ni `tests/`: el manifiesto no cambia.

## g0.8 r26 (2026-11-10): parche 0019 (D-71) — verificación de la cadena de auditoría por lotes (cambia `src/`)
- **Origen:** primer pendiente de D-70. Detalle, medida y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-71) y en `patches/README.md` (fila 18).
- **Código:** `src/dtos/store.py` (`iter_audit_chain`, `count_audit_entries`) y `src/dtos/audit.py` (`AuditChain.verify` por lotes); ambos en la serie (`patches/0019-…patch`).
- **Pruebas nuevas:** `tests/dtos_tests/test_audit_streaming.py`. `tests/patch_tests/test_patch_series.py` incorpora el 0019 a la serie.
- **Documentación:** `patches/README.md`, `README.md`, `run_tests.py` (solo el texto de ayuda), `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (92 archivos; `SHA256SUMS.txt` copia idéntica), SHA-256 `02107ad5afff9331b1f9df98c18c5e88f8bfd3f3138c6e473cdc337d6af45ea5`.

## g0.8 r27 (2026-11-10): parche 0020 (D-72) — backup en caliente con `VACUUM INTO` (cambia `src/`)
- **Origen:** pendiente de D-70. Detalle, medidas y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-72) y en `patches/README.md` (fila 19).
- **Código:** `src/dtos/store.py` (`SQLiteStore.backup_to`, `BackupResult`); en la serie (`patches/0020-…patch`).
- **Pruebas nuevas:** `tests/dtos_tests/test_backup.py`. `tests/patch_tests/test_patch_series.py` incorpora el 0020 a la serie.
- **Documentación:** `patches/README.md`, `README.md`, `run_tests.py` (solo el texto de ayuda), `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (93 archivos; `SHA256SUMS.txt` copia idéntica), SHA-256 `8bfe67add13705d09d2206aa96c8c124e0ec52b1bd3b760d17fbf5abd638719f`.
- **Corrección documental de r27 (2026-11-10):** `docs/decisiones/DECISIONES_v1_1_1.md` (D-72) pasa de «a ratificar» a ratificada (se mantiene `verify_chain=False` para respaldar bases con la cadena ya alterada). Sin cambios en `src/`, `config/`, `schemas/` ni `tests/`: el manifiesto no cambia.

## g0.8 r28 (2026-11-10): parche 0021 (D-73) — `journal_size_limit` explícito (cambia `src/`)
- **Origen:** pendiente de D-70 y consecuencia de D-72. Detalle, medidas y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-73) y en `patches/README.md` (fila 20).
- **Código:** `src/dtos/store.py` (`journal_size_limit_bytes`, `DEFAULT_JOURNAL_SIZE_LIMIT_BYTES`); en la serie (`patches/0021-…patch`).
- **Pruebas nuevas:** `tests/dtos_tests/test_journal_size_limit.py`. `tests/patch_tests/test_patch_series.py` incorpora el 0021 a la serie.
- **Documentación:** `patches/README.md`, `README.md`, `run_tests.py` (solo el texto de ayuda), `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (94 archivos; `SHA256SUMS.txt` copia idéntica), SHA-256 `4aa6f72011062299d3f0d42b5865086bfb3d2f9fff2dc10043aa23741debc685`.

## g0.8 r29 (2026-11-10): parche 0022 (D-74) — `busy_timeout_ms` y `journal_size_limit_bytes` parametrizables en UNAE, adaptador y capa proactiva (cambia `src/`)
- **Origen:** pendiente de D-70 y D-73. Detalle, medidas y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-74) y en `patches/README.md` (fila 21).
- **Código:** `src/unae/store.py` (en la serie: `patches/0022-…patch`), `src/adapter/adapter.py` y `src/proactive/shadow.py` (no son originales parchados y no figuran en la serie).
- **Pruebas:** nueva `tests/integrity_tests/test_sqlite_policy_params.py`; `tests/integrity_tests/test_connection_policy.py` (ahora mide también `journal_size_limit`) y `tests/patch_tests/test_patch_series.py` (incorpora el 0022).
- **Documentación:** `patches/README.md`, `README.md`, `run_tests.py` (solo el texto de ayuda), `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (95 archivos; `SHA256SUMS.txt` copia idéntica), SHA-256 `a462d867f77090c41db76e4d49a9dd403e2bf07396d91eebcd418f3bd8c8895b`.

## g0.8 r30 (2026-11-11): parche 0023 (D-75) — tope de `busy_timeout_ms` (2**31 - 1) y normalización a `int` exacto de la política de conexión en los cuatro almacenes (cambia `src/`)
- **Origen:** hallazgos 1 y 2 de la auditoría de SQLite de r29 (sobre D-74). Detalle, medidas y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-75) y en `patches/README.md` (fila 22).
- **Código:** `src/dtos/store.py` y `src/unae/store.py` (en la serie: `patches/0023-…patch`), `src/adapter/adapter.py` y `src/proactive/shadow.py` (no son originales parchados y no figuran en la serie).
- **Pruebas:** nueva `tests/integrity_tests/test_sqlite_policy_bounds.py` (10 pruebas sobre los cuatro almacenes); `tests/patch_tests/test_patch_series.py` incorpora el 0023.
- **Documentación:** `patches/README.md`, `README.md`, `run_tests.py` (solo el texto de ayuda), `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (96 archivos, +1 respecto a r29; `SHA256SUMS.txt` copia idéntica), SHA-256 `f2a5ee0a7a574e4f3cd52a48980704511b473cea6ddf6bcf4b125edf53135bf6`.

## g0.8 r31 (2026-11-11): parche 0024 (D-76) — reintento acotado de la apertura en UNAE, adaptador y capa proactiva (cambia `src/`)
- **Origen:** pendiente de D-67 (solo DTOS) y de D-74/D-75. Detalle, medidas y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-76) y en `patches/README.md` (fila 23). Con `busy_timeout_ms=0` no se reintenta (por diseño); no se midió una mejora real en Linux.
- **Código:** `src/unae/store.py` (en la serie: `patches/0024-…patch`), `src/adapter/adapter.py` y `src/proactive/shadow.py` (no son originales parchados y no figuran en la serie).
- **Pruebas:** nueva `tests/integrity_tests/test_open_retry_stores.py` (11 pruebas sobre los tres almacenes); `tests/patch_tests/test_patch_series.py` incorpora el 0024.
- **Documentación:** `patches/README.md`, `README.md`, `run_tests.py` (solo el texto de ayuda), `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (97 archivos, +1 respecto a r30; `SHA256SUMS.txt` copia idéntica), SHA-256 `b3608e6c67a5c5dbe5d3d9159f60e4f079425a28869bdcf5463252e6463e30b0`.

## g0.8 r32 (2026-11-11): cambio 0025 (D-77) — migración atómica de columnas y `PRAGMA busy_timeout` explícito en el adaptador, `ROLLBACK` seguro en la capa proactiva (cambia `src/`)
- **Origen:** hallazgos 1, 2 y 3 de la auditoría de SQLite de r31. Detalle, medidas y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-77).
- **Código:** `src/adapter/adapter.py` y `src/proactive/shadow.py` (no son originales parchados y no figuran en la serie; por eso NO hay archivo nuevo en `patches/` y la serie sigue en 0024).
- **Pruebas:** nueva `tests/integrity_tests/test_open_hardening_stores.py` (9 pruebas; contra r31 fallan: 14 fallos y 3 errores, contando subpruebas). `tests/patch_tests/test_patch_series.py` no cambia.
- **Documentación:** `README.md`, `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (98 archivos, +1 respecto a r31; `SHA256SUMS.txt` copia idéntica), SHA-256 `a456fa05b8bc208dd3d2f3d1492603394ae8425f3f86a86df6c6a4dfbba899dc`.

## g0.8 r33 (2026-11-11): D-78 — pruebas portables a Windows y guardia (cambia solo `tests/`)
- **Origen:** ejecución de r32 en Windows (Python 3.15.2) aportada por el usuario: 7 errores `PermissionError: [WinError 32]` al limpiar directorios temporales (2 en integrity, 5 en unae) y 1 script de dtos caído (`unable to open database file`). Detalle, medidas y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-78).
- **Código:** ninguno. `src/` y `patches/` no cambian; la serie sigue en 0024.
- **Pruebas:** `tests/integrity_tests/test_open_retry_stores.py` (2 sitios) y `tests/unae_tests/test_rollback_and_keys.py` (2 sitios): `with sqlite3.connect(...)` pasa a `with closing(sqlite3.connect(...)) as c, c:`; `tests/dtos_tests/test_journal_size_limit.py`: «con.db» pasa a «limite.db» (CON es un nombre de dispositivo reservado en Windows). Nueva `tests/integrity_tests/test_windows_portability.py` (4 pruebas; contra r32 fallan 2, con 5 hallazgos).
- **Documentación:** `README.md`, `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (99 archivos, +1 respecto a r32; cambian 3 líneas —los tres archivos de prueba modificados— y se añade la de `test_windows_portability.py`; `SHA256SUMS.txt` copia idéntica), SHA-256 `8d136484671787c3faed333a1b0f8f0ba356d0afea49f2c3f10c00cb2813ed93`.

## g0.8 r34 (2026-11-12): D-79 y D-80 — modo rápido de las pruebas de WAL y paridad FSM en proceso (cambia `tests/`, `run_tests.py` y documentación; `src/` y `patches/` no cambian)
- **Origen:** análisis de los registros de pruebas del usuario en hardware limitado (portátil de 2 GB de RAM; HDD y USB). Detalle, medidas y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-79, D-80).
- **Pruebas:** `tests/integrity_tests/test_sqlite_policy_params.py` (el cambio de una sola transacción del usuario, `wal_test_una_transaccion.diff`, más la carga completa/rápida y la clase `CargaDelModoRapido`, +2 pruebas), `tests/dtos_tests/test_journal_size_limit.py` (carga completa/rápida) y `tests/unae_tests/test_semantics.py` (`test_fsm_parity_with_dtos` omitida → 2 pruebas en proceso; ya no importa `os`).
- **Runner (fuera del alcance del manifiesto):** `run_tests.py`: opción `--fast`; `ALLOWED_SKIPS["unae"]` sin la omisión de paridad.
- **Documentación:** `README.md`, `meta/VERSION.md`, `docs/decisiones/DECISIONES_v1_1_1.md`.
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (99 archivos, sin cambios de lista; cambian los hashes de los tres archivos de prueba; `SHA256SUMS.txt` copia idéntica). Mismo comando de regeneración que en g0.6 r4.

## g0.8 r35 (2026-11-12): D-81 — CI en GitHub Actions (solo `.github/` y documentación; `src/`, `config/`, `schemas/`, `tests/` y `patches/` no cambian)
- `.github/workflows/ci.yml` (nuevo, fuera del alcance del manifiesto). Detalle y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-81).
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (99 archivos; sin cambios; `SHA256SUMS.txt` copia idéntica).

## g0.8 r36 (2026-11-12): D-82 — archivos para GitHub (`.gitattributes`, `.gitignore`, `CLAUDE.md`; `src/`, `config/`, `schemas/`, `tests/` y `patches/` no cambian)
- Archivos nuevos en la raíz, fuera del alcance del manifiesto. Detalle y límites en `docs/decisiones/DECISIONES_v1_1_1.md` (D-82).
- Manifiesto vigente: `meta/SHA256SUMS_v1_1_1.txt` (99 archivos; sin cambios; `SHA256SUMS.txt` copia idéntica).
