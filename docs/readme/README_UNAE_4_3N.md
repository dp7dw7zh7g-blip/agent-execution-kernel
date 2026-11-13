# UNAE 4.3N — concurrencia multiproceso e integridad de recursos

## Estado actual en este árbol (agent-execution-kernel, antes ia-personal; g0.6 r1)

- `src/unae/store.py` ya NO es copia del original aportado: se añadió un único método público, `Store.get_operation(operation_id)` (lectura de solo lectura del diario de ejecución, parche 0004; SHA-256 `3af567b53a88f11e` → `9b08606fdc50e714`). Lo usa el puente del adaptador en lugar de leer `execution_journal` directamente.
- `SCHEMA_VERSION = 4` (las menciones a la versión 2 más abajo son históricas).
- Pruebas: 62 en la suite de UNAE (61 ejecutadas y 1 omitida: paridad con `dtos/`, requiere `DTOS_FSM_JSON`), incluidas las de caída y concurrencia. Las 6 últimas son `tests/unae_tests/test_get_operation.py`.
- Cómo ejecutarlas aquí: `python3 run_tests.py unae` desde la raíz del árbol. El comando de la sección siguiente (`unittest discover` desde `unae43m`) corresponde al paquete original.
- El resto de `src/unae/` sigue siendo copia byte a byte del original.

Paquete de referencia local en Python + SQLite. Parte de la referencia 4.3M y añade pruebas de contención entre procesos independientes usando el modo `spawn` de `multiprocessing`.

## Pruebas añadidas

- Dos procesos compiten por una capacidad de cómputo que solo permite una reserva: debe ganar exactamente uno.
- Dos procesos envían la misma solicitud idempotente: debe crearse una única reserva y el segundo debe recibir `ALREADY_RESERVED`.
- Se verifican tanto el resultado de cada proceso como los contadores persistidos y el número de reservas.

## Ejecutar todas las pruebas

Desde la carpeta `unae43m`:

```bash
python -m unittest discover -s tests -v
```

## Interpretación y límites

Las pruebas de contención verifican la serialización local de SQLite y las condiciones de capacidad para estos escenarios. No demuestran resistencia ante todos los patrones de carga, sistemas de archivos de red, corrupción de almacenamiento ni fallos eléctricos. Tampoco prueban todavía el despachador outbox, autenticidad criptográfica de recibos, migraciones versionadas completas o efectos externos idempotentes.

Esta es una implementación de referencia experimental, no una certificación de producción.

## Parche posterior a 4.3N (reservas huérfanas)
- `release_reservation(op, token)`: cancela y libera una reserva RESERVED (nunca ejecutó).
- `acquire_authority` ahora, en la misma transacción, cancela las RESERVED del dueño anterior y pasa sus RUNNING a UNKNOWN (conservan recursos).
- `begin_reconciliation(op, token)`: UNKNOWN -> RECONCILING bajo la autoridad vigente; re-fencea para poder registrar recibo y hacer commit.
- Pruebas nuevas en `tests/test_release.py` (14 en total, todas pasan).
- `reserve_and_register` devuelve `OPERATION_CONFLICT` (en lugar de lanzar `IntegrityError`) si `operation_id` o `reservation_id` ya existen con otra clave.
- `expire_reservations(max_age_s, now=None)`: cancela reservas RESERVED antiguas y libera presupuesto (no toca RUNNING).
- `tests/test_concurrency.py`: ahora con `Barrier` entre procesos y 5 rondas por prueba, para que la contención sea real.
- Total: 16 pruebas, todas pasan.
- Sobreconsumo: decisión de diseño = el commit se acepta (la acción externa ya ocurrió), se registra en `resource_overruns` (`list_overruns`) y la cuenta queda bloqueada para nuevas reservas hasta que `account_headroom` vuelva a ser positivo. La contención real debe ocurrir en el proveedor (tope de cómputo/tiempo en la llamada).
- Total: 17 pruebas.
- Outbox (4.3P): `pending_events`, `dispatch_outbox(sender)` (llama al sender fuera de transacción; reintenta y pasa a DEAD tras `max_attempts`), `mark_event_sent/failed`. Garantía: **al menos una vez**; el consumidor deduplica con `consume_once(consumer_id, event_id, handler)`, que aplica el efecto y registra el inbox en una sola transacción.
- Migraciones (4.3Q): `SCHEMA_VERSION = 2` (en ese momento; hoy 4), tabla `_MIGRATIONS`, `Store.schema_version()`. Se aplican atómicamente al abrir (releyendo la versión bajo lock, seguro con arranque concurrente); una base más nueva que el código se rechaza sin tocarla. La v2 agrega `claimed_by` y `claim_expires_at` a `outbox`. Las bases 4.3N (v1) se migran conservando datos.
- Lease de eventos: `claim_events(worker_id, limit, lease_s)`; `dispatch_outbox(..., worker_id, lease_s)` ahora reclama antes de enviar, así dos despachadores vivos no envían el mismo evento. Si un worker pierde su lease (tardó más que `lease_s`) el evento se cuenta como `lost` y puede reenviarse: sigue siendo al menos una vez, el consumidor deduplica con `consume_once`. `dispatch_outbox` devuelve ahora `{"sent","failed","dead","lost"}`.
- Recibos autenticados (4.3R): `sign_receipt(secret, provider_id=..., ...)` (HMAC-SHA256, con separación de dominio) lo usa el adaptador del proveedor; `Store(path, keyring={provider_id: secreto | (secretos...)}, require_signed_receipts=False)` los verifica en `record_receipt(..., provider_id=, signature=)`. Los secretos viven en memoria, nunca en la base. La operación se liga al proveedor en `mark_running(..., provider_id=)`; una operación ligada exige recibo firmado por ese proveedor. Nuevos retornos: `BAD_SIGNATURE` (firma inválida, contenido alterado o proveedor desconocido) y `UNSIGNED_REJECTED`. Una tupla de secretos permite rotar claves. Migración v3 agrega `provider_id` (journal y receipts) y `signature`.
  - Alcance: autentica **quién** emitió el recibo y que **no fue alterado**; no prueba que el proveedor haya dicho la verdad sobre el consumo real. Sin `require_signed_receipts=True` el camino sin firma sigue disponible (compatibilidad 4.3N).
- Caídas (4.3S previa): `Store(..., fault_hook=)` invoca `hook("<método>:before_commit")` y `hook("<método>:after_commit")` en cada commit; `verify_integrity()` revisa los invariantes del libro mayor (contadores = reservas vivas, usado = recibos reconciliados, versión = commits, un evento de outbox por commit, journal = reservations, tokens). `tests/crash_scenario.py` ejecuta un ciclo completo (reserva, ejecución, recibo firmado, commit, cambio de autoridad, reconciliación, expiración, outbox) en un subproceso que se mata con SIGKILL en cada punto de inyección; `tests/test_crash.py` exige que (1) el estado quede consistente tal cual, y (2) un nuevo dueño pueda llevarlo a reposo sin reservas ni eventos retenidos. También prueba SIGKILL en medio de una migración de esquema.
  - Dos huecos reales encontrados por esta prueba y corregidos en `acquire_authority`: operaciones con recibo ya registrado (SUCCEEDED/FAILED) y operaciones en RECONCILING del dueño anterior quedaban huérfanas reteniendo recursos. Ahora se re-fencean y vuelven a UNKNOWN, respectivamente.
  - Límite: SIGKILL de proceso (atomicidad lógica sobre SQLite WAL); no simula pérdida de energía ni fallos de fsync. Los puntos de inyección están en los commits, no entre sentencias de una misma transacción.
- Rendimiento (4.3S): `bench/bench.py` (`PYTHONPATH=. python3 bench/bench.py [--quick]`) mide ciclos completos reserva -> running -> recibo -> commit, latencia por paso, escalado con 1/2/4/8 procesos y planes de consulta con 30 000 filas. El perfil mostró que ~30 % del tiempo era `Connection.close`: cada operación cerraba la última conexión y SQLite hacía checkpoint y borraba el `-wal` en cada llamada. Solución: `Store(keep_open=True)` (por defecto) mantiene una conexión ancla; `Store.close()` / `with Store(...)` la liberan. Nueva opción `synchronous="FULL"|"NORMAL"` (por defecto FULL; NORMAL con WAL puede perder los últimos commits ante un corte de energía, sin corromper la base).
  - Medido en un sandbox de 1 CPU (indicativo; mídelo en tu dispositivo): un proceso, ciclo completo (4 transacciones): FULL 107 -> 263 ciclos/s, NORMAL 112 -> 381 ciclos/s con ancla. Latencia p50 por transacción ~0,4-1,2 ms. Con 1/2/4/8 procesos la tasa total queda plana (FULL ~230-255, NORMAL ~320-375 ciclos/s) y la cola p99 del commit sube a decenas/cientos de ms: SQLite serializa a los escritores, así que más procesos no suman rendimiento, solo espera. Sin errores de bloqueo en ninguna corrida.
  - Consultas calientes: todas usan índice (30 000 filas, 4-10 microsegundos). No se agregaron índices nuevos porque no hacían falta.
  - Límites: no usar el Store a través de `fork()`; en Windows cerrar el Store (`close()`) antes de borrar la carpeta de la base.
- Reintento tras rotar el token: el `fencing_token` salió de la huella de idempotencia (es metadato de autoridad, no parte de la solicitud lógica); las filas guardadas por 4.3N con la huella antigua se siguen reconociendo. `reserve_and_register` ante una clave ya vista devuelve ahora: `KEY_CONFLICT` (otra solicitud con la misma clave, sin importar el token), `CANCELLED` (la solicitud fue liberada, expirada o descartada por un cambio de autoridad: usar nueva clave y nuevo `operation_id`), `STALE_AUTHORITY` (quien reintenta ya no tiene la autoridad) o `ALREADY_RESERVED`. Cambio de comportamiento respecto a 4.3N: un reintento con token viejo ya no recibe `ALREADY_RESERVED`.
- Revisión 2 (semántica): ver `registro_maestro_UNAE_4_3O_a_4_3S.md`. Esquema v4: `commit_log` guarda `outcome`, `attestation`, `via_reconciliation` (RECONCILED no prueba por sí solo que el efecto externo terminó bien: usa `list_unverified_settlements()`); la tabla de transiciones es única (`fsm_table()`) y la base la hace cumplir con triggers; la firma de recibos cubre `task_id` (`record_receipt(..., task_id=)`); `mark_running(..., provider_id=)` exige `provider_operation_id`; un recibo registrado pasa la operación a la autoridad vigente.
- Total (histórico, 4.3S rev3; hoy ver «Estado actual»): 55 pruebas (~10 s; 1 omitida: paridad con dtos/, requiere `DTOS_FSM_JSON`).
Pendiente: `audience` en la firma, paridad con `dtos/`, distribución segura del keyring, medir en el dispositivo real (Edge).
