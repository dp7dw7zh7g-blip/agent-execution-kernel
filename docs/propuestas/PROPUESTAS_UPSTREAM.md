# Propuestas para los paquetes originales

P-1, P-2 y P-3 están ADOPTADOS en `src/dtos/` desde g0.4 r5 (D-34) y P-5 parcialmente desde r6 (D-35); registro en `patches/`. P-4 desde r7 (D-36), único cambio en UNAE.

## P-1. DTOS: `EffectRepository.reconcile` atómico — ADOPTADO (parche 0001) (hallazgo H-4, verificado)
`commit_effect_attempt_transition()` e `insert_effect()` abren una transacción cada una y `SQLiteStore.transaction()` no es reentrante. Hay DOS vías a un
intento terminal sin fila en `effects`: (a) una caída entre ambas llamadas; (b) sin caída, cuando el ledger rechaza la inserción (p. ej. la clave
`(provider, idempotency_key)` ya existe): el estado ya cambió. El parche añade `SQLiteStore.reconcile_effect_attempt_atomic` (estado + ledger en una sola
transacción, con la misma traducción de `IntegrityError` a `ConflictError` que `insert_effect`) y `reconcile()` lo usa.

## P-2. DTOS: `canonical_json` rechaza NaN/Infinity — ADOPTADO (parche 0002) (verificado)
`_reject_non_finite` nunca se llamaba. El parche añade `allow_nan=False` y CONSERVA `default=_json_default`.

## P-3. DTOS: lectura pública del ledger — ADOPTADO (incluido en 0001) (hallazgo H-2)
`SQLiteStore.get_effects_by_attempt(attempt_id)`. El puente la usa si existe y, si no, cae en `_conn`. Se consulta por INTENTO, no por operación.

## P-4. UNAE: consulta pública de una operación — ADOPTADO (parche 0004, hallazgo H-3)
El puente lee `execution_journal` directamente. Propuesta: `Store.get_operation(operation_id) -> dict | None` con `operation_id, task_id, reservation_id, status, fencing_token`.

## P-5. DTOS: persistir la evidencia de la reconciliación — ADOPTADO PARCIALMENTE (parche 0003, hallazgo H-5)
La evidencia de los resultados terminales queda en `effects.evidence` (misma transacción que el estado). Sin cubrir: PARTIAL, inconcluyentes y rechazos, que no escriben ledger; su evidencia sigue solo en `reconciliation_log` del adaptador.

## Pendiente de decisión (no es código)
- `fencing_token` > 2^53 pierde precisión en consumidores JavaScript; ninguno de los dos canonicalizadores es JCS (RFC 8785). Irrelevante si ambos extremos son Python; si no, hay que fijar un tope o codificar el token como texto en el contrato.
