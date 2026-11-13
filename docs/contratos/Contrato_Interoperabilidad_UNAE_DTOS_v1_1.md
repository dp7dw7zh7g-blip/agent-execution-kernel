CONTRATO DE INTEROPERABILIDAD UNAE 4.3S rev3 ↔ DTOS
Versión 1.1 — corrección de hallazgos críticos
Fecha: 2026-10-23
Estado: contrato de integración corregido a nivel de diseño; adaptador y pruebas end-to-end pendientes.

0. Alcance y preservación
Este documento corrige las ambigüedades críticas detectadas en la auditoría del contrato v1.0. No modifica UNAE rev2, UNAE rev3 ni el paquete original DTOS. Los cambios de código, si fueran necesarios, deberán vivir en una nueva versión independiente.

1. Decisión arquitectónica
UNAE y DTOS mantienen máquinas de estados separadas. No se fusionan FSM, no se equiparan estados por compartir nombre y ningún componente escribe directamente el estado interno del otro.
- UNAE es autoridad sobre el ciclo transaccional de la operación: RESERVED, RUNNING, UNKNOWN, RECONCILING, SUCCEEDED, FAILED, CANCELLED, RECONCILED.
- DTOS CognitiveCycleFSM gobierna el ciclo cognitivo; COMPLETED no demuestra éxito externo.
- DTOS EffectFSM gobierna el intento de efecto y su reconciliación.
- RecoveryTriggerFSM y BootstrapFSM gobiernan ciclos de recuperación/bootstrap; no sustituyen la reconciliación transaccional de UNAE.

2. Decisión crítica C-01: fencing_token
El sobre de integración define `fencing_token` como entero JSON no negativo (integer, minimum 0), nunca como string.
El adaptador es el único lugar autorizado a mapear el entero de UNAE al campo `EffectIntent.fencing_token` de DTOS, que actualmente es Optional[str]. La representación DTOS será la cadena decimal canónica del entero: sin signo, espacios, decimales ni ceros iniciales, salvo el valor "0".
Reglas:
- Rechazar booleanos, floats, strings recibidos por el protocolo, valores negativos y valores fuera del rango admitido por la implementación.
- Al enviar a DTOS, serializar el entero como cadena decimal canónica.
- Al volver desde DTOS, aceptar solo una cadena que cumpla ^(0|[1-9][0-9]*)$ y convertirla a entero.
- Nunca tratar el token como identidad de operación ni como clave de idempotencia.
- El token no es autorización por sí mismo: UNAE debe comprobar autoridad vigente en su propio límite de ejecución/commit.

3. Decisión crítica C-02: correlación durable operación–intención–intento
Identidades distintas:
- `message_id`: identidad única del mensaje.
- `correlation_id`: correlación de una interacción extremo a extremo.
- `operation_id`: identidad de operación UNAE.
- `effect_intent_id`: identidad de intención DTOS.
- `effect_attempt_id`: identidad de un intento concreto.
- `idempotency_key`: identidad estable de la operación lógica del proveedor.
El adaptador debe cargar la intención DTOS por `effect_intent_id` y verificar que su `operation_id` coincide exactamente con el `operation_id` del mensaje. Debe cargar el intento por `effect_attempt_id` y verificar que está asociado a la intención esperada. Si el esquema original de DTOS no permite demostrar la relación de forma durable, el adaptador mantiene un registro de correlación durable y append-only con las claves (effect_intent_id, effect_attempt_id, operation_id, provider, idempotency_key). Ante cualquier discrepancia, rechaza o pone en cuarentena el mensaje; nunca confía solo en el operation_id suministrado por quien llama a reconcile.
No se cambia el esquema del DTOS original. Cualquier extensión futura de esquema requiere nueva versión.

4. Decisión crítica C-03: política exacta para UNAE UNKNOWN
Regla general: UNKNOWN bloquea el retry ciego y la creación automática de una operación sustituta.
Política compatible con la API actual:
A. La vía normal para investigar un resultado incierto es `begin_reconciliation(operation_id, fencing_token)`, que mueve UNKNOWN → RECONCILING si la autoridad vigente lo permite.
B. Un recibo validado por `record_receipt()` puede resolver una operación en RUNNING, UNKNOWN o RECONCILING según la API actual. La integración puede usar esta vía directa desde UNKNOWN solo cuando el recibo está ligado inequívocamente a operation_id y reservation_id y supera las validaciones de integridad/firma exigidas por la configuración de UNAE. La aceptación del recibo no autoriza por sí sola el commit ni un retry.
C. Si el recibo no es concluyente, no es verificable o hay contradicción, mantener UNKNOWN/RECONCILING y continuar investigación o escalar a revisión humana.
D. Un reintento del mismo efecto lógico solo se permite tras resolución documentada y cuando las garantías de idempotencia del proveedor estén explícitas. No crear un operation_id nuevo para eludir la incertidumbre.
La integración debe registrar cuál de las vías A o B se utilizó.

5. Decisión crítica C-04: deduplicación e idempotencia
DTOS Outbox puede entregar al menos una vez; por tanto, el receptor debe tolerar redelivery. Para la primera versión del adaptador:
- `message_id` es único e inmutable por mensaje lógico y se usa como `dedup_key` del Inbox receptor.
- En un redelivery del mismo mensaje, conservar exactamente el mismo `message_id` y el mismo payload canónico.
- Si un mismo `message_id` reaparece con payload diferente, ponerlo en cuarentena como conflicto de integridad; no procesarlo como duplicado benigno.
- `operation_id` no es `message_id`; una operación puede tener muchos mensajes.
- `idempotency_key` se mantiene estable para la misma intención lógica y no se cambia solo para sortear un UNKNOWN.
- `effect_attempt_id` es nuevo por cada intento autorizado; no sustituye la idempotency_key.
- El adaptador guarda el resultado del consumo junto a message_id de forma atómica con su efecto local. Si el transporte no permite atomicidad con el destino, usar inbox/outbox durable y reconciliación; no afirmar exactly-once global.

6. Sobre de mensaje v1.1
El JSON Schema `unae_dtos_message.schema.json` adjunto es normativo para la envoltura del mensaje. Las reglas condicionales por tipo son:
- OPERATION_REQUEST: operation_id, correlation_id, message_id y schema_version obligatorios; puede incluir cognitive_cycle_id y authority_ref.
- EFFECT_DISPATCHED: operation_id, effect_intent_id, effect_attempt_id, idempotency_key y fencing_token obligatorios.
- EFFECT_RESULT: operation_id, effect_intent_id, effect_attempt_id, idempotency_key, result y evidence_refs obligatorios.
- RECONCILIATION_RESULT: operation_id, result, evidence_refs y correlation_id obligatorios.
- RECOVERY_NOTICE: correlation_id, result y evidence_refs obligatorios; operation_id es obligatorio si el aviso afecta una operación concreta.
Los mensajes desconocidos o con contract_version incompatible se rechazan o quedan en cuarentena. No se interpretan silenciosamente.

7. Evidencia y recibos
`evidence_refs` debe ser una lista de referencias durables a evidencia inmutable. No se acepta una lista vacía para resolver una incertidumbre. Una referencia debe permitir recuperar el artefacto, comprobar su integridad y determinar quién lo produjo y cuándo.
`EffectStatus.CONFIRMED` no fuerza UNAE SUCCEEDED. Un recibo del proveedor no fuerza UNAE RECONCILED. RECONCILED certifica el cierre transaccional local según UNAE, no la verdad universal del proveedor. La autenticidad depende de las verificaciones de recibo configuradas en UNAE.

8. Invariantes normativos
I-01 FSM separadas; sin escrituras cruzadas.
I-02 Sin autorización implícita desde COMPLETED, READY_TO_COMMIT o COMMITTING.
I-03 Correlación durable entre operación, intención e intento.
I-04 UNKNOWN es local a cada FSM.
I-05 No retry ciego.
I-06 Resolver incertidumbre solo mediante mecanismo protegido y evidencia suficiente.
I-07 NOT_FOUND por sí solo no significa REJECTED ni ausencia de efecto.
I-08 CONFIRMED no propaga automáticamente SUCCEEDED.
I-09 Respuesta externa aislada no propaga RECONCILED.
I-10 Validar autoridad/fencing en el límite de ejecución/commit.
I-11 Redelivery no duplica operación ni efecto lógico.
I-12 Auditar actor, motivo, timestamps, IDs y evidencia.
I-13 Rechazar/cuarentenar versiones incompatibles y message_id con payload conflictivo.
I-14 Ante discrepancias, fail closed: no inventar resultado.

9. Pruebas de aceptación mínimas
AT-01 CognitiveCycle COMPLETED sin autorización no provoca dispatch.
AT-02 message_id duplicado con payload idéntico se consume una sola vez.
AT-03 mismo message_id con payload diferente se cuarentena.
AT-04 fencing_token negativo, float, bool o string de protocolo se rechaza.
AT-05 token se convierte entre entero UNAE y string decimal DTOS solo en el adaptador.
AT-06 operación de la intención no coincide con operation_id del mensaje: rechazo/cuarentena.
AT-07 intento no pertenece a la intención indicada: rechazo/cuarentena.
AT-08 UNKNOWN no genera retry automático ni operación sustituta.
AT-09 recibo firmado/verificable válido desde UNKNOWN puede registrarse por record_receipt según configuración; el commit sigue sujeto a autoridad y validaciones.
AT-10 evidencia vacía o estrategia de reconciliación desconocida no resuelve efecto DTOS.
AT-11 NOT_FOUND no se convierte automáticamente en REJECTED.
AT-12 lease/fencing inválido bloquea el dispatch/commit.
AT-13 caída entre dispatch y recibo no genera repetición ciega tras reinicio.
AT-14 efecto CONFIRMED no fuerza UNAE SUCCEEDED/RECONCILED.
AT-15 versiones incompatibles quedan rechazadas o en cuarentena.
AT-16 pruebas unitarias y de integración se reportan por separado.

10. Estado de cierre
Este v1.1 resuelve las decisiones normativas críticas de tipo de fencing, correlación, UNKNOWN y deduplicación. No afirma que exista una integración implementada. Antes de declarar integración validada, debe implementarse el adaptador, aplicar el JSON Schema, añadir persistencia durable de correlación si hace falta y ejecutar AT-01…AT-16.
Los originales UNAE rev2, UNAE rev3 y DTOS permanecen inmutables.
