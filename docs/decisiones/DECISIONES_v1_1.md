# Registro de decisiones v1.1

- D-01 (C-01): `fencing_token` en el protocolo es entero JSON no negativo. Adaptador serializa a string decimal canónica solo al entrar en el modelo DTOS actual.
- D-02 (C-02): no confiar en `operation_id` aportado al reconciliar sin verificarlo contra la intención persistida y el intento. Si no puede probarse la relación, usar registro durable append-only del adaptador y cuarentena ante conflicto.
- D-03 (C-03): vía normal UNKNOWN→RECONCILING; se permite el camino compatible de recibo validado desde UNKNOWN mediante `record_receipt()` solo con evidencia ligada a operación/reserva y validaciones de firma según configuración. No autoriza commit automático.
- D-04 (C-04): `message_id` es `dedup_key` estable del inbox. Payload distinto con mismo `message_id` es conflicto, no duplicado benigno. Separar message_id, operation_id, effect_attempt_id e idempotency_key.
- D-05: no se modifica ningún paquete original; la integración se implementa en versión independiente.
