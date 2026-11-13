-- schema.sql
-- ==========
-- Schema baseline (OPEN-007 resuelto, corrección 176 del Registro Maestro).
-- Reglas impuestas AQUÍ, a nivel de SQLite, no solo en Python:
--   1. Toda tabla artifact-like: UNIQUE(artifact_id, version, content_hash).
--   2. Ninguna tabla de estado se actualiza sin expected_version en el
--      WHERE — eso lo garantiza el código (store.py), pero aquí se
--      garantiza que version existe y es NOT NULL en todas ellas.
--   3. Tablas de historia (audit_log, effects, reasoning_runs) son
--      append-only: triggers rechazan UPDATE/DELETE, no solo permisos de
--      aplicación (P-065).
--   4. GENESIS_AUTHORITY_RECORD: UNIQUE(installation_id) — P-062/P-063,
--      la unicidad de la autoridad génesis sobrevive a un bug de
--      concurrencia en Python porque el constraint vive en storage.

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ---------------------------------------------------------------------
-- REG-001 Components
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS components (
    component_id    TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    responsibility  TEXT NOT NULL,
    lifecycle       TEXT NOT NULL,
    dependencies    TEXT NOT NULL,      -- JSON array de component_id
    criticality     TEXT NOT NULL,
    version         TEXT NOT NULL
);

-- ---------------------------------------------------------------------
-- REG-002 Contracts
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contracts (
    contract_id     TEXT NOT NULL,
    version         TEXT NOT NULL,
    producer        TEXT NOT NULL,
    consumer        TEXT NOT NULL,
    schema_hash     TEXT NOT NULL,
    compatibility   TEXT NOT NULL,
    PRIMARY KEY (contract_id, version)
);

-- ---------------------------------------------------------------------
-- REG-003 Providers
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS providers (
    provider_id     TEXT PRIMARY KEY,
    family          TEXT NOT NULL,
    version         TEXT NOT NULL,
    location        TEXT NOT NULL,
    capabilities    TEXT NOT NULL,      -- JSON
    resources       TEXT NOT NULL,      -- JSON
    status          TEXT NOT NULL
);

-- ---------------------------------------------------------------------
-- REG-004 Policies
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS policies (
    policy_id       TEXT NOT NULL,
    version         TEXT NOT NULL,
    scope           TEXT NOT NULL,
    effective_from  TEXT NOT NULL,
    hash            TEXT NOT NULL,
    -- Parche 0008 (D-51): texto JSON canónico de la política; NULL = política sin contenido (las filas anteriores al parche).
    -- Si no es NULL, `hash` == SHA-256 de este texto en UTF-8 (lo verifica PolicyRecord, no SQLite).
    content         TEXT,
    PRIMARY KEY (policy_id, version)
);

-- Parche 0009 (D-52): las políticas son INMUTABLES. Un cambio es una versión nueva (otra fila), nunca un UPDATE: así un rule_hash
-- guardado en un mandato apunta siempre a un contenido que no cambia por debajo.
CREATE TRIGGER IF NOT EXISTS trg_policies_no_update
BEFORE UPDATE ON policies
BEGIN
    SELECT RAISE(ABORT, 'policies es append-only (D-52): UPDATE prohibido, registrar una nueva versión');
END;

CREATE TRIGGER IF NOT EXISTS trg_policies_no_delete
BEFORE DELETE ON policies
BEGIN
    SELECT RAISE(ABORT, 'policies es append-only (D-52): DELETE prohibido');
END;

-- Parche 0009 r3 (D-52c): `INSERT OR REPLACE` / `REPLACE INTO` resuelve un conflicto de clave BORRANDO la fila previa, y con
-- `recursive_triggers = 0` (valor por defecto de SQLite) ese borrado NO dispara trg_*_no_delete: sin esta guarda, la fila «inmutable»
-- se sobrescribía en silencio. Un BEFORE INSERT sí se ejecuta antes de resolver el conflicto, para INSERT, REPLACE, OR IGNORE y upsert.
CREATE TRIGGER IF NOT EXISTS trg_policies_no_replace
BEFORE INSERT ON policies
WHEN EXISTS (SELECT 1 FROM policies WHERE policy_id = NEW.policy_id AND version = NEW.version)
BEGIN
    SELECT RAISE(ABORT, 'policies es append-only (D-52c): la (policy_id, version) ya existe; registrar una nueva versión');
END;

-- ---------------------------------------------------------------------
-- REG-005 Authorities  (incluye GENESIS_AUTHORITY_RECORD, epoch=1)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS authorities (
    authority_id    TEXT PRIMARY KEY,
    installation_id TEXT NOT NULL,
    principal       TEXT NOT NULL,
    epoch           INTEGER NOT NULL CHECK (epoch >= 0),
    capabilities    TEXT NOT NULL,      -- JSON array
    expiry          TEXT,
    fencing         INTEGER NOT NULL CHECK (fencing >= 0),
    is_genesis      INTEGER NOT NULL DEFAULT 0 CHECK (is_genesis IN (0, 1))
);

-- P-062/P-063: como máximo UNA fila genesis por instalación. El índice
-- parcial solo aplica UNIQUE sobre filas con is_genesis = 1, así las
-- autoridades normales (epoch > 1, rotadas) no compiten por este slot.
CREATE UNIQUE INDEX IF NOT EXISTS uq_genesis_per_installation
    ON authorities (installation_id)
    WHERE is_genesis = 1;

-- ---------------------------------------------------------------------
-- REG-006 Operations
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS operations (
    operation_id            TEXT PRIMARY KEY,
    semantic_fingerprint    TEXT NOT NULL,
    status                  TEXT NOT NULL,
    goal_id                 TEXT NOT NULL,
    task_id                 TEXT NOT NULL,
    version                 INTEGER NOT NULL DEFAULT 1
);

-- ---------------------------------------------------------------------
-- REG-007 Tasks
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tasks (
    task_id             TEXT PRIMARY KEY,
    goal_id              TEXT NOT NULL,
    status               TEXT NOT NULL,
    current_cycle_id     TEXT,
    version               INTEGER NOT NULL DEFAULT 1
);

-- ---------------------------------------------------------------------
-- REG-008 Cognitive Cycles  (ejemplo de referencia para optimistic
-- concurrency: expected_version en el WHERE, rows_affected verificado
-- en Python — store.py)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cognitive_cycles (
    cycle_id        TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL,
    state           TEXT NOT NULL,
    cycle_version   INTEGER NOT NULL DEFAULT 1,
    state_hash      TEXT NOT NULL,
    context_hash    TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

-- ---------------------------------------------------------------------
-- REG-009 Reasoning Runs — APPEND-ONLY (historia de invocaciones reales)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reasoning_runs (
    request_id          TEXT PRIMARY KEY,
    provider_id         TEXT NOT NULL,
    provider_version    TEXT NOT NULL,
    input_hash          TEXT NOT NULL,
    output_hash         TEXT NOT NULL,
    status              TEXT NOT NULL,
    iterations          INTEGER NOT NULL,
    duration            INTEGER NOT NULL,
    recorded_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TRIGGER IF NOT EXISTS trg_reasoning_runs_no_update
BEFORE UPDATE ON reasoning_runs
BEGIN
    SELECT RAISE(ABORT, 'reasoning_runs es append-only (P-065): UPDATE prohibido');
END;

CREATE TRIGGER IF NOT EXISTS trg_reasoning_runs_no_delete
BEFORE DELETE ON reasoning_runs
BEGIN
    SELECT RAISE(ABORT, 'reasoning_runs es append-only (P-065): DELETE prohibido');
END;

-- ---------------------------------------------------------------------
-- REG-010 Artifacts — identidad = artifact_id + version + content_hash
-- (P-039). APPEND-ONLY: una corrección crea fila nueva (CORRECTS/
-- SUPERSEDES), nunca se pisa la anterior.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id     TEXT NOT NULL,
    version         INTEGER NOT NULL,
    artifact_class  TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    provenance      TEXT NOT NULL,
    status          TEXT NOT NULL,
    corrects        TEXT,               -- artifact_id anterior, si aplica
    supersedes      TEXT,               -- artifact_id anterior, si aplica
    recorded_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (artifact_id, version, content_hash)
);

CREATE TRIGGER IF NOT EXISTS trg_artifacts_no_update
BEFORE UPDATE ON artifacts
BEGIN
    SELECT RAISE(ABORT, 'artifacts es append-only (P-065): UPDATE prohibido, crear nueva version');
END;

CREATE TRIGGER IF NOT EXISTS trg_artifacts_no_delete
BEFORE DELETE ON artifacts
BEGIN
    SELECT RAISE(ABORT, 'artifacts es append-only (P-065): DELETE prohibido, usar retention/archive');
END;

-- ---------------------------------------------------------------------
-- REG-011 Evidence
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS evidence (
    evidence_id      TEXT PRIMARY KEY,
    source            TEXT NOT NULL,
    observation_id    TEXT NOT NULL,
    freshness         TEXT NOT NULL,
    reliability       TEXT NOT NULL,
    contradictions    TEXT NOT NULL DEFAULT '[]'   -- JSON array
);

-- ---------------------------------------------------------------------
-- REG-012 Effects — APPEND-ONLY (P-065)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS effects (
    effect_id           TEXT PRIMARY KEY,
    operation_id        TEXT NOT NULL,
    intent_id           TEXT NOT NULL,
    attempt_id          TEXT NOT NULL,
    provider             TEXT NOT NULL,
    idempotency_key       TEXT NOT NULL,
    status                TEXT NOT NULL,
    recorded_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    evidence              TEXT   -- JSON canónico de la evidencia con la que se resolvió el intento (NULL en filas anteriores o escritas sin evidencia)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_effects_idempotency
    ON effects (provider, idempotency_key);

CREATE TRIGGER IF NOT EXISTS trg_effects_no_update
BEFORE UPDATE ON effects
BEGIN
    SELECT RAISE(ABORT, 'effects es append-only (P-065): UPDATE prohibido, registrar nuevo EffectAttempt');
END;

CREATE TRIGGER IF NOT EXISTS trg_effects_no_delete
BEFORE DELETE ON effects
BEGIN
    SELECT RAISE(ABORT, 'effects es append-only (P-065): DELETE prohibido');
END;

-- ---------------------------------------------------------------------
-- effect_reconciliations — historial APPEND-ONLY de cada reconciliación
-- aceptada por la FSM, terminal o no (CONFIRMED, REJECTED, PARTIAL,
-- UNKNOWN). Existe porque `effects` guarda UNA fila por efecto lógico
-- (uq_effects_idempotency) y solo para resultados terminales: una fila
-- PARTIAL ahí bloquearía para siempre la fila terminal del intento que
-- complete el efecto con la misma idempotency_key. La evidencia (JSON
-- canónico, máx. MAX_EVIDENCE_SIZE_BYTES) se escribe en la MISMA
-- transacción que el cambio de estado del intento.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS effect_reconciliations (
    reconciliation_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id          TEXT NOT NULL,
    from_status         TEXT NOT NULL,
    to_status           TEXT NOT NULL,
    strategy            TEXT NOT NULL,
    evidence            TEXT NOT NULL,
    recorded_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS ix_effect_reconciliations_attempt
    ON effect_reconciliations (attempt_id);

CREATE TRIGGER IF NOT EXISTS trg_effect_reconciliations_no_update
BEFORE UPDATE ON effect_reconciliations
BEGIN
    SELECT RAISE(ABORT, 'effect_reconciliations es append-only: UPDATE prohibido');
END;

CREATE TRIGGER IF NOT EXISTS trg_effect_reconciliations_no_delete
BEFORE DELETE ON effect_reconciliations
BEGIN
    SELECT RAISE(ABORT, 'effect_reconciliations es append-only: DELETE prohibido');
END;

-- ---------------------------------------------------------------------
-- REG-013 Recovery
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recovery_runs (
    recovery_run_id    TEXT PRIMARY KEY,
    epoch               INTEGER NOT NULL,
    incarnation         INTEGER NOT NULL,
    leadership          TEXT NOT NULL,
    checkpoint          TEXT NOT NULL,
    status               TEXT NOT NULL
);

-- ---------------------------------------------------------------------
-- REG-014 Invariants — el puente P-xxx/I-xxx <-> evaluador real
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS invariants (
    invariant_id    TEXT PRIMARY KEY,   -- "P-013" o "I-003"
    severity        TEXT NOT NULL,
    scope           TEXT NOT NULL,
    evaluator       TEXT NOT NULL,
    policy          TEXT NOT NULL
);

-- ---------------------------------------------------------------------
-- REG-015 Projections
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS projections (
    projection_id    TEXT NOT NULL,
    version           INTEGER NOT NULL,
    checkpoint        TEXT NOT NULL,
    status            TEXT NOT NULL,
    hash              TEXT NOT NULL,
    PRIMARY KEY (projection_id, version)
);

-- ---------------------------------------------------------------------
-- REG-016 Retention
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retention (
    artifact_id             TEXT PRIMARY KEY,
    retention_class          TEXT NOT NULL,
    references_json           TEXT NOT NULL DEFAULT '[]',
    deletion_eligibility       INTEGER NOT NULL DEFAULT 0 CHECK (deletion_eligibility IN (0, 1))
);

-- ---------------------------------------------------------------------
-- REG-017 Mandates (parche 0009, D-52) — UN mandato por Task, inmutable.
-- Alcance y fuerza concedidos a un trabajo. Si trigger_kind = SENSOR, la regla de política es obligatoria.
-- La regla (rule_id, policy_id, policy_version, rule_hash) va completa o ausente. Las combinaciones (strength, trigger_kind) las valida
-- el DTO (Python), no SQL. El contenido de la regla NO se verifica aquí: llega con la revalidación (parche 0011).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mandates (
    mandate_id      TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL UNIQUE REFERENCES tasks (task_id),
    strength        TEXT NOT NULL CHECK (strength IN ('EXPLICIT', 'INFERRED')),
    trigger_kind    TEXT NOT NULL CHECK (trigger_kind IN ('USER', 'AGENT', 'SENSOR')),
    rule_id         TEXT,
    policy_id       TEXT,
    policy_version  TEXT,
    rule_hash       TEXT,
    created_at      TEXT NOT NULL,
    CHECK ((rule_id IS NULL AND policy_id IS NULL AND policy_version IS NULL AND rule_hash IS NULL)
        OR (rule_id IS NOT NULL AND policy_id IS NOT NULL AND policy_version IS NOT NULL AND rule_hash IS NOT NULL)),
    CHECK (trigger_kind <> 'SENSOR' OR rule_id IS NOT NULL),
    FOREIGN KEY (policy_id, policy_version) REFERENCES policies (policy_id, version)
);

CREATE TRIGGER IF NOT EXISTS trg_mandates_no_update
BEFORE UPDATE ON mandates
BEGIN
    SELECT RAISE(ABORT, 'mandates es append-only (D-52): UPDATE prohibido');
END;

CREATE TRIGGER IF NOT EXISTS trg_mandates_no_delete
BEFORE DELETE ON mandates
BEGIN
    SELECT RAISE(ABORT, 'mandates es append-only (D-52): DELETE prohibido');
END;

-- D-52c: REPLACE puede borrar el mandato previo por conflicto en mandate_id (PK) o en task_id (UNIQUE); se guardan ambas claves.
CREATE TRIGGER IF NOT EXISTS trg_mandates_no_replace
BEFORE INSERT ON mandates
WHEN EXISTS (SELECT 1 FROM mandates WHERE mandate_id = NEW.mandate_id OR task_id = NEW.task_id)
BEGIN
    SELECT RAISE(ABORT, 'mandates es append-only (D-52c): el mandate_id o la Task ya tienen mandato; no se reemplaza');
END;

-- mandate_events — historial APPEND-ONLY del mandato. El tipo lo valida el DTO/Store (enum MandateEventType), no SQL, para que
-- añadir tipos (parche 0011) no exija migrar la tabla. payload = JSON canónico.
CREATE TABLE IF NOT EXISTS mandate_events (
    event_id        TEXT PRIMARY KEY,
    mandate_id      TEXT NOT NULL REFERENCES mandates (mandate_id),
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_mandate_events_mandate ON mandate_events (mandate_id, created_at);

CREATE TRIGGER IF NOT EXISTS trg_mandate_events_no_update
BEFORE UPDATE ON mandate_events
BEGIN
    SELECT RAISE(ABORT, 'mandate_events es append-only (D-52): UPDATE prohibido');
END;

CREATE TRIGGER IF NOT EXISTS trg_mandate_events_no_delete
BEFORE DELETE ON mandate_events
BEGIN
    SELECT RAISE(ABORT, 'mandate_events es append-only (D-52): DELETE prohibido');
END;

-- D-52c: ver trg_policies_no_replace.
CREATE TRIGGER IF NOT EXISTS trg_mandate_events_no_replace
BEFORE INSERT ON mandate_events
WHEN EXISTS (SELECT 1 FROM mandate_events WHERE event_id = NEW.event_id)
BEGIN
    SELECT RAISE(ABORT, 'mandate_events es append-only (D-52c): el event_id ya existe; no se reemplaza');
END;

-- ---------------------------------------------------------------------
-- effect_attempts — estado MUTABLE del ciclo de vida de un intento de
-- efecto en curso (INTENDED -> DISPATCHED -> ... -> CONFIRMED/REJECTED).
-- Distinto de `effects` (que es el ledger append-only del resultado ya
-- resuelto). Mismo patrón de optimistic concurrency que
-- cognitive_cycles: expected_version en el WHERE del UPDATE.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS effect_attempts (
    attempt_id          TEXT PRIMARY KEY,
    intent_id            TEXT NOT NULL,
    provider              TEXT NOT NULL,
    idempotency_key         TEXT NOT NULL,
    status                  TEXT NOT NULL,
    version                  INTEGER NOT NULL DEFAULT 1
);

-- ---------------------------------------------------------------------
-- recovery_triggers — estado MUTABLE del ciclo de vida de un
-- RecoveryTrigger (NONE -> SUSPECT -> DECLARED -> RECOVERY_STARTING).
-- Mismo patrón de optimistic concurrency.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recovery_triggers (
    trigger_id                TEXT PRIMARY KEY,
    suspected_node_id          TEXT NOT NULL,
    incarnation_at_suspicion    INTEGER NOT NULL,
    evidence                     TEXT NOT NULL,   -- JSON
    declared_by                   TEXT,
    state                          TEXT NOT NULL,
    version                         INTEGER NOT NULL DEFAULT 1
);
-- ---------------------------------------------------------------------
-- Inbox / Outbox (FASE 1.7 del documento de actualizaciones, ya
-- integrada conceptualmente al Registro Maestro). Durabilidad de
-- eventos vive aquí, nunca en el transporte (P-066).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox (
    outbox_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type        TEXT NOT NULL,
    payload            TEXT NOT NULL,        -- JSON canónico (JCS)
    dependency_epoch    INTEGER NOT NULL DEFAULT 0,
    published           INTEGER NOT NULL DEFAULT 0 CHECK (published IN (0, 1)),
    recorded_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- D-70 (parche 0018): índice PARCIAL de la cola. `get_pending_outbox` filtra `published = 0` y ordena por `outbox_id`; sin este índice recorría
-- TODA la tabla (con 200 000 eventos ya publicados: 7,8 ms por consulta frente a 0,01 ms) y el coste crecía sin tope porque el outbox no se
-- purgaba. Al ser parcial solo indexa los pendientes (la cola real), así que no engorda con el histórico. IF NOT EXISTS: las bases existentes
-- lo reciben al abrirse.
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox (outbox_id) WHERE published = 0;

-- D-65 (parche 0014): fallos de publicación y cuarentena (dead-letter) del outbox. Un evento que no se puede publicar nunca
-- (payload no JSON) o que agotó sus intentos pasa a quarantined = 1 y deja de ocupar la cabeza de la cola; la fila de `outbox`
-- no se toca (el hecho sigue ahí para la revisión manual). No hay clave foránea: la cuarentena no debe bloquear nada.
CREATE TABLE IF NOT EXISTS outbox_failures (
    outbox_id        INTEGER PRIMARY KEY,
    attempts          INTEGER NOT NULL CHECK (attempts >= 1),
    last_error         TEXT NOT NULL,
    quarantined         INTEGER NOT NULL DEFAULT 0 CHECK (quarantined IN (0, 1)),
    updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- D-65: exclusión mutua del publicador del outbox entre procesos, portable (sustituye al flock del 0013, que no existe en Windows).
-- Una sola fila: quien tiene un lease no vencido es el único publicador; el lease se renueva antes de cada envío y caduca solo.
CREATE TABLE IF NOT EXISTS outbox_publisher_lease (
    lease_name       TEXT PRIMARY KEY CHECK (lease_name = 'outbox'),
    holder            TEXT NOT NULL,
    expires_at         REAL NOT NULL
);

-- D-66 (parche 0015): bitácora APPEND-ONLY de los reencolados de eventos en cuarentena (`SQLiteStore.requeue_quarantined`). Una cuarentena por
-- un fallo transitorio (p. ej. una caída larga del destino) debe poder revertirse sin editar SQL a mano, y cada reversión deja rastro: quién,
-- por qué y qué contador/error tenía el evento. Sin clave foránea (como outbox_failures) y sin UPDATE ni DELETE (triggers).
CREATE TABLE IF NOT EXISTS outbox_requeues (
    requeue_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    outbox_id          INTEGER NOT NULL,
    requeued_by         TEXT NOT NULL CHECK (length(trim(requeued_by)) > 0),
    reason               TEXT NOT NULL CHECK (length(trim(reason)) > 0),
    previous_attempts     INTEGER NOT NULL,
    previous_error         TEXT NOT NULL,
    requeued_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE TRIGGER IF NOT EXISTS trg_outbox_requeues_no_update BEFORE UPDATE ON outbox_requeues
BEGIN SELECT RAISE(ABORT, 'outbox_requeues es append-only (D-66): no se actualiza'); END;
CREATE TRIGGER IF NOT EXISTS trg_outbox_requeues_no_delete BEFORE DELETE ON outbox_requeues
BEGIN SELECT RAISE(ABORT, 'outbox_requeues es append-only (D-66): no se borra'); END;

CREATE TABLE IF NOT EXISTS inbox (
    dedup_key        TEXT PRIMARY KEY,     -- idempotency/dedup identity
    event_type        TEXT NOT NULL,
    payload            TEXT NOT NULL,
    processed           INTEGER NOT NULL DEFAULT 0 CHECK (processed IN (0, 1)),
    recorded_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- ---------------------------------------------------------------------
-- Audit log — hash chain, APPEND-ONLY estricto (P-065 + sección AUDIT
-- INTEGRITY del Registro Maestro). No se "repara" un fork/corrupción
-- borrando filas: eso violaría esta misma tabla.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_log (
    seq              INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_hash        TEXT NOT NULL,
    prev_hash          TEXT NOT NULL,
    payload             TEXT NOT NULL,
    recorded_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TRIGGER IF NOT EXISTS trg_audit_log_no_update
BEFORE UPDATE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log es append-only estricto: UPDATE prohibido incluso para "reparar"');
END;

CREATE TRIGGER IF NOT EXISTS trg_audit_log_no_delete
BEFORE DELETE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log es append-only estricto: DELETE prohibido incluso para "reparar"');
END;

-- ---------------------------------------------------------------------
-- D-52d: guardas anti-REPLACE en las cinco tablas históricas restantes (artifacts, audit_log, effects, effect_reconciliations,
-- reasoning_runs). Mismo mecanismo que D-52c: con `recursive_triggers = 0` un `INSERT OR REPLACE` / `REPLACE INTO` borra la fila en
-- conflicto SIN disparar trg_*_no_delete y la sobrescribe en silencio. Un BEFORE INSERT se ejecuta antes de resolver el conflicto
-- (INSERT, REPLACE, OR IGNORE y upsert). Se guardan TODAS las claves únicas de cada tabla, porque REPLACE resuelve conflictos en cualquiera.
-- ---------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS trg_artifacts_no_replace
BEFORE INSERT ON artifacts
WHEN EXISTS (SELECT 1 FROM artifacts WHERE artifact_id = NEW.artifact_id AND version = NEW.version AND content_hash = NEW.content_hash)
BEGIN
    SELECT RAISE(ABORT, 'artifacts es append-only (D-52d): la (artifact_id, version, content_hash) ya existe; no se reemplaza');
END;

CREATE TRIGGER IF NOT EXISTS trg_reasoning_runs_no_replace
BEFORE INSERT ON reasoning_runs
WHEN EXISTS (SELECT 1 FROM reasoning_runs WHERE request_id = NEW.request_id)
BEGIN
    SELECT RAISE(ABORT, 'reasoning_runs es append-only (D-52d): el request_id ya existe; no se reemplaza');
END;

-- effects: dos claves únicas (effect_id PK y (provider, idempotency_key)); REPLACE por cualquiera de ellas borraría el efecto previo.
CREATE TRIGGER IF NOT EXISTS trg_effects_no_replace
BEFORE INSERT ON effects
WHEN EXISTS (SELECT 1 FROM effects WHERE effect_id = NEW.effect_id
                                      OR (provider = NEW.provider AND idempotency_key = NEW.idempotency_key))
BEGIN
    SELECT RAISE(ABORT, 'effects es append-only (D-52d): el effect_id o la (provider, idempotency_key) ya existen; no se reemplaza');
END;

-- reconciliation_id es AUTOINCREMENT: solo hay conflicto posible si quien inserta fija el id explícitamente (NEW.id NULL => se asigna uno nuevo).
CREATE TRIGGER IF NOT EXISTS trg_effect_reconciliations_no_replace
BEFORE INSERT ON effect_reconciliations
WHEN NEW.reconciliation_id IS NOT NULL
 AND EXISTS (SELECT 1 FROM effect_reconciliations WHERE reconciliation_id = NEW.reconciliation_id)
BEGIN
    SELECT RAISE(ABORT, 'effect_reconciliations es append-only (D-52d): el reconciliation_id ya existe; no se reemplaza');
END;

-- seq es AUTOINCREMENT: igual que arriba, solo un seq explícito puede chocar. Proteger la cadena de hashes de reescrituras silenciosas.
CREATE TRIGGER IF NOT EXISTS trg_audit_log_no_replace
BEFORE INSERT ON audit_log
WHEN NEW.seq IS NOT NULL
 AND EXISTS (SELECT 1 FROM audit_log WHERE seq = NEW.seq)
BEGIN
    SELECT RAISE(ABORT, 'audit_log es append-only estricto (D-52d): el seq ya existe; no se reemplaza');
END;

-- ---------------------------------------------------------------------
-- D-53: validación de la referencia a la regla en `mandates` (rule_hash), en SQLite y no solo en el DTO, para que ni una conexión
-- que se salte Python ni una FK desactivada dejen referencias colgantes. Son BEFORE INSERT (misma familia que los *_no_replace).
-- Se comprueba: (1) formato de rule_hash = SHA-256 hex minúscula (64); (2) la política (policy_id, policy_version) EXISTE; (3) la
-- política tiene CONTENIDO (content NOT NULL): sin contenido no hay regla a la que apuntar. NO se comprueba que rule_id exista
-- dentro de content ni que rule_hash sea el de esa regla: el formato de las reglas no está definido (parche 0011).
-- ---------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS trg_mandates_rule_hash_format
BEFORE INSERT ON mandates
WHEN NEW.rule_hash IS NOT NULL
 AND (length(NEW.rule_hash) <> 64 OR NEW.rule_hash GLOB '*[^0-9a-f]*')
BEGIN
    SELECT RAISE(ABORT, 'mandates (D-53): rule_hash debe ser un SHA-256 en hexadecimal minuscula (64 caracteres)');
END;

CREATE TRIGGER IF NOT EXISTS trg_mandates_policy_must_exist
BEFORE INSERT ON mandates
WHEN NEW.policy_id IS NOT NULL
 AND NOT EXISTS (SELECT 1 FROM policies WHERE policy_id = NEW.policy_id AND version = NEW.policy_version)
BEGIN
    SELECT RAISE(ABORT, 'mandates (D-53): la politica (policy_id, policy_version) referenciada no existe');
END;

CREATE TRIGGER IF NOT EXISTS trg_mandates_policy_must_have_content
BEFORE INSERT ON mandates
WHEN NEW.policy_id IS NOT NULL
 AND EXISTS (SELECT 1 FROM policies WHERE policy_id = NEW.policy_id AND version = NEW.policy_version AND content IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'mandates (D-53): la politica referenciada no tiene contenido (content NULL); no hay regla a la que apuntar');
END;
