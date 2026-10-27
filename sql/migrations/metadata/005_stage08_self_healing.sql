-- Stage 08: durable incarnation-based membership and controller audit.
-- No balance changes. Existing A1/A2/B1/B2 begin at generation 1.
ALTER TABLE replica_registry
    ADD COLUMN healing_since TIMESTAMPTZ,
    ADD COLUMN last_reconciled_at TIMESTAMPTZ;

CREATE TABLE replica_healing_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shard_id TEXT NOT NULL CHECK (shard_id IN ('A','B','C')),
    slot INTEGER NOT NULL CHECK (slot >= 1),
    generation BIGINT NOT NULL CHECK (generation >= 1),
    action TEXT NOT NULL CHECK (action IN
        ('REPLACEMENT_PLANNED','CONTAINER_CREATED','CONTAINER_STARTED',
         'REPLICA_HEALTHY','REPLICA_UNHEALTHY','CONTAINER_STOPPED',
         'CONTROLLER_RECOVERED','RECOVERY_ERROR')),
    container_id TEXT,
    details TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX replica_healing_events_recent ON replica_healing_events(created_at DESC);

-- The controller uses pg_try_advisory_lock(session lock) to reconcile one
-- slot at a time across controller restarts or accidental duplicate instances.
-- The database stores the target incarnation BEFORE Docker container creation.
