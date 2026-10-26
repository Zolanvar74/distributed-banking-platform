-- Stage 02: durable metadata foundation. Applied once by scripts/apply_migrations.py.
-- Kept compatible with Stage 01 data and its original 01_init.sql.

-- A monotonic global map revision, distinct from each range's ownership epoch.
CREATE TABLE routing_epoch (
    singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO routing_epoch (singleton, epoch) VALUES (1, 1);

CREATE FUNCTION require_increasing_routing_epoch() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.epoch <= OLD.epoch THEN
        RAISE EXCEPTION 'Routing map epoch must increase (old %, new %)', OLD.epoch, NEW.epoch;
    END IF;
    NEW.changed_at := clock_timestamp();
    RETURN NEW;
END;
$$;
CREATE TRIGGER routing_epoch_increases
    BEFORE UPDATE ON routing_epoch
    FOR EACH ROW EXECUTE FUNCTION require_increasing_routing_epoch();

-- Two active ranges may NEVER overlap. Only the map-control transaction may
-- be allowed to publish ownership changes (application authorization later).
ALTER TABLE shard_map
    ADD CONSTRAINT shard_map_active_no_overlap
    EXCLUDE USING gist ((int4range(range_start, range_end, '[]')) WITH &&)
    WHERE (active);
ALTER TABLE shard_map
    ADD CONSTRAINT shard_map_known_owner
    CHECK (shard_id IN ('A', 'B', 'C'));
CREATE INDEX shard_map_active_lookup ON shard_map (range_start, range_end) WHERE active;

CREATE FUNCTION prevent_range_epoch_regression() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.epoch < OLD.epoch THEN
        RAISE EXCEPTION 'Range ownership epoch must not decrease';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER range_epoch_no_regression
    BEFORE UPDATE ON shard_map
    FOR EACH ROW EXECUTE FUNCTION prevent_range_epoch_regression();

-- Prepare for dynamic replica discovery and reconciliation. Stage-01 replicas
-- remain usable; health is still probed in real time by Gateway.
ALTER TABLE replica_registry
    ADD COLUMN slot INTEGER,
    ADD COLUMN container_id TEXT,
    ADD COLUMN lifecycle TEXT NOT NULL DEFAULT 'REGISTERED',
    ADD COLUMN generation BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN last_heartbeat TIMESTAMPTZ;
UPDATE replica_registry
   SET slot = substring(replica_id FROM '[0-9]+$')::integer
 WHERE slot IS NULL;
ALTER TABLE replica_registry
    ALTER COLUMN slot SET NOT NULL,
    ADD CONSTRAINT replica_slot_positive CHECK (slot >= 1),
    ADD CONSTRAINT replica_port_range CHECK (port BETWEEN 1 AND 65535),
    ADD CONSTRAINT replica_generation_positive CHECK (generation >= 1),
    ADD CONSTRAINT replica_lifecycle_valid CHECK
      (lifecycle IN ('REGISTERED', 'HEALTHY', 'UNHEALTHY', 'REPLACING', 'DRAINING'));
CREATE UNIQUE INDEX replica_one_slot_per_shard ON replica_registry(shard_id, slot);

CREATE TABLE desired_replica_count (
    shard_id TEXT PRIMARY KEY CHECK (shard_id IN ('A','B','C')),
    desired_count INTEGER NOT NULL CHECK (desired_count >= 2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO desired_replica_count(shard_id, desired_count) VALUES ('A', 2), ('B', 2);

-- State persisted BEFORE any 2PC outcome is broadcast. All 2PC logic is
-- implemented in later stages; these tables only define its storage contract.
CREATE TABLE transactions (
    tx_id UUID PRIMARY KEY,
    client_request_id TEXT NOT NULL UNIQUE CHECK (length(client_request_id) BETWEEN 1 AND 200),
    request_fingerprint CHAR(64) NOT NULL CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    from_account VARCHAR(5) NOT NULL,
    to_account VARCHAR(5) NOT NULL,
    amount NUMERIC(20,2) NOT NULL CHECK (amount > 0),
    state TEXT NOT NULL DEFAULT 'INITIATED' CHECK (state IN
        ('INITIATED', 'PREPARING', 'PREPARED', 'COMMIT_DECIDED', 'ABORT_DECIDED', 'COMPLETED')),
    decision TEXT CHECK (decision IN ('COMMIT','ABORT')),
    decision_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT transactions_account_distinct CHECK (from_account <> to_account),
    CONSTRAINT transactions_decision_coherent CHECK
        ((state <> 'COMMIT_DECIDED' OR decision = 'COMMIT') AND
         (state <> 'ABORT_DECIDED' OR decision = 'ABORT') AND
         (state <> 'COMPLETED' OR decision IS NOT NULL) AND
         (decision IS NULL OR decision_at IS NOT NULL))
);
CREATE FUNCTION forbid_decision_reversal() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.decision IS NOT NULL AND NEW.decision IS DISTINCT FROM OLD.decision THEN
        RAISE EXCEPTION 'A durable 2PC decision must never be reversed';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER transactions_decision_immutable
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION forbid_decision_reversal();
CREATE INDEX transactions_incomplete ON transactions(created_at)
    WHERE state <> 'COMPLETED';

CREATE TABLE transaction_participants (
    tx_id UUID NOT NULL REFERENCES transactions(tx_id) ON DELETE RESTRICT,
    shard_id TEXT NOT NULL CHECK (shard_id IN ('A','B','C')),
    prepare_gid TEXT NOT NULL UNIQUE CHECK (length(prepare_gid) BETWEEN 1 AND 200),
    vote TEXT NOT NULL DEFAULT 'PENDING' CHECK (vote IN ('PENDING','YES','NO')),
    completion_ack BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tx_id, shard_id)
);

CREATE TABLE migration_jobs (
    job_id UUID PRIMARY KEY,
    source_shard TEXT NOT NULL CHECK (source_shard IN ('A','B','C')),
    target_shard TEXT NOT NULL CHECK (target_shard IN ('A','B','C')),
    range_start INTEGER NOT NULL,
    range_end INTEGER NOT NULL,
    phase TEXT NOT NULL DEFAULT 'PLANNED' CHECK (phase IN
       ('PLANNED','TARGET_READY','SNAPSHOTTING','CATCHING_UP','FENCING',
        'FINAL_REPLAY','MAP_ACTIVATED','VERIFYING','PAUSED','COMPLETED')),
    epoch_before BIGINT NOT NULL CHECK (epoch_before > 0),
    epoch_after BIGINT CHECK (epoch_after > epoch_before),
    snapshot_id TEXT,
    cutover_decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (source_shard <> target_shard),
    CHECK (range_start > 0 AND range_end >= range_start),
    -- Jobs must not compete for the same account range until completed.
    CONSTRAINT no_concurrent_overlapping_migrations
        EXCLUDE USING gist ((int4range(range_start, range_end, '[]')) WITH &&)
        WHERE (phase <> 'COMPLETED')
);

-- Durable checkpoint per account. This is NOT a global commit-order cursor.
CREATE TABLE migration_account_checkpoints (
    job_id UUID NOT NULL REFERENCES migration_jobs(job_id) ON DELETE RESTRICT,
    account_id VARCHAR(5) NOT NULL CHECK (account_id ~ '^A[0-9]{4}$'),
    snapshot_version BIGINT NOT NULL CHECK (snapshot_version >= 0),
    applied_version BIGINT NOT NULL CHECK (applied_version >= snapshot_version),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, account_id)
);

-- The manager will use this for durable recovery across phase transitions.
CREATE TABLE migration_recovery_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES migration_jobs(job_id) ON DELETE RESTRICT,
    phase TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
