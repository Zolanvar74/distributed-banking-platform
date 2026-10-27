-- Stage 09. Persist exact crash experiments; never mutate account balances.
-- An ARMED fault fires at most once, BEFORE process termination; an experiment
-- becomes VERIFIED only after checking Docker Engine and Metadata evidence.
CREATE TABLE healing_crash_experiments (
    experiment_id UUID PRIMARY KEY,
    replica_id TEXT NOT NULL,
    shard_id TEXT NOT NULL CHECK (shard_id IN ('A','B','C')),
    slot INTEGER NOT NULL CHECK (slot >= 1),
    point TEXT NOT NULL CHECK (point IN
        ('AFTER_RESERVE','AFTER_CREATE','AFTER_START','AFTER_HEALTHY')),
    previous_generation BIGINT NOT NULL CHECK (previous_generation >= 1),
    expected_generation BIGINT NOT NULL CHECK (expected_generation > previous_generation),
    previous_container_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'ARMED' CHECK
        (state IN ('ARMED','FIRED','VERIFIED','CANCELLED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    fired_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    CHECK ((state = 'ARMED' AND fired_at IS NULL AND verified_at IS NULL)
      OR (state = 'FIRED' AND fired_at IS NOT NULL AND verified_at IS NULL)
      OR (state = 'VERIFIED' AND fired_at IS NOT NULL AND verified_at IS NOT NULL)
      OR state = 'CANCELLED')
);
CREATE UNIQUE INDEX healing_one_unresolved_trial_per_slot
    ON healing_crash_experiments(shard_id, slot)
    WHERE state IN ('ARMED','FIRED');
CREATE INDEX healing_crash_recent
    ON healing_crash_experiments(created_at DESC);
