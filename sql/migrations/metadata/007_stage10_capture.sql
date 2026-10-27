-- Stage 10: durable *source-observation* checkpoints. These are NOT target replay
-- acknowledgments; that separate evidence is implemented in Stage 11.
CREATE TABLE migration_source_observations (
    job_id UUID NOT NULL REFERENCES migration_jobs(job_id) ON DELETE RESTRICT,
    account_id VARCHAR(5) NOT NULL CHECK (account_id ~ '^A[0-9]{4}$'),
    snapshot_version BIGINT NOT NULL CHECK (snapshot_version >= 0),
    observed_version BIGINT NOT NULL CHECK (observed_version >= snapshot_version),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, account_id)
);
CREATE INDEX migration_source_observations_job ON migration_source_observations(job_id);
