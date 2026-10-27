-- Durable acknowledgments AFTER target C commits. Source observed versions
-- remain distinct from target applied versions.
CREATE TABLE migration_target_progress (
    job_id UUID NOT NULL REFERENCES migration_jobs(job_id) ON DELETE RESTRICT,
    account_id VARCHAR(5) NOT NULL CHECK (account_id ~ '^A[0-9]{4}$'),
    snapshot_version BIGINT NOT NULL CHECK (snapshot_version >= 0),
    applied_version BIGINT NOT NULL CHECK (applied_version >= snapshot_version),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(job_id, account_id)
);
CREATE INDEX migration_target_progress_job_idx ON migration_target_progress(job_id);
INSERT INTO desired_replica_count(shard_id,desired_count)
VALUES ('C',2) ON CONFLICT (shard_id) DO NOTHING;
