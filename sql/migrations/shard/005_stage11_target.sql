-- Stage 11 target staging ledger. Applied in A/B/C for a uniform schema;
-- only Shard C receives target records. No writable ownership is installed.
CREATE TABLE migration_target_batches (
    job_id UUID PRIMARY KEY,
    range_start INTEGER NOT NULL,
    range_end INTEGER NOT NULL,
    row_count INTEGER NOT NULL CHECK (row_count > 0),
    snapshot_digest CHAR(64) NOT NULL,
    copied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (range_start > 0 AND range_end >= range_start)
);
CREATE TABLE migration_target_accounts (
    job_id UUID NOT NULL REFERENCES migration_target_batches(job_id) ON DELETE RESTRICT,
    account_id VARCHAR(5) NOT NULL CHECK (account_id ~ '^A[0-9]{4}$'),
    balance NUMERIC(20,2) NOT NULL CHECK (balance >= 0),
    snapshot_balance NUMERIC(20,2) NOT NULL CHECK (snapshot_balance >= 0),
    version BIGINT NOT NULL CHECK (version >= 0),
    snapshot_version BIGINT NOT NULL CHECK (snapshot_version >= 0),
    PRIMARY KEY(job_id,account_id),
    CHECK (version >= snapshot_version)
);
CREATE TABLE migration_target_applied_groups (
    job_id UUID NOT NULL REFERENCES migration_target_batches(job_id) ON DELETE RESTRICT,
    change_group_id UUID NOT NULL,
    applied_entries INTEGER NOT NULL CHECK (applied_entries IN (1,2)),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(job_id,change_group_id)
);
CREATE TABLE migration_target_replayed_changes (
    job_id UUID NOT NULL,
    account_id VARCHAR(5) NOT NULL,
    version BIGINT NOT NULL,
    change_group_id UUID NOT NULL,
    PRIMARY KEY(job_id,account_id,version),
    FOREIGN KEY(job_id,account_id) REFERENCES migration_target_accounts(job_id,account_id)
);
CREATE INDEX migration_target_replay_group_idx
    ON migration_target_replayed_changes(job_id,change_group_id);
