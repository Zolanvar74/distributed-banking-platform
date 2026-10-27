-- Stage 10 source-side durable consistent snapshot. A single INSERT ... SELECT
-- reads one PostgreSQL MVCC statement snapshot while normal account writes run.
-- The batch, all rows, and fingerprint commit atomically. No source write freeze.
CREATE TABLE migration_snapshot_batches (
    job_id UUID PRIMARY KEY,
    range_start INTEGER NOT NULL,
    range_end INTEGER NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    snapshot_digest CHAR(64) NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (range_start > 0 AND range_end >= range_start)
);
CREATE TABLE migration_snapshot_accounts (
    job_id UUID NOT NULL REFERENCES migration_snapshot_batches(job_id) ON DELETE RESTRICT,
    account_id VARCHAR(5) NOT NULL CHECK (account_id ~ '^A[0-9]{4}$'),
    balance NUMERIC(20,2) NOT NULL CHECK (balance >= 0),
    version BIGINT NOT NULL CHECK (version >= 0),
    PRIMARY KEY (job_id, account_id)
);
CREATE INDEX migration_snapshot_accounts_job ON migration_snapshot_accounts(job_id);
