-- Stage 13: a durable lookup for request IDs finalized on Shard B before
-- B->C cutover. B is fenced before this one-time snapshot is declared ready.
-- Never modify an earlier applied schema migration.
CREATE TABLE migrated_legacy_requests (
    request_id TEXT PRIMARY KEY,
    source_shard TEXT NOT NULL CHECK (source_shard = 'B'),
    request_fingerprint CHAR(64) NOT NULL,
    tx_id UUID NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING','COMMITTED','ABORTED')),
    response JSONB,
    source_updated_at TIMESTAMPTZ NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE migrated_request_backfill (
    job_id UUID PRIMARY KEY REFERENCES migration_jobs(job_id) ON DELETE RESTRICT,
    source_shard TEXT NOT NULL CHECK (source_shard='B'),
    state TEXT NOT NULL CHECK(state='COMPLETE'),
    row_count BIGINT NOT NULL CHECK(row_count >= 0),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
