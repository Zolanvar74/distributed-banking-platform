-- Stage 02: schema shared by Shards A/B and future Shard C.
-- Accounts from Stage 01 and existing balances are preserved.
ALTER TABLE accounts
    ADD CONSTRAINT accounts_format CHECK (account_id ~ '^A[0-9]{4}$'),
    ADD CONSTRAINT accounts_version_nonnegative CHECK (version >= 0);

-- A DB-local write fence checked under lock inside every future bank mutation.
-- The authoritative writes MUST use this fence in the same DB transaction.
CREATE TABLE ownership_fences (
    range_start INTEGER NOT NULL,
    range_end INTEGER NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    phase TEXT NOT NULL CHECK (phase IN ('OWNED','MIGRATING','FENCED','TARGET_READY')),
    writable BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (range_start, range_end),
    CHECK (range_start > 0 AND range_end >= range_start),
    CHECK (NOT writable OR phase IN ('OWNED','MIGRATING')),
    CONSTRAINT local_fences_no_overlap
      EXCLUDE USING gist ((int4range(range_start, range_end, '[]')) WITH &&)
);
-- A and B start as owners. New Shard C remains without writable fences until
-- the Migration Manager explicitly installs and activates ownership.
INSERT INTO ownership_fences(range_start, range_end, epoch, phase, writable)
SELECT 1, 1000, 1, 'OWNED', TRUE WHERE current_database() = 'shard_a';
INSERT INTO ownership_fences(range_start, range_end, epoch, phase, writable)
SELECT 1001, 2000, 1, 'OWNED', TRUE WHERE current_database() = 'shard_b';

-- Local request de-duplication (participant-side or same-shard operations).
CREATE TABLE local_request_dedup (
    client_request_id TEXT PRIMARY KEY CHECK (length(client_request_id) BETWEEN 1 AND 200),
    request_fingerprint CHAR(64) NOT NULL CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    tx_id UUID NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING','COMMITTED','ABORTED')),
    response JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Changes for a single local banking operation belong to one durable group.
-- A same-shard transfer writes one group and two account_changes atomically.
CREATE TABLE account_change_groups (
    change_group_id UUID PRIMARY KEY,
    tx_id UUID NOT NULL,
    expected_entries SMALLINT NOT NULL CHECK (expected_entries >= 1),
    operation_kind TEXT NOT NULL CHECK (operation_kind IN
      ('DEPOSIT','WITHDRAW','SAME_SHARD_TRANSFER','CROSS_SHARD_TRANSFER','OTHER')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Per-account version advances with the balance update, in one transaction.
-- BIGSERIAL/event timestamps alone DO NOT establish commit order.
CREATE TABLE account_changes (
    account_id VARCHAR(5) NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
    version BIGINT NOT NULL CHECK (version > 0),
    change_group_id UUID NOT NULL REFERENCES account_change_groups(change_group_id) ON DELETE RESTRICT,
    before_balance NUMERIC(20,2) NOT NULL CHECK (before_balance >= 0),
    after_balance NUMERIC(20,2) NOT NULL CHECK (after_balance >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (account_id, version)
);
CREATE INDEX account_changes_group_idx ON account_changes(change_group_id);

-- Target-shard ledger prevents applying the same complete group twice.
-- A future migration worker must apply group effects + record atomically.
CREATE TABLE migration_applied_groups (
    job_id UUID NOT NULL,
    change_group_id UUID NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, change_group_id)
);

-- Participant evidence for eventual 2PC reconciliation. A COMMITTED ledger
-- row becomes visible only after that participant's prepared transaction
-- commits; absence of a row alone does NOT prove an ABORT.
CREATE TABLE participant_commit_ledger (
    tx_id UUID NOT NULL,
    shard_id TEXT NOT NULL CHECK (shard_id IN ('A','B','C')),
    prepare_gid TEXT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tx_id, shard_id),
    UNIQUE (prepare_gid)
);
