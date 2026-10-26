-- Stage 01 metadata; migration safety rules will be strengthened in stage 02.
CREATE TABLE shard_map (
    range_start INTEGER NOT NULL,
    range_end INTEGER NOT NULL,
    shard_id TEXT NOT NULL,
    epoch BIGINT NOT NULL DEFAULT 1,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (range_start, range_end),
    CONSTRAINT valid_range CHECK (range_start > 0 AND range_start <= range_end),
    CONSTRAINT positive_epoch CHECK (epoch > 0)
);
INSERT INTO shard_map (range_start, range_end, shard_id, epoch) VALUES
    (1, 1000, 'A', 1),
    (1001, 2000, 'B', 1);

CREATE TABLE replica_registry (
    replica_id TEXT PRIMARY KEY,
    shard_id TEXT NOT NULL,
    hostname TEXT NOT NULL,
    port INTEGER NOT NULL DEFAULT 8000,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX replica_registry_shard_idx ON replica_registry(shard_id);
