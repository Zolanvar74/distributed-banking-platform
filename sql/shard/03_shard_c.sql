-- Stage 11: Shard C begins empty. No application-write ownership initially.
CREATE TABLE accounts (
    account_id VARCHAR(5) PRIMARY KEY,
    balance NUMERIC(20,2) NOT NULL CHECK (balance >= 0),
    version BIGINT NOT NULL DEFAULT 0
);
