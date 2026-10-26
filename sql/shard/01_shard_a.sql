-- Stage 01 read-only sample accounts (do not interpret as final banking schema).
CREATE TABLE accounts (
    account_id VARCHAR(5) PRIMARY KEY,
    balance NUMERIC(20,2) NOT NULL CHECK (balance >= 0),
    version BIGINT NOT NULL DEFAULT 0
);
INSERT INTO accounts (account_id,balance) VALUES
    ('A0001',100.00),('A0500',1000.00),('A1000',300.00);
