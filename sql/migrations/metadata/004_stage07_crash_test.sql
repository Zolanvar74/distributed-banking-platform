-- Explicitly armed, one-time crash injection for isolated 2PC recovery demo.
-- These rows are metadata only. Never update account balances in this migration.
CREATE TABLE stage07_fault_injections (
    request_id TEXT PRIMARY KEY CHECK (request_id LIKE 'stage07-crash-%'),
    status TEXT NOT NULL DEFAULT 'ARMED' CHECK (status IN ('ARMED', 'TRIPPED')),
    tx_id UUID UNIQUE REFERENCES transactions(tx_id),
    from_account VARCHAR(5) NOT NULL CHECK (from_account = 'A0500'),
    to_account VARCHAR(5) NOT NULL CHECK (to_account = 'A1500'),
    amount NUMERIC(20,2) NOT NULL CHECK (amount > 0),
    before_source NUMERIC(20,2) NOT NULL CHECK (before_source >= 0),
    before_destination NUMERIC(20,2) NOT NULL CHECK (before_destination >= 0),
    armed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    fired_at TIMESTAMPTZ,
    CHECK ((status='ARMED' AND fired_at IS NULL AND tx_id IS NULL) OR
           (status='TRIPPED' AND fired_at IS NOT NULL AND tx_id IS NOT NULL))
);
