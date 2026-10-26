-- Stage 06: durable participant routing snapshot, recovery audit and monitoring.
-- Does not modify any balances or Stage 02/05 applied migration checksums.
ALTER TABLE transaction_participants
  ADD COLUMN role TEXT CHECK (role IN ('DEBIT','CREDIT')),
  ADD COLUMN account_id VARCHAR(5) CHECK (account_id ~ '^A[0-9]{4}$'),
  ADD COLUMN routing_epoch BIGINT CHECK (routing_epoch > 0),
  ADD COLUMN last_error TEXT,
  ADD COLUMN ack_at TIMESTAMPTZ;

CREATE INDEX transactions_recovery_idx ON transactions(updated_at, created_at)
  WHERE state <> 'COMPLETED';

CREATE TABLE transaction_recovery_events (
  event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tx_id UUID NOT NULL REFERENCES transactions(tx_id) ON DELETE RESTRICT,
  event_type TEXT NOT NULL,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
