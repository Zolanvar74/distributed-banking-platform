-- Stage 05: safe local banking operations (both existing shards and future C).
-- Does not modify or reset balances. Leaves Stage 02 migrations untouched.
ALTER TABLE local_request_dedup
    ADD CONSTRAINT local_dedup_status_response_coherent
    CHECK ((status = 'PENDING' AND response IS NULL) OR
           (status IN ('COMMITTED', 'ABORTED') AND response IS NOT NULL));

CREATE INDEX local_request_dedup_state_idx ON local_request_dedup(status, updated_at);
CREATE INDEX account_changes_group_account_idx ON account_changes(change_group_id, account_id);

-- A durable group must have a declared one- or two-entry size for this and
-- upcoming stages; reconciliation will check actual complete group membership.
ALTER TABLE account_change_groups
    ADD CONSTRAINT change_groups_expected_entry_count CHECK (expected_entries IN (1, 2));
