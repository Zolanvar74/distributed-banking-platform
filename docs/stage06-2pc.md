# Stage 06 — Durable Cross-Shard Two-Phase Commit

## Protocol

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant T as Coordinator+Metadata DB
    participant A as Shard A PostgreSQL
    participant B as Shard B PostgreSQL
    C->>G: POST /transfers (request_id)
    G->>T: Cross-shard transfer
    T->>T: Persist INITIATED and participant GIDs
    T->>A: PREPARE debit
    A->>A: Lock, UPDATE, journal; PREPARE TRANSACTION
    A-->>T: YES
    T->>B: PREPARE credit
    B->>B: Lock, UPDATE, journal; PREPARE TRANSACTION
    B-->>T: YES
    T->>T: Persist COMMIT_DECIDED before delivery
    T->>A: COMMIT PREPARED
    A-->>T: COMMITTED (ledger proof)
    T->>B: COMMIT PREPARED
    B-->>T: COMMITTED (ledger proof)
    T->>T: Persist COMPLETED (both ACKs)
    T-->>G: COMMITTED
    G-->>C: Success + tx_id
```

If any participant votes **NO**, the coordinator durably records **ABORT_DECIDED** and sends `ROLLBACK PREPARED` to previous YES-voters. A request timeout is **UNKNOWN**, not NO; the coordinator keeps the transaction PENDING and retries by scanning durable metadata. A recorded COMMIT can never be replaced with ABORT (database trigger).

## Recovery and safety details

- Metadata `transactions`, `transaction_participants` (vote, GID, route epoch, acknowledgment) and `transaction_recovery_events` are durable; the coordinator resumes unfinished operations after restart every four seconds.
- Each shard stores its GID in `pg_prepared_xacts`; prepared transactions preserve row locks and uncommitted journal writes through a PostgreSQL restart.
- `participant_commit_ledger` is inserted INSIDE the prepared transaction. Visible ledger record proves a COMMIT; *absence* of the ledger does not prove an ABORT.
- Repeated PREPARE sees an existing prepared GID or committed ledger. Repeated COMMIT is acknowledged by committed ledger. Retry a client request with the **same request_id** and identical payload; query `GET /transfers/requests/{request_id}` or `GET /transactions/{tx_id}`.
- No "success" before both participants return and have their acknowledgments durably recorded. Incomplete cases expose HTTP 202 and `status=PENDING`; Gateway returns UNKNOWN on connectivity failure.
- Snapshot of shard range epoch is stored for each participant. Shard-level `ownership_fences` enforce the recorded route at PREPARE. Atomic online cutover coordination with PREPARED transactions is future Stage 13 work; do NOT run manual shard-map mutations during this stage.
- Shard DBs must have `max_prepared_transactions > 0`. Prepared transactions must not be left unresolved indefinitely; monitor `pg_prepared_xacts`, maintain Metadata volume, and ensure only Coordinator may drive final decisions.

## Intentional limits of Stage 06

- End-to-end crash *injection* (Coordinator crash after durable COMMIT and before all acknowledgments) will be exercised in Stage 07. Automatic periodic retry exists already, but it is not proof of that fault-injection scenario.
- Request IDs are durable for cross-shard transfers in Metadata; they are not yet globally unified with stage-05 shard-local idempotency keys. Stage 07 strengthens coordination/status for uncertain outcomes.
- Internal-only endpoints use a development shared token and are not exposed on published host ports. Set a private `TWO_PC_TOKEN` in `.env` before sharing a deployment. This is a class assignment prototype, not a production authentication design.
- 2PC is blocking by nature while participants are prepared and the Coordinator or a shard is unavailable. We expose PENDING rather than falsely report failure or success.

## Test

Run from PowerShell in existing project folder (do not delete volumes):

```powershell
docker compose up --build -d
docker compose run --rm schema-migrator python -m scripts.verify_stage06
```

Successful output ends with `STAGE 06 TWO-PHASE COMMIT CHECK: ALL CHECKS PASSED`. The test makes real transfers, rejects insufficient balance, tests duplicate keys, verifies both ledgers and reverses the first transfer on success. **If interrupted, do not reset balances**: query by request ID and inspect prepared GIDs.
