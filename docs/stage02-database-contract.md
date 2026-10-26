# Stage 02 — Durable database foundation

Stage 02 extends the existing **three Stage 01 PostgreSQL databases**, preserving their accounts and volumes. `sql/migrations/` is versioned, and `schema_migrations` stores the SHA-256 of every applied SQL file. The migration runner locks each DB with a transaction-scoped advisory lock and either commits the entire migration plus its record or rolls both back.

## Ownership and version model

- `routing_epoch` (Metadata DB): **global map revision**, monotonically increased by the migration-manager during a future cutover. As of Stage 02 it remains `1` and **no map edit API exists**.
- `shard_map` (Metadata DB): disjoint active account ranges, with `epoch` for each range; each active account belongs to one shard. A GiST exclusion constraint rejects intersecting active ranges.
- `ownership_fences` (each Shard DB): disjoint ranges, per-range epoch, write-allowed flag and migration state. On Stage 02, A has `[1,1000]` and B has `[1001,2000]` at epoch 1. A future banking write must lock the appropriate fence **inside the same PostgreSQL transaction** and fail if either the epoch or write permission is wrong. The Gateway's map lookup is **not** a write fence.
- Shard C will not be allocated a writable fence just by being created: target-side activation is a deliberate later protocol step.

**Important not-yet-implemented invariant**: the Stage-02 SQL constraints alone do not ensure global single-writer ownership during an outage. The cutover protocol (including fencing-before-activation, durable cutover decisions, and locking) is implemented and tested in Stages 12–14.

## Two-Phase Commit storage

- `transactions`: unique `tx_id`, globally unique client request ID, request fingerprint, amount, accounts, logical state, durable decision and timestamps.
- `transaction_participants`: prepare GID, participant vote and final acknowledgment per shard.
- `participant_commit_ledger` (each Shard DB): committed participant evidence; a ledger entry should be written *within* the same prepared transaction as balance changes. Its absence is never proof of ABORT.
- `local_request_dedup` (each Shard DB): tracks idempotent requests; its app-level write/check protocol comes later.

Storage is prepared, but there are no exposed banking mutation/2PC APIs in Stage 02. **No distributed transaction has yet been implemented.**

## Migration storage and ordering

- `migration_jobs`: persistent phase, source/target ranges, cutover epoch and timing. A GiST exclusion constraint prevents concurrent unfinished migrations of overlapping ranges.
- `migration_account_checkpoints`: durable `(job_id, account_id, snapshot_version, applied_version)`.
- `migration_recovery_events`: auditable phase transitions and diagnostics.
- `account_change_groups` and `account_changes` (source Shard DB): per-account versions and local atomic groups, to be inserted in the same DB transaction as every future balance change. Same-shard transfers will generate both changes in one group.
- `migration_applied_groups` (target Shard DB): a ledger for idempotent atomic group replay.

**Ordering caveat:** per-account versioning gives ordering **within an account**, not a global commit sequence. `created_at`, `BIGSERIAL` and sequence allocation order are **not** sufficient to track PostgreSQL commit order. Before implementing online migration, we must validate how to capture and replay multi-account groups and prepared 2PC transactions without missing or misordering committed changes. The eventual design may require a commit-ordered logical decoding stream, or another verified transactional capture protocol. The existence of these tables does not constitute completed CDC or crash-safe migration.

## Self-Healing data model

- `replica_registry`: Stage 01 compatible fields (`replica_id`, `shard_id`, `hostname`, `port`), plus `slot`, `generation`, `container_id`, lifecycle and heartbeat fields.
- `desired_replica_count`: desired count of at least 2 for A and B. C is added by the migration controller later.
- Gateway **still checks live health** at request time, and Stage 01 replicas still register themselves. Docker-based replacement is not yet enabled.

## Migration procedure and failure handling

Run `docker compose up --build -d` with the new compose file. The one-shot `schema-migrator` starts only after all three DBs pass health checks. All application services depend on the migrator's successful exit. Existing `metadata-data`, `shard-a-data`, and `shard-b-data` volumes are reused because the Compose project name stays `distributed-banking`.

Run the migrator repeatedly using `docker compose run --rm schema-migrator`. Already-applied migrations are reported `SKIP`, with checksum comparison. A modified migration file is rejected; write a *new numbered file* for subsequent changes. Never edit previously applied SQL.

The three databases are migrated **sequentially**, not as a single cross-database transaction. If one DB fails, completed migrations in preceding DBs remain durable, and the next run resumes only the missing migrations. As the stage provides only additive schemas and the app is read-only, this is acceptable here. Future incompatible migrations will need explicit compatibility ordering.

All additions are for local development, not internet-facing deployment. Secrets are in `.env`, which is ignored in Git; do not expose databases or internal APIs publicly.
