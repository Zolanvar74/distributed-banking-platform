# Architecture baseline — Stage 02

## Running now

- Gateway reads account balances and selects healthy banking replicas.
- Shard A and B each have one PostgreSQL database and two stateless app replicas.
- The Metadata DB stores Shard Map and replica endpoints; schemas now also include durable routing epochs, 2PC states, desired replicas and migration states.
- Each Shard DB now has write-fence ranges, future transaction journals, idempotency and migration replay tables.
- A one-shot versioned `schema-migrator` is run before application startup and preserves existing Volume data.
- Coordinator, Migration Manager and Self-Healing are still **placeholder processes** with `/health` only.

## Planned and NOT yet running

- Read-write banking operations with DB-level locks and epoch checks.
- Actual 2PC participant PREPARE, durable decisions, and recovery.
- Online creation of Shard C, consistent Snapshot, commit-ordered CDC and cutover.
- Docker API-based replica replacement and crash-resilient reconciliation.

See `docs/stage02-database-contract.md` for the data model, migration safety and limitations.
