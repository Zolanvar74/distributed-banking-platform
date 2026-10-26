# Architecture baseline — Stage 03

## Running now

- Gateway uses Metadata's active Shard Map for account ownership and discovers candidate replicas using the persistent Registry.
- Gateway checks live replica `/health` responses **and verifies shard and replica identity**, then load balances per owning shard.
- Every Gateway-to-replica balance lookup carries the chosen shard and map epoch; the replica checks its **local ownership fence** and reads the account from the same database snapshot.
- Shards A and B have independent persistent PostgreSQL databases; their two application replicas share only the owning shard's database.
- Stage 03 integration checks inspect *both physical databases* and compare actual account placement, Gateway responses and local routing guards.
- Versioned migrations, immutable 2PC decision scaffolding, per-shard mutation journals, migration checkpoints and Self-Healing registry remain available from Stage 02.
- The Coordinator, Migration Manager and Self-Healing services are still **health-only placeholders**, not controllers.

## Planned, NOT yet running

- Mutating banking operations, locks for replica concurrency, local atomic transfers.
- True PostgreSQL prepared 2PC with durable commit/abort decisions and coordinator recovery.
- Container replacement by a dedicated Self-Healing Controller and recovery from its crashes.
- Consistent snapshots, commit-ordered change replay, online Shard C and crash-safe cutover.
- **Atomic writable ownership**: current read fence check is not a substitute for write-path locking or a fully coordinated epoch transition.

See `docs/stage02-database-contract.md` and `docs/stage03-routing-contract.md`.
