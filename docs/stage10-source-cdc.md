# Stage 10 — Migration Manager & durable source change capture

## Scope and explicit non-goals

This stage establishes a **crash-resumable consistent snapshot** of the migration
range `A1501..A2000` on Shard B, plus a durable journal/checkpoint for online
changes while B continues serving reads/writes. **It does not create Shard C,
replay changes to C, fence B, or update Shard Map.** Those are Stages 11–13.

This CDC is implemented as a **transactional application journal** (Stage 05
`account_change_groups` / `account_changes`), not a PostgreSQL WAL logical
replication slot. Every supported banking operation writes its account update
and journal rows in one transaction (including PostgreSQL `PREPARE TRANSACTION`
participants). For externally modified account balances, such as direct SQL
`UPDATE accounts` that bypasses the application, capture is not guaranteed;
source observation explicitly rejects a version/journal mismatch. Never
manually modify authoritative balances.

### Consistent snapshot

`create_source_snapshot` commits the entire copy of `(account_id, balance,
version)` via a **single SQL `INSERT ... SELECT`** statement using one
PostgreSQL MVCC statement snapshot. It does not freeze or lock account rows.
`migration_snapshot_batches` and `migration_snapshot_accounts` commit in the
same database transaction, with count and SHA-256 digest. The operation is
idempotent by `job_id`: after a crash, it reuses and validates the existing
snapshot instead of recapturing at a different moment.

The metadata update happens *after* source durability. If the manager dies
between source and metadata commits, `snapshot` can safely be retried.

### Durable source journal and checkpoint

`account_changes` is durable in Shard B and versioned **per account**. It must
not be interpreted as a global commit-order feed. A single read-only
`REPEATABLE READ` snapshot checks all journal versions `> snapshot_version`,
validates contiguous versions plus before/after balances, and compares the
computed end state to the live account row. An invalid/missing chain produces
an error, not silent skipping.

`migration_source_observations` in Metadata records per-account versions that
were observed by the migration manager; the checkpoint advances monotonically
with `GREATEST`. A controller crash or temporary outage does not erase the
journal. The manager's polling loop resumes scanning after restart.

**Important:** `observed_version` here means **read/validated on B only**.
It does **not** mean copied/applied to C. Stage 11 must maintain separate
transactional acknowledgments for successful target replay. Changes for
multi-account source transaction groups must be replayed atomically whenever
both affected accounts are within the migrated range. Cross-boundary
same-shard transfers and in-flight 2PC need the Stage 13 ownership protocol.

### Windows upgrade (preserves data and healed Replica generations)

Because Stage 08/09 may have created replacement containers, **do not run
`docker compose up --build -d` indiscriminately**: it can restart old Compose
Replica containers alongside healed generations. Upgrade just the migrator and
manager in this stage:

```powershell
# In the existing directory containing compose.yaml; keep .env and .git
# Never use docker compose down -v

docker compose config --quiet
docker compose build
# Apply only additive migration files (old migration checksums are unchanged):
docker compose run --rm schema-migrator python scripts/apply_migrations.py
# Replace the Stage-09 skeleton with the Stage-10 migration-manager only:
docker compose up -d --no-deps --force-recreate migration-manager
docker compose logs --tail 50 migration-manager
```

If you have already changed or removed the initial Shard B ownership map, do
not start Stage 10: preflight rejects an incompatible map.

### Acceptance tests

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=preflight
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=bootstrap
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=journal
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=status
```

The `bootstrap` test plans one durable migration job, captures an immutable
snapshot, repeats the operation to establish idempotency, and scans its
journal. Repeated runs reuse the same job. The `journal` test makes a real
`+1.00` deposit and `-1.00` withdrawal on `A2000` with distinct Request IDs;
it verifies that a duplicate deposit is not applied twice, that the account's
version increments by two, that the old source snapshot stays immutable, and
that observed versions catch up. Run without other writes on `A2000`.

**If the test fails between deposit and withdrawal, DO NOT retry it with new
request IDs or manually correct the account.** Examine the request states and
account balances first.

### Stage 11 handoff contract

- Input: `(job_id, snapshot rows, snapshot digest, per-account journal)`
- Create C database and at least two app Replicas in Stage 11.
- Copy snapshot to C into a non-writable target table; verify exact row count,
  digest and account membership before claiming target has caught up.
- Replay all complete transaction groups with a target-side atomic idempotency
  journal and account version checks. Respect overlapping accounts/order.
- During Stage 12, fence old B owner at its **database write path** before
  routing map epoch activation. Stage 13 must resolve in-flight 2PC safely.

**Completion:** stage10 acceptance confirms source snapshot/capture only,
never live migration/cutover correctness.

### Restart proof (without crash injection)

After `bootstrap`, check that immutable snapshot and durable checkpoints
survive process recreation:

```powershell
docker compose up -d --no-deps --force-recreate migration-manager
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=status
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=bootstrap
```

The job ID, digest and snapshot count must remain unchanged. No database
volumes or banking replicas need to be restarted.

See [Git workflow](git-workflow.md) for committing Stage 09 before replacing
its files and publishing Stage 10 as a separate truthful tag.
