# Distributed Banking Platform — Stage 02

Python 3.12 + FastAPI | PostgreSQL 16 | Docker Compose | Windows PowerShell

Stage 02 adds a **versioned, persistent database schema** for 2PC, migration and self-healing. This is a continuation of Stage 01: balance lookup, shard routing and healthy replica selection still work. The Coordinator, Migration Manager and Self-Healing Controller are still health-only placeholders; **they do not perform 2PC, CDC or recovery yet**.

## Upgrade the Stage 01 project on Windows (preserve data)

1. Download and extract the Stage 02 ZIP. It contains the same `distributed-banking-platform/` root folder as Stage 01.
2. Replace the **source files** in your existing Stage 01 project with the Stage 02 files. Keep your existing `.env` and PostgreSQL volumes; `.env` is not in the ZIP. Do not extract into an additional nested folder. Alternatively, use a fresh folder if Stage 01 data does not need to be retained, then create `.env` from the example.
3. Open PowerShell at the project root where `compose.yaml` is located. With Docker Desktop running, execute:

```powershell
# Only for a newly extracted standalone project that has no existing .env:
# Copy-Item .env.example .env

docker compose config --quiet
docker compose up --build -d
docker compose ps
docker compose logs schema-migrator
docker compose run --rm schema-migrator python scripts/verify_stage02.py
docker compose exec -T gateway python scripts/smoke_test.py
```

Expected success messages:

```text
STAGE 02 MIGRATIONS: ALL APPLIED OR ALREADY PRESENT
STAGE 02 SCHEMA CHECK: ALL CHECKS PASSED
STAGE 01 SMOKE TEST: ALL CHECKS PASSED
```

The smoke test retains its Stage 01 label because it verifies the same read-only behavior.

## How many containers?

- **11 long-running containers:** 3 PostgreSQL, 4 banking replicas, Gateway, Coordinator, Migration Manager, Self-Healing placeholder.
- **1 completed one-shot `schema-migrator`:** visible with `docker compose ps -a`, but not a continuously running application container.

If schema-migrator fails, the app services will not start successfully; read its logs and fix the error. **Do not reset the volumes.**

## Migration idempotency test

```powershell
docker compose run --rm schema-migrator
```

Already completed migrations must print `SKIP`, not `APPLIED`. Each database independently records a hash in `schema_migrations`.

## Key files

```text
compose.yaml                          service startup ordering + one-shot migrator
Dockerfile                            includes SQL migrations inside image
scripts/apply_migrations.py            safe transactional schema migrations
scripts/verify_stage02.py              Stage 02 read-only verification
sql/migrations/metadata/002_foundation.sql   dynamic ownership, 2PC, migration, registry
sql/migrations/shard/002_foundation.sql      local fences, journals, request dedup
sql/{metadata,shard}/*                 legacy first-run initialization (Stage 01)
docs/stage02-database-contract.md      explanation, assumptions, upcoming protocols
```

## Important limitations of Stage 02

- The old read-only balance API remains live; **no deposits/withdrawals/transfers are exposed yet**.
- `ownership_fences` are schema-level contracts for the later transactional write path, not yet enforced by a banking mutation API.
- `account_changes` is not filled by banking operations at this stage; full ordered change capture and catch-up are implemented later, and require multi-account/2PC scrutiny.
- No automated replica replacement happens yet; `desired_replica_count` alone does not heal a failed replica.
- This assignment is designed for a single-machine demonstration, not an internet-facing banking deployment.

## Troubleshooting

```powershell
docker compose logs --tail=100 schema-migrator
docker compose logs --tail=100 metadata-db
docker compose ps -a
```

**Never run `docker compose down -v` on a project whose data you need.** The `-v` flag deletes named Postgres volumes. `docker compose down` alone keeps those volumes.

Stage 03: verify account ownership and shard routing, add explicit shard distribution tests, then proceed with dynamic replication, banking writes, 2PC, self-healing and live migration.
