# Distributed Banking Platform — Stage 05

**Python 3.12 · FastAPI · PostgreSQL 16 · Docker Compose**  
Continuation of Stage 04 with **existing persistent volumes**. Both advanced bonuses (**Live Re-Sharding** and **Autonomous Self-Healing**) are mandatory future milestones.

## Implemented

- Shards A/B with 2 application replicas each sharing shard-local PostgreSQL storage.
- Metadata-based ownership map/epoch, health-probed replica Round Robin, read failover.
- Deposits, withdrawals, **same-shard transfers**, durable idempotent request IDs, account versioning, atomic change journals and write fences under PostgreSQL row locks.
- Shard-local status lookup for uncertain local operations.
- Cross-shard transfers **not yet available**: explicit HTTP 501 until real 2PC is implemented.
- Stubs and persistent schema for Coordinator, Migration Manager and Self-Healing, not yet complete.

## Upgrade from Stage 04 (Windows PowerShell)

Extract the ZIP and copy its `distributed-banking-platform` contents **into your current working Git repository**. Preserve `.git`, `.env` and named PostgreSQL volumes. No new `git init`. Do **not** use `docker compose down -v`.

```powershell
docker compose config --quiet
docker compose up --build -d
docker compose ps
docker compose logs schema-migrator
```

New `003_stage05_local_acid.sql` is applied to **both** shard DBs by the existing one-shot migrator. `002_foundation.sql` checksums and old data are unchanged; migration is transactional and rerunnable. The 11 long-running services remain; `schema-migrator` exits successfully.

## Test the release

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage02
docker compose run --rm schema-migrator python -m scripts.verify_stage03
docker compose run --rm schema-migrator python -m scripts.verify_stage04 --scenario=normal
docker compose run --rm schema-migrator python -m scripts.verify_stage05
docker compose exec -T gateway python -m scripts.smoke_test
```

Expected new acceptance line:

```text
STAGE 05 BANKING/CONCURRENCY CHECK: ALL CHECKS PASSED
```

The 05 test *writes to real accounts but compensates all balance changes on success*. It does not delete logs, deduplication records or incremented account versions. Do not run under unrelated workload; interrupted runs may leave effects that require reconciliation.

Detailed endpoint bodies, rollback guarantees, concurrency mechanism, testing and limitations are in **[docs/stage05-local-banking.md](docs/stage05-local-banking.md)**. Swagger documentation: <http://localhost:8000/docs>. Stage 04 runbook is still in `docs/stage04-replica-failover.md`.

## GitHub

Once all Docker tests pass, commit to your existing repository and create tag `stage-05`:

```powershell
git status
git add .
git commit -m "feat: stage 05 local ACID banking and concurrency"
git push origin main
git tag stage-05
git push origin stage-05
```

Keep `.env` out of GitHub (`git ls-files .env` prints nothing). Do not publish database passwords or customer data. Next: Stage 06 — **genuine cross-shard 2PC**.
