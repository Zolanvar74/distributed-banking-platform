# Distributed Banking Platform — Stage 03

**Python 3.12 · FastAPI · PostgreSQL 16 · Docker Compose**  
This is a continuation of the working Stage 02 project, **not** a fresh installation.

Stage 03 verifies correct two-shard account distribution, Gateway owner selection, live replica checks, and shard-local read routing claims. It preserves all Stage 02 metadata (durable 2PC placeholders, migration checkpoints/fences and replica registry). **Deposits, withdrawals, transfers, actual 2PC, self-healing and online migration are not yet implemented.** Both expert bonuses remain required project milestones.

## Upgrade without wiping previous account data

1. Extract this ZIP. Copy the files from its `distributed-banking-platform` folder **into the existing project root** (the folder containing your `.git` and `.env`). Do not replace the existing `.env` or `.git` folder.
2. Run in **Windows PowerShell** from the project root:

```powershell
docker compose config --quiet
docker compose up --build -d
docker compose ps
docker compose run --rm schema-migrator python scripts/verify_stage02.py
docker compose exec -T gateway python scripts/smoke_test.py
docker compose run --rm schema-migrator python scripts/verify_stage03.py
```

Expected final messages:

```text
STAGE 02 SCHEMA CHECK: ALL CHECKS PASSED
STAGE 01 SMOKE TEST: ALL CHECKS PASSED
STAGE 03 SHARDING CHECK: ALL CHECKS PASSED
```

The exact Stage 02 and Stage 01 regression test names are deliberately unchanged.

**Never use `docker compose down -v`** because `-v` deletes named Postgres volumes. The Compose `name: distributed-banking` remains the same, and its named volumes are reused. The Stage 02 SQL migration checksums and files are unchanged. The one-shot `schema-migrator` exits after completion; 11 application/DB containers remain running.

## Manually demonstrate routing

Open:

- http://localhost:8000/debug/shard-map
- http://localhost:8000/debug/route/A0500
- http://localhost:8000/accounts/A0500/balance
- http://localhost:8000/debug/route/A1500
- http://localhost:8000/accounts/A1500/balance

Inspect the two physical databases separately:

```powershell
docker compose exec -T shard-a-db psql -U banking -d shard_a -c "SELECT account_id, balance FROM accounts ORDER BY account_id;"
docker compose exec -T shard-b-db psql -U banking -d shard_b -c "SELECT account_id, balance FROM accounts ORDER BY account_id;"
```

All Stage 03 details, endpoint contracts and limits: [docs/stage03-routing-contract.md](docs/stage03-routing-contract.md).

## Troubleshooting

```powershell
docker compose ps -a
docker compose logs --tail=100 schema-migrator
docker compose logs --tail=100 gateway shard-a-1 shard-a-2 shard-b-1 shard-b-2
```

If the new health/identity filtering rejects traffic, inspect the banking services' registrations and check whether the correct containers started. Stage 03 uses the existing `replica_registry`; the future autonomous controller is not yet active.

## Commit Stage 03 to the SAME GitHub repository

Only after all tests pass:

```powershell
git status
git add .
git status
git commit -m "feat: verify stage 03 ownership and shard-aware routing"
git push origin main
git tag stage-03
git push origin stage-03
```

Check that `.env` is **not** tracked: `git ls-files .env` must print nothing. Never commit database dumps containing private data or `.env` credentials.
