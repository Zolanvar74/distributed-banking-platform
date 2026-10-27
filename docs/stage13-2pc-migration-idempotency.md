# Stage 13 — 2PC + live migration / exact-once effect safeguards

## What this stage closes

Stage 12 switched ownership for `A1501–A2000` from B to C. **The B database still
contains the durable `local_request_dedup` ledger**, so a retry of an operation
already committed on B must *never* be executed a second time against C.
Stage 13 captures those old request IDs and persists them in metadata, before
allowing new requests touching the moved accounts.

1. The migration `009_stage13_legacy_idempotency.sql` adds
   `migrated_legacy_requests` and `migrated_request_backfill` to metadata.
2. After B is fenced and C is active, the archive runner reads all B historical
   local request outcomes from a single repeatable-read snapshot. It commits
   the rows **and** the `COMPLETE` marker in one metadata transaction.
3. Until `COMPLETE`, Gateway responds `503/PENDING` to any new monetary request
   touching the moved account range. Clients must retry the **same** request ID.
4. When a historical ID is found, Gateway compares the original fingerprint,
   returns the durable original result on an identical retry, and returns `409`
   on a conflicting payload. No new shard mutation occurs.
5. For cross-shard requests, Gateway first checks the Coordinator's durable
   `transactions` table, even when current Shard Map would route the request as
   same-shard. The Coordinator also defends against direct internal calls
   reusing IDs from former B local transfers.
6. A previously prepared/committed participant is resolved only on its ORIGINAL
   shard as stored in the `transaction_participants` table. Never substitute
   the new owner C for B while delivering an old COMMIT decision.

**Scope / limitation:** The exercise has a single B->C migration. Its post-cutover
request archive is one-way and does not constitute a fully generic global request
registry for unlimited future migrations. Historical `PENDING` rows remain
uncertain and are NOT automatically replayed against a new owner. Controller and
Coordinator crash recovery experiments from earlier stages remain essential.

## Installation and execution on Windows

**Do not run Stage 13 if Stage 12 `stale` and `recovered` have not passed.**
Stage 13 expects exactly one migration job already in `VERIFYING` or `COMPLETED`.
Avoid unrelated banking requests during the short deployment/backfill window.
Keep `.git`, `.env`, Docker volumes, and all existing container generations.

```powershell
# In the project root after copying the updated files
# First build and run a one-shot DB schema upgrade

docker compose config --quiet
docker compose build
docker compose run --rm schema-migrator python scripts/apply_migrations.py

# IMPORTANT: install the Gateway which fences moved-range requests BEFORE
# backfilling historical B results.
docker compose up -d --no-deps --force-recreate gateway coordinator

# One-time, idempotent, atomic metadata archive (no balance modifications)
docker compose run --rm schema-migrator python -m scripts.backfill_stage13

# Read-only verification
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=preflight

# Historical B request ID replay (no monetary effects)
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=legacy

# Real 2PC transfers + duplicate message + conflict check
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=cross

# B rejects stale direct 2PC PREPARE, even bypassing Gateway
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=stale2pc

# Four concurrent transfers against the same two accounts
# Do not run in parallel with other account operations.
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=overlap
```

Caution: a `--force-recreate` on every replica is **not** safe after Self-Healing
has replaced containers. Only `gateway` and `coordinator` are recreated here.
Ensure `STAGE07_ENABLE_FAULTS=0` and that no prepared transaction remains stuck
before running monetary experiments. Stage 13's test script prints request IDs.
If a test exits unexpectedly, query `/transfers/requests/{request_id}`, wait for
recovery and do **not** perform SQL/manual balance repair or retry with a new ID.

## Expected results

- `STAGE 13 LEGACY REQUEST ARCHIVE: COMPLETE (... B rows)`
- `STAGE 13 PREFLIGHT: ALL CHECKS PASSED`
- `STAGE 13 LEGACY RETRY: ALL CHECKS PASSED`
- `STAGE 13 POST-CUTOVER 2PC: ALL CHECKS PASSED`
- `STAGE 13 CONCURRENT 2PC: ALL CHECKS PASSED`
- `STAGE 13 STALE 2PC FENCING: ALL CHECKS PASSED`

Failures involving external Docker networking, deadlock timing and recovery
cannot be ruled out by Python unit tests; the real Docker test is mandatory.

## Version control

Only tag `stage-13` when all required tests have passed on Docker Desktop:

```powershell
git status
git diff --check
git ls-files .env
# Empty output for .env is required.
git add .
git commit -m "feat: stage 13 migration-safe idempotency and cross-shard retries"
git push origin main
git tag stage-13
git push origin stage-13
```

Never force-push or overwrite an existing stage tag. Prefer a corrective commit
with a new tag, if an earlier stage tag already exists.
