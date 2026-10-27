# Stage 11 — add Shard C and replay source account journal to non-writable shadow storage

**Assignment scope:** add independent Shard C PostgreSQL and two banking app replicas; online copy of `A1501..A2000`; replay grouped committed source changes while Shard B keeps processing banking operations. This is **not cutover**; Stage 12/13 handles epoch fencing, materialization, ownership transfer and prepared 2PC reconciliation.

## Storage and guarantees

* `shard-c-db` owns an independent named volume `shard-c-data` and starts with an empty `accounts` table; no `writable=true` ownership fence exists in C. Consequently, C1/C2 are healthy but correctly reject migrated-account reads/writes at the ordinary Banking API, including direct stale-route attempts. Gateway still routes `A1501..A2000` to Shard B.
* Shard B's immutable Stage-10 snapshot contains `(account_id,balance,version)` and a digest. One transaction on C copies it to `migration_target_batches` and `migration_target_accounts`. The shadow table keeps both original snapshot balance/version and its newer replay state. Repeating copy checks the original digest/membership without overwriting replayed balances.
* Stage-05 account changes and group records commit in the same source transaction as balances, including 2PC prepared transactions once `COMMIT PREPARED` has completed. We read a REPEATABLE READ MVCC source view; version-by-account and before/after checks detect missing, duplicated, or inconsistent journal effects. **We do not assume timestamps or BIGSERIAL IDs reflect commit order.**
* Replaying a committed group updates **all migrated accounts affected by that group** in a single C transaction with `migration_target_applied_groups` as an atomic idempotency record. A group involving an account outside this migrating range replays only the migrating member. Replay respects per-account predecessor versions even across overlapping same-shard transfers. Full source group entries must be committed and present.
* Metadata `migration_target_progress` is **acknowledged only after target C commits**. If migration-manager crashes between C commit and progress acknowledgment, it uses the replay ledger and per-account versions to retry safely. A background loop resumes catch-up every few seconds. `caught_up_at_source_read` certifies one consistent read point only; it is not a permanent final cutover guarantee while B accepts new writes.
* No direct SQL balance modifications are supported during the migration; those bypass the transactionally maintained application journal. If the source membership changes or a journal gap occurs, the worker rejects the operation instead of claiming success.

## Upgrade on Windows PowerShell (keep existing project and PostgreSQL volumes)

1. Commit current verified Stage 10 tree first, using `git status`/`git diff`, and tag `stage-10` **only if the Stage 10 acceptance suite passed**. Do not silently label untested code.
2. Extract the Stage 11 ZIP. Copy contents of `distributed-banking-platform` over the existing project, preserving the existing `.git` directory and `.env`. **Do not** use `docker compose down -v` or recreate all banking replicas. Stage 08/09 may have made replacement Replica generations.
3. In your existing PowerShell project directory:

```powershell
# 1. Check configuration and build latest application image
#    This does not restart old healed replica containers.
docker compose config --quiet
docker compose build

# 2. Start new isolated Shard C database
docker compose up -d shard-c-db

# 3. Apply additive migrations on Metadata, A, B and C.
#    The runner refuses edits to already-applied migrations.
docker compose run --rm schema-migrator python scripts/apply_migrations.py

# 4. Start only the two NEW banking replicas (without touching A1/A2/B1/B2)
docker compose up -d --no-deps shard-c-1 shard-c-2

# 5. Update migration-manager and self-healing controller (without
#    restarting any existing healed banking replicas)
docker compose up -d --no-deps --force-recreate migration-manager self-healing

docker compose ps
docker compose logs --tail 60 migration-manager self-healing
```

**Docker Engine socket warning:** self-healing has high-privilege Docker socket access, so use only a trusted local machine. No Docker automatic restart policies are introduced.

## Stage 11 acceptance tests

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage11 --scenario=preflight
docker compose run --rm schema-migrator python -m scripts.verify_stage11 --scenario=bootstrap
docker compose run --rm schema-migrator python -m scripts.verify_stage11 --scenario=online
```

`preflight`: C database and C1/C2 are healthy, but C has zero write fences; B is still the active map owner. `bootstrap`: preserves immutable B snapshot, copies it idempotently to C, verifies target snapshot digest, and starts group replay with durable per-account acks. `online`: 4 concurrent deposits then 4 concurrent withdrawals via the **Gateway**, with source replay between the batches; duplicate replay is a no-op and C matches the source balance and version. The test uses real `A2000` money operations and prints request IDs before running. **Do not run other transactions on A2000 while the test executes.** If it is interrupted, inspect the request IDs / balances and pending 2PC transactions before running a new trial; do not manually adjust authoritative balances or blindly retry fresh IDs.

### Crash/restart check

```powershell
docker compose up -d --no-deps --force-recreate migration-manager
docker compose run --rm schema-migrator python -m scripts.verify_stage11 --scenario=resume
```

Confirm that same snapshot/job survives, C catches up, and Gateway still sends all migrated-account requests to B.

### Direct proof via SQL

```powershell
# Authoritative source (B): account A2000.
docker compose exec -T shard-b-db psql -U banking -d shard_b -c "SELECT account_id,balance,version FROM accounts WHERE account_id='A2000';"

# Non-writable target C: shadow state, not active banking account.
docker compose exec -T shard-c-db psql -U banking -d shard_c -c "SELECT account_id,balance,version FROM migration_target_accounts ORDER BY account_id;"

# Durable coordinator/manager progress, distinct from source observations.
docker compose exec -T metadata-db psql -U banking -d metadata -c "SELECT account_id,snapshot_version,applied_version FROM migration_target_progress ORDER BY account_id;"
```

Note: Shard B is still authoritative until Stage 12/13's correctly fenced final cutover; the target's `accounts` table must remain empty at this stage.

## Git / GitHub

After **all actual acceptance tests succeed**:

```powershell
git status
git diff --check
git check-ignore -v .env
git ls-files .env  # MUST print nothing
# Stage files only; make sure there are no generated DB volumes/secrets.
git add .
git diff --cached --check
git commit -m "feat: stage 11 online shadow copy and atomic replay to shard C"
git push origin main
# Only if the tag does not already exist:
git tag stage-11
git push origin stage-11
```

GitHub Actions checks static/contract code; the real multi-container Docker tests are still run locally. See `.github/workflows/python-ci.yml`.
