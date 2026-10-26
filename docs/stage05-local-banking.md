# Stage 05 — ACID banking, concurrency and shard-local idempotency

## Scope and what is **not** implemented

Included: deposit, withdraw, one-transaction **same-shard** transfers, PostgreSQL row locks across A1/A2 (or B1/B2), durable request IDs, shard-local operation status, per-account versions and change groups, epoch-checked local write fences, concurrent-load checks and accounting invariants. Both bonus milestones remain required. **Cross-shard transfer is deliberately HTTP 501 until the real 2PC Coordinator is implemented in Stage 06.** No sequence of unrelated committed debits and credits is ever called a cross-shard transfer.

No authentication, authorization, TLS, production monitoring or fraud controls are provided: **this is a local assignment demo, not a bank-ready service**. Shard-level idempotency is currently local to the owning DB; Stage 07 adds cross-shard/global semantics. Stage 05's fence lock is an essential primitive for later live migration, *not* complete live cutover or CDC.

## API — send amounts as JSON **strings**, never floating-point numbers

- `POST /accounts/A0500/deposit` with `{"amount":"10.25","request_id":"<unique-id>"}`
- `POST /accounts/A0500/withdraw` with the same body structure
- `POST /transfers` with `{"from_account":"A0500","to_account":"A0001","amount":"5.25","request_id":"<unique-id>"}`
- `GET /accounts/A0500/operations/<request_id>` returns a committed/aborted shard-local result
- `GET /accounts/A0500/balance` unchanged; `/docs` shows current API

Amounts permit at most two decimal places, range `0.01` to `999999999999999999.99` (NUMERIC(20,2)). `request_id` length 1–200; use UUIDs in clients. The **same** ID **and same payload** may be retried without duplicate effects; same ID and *different* payload receives 409. Insufficient funds is HTTP 409 and is saved as a durable rejected outcome. Missing account is 404. The user must NOT reuse an ID for a different operation on that shard.

A sample committed operation returns:

```json
{"status":"COMMITTED","request_id":"example-0001","tx_id":"<uuid>","kind":"DEPOSIT", "account_id":"A0500","amount":"10.25","balance":"1010.25","shard":"A","served_by":"A1","idempotent_replay":false}
```

That balance is illustrative only, not a guaranteed current balance. For duplicate requests, `idempotent_replay` is true and tx ID stays the same; `served_by` reports which replica returned the replay. The Gateway forwards each write to **one selected replica only**. It does not automatically retry uncertain POSTs: a dropped response after SQL COMMIT must return `outcome: UNKNOWN`, not a false failure or false success. Check status and/or retry the **same ID and same payload**.

## Why it is atomic

1. Gateway looks up `shard_map` and probes healthy replica candidates. Both source and recipient must be owned by the **same** shard for a local transfer.
2. Banking replica opens one PostgreSQL transaction, locks the required `ownership_fences` rows with `FOR SHARE` and verifies routing epoch, phase, and writability. A migration cutover will need an opposing exclusive lock.
3. It inserts a unique `local_request_dedup` record. Conflicting duplicates across replicas are serialized by the *database unique index*, not process memory.
4. It locks account rows with `SELECT .. FOR UPDATE` in sorted account-ID order. Checks funds and the NUMERIC(20,2) ceiling.
5. It updates balances **and** versions, inserts a durable `account_change_groups` row and 1/2 `account_changes` rows; then persists a committed/aborted response in `local_request_dedup` within **the same transaction**.
6. HTTP 200 is returned only **after** the context manager successfully committed. On DB errors the endpoint reports `503 UNKNOWN` conservatively. Partially committed local transfers cannot arise.

### Migration journal limits

Versions and groups are durable, but do NOT by themselves constitute a globally ordered commit stream. The later migration design must provide consistent snapshots, per-account (or correct commit-order) change capture, durable replay checkpoints, final cutover and fencing. In particular, `BIGSERIAL`/timestamps must not be mistaken for commit order.

## Run on Windows PowerShell (keep `.env`, `.git`, DB volumes)

```powershell
docker compose config --quiet
docker compose up --build -d
docker compose ps
docker compose logs schema-migrator
docker compose run --rm schema-migrator python -m scripts.verify_stage02
docker compose run --rm schema-migrator python -m scripts.verify_stage03
docker compose run --rm schema-migrator python -m scripts.verify_stage04 --scenario=normal
docker compose run --rm schema-migrator python -m scripts.verify_stage05
docker compose exec -T gateway python -m scripts.smoke_test
```

`verify_stage05` uses unique request IDs, concurrent 16 deposits + 16 withdrawals, both A replicas, B shard operations, same-shard transfers and reverse transfers, history checks, stale-epoch rejection and cross-shard denial. On **successful completion** all tested account balances are restored exactly. Versions and journals correctly advance, and request IDs stay recorded. Do NOT run while unrelated writers are operating on its seeded test accounts. If interrupted, reconcile via the status endpoint; **do not erase DB volumes or blindly rerun resets**.

Inspect durable ledger:

```powershell
docker compose exec -T shard-a-db psql -U banking -d shard_a -c "SELECT account_id,balance,version FROM accounts ORDER BY account_id;"
docker compose exec -T shard-a-db psql -U banking -d shard_a -c "SELECT operation_kind,expected_entries,tx_id FROM account_change_groups ORDER BY created_at DESC LIMIT 10;"
```

## Git after all checks pass

```powershell
git add .
git commit -m "feat: stage 05 local ACID banking and concurrency"
git push origin main
git tag stage-05
git push origin stage-05
```

Do not use `docker compose down -v` and do not commit `.env`. Real Docker/SQL tests must run on the user's system; syntax/unit tests alone do not establish integration success.
