## Stage 12: Atomic Cutover / Stale Route Fencing

New: B→C migration ownership activation with a durable fail-closed state
machine, and shard-level epoch fencing. **Cutover is one-way** and must not
be run until Stage 11 tests have passed. Follow
[the Stage 12 runbook](docs/stage12-atomic-cutover.md) exactly.

# Distributed Banking Platform — Stage 09

**Python 3.12 · FastAPI · PostgreSQL 16 · Docker Compose.** Continuation of Stage 08 on existing named volumes.

**Stage 09 makes the custom self-healing controller crash-consistent (Bonus II).** Read [the Stage 09 recovery runbook](docs/stage09-controller-crash-recovery.md) and [the Stage 08 baseline](docs/stage08-self-healing.md). It also fixes the Stage 08 `dict_row` advisory-lock `KeyError: 0` bug. The `/health` endpoint now checks successful reconciliation cycles rather than merely a living thread.

- Docker Engine API starts a **new** container when a replica fails; no restart policies.
- The controller preserves healthy logical slots (A1/A2, B1/B2).
- Persistent replica generations, deterministic names and PostgreSQL advisory leader lock help adoption after controller restarts.
- Dynamic replica membership is detected by Gateway and Coordinator.
- Four controlled controller-crash injection points plus durable recovery tests are implemented in Stage 09.


Stage 07 adds **repeatable Coordinator crash recovery proof** to Stage 06 genuine
2PC. In addition to the existing banking, routing, and 2PC functionality:

- Deterministic one-shot process crash after the first durable COMMIT ACK.
- Recovery from COMMIT_DECIDED after restart and safe duplicate delivery.
- Durable PENDING transaction status readable by Gateway even when Coordinator is down.
- Integration checks for both shard prepared/commit states and money conservation.

Stage 06 capabilities retained:

- Shard-local SQL `PREPARE TRANSACTION` for debit/credit; separate shard-local PostgreSQL instances.
- Durable Coordinator decisions (COMMIT/ABORT), vote tracking, safe repeated messages, and a periodic recovery worker.
- Gateway routes same-shard transfers to one atomic local transaction and cross-shard transfers to the Coordinator.
- Status query by `tx_id` or `request_id`, PENDING/UNKNOWN for unresolved work; success only after both durable ACKs.
- Earlier balance, sharding, replication and failover APIs remain.

**Not yet complete:** online migration with safe 2PC cutover (Stages 10–14). Both bonus requirements remain mandatory for finishing the assignment.

## Windows upgrade

1. Extract ZIP and copy files into **existing** Git project. Keep `.git`, `.env` and named database volumes.
2. In your existing `.env`, optionally add `TWO_PC_TOKEN=a-long-private-random-value`. If omitted, Compose uses a **development-only** token; do not use the default outside a trusted local development environment.
3. In the folder containing `compose.yaml`, run:

```powershell
docker compose config --quiet
docker compose up --build -d
docker compose ps
docker compose logs schema-migrator
```

**Never run `docker compose down -v`** — it deletes your persistent database data.

## Verify

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage02
docker compose run --rm schema-migrator python -m scripts.verify_stage03
docker compose run --rm schema-migrator python -m scripts.verify_stage04 --scenario=normal
docker compose run --rm schema-migrator python -m scripts.verify_stage05
docker compose run --rm schema-migrator python -m scripts.verify_stage06
docker compose run --rm schema-migrator python -m pytest -q tests
```

Expected: `STAGE 06 TWO-PHASE COMMIT CHECK: ALL CHECKS PASSED`. After correcting Stage 08, confirm `python -m scripts.verify_stage08 --scenario=preflight`; then use `python -m scripts.verify_stage09 --scenario=preflight` and the Stage 09 runbook for crash injection. **Run Stage 05 before intentionally stopping static Replica A1** because the earlier acceptance test directly addresses that Compose service.


## Stage 07 exact crash test

Run the test in **[Stage 07 crash-recovery runbook](docs/stage07-crash-recovery.md)**.
It includes arming a unique request ID, enabling the opt-in crash flag,
provably checking durable COMMIT and partial acknowledgements **before**
restart, then restarting and checking final transaction/balance consistency.
**Never run `docker compose down -v`.** The `.env` and `.git` from prior stages
must be retained. The crash flag is **OFF by default**.

## Public API example

```powershell
$rid = [guid]::NewGuid().ToString()
$body = @{ from_account="A0500"; to_account="A1500"; amount="2.30"; request_id=$rid } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/transfers" -ContentType "application/json" -Body $body
Invoke-RestMethod "http://localhost:8000/transfers/requests/$rid"
```

The example makes a real, non-reversed transfer. For development tests, the automatic test reverses its changes on success. If the response is `PENDING`, query status later, **never change request_id for a retry of the same intended payment**.

See [2PC protocol, recovery and limits](docs/stage06-2pc.md) and earlier stage runbooks under `docs/`.

## Publish to existing GitHub repository

```powershell
git add .
git commit -m "feat: stage 07 controlled coordinator crash recovery"
git push origin main
git tag stage-07
git push origin stage-07
```

Publish only after all acceptance checks pass. Keep `.env` out of version control.

## Stage 08 healing test

Run `docker compose run --rm schema-migrator python -m scripts.verify_stage08 --scenario=preflight` before stopping any replica.

Then stop `shard-a-1`; the custom controller creates a replacement. Run `docker compose run --rm schema-migrator python -m scripts.verify_stage08 --scenario=healed` and inspect audit events in Metadata. For the concurrent live demo, use the two-terminal instructions in [the Stage 08 runbook](docs/stage08-self-healing.md).

**Docker socket is mounted into the self-healing service and grants host-level privileges; use only on your trusted, local development machine.**

After tests pass, commit and tag `stage-08` on the existing GitHub repository.

## Stage 09 Controller crash recovery

The full scenario matrix (`AFTER_RESERVE`, `AFTER_CREATE`, `AFTER_START`,
`AFTER_HEALTHY`) and Windows PowerShell commands are in
[docs/stage09-controller-crash-recovery.md](docs/stage09-controller-crash-recovery.md).
No Docker restart policy or external self-healing tool is used. Run Stage 08
`preflight` successfully before fault injection.

## Stage 10 — Source snapshot + online change capture (without cutover)

Stage 10 introduces the migration manager's crash-resumable source snapshot and
persistent per-account journal observations for `A1501..A2000` while Shard B
continues accepting banking operations. It does **not** add Shard C or change
the owner: full migration remains a Stage 11–13 deliverable.

Upgrade without restarting old Compose Replicas if Stage 08/09 healing already
replaced them. Run the PowerShell commands and four acceptance tests in
[docs/stage10-source-cdc.md](docs/stage10-source-cdc.md).

GitHub Actions runs Python unit/contract tests and validates Compose config on
each push to `main`. Docker-backed integration acceptance still needs the
local Compose environment.

For Stage 09/10 commits, tags and new GitHub Actions checks, see
[docs/git-workflow.md](docs/git-workflow.md).

## Stage 11 — online shadow copy and catch-up on Shard C

Stage 11 adds `shard-c-db` plus C1/C2 replicas and atomically replays source
banking change groups into non-writable `migration_target_accounts`. The
Gateway continues routing the migrating range to B; ownership changes and
write fencing occur **only** in later stages. See
[docs/stage11-live-shadow-replay.md](docs/stage11-live-shadow-replay.md)
for the safe Windows upgrade, validation and Git instructions.

## Stage 13 — Cross-shard 2PC after live migration

After **Stage 12's verified cutover**, run the Stage 13 schema upgrade, recreate
Gateway/Coordinator only, backfill historical B request IDs, then verify legacy
retries and A<->C 2PC. **Do not rerun the Stage 12 preflight/cutover** after
ownership changes. Full Windows PowerShell commands and the fail-closed
idempotency contract: [`docs/stage13-2pc-migration-idempotency.md`](docs/stage13-2pc-migration-idempotency.md).


## Stage 14 — fault-injection acceptance

See [`docs/stage14-final-acceptance.md`](docs/stage14-final-acceptance.md) before any fault injection. Faults are disabled by default, and destructive/restart tests are for a dedicated Compose test project, not production data.
