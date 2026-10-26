# Distributed Banking Platform — Stage 07

**Python 3.12 · FastAPI · PostgreSQL 16 · Docker Compose.** Continuation of Stage 06 on existing named volumes.

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

**Not yet complete:** replacement containers (Stages 08–09), and online migration with safe 2PC cutover (Stages 10–14). Both bonuses remain required to finish the assignment.

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

Expected: `STAGE 06 TWO-PHASE COMMIT CHECK: ALL CHECKS PASSED`.


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
