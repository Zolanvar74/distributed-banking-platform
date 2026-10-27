# Stage 09 — Crash-consistent Self-Healing Controller (Bonus II)

## Purpose

Prove a **custom** Controller can be killed during recovery of a failed
banking replica, then resume **without creating two live containers** for one
logical slot. Persistence is in PostgreSQL. New Docker containers use
monotonically increasing generations and deterministic names; Gateway discovers
them from Metadata. This is a functional recovery demo, not a Docker restart
policy. All Compose services and created containers have automatic restart
**disabled**. Docker socket is exposed only inside the trusted local
`self-healing` service; do not deploy this on a shared host.

## Crash points and durable evidence

| Point | What has already happened before process exits? | Recovery requirement |
|---|---|---|
| `AFTER_RESERVE` | Old replica stopped, new generation & `REPLACEMENT_PLANNED` persisted | Create **same** reserved generation |
| `AFTER_CREATE` | Replacement Docker container **created** but not started | Adopt created container, then start it |
| `AFTER_START` | Docker container started, `CONTAINER_STARTED` persisted | Adopt running container and register healthy |
| `AFTER_HEALTHY` | `REPLICA_HEALTHY` and `container_id` persisted | Keep same healthy generation and container |

A one-shot experiment row transitions `ARMED -> FIRED -> VERIFIED` in
`healing_crash_experiments`. The process crashes **after FIRED is committed**
(the controller uses an autocommit connection). Startup reacquires a session
PostgreSQL advisory leader lock; lock is released by the crashed connection.
Experiment IDs isolate one run. A unique partial index prevents two unresolved
experiments targeting the same slot. No fault is injected without an **explicit
arm** and `STAGE09_ENABLE_FAULTS=1`.

## Installation (Windows PowerShell)

1. Copy the Stage 09 files onto the existing Git checkout (preserve `.git`,
   `.env` and PostgreSQL named volumes).
2. `docker compose config --quiet`
3. `docker compose up --build -d`
4. `docker compose logs schema-migrator`
5. `docker compose run --rm schema-migrator python -m pytest -q tests`
6. `docker compose run --rm schema-migrator python -m scripts.verify_stage09 --scenario=preflight`

If stage 08 created a managed replacement, **do not start the retired static
Compose container manually**. Stage 09's controller stops stale generations
it observes in the background, including after a Compose re-deploy. Do not run
`docker compose down -v`: database volumes contain your persistent balances,
transaction decisions and recovery state.

### Run one precise controlled crash (repeat for each point)

Make sure all four logical replica slots have returned to healthy. Enable
local controlled crashes (default is OFF):

```powershell
$env:STAGE09_ENABLE_FAULTS = "1"
docker compose up -d --no-deps --force-recreate self-healing
```

Choose `B2` for a predictable second healthy B replica. Works even after B2
has been replaced before; Docker labels identify the **current** generation.

```powershell
$trial = [guid]::NewGuid().ToString()
docker compose run --rm schema-migrator python -m scripts.verify_stage09 --scenario=arm --replica=B2 --point=AFTER_RESERVE --trial-id=$trial
```

If this reports ARMED, in a **second PowerShell window** stop exactly the
currently running B2 container (not the controller, B1 or the database):

```powershell
$ids = @(docker ps --filter "label=banking.project=distributed-banking" --filter "label=banking.shard=B" --filter "label=banking.slot=2" --filter "status=running" --format "{{.ID}}")
if ($ids.Count -ne 1) { throw "Expected exactly one running B2 container" }
docker stop $ids[0]
```

Back in the first PowerShell, verify the precise *intermediate* state:

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage09 --scenario=crashed --trial-id=$trial
docker compose ps self-healing
```

It must report `STAGE 09 CONTROLLER CRASH: VERIFIED; SAFE TO RESTART`. The
Controller is down, the durable fault is `FIRED`, and B1 still answers banking
requests via Gateway. **Do not use `docker compose up` or Docker restart
policies here:** only manually restart the stopped Controller:

```powershell
docker compose start self-healing
docker compose run --rm schema-migrator python -m scripts.verify_stage09 --scenario=recovered --trial-id=$trial
docker compose run --rm schema-migrator python -m scripts.verify_stage09 --scenario=show --trial-id=$trial
```

Expected: `STAGE 09 CONTROLLER RECOVERY: ALL CHECKS PASSED`. The verifier
checks Metadata's expected generation, the Docker API's unique active
incarnation, no older running replicas, both healthy logical slots for both
shards, working Gateway banking requests, and checks stability across another
Controller cycle.

**To repeat the test for the other three crash points**, generate a **new**
`$trial` GUID, set `--point=AFTER_CREATE`, `AFTER_START`, or `AFTER_HEALTHY`,
run the same steps again. Do not reuse a past experiment ID and do not stop B2
without arming a new run; otherwise the Controller will simply heal normally.

### Concurrent banking proof

Optional but recommended: open an additional terminal during the sequence,
before stopping B2:

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage08 --scenario=load --duration=60
```

This test performs zero-net paired bank operations on `A0500` and concurrent
reads; B2's failure and Controller's crash should not interrupt Shard A.
During the Controller outage B1 remains available for Shard B, and the
`crashed` verifier checks both balances. For a full cross-shard money-conservation
check after recovery run `python -m scripts.verify_stage06` in the migrator.
Never perform unrelated writes to `A0500` during the paired-write load test;
if a test is interrupted, reconcile request IDs before any manual repair.

### Turn off fault injection after testing

```powershell
$env:STAGE09_ENABLE_FAULTS = "0"
docker compose up -d --no-deps --force-recreate self-healing
Remove-Item Env:\STAGE09_ENABLE_FAULTS

docker compose run --rm schema-migrator python -m scripts.verify_stage09 --scenario=preflight
```

## Reset / deployment hygiene

**Safe ongoing development:** leave the new, current managed containers
running. A `docker compose up --build -d` can recreate Compose-owned original
containers. The controller will stop any stale running generations; verify
convergence with `--scenario=preflight`. Do **not** reset registry generation
numbers manually and do **not** delete stopped old containers while a trial is
unverified. The Stage 09 verifier allows stopped historical generations as
forensic evidence. Never use `down -v`.

## What is proved, and what is not

- Proves durable stage-by-stage recovery and avoiding duplicate *running*
  incarnations for a logical slot under the four controlled crash points.
- Proves another healthy replica and Gateway remain available in the
  single-replica + controller-crash test.
- Single local Docker Engine host, a functioning Metadata DB, and at least
  one remaining healthy app replica per shard are assumed. This is **not**
  database replication or tolerance of a host/Metadata loss.
- This experiment does not yet prove Live Re-Sharding. Stages 10–14 still
  need the CDC, Epoch, fencing, durable migration recovery and 2PC cutover tests.
