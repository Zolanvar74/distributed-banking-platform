# Stage 14 — adversarial fault injection and full acceptance

> Applies to **Stage 13 + Stage 14**, not a generic PostgreSQL demo. These are
> real process-termination faults; do **not** enable them on your regular Compose
> project. Keep `.env`, `.git` and existing DB volumes intact.

## Scope, safety, and the one-way migration rule

Stage 12 cutover is intentionally one-way. **Do not** try to run four cutover
fault experiments on a project where Shard C already owns A1501–A2000. Each
fault needs its **own fresh, disposable, isolated Compose project with its own
volumes**. Stage 14 does **not** automatically delete/recreate any DB volume.
Set `STAGE14_ENABLE_FAULTS=1` **only** in those isolated fault runs. The default
is `0`. Docker auto-restart policies are still absent. Crash point claims are
persistent and one-shot. Stage 09 has its own separate controller-fault flag.

`docker compose down -v` WILL DESTROY CURRENT PROJECT DATA. Never run this on
`distributed-banking` or without verifying which project is selected.

### Windows PowerShell: regular project, safe read-only audit

After Stage 12 cutover and Stage 13 legacy-request backfill:

```powershell
docker compose config --quiet
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=preflight
docker compose run --rm schema-migrator python -m scripts.verify_stage14 --scenario=postcutover --json
```

For actual money-conservation evidence run the existing paired transfer and
concurrency checks **without other banking clients**. The Stage 13 checks use
real transactions and separate compensating transfers; if interrupted, query
request status rather than changing balances directly:

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=cross
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=overlap
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=stale2pc
```

Previous Stage 10/11 `preflight` and Stage 12 `cutover` tests assume pre-cutover
ownership; they are **not** appropriate after Stage 12. Some old Stage 04/05/06
tests assume original Compose replica names rather than replaced generations.
Run early-stage demos in a fresh test project *before* re-sharding, or use the
current generation-aware Stage 08/09/12/13 integration tests.

## Isolated fault lab (same machine, no interference with main DB)

In PowerShell, on a machine with sufficient memory and a trusted local Docker
Desktop engine, select a unique project **and unused host port for each point**:

```powershell
$env:COMPOSE_PROJECT_NAME = 'banking-s14-snapshot'
$env:GATEWAY_PORT = '8114'
$env:STAGE14_ENABLE_FAULTS = '1'
docker compose config --quiet
docker compose up --build -d
```

For a second independent experiment change `banking-s14-snapshot` to
`banking-s14-delta` (and port to `8115`), similarly `banking-s14-fence` (`8116`),
`banking-s14-map` (`8117`). **Do not** just change the name on an active
terminal and accidentally point subsequent commands at a different project.

The isolated Compose project name is used in its network, labels and managed
replica names. The `self-healing` container still has sensitive access to the
host's Docker socket: run fault experiments on a *trusted local machine only*.
For every experiment, run a fresh `docker compose up --build -d` with that
project name and check:

```powershell
docker compose ps
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=preflight
```

The `stage14_fault` administration script runs **only** on the internal Docker
network. `--action arm` must be invoked with its opt-in flag explicitly:

```powershell
docker compose run --rm -e STAGE14_ENABLE_FAULTS=1 schema-migrator python -m scripts.stage14_fault --action arm --point SNAPSHOT_PERSISTED
```

After a true injected process exit, the `migration-manager` process/container
should be exited (verify with `docker compose ps -a migration-manager`). A
failed HTTP response by itself is not evidence. Use durable proof:

```powershell
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action inspect
```

### Test 1 — crash after the source Snapshot is durably stored

Fresh **snapshot** lab:

```powershell
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action plan
docker compose run --rm -e STAGE14_ENABLE_FAULTS=1 schema-migrator python -m scripts.stage14_fault --action arm --point SNAPSHOT_PERSISTED
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action trigger --point SNAPSHOT_PERSISTED
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action verify --point SNAPSHOT_PERSISTED --moment crashed
```

The crash happens **after** source snapshot transaction COMMIT but **before**
Metadata records `snapshot_id`. Restart manager and complete the same snapshot:

```powershell
$env:STAGE14_ENABLE_FAULTS = '0'
docker compose up -d --no-deps --force-recreate migration-manager
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=bootstrap
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action verify --point SNAPSHOT_PERSISTED --moment recovered
```

`SNAPSHOT_PERSISTED` should have `FIRED` (one-shot), and the digest and account
count must remain stable.

### Test 2 — crash after a durable replay group commit but before Metadata ACK

Fresh **delta** lab; establish a snapshot and copied target, and stop the
migration manager so source changes accumulate before arming:

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=bootstrap
docker compose run --rm schema-migrator python -m scripts.verify_stage11 --scenario=bootstrap
docker compose stop migration-manager
$rid = [guid]::NewGuid().ToString()
$body = @{ amount='1.00'; request_id=$rid } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri 'http://localhost:8115/accounts/A2000/deposit' -ContentType 'application/json' -Body $body
$rid2 = [guid]::NewGuid().ToString()
$body2 = @{ amount='1.00'; request_id=$rid2 } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri 'http://localhost:8115/accounts/A2000/withdraw' -ContentType 'application/json' -Body $body2
docker compose run --rm -e STAGE14_ENABLE_FAULTS=1 schema-migrator python -m scripts.stage14_fault --action arm --point DELTA_GROUP_APPLIED
docker compose start migration-manager
```

No new user request ID on retries. Check `FIRED` and replay group ledger before
restarting, then restart the manager and wait for its replay loop to catch up:

```powershell
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action verify --point DELTA_GROUP_APPLIED --moment crashed
$env:STAGE14_ENABLE_FAULTS = '0'
docker compose up -d --no-deps --force-recreate migration-manager
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action verify --point DELTA_GROUP_APPLIED --moment recovered
```

The recovered verification may need a repeat after the background replay pass.
It checks that source and shadow balance/version match, without performing
replay itself or modifying account balances.

### Test 3 — crash after closing Metadata FENCING gate

Fresh **fence** lab:

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=bootstrap
docker compose run --rm schema-migrator python -m scripts.verify_stage11 --scenario=bootstrap
docker compose run --rm schema-migrator python -m scripts.verify_stage11 --scenario=online
docker compose run --rm -e STAGE14_ENABLE_FAULTS=1 schema-migrator python -m scripts.stage14_fault --action arm --point FENCING_GATE_CLOSED
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action trigger --point FENCING_GATE_CLOSED
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action verify --point FENCING_GATE_CLOSED --moment crashed
$env:STAGE14_ENABLE_FAULTS = '0'
docker compose up -d --no-deps --force-recreate migration-manager
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action verify --point FENCING_GATE_CLOSED --moment recovered
```

At crash, Metadata must still show Shard B as active owner (no early C
activation). The recovery worker continues the durable phase machine to
`VERIFYING` or halts safely on outstanding 2PC.

### Test 4 — crash immediately after atomic Shard Map activation

Fresh **map** lab, analogous bootstrap:

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage10 --scenario=bootstrap
docker compose run --rm schema-migrator python -m scripts.verify_stage11 --scenario=bootstrap
docker compose run --rm schema-migrator python -m scripts.verify_stage11 --scenario=online
docker compose run --rm -e STAGE14_ENABLE_FAULTS=1 schema-migrator python -m scripts.stage14_fault --action arm --point MAP_ACTIVATED
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action trigger --point MAP_ACTIVATED
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action verify --point MAP_ACTIVATED --moment crashed
$env:STAGE14_ENABLE_FAULTS = '0'
docker compose up -d --no-deps --force-recreate migration-manager
docker compose run --rm schema-migrator python -m scripts.stage14_fault --action verify --point MAP_ACTIVATED --moment recovered
```

At crash, Metadata points to C but C remains read-only and B is fenced.
After restart, C becomes writable with the new Epoch and old B remains fenced.
The test intentionally crashes **before target activation**; archival/cleanup
remains conservative because old B data is retained for audit. Do not manually
remove the B rows.

### Recovery verification after Cutover

On *fence* or *map* lab after recovery:

```powershell
docker compose run --rm schema-migrator python -m scripts.backfill_stage13
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=preflight
docker compose run --rm schema-migrator python -m scripts.verify_stage14 --scenario=postcutover --json
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=cross
docker compose run --rm schema-migrator python -m scripts.verify_stage13 --scenario=overlap
```

### 7 required experiments and bonus acceptance matrix

| Requirement | Evidence before cutover | Evidence after cutover / under failure |
|---|---|---|
| 1. Shard distribution | Stage03 `verify_stage03` | Stage14 `postcutover` owner map/DB checks |
| 2. Replica load distribution | Stage04 `normal` | Stage14 `postcutover` six healthy replicas |
| 3. Concurrent replica writes | Stage05 `verify_stage05` | Stage13 `overlap` for cross-shard transfers |
| 4. Replica failover | Stage04 `a1-down` | Stage08 `healed` (new generation/container) |
| 5. Local and cross-shard | Stage05 + Stage06 | Stage13 `cross` (A↔C) |
| 6. Interrupted 2PC | Stage07 crash/recovery | Confirm no unresolved prepared 2PC in Stage14 audit |
| 7. Concurrent cross-shard | Stage13 `overlap` | Stage14 postcutover audit + request status logs |
| Bonus I (+12%) | Stages10–11 `bootstrap`,`online`,`resume` | Four Stage14 fault points + Stage12 `stale`, Stage13 `legacy`,`cross` |
| Bonus II (+10%) | Stages08–09 crash/integration demos | Stage14 checks current slot health; Docker duplicate proof from Stage09 |

**Important:** Stage14 does *not* rerun the Stage07 controlled COMMIT crash or
Stage09 controller crash automatically. Keep those prior experiment logs, or
repeat in separate safe demo projects with their respective specific flags.

### Git and evidence collection

Keep `PS transcript`, `docker compose ps`, `docker compose logs migration-manager`,
`--action inspect`, `--moment crashed`, and `--moment recovered` output for each
of the four independently isolated projects. Label evidence with project name,
point, job id, experiment id, request ids and Git commit SHA.

```powershell
git status
git diff --check
git ls-files .env
git add .
git commit -m "test: stage 14 adversarial recovery and acceptance matrix"
git push origin main
git tag stage-14
git push origin stage-14
```

Create the tag only after integration results pass. Always turn off the fault
flag and clear the shell variables when finished:

```powershell
Remove-Item Env:\STAGE14_ENABLE_FAULTS -ErrorAction SilentlyContinue
Remove-Item Env:\COMPOSE_PROJECT_NAME -ErrorAction SilentlyContinue
Remove-Item Env:\GATEWAY_PORT -ErrorAction SilentlyContinue
```

No production deployment, direct SQL balance repairs, automated volume
deletion, or silent override of uncertain 2PC decisions is part of this stage.
