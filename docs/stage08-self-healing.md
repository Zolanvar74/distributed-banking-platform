# Stage 08 — Autonomous Self-Healing (Bonus II, +10%)

## Goal

Custom Docker Engine controller restores the **configured count of healthy replicas**
for each active shard. This is real container creation, not merely Gateway
failover. There are **no** Docker `restart` policies, Kubernetes, Swarm or
prebuilt self-healing services.

## Lifecycle and crash invariants

1. Compose starts A1, A2, B1, B2 in generation 1. Replica registration is durable.
2. Controller obtains a **session-scoped PostgreSQL advisory leader lock**.
3. Every 2 seconds it reads `desired_replica_count`, registry state, Docker
   labels and replica `/health`. Two failed probes trigger repair.
4. The controller does not replace application replicas if their **shared shard
   database** is unavailable.
5. It **stops the unhealthy old container**, durably increments that logical
   slot's generation in Metadata and records a `REPLACEMENT_PLANNED` event.
6. It creates a **new** container with a deterministic name, e.g.
   `distributed-banking-healed-a1-g2`. Only the app image, banking environment,
   service network and minimal privileges are provided (never the Docker socket).
7. The replacement registers `A1` at generation 2. A stale generation-1
   process cannot overwrite the generation-2 registry row. Gateway also checks
   the generation in the replica's health response.
8. Once healthy, the controller persists `HEALTHY`, new `container_id`,
   hostname and heartbeat. Gateway discovers the replacement from Metadata.
9. If the controller dies **after the generation is reserved** or **after
   Docker create**, on restarting it inspects labels and adopts the existing
   container. It MUST NOT create a second copy of that incarnation.
10. If the replacement actually exits or stays unhealthy beyond startup grace,
    the controller can advance to a new generation rather than using an
    automatic restart policy.

## Security restriction

`/var/run/docker.sock` gives a process near-root access to the Docker host.
It is mounted **only into `self-healing`** for a disposable, local Docker
Desktop demonstration. Keep gateway bound to 127.0.0.1, do not publish the
controller port, never use this Compose file on a shared or Internet-facing
server, and do not run untrusted code in the controller image. Real-world use
should place the controller on a tightly controlled host or use a restricted
Docker Engine API proxy, while preserving *custom* reconciliation logic.

## Install / run (PowerShell, from project root)

1. Back up database volumes if desired. Preserve `.env` and `.git`.
2. **Never use `docker compose down -v`.**
3. `docker compose config --quiet`
4. `docker compose up --build -d`
5. `docker compose ps`
6. `docker compose logs --tail 100 self-healing`
7. `docker compose run --rm schema-migrator python -m scripts.verify_stage08 --scenario=preflight`

## Replica replacement test

**PowerShell window A:**

```powershell
# Observe a real creation by the custom controller:
docker compose logs -f self-healing
```

**PowerShell window B:**

```powershell
docker compose stop shard-a-1
# Give the controller time to detect and create a new container.
docker compose run --rm schema-migrator python -m scripts.verify_stage08 --scenario=healed
# A1 will be served by a generation >= 2, not by the stopped Compose container.
docker ps --filter "label=banking.managed=true" --format "table {{.Names}}\t{{.Status}}"
```

### Banking load while healing

Open **PowerShell A** and run:

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage08 --scenario=load --duration=60
```

Within those 60 seconds, in **PowerShell B** run `docker compose stop shard-b-1`
(or if A1 has not yet been healed, stop `shard-a-1` instead). The load checks
reads and offsetting `0.01` deposit/withdraw pairs on `A0500`. To exercise the
Shard A repair **during** load, run this experiment on a fresh Stage 08 install
before the earlier A1 demonstration. The script retries uncertain write
outcomes using the **same** request ID.

Afterward, run `--scenario=healed` when the failed replica is A1, then
`--scenario=preflight` to verify both healthy slots per shard. For B1 recovery,
inspect `replica_registry` via the SQL below instead.

**Important:** no unrelated writes to A0500 during the load demo. If the load
script is interrupted between a deposit and its compensating withdrawal,
inspect the local request status before any manual compensation.

## Direct evidence from Metadata

```powershell
docker compose exec -T metadata-db psql -U banking -d metadata -c "SELECT replica_id,hostname,generation,lifecycle,container_id FROM replica_registry ORDER BY shard_id,slot;"
docker compose exec -T metadata-db psql -U banking -d metadata -c "SELECT shard_id,slot,generation,action,created_at FROM replica_healing_events ORDER BY event_id DESC LIMIT 15;"
```

## Controller crash during replacement (Stage 09)

For Stage 09 we will inject controller crashes in the generation-reserved and
container-created windows and assert no duplicate replicas are created. Stage
08 establishes the durable state, deterministic names, leader lock and adoption
algorithm, but **does not yet prove the crash tests**.

## Limits and safe operation

* Docker Socket compromise is host compromise; development use only.
* Loss of shard DB cannot be solved by replacing app replicas.
* A Compose-original container stopped during a healing demo should **not** be
  manually restarted: it belongs to an older generation. Stage 09 adds a safe
  lifecycle/reset workflow for repeatedly redeploying the original Compose
  slots. Keep the generated replacement running in the meantime.
* Do not run `docker compose down` and expect it to delete dynamically created
  containers: they were created directly by the controller, not Compose.
* The demonstration tests the healthy slot count, container creation and
  banking correctness but does not yet prove controller crash recovery.
