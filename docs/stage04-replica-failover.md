# Stage 04 — Replica health, load distribution and failover

This is the fourth stage of the full (including bonus) Distributed Banking Platform.

## Guarantees delivered

- Gateway loads replica locations from PostgreSQL replica_registry (no hardcoded host list).
- Each route probes candidate replica health and verifies claimed shard and replica identity.
- Round Robin chooses among healthy replicas **of the owning shard only**.
- For the read-only balance endpoint, if a healthy replica stops between probe
  and request, Gateway can retry the alternate healthy replica of the SAME shard.
- Logs show the replica that served each request.
- `GET /debug/replicas/A` and `/debug/replicas/B` show current healthy candidates.
- No changes to persistent SQL volumes, no migration checksum changes.
- Build sets PYTHONPATH=/app to fix direct invocation of `scripts/verify_stage03.py`.

## Not yet implemented

This stage does **not** provision new containers; automated self-healing is a
separate later stage. It does not implement money-changing endpoints or 2PC.
Automatic retry is intentionally read-only. Migration/epoch fencing for writes
will be implemented later. Both bonuses remain mandatory project deliverables.

## Deploy from Stage 03 (PowerShell on Windows)

Keep the old `.env` and `.git` directory; replace project source files but do not
change/remove Docker volumes. Never use `docker compose down -v`.

```powershell
docker compose config --quiet
docker compose up --build -d
docker compose ps
```

Verify regression tests and healthy two-replica distribution:

```powershell
docker compose run --rm schema-migrator python -m scripts.verify_stage03
docker compose run --rm schema-migrator python -m scripts.verify_stage04 --scenario=normal
```

Simulate outage of **one** replica of Shard A:

```powershell
docker compose stop shard-a-1
docker compose run --rm schema-migrator python -m scripts.verify_stage04 --scenario=a1-down
docker compose logs --tail=50 gateway
```

Only A2 should serve requests to A0500. Shard B should continue normal
load-balancing between B1 and B2.

Restore the stopped replica (manual in Stage 04):

```powershell
docker compose start shard-a-1
docker compose run --rm schema-migrator python -m scripts.verify_stage04 --scenario=normal
```

If a test fails, send: `docker compose ps -a`, `docker compose logs --tail=80 gateway`,
and the full output of the failing test. Do not reset the DB volumes.

## Git

After all successful tests:

```powershell
git add .
git commit -m "feat: stage 04 load balancing and replica failover"
git push origin main
git tag stage-04
git push origin stage-04
```
