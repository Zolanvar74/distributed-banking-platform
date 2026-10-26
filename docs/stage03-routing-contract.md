# Stage 03 — Shard ownership and routing verification

## Scope

This stage **proves** the existing two-shard read path. It does **not** add banking writes, 2PC, CDC, an autonomous controller, a new Shard C, or a dynamic map mutation API. Existing Stage 02 tables and seeded account balances are preserved unchanged; no new SQL migration is needed.

## Ownership path

1. Gateway validates canonical ASCII account IDs (`A` + exactly four digits).
2. Gateway reads the **active** `shard_map` from persistent Metadata for *each* incoming account request. No account-to-shard dictionary or immutable account-range condition is hardcoded in the Gateway.
3. Gateway queries `replica_registry` for candidate replicas of only the owning shard (lifecycle `REGISTERED`/`HEALTHY`). Candidates are *not* automatically considered healthy: each must answer `/health` with exactly matching `shard` and `replica` identity.
4. A shard-local Round Robin counter selects one of the confirmed healthy replicas.
5. Gateway calls the selected replica's internal balance route, passing `X-Expected-Shard` and `X-Routing-Epoch` from Metadata.
6. The replica checks its **own database** `ownership_fences` for that account number, epoch and writability using a consistent read snapshot; it rejects wrong shard, stale route, missing claims or unowned range. Only then is the balance returned.
7. Gateway verifies the successful response's account ID, shard ID and replica ID. An inconsistent successful result produces 502 instead of incorrectly reporting another shard's balance.

The local check in step 6 is a **read guard only**. The write path must later lock the fence **in the same transaction** as account changes. A header is not authentication and this internal API is not safe to expose to untrusted networks; the Gateway is bound to `127.0.0.1` on the Docker host.

## HTTP API

- `GET /health`: Gateway liveness.
- `GET /debug/shard-map`: current global map revision + active shard ranges (read-only inspection; only for local demo).
- `GET /debug/route/A0500`: chosen shard/epoch/replica; consumes an RR selection.
- `GET /accounts/A0500/balance`: public read through Gateway; returns balance and `served_by`.
- `GET /internal/accounts/A0500/balance`: internal shard read, requires headers `X-Expected-Shard: A` and `X-Routing-Epoch: 1`.

Errors: 404 for unknown account/map entry, 409 for wrong shard or stale/fenced epoch, 428 for missing routing claims, 503 for unavailable replicas, 502 for inconsistent upstream identity.

## Tests and evidence

`docker compose run --rm schema-migrator python scripts/verify_stage03.py` connects independently to both shard databases, checks every stored account against active ownership, checks disjoint placement (no duplicates), validates local fences and then checks Gateway/Replica HTTP behavior. It tests both replica selections and route guards. It **does not** change balances or move data.

`docker compose exec -T gateway python scripts/smoke_test.py` reruns the original Stage 01 regression. `docker compose run --rm schema-migrator python scripts/verify_stage02.py` tests Stage 02 schemas.

For a report, retain terminal output and selected screenshots: the two DB account queries, `/debug/shard-map`, `/debug/route/...`, tests, and the per-Replica logs. Logs are just supporting evidence; correctness comes from database and API assertions.

## Next stages and Bonus dependencies

- Stage 04: live replica discovery/failover and load balancing tests.
- Stage 05: account writes with row/fence locks and concurrent-transfer safety.
- Stage 06–07: durable PostgreSQL 2PC decisions/participant recovery.
- Stage 08–09: self-healing, crash-safe reconciliation and exactly desired replicas.
- Stage 10–14: commit-safe change capture, migration snapshot/delta replay, epoch-based fencing with atomic cutover and crash recovery.

For Live Re-Sharding, the routing map can change. The **write** fencing protocol (not implemented in this stage) must stop writes on the old owner before activating writes on the new owner. Cross-service map lookup and the shard-local read guard alone do not prove atomic migration.
