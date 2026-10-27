# Stage 12 — Atomic shard-map cutover and write fencing

## Goal and scope

Move ownership of `A1501–A2000` from B to C using a persistent monotonically
increasing routing epoch and **a fail-closed protocol**. Shard B remains the
owner for `A1001–A1500`; Shard A is unchanged. Only the moved accounts that
actually exist in B are copied into C. Normal banking operations continue on
unaffected accounts. Migration-range writes may briefly receive HTTP `409` or
`503` during final cutover; clients must retry safely and query uncertain
outcomes. Do not delete old B account rows during this stage.

**Do not run the cutover before Stage 11 bootstrap and online catch-up have
passed. Do not run Stage 11's preflight/online scripts *after* cutover.**

## Crash-safe ordering

The protocol spans three independent PostgreSQL databases. There is no
cross-database ACID transaction for B, C and Metadata; instead the steps are
ordered so two writable owners never coexist:

1. **FENCING:** In Metadata, lock `routing_epoch FOR UPDATE` (this conflicts
   with the Coordinator's `FOR SHARE` reservation lock), persist phase
   `FENCING` and allocate `epoch_after = epoch_before + 1`. The Coordinator
   blocks *new* 2PC transactions touching the migration range until map
   activation. Existing transaction IDs remain queryable and recoverable.
2. **B write fence:** Lock the original B fence row `1001–2000 FOR UPDATE`.
   The bank's local mutations and 2PC PREPAREs hold a `FOR SHARE` lock here,
   including prepared transactions. If a transaction still holds it, this
   step waits and times out safely; the manager never forcibly resolves 2PC.
   Split B into a writable `1001–1500` row and a **non-writable** `1501–2000`
   row in one B transaction.
3. **FINAL_REPLAY:** Wait for unresolved 2PC transactions *touching the moving
   range* to complete, then replay the final journal tail from B to C and
   compare each moved account's exact `(balance, version)` pair. If anything
   is uncertain, keep the migration paused with B fenced and C non-writable.
4. **C staging:** Copy verified target staging rows into C's live `accounts`
   table and install a `TARGET_READY` C fence at the new epoch with
   `writable=false`. This is done in one C transaction and is idempotent.
5. **MAP_ACTIVATED:** In **one Metadata transaction**, atomically shrink B's
   map range to `1001–1500`, insert C's active `1501–2000` map range with the
   incremented epoch, update `routing_epoch`, and persist the map decision.
   At this instant B is already fenced and C remains non-writable.
6. **VERIFYING:** Activate C's fence at the new epoch and mark the job
   `VERIFYING`. After activation, Gateway routes moved accounts to C and
   direct requests with B's old epoch are rejected by the bank itself.

Stage 12 leaves the job in `VERIFYING` so stages 13–14 can run retries,
2PC/migration interleaving checks, crash injection, and final cleanup before
marking the migration `COMPLETED`.

## Restart and failure behavior

The Migration Manager polls for jobs in `FENCING`, `FINAL_REPLAY`, or
`MAP_ACTIVATED` and replays each remaining step. It uses a session-level
advisory lock to avoid concurrent managers; each database step commits
atomically and can be retried. Never change a durable COMMIT decision into
ABORT to accelerate migration. A pending prepared transaction is a legitimate
reason to postpone cutover.

`MAP_ACTIVATED` with `C` not writable is **safe but temporarily unavailable**;
the manager must recover and enable C before the move succeeds. After
`VERIFYING`, C is the sole write owner. PostgreSQL server or metadata database
unavailability also makes the process fail closed, not dual-writable.

## Exact Windows / PowerShell deployment

1. Preserve `.env` and `.git`; copy the project files, without deleting named
   PostgreSQL volumes. In the project root, run:

   ```powershell
   docker compose config --quiet
   docker compose build
   docker compose run --rm schema-migrator python scripts/apply_migrations.py
   docker compose up -d --no-deps --force-recreate coordinator migration-manager
   ```

2. Verify Stage 11 results were successful, then perform Stage 12 preflight:

   ```powershell
   docker compose run --rm schema-migrator python -m scripts.verify_stage12 --scenario=preflight
   ```

3. **This step is one-way.** Ensure no unrelated exercise is running and take
   a database backup if you need to repeat the demo from the beginning.

   ```powershell
   docker compose run --rm schema-migrator python -m scripts.verify_stage12 --scenario=cutover
   ```

4. Verify stale routes are rejected at the B banking replica (not just at
   Gateway), and that two valid writes now go to C:

   ```powershell
   docker compose run --rm schema-migrator python -m scripts.verify_stage12 --scenario=stale
   ```

5. Restart **only** the manager, verify persisted ownership without a reset:

   ```powershell
   docker compose up -d --no-deps --force-recreate migration-manager
   docker compose run --rm schema-migrator python -m scripts.verify_stage12 --scenario=recovered
   ```

6. Do not run `docker compose down -v`. Do not restart old, replaced replicas
   from the self-healing experiments. Do not edit the epoch manually.

## Observable success evidence

- `shard_map`: `[1,1000]→A`, `[1001,1500]→B`, `[1501,2000]→C`.
- Global routing epoch strictly increased (`1→2` in the initial run).
- B's local fence for `1501–2000` is non-writable; C's fence is writable with
  the **new** epoch.
- `A1500` still routes to B, `A2000` now routes to C.
- A forged direct POST to B with the stale epoch produces HTTP `409` and
  does **not** change either balance or account version.
- Repeating the cutover/resume operation does not create duplicate maps or
  advance the epoch a second time.

## Deliberate limits / Stage 13 dependencies

- In-flight 2PC prepared transactions need to resolve before ownership moves;
  the manager will refuse to proceed while unresolved. Stage 13 exercises
  overlapping concurrent transfers and guarantees safe retry semantics
  across ownership changes, including preservation of pre-cutover request
  identities.
- Historical local request de-duplication tables and cross-boundary
  same-shard transfers require explicit treatment for *exactly-once* retries
  through ownership change; they are not silently declared solved here.
- Stage 14 will validate crashes at each migration phase and persistent
  convergence. Deleting old B data is forbidden until final verification.

For GitHub, commit and tag `stage-12` only after actual Docker tests pass.
