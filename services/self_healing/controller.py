"""Custom self-healing reconciler, Docker Engine + PostgreSQL only.

Invariants:
* One stable logical replica slot per shard. Replacements have increasing generations.
* A Postgres advisory leader lock serializes reconcilers, including after crashes.
* Reserve generation in durable registry BEFORE Docker create.
* Deterministic container names + labels let a restarted controller adopt containers.
* Old container is stopped before a new generation is exposed to Gateway.
* No Docker restart policy, Kubernetes, Swarm, or external healing controller.
"""
from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import httpx
import psycopg
from psycopg.rows import dict_row

from shared.healing_contract import (PROJECT, Slot, labels_for, labels_match,
                                     replacement_name, should_replace)

log = logging.getLogger("banking.self_healing")
META = os.environ.get("METADATA_DATABASE_URL", "")
INTERVAL = float(os.environ.get("HEALING_POLL_SECONDS", "2"))
THRESHOLD = int(os.environ.get("HEALING_FAILURE_THRESHOLD", "2"))
START_GRACE = int(os.environ.get("HEALING_START_GRACE_SECONDS", "40"))
NETWORK = os.environ.get("HEALING_DOCKER_NETWORK", "distributed-banking_bank-net")
IMAGE = os.environ.get("HEALING_REPLICA_IMAGE", "distributed-banking-app:stage09")
FAULTS_ENABLED = os.environ.get("STAGE09_ENABLE_FAULTS", "0") == "1"
# Process-local liveness information exposed by /health. A running thread is
# insufficient: an exception/reconnect loop must not advertise healthy status.
LAST_SUCCESSFUL_CYCLE = 0.0


def controller_cycle_healthy(max_age_seconds: float = 30.0) -> bool:
    return LAST_SUCCESSFUL_CYCLE > 0 and time.monotonic() - LAST_SUCCESSFUL_CYCLE < max_age_seconds



def controlled_crash(db, slot: Slot, generation: int, point: str) -> None:
    """One-shot, durable development-only crash; never an automatic restart.

    The row is marked FIRED and committed by the autocommit connection BEFORE
    exiting the whole controller process. A restarted process cannot refire it.
    """
    if not FAULTS_ENABLED:
        return
    if point not in {"AFTER_RESERVE", "AFTER_CREATE", "AFTER_START", "AFTER_HEALTHY"}:
        raise ValueError("invalid controller crash point")
    hit = db.execute("""UPDATE healing_crash_experiments
                SET state='FIRED', fired_at=clock_timestamp()
                WHERE experiment_id = (
                  SELECT experiment_id FROM healing_crash_experiments
                  WHERE shard_id=%s AND slot=%s AND expected_generation=%s
                    AND point=%s AND state='ARMED'
                  ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED
                ) RETURNING experiment_id""",
                (slot.shard, slot.number, generation, point)).fetchone()
    if hit:
        log.critical("CONTROLLED CRASH point=%s slot=%s gen=%s experiment=%s",
                     point, slot.replica_id, generation, hit["experiment_id"])
        logging.shutdown()
        os._exit(93)  # deliberate process-level fault, not a caught exception


def start_candidate(db, slot: Slot, generation: int, container) -> None:
    container.start()
    audit(db, slot, generation, "CONTAINER_STARTED", container.id)
    controlled_crash(db, slot, generation, "AFTER_START")


def audit(db, slot: Slot, generation: int, action: str,
          container_id: str | None = None, details: str | None = None) -> None:
    db.execute("""INSERT INTO replica_healing_events
               (shard_id,slot,generation,action,container_id,details)
               VALUES (%s,%s,%s,%s,%s,%s)""",
               (slot.shard, slot.number, generation, action, container_id, details))


def registry(db, slot: Slot) -> dict:
    row = db.execute("""SELECT replica_id,shard_id,slot,hostname,generation,lifecycle,
                       healing_since,container_id FROM replica_registry
                       WHERE shard_id=%s AND slot=%s""",
                     (slot.shard, slot.number)).fetchone()
    if row is None:
        raise RuntimeError(f"replica slot missing from metadata: {slot.replica_id}")
    return row


def target_slots(db) -> list[Slot]:
    rows = db.execute("SELECT shard_id,desired_count FROM desired_replica_count ORDER BY shard_id").fetchall()
    return [Slot(row["shard_id"], i) for row in rows for i in range(1, row["desired_count"] + 1)]


def containers_for(docker_client, slot: Slot):
    candidates = docker_client.containers.list(all=True, filters={"label": [
        f"banking.project={PROJECT}", "banking.role=replica",
        f"banking.shard={slot.shard}", f"banking.slot={slot.number}",
    ]})
    return [c for c in candidates if labels_match(c, slot)]


def db_available(db, slot: Slot) -> bool:
    # Don't destroy healthy application topology because a shared shard DB is down.
    # The controller's own connection is a distinct metadata database.
    url = os.environ.get(f"SHARD_{slot.shard}_DATABASE_URL")
    if not url:
        return False
    try:
        with psycopg.connect(url, connect_timeout=2) as check:
            check.execute("SELECT 1")
        return True
    except psycopg.Error:
        return False


def healthy(container, slot: Slot, gen: int) -> bool:
    if container.status != "running":
        return False
    try:
        r = httpx.get(f"http://{container.name}:8000/health", timeout=1.5, trust_env=False)
        if r.status_code != 200:
            return False
        body = r.json()
        return (body.get("status") == "ok" and body.get("shard") == slot.shard
                and body.get("replica") == slot.replica_id
                and body.get("generation") == gen)
    except (httpx.RequestError, ValueError, TypeError):
        return False


def set_healthy(db, slot: Slot, gen: int, container) -> None:
    old = registry(db, slot)
    # Guard against accidental stale controller updates.
    if old["generation"] != gen:
        return
    db.execute("""UPDATE replica_registry SET lifecycle='HEALTHY',
               hostname=%s, container_id=%s, port=8000,
               last_heartbeat=clock_timestamp(), last_reconciled_at=clock_timestamp(),
               healing_since=NULL WHERE replica_id=%s AND generation=%s""",
               (container.name, container.id, slot.replica_id, gen))
    if old["lifecycle"] != "HEALTHY" or old["container_id"] != container.id:
        audit(db, slot, gen, "REPLICA_HEALTHY", container.id)
        controlled_crash(db, slot, gen, "AFTER_HEALTHY")
        log.info("healthy slot=%s generation=%s container=%s", slot.replica_id, gen, container.name)


def reserve_generation(db, slot: Slot, gen: int) -> None:
    new_host = replacement_name(slot, gen)
    row = db.execute("""UPDATE replica_registry SET generation=%s, hostname=%s,
            container_id=NULL, lifecycle='REPLACING',
            healing_since=clock_timestamp(),last_reconciled_at=clock_timestamp()
            WHERE replica_id=%s AND generation < %s RETURNING replica_id""",
            (gen, new_host, slot.replica_id, gen)).fetchone()
    if row is None:
        raise RuntimeError("generation reservation race")
    audit(db, slot, gen, "REPLACEMENT_PLANNED")
    log.warning("replacement planned slot=%s generation=%s", slot.replica_id, gen)


def _replacement_env(slot: Slot, gen: int, name: str) -> dict[str, str]:
    return {
        "SHARD_ID": slot.shard, "REPLICA_ID": slot.replica_id,
        "REPLICA_HOST": name, "REPLICA_GENERATION": str(gen),
        "SHARD_DATABASE_URL": os.environ[f"SHARD_{slot.shard}_DATABASE_URL"],
        "METADATA_DATABASE_URL": META,
        "INTERNAL_2PC_TOKEN": os.environ["INTERNAL_2PC_TOKEN"],
        "PYTHONPATH": "/app", "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
    }


def create_or_adopt(docker_client, db, slot: Slot, gen: int, known):
    name = replacement_name(slot, gen)
    candidate = next((c for c in known if c.name == name and labels_match(c, slot, gen)), None)
    if candidate is None:
        try:
            candidate = docker_client.containers.get(name)
        except Exception as exc:
            # Only absence is an excuse to create; other Docker failures must surface.
            if exc.__class__.__name__ != "NotFound":
                raise
        if candidate is not None and not labels_match(candidate, slot, gen):
            raise RuntimeError(f"container name collision: {name}")
    if candidate is None:
        try:
            candidate = docker_client.containers.create(
                IMAGE, name=name, hostname=name,
                command=["uvicorn", "services.banking.main:app", "--host", "0.0.0.0", "--port", "8000"],
                environment=_replacement_env(slot, gen, name),
                labels=labels_for(slot, gen, managed=True),
                network=NETWORK, detach=True,
                restart_policy={"Name": "no"},
                cap_drop=["ALL"], security_opt=["no-new-privileges:true"],
                mem_limit="256m", pids_limit=128,
            )
        except Exception as exc:
            # Another attempt may have created the same name before an interrupted response.
            try:
                candidate = docker_client.containers.get(name)
            except Exception:
                raise exc
            if not labels_match(candidate, slot, gen):
                raise RuntimeError(f"container name collision: {name}") from exc
        audit(db, slot, gen, "CONTAINER_CREATED", candidate.id)
        controlled_crash(db, slot, gen, "AFTER_CREATE")
        log.warning("created replacement slot=%s container=%s", slot.replica_id, name)
    candidate.reload()
    if candidate.status == "created":
        start_candidate(db, slot, gen, candidate)
    elif candidate.status not in {"running", "restarting"}:
        # A failed replacement is never silently restarted. Give it a NEW generation.
        raise RuntimeError(f"replacement {name} already exited: {candidate.status}")
    return candidate


def stop_old(db, slot: Slot, generations, new_gen: int) -> None:
    for container in generations:
        if not labels_match(container, slot):
            continue
        old_gen = int(container.labels["banking.generation"])
        if old_gen >= new_gen:
            continue
        container.reload()
        if container.status == "running":
            container.stop(timeout=3)
            container.reload()
            if container.status == "running":
                container.kill()
                container.reload()
            if container.status == "running":
                raise RuntimeError("unsafe to replace while previous replica still running")
            audit(db, slot, old_gen, "CONTAINER_STOPPED", container.id)
        # Compose-owned originals are retained (stopped) for evidence. Never
        # delete or automatically restart them. Managed stale copies may remain
        # exited; labels + database generation prevent them from being selected.


class Reconciler:
    def __init__(self, docker_client):
        self.docker = docker_client
        self.failures = defaultdict(int)

    def reconcile_one(self, db, slot: Slot) -> None:
        if not db_available(db, slot):
            log.warning("database unavailable; refusing replacement shard=%s", slot.shard)
            return
        row = registry(db, slot)
        gen = row["generation"]
        candidates = containers_for(self.docker, slot)
        exact = [c for c in candidates if labels_match(c, slot, gen)]
        current = exact[0] if exact else None
        if current is not None:
            current.reload()
            if (row["lifecycle"] == "REPLACING" and current.status == "created"):
                stop_old(db, slot, candidates, gen)
                start_candidate(db, slot, gen, current)
                return
        is_healthy = current is not None and healthy(current, slot, gen)
        if is_healthy:
            # Reconcile stale running generations even after a Compose redeploy.
            # The durable generation, not a container's self-claim, is authority.
            stop_old(db, slot, candidates, gen)
            self.failures[slot] = 0
            set_healthy(db, slot, gen, current)
            return
        self.failures[slot] += 1
        since = row["healing_since"]
        grace = (since is not None and
                 (datetime.now(timezone.utc) - since).total_seconds() < START_GRACE and
                 current is not None and current.status in {"running", "created", "restarting"})
        action = should_replace(gen, row["lifecycle"], current is not None,
                                False, self.failures[slot], THRESHOLD, grace)
        if action == "WAIT":
            return
        if action == "CREATE_NEXT":
            new_gen = gen + 1
            stop_old(db, slot, candidates, new_gen)
            reserve_generation(db, slot, new_gen)
            controlled_crash(db, slot, new_gen, "AFTER_RESERVE")
        else:
            new_gen = gen
            stop_old(db, slot, candidates, new_gen)
        # Deterministic name: adopt same generation after controller crashes.
        create_or_adopt(self.docker, db, slot, new_gen, candidates)
        self.failures[slot] = 0

    def cycle(self, db) -> None:
        slots = target_slots(db)
        for slot in slots:
            try:
                self.reconcile_one(db, slot)
            except Exception as exc:
                log.exception("reconciliation failed slot=%s", slot.replica_id)
                try:
                    audit(db, slot, max(registry(db, slot)["generation"], 1),
                          "RECOVERY_ERROR", details=type(exc).__name__)
                except Exception:
                    log.exception("failed to persist healing error")


def run_controller() -> None:
    # Docker Socket has host-equivalent privileges. Mount ONLY to this dedicated
    # development controller; NEVER expose this service port to the public.
    import docker

    docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock", timeout=8)
    docker_client.ping()  # fail fast if socket missing
    rec = Reconciler(docker_client)
    global LAST_SUCCESSFUL_CYCLE
    while True:
        try:
            with psycopg.connect(META, row_factory=dict_row, autocommit=True, connect_timeout=5) as db:
                locked = db.execute("SELECT pg_try_advisory_lock(hashtext('banking-healing-leader-v1')) AS acquired").fetchone()
                if not locked or not locked["acquired"]:
                    log.warning("another controller holds healing leader lock; waiting")
                    time.sleep(INTERVAL)
                    continue
                log.warning("self-healing controller acquired leader lock and reconciled state")
                # Lock automatically releases on process death/session loss.
                while True:
                    db.execute("SELECT 1")
                    rec.cycle(db)
                    LAST_SUCCESSFUL_CYCLE = time.monotonic()
                    time.sleep(INTERVAL)
        except Exception:
            log.exception("self-healing controller loop interrupted; reconnecting")
            time.sleep(max(2, INTERVAL))
