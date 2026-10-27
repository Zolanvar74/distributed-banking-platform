"""Stage 09 controller crash/reconciliation integration proof (Docker run container).

Phases: preflight -> arm -> human stops target Docker container -> crashed ->
        human starts controller -> recovered. The test container has NO Docker
        socket. Container-level evidence comes from an internal authenticated
        inspection endpoint hosted ONLY in the controller container.
"""
from __future__ import annotations

import argparse
import os
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal

import httpx
import psycopg
from psycopg.rows import dict_row

from shared.healing_contract import Slot, check_container_evidence

META = os.environ["METADATA_DATABASE_URL"]
TOKEN = os.environ["INTERNAL_2PC_TOKEN"]
CONTROLLER = "http://self-healing:8000"
GATEWAY = "http://gateway:8000"
POINTS = ("AFTER_RESERVE", "AFTER_CREATE", "AFTER_START", "AFTER_HEALTHY")


@dataclass
class Trial:
    experiment_id: uuid.UUID
    replica_id: str
    shard: str
    slot: int
    point: str
    before: int
    after: int
    old_id: str
    state: str


def get_trial(db, experiment_id: uuid.UUID) -> Trial:
    row = db.execute("SELECT * FROM healing_crash_experiments WHERE experiment_id=%s",
                     (experiment_id,)).fetchone()
    assert row, f"experiment not found: {experiment_id}"
    return Trial(row["experiment_id"], row["replica_id"], row["shard_id"],
                 row["slot"], row["point"], row["previous_generation"],
                 row["expected_generation"], row["previous_container_id"], row["state"])


def get_slot(db, replica_id: str) -> dict:
    row = db.execute("""SELECT replica_id,shard_id,slot,hostname,container_id,
                       generation,lifecycle FROM replica_registry
                       WHERE replica_id=%s""", (replica_id,)).fetchone()
    assert row, f"unknown logical replica {replica_id}"
    return row


def get_status(client: httpx.Client):
    response = client.get(f"{CONTROLLER}/health")
    response.raise_for_status()
    return response.json()


def docker_evidence(client: httpx.Client, shard: str, slot: int) -> list[dict]:
    r = client.get(f"{CONTROLLER}/internal/slots/{shard}/{slot}/containers",
                   headers={"X-Internal-Token": TOKEN})
    r.raise_for_status()
    return r.json()["containers"]


def discovery(client: httpx.Client, shard: str) -> dict:
    r = client.get(f"{GATEWAY}/debug/replicas/{shard}")
    r.raise_for_status()
    return r.json()


def check_balances(client: httpx.Client) -> None:
    for acc, shard in (("A0500", "A"), ("A1500", "B")):
        r = client.get(f"{GATEWAY}/accounts/{acc}/balance")
        assert r.status_code == 200, (acc, r.status_code, r.text[:200])
        body = r.json()
        assert body["shard"] == shard and Decimal(body["balance"]) >= 0


def preflight(timeout: int = 80) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=8, trust_env=False) as client, \
                 psycopg.connect(META, row_factory=dict_row) as db:
                health = get_status(client)
                assert health["status"] == "ok", health
                for shard in ("A", "B"):
                    current = discovery(client, shard)
                    assert set(current["healthy"]) == {shard+"1", shard+"2"}, current
                    for number in (1, 2):
                        replica = get_slot(db, shard+str(number))
                        assert replica["lifecycle"] == "HEALTHY" and replica["container_id"], replica
                        containers = docker_evidence(client, shard, number)
                        check_container_evidence(containers, replica["generation"], replica["container_id"])
                check_balances(client)
            print("PASS controller healthy; A/B each have exactly two live logical replicas", flush=True)
            print("PASS Docker evidence: one running incarnation per slot, no stale running replica", flush=True)
            print("STAGE 09 PREFLIGHT: ALL CHECKS PASSED", flush=True)
            return
        except (AssertionError, httpx.RequestError, httpx.HTTPStatusError, psycopg.Error) as exc:
            last = str(exc)[:500]
            time.sleep(2)
    raise AssertionError(f"stage09 preflight not ready within {timeout}s: {last}")


def arm(experiment_id: uuid.UUID, replica_id: str, point: str) -> None:
    assert point in POINTS
    assert len(replica_id) == 2 and replica_id[0] in "AB" and replica_id[1] in "12"
    slot = Slot(replica_id[0], int(replica_id[1]))
    with httpx.Client(timeout=8, trust_env=False) as client, \
         psycopg.connect(META, row_factory=dict_row) as db:
        health = get_status(client)
        assert health.get("faults_enabled") is True, (
            "controller crash injection is OFF: set $env:STAGE09_ENABLE_FAULTS='1' "
            "then docker compose up -d --no-deps --force-recreate self-healing", health)
        row = get_slot(db, replica_id)
        assert row["lifecycle"] == "HEALTHY" and row["container_id"], row
        assert replica_id in discovery(client, slot.shard)["healthy"]
        containers = docker_evidence(client, slot.shard, slot.number)
        check_container_evidence(containers, row["generation"], row["container_id"])
        old = next(c for c in containers if c["id"] == row["container_id"])
        with db.transaction():
            db.execute("""INSERT INTO healing_crash_experiments
                (experiment_id,replica_id,shard_id,slot,point,
                 previous_generation,expected_generation,previous_container_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (experiment_id, replica_id, slot.shard, slot.number, point,
                 row["generation"], row["generation"]+1, row["container_id"]))
    print(f"ARMED experiment={experiment_id} replica={replica_id} point={point}", flush=True)
    print(f"Current running container: {old['name']} ({old['id'][:12]})", flush=True)
    print("STOP ONLY THE CONTAINER LISTED ABOVE using Docker CLI from PowerShell.", flush=True)
    print(f"Expected new generation: {row['generation']+1}", flush=True)
    print("STAGE 09 CRASH FAULT: ARMED", flush=True)


def crashed(experiment_id: uuid.UUID, timeout: int) -> None:
    deadline = time.monotonic()+timeout
    state = None
    while time.monotonic() < deadline:
        with psycopg.connect(META, row_factory=dict_row) as db:
            t = get_trial(db, experiment_id)
            row = get_slot(db, t.replica_id)
            state = (t, row)
        if t.state == "FIRED":
            break
        time.sleep(1)
    assert state is not None
    t, row = state
    assert t.state == "FIRED", ("crash point did not fire", t, row)
    assert row["generation"] == t.after, ("generation changed unexpectedly", t, row)
    if t.point == "AFTER_HEALTHY":
        assert row["lifecycle"] == "HEALTHY" and row["container_id"]
    else:
        assert row["lifecycle"] in {"REPLACING", "REGISTERED", "HEALTHY"}
    with httpx.Client(timeout=3, trust_env=False) as client:
        try:
            get_status(client)
        except (httpx.RequestError, httpx.HTTPStatusError):
            pass
        else:
            raise AssertionError("Controller still responds, crash not proven")
        current = discovery(client, t.shard)
        other = t.shard + ("2" if t.slot == 1 else "1")
        assert other in current["healthy"], current
        check_balances(client)
    print(f"PASS durable crash recorded at {t.point}, generation={t.after}", flush=True)
    print(f"PASS controller unreachable; Gateway still serves via {other}", flush=True)
    print("STAGE 09 CONTROLLER CRASH: VERIFIED; SAFE TO RESTART", flush=True)


def recovered(experiment_id: uuid.UUID, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    last = None
    with httpx.Client(timeout=8, trust_env=False) as client:
        while time.monotonic() < deadline:
            try:
                with psycopg.connect(META, row_factory=dict_row) as db:
                    t = get_trial(db, experiment_id)
                    row = get_slot(db, t.replica_id)
                if t.state not in {"FIRED", "VERIFIED"}:
                    raise AssertionError("experiment must have actually crashed")
                assert row["generation"] == t.after, ("generation drift", t, row)
                if row["lifecycle"] != "HEALTHY" or not row["container_id"]:
                    last = row
                    time.sleep(2)
                    continue
                health = get_status(client)
                assert health["status"] == "ok"
                containers = docker_evidence(client, t.shard, t.slot)
                check_container_evidence(containers, t.after, row["container_id"])
                # old generation has no running copies, and the incarnation
                # is stable across another full controller cycle.
                time.sleep(5)
                containers_again = docker_evidence(client, t.shard, t.slot)
                check_container_evidence(containers_again, t.after, row["container_id"])
                for shard in ("A", "B"):
                    state = discovery(client, shard)
                    assert set(state["healthy"]) == {shard+"1", shard+"2"}, state
                check_balances(client)
                with psycopg.connect(META) as db:
                    db.execute("""UPDATE healing_crash_experiments
                                  SET state='VERIFIED',verified_at=clock_timestamp()
                                  WHERE experiment_id=%s AND state='FIRED'""", (experiment_id,))
                print(f"PASS persisted gen={t.after} retained, container={row['container_id'][:12]}")
                print("PASS no duplicate current incarnation or running stale container")
                print("PASS two healthy replicas per shard and banking endpoints work")
                print("STAGE 09 CONTROLLER RECOVERY: ALL CHECKS PASSED", flush=True)
                return
            except (httpx.RequestError, httpx.HTTPStatusError, psycopg.OperationalError) as exc:
                last = repr(exc)
            time.sleep(2)
    raise AssertionError(f"controller did not reconcile in time: {last}")


def show(experiment_id: uuid.UUID):
    with psycopg.connect(META, row_factory=dict_row) as db:
        t = get_trial(db, experiment_id)
        row = get_slot(db, t.replica_id)
        events = db.execute("""SELECT generation,action,container_id,created_at
                       FROM replica_healing_events WHERE shard_id=%s AND slot=%s
                       AND generation IN (%s,%s) ORDER BY event_id DESC LIMIT 12""",
                       (t.shard, t.slot, t.before, t.after)).fetchall()
    print("Experiment:", t)
    print("Registry:", row)
    for e in events: print("Event:", e)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True,
                        choices=("preflight", "arm", "crashed", "recovered", "show"))
    parser.add_argument("--trial-id", type=uuid.UUID)
    parser.add_argument("--replica", default="B2")
    parser.add_argument("--point", choices=POINTS, default="AFTER_RESERVE")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    if args.scenario == "preflight":
        preflight(args.timeout)
        return
    if args.trial_id is None:
        parser.error("--trial-id is required for this scenario")
    if args.scenario == "arm": arm(args.trial_id, args.replica, args.point)
    elif args.scenario == "crashed": crashed(args.trial_id, args.timeout)
    elif args.scenario == "recovered": recovered(args.trial_id, args.timeout)
    elif args.scenario == "show": show(args.trial_id)


if __name__ == "__main__": main()
