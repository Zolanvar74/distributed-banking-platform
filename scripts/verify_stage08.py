"""Stage 08 live acceptance tests through Gateway + durable Metadata.

preflight: confirm four healthy logical replica slots, controller state.
healed: AFTER stopping shard-a-1, require a NEW Docker container for slot A1.
load: run concurrent reads and exact-once paired writes for given seconds;
      manually stop shard-a-1 in another PowerShell while load is running.

No Docker socket in this test container; only the controller has that privilege.
"""
from __future__ import annotations

import argparse
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Event, Lock

import httpx
import psycopg
from psycopg.rows import dict_row

BASE = "http://gateway:8000"
META = os.environ["METADATA_DATABASE_URL"]


def snapshot(client: httpx.Client, shard: str) -> dict:
    r = client.get(f"{BASE}/debug/replicas/{shard}")
    assert r.status_code == 200, (r.status_code, r.text)
    return r.json()


def row_for(replica_id: str):
    with psycopg.connect(META, row_factory=dict_row) as db:
        return db.execute("""SELECT replica_id,hostname,generation,lifecycle,
            container_id,last_heartbeat FROM replica_registry WHERE replica_id=%s""",
            (replica_id,)).fetchone()


def controller_state() -> dict:
    # Network internal endpoint: controller is NOT exposed to host/public ports.
    with httpx.Client(timeout=5, trust_env=False) as c:
        r = c.get("http://self-healing:8000/status")
        assert r.status_code == 200, (r.status_code, r.text)
        return r.json()


def preflight() -> None:
    deadline = time.monotonic() + 40
    last = None
    while time.monotonic() < deadline:
        state = controller_state()
        desired = {r["shard_id"]: r["desired_count"] for r in state["desired"]}
        assert desired.get("A") == 2 and desired.get("B") == 2, desired
        healthy_enough = True
        with httpx.Client(timeout=8, trust_env=False) as c:
            for shard in ("A", "B"):
                r = snapshot(c, shard)
                last = (shard, r)
                if set(r["healthy"]) != {f"{shard}1", f"{shard}2"}:
                    healthy_enough = False
                for slot in (1, 2):
                    data = row_for(f"{shard}{slot}")
                    if not data or data["generation"] < 1 or data["lifecycle"] != "HEALTHY":
                        healthy_enough = False
        if healthy_enough:
            print("PASS both shards: desired=2, healthy=2, registry current", flush=True)
            print("STAGE 08 PREFLIGHT: ALL CHECKS PASSED", flush=True)
            return
        time.sleep(2)
    raise AssertionError(f"healthy replica quota not ready: {last}")


def healed(timeout: int = 100) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        data = row_for("A1")
        with httpx.Client(timeout=8, trust_env=False) as c:
            r = snapshot(c, "A")
        last = (data, r)
        if (data and data["generation"] >= 2 and data["container_id"]
                and data["hostname"] != "shard-a-1" and data["lifecycle"] == "HEALTHY"
                and set(r["healthy"]) == {"A1", "A2"}):
            # Prove the new A1 participates in actual banking requests.
            served = set()
            with httpx.Client(timeout=8, trust_env=False) as c:
                for _ in range(24):
                    resp = c.get(f"{BASE}/accounts/A0500/balance")
                    assert resp.status_code == 200, (resp.status_code, resp.text)
                    served.add(resp.json()["served_by"])
            assert served == {"A1", "A2"}, served
            print(f"PASS fresh Docker container A1: generation={data['generation']} "
                  f"hostname={data['hostname']} container_id={data['container_id'][:12]}", flush=True)
            print("PASS recovered A1 joins A2 in Gateway load balancing", flush=True)
            print("STAGE 08 SELF-HEALING CHECK: ALL CHECKS PASSED", flush=True)
            return
        time.sleep(2)
    raise AssertionError(f"replacement did not become healthy within timeout: {last}")


def balance(client: httpx.Client) -> Decimal:
    r = client.get(f"{BASE}/accounts/A0500/balance")
    assert r.status_code == 200, (r.status_code, r.text[:200])
    return Decimal(r.json()["balance"])


def ensure_write(client: httpx.Client, operation: str, request_id: str) -> None:
    """Retry an UNKNOWN outcome with the SAME ID. Never manufacture a new ID."""
    deadline = time.monotonic() + 35
    payload = {"amount": "0.01", "request_id": request_id}
    while time.monotonic() < deadline:
        try:
            response = client.post(f"{BASE}/accounts/A0500/{operation}", json=payload)
            if response.status_code == 200 and response.json().get("status") == "COMMITTED":
                return
            if response.status_code not in (502, 503, 504):
                raise AssertionError((response.status_code, response.text[:200]))
        except (httpx.RequestError, ValueError):
            pass
        try:
            status = client.get(f"{BASE}/accounts/A0500/operations/{request_id}")
            if status.status_code == 200 and status.json().get("status") == "COMMITTED":
                return
        except (httpx.RequestError, ValueError):
            pass
        time.sleep(.35)
    raise AssertionError(f"write outcome unresolved: {operation} request_id={request_id}")


def load(duration: int) -> None:
    stop = Event()
    lock = Lock()
    summary = {"reads": 0, "pairs": 0}
    errors = []
    with httpx.Client(timeout=10, trust_env=False) as c:
        original = balance(c)

    def reader():
        with httpx.Client(timeout=7, trust_env=False) as c:
            while not stop.is_set():
                try:
                    result = c.get(f"{BASE}/accounts/A0500/balance")
                    assert result.status_code == 200 and result.json().get("shard") == "A", result.text
                    with lock:
                        summary["reads"] += 1
                except Exception as exc:
                    with lock:
                        errors.append(str(exc)[:150])
                time.sleep(.08)

    def writer():
        with httpx.Client(timeout=12, trust_env=False) as c:
            while not stop.is_set():
                # Paired writes give net-zero change on successful completion.
                # A fault during either request must be retried with SAME ID.
                try:
                    ensure_write(c, "deposit", f"stage08-in-{uuid.uuid4()}")
                    ensure_write(c, "withdraw", f"stage08-out-{uuid.uuid4()}")
                    with lock:
                        summary["pairs"] += 1
                except Exception as exc:
                    with lock:
                        errors.append(str(exc)[:200])
                    stop.set()

    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = [pool.submit(reader) for _ in range(3)]
        jobs.append(pool.submit(writer))
        time.sleep(duration)
        stop.set()
        for job in jobs:
            job.result(timeout=55)
    with httpx.Client(timeout=10, trust_env=False) as c:
        final = balance(c)
        healthy_a = snapshot(c, "A")
    print("LOAD SUMMARY:", summary, "current A:", healthy_a, flush=True)
    assert not errors, f"errors under load: {errors[:5]}"
    assert summary["reads"] >= 10 and summary["pairs"] >= 2, summary
    assert final == original, ("money conservation failed or unrelated writes ran", original, final)
    print("STAGE 08 CONCURRENT BANKING LOAD: ALL CHECKS PASSED", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, choices=["preflight", "healed", "load"])
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--timeout", type=int, default=100)
    args = parser.parse_args()
    if args.scenario == "preflight": preflight()
    elif args.scenario == "healed": healed(args.timeout)
    else: load(args.duration)
