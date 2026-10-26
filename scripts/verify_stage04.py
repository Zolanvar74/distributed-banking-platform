"""Stage 04 live proof, run from Compose network.

When all 4 replicas are running:
    docker compose run --rm schema-migrator python -m scripts.verify_stage04 --scenario=normal
When shard-a-1 is STOPPED (no automatic restart):
    docker compose run --rm schema-migrator python -m scripts.verify_stage04 --scenario=a1-down
After restarting shard-a-1:
    docker compose run --rm schema-migrator python -m scripts.verify_stage04 --scenario=normal
"""
from __future__ import annotations

import argparse
from collections import Counter

import httpx

BASE = "http://gateway:8000"


def run(scenario: str) -> None:
    expected_a = {"A1", "A2"} if scenario == "normal" else {"A2"}
    with httpx.Client(timeout=7, trust_env=False) as client:
        for shard, expected in (("A", expected_a), ("B", {"B1", "B2"})):
            resp = client.get(f"{BASE}/debug/replicas/{shard}")
            assert resp.status_code == 200, (resp.status_code, resp.text)
            report = resp.json()
            assert set(report["healthy"]) == expected, report
            assert report["healthy_count"] == len(expected), report
            if shard == "A" and scenario == "a1-down":
                assert "A1" in report["unhealthy"], report
            print(f"PASS {shard} health discovery: {report}", flush=True)
        for shard, account, expected in (("A", "A0500", expected_a),
                                         ("B", "A1500", {"B1", "B2"})):
            served = Counter()
            for _ in range(24):
                response = client.get(f"{BASE}/accounts/{account}/balance")
                assert response.status_code == 200, (response.status_code, response.text)
                payload = response.json()
                assert payload["shard"] == shard and payload["account_id"] == account, payload
                served[payload["served_by"]] += 1
            assert set(served) == expected, (shard, served)
            print(f"PASS {shard} request distribution: {dict(served)}", flush=True)
        response = client.get(f"{BASE}/accounts/A2001/balance")
        assert response.status_code == 404, (response.status_code, response.text)
        print("PASS invalid ownership requests rejected", flush=True)
    print("STAGE 04 FAILOVER CHECK: ALL CHECKS PASSED", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["normal", "a1-down"], default="normal")
    args = parser.parse_args()
    run(args.scenario)
