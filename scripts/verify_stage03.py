"""Stage 03 integration proof: run on schema-migrator's Docker network.

    docker compose run --rm schema-migrator python scripts/verify_stage03.py

Reads all active account rows in BOTH database shards and validates exactly one
owning shard; verifies the Gateway and shard-local read fence using real HTTP.
This remains a read-only test and does not seed/change account balances.
"""
from __future__ import annotations

import os
from collections import defaultdict
from decimal import Decimal

import httpx
import psycopg
from psycopg.rows import dict_row

from shared.routing import account_number

BASE = "http://gateway:8000"
DATABASES = {
    "A": ("SHARD_A_DATABASE_URL", "http://shard-a-1:8000"),
    "B": ("SHARD_B_DATABASE_URL", "http://shard-b-1:8000"),
}


def check_http(response: httpx.Response, code: int, label: str) -> dict:
    assert response.status_code == code, (
        f"{label}: expected {code}, got {response.status_code}: {response.text[:400]}"
    )
    return response.json()


def verify() -> None:
    with psycopg.connect(os.environ["METADATA_DATABASE_URL"], row_factory=dict_row) as conn:
        revision = conn.execute("SELECT epoch FROM routing_epoch WHERE singleton=1").fetchone()["epoch"]
        ranges = conn.execute(
            "SELECT shard_id, range_start, range_end, epoch FROM shard_map "
            "WHERE active ORDER BY range_start"
        ).fetchall()
    assert revision == 1, f"Unexpected ownership map revision: {revision}"
    assert [(r["shard_id"], r["range_start"], r["range_end"], r["epoch"]) for r in ranges] == [
        ("A", 1, 1000, 1), ("B", 1001, 2000, 1)
    ], f"Unexpected active map: {ranges}"
    print("PASS shard map: 2 disjoint ranges, no gap, map epoch 1", flush=True)

    def owner(number: int) -> dict | None:
        matches = [r for r in ranges if r["range_start"] <= number <= r["range_end"]]
        assert len(matches) <= 1, f"Multiple active owners: {number}"
        return matches[0] if matches else None

    rows_by_shard: dict[str, dict] = {}
    for shard, (db_env, _) in DATABASES.items():
        with psycopg.connect(os.environ[db_env], row_factory=dict_row) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            rows = conn.execute(
                "SELECT account_id, balance, version FROM accounts ORDER BY account_id"
            ).fetchall()
            fences = conn.execute(
                "SELECT range_start, range_end, epoch, writable, phase FROM ownership_fences"
            ).fetchall()
        assert rows, f"No test accounts in Shard {shard}"
        local = {}
        for row in rows:
            account_id = row["account_id"]
            n = account_number(account_id)
            assert n is not None, f"Invalid account identifier in {shard}: {account_id}"
            o = owner(n)
            assert o is not None and o["shard_id"] == shard, (
                f"{account_id} physically in {shard} but owner is {o}"
            )
            owned_fences = [f for f in fences if f["range_start"] <= n <= f["range_end"]]
            assert len(owned_fences) == 1, f"No unique local fence for {account_id}"
            fence = owned_fences[0]
            assert fence["epoch"] == o["epoch"] and fence["writable"] and fence["phase"] == "OWNED", (
                f"Fence disagreement on {shard}: {account_id}: {fence}"
            )
            assert row["balance"] >= Decimal("0") and row["version"] >= 0
            local[account_id] = row["balance"]
        rows_by_shard[shard] = local
        print(f"PASS database {shard}: {len(rows)} accounts owned locally, valid fences", flush=True)

    common = set(rows_by_shard["A"]) & set(rows_by_shard["B"])
    assert not common, f"Duplicate physical accounts in both DBs: {sorted(common)}"
    for shard, account in (("A", "A0500"), ("B", "A1500")):
        assert account in rows_by_shard[shard], f"Missing seeded {account} in database {shard}"
    print("PASS physical placement: no account copied into both databases", flush=True)

    with httpx.Client(timeout=6, trust_env=False) as client:
        map_response = check_http(client.get(f"{BASE}/debug/shard-map"), 200, "shard map API")
        assert map_response["routing_epoch"] == revision
        assert [(x["shard_id"], x["range_start"], x["range_end"], x["epoch"])
                for x in map_response["active_ranges"]] == [
            (r["shard_id"], r["range_start"], r["range_end"], r["epoch"])
            for r in ranges
        ]
        print("PASS Gateway: shard-map read agrees with persistent metadata", flush=True)

        selected_by_shard = defaultdict(set)
        for shard, accounts in rows_by_shard.items():
            for account_id, balance in accounts.items():
                record = check_http(
                    client.get(f"{BASE}/accounts/{account_id}/balance"), 200, account_id
                )
                assert record["account_id"] == account_id and record["shard"] == shard
                assert Decimal(record["balance"]) == balance, f"Balance mismatch: {account_id}"
                assert record["served_by"] in {f"{shard}1", f"{shard}2"}
                selected_by_shard[shard].add(record["served_by"])
                route = check_http(client.get(f"{BASE}/debug/route/{account_id}"), 200, "route")
                assert route["owner_shard"] == shard and route["shard_epoch"] == 1
            sample = sorted(accounts)[0]
            for _ in range(8):
                record = check_http(client.get(f"{BASE}/accounts/{sample}/balance"), 200, "rr")
                selected_by_shard[shard].add(record["served_by"])
            assert selected_by_shard[shard] == {f"{shard}1", f"{shard}2"}, (
                f"Missing replica selection in shard {shard}: {selected_by_shard[shard]}"
            )
            print(f"PASS Gateway {shard}: correct owner, balances, A/B independent replicas", flush=True)

        for account_id in ("A0000", "A2001", "B0500", "A99", "A１２３４"):
            check_http(client.get(f"{BASE}/accounts/{account_id}/balance"), 404, "invalid")
        # Account syntactically valid and inside mapped range, but not seeded.
        check_http(client.get(f"{BASE}/accounts/A0002/balance"), 404, "missing account")
        print("PASS invalid/unowned/unseeded account requests are rejected", flush=True)

        good_headers = {"X-Expected-Shard": "A", "X-Routing-Epoch": "1"}
        a1 = DATABASES["A"][1]
        b1 = DATABASES["B"][1]
        check_http(client.get(f"{a1}/internal/accounts/A0500/balance", headers=good_headers), 200,
                   "valid direct route")
        check_http(client.get(f"{a1}/internal/accounts/A1500/balance", headers=good_headers), 409,
                   "cross-shard routing")
        check_http(client.get(f"{b1}/internal/accounts/A0500/balance",
                              headers={"X-Expected-Shard": "B", "X-Routing-Epoch": "1"}), 409,
                   "wrong shard")
        check_http(client.get(f"{a1}/internal/accounts/A0500/balance",
                              headers={"X-Expected-Shard": "B", "X-Routing-Epoch": "1"}), 409,
                   "contradictory shard claim")
        check_http(client.get(f"{a1}/internal/accounts/A0500/balance",
                              headers={"X-Expected-Shard": "A", "X-Routing-Epoch": "2"}), 409,
                   "stale/future epoch")
        check_http(client.get(f"{a1}/internal/accounts/A0500/balance"), 428,
                   "missing route claims")
        print("PASS shard-local defense: wrong owner / stale epoch / missing claims rejected", flush=True)

    print("STAGE 03 SHARDING CHECK: ALL CHECKS PASSED", flush=True)


if __name__ == "__main__":
    verify()
