"""Real PostgreSQL + HTTP Stage 05 acceptance test; reverses all balance deltas.

Run ONLY with both A replicas and B replicas healthy and without other writers:
    docker compose run --rm schema-migrator python -m scripts.verify_stage05

Each run uses unique request IDs. If a run aborts midway, some changes MAY remain;
never blindly reset balances. Use the status API and local journal to reconcile.
"""
from __future__ import annotations

import os
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import httpx
import psycopg
from psycopg.rows import dict_row

BASE = "http://gateway:8000"


def unique() -> str:
    return f"stage05-{uuid.uuid4()}"


def require(r: httpx.Response, status: int, label: str) -> dict:
    assert r.status_code == status, f"{label}: expected {status}, got {r.status_code}: {r.text[:600]}"
    return r.json()


def balance(c: httpx.Client, account: str) -> Decimal:
    return Decimal(require(c.get(f"{BASE}/accounts/{account}/balance"), 200, f"balance {account}")["balance"])


def write(c: httpx.Client, account: str, op: str, amount: str,
          request_id: str, status: int = 200) -> dict:
    return require(c.post(f"{BASE}/accounts/{account}/{op}",
                          json={"amount": amount, "request_id": request_id}), status, op)


def transfer(c: httpx.Client, src: str, dst: str, amount: str,
             request_id: str, status: int = 200) -> dict:
    return require(c.post(f"{BASE}/transfers", json={
        "from_account": src, "to_account": dst, "amount": amount, "request_id": request_id,
    }), status, "transfer")


def snapshot(db_env: str, accounts: tuple[str, ...]) -> dict:
    with psycopg.connect(os.environ[db_env], row_factory=dict_row) as conn:
        results = conn.execute(
            "SELECT account_id, balance, version FROM accounts "
            "WHERE account_id = ANY(%s::varchar[]) ORDER BY account_id", (list(accounts),)
        ).fetchall()
        return {r["account_id"]: (r["balance"], r["version"]) for r in results}


def verify() -> None:
    with httpx.Client(timeout=35, trust_env=False) as c:
        # The self-healing bonus is not active yet: require the normal Stage 04 case.
        for shard in ("A", "B"):
            info = require(c.get(f"{BASE}/debug/replicas/{shard}"), 200, "replicas")
            assert set(info["healthy"]) == {f"{shard}1", f"{shard}2"}, info
        a0 = snapshot("SHARD_A_DATABASE_URL", ("A0001", "A0500"))
        b0 = snapshot("SHARD_B_DATABASE_URL", ("A1500",))
        assert len(a0) == 2 and len(b0) == 1, (a0, b0)
        for db in ("SHARD_A_DATABASE_URL", "SHARD_B_DATABASE_URL"):
            with psycopg.connect(os.environ[db]) as conn:
                assert conn.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version='003_stage05_local_acid.sql'"
                ).fetchone() == (1,), f"stage05 migration not applied in {db}"
        print("PASS Stage 05 migration applied to both shard databases", flush=True)

        # One deposit, a concurrent-safe duplicate retry and a compensating withdrawal.
        req = unique()
        first = write(c, "A0500", "deposit", "17.50", req)
        assert first["status"] == "COMMITTED" and not first["idempotent_replay"], first
        again = write(c, "A0500", "deposit", "17.5", req)
        assert again["idempotent_replay"] and again["tx_id"] == first["tx_id"], again
        assert balance(c, "A0500") == a0["A0500"][0] + Decimal("17.50")
        conflicting = write(c, "A0500", "withdraw", "17.5", req, 409)
        assert "different operation" in conflicting["detail"]
        result = write(c, "A0500", "withdraw", "17.50", unique())
        assert result["status"] == "COMMITTED"
        assert balance(c, "A0500") == a0["A0500"][0]
        status = require(c.get(f"{BASE}/accounts/A0500/operations/{req}"), 200, "status")
        assert status["status"] == "COMMITTED" and status["result"]["tx_id"] == first["tx_id"]
        print("PASS durable idempotency, exact-money arithmetic and status lookup", flush=True)

        # Persisted rejection: identical retries remain rejected; no debit occurred.
        reject_id = unique()
        reject = write(c, "A0001", "withdraw", "999999999999999999.99", reject_id, 409)
        assert reject["detail"] == "insufficient funds", reject
        again = write(c, "A0001", "withdraw", "999999999999999999.99", reject_id, 409)
        assert again["idempotent_replay"] is True
        assert balance(c, "A0001") == a0["A0001"][0]
        print("PASS insufficient funds rejected and durable reject replay", flush=True)

        # Opposing operations through arbitrary replicas, with the same DB row lock.
        writes = [("deposit", unique()) for _ in range(16)] + [
            ("withdraw", unique()) for _ in range(16)]
        def worker(item):
            op, rid = item
            with httpx.Client(timeout=35, trust_env=False) as local_client:
                return write(local_client, "A0500", op, "1.00", rid)
        with ThreadPoolExecutor(max_workers=6) as pool:
            responses = list(pool.map(worker, writes))
        served = Counter(x["served_by"] for x in responses)
        assert set(served) == {"A1", "A2"}, served
        assert all(x["status"] == "COMMITTED" and not x["idempotent_replay"] for x in responses)
        assert balance(c, "A0500") == a0["A0500"][0]
        print(f"PASS 32 concurrent writes across A1/A2: {dict(served)}, no lost updates", flush=True)

        # Two account changes in one atomic local transfer and compensating reverse.
        before_a = (balance(c, "A0500"), balance(c, "A0001"))
        xfer = transfer(c, "A0500", "A0001", "5.25", unique())
        assert xfer["kind"] == "SAME_SHARD_TRANSFER" and xfer["status"] == "COMMITTED"
        assert balance(c, "A0500") == before_a[0] - Decimal("5.25")
        assert balance(c, "A0001") == before_a[1] + Decimal("5.25")
        assert sum((balance(c, "A0500"), balance(c, "A0001"))) == sum(before_a)
        transfer(c, "A0001", "A0500", "5.25", unique())
        assert (balance(c, "A0500"), balance(c, "A0001")) == before_a
        with psycopg.connect(os.environ["SHARD_A_DATABASE_URL"]) as conn:
            entry = conn.execute(
                "SELECT expected_entries, (SELECT COUNT(*) FROM account_changes "
                "WHERE change_group_id=g.change_group_id) AS actual_entries "
                "FROM account_change_groups g WHERE tx_id=%s", (xfer["tx_id"],)
            ).fetchone()
            assert entry == (2, 2), entry
        print("PASS one-transaction same-shard transfer, 2 journal entries, conservation", flush=True)

        # B shard independent physical DB, no change to authoritative ownership.
        b_seen = {write(c, "A1500", "deposit", "3.00", unique())["served_by"],
                  write(c, "A1500", "withdraw", "3.00", unique())["served_by"]}
        assert b_seen == {"B1", "B2"}, b_seen
        assert balance(c, "A1500") == b0["A1500"][0]
        # Stage 06 replaces deliberate 501 with genuine 2PC. The standalone
        # Stage 06 test exercises cross-shard transfers.
        assert balance(c, "A0500") == a0["A0500"][0]
        assert balance(c, "A1500") == b0["A1500"][0]
        print("PASS B writes distributed; 2PC covered by Stage 06 verification", flush=True)

        bad = require(c.post("http://shard-a-1:8000/internal/accounts/A0500/deposit",
                             json={"amount": "1.00", "request_id": unique()},
                             headers={"X-Expected-Shard": "A", "X-Routing-Epoch": "999"}), 409,
                      "stale epoch write")
        assert "fenced" in bad["detail"] or "epoch" in bad["detail"]
        print("PASS shard-local stale-epoch write prevented", flush=True)

        a1 = snapshot("SHARD_A_DATABASE_URL", ("A0001", "A0500"))
        b1 = snapshot("SHARD_B_DATABASE_URL", ("A1500",))
        assert {k:v[0] for k,v in a0.items()} == {k:v[0] for k,v in a1.items()}, (a0,a1)
        assert {k:v[0] for k,v in b0.items()} == {k:v[0] for k,v in b1.items()}, (b0,b1)
        assert a1["A0500"][1] == a0["A0500"][1] + 36, (a0,a1)  # 2 + 32 + 2 transfers
        assert a1["A0001"][1] == a0["A0001"][1] + 2, (a0,a1)
        assert b1["A1500"][1] == b0["A1500"][1] + 2, (b0,b1)
        print("PASS all sample balances restored; account version increments exact", flush=True)
    print("STAGE 05 BANKING/CONCURRENCY CHECK: ALL CHECKS PASSED", flush=True)


if __name__ == "__main__":
    verify()
