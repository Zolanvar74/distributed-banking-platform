"""Runnable inside gateway container: docker compose exec -T gateway python scripts/smoke_test.py"""
import json
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"


def get(path: str) -> dict:
    with urllib.request.urlopen(BASE + path, timeout=8) as res:
        return json.load(res)


def check() -> None:
    health = get("/health")
    assert health["status"] == "ok", health

    tests = [
        ("A0001", "A", "100.00"),
        ("A0500", "A", "1000.00"),
        ("A1000", "A", "300.00"),
        ("A1001", "B", "300.00"),
        ("A1500", "B", "500.00"),
        ("A2000", "B", "100.00"),
    ]
    for account, shard, amount in tests:
        record = get(f"/accounts/{account}/balance")
        assert record["shard"] == shard, record
        assert record["balance"] == amount, record
        print(f"PASS balance {account} -> {shard} / {amount} / {record['served_by']}")

    for shard, account in [("A", "A0500"), ("B", "A1500")]:
        selected = {get(f"/debug/route/{account}")["selected_replica"] for _ in range(8)}
        assert selected == {f"{shard}1", f"{shard}2"}, selected
        print(f"PASS round-robin shard {shard}: {', '.join(sorted(selected))}")

    for bad in ["A0000", "A2001", "B0001"]:
        try:
            get(f"/accounts/{bad}/balance")
            raise AssertionError(f"unexpected valid account: {bad}")
        except urllib.error.HTTPError as exc:
            assert exc.code == 404, (bad, exc.code)
        print(f"PASS reject invalid account {bad}")
    print("STAGE 01 SMOKE TEST: ALL CHECKS PASSED")


if __name__ == "__main__":
    for attempt in range(1, 7):
        try:
            check()
            break
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == 6:
                raise
            print(f"Services not ready ({exc}), retrying {attempt}/6...")
            time.sleep(3)
