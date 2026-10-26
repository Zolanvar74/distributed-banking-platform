"""Pure routing policy tests; no databases or Docker required."""
import pytest

from services.gateway.balancer import RoundRobin, identity_ok


def test_cannot_select_wrong_identity():
    assert identity_ok({"status": "ok", "shard": "A", "replica": "A1"}, "A", "A1")
    assert not identity_ok({"status": "ok", "shard": "B", "replica": "A1"}, "A", "A1")
    assert not identity_ok({"status": "ok", "shard": "A", "replica": "A2"}, "A", "A1")
    assert not identity_ok({"status": "down", "shard": "A", "replica": "A1"}, "A", "A1")
    assert not identity_ok([], "A", "A1")


def test_round_robin_independent_shards():
    balancer = RoundRobin()
    a = [{"replica_id": "A1"}, {"replica_id": "A2"}]
    b = [{"replica_id": "B1"}, {"replica_id": "B2"}]
    assert [balancer.ordered("A", a)[0]["replica_id"] for _ in range(4)] == ["A1", "A2", "A1", "A2"]
    assert [balancer.ordered("B", b)[0]["replica_id"] for _ in range(2)] == ["B1", "B2"]


def test_missing_replica_skipped_and_can_return():
    balancer = RoundRobin()
    healthy = [{"replica_id": "A2"}]
    assert all(balancer.ordered("A", healthy)[0]["replica_id"] == "A2" for _ in range(5))
    recovered = [{"replica_id": "A1"}, {"replica_id": "A2"}]
    assert {balancer.ordered("A", recovered)[0]["replica_id"] for _ in range(6)} == {"A1", "A2"}


def test_empty_candidates():
    assert RoundRobin().ordered("A", []) == []
