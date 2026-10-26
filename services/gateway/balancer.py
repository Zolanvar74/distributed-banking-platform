"""Replica identity validation and deterministic per-shard round robin.

Stage 04: failover applies only to safe, read-only GET balance requests. Never
retry money-moving requests without a request-id and durable deduplication.
"""
from __future__ import annotations

import itertools
from collections.abc import Sequence


def identity_ok(identity: object, shard_id: str, replica_id: str) -> bool:
    return (isinstance(identity, dict)
            and identity.get("status") == "ok"
            and identity.get("shard") == shard_id
            and identity.get("replica") == replica_id)


class RoundRobin:
    def __init__(self) -> None:
        self._counters: dict[str, itertools.count] = {}

    def ordered(self, shard_id: str, healthy: Sequence[dict]) -> list[dict]:
        if not healthy:
            return []
        counter = self._counters.setdefault(shard_id, itertools.count())
        start = next(counter) % len(healthy)
        return list(healthy[start:]) + list(healthy[:start])
