"""Persisted metadata lookups for shard-aware routing and replica discovery.

The Gateway reads the active shard_map on each request (no baked-in shard
ownership or map cache). Registry rows are candidates, not health proofs.
"""
import os
import re
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row

from shared.routing import ACCOUNT_PATTERN, account_number


@dataclass(frozen=True)
class Owner:
    shard_id: str
    epoch: int


def get_owner(account_id: str) -> Owner | None:
    number = account_number(account_id)
    if number is None:
        return None
    with psycopg.connect(os.environ["METADATA_DATABASE_URL"], row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT shard_id, epoch FROM shard_map "
            "WHERE %s BETWEEN range_start AND range_end AND active = true",
            (number,),
        ).fetchone()
    if row is None:
        return None
    return Owner(shard_id=row["shard_id"], epoch=row["epoch"])


def active_shard_map() -> dict:
    """Read global revision and ownership ranges within one DB snapshot."""
    with psycopg.connect(os.environ["METADATA_DATABASE_URL"], row_factory=dict_row) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        epoch = conn.execute("SELECT epoch FROM routing_epoch WHERE singleton=1").fetchone()
        rows = conn.execute(
            "SELECT range_start, range_end, shard_id, epoch "
            "FROM shard_map WHERE active = true ORDER BY range_start"
        ).fetchall()
    return {"routing_epoch": epoch["epoch"], "active_ranges": list(rows)}


def list_replicas(shard_id: str) -> list[dict]:
    with psycopg.connect(os.environ["METADATA_DATABASE_URL"], row_factory=dict_row) as conn:
        return list(conn.execute(
            "SELECT replica_id, hostname, port FROM replica_registry "
            "WHERE shard_id = %s AND lifecycle IN ('REGISTERED','HEALTHY') "
            "ORDER BY replica_id",
            (shard_id,),
        ).fetchall())


def register_replica(shard_id: str, replica_id: str, hostname: str) -> None:
    # Stable slot = shard-specific replica number. Registry is NOT the same as
    # an authoritative health check. Later Controller handles generations.
    match = re.search(r"([0-9]+)$", replica_id)
    if match is None:
        raise ValueError(f"Replica ID must end in a positive slot number: {replica_id}")
    slot = int(match.group(1))
    if slot < 1:
        raise ValueError("Replica slot must be positive")
    with psycopg.connect(os.environ["METADATA_DATABASE_URL"]) as conn:
        conn.execute(
            """INSERT INTO replica_registry(replica_id, shard_id, hostname, port, slot)
               VALUES (%s, %s, %s, 8000, %s)
               ON CONFLICT (replica_id)
               DO UPDATE SET shard_id = EXCLUDED.shard_id,
                             hostname = EXCLUDED.hostname,
                             port = EXCLUDED.port,
                             slot = EXCLUDED.slot,
                             lifecycle = 'REGISTERED',
                             registered_at = now()""",
            (replica_id, shard_id, hostname, slot),
        )
