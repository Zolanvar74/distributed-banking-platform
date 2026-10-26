"""Metadata DB access used by the Stage-02 read-only services."""
import os
import re
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row

ACCOUNT_PATTERN = re.compile(r"^A(\d{4})$")


@dataclass(frozen=True)
class Owner:
    shard_id: str
    epoch: int


def get_owner(account_id: str) -> Owner | None:
    match = ACCOUNT_PATTERN.fullmatch(account_id)
    if not match:
        return None
    account_number = int(match.group(1))
    with psycopg.connect(os.environ["METADATA_DATABASE_URL"], row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT shard_id, epoch FROM shard_map "
                "WHERE %s BETWEEN range_start AND range_end AND active = true",
                (account_number,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return Owner(shard_id=row["shard_id"], epoch=row["epoch"])


def list_replicas(shard_id: str) -> list[dict]:
    with psycopg.connect(os.environ["METADATA_DATABASE_URL"], row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT replica_id, hostname, port FROM replica_registry "
                "WHERE shard_id = %s ORDER BY replica_id",
                (shard_id,),
            )
            return list(cursor.fetchall())


def register_replica(shard_id: str, replica_id: str, hostname: str) -> None:
    # Stable slot = shard-specific replica number. The UNIQUE(shard_id, slot)
    # constraint stops two logical replicas from occupying the same slot.
    match = re.search(r"(\d+)$", replica_id)
    if match is None:
        raise ValueError(f"Replica ID must end in a positive slot number: {replica_id}")
    slot = int(match.group(1))
    if slot < 1:
        raise ValueError("Replica slot must be positive")
    with psycopg.connect(os.environ["METADATA_DATABASE_URL"]) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO replica_registry(replica_id, shard_id, hostname, port, slot)
                   VALUES (%s, %s, %s, 8000, %s)
                   ON CONFLICT (replica_id)
                   DO UPDATE SET shard_id = EXCLUDED.shard_id,
                                 hostname = EXCLUDED.hostname,
                                 port = EXCLUDED.port,
                                 slot = EXCLUDED.slot,
                                 registered_at = now()""",
                (replica_id, shard_id, hostname, slot),
            )
