"""Stage 10 foundation: consistent snapshot and durable source journal observation.

Stage 10 does NOT migrate ownership or copy to C.  `account_changes` from
Stage 05 is the transactionally committed source change journal; the record
versions are per-account (not a global transaction commit-order cursor).
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

RANGE_START = 1501
RANGE_END = 2000


class CaptureError(RuntimeError):
    pass


def snapshot_digest(rows: list[dict]) -> str:
    """Stable checksum over canonical PostgreSQL decimal / version values."""
    lines = [f"{r['account_id']}:{Decimal(r['balance']):.2f}:{int(r['version'])}\n"
             for r in sorted(rows, key=lambda x: x['account_id'])]
    return hashlib.sha256("".join(lines).encode("utf-8")).hexdigest()


def create_source_snapshot(source_dsn: str, job_id: UUID, start: int = RANGE_START,
                           end: int = RANGE_END) -> dict:
    """One committed snapshot, even if a process crashes and retries.

    All captured rows come from one INSERT .. SELECT statement MVCC snapshot.
    A transaction advisory lock serializes repeated snapshots for this job.
    We deliberately do NOT SELECT FOR UPDATE on account rows.
    """
    if not 1 <= start <= end <= 2000:
        raise ValueError("invalid snapshot bounds")
    with psycopg.connect(source_dsn, row_factory=dict_row, connect_timeout=8) as db:
        with db.transaction():
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"snapshot:{job_id}",))
            exists = db.execute(
                "SELECT range_start,range_end,row_count,snapshot_digest "
                "FROM migration_snapshot_batches WHERE job_id=%s", (job_id,)
            ).fetchone()
            if exists:
                if exists["range_start"] != start or exists["range_end"] != end:
                    raise CaptureError("job snapshot range changed")
            else:
                # Batch and its rows become visible together on COMMIT.
                db.execute(
                    "INSERT INTO migration_snapshot_batches "
                    "(job_id,range_start,range_end,snapshot_digest) VALUES (%s,%s,%s,%s)",
                    (job_id, start, end, "0" * 64),
                )
                db.execute(
                    "INSERT INTO migration_snapshot_accounts "
                    "(job_id,account_id,balance,version) "
                    "SELECT %s,account_id,balance,version FROM accounts "
                    "WHERE substring(account_id from 2)::integer BETWEEN %s AND %s",
                    (job_id, start, end),
                )
            rows = db.execute(
                "SELECT account_id,balance,version FROM migration_snapshot_accounts "
                "WHERE job_id=%s ORDER BY account_id", (job_id,)
            ).fetchall()
            digest = snapshot_digest(rows)
            if exists:
                if exists["row_count"] != len(rows) or exists["snapshot_digest"] != digest:
                    raise CaptureError("persisted snapshot digest or count mismatch")
            else:
                db.execute(
                    "UPDATE migration_snapshot_batches SET row_count=%s,snapshot_digest=%s "
                    "WHERE job_id=%s", (len(rows), digest, job_id),
                )
    return {"job_id": str(job_id), "row_count": len(rows), "snapshot_digest": digest}


def read_and_validate_source(source_dsn: str, job_id: UUID) -> tuple[list[dict], dict]:
    """Validate every account's gap-free committed journal tail in one MVCC view.

    A prepared-but-uncommitted 2PC event is invisible here; once committed,
    account version and journal row become visible atomically.
    """
    with psycopg.connect(source_dsn, row_factory=dict_row, connect_timeout=8) as db:
        db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        batch = db.execute(
            "SELECT range_start,range_end,row_count,snapshot_digest "
            "FROM migration_snapshot_batches WHERE job_id=%s", (job_id,)
        ).fetchone()
        if batch is None:
            raise CaptureError("source snapshot not yet created")
        snaps = db.execute(
            "SELECT account_id,balance,version FROM migration_snapshot_accounts "
            "WHERE job_id=%s ORDER BY account_id", (job_id,)
        ).fetchall()
        if len(snaps) != batch["row_count"] or snapshot_digest(snaps) != batch["snapshot_digest"]:
            raise CaptureError("source snapshot integrity failed")
        current = db.execute(
            "SELECT account_id,balance,version FROM accounts "
            "WHERE substring(account_id from 2)::integer BETWEEN %s AND %s",
            (batch["range_start"], batch["range_end"]),
        ).fetchall()
        if {r["account_id"] for r in snaps} != {r["account_id"] for r in current}:
            raise CaptureError("account membership changed since snapshot; migration cannot proceed")
        journal = db.execute(
            "SELECT ch.account_id,ch.version,ch.before_balance,ch.after_balance, "
            "ch.change_group_id,g.expected_entries,g.operation_kind "
            "FROM account_changes ch JOIN account_change_groups g "
            "ON g.change_group_id=ch.change_group_id "
            "JOIN migration_snapshot_accounts s ON s.job_id=%s AND s.account_id=ch.account_id "
            "WHERE ch.version > s.version ORDER BY ch.account_id,ch.version", (job_id,)
        ).fetchall()
        # Each (account_id, version) is unique and committed atomically with
        # its balance update. Validate the before/after chain, not just max().
        changes_by_acc: dict[str,list[dict]] = {}
        for change in journal:
            changes_by_acc.setdefault(change["account_id"], []).append(change)
        current_by_acc = {r["account_id"]: r for r in current}
        observations = []
        for snap in snaps:
            account_id = snap["account_id"]
            version = snap["version"]
            balance = snap["balance"]
            for change in changes_by_acc.get(account_id, []):
                if change["version"] != version + 1 or change["before_balance"] != balance:
                    raise CaptureError(f"journal gap or balance mismatch for {account_id}")
                version = change["version"]
                balance = change["after_balance"]
            live = current_by_acc[account_id]
            if version != live["version"] or balance != live["balance"]:
                raise CaptureError(f"source version/balance not explained by journal for {account_id}")
            observations.append({"account_id": account_id,
                                 "snapshot_version": snap["version"],
                                 "observed_version": version})
    return observations, {"journal_entries": len(journal), "row_count": len(snaps),
                          "snapshot_digest": batch["snapshot_digest"]}


def persist_observations(metadata_dsn: str, job_id: UUID, observations: list[dict],
                         journal_stats: dict) -> dict:
    """Checkpoint source observation, never pretend changes reached target C."""
    with psycopg.connect(metadata_dsn, row_factory=dict_row, connect_timeout=8) as db:
        with db.transaction():
            job = db.execute(
                "SELECT phase,snapshot_id FROM migration_jobs WHERE job_id=%s FOR UPDATE",
                (job_id,),
            ).fetchone()
            if not job or job["snapshot_id"] != str(job_id):
                raise CaptureError("job not registered as source snapshot")
            if job["phase"] not in ("SNAPSHOTTING", "TARGET_READY", "CATCHING_UP"):
                raise CaptureError(f"phase {job['phase']} does not allow source capture")
            for item in observations:
                db.execute(
                    "INSERT INTO migration_source_observations "
                    "(job_id,account_id,snapshot_version,observed_version) "
                    "VALUES (%s,%s,%s,%s) ON CONFLICT (job_id,account_id) "
                    "DO UPDATE SET observed_version=GREATEST("
                    "migration_source_observations.observed_version,EXCLUDED.observed_version), "
                    "updated_at=clock_timestamp() "
                    "WHERE migration_source_observations.snapshot_version=EXCLUDED.snapshot_version",
                    (job_id,item["account_id"],item["snapshot_version"],item["observed_version"]),
                )
            # Source observation rows must match the immutable snapshot
            # membership: never silently accept a failed ON CONFLICT update.
            actual_count = db.execute(
                "SELECT COUNT(*) FROM migration_source_observations WHERE job_id=%s",
                (job_id,),
            ).fetchone()["count"]
            if actual_count != len(observations):
                raise CaptureError("source observation membership/checkpoint mismatch")
            latest = db.execute(
                "SELECT COUNT(*)::integer AS accounts,"
                "COALESCE(SUM(observed_version-snapshot_version),0)::bigint AS observed_deltas "
                "FROM migration_source_observations WHERE job_id=%s", (job_id,),
            ).fetchone()
    return {"job_id": str(job_id), "accounts": latest["accounts"],
            "observed_deltas": latest["observed_deltas"], **journal_stats,
            "target_replayed": False}
