"""Stage 11: non-writable target C snapshot and crash-resumable account journal replay.

Only the B database is authoritative until Stage 12/13 cutover. Target rows
live in migration_target_accounts and are NEVER exposed as banking balances.
Each whole source change group affecting migrated accounts is applied within
ONE PostgreSQL transaction, with a per-group idempotency record.
"""
from __future__ import annotations

from shared.stage14_faults import crash_if_armed

from collections import defaultdict
from decimal import Decimal
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from shared.migration_capture import CaptureError, snapshot_digest


def source_snapshot(dsn: str, job_id: UUID) -> tuple[dict, list[dict]]:
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=8) as db:
        db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        batch = db.execute(
            "SELECT range_start,range_end,row_count,snapshot_digest "
            "FROM migration_snapshot_batches WHERE job_id=%s", (job_id,),
        ).fetchone()
        if batch is None:
            raise CaptureError("snapshot missing on source B; run stage10 bootstrap first")
        rows = db.execute(
            "SELECT account_id,balance,version FROM migration_snapshot_accounts "
            "WHERE job_id=%s ORDER BY account_id", (job_id,),
        ).fetchall()
        if len(rows) != batch["row_count"] or snapshot_digest(rows) != batch["snapshot_digest"]:
            raise CaptureError("immutable source snapshot verification failed")
        if not rows or batch["range_start"] != 1501 or batch["range_end"] != 2000:
            raise CaptureError("unsupported source snapshot range or empty source")
        if any(not 1501 <= int(x["account_id"][1:]) <= 2000 for x in rows):
            raise CaptureError("source snapshot contains out-of-range account")
    return batch, rows


def copy_snapshot(source_dsn: str, target_dsn: str, job_id: UUID) -> dict:
    batch, rows = source_snapshot(source_dsn, job_id)
    with psycopg.connect(target_dsn, row_factory=dict_row, connect_timeout=8) as db:
        with db.transaction():
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"stage11:{job_id}",))
            existing = db.execute(
                "SELECT range_start,range_end,row_count,snapshot_digest "
                "FROM migration_target_batches WHERE job_id=%s FOR UPDATE", (job_id,),
            ).fetchone()
            if existing is None:
                db.execute(
                    "INSERT INTO migration_target_batches "
                    "(job_id,range_start,range_end,row_count,snapshot_digest) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (job_id, batch["range_start"],batch["range_end"],
                     batch["row_count"], batch["snapshot_digest"]),
                )
                for r in rows:
                    db.execute(
                        "INSERT INTO migration_target_accounts "
                        "(job_id,account_id,balance,snapshot_balance,version,snapshot_version) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (job_id, r["account_id"],r["balance"],r["balance"],
                         r["version"],r["version"]),
                    )
            elif any(existing[k] != batch[k] for k in
                     ("range_start","range_end","row_count","snapshot_digest")):
                raise CaptureError("target snapshot identity or fingerprint changed")
            persisted = db.execute(
                "SELECT account_id,snapshot_balance AS balance,snapshot_version AS version "
                "FROM migration_target_accounts WHERE job_id=%s ORDER BY account_id", (job_id,),
            ).fetchall()
            if len(persisted) != len(rows) or snapshot_digest(persisted) != batch["snapshot_digest"]:
                raise CaptureError("target snapshot mismatch; refusing to overwrite")
            if {x["account_id"] for x in persisted} != {x["account_id"] for x in rows}:
                raise CaptureError("target snapshot membership differs from source")
    return {"job_id":str(job_id), "accounts":len(rows),
            "snapshot_digest":batch["snapshot_digest"], "copied":True}


def source_tail(source_dsn: str, job_id: UUID) -> tuple[dict[str,dict], dict[str,list[dict]], int]:
    """Read source balances, full group completeness and tail from one MVCC view.

    A per-account version (NOT a BIGSERIAL) is the replay cursor. A group's
    affected migrating accounts must be updated atomically on target C.
    """
    with psycopg.connect(source_dsn,row_factory=dict_row,connect_timeout=8) as db:
        db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        batch = db.execute("SELECT row_count,snapshot_digest FROM migration_snapshot_batches "
                           "WHERE job_id=%s",(job_id,)).fetchone()
        if batch is None:
            raise CaptureError("source batch is missing")
        snaps = db.execute("SELECT account_id,balance,version FROM migration_snapshot_accounts "
                           "WHERE job_id=%s ORDER BY account_id",(job_id,)).fetchall()
        if len(snaps)!=batch["row_count"] or snapshot_digest(snaps)!=batch["snapshot_digest"]:
            raise CaptureError("source snapshot was modified")
        current = db.execute(
            "SELECT account_id,balance,version FROM accounts "
            "WHERE substring(account_id from 2)::integer BETWEEN 1501 AND 2000 "
            "ORDER BY account_id",
        ).fetchall()
        if {x["account_id"] for x in current}!={x["account_id"] for x in snaps}:
            raise CaptureError("source account membership changed")
        tail=db.execute(
            "SELECT ch.account_id,ch.version,ch.change_group_id,"
            "ch.before_balance,ch.after_balance,g.expected_entries,g.operation_kind "
            "FROM account_changes ch "
            "JOIN migration_snapshot_accounts s ON s.account_id=ch.account_id AND s.job_id=%s "
            "JOIN account_change_groups g ON g.change_group_id=ch.change_group_id "
            "WHERE ch.version>s.version ORDER BY ch.account_id,ch.version",(job_id,),
        ).fetchall()
        ids={r["change_group_id"] for r in tail}
        # Check all source group members, including possible out-of-range ones.
        if ids:
            counts=db.execute(
                "SELECT change_group_id,COUNT(*)::integer AS n FROM account_changes "
                "WHERE change_group_id=ANY(%s::uuid[]) GROUP BY change_group_id",
                (list(ids),),
            ).fetchall()
            totals={r["change_group_id"]:r["n"] for r in counts}
            for ch in tail:
                if totals.get(ch["change_group_id"])!=ch["expected_entries"]:
                    raise CaptureError("source group incomplete: "+str(ch["change_group_id"]))
        per_account=defaultdict(list)
        groups=defaultdict(list)
        for ch in tail:
            per_account[ch["account_id"]].append(ch)
            groups[ch["change_group_id"]].append(ch)
        live={r["account_id"]:r for r in current}
        for snap in snaps:
            version,balance=snap["version"],snap["balance"]
            for ch in per_account[snap["account_id"]]:
                if ch["version"]!=version+1 or ch["before_balance"]!=balance:
                    raise CaptureError("source journal chain invalid for "+snap["account_id"])
                version,balance=ch["version"],ch["after_balance"]
            if version!=live[snap["account_id"]]["version"] or balance!=live[snap["account_id"]]["balance"]:
                raise CaptureError("source journal differs from committed account state")
        return live,dict(groups),len(tail)


def target_rows(dsn: str, job_id: UUID) -> dict[str,dict]:
    with psycopg.connect(dsn,row_factory=dict_row,connect_timeout=8) as db:
        rows=db.execute("SELECT account_id,balance,version,snapshot_version "
                        "FROM migration_target_accounts WHERE job_id=%s",(job_id,)).fetchall()
    return {x["account_id"]:x for x in rows}


def _apply_one(target_dsn: str, job_id: UUID, gid: UUID, entries: list[dict]) -> bool:
    """Single transaction commits all in-range effects and the replay ledger."""
    with psycopg.connect(target_dsn,row_factory=dict_row,connect_timeout=8) as db:
        with db.transaction():
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",(f"stage11:{job_id}",))
            batch=db.execute("SELECT job_id FROM migration_target_batches "
                             "WHERE job_id=%s FOR UPDATE",(job_id,)).fetchone()
            if batch is None:
                raise CaptureError("target snapshot not copied")
            previous=db.execute("SELECT applied_entries FROM migration_target_applied_groups "
                                "WHERE job_id=%s AND change_group_id=%s",(job_id,gid)).fetchone()
            if previous is not None:
                if previous["applied_entries"]!=len(entries):
                    raise CaptureError("replayed group membership changed")
                return False
            for change in sorted(entries,key=lambda c:c["account_id"]):
                row=db.execute("SELECT version,balance FROM migration_target_accounts "
                               "WHERE job_id=%s AND account_id=%s FOR UPDATE",
                               (job_id,change["account_id"])).fetchone()
                if row is None or row["version"]+1!=change["version"] or row["balance"]!=change["before_balance"]:
                    raise CaptureError("target replay version or balance mismatch")
            for ch in sorted(entries,key=lambda c:c["account_id"]):
                db.execute("UPDATE migration_target_accounts SET version=%s,balance=%s "
                           "WHERE job_id=%s AND account_id=%s",
                           (ch["version"],ch["after_balance"],job_id,ch["account_id"]))
                db.execute("INSERT INTO migration_target_replayed_changes "
                           "(job_id,account_id,version,change_group_id) VALUES (%s,%s,%s,%s)",
                           (job_id,ch["account_id"],ch["version"],gid))
            db.execute("INSERT INTO migration_target_applied_groups "
                       "(job_id,change_group_id,applied_entries) VALUES (%s,%s,%s)",
                       (job_id,gid,len(entries)))
    return True



def ready_group_ids(remaining: dict, target: dict) -> list:
    """Group-level DAG scheduling based on per-account version predecessors."""
    ready=[]
    for gid,changes in remaining.items():
        versions=[(ch["version"],target[ch["account_id"]]["version"]) for ch in changes]
        if all(v<=cur for v,cur in versions) or all(v==cur+1 for v,cur in versions):
            ready.append(gid)
        elif any(v<=cur for v,cur in versions):
            raise CaptureError("partial replay of atomic change group detected")
    return sorted(ready,key=str)

def replay_once(source_dsn: str, target_dsn: str, meta_dsn: str, job_id: UUID) -> dict:
    source, groups, events = source_tail(source_dsn,job_id)
    target=target_rows(target_dsn,job_id)
    if set(source)!=set(target):
        raise CaptureError("target membership differs from source membership")
    # A group must be replayed atomically for every affected migrated account.
    # Different groups can be applied in any order compatible with ALL their
    # individual account-version dependencies, even under overlapping transfers.
    remaining=dict(groups)
    processed=0
    while remaining:
        ready=ready_group_ids(remaining,target)
        if not ready:
            raise CaptureError("no replayable group; source version dependencies inconsistent")
        for gid in sorted(ready,key=str):
            did_apply=_apply_one(target_dsn,job_id,gid,remaining[gid])
            if did_apply:
                processed+=1
                # Target replay group already committed; Metadata ACK may not be.
                crash_if_armed(job_id, "DELTA_GROUP_APPLIED")
            # Reload because another worker may have advanced the target while
            # we waited for the target transaction lock.
            target=target_rows(target_dsn,job_id)
            del remaining[gid]
    # Re-read committed target state, THEN acknowledge in Metadata. Crash after
    # committing to C but before this ACK is safe: repeat group ledger checks.
    target=target_rows(target_dsn,job_id)
    with psycopg.connect(meta_dsn,row_factory=dict_row,connect_timeout=8) as meta:
        with meta.transaction():
            job=meta.execute("SELECT phase,snapshot_id FROM migration_jobs WHERE job_id=%s FOR UPDATE",
                             (job_id,)).fetchone()
            if job is None or job["snapshot_id"]!=str(job_id) or job["phase"] not in (
                "TARGET_READY","CATCHING_UP","FINAL_REPLAY"):
                raise CaptureError("migration phase incompatible with target acknowledgments")
            for acc,row in target.items():
                meta.execute(
                    "INSERT INTO migration_target_progress(job_id,account_id,snapshot_version,applied_version) "
                    "VALUES (%s,%s,%s,%s) ON CONFLICT(job_id,account_id) DO UPDATE "
                    "SET applied_version=GREATEST(migration_target_progress.applied_version,"
                    "EXCLUDED.applied_version),updated_at=clock_timestamp() "
                    "WHERE migration_target_progress.snapshot_version=EXCLUDED.snapshot_version",
                    (job_id,acc,row["snapshot_version"],row["version"]),
                )
            if job["phase"]=="TARGET_READY":
                meta.execute("UPDATE migration_jobs SET phase='CATCHING_UP',"
                             "updated_at=clock_timestamp() WHERE job_id=%s",(job_id,))
                meta.execute("INSERT INTO migration_recovery_events(job_id,phase,details) "
                             "VALUES (%s,'CATCHING_UP','{\"stage\":11}'::jsonb)",(job_id,))
    caught_up=all(target[k]["version"]==r["version"] and target[k]["balance"]==r["balance"]
                  for k,r in source.items())
    return {"job_id":str(job_id),"source_tail_events":events,"groups_applied_this_pass":processed,
            "accounts":len(target),"caught_up_at_source_read":caught_up,
            "ownership_unchanged":True}
