"""Atomic B->metadata historical idempotency backfill, safe to rerun.

Run only after stage12 B fencing and C ownership publication. B is read-only
for the migrated range, but local dedup history in B belongs to one shared
Shard B database. We import its entire local ledger to cover even rejected
transfers whose stored error response omits account IDs.
"""
from __future__ import annotations

import os

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

META = os.environ['METADATA_DATABASE_URL']
B = os.environ['SHARD_B_DATABASE_URL']


def backfill() -> int:
    # Acquire a metadata session-wide lease so two backfill workers cannot race.
    with psycopg.connect(META, autocommit=True, row_factory=dict_row, connect_timeout=8) as lock:
        lock.execute("SELECT pg_advisory_lock(hashtext('stage13-b-request-archive'))")
        try:
            with psycopg.connect(META, row_factory=dict_row, connect_timeout=8) as md:
                job = md.execute(
                    "SELECT job_id,phase FROM migration_jobs WHERE source_shard='B' "
                    "AND target_shard='C' AND range_start=1501 AND range_end=2000 "
                    "ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
                if not job or job['phase'] not in ('VERIFYING','COMPLETED'):
                    raise RuntimeError('stage12 cutover must complete before backfill')
                existing = md.execute(
                    "SELECT row_count FROM migrated_request_backfill WHERE job_id=%s",
                    (job['job_id'],),
                ).fetchone()
                if existing:
                    print(f"Stage 13 request archive already complete ({existing['row_count']} rows)")
                    return int(existing['row_count'])

            with psycopg.connect(B, row_factory=dict_row, connect_timeout=8) as source:
                source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                fence = source.execute(
                    "SELECT writable, phase FROM ownership_fences WHERE range_start=1501 "
                    "AND range_end=2000"
                ).fetchone()
                if not fence or fence['writable'] or fence['phase']!='FENCED':
                    raise RuntimeError('B still writable: refusing legacy backfill')
                # Keep the REPEATABLE READ snapshot fixed until all rows are fetched.
                rows = source.execute(
                    "SELECT client_request_id,request_fingerprint,tx_id,status,response,updated_at "
                    "FROM local_request_dedup ORDER BY client_request_id"
                ).fetchall()
                # Any surviving PENDING row lacks a proven final decision. Copy
                # it as PENDING, and replay will not reissue it on a new owner.

            with psycopg.connect(META, row_factory=dict_row, connect_timeout=8) as md:
                with md.transaction():
                    # The visible COMPLETE marker must commit in the SAME metadata
                    # transaction as the historical records (no partial readers).
                    for r in rows:
                        md.execute(
                            "INSERT INTO migrated_legacy_requests "
                            "(request_id,source_shard,request_fingerprint,tx_id,status,response,source_updated_at) "
                            "VALUES (%s,'B',%s,%s,%s,%s,%s) "
                            "ON CONFLICT (request_id) DO NOTHING",
                            (r['client_request_id'],r['request_fingerprint'],r['tx_id'],
                             r['status'], Jsonb(r['response']) if r['response'] is not None else None,
                             r['updated_at']),
                        )
                        check = md.execute(
                            "SELECT request_fingerprint,tx_id,status,response FROM migrated_legacy_requests "
                            "WHERE request_id=%s", (r['client_request_id'],),
                        ).fetchone()
                        if (check['request_fingerprint']!=r['request_fingerprint']
                                or check['tx_id']!=r['tx_id']
                                or check['status']!=r['status']
                                or check['response']!=r['response']):
                            raise RuntimeError('conflicting preexisting legacy request archive row')
                    # Must still be in the intended post-cutover phase.
                    job_phase = md.execute(
                        "SELECT phase FROM migration_jobs WHERE job_id=%s FOR SHARE",
                        (job['job_id'],),
                    ).fetchone()
                    if job_phase['phase'] not in ('VERIFYING','COMPLETED'):
                        raise RuntimeError('migration ownership changed during backfill')
                    md.execute(
                        "INSERT INTO migrated_request_backfill(job_id,source_shard,state,row_count) "
                        "VALUES (%s,'B','COMPLETE',%s) ON CONFLICT (job_id) DO NOTHING",
                        (job['job_id'],len(rows)),
                    )
            print(f'STAGE 13 LEGACY REQUEST ARCHIVE: COMPLETE ({len(rows)} B rows)', flush=True)
            return len(rows)
        finally:
            lock.execute("SELECT pg_advisory_unlock(hashtext('stage13-b-request-archive'))")


if __name__ == '__main__':
    backfill()
