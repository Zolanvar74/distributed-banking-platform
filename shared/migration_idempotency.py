"""Stage 13: old B request IDs are authoritative even after ownership changes.

This archive does NOT issue writes and does not infer completion from a missing
row. It is a read-only guard against duplicate client messages sent with a
new routing topology. A partial backfill is NEVER marked COMPLETE.
"""
from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from shared.banking_contract import fingerprint
from shared.routing import account_number


MOVING_LOW, MOVING_HIGH = 1501, 2000


def touches_migrated_range(accounts: list[str]) -> bool:
    return any((n := account_number(account)) is not None and
               MOVING_LOW <= n <= MOVING_HIGH for account in accounts)


def archive_guard(dsn: str, request_id: str, *, kind: str, source: str,
                  amount: str, destination: str | None = None) -> tuple[int, dict] | None:
    """Resolve previously recorded B requests, fail closed if archive not ready.

    Called for every post-cutover operation that touches the migrated range.
    - None: no old request ID; proceed through the CURRENT owning shard.
    - (code, body): identical replay, fingerprint conflict, or pending status.

    For transfer retries originating on B, the *historical* fingerprint is
    SAME_SHARD_TRANSFER even if its current route is cross-shard B->C.
    """
    expected = fingerprint(kind, source, amount, destination)
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=4) as db:
        with db.transaction():
            db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            migrated = db.execute(
                "SELECT job_id, phase FROM migration_jobs "
                "WHERE source_shard='B' AND target_shard='C' "
                "AND range_start=1501 AND range_end=2000 "
                "AND phase IN ('MAP_ACTIVATED','VERIFYING','COMPLETED') "
                "ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if migrated is None:
                return None
            ready = db.execute(
                "SELECT 1 FROM migrated_request_backfill WHERE job_id=%s AND state='COMPLETE'",
                (migrated['job_id'],),
            ).fetchone()
            if not ready:
                return 503, {"status":"PENDING", "request_id":request_id,
                             "detail":"migrated request index not ready; retry same request_id"}
            recorded = db.execute(
                "SELECT request_fingerprint,status,response FROM migrated_legacy_requests "
                "WHERE request_id=%s", (request_id,),
            ).fetchone()
    if recorded is None:
        return None
    if recorded['request_fingerprint'] != expected:
        return 409, {"detail":"request_id already used for a different operation",
                     "request_id":request_id}
    if recorded['status']=='PENDING' or recorded['response'] is None:
        return 202, {"status":"PENDING","request_id":request_id,
                     "detail":"historical request unresolved; do not submit a new request_id"}
    reply = recorded['response']
    return int(reply['http_status']), dict(reply['body'])


def archived_local_status(dsn: str, account_id: str, request_id: str) -> tuple[int,dict] | None:
    """Expose a historical status for a moved account without querying stale C.

    Returns None for an unrelated ID; callers may continue to the new owner.
    The successful original response contains the affected account ID, which
    we use to avoid exposing one account's receipt under another account path.
    """
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=4) as db:
        record = db.execute(
            "SELECT l.tx_id,l.status,l.response FROM migrated_legacy_requests l "
            "JOIN migrated_request_backfill b ON b.state='COMPLETE' "
            "JOIN migration_jobs j ON j.job_id=b.job_id "
            "WHERE l.request_id=%s AND j.source_shard='B' AND j.target_shard='C' "
            "AND j.phase IN ('MAP_ACTIVATED','VERIFYING','COMPLETED')",
            (request_id,),
        ).fetchone()
    if not record or not record['response']:
        return None
    original = record['response']['body']
    if original.get('account_id') != account_id and original.get('to_account') != account_id:
        return None
    return 200, {"request_id":request_id,"tx_id":str(record['tx_id']),
                 "status":record['status'],"result":dict(original),
                 "historical_owner":"B"}
