"""Stage 12: fail-closed B->C ownership cutover, using durable phases.

The three databases cannot be committed atomically in a *single* PostgreSQL
transaction. Safety follows from this ordered, restartable protocol:
  metadata FENCING gate -> B write fence -> final replay -> C staged readonly
  -> one atomic metadata map switch -> C write activation.
At NO time can B and C both have writable ownership fences. Brief unavailability
between steps is intentional and safer than duplicate owners.
"""
from __future__ import annotations

from shared.stage14_faults import crash_if_armed

from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from shared.migration_capture import CaptureError
from shared.migration_target import replay_once, source_tail, target_rows

START, END = 1501, 2000


def _range_accounts(dsn: str) -> dict[str, tuple]:
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=8) as db:
        rows = db.execute(
            "SELECT account_id,balance,version FROM accounts "
            "WHERE substring(account_id from 2)::integer BETWEEN %s AND %s",
            (START, END),
        ).fetchall()
    return {r['account_id']: (r['balance'], r['version']) for r in rows}


def _job(meta_dsn: str, job_id: UUID) -> dict:
    with psycopg.connect(meta_dsn, row_factory=dict_row, connect_timeout=8) as db:
        r = db.execute('SELECT * FROM migration_jobs WHERE job_id=%s', (job_id,)).fetchone()
    if not r or (r['source_shard'], r['target_shard'], r['range_start'], r['range_end']) != ('B', 'C', START, END):
        raise CaptureError('invalid migration job or range')
    return r


def _source_fenced(source_dsn: str) -> bool:
    with psycopg.connect(source_dsn, row_factory=dict_row, connect_timeout=8) as db:
        old = db.execute(
            'SELECT epoch,phase,writable FROM ownership_fences '
            'WHERE range_start=1501 AND range_end=2000'
        ).fetchone()
        retained = db.execute(
            'SELECT epoch,phase,writable FROM ownership_fences '
            'WHERE range_start=1001 AND range_end=1500'
        ).fetchone()
    return bool(old and not old['writable'] and old['phase']=='FENCED' and retained and retained['writable'])


def begin_fencing(meta_dsn: str, job_id: UUID) -> dict:
    """The epoch row lock serializes fencing gate against new 2PC reservations."""
    with psycopg.connect(meta_dsn, row_factory=dict_row, connect_timeout=8) as db:
        with db.transaction():
            epoch = db.execute('SELECT epoch FROM routing_epoch WHERE singleton=1 FOR UPDATE').fetchone()['epoch']
            job = db.execute('SELECT * FROM migration_jobs WHERE job_id=%s FOR UPDATE', (job_id,)).fetchone()
            if not job or (job['source_shard'],job['target_shard'],job['range_start'],job['range_end']) != ('B','C',START,END):
                raise CaptureError('invalid migration job')
            if job['phase'] in ('FENCING','FINAL_REPLAY','MAP_ACTIVATED','VERIFYING'):
                return dict(job)
            if job['phase'] != 'CATCHING_UP' or job['snapshot_id'] != str(job_id):
                raise CaptureError('stage11 catch-up must complete before cutover')
            if epoch != job['epoch_before']:
                raise CaptureError('shard map changed before migration')
            db.execute(
                "UPDATE migration_jobs SET phase='FENCING', epoch_after=%s, "
                'updated_at=clock_timestamp() WHERE job_id=%s', (epoch+1,job_id),
            )
            db.execute(
                "INSERT INTO migration_recovery_events(job_id,phase,details) "
                "VALUES (%s,'FENCING','{\"stage\":12,\"gate\":\"closed\"}'::jsonb)", (job_id,),
            )
            return dict(db.execute('SELECT * FROM migration_jobs WHERE job_id=%s',(job_id,)).fetchone())


def fence_source(source_dsn: str, epoch_before: int) -> None:
    """Conflicts with FOR SHARE fences held by writes and prepared 2PC xacts.

    Prepared or slow writers may cause a bounded lock timeout. Retry later;
    never force-commit/rollback a prepared transaction to advance the map.
    """
    with psycopg.connect(source_dsn, row_factory=dict_row, connect_timeout=8) as db:
        with db.transaction():
            db.execute("SET LOCAL lock_timeout='4s'")
            locked=db.execute(
                'SELECT range_start,range_end,epoch,phase,writable FROM ownership_fences '
                'WHERE (range_start=1001 AND range_end=2000) '
                'OR (range_start=1001 AND range_end=1500) '
                'OR (range_start=1501 AND range_end=2000) '
                'ORDER BY range_start FOR UPDATE',
            ).fetchall()
            before=next((r for r in locked if (r['range_start'],r['range_end'])==(1001,2000)),None)
            low=next((r for r in locked if (r['range_start'],r['range_end'])==(1001,1500)),None)
            high=next((r for r in locked if (r['range_start'],r['range_end'])==(1501,2000)),None)
            if before:
                if low or high or before['epoch']!=epoch_before or not before['writable']:
                    raise CaptureError('unexpected B fence layout before split')
                db.execute('UPDATE ownership_fences SET range_end=1500 WHERE range_start=1001 AND range_end=2000')
                db.execute(
                    "INSERT INTO ownership_fences(range_start,range_end,epoch,phase,writable) "
                    "VALUES (1501,2000,%s,'FENCED',false)",(epoch_before,),
                )
            elif not (low and high and low['writable'] and low['epoch']==epoch_before
                      and high['phase']=='FENCED' and not high['writable'] and high['epoch']==epoch_before):
                raise CaptureError('B source fence layout invalid; refusing cutover')


def _unresolved(meta_dsn: str) -> int:
    with psycopg.connect(meta_dsn,row_factory=dict_row,connect_timeout=8) as db:
        return db.execute(
            "SELECT COUNT(DISTINCT t.tx_id)::integer AS n FROM transactions t "
            "JOIN transaction_participants p ON p.tx_id=t.tx_id "
            "WHERE t.state<>'COMPLETED' AND p.account_id ~ '^A[0-9]{4}$' "
            "AND substring(p.account_id from 2)::integer BETWEEN 1501 AND 2000"
        ).fetchone()['n']


def require_drained(meta_dsn: str, source_dsn: str) -> None:
    if _unresolved(meta_dsn):
        raise CaptureError('migration-related 2PC still unresolved; recovery must complete first')
    # Only forbid prepared GIDs for moved accounts. Independent transfers
    # involving B's retained 1001..1500 range are allowed to continue after
    # splitting the B fence; a global B prepared count would starve migration.
    with psycopg.connect(meta_dsn,row_factory=dict_row,connect_timeout=8) as db:
        gids=[r['prepare_gid'] for r in db.execute(
            "SELECT p.prepare_gid FROM transaction_participants p "
            "WHERE p.shard_id='B' AND p.account_id ~ '^A[0-9]{4}$' "
            "AND substring(p.account_id from 2)::integer BETWEEN 1501 AND 2000"
        ).fetchall()]
    if gids:
        with psycopg.connect(source_dsn,row_factory=dict_row,connect_timeout=8) as db:
            prepared=db.execute('SELECT COUNT(*)::integer AS n FROM pg_prepared_xacts '
                                'WHERE database=current_database() AND gid=ANY(%s::text[])',
                                (gids,)).fetchone()['n']
        if prepared:
            raise CaptureError('migrating range still has prepared B participants')


def advance_final_replay(meta_dsn: str, job_id: UUID) -> None:
    with psycopg.connect(meta_dsn,row_factory=dict_row,connect_timeout=8) as db:
        with db.transaction():
            row=db.execute('SELECT phase FROM migration_jobs WHERE job_id=%s FOR UPDATE',(job_id,)).fetchone()
            if row and row['phase']=='FENCING':
                db.execute("UPDATE migration_jobs SET phase='FINAL_REPLAY',updated_at=clock_timestamp() "
                           'WHERE job_id=%s',(job_id,))
                db.execute("INSERT INTO migration_recovery_events(job_id,phase,details) VALUES "
                           "(%s,'FINAL_REPLAY','{\"stage\":12,\"source_fenced\":true}'::jsonb)",(job_id,))
            elif not row or row['phase']!='FINAL_REPLAY':
                raise CaptureError('unexpected final replay state')


def final_sync(source_dsn: str, target_dsn: str, meta_dsn: str, job_id: UUID) -> int:
    if not _source_fenced(source_dsn):
        raise CaptureError('B must be fenced before final copy')
    require_drained(meta_dsn,source_dsn)
    result=replay_once(source_dsn,target_dsn,meta_dsn,job_id)
    if not result['caught_up_at_source_read']:
        raise CaptureError('target not caught up after final replay')
    source = _range_accounts(source_dsn)
    shadow = target_rows(target_dsn,job_id)
    if not source or set(source)!=set(shadow) or any(
        source[a] != (shadow[a]['balance'],shadow[a]['version']) for a in source
    ):
        raise CaptureError('post-fence B/C shadow mismatch')
    return len(source)


def stage_target_readonly(target_dsn: str, job_id: UUID, epoch_after: int) -> None:
    """Promote checked target rows into live table, keeping C NOT writable."""
    with psycopg.connect(target_dsn,row_factory=dict_row,connect_timeout=8) as db:
        with db.transaction():
            db.execute("SET LOCAL lock_timeout='5s'")
            batch=db.execute('SELECT row_count FROM migration_target_batches WHERE job_id=%s FOR UPDATE',
                             (job_id,)).fetchone()
            if not batch or not batch['row_count']:
                raise CaptureError('target snapshot absent')
            db.execute(
                'INSERT INTO accounts(account_id,balance,version) '
                'SELECT account_id,balance,version FROM migration_target_accounts WHERE job_id=%s '
                'ON CONFLICT(account_id) DO NOTHING',(job_id,),
            )
            wrong=db.execute(
                'SELECT COUNT(*)::integer AS n FROM migration_target_accounts s '
                'LEFT JOIN accounts a ON a.account_id=s.account_id '
                'WHERE s.job_id=%s AND (a.account_id IS NULL OR a.balance<>s.balance OR a.version<>s.version)',
                (job_id,),
            ).fetchone()['n']
            if wrong:
                raise CaptureError('C live accounts inconsistent with staged target')
            extra=db.execute(
                'SELECT COUNT(*)::integer AS n FROM accounts a WHERE '
                'substring(a.account_id from 2)::integer BETWEEN 1501 AND 2000 '
                'AND NOT EXISTS (SELECT 1 FROM migration_target_accounts t '
                'WHERE t.job_id=%s AND t.account_id=a.account_id)', (job_id,),
            ).fetchone()['n']
            if extra:
                raise CaptureError('C contains extra live accounts not in source snapshot')
            existing=db.execute('SELECT epoch,phase,writable FROM ownership_fences '
                                'WHERE range_start=1501 AND range_end=2000 FOR UPDATE').fetchone()
            if existing is None:
                db.execute("INSERT INTO ownership_fences(range_start,range_end,epoch,phase,writable) "
                           "VALUES (1501,2000,%s,'TARGET_READY',false)",(epoch_after,))
            elif existing['epoch']!=epoch_after or existing['phase'] not in ('TARGET_READY','OWNED'):
                raise CaptureError('conflicting C ownership fence')
            # No path in this function changes C to writable.


def activate_map(meta_dsn: str, job_id: UUID) -> None:
    """Atomic B-range split and C-range publication in ONE metadata commit."""
    with psycopg.connect(meta_dsn,row_factory=dict_row,connect_timeout=8) as db:
        with db.transaction():
            epoch=db.execute('SELECT epoch FROM routing_epoch WHERE singleton=1 FOR UPDATE').fetchone()['epoch']
            job=db.execute('SELECT * FROM migration_jobs WHERE job_id=%s FOR UPDATE',(job_id,)).fetchone()
            if not job or job['epoch_after'] is None:
                raise CaptureError('missing durable migration epoch')
            if job['phase'] in ('MAP_ACTIVATED','VERIFYING'):
                if epoch != job['epoch_after']:
                    raise CaptureError('active epoch inconsistent with durable map decision')
                return
            if job['phase']!='FINAL_REPLAY' or epoch != job['epoch_before']:
                raise CaptureError('unexpected map activation state')
            old=db.execute('SELECT range_start,range_end,shard_id,epoch FROM shard_map '
                           'WHERE active=true AND range_start<=1501 AND range_end>=2000 FOR UPDATE').fetchall()
            if len(old)!=1 or old[0]['shard_id']!='B' or old[0]['epoch']!=job['epoch_before']:
                raise CaptureError('B no longer owns complete migration range')
            db.execute('UPDATE shard_map SET range_end=1500 '
                       'WHERE active=true AND shard_id=\'B\' AND range_start=%s AND range_end=%s',
                       (old[0]['range_start'],old[0]['range_end']))
            db.execute("INSERT INTO shard_map(range_start,range_end,shard_id,epoch,active) "
                       "VALUES (1501,2000,'C',%s,true)",(job['epoch_after'],))
            db.execute('UPDATE routing_epoch SET epoch=%s WHERE singleton=1',(job['epoch_after'],))
            db.execute("UPDATE migration_jobs SET phase='MAP_ACTIVATED',cutover_decided_at=clock_timestamp(),"
                       'updated_at=clock_timestamp() WHERE job_id=%s',(job_id,))
            db.execute("INSERT INTO migration_recovery_events(job_id,phase,details) VALUES "
                       "(%s,'MAP_ACTIVATED','{\"stage\":12,\"map_atomic\":true}'::jsonb)",(job_id,))


def activate_target(target_dsn: str, epoch_after: int) -> None:
    with psycopg.connect(target_dsn,row_factory=dict_row,connect_timeout=8) as db:
        with db.transaction():
            db.execute("SET LOCAL lock_timeout='5s'")
            row=db.execute('SELECT epoch,phase,writable FROM ownership_fences '
                           'WHERE range_start=1501 AND range_end=2000 FOR UPDATE').fetchone()
            if not row or row['epoch']!=epoch_after or row['phase'] not in ('TARGET_READY','OWNED'):
                raise CaptureError('C target fence not prepared for activation')
            db.execute("UPDATE ownership_fences SET phase='OWNED',writable=true "
                       'WHERE range_start=1501 AND range_end=2000')


def mark_verifying(meta_dsn: str, job_id: UUID) -> None:
    with psycopg.connect(meta_dsn,row_factory=dict_row,connect_timeout=8) as db:
        with db.transaction():
            row=db.execute('SELECT phase FROM migration_jobs WHERE job_id=%s FOR UPDATE',(job_id,)).fetchone()
            if row and row['phase']=='MAP_ACTIVATED':
                db.execute("UPDATE migration_jobs SET phase='VERIFYING',updated_at=clock_timestamp() "
                           'WHERE job_id=%s',(job_id,))
                db.execute("INSERT INTO migration_recovery_events(job_id,phase,details) VALUES "
                           "(%s,'VERIFYING','{\"stage\":12,\"target_writable\":true}'::jsonb)",(job_id,))
            elif not row or row['phase']!='VERIFYING':
                raise CaptureError('unexpected target activation phase')


def cutover_once(meta_dsn: str, source_dsn: str, target_dsn: str, job_id: UUID) -> dict:
    """Idempotent state machine; session advisory lock excludes other managers."""
    with psycopg.connect(meta_dsn, autocommit=True, row_factory=dict_row, connect_timeout=8) as lock:
        got=lock.execute('SELECT pg_try_advisory_lock(hashtext(%s)) AS locked',
                         (f'stage12:{job_id}',)).fetchone()
        if not got or not got['locked']:
            raise CaptureError('another migration worker owns the cutover lease')
        try:
            job=_job(meta_dsn,job_id)
            if job['phase'] in ('PLANNED','SNAPSHOTTING','TARGET_READY'):
                raise CaptureError('complete stage11 catch-up first')
            if job['phase']=='CATCHING_UP':
                begin_fencing(meta_dsn,job_id)
                crash_if_armed(job_id, "FENCING_GATE_CLOSED")
                job=_job(meta_dsn,job_id)
            if job['phase']=='FENCING':
                fence_source(source_dsn,job['epoch_before'])
                require_drained(meta_dsn,source_dsn)
                advance_final_replay(meta_dsn,job_id)
                job=_job(meta_dsn,job_id)
            if job['phase']=='FINAL_REPLAY':
                final_sync(source_dsn,target_dsn,meta_dsn,job_id)
                stage_target_readonly(target_dsn,job_id,job['epoch_after'])
                # Source must remain fenced; verify again before publishing map.
                if not _source_fenced(source_dsn):
                    raise CaptureError('B unfenced unexpectedly; refuse map activation')
                require_drained(meta_dsn,source_dsn)
                activate_map(meta_dsn,job_id)
                crash_if_armed(job_id, "MAP_ACTIVATED")
                job=_job(meta_dsn,job_id)
            if job['phase']=='MAP_ACTIVATED':
                if not _source_fenced(source_dsn):
                    raise CaptureError('B unexpectedly writable AFTER cutover; manual investigation needed')
                activate_target(target_dsn,job['epoch_after'])
                mark_verifying(meta_dsn,job_id)
            job=_job(meta_dsn,job_id)
            if job['phase']!='VERIFYING':
                raise CaptureError(f'cutover not yet active: {job["phase"]}')
            return {'job_id':str(job_id),'phase':job['phase'],'epoch_before':job['epoch_before'],
                    'epoch_after':job['epoch_after'],'active_owner':'C','old_shard_fenced':True,
                    'completion_pending_stage13_14':True}
        finally:
            lock.execute('SELECT pg_advisory_unlock(hashtext(%s))', (f'stage12:{job_id}',))
