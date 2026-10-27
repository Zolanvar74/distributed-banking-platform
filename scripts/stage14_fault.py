"""Stage 14 controlled fault administration, always run in schema-migrator.

NEVER arm outside an isolated disposable Compose project. Preflight/verify
are read-only. Arming is an explicit administrative action, not a regular test.
"""
from __future__ import annotations

import argparse
import os
from uuid import UUID, uuid4

import httpx
import psycopg
from psycopg.rows import dict_row

from shared.stage14_faults import POINTS

META = os.environ['METADATA_DATABASE_URL']
B = os.environ['SHARD_B_DATABASE_URL']
C = os.environ['SHARD_C_DATABASE_URL']
MANAGER = 'http://migration-manager:8000'
HEADERS = {'X-Internal-Token': os.environ['INTERNAL_2PC_TOKEN']}


def one(dsn, sql, params=()):
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=8) as db:
        return db.execute(sql, params).fetchone()


def latest_job():
    job = one(META, "SELECT * FROM migration_jobs WHERE source_shard='B' "
              "AND target_shard='C' AND range_start=1501 AND range_end=2000 "
              "ORDER BY created_at DESC LIMIT 1")
    if not job:
        raise RuntimeError('create Stage10 migration job first: --action plan')
    return job


def plan():
    with httpx.Client(timeout=20, trust_env=False) as client:
        r = client.post(MANAGER + '/internal/migrations/stage10/plan', headers=HEADERS)
        r.raise_for_status()
        print('MIGRATION JOB:', r.json(), flush=True)


def arm(point):
    if point not in POINTS:
        raise ValueError('unknown point')
    if os.environ.get('STAGE14_ENABLE_FAULTS') != '1':
        raise RuntimeError('admin container needs STAGE14_ENABLE_FAULTS=1; explicitly pass -e STAGE14_ENABLE_FAULTS=1')
    job = latest_job()
    allowed = {'SNAPSHOT_PERSISTED': {'PLANNED'},
               'DELTA_GROUP_APPLIED': {'TARGET_READY', 'CATCHING_UP'},
               'FENCING_GATE_CLOSED': {'CATCHING_UP'},
               'MAP_ACTIVATED': {'CATCHING_UP'}}
    if job['phase'] not in allowed[point]:
        raise RuntimeError(f'not safe to arm {point} during {job["phase"]}; expected {allowed[point]}')
    if point == 'DELTA_GROUP_APPLIED':
        from shared.migration_target import source_tail, target_rows
        live, groups, _ = source_tail(B, job['job_id'])
        shadow = target_rows(C, job['job_id'])
        if not shadow or not any(any(ch['version'] > shadow[ch['account_id']]['version']
                                     for ch in members) for members in groups.values()):
            raise RuntimeError('no un-replayed change; first stop manager and create a source write on B')
    if point in ('FENCING_GATE_CLOSED', 'MAP_ACTIVATED'):
        from shared.migration_cutover import _unresolved
        if _unresolved(META) > 0:
            raise RuntimeError('unresolved migration 2PC: do NOT run a fault cutover')
    experiment_id = uuid4()
    baseline=one(C, 'SELECT COUNT(*)::integer AS n FROM migration_target_applied_groups WHERE job_id=%s',
                 (job['job_id'],))['n'] if point=='DELTA_GROUP_APPLIED' else 0
    with psycopg.connect(META, row_factory=dict_row, connect_timeout=8) as db:
        with db.transaction():
            db.execute('INSERT INTO stage14_fault_experiments(experiment_id,job_id,point,baseline_applied_groups) '
                       'VALUES (%s,%s,%s,%s)', (experiment_id, job['job_id'], point, baseline))
    print(f'ARMED experiment={experiment_id} job={job["job_id"]} point={point}', flush=True)
    print('ONE-SHOT process fault; never attempt in production or on your main database.', flush=True)


def trigger(point):
    job = latest_job()
    endpoint = ('snapshot' if point == 'SNAPSHOT_PERSISTED' else
                'stage11/replay' if point == 'DELTA_GROUP_APPLIED' else 'stage12/cutover')
    print(f'TRIGGER {point} job={job["job_id"]} endpoint={endpoint}', flush=True)
    try:
        with httpx.Client(timeout=45, trust_env=False) as client:
            r = client.post(f'{MANAGER}/internal/migrations/{job["job_id"]}/{endpoint}',headers=HEADERS)
            print('HTTP:', r.status_code, r.text[:500], flush=True)
    except (httpx.RequestError, httpx.TimeoutException) as exc:
        print('EXPECTED CONNECTION INTERRUPTION (verify durable FIRED state next):',type(exc).__name__,flush=True)


def inspect():
    job = latest_job()
    print('JOB', {k: str(job[k]) for k in ('job_id','phase','snapshot_id','epoch_before','epoch_after')}, flush=True)
    with psycopg.connect(META, row_factory=dict_row, connect_timeout=8) as db:
        for row in db.execute('SELECT experiment_id,point,state,fired_at '
                              'FROM stage14_fault_experiments WHERE job_id=%s ORDER BY created_at',(job['job_id'],)):
            print('FAULT', dict(row), flush=True)


def verify(point, moment):
    job = latest_job()
    e = one(META, "SELECT * FROM stage14_fault_experiments WHERE job_id=%s AND point=%s "
                "ORDER BY created_at DESC LIMIT 1", (job['job_id'], point))
    assert e and e['state']=='FIRED' and e['fired_at'], f'fault not triggered: {e}'
    fired_phase = {'SNAPSHOT_PERSISTED': 'PLANNED',
                   'FENCING_GATE_CLOSED': 'FENCING',
                   'MAP_ACTIVATED': 'MAP_ACTIVATED'}
    if point == 'SNAPSHOT_PERSISTED':
        batch = one(B, 'SELECT row_count,snapshot_digest FROM migration_snapshot_batches WHERE job_id=%s', (job['job_id'],))
        assert batch and batch['row_count'] > 0 and len(batch['snapshot_digest']) == 64, batch
        if moment == 'crashed':
            assert job['phase'] == 'PLANNED', job['phase']
        else:
            assert job['snapshot_id'] == str(job['job_id']) and job['phase'] != 'PLANNED', job['phase']
    elif point == 'DELTA_GROUP_APPLIED':
        applied = one(C, 'SELECT COUNT(*)::int AS n FROM migration_target_applied_groups WHERE job_id=%s', (job['job_id'],))
        assert applied and applied['n'] > e['baseline_applied_groups'], (applied,e['baseline_applied_groups'])
        if moment == 'recovered':
            from shared.migration_target import source_tail, target_rows
            live, _, _ = source_tail(B, job['job_id'])
            shadow=target_rows(C, job['job_id'])
            assert set(live)==set(shadow)
            assert all((r['balance'],r['version'])==(shadow[k]['balance'],shadow[k]['version']) for k,r in live.items())
    elif point in ('FENCING_GATE_CLOSED','MAP_ACTIVATED'):
        if moment == 'crashed':
            assert job['phase'] == fired_phase[point], (point,job['phase'])
        else:
            assert job['phase'] in ('VERIFYING','COMPLETED'), job['phase']
        if point=='FENCING_GATE_CLOSED' and moment=='crashed':
            owner=one(META,'SELECT shard_id FROM shard_map WHERE active=true AND range_start<=2000 AND range_end>=2000')
            assert owner['shard_id']=='B',owner
        if point=='MAP_ACTIVATED':
            owner=one(META,'SELECT shard_id FROM shard_map WHERE active=true AND range_start<=2000 AND range_end>=2000')
            assert owner['shard_id']=='C',owner
            b_fence=one(B,'SELECT writable FROM ownership_fences WHERE range_start=1501 AND range_end=2000')
            c_fence=one(C,'SELECT writable FROM ownership_fences WHERE range_start=1501 AND range_end=2000')
            assert b_fence and not b_fence['writable']
            assert c_fence and (c_fence['writable'] == (moment=='recovered')),c_fence
    print(f'STAGE 14 {point} {moment.upper()}: ALL CHECKS PASSED', flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--action',required=True,choices=['plan','arm','trigger','inspect','verify'])
    parser.add_argument('--point', choices=sorted(POINTS))
    parser.add_argument('--moment', choices=['crashed','recovered'])
    a=parser.parse_args()
    if a.action in ('arm','trigger','verify') and not a.point:
        parser.error('--point required')
    if a.action=='verify' and not a.moment:
        parser.error('--moment required')
    {'plan':lambda:plan(),'arm':lambda:arm(a.point),'trigger':lambda:trigger(a.point),
     'inspect':inspect,'verify':lambda:verify(a.point,a.moment)}[a.action]()
