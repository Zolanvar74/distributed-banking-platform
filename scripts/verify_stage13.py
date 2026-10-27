"""Stage 13 post-cutover integration tests. Run inside schema-migrator.

Scenarios:
  preflight : read-only verification of owner/fence/backfill.
  legacy    : prove a historical B request never executes on C twice.
  cross     : test A<->C 2PC and a duplicate / conflicting request ID.
  overlap   : concurrent A->C transfers overlapping both accounts.
  stale2pc  : old B must reject a direct PREPARE with pre-cutover epoch.

These tests write real money. No silent rollback on a failed/uncertain test:
keep printed request IDs and use GET status to recover before cleanup.
"""
from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from uuid import uuid4

import httpx
import psycopg
from psycopg.rows import dict_row

META = os.environ['METADATA_DATABASE_URL']
A = os.environ['SHARD_A_DATABASE_URL']
B = os.environ['SHARD_B_DATABASE_URL']
C = os.environ['SHARD_C_DATABASE_URL']
GATEWAY = 'http://gateway:8000'


def one(dsn, query, params=()):
    with psycopg.connect(dsn,row_factory=dict_row,connect_timeout=6) as db:
        return db.execute(query,params).fetchone()


def state(dsn, account):
    result=one(dsn, 'SELECT balance,version FROM accounts WHERE account_id=%s',(account,))
    assert result is not None,(dsn,account)
    return result


def preflight():
    with psycopg.connect(META,row_factory=dict_row) as db:
        job=db.execute("SELECT job_id,phase FROM migration_jobs WHERE source_shard='B' "
                       "AND target_shard='C' AND range_start=1501 AND range_end=2000 "
                       "ORDER BY created_at DESC LIMIT 1").fetchone()
        assert job and job['phase'] in ('VERIFYING','COMPLETED'),job
        marker=db.execute("SELECT row_count FROM migrated_request_backfill "
                          "WHERE job_id=%s AND state='COMPLETE'",(job['job_id'],)).fetchone()
        assert marker is not None, 'run python -m scripts.backfill_stage13 first'
        count=db.execute("SELECT COUNT(*) AS n FROM migrated_legacy_requests").fetchone()['n']
        assert count >= marker['row_count'],(count,marker)
        ownership=db.execute("SELECT shard_id,epoch FROM shard_map WHERE 2000 BETWEEN range_start "
                             "AND range_end AND active=true").fetchone()
        assert ownership['shard_id']=='C',ownership
    fence_b=one(B,'SELECT writable FROM ownership_fences WHERE range_start=1501 AND range_end=2000')
    fence_c=one(C,'SELECT writable,epoch FROM ownership_fences WHERE range_start=1501 AND range_end=2000')
    assert fence_b and not fence_b['writable'] and fence_c and fence_c['writable']
    assert fence_c['epoch']==ownership['epoch'],(fence_c,ownership)
    print(f'PASS B read-only, C owns epoch {ownership["epoch"]}; archived {count} old B request IDs')
    print('STAGE 13 PREFLIGHT: ALL CHECKS PASSED')


def legacy():
    preflight()
    # A historical B success from the stage11 A2000 online test, if present.
    with psycopg.connect(META,row_factory=dict_row) as db:
        rows=db.execute("SELECT request_id,status,response FROM migrated_legacy_requests "
                        "WHERE status='COMMITTED' AND response IS NOT NULL "
                        "ORDER BY source_updated_at DESC LIMIT 2000").fetchall()
    entry=None
    for r in rows:
        reply=r['response']
        body=reply.get('body',{}) if isinstance(reply,dict) else {}
        if (body.get('account_id')=='A2000'
                and body.get('kind') in ('DEPOSIT','WITHDRAW','SAME_SHARD_TRANSFER')
                and body.get('amount')):
            entry=(r,body)
            break
    assert entry is not None, ('no historical A2000 success found in B request archive; '
                               'stage11 online test should run before cutover')
    recorded, body=entry
    url=(f'{GATEWAY}/transfers' if body['kind']=='SAME_SHARD_TRANSFER' else
         f'{GATEWAY}/accounts/{body["account_id"]}/{body["kind"].lower()}')
    payload={'amount':body['amount'],'request_id':recorded['request_id']}
    if body['kind']=='SAME_SHARD_TRANSFER':
        payload.update(from_account=body['account_id'],to_account=body['to_account'])
    before_b=state(B,'A2000');before_c=state(C,'A2000')
    with httpx.Client(timeout=30,trust_env=False) as client:
        result=client.post(url,json=payload)
    assert result.status_code==recorded['response']['http_status'],(result.status_code,result.text)
    assert result.json()==recorded['response']['body'],(result.json(),recorded['response'])
    assert state(B,'A2000')==before_b and state(C,'A2000')==before_c
    if body['kind'] != 'SAME_SHARD_TRANSFER':
        with httpx.Client(timeout=15,trust_env=False) as client:
            old_status=client.get(f'{GATEWAY}/accounts/A2000/operations/{recorded["request_id"]}')
        assert old_status.status_code==200,old_status.text
        assert old_status.json()['status']=='COMMITTED'
    print('PASS exact pre-cutover request_id replay returned old outcome with no new effects')
    print('STAGE 13 LEGACY RETRY: ALL CHECKS PASSED')


def await_committed(client, request_id, limit_seconds=100):
    end=time.monotonic()+limit_seconds
    while time.monotonic()<end:
        resp=client.get(f'{GATEWAY}/transfers/requests/{request_id}')
        if resp.status_code==200 and resp.json().get('status')=='COMMITTED':
            return resp.json()
        if resp.status_code==409 and resp.json().get('status')=='ABORTED':
            raise AssertionError(f'transfer aborted; request_id={request_id}: {resp.text}')
        time.sleep(2)
    raise AssertionError(f'transfer still unknown; preserve request_id and recover: {request_id}')


def run_transfer(client,source,target,amount='1.00',request_id=None):
    request_id=request_id or f'stage13-{uuid4()}'
    body={'from_account':source,'to_account':target,'amount':amount,'request_id':request_id}
    print(f'TRANSFER REQUEST ID: {request_id} ({source}->{target})',flush=True)
    resp=client.post(f'{GATEWAY}/transfers',json=body)
    assert resp.status_code in (200,202,503),(resp.status_code,resp.text[:900])
    committed=await_committed(client,request_id)
    return body,committed


def cross():
    preflight()
    start_a=state(A,'A0500');start_c=state(C,'A2000');start_b=state(B,'A2000')
    with httpx.Client(timeout=40,trust_env=False) as client:
        payload,done=run_transfer(client,'A0500','A2000')
        again=client.post(f'{GATEWAY}/transfers',json=payload)
        assert again.status_code==200 and again.json().get('tx_id')==done['tx_id'],again.text
        conflict={**payload,'amount':'2.00'}
        bad=client.post(f'{GATEWAY}/transfers',json=conflict)
        assert bad.status_code==409,(bad.status_code,bad.text)
        end_a=state(A,'A0500');end_c=state(C,'A2000')
        assert end_a['balance']==start_a['balance']-Decimal('1.00')
        assert end_c['balance']==start_c['balance']+Decimal('1.00')
        assert end_a['version']==start_a['version']+1
        assert end_c['version']==start_c['version']+1
        assert state(B,'A2000')==start_b
        assert end_a['balance']+end_c['balance']==start_a['balance']+start_c['balance']
        # Compensating bank transfer uses its OWN unique request ID; NEVER
        # directly adjust balances in SQL after an uncertain transfer.
        run_transfer(client,'A2000','A0500')
    assert state(A,'A0500')['balance']==start_a['balance']
    assert state(C,'A2000')['balance']==start_c['balance']
    assert state(B,'A2000')==start_b
    print('PASS A<->C 2PC, duplicate request ID, payload conflict and money conservation')
    print('STAGE 13 POST-CUTOVER 2PC: ALL CHECKS PASSED')


def stale2pc():
    preflight()
    from uuid import uuid4
    from shared.participant_2pc import finish
    txid = uuid4()
    before=state(B,'A2000')
    previous=one(META,"SELECT epoch_before FROM migration_jobs WHERE source_shard='B' "
                  "AND target_shard='C' ORDER BY created_at DESC LIMIT 1")['epoch_before']
    # Direct attack on the OLD shard bypasses Gateway. If a broken fence
    # unexpectedly prepares, ALWAYS roll it back to avoid stranded locks.
    with httpx.Client(timeout=30,trust_env=False) as client:
        with psycopg.connect(META,row_factory=dict_row) as db:
            candidates=db.execute("SELECT hostname,port FROM replica_registry "
                                  "WHERE shard_id='B' AND lifecycle IN ('REGISTERED','HEALTHY') "
                                  "ORDER BY replica_id").fetchall()
        assert candidates,'no B participant candidates'
        reply=None
        for r in candidates:
            try:
                host=f"http://{r['hostname']}:{r['port']}"
                check=client.get(host+'/health',timeout=2)
                if check.status_code!=200:
                    continue
                reply=client.post(host+'/internal/2pc/prepare',
                    headers={'X-Internal-Token':os.environ['INTERNAL_2PC_TOKEN']},
                    json={'tx_id':str(txid),'shard_id':'B','role':'CREDIT',
                          'account_id':'A2000','amount':'1.00','epoch':previous})
                break
            except httpx.RequestError:
                pass
        assert reply is not None,'no healthy B candidate for stale prepare probe'
    try:
        assert reply.status_code==200 and reply.json().get('vote')=='NO',(
            reply.status_code,reply.text)
    finally:
        # A participant that incorrectly prepares must never be left stranded.
        finish(B,'B',txid,'ABORT')
    assert state(B,'A2000')==before
    print('PASS stale direct PREPARE at old B was refused; balance/version unchanged')
    print('STAGE 13 STALE 2PC FENCING: ALL CHECKS PASSED')


def overlap():
    preflight()
    before_a=state(A,'A0500');before_c=state(C,'A2000');before_b=state(B,'A2000')
    if before_a['balance']<Decimal('4.00'):
        raise AssertionError('A0500 needs >=4.00 to run overlapping transfer test')
    ids=[f'stage13-overlap-{uuid4()}' for _ in range(4)]
    print('OVERLAP REQUEST IDs:',*ids,sep='\n',flush=True)
    def dispatch(rid):
        with httpx.Client(timeout=40,trust_env=False) as client:
            return client.post(f'{GATEWAY}/transfers',json={
                'from_account':'A0500','to_account':'A2000',
                'amount':'1.00','request_id':rid}).status_code
    with ThreadPoolExecutor(max_workers=4) as pool:
        codes=list(pool.map(dispatch,ids))
    assert all(code in (200,202,503) for code in codes),codes
    with httpx.Client(timeout=30,trust_env=False) as client:
        for rid in ids:
            await_committed(client,rid,limit_seconds=120)
        after_a=state(A,'A0500');after_c=state(C,'A2000')
        assert after_a['balance']==before_a['balance']-Decimal('4.00')
        assert after_c['balance']==before_c['balance']+Decimal('4.00')
        assert after_a['version']==before_a['version']+4
        assert after_c['version']==before_c['version']+4
        assert state(B,'A2000')==before_b
        for _ in ids:
            run_transfer(client,'A2000','A0500')
    assert state(A,'A0500')['balance']==before_a['balance']
    assert state(C,'A2000')['balance']==before_c['balance']
    assert state(B,'A2000')==before_b
    print('PASS 4 overlapping transactions, exactly-once effects, conserved money')
    print('STAGE 13 CONCURRENT 2PC: ALL CHECKS PASSED')


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--scenario',required=True,choices=('preflight','legacy','cross','overlap','stale2pc'))
    globals()[parser.parse_args().scenario]()
