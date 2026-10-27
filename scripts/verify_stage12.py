"""Stage 12 integration verification. Run from schema-migrator in Docker.

Scenarios: preflight (read-only); cutover (one-way admin action);
           stale (prove stale writes rejected; new owner does actual safe writes);
           recovered (verify durable map and fences after manager restart).
Never run preflight or stage11 tests after a completed cutover.
"""
from __future__ import annotations

import argparse
import os
import time
from uuid import uuid4

import httpx
import psycopg
from psycopg.rows import dict_row

META=os.environ['METADATA_DATABASE_URL']
B=os.environ['SHARD_B_DATABASE_URL']
C=os.environ['SHARD_C_DATABASE_URL']
AUTH={'X-Internal-Token':os.environ['INTERNAL_2PC_TOKEN']}
MANAGER='http://migration-manager:8000'
GATEWAY='http://gateway:8000'


def row(dsn,sql,values=()):
    with psycopg.connect(dsn,row_factory=dict_row,connect_timeout=8) as db:
        return db.execute(sql,values).fetchone()


def latest_job():
    r=row(META,"SELECT job_id,phase,epoch_before,epoch_after FROM migration_jobs "
               "WHERE source_shard='B' AND target_shard='C' AND range_start=1501 AND range_end=2000 "
               'ORDER BY created_at DESC LIMIT 1')
    assert r, 'run stage11 bootstrap and online checks first'
    return r


def get(client,url,**kw):
    r=client.get(url,**kw)
    assert r.status_code==200,(r.status_code,r.text[:800])
    return r.json()


def preflight():
    j=latest_job()
    assert j['phase']=='CATCHING_UP',f'expected CATCHING_UP; got {j["phase"]}'
    with psycopg.connect(META,row_factory=dict_row,connect_timeout=8) as db:
        owner=db.execute('SELECT shard_id,epoch FROM shard_map WHERE active=true '
                         'AND range_start<=2000 AND range_end>=2000').fetchone()
        assert owner['shard_id']=='B',owner
        unresolved=db.execute("SELECT COUNT(*)::integer AS n FROM transactions t "
            "JOIN transaction_participants p ON p.tx_id=t.tx_id "
            "WHERE t.state<>'COMPLETED' AND p.account_id ~ '^A[0-9]{4}$' "
            "AND substring(p.account_id from 2)::integer BETWEEN 1501 AND 2000").fetchone()['n']
    assert unresolved==0,f'{unresolved} 2PC participants still pending; query status and recover first'
    shadow=row(C,'SELECT COUNT(*)::integer AS n FROM migration_target_accounts WHERE job_id=%s',(j['job_id'],))
    assert shadow['n']>0,'no target snapshot in C'
    f=row(B,'SELECT writable FROM ownership_fences WHERE range_start=1001 AND range_end=2000')
    assert f and f['writable'], 'B source fence unexpectedly changed'
    print(f'PASS preflight: B currently authoritative; shadow accounts={shadow["n"]}; no unresolved 2PC')
    print('STAGE 12 PREFLIGHT: ALL CHECKS PASSED')


def cutover():
    j=latest_job()
    print(f'CUTOVER JOB: {j["job_id"]} (one-way ownership switch)',flush=True)
    with httpx.Client(timeout=100,trust_env=False) as c:
        r=c.post(f'{MANAGER}/internal/migrations/{j["job_id"]}/stage12/cutover',headers=AUTH)
        if r.status_code!=200:
            print(f'CUTOVER RESPONSE {r.status_code}: {r.text[:700]}',flush=True)
            # A concurrently running durable background recovery worker may
            # own the lease. Give it time to finish rather than double-driving.
            for _ in range(12):
                now=latest_job()
                if now['phase']=='VERIFYING':
                    break
                time.sleep(2)
            else:
                raise AssertionError('cutover not completed; query unresolved 2PC / migration phase')
        else:
            answer=r.json()
            assert answer['phase']=='VERIFYING' and answer['active_owner']=='C',answer
    recovered()
    print('STAGE 12 CUTOVER: ALL CHECKS PASSED')


def _current_replica(client,shard):
    # Dynamic registry: do not assume the original Compose hostname survives
    # the self-healing replacement process.
    with psycopg.connect(META,row_factory=dict_row,connect_timeout=8) as db:
        replicas=db.execute("SELECT hostname,port,replica_id,generation FROM replica_registry "
                            "WHERE shard_id=%s AND lifecycle IN ('REGISTERED','HEALTHY')",(shard,)).fetchall()
    for r in replicas:
        url=f"http://{r['hostname']}:{r['port']}"
        try:
            h=client.get(url+'/health',timeout=2)
            if h.status_code==200 and h.json().get('replica')==r['replica_id'] and h.json().get('generation')==r['generation']:
                return url
        except httpx.RequestError:
            pass
    raise AssertionError(f'no healthy {shard} replicas discovered')


def recovered():
    j=latest_job()
    assert j['phase']=='VERIFYING',j
    e=j['epoch_after']
    with psycopg.connect(META,row_factory=dict_row,connect_timeout=8) as db:
        ranges=db.execute('SELECT shard_id,range_start,range_end,epoch FROM shard_map '
                          'WHERE active=true ORDER BY range_start').fetchall()
        routing=db.execute('SELECT epoch FROM routing_epoch WHERE singleton=1').fetchone()['epoch']
    assert [(r['shard_id'],r['range_start'],r['range_end']) for r in ranges]==[
        ('A',1,1000),('B',1001,1500),('C',1501,2000)],ranges
    assert routing==e and e>j['epoch_before'],(e,j)
    bf=row(B,'SELECT epoch,writable,phase FROM ownership_fences '
             'WHERE range_start=1501 AND range_end=2000')
    cf=row(C,'SELECT epoch,writable,phase FROM ownership_fences '
             'WHERE range_start=1501 AND range_end=2000')
    assert bf and not bf['writable'] and bf['phase']=='FENCED',bf
    assert cf and cf['writable'] and cf['epoch']==e and cf['phase']=='OWNED',cf
    with httpx.Client(timeout=15,trust_env=False) as c:
        balance=get(c,f'{GATEWAY}/accounts/A2000/balance')
        assert balance['shard']=='C',balance
        retained=get(c,f'{GATEWAY}/accounts/A1500/balance')
        assert retained['shard']=='B',retained
    print(f'PASS durable owner map C epoch={e}; B range fenced; C writable')
    print('PASS Gateway A2000->C and A1500->B')
    print('STAGE 12 RECOVERY CHECK: ALL CHECKS PASSED')


def stale():
    recovered()
    j=latest_job()
    old=j['epoch_before']
    new=j['epoch_after']
    source_before=row(B,"SELECT balance,version FROM accounts WHERE account_id='A2000'")
    target_before=row(C,"SELECT balance,version FROM accounts WHERE account_id='A2000'")
    with httpx.Client(timeout=20,trust_env=False) as c:
        b_url=_current_replica(c,'B')
        c_url=_current_replica(c,'C')
        # Attack old B directly, bypassing Gateway's current shard map.
        for path in ('deposit','withdraw'):
            bad=c.post(b_url+f'/internal/accounts/A2000/{path}',
                headers={'X-Expected-Shard':'B','X-Routing-Epoch':str(old)},
                json={'amount':'1.00','request_id':f'stage12-stale-b-{uuid4()}'})
            assert bad.status_code==409,(path,bad.status_code,bad.text[:600])
        old_c=c.post(c_url+'/internal/accounts/A2000/deposit',
            headers={'X-Expected-Shard':'C','X-Routing-Epoch':str(old)},
            json={'amount':'1.00','request_id':f'stage12-stale-c-{uuid4()}'})
        assert old_c.status_code==409,(old_c.status_code,old_c.text[:600])
        assert row(B,"SELECT balance,version FROM accounts WHERE account_id='A2000'")==source_before
        assert row(C,"SELECT balance,version FROM accounts WHERE account_id='A2000'")==target_before
        # Normal banking writes MUST go to C and update only C.
        dep_id=f'stage12-active-deposit-{uuid4()}'
        wdr_id=f'stage12-active-withdraw-{uuid4()}'
        print(f'NEW OWNER TEST REQUEST IDs: {dep_id} {wdr_id}',flush=True)
        dep=c.post(f'{GATEWAY}/accounts/A2000/deposit',
                   json={'amount':'1.00','request_id':dep_id})
        assert dep.status_code==200,(dep.status_code,dep.text[:900])
        assert dep.json().get('shard')=='C',dep.json()
        wdr=c.post(f'{GATEWAY}/accounts/A2000/withdraw',
                   json={'amount':'1.00','request_id':wdr_id})
        assert wdr.status_code==200,(wdr.status_code,wdr.text[:900])
        assert wdr.json().get('shard')=='C',wdr.json()
    target_after=row(C,"SELECT balance,version FROM accounts WHERE account_id='A2000'")
    assert target_after['balance']==target_before['balance']
    assert target_after['version']==target_before['version']+2,(target_before,target_after)
    assert row(B,"SELECT balance,version FROM accounts WHERE account_id='A2000'")==source_before
    print('PASS direct stale B and stale C epoch mutations rejected with no write effects')
    print('PASS fresh Gateway writes change C only; B balance and version unchanged')
    print('STAGE 12 STALE ROUTE FENCING: ALL CHECKS PASSED')


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--scenario',choices=('preflight','cutover','stale','recovered'),required=True)
    args=parser.parse_args()
    globals()[args.scenario]()
