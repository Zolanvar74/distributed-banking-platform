"""Final non-mutating post-cutover acceptance audit against live PostgreSQL.

Does NOT run old phase-specific tests or create new transactions. To establish
money conservation under concurrent load, also run the paired transfer tests
from Stage 13 and record their before/after evidence.
"""
from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal

import httpx
import psycopg
from psycopg.rows import dict_row

META = os.environ['METADATA_DATABASE_URL']
DBS = {s: os.environ[f'SHARD_{s}_DATABASE_URL'] for s in 'ABC'}
GATEWAY = 'http://gateway:8000'


def rows(dsn, query, args=()):
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=8) as db:
        return (db.execute(query, args) if args else db.execute(query)).fetchall()


def exactly_one(dsn, query, args=()):
    data=rows(dsn,query,args)
    assert len(data)==1,(query,data)
    return data[0]


def audit():
    mapping=rows(META,'SELECT range_start,range_end,shard_id,epoch FROM shard_map '
                      'WHERE active=true ORDER BY range_start')
    intervals=[(r['range_start'],r['range_end'],r['shard_id']) for r in mapping]
    assert intervals==[(1,1000,'A'),(1001,1500,'B'),(1501,2000,'C')],intervals
    epoch=exactly_one(META,'SELECT epoch FROM routing_epoch WHERE singleton=1')['epoch']
    job=exactly_one(META,'SELECT job_id,phase,epoch_before,epoch_after,cutover_decided_at '
                         'FROM migration_jobs WHERE source_shard=\'B\' AND target_shard=\'C\' '
                         'AND range_start=1501 AND range_end=2000 '
                         'ORDER BY created_at DESC LIMIT 1')
    assert job['phase'] in ('VERIFYING','COMPLETED') and job['cutover_decided_at'],job
    assert job['epoch_after']==epoch and epoch>job['epoch_before'],job
    bf=exactly_one(DBS['B'],'SELECT writable,phase,epoch FROM ownership_fences '
                         'WHERE range_start=1501 AND range_end=2000')
    cf=exactly_one(DBS['C'],'SELECT writable,phase,epoch FROM ownership_fences '
                         'WHERE range_start=1501 AND range_end=2000')
    assert not bf['writable'] and bf['phase']=='FENCED',bf
    assert cf['writable'] and cf['phase']=='OWNED' and cf['epoch']==epoch,cf
    # Do not confuse retained old B rows with active money: sum active shard rows only.
    active_rows={}
    for start,end,shard in intervals:
        dbrows=rows(DBS[shard],
            'SELECT account_id,balance,version FROM accounts '
            'WHERE substring(account_id from 2)::integer BETWEEN %s AND %s',
            (start,end))
        assert dbrows,f'{shard}: no authoritative accounts in {start}-{end}'
        for account in dbrows:
            k=account['account_id']
            assert k not in active_rows and start<=int(k[1:])<=end,k
            assert account['balance']>=0 and account['version']>=0,account
            active_rows[k]=(shard,account['balance'],account['version'])
    # Confirms transferred range has no missing source accounts and B stayed unchanged
    # in ownership, but does not incorrectly count stale B balances as money.
    old_accounts=rows(DBS['B'],'SELECT account_id FROM accounts '
                         'WHERE substring(account_id from 2)::integer BETWEEN 1501 AND 2000')
    assert {r['account_id'] for r in old_accounts}=={
        a for a,v in active_rows.items() if v[0]=='C'}, 'migrating account set mismatch'
    in_flight=exactly_one(META,
        "SELECT COUNT(*)::int AS n FROM transactions t "
        "JOIN transaction_participants p ON p.tx_id=t.tx_id "
        "WHERE t.state<>'COMPLETED' AND p.account_id ~ '^A[0-9]{4}$' "
        "AND substring(p.account_id from 2)::integer BETWEEN 1501 AND 2000")['n']
    assert in_flight==0,f'{in_flight} unresolved migration-related 2PC transfers'
    prepared={}
    for shard in 'ABC':
        prepared[shard]=exactly_one(DBS[shard],
            "SELECT COUNT(*)::int AS n FROM pg_prepared_xacts "
            "WHERE database=current_database() AND gid LIKE %s",
            ("bank2pc:%",))['n']
    assert all(n==0 for n in prepared.values()), f'unresolved prepared xacts: {prepared}'
    discover={}
    with httpx.Client(timeout=12,trust_env=False) as client:
        for shard in 'ABC':
            r=client.get(f'{GATEWAY}/debug/replicas/{shard}')
            assert r.status_code==200,(shard,r.status_code,r.text[:250])
            discover[shard]=r.json()
            assert set(discover[shard]['healthy'])=={shard+'1',shard+'2'},discover[shard]
        for aid,shard in [('A0500','A'),('A1500','B'),('A2000','C')]:
            r=client.get(f'{GATEWAY}/accounts/{aid}/balance')
            assert r.status_code==200 and r.json().get('shard')==shard,(aid,r.status_code,r.text[:250])
    total=sum((v[1] for v in active_rows.values()),Decimal('0.00'))
    report={'result':'PASS','stage':14,'migration_job':str(job['job_id']),
            'migration_phase':job['phase'],'routing_epoch':epoch,
            'active_ranges':intervals,'authoritative_account_count':len(active_rows),
            'authoritative_total_snapshot':str(total),
            'prepared_transactions':prepared,
            'migration_2pc_in_flight':in_flight,
            'healthy_replicas':{s:discover[s]['healthy'] for s in 'ABC'},
            'note':'A point-in-time sum is not by itself a proof of conservation; run stage13 paired transfer/concurrency tests.'}
    print('PASS map B retained range / C migrated range, epoch, cutover decision')
    print('PASS B immutable fence; C writable; migrated accounts exist only at active C owner')
    print('PASS no unresolved 2PC participants; six healthy replicas; Gateway routes to A/B/C')
    print('STAGE 14 READ-ONLY POST-CUTOVER AUDIT: ALL CHECKS PASSED')
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--scenario',required=True,choices=['postcutover'])
    parser.add_argument('--json',action='store_true')
    a=parser.parse_args()
    report=audit()
    if a.json:
        print('STAGE14_EVIDENCE_JSON='+json.dumps(report,ensure_ascii=False,default=str,sort_keys=True))
