"""Stage 07 real-database crash-recovery acceptance harness.

Run with the existing schema-migrator image, outside the Coordinator container.
The driver is deliberately phase-based: the Coordinator must be actually DEAD
between `crashed` and `recovered`. It never resets PostgreSQL volumes.
"""
from __future__ import annotations

import argparse
import os
import time
from decimal import Decimal
from uuid import UUID

import httpx
import psycopg
from psycopg.rows import dict_row

from shared.durable_status import status_for_request
from shared.two_pc import gid_for

META = os.environ['METADATA_DATABASE_URL']
SHARD_A = os.environ['SHARD_A_DATABASE_URL']
SHARD_B = os.environ['SHARD_B_DATABASE_URL']
GATEWAY = 'http://gateway:8000'
COORDINATOR = 'http://coordinator:8000'
AMOUNT = Decimal('1.00')


def require(condition: bool, description: str) -> None:
    if not condition:
        raise AssertionError(description)
    print('PASS ' + description, flush=True)


def first_balance(dsn: str, account: str) -> Decimal:
    with psycopg.connect(dsn) as conn:
        row = conn.execute('SELECT balance FROM accounts WHERE account_id=%s', (account,)).fetchone()
    if row is None:
        raise AssertionError('missing test account: ' + account)
    return row[0]


def pg_evidence(dsn: str, txid: UUID, shard: str) -> tuple[int, int, int]:
    with psycopg.connect(dsn) as conn:
        ledger = conn.execute('SELECT COUNT(*) FROM participant_commit_ledger WHERE tx_id=%s AND shard_id=%s',
                              (txid, shard)).fetchone()[0]
        prepared = conn.execute(
            'SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid=%s AND database=current_database()',
            (gid_for(txid, shard),),
        ).fetchone()[0]
        changes = conn.execute('SELECT COUNT(*) FROM account_changes WHERE change_group_id=%s',
                               (txid,)).fetchone()[0]
    return ledger, prepared, changes


def fault_row(request_id: str) -> dict:
    with psycopg.connect(META, row_factory=dict_row) as conn:
        row = conn.execute('SELECT * FROM stage07_fault_injections WHERE request_id=%s', (request_id,)).fetchone()
    assert row is not None, 'no armed fault for this request_id'
    return row


def baseline(request_id: str) -> tuple[dict, UUID]:
    record = fault_row(request_id)
    assert record['status'] == 'TRIPPED' and record['tx_id'] is not None, record
    return record, record['tx_id']


def arm(request_id: str):
    if not request_id.startswith('stage07-crash-') or len(request_id) > 190:
        raise AssertionError('request_id must start stage07-crash- and be <= 190 characters')
    with httpx.Client(timeout=3, trust_env=False) as client:
        r = client.get(COORDINATOR + '/health')
        require(r.status_code == 200 and r.json().get('fault_injection_enabled') is True,
                'Coordinator explicitly enabled for Stage 07 controlled crash')
        require(client.get(GATEWAY + '/health').status_code == 200,
                'Gateway is still reachable')
    with psycopg.connect(META) as conn:
        require(conn.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE version='004_stage07_crash_test.sql'"
        ).fetchone()[0] == 1, 'Stage 07 metadata migration applied')
        unfinished = conn.execute("SELECT COUNT(*) FROM transactions WHERE state <> 'COMPLETED'").fetchone()[0]
        require(unfinished == 0, 'no pre-existing unresolved 2PC transaction')
    for shard, dsn in [('A', SHARD_A), ('B', SHARD_B)]:
        with psycopg.connect(dsn) as conn:
            active = conn.execute(
                "SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid LIKE 'bank2pc:%' AND database=current_database()"
            ).fetchone()[0]
            require(active == 0, f'no pre-existing prepared GIDs in Shard {shard}')
    source = first_balance(SHARD_A, 'A0500')
    target = first_balance(SHARD_B, 'A1500')
    require(source >= AMOUNT, 'A0500 has sufficient balance to transfer 1.00')
    require(target + AMOUNT <= Decimal('999999999999999999.99'),
            'A1500 will not overflow numeric balance range')
    with psycopg.connect(META) as conn:
        conn.execute('''INSERT INTO stage07_fault_injections
            (request_id,from_account,to_account,amount,before_source,before_destination)
            VALUES (%s,'A0500','A1500',%s,%s,%s)''',
            (request_id, AMOUNT, source, target))
    print('STAGE 07 FAULT ARMED: ' + request_id, flush=True)
    print('NOW POST /transfers via Gateway; Coordinator should exit abruptly.', flush=True)


def crashed(request_id: str):
    record, txid = baseline(request_id)
    require(record['fired_at'] is not None, 'one-shot crash marker durably persisted')
    code, result = status_for_request(META, request_id)
    require(code == 202 and result['status'] == 'PENDING' and
            result['decision'] == 'COMMIT' and result['state'] == 'COMMIT_DECIDED',
            'durable COMMIT decision is PENDING, never reported as completed')
    participants = {p['shard']: p for p in result['participants']}
    require(set(participants) == {'A', 'B'} and
            participants['A']['ack'] is True and participants['B']['ack'] is False,
            'A has durable ACK and B has no ACK')
    require(all(p['vote'] == 'YES' for p in participants.values()),
            'both participants voted YES before decision')
    require(pg_evidence(SHARD_A, txid, 'A') == (1, 0, 1),
            'Shard A is committed exactly once')
    require(pg_evidence(SHARD_B, txid, 'B') == (0, 1, 0),
            'Shard B is durably PREPARED but not yet committed')
    require(first_balance(SHARD_A, 'A0500') == record['before_source'] - AMOUNT and
            first_balance(SHARD_B, 'A1500') == record['before_destination'],
            'intermediate balances match one committed and one prepared participant')
    with httpx.Client(timeout=5, trust_env=False) as client:
        r = client.get(GATEWAY + '/transfers/requests/' + request_id)
        require(r.status_code == 202 and r.json()['status'] == 'PENDING' and
                r.json()['tx_id'] == str(txid),
                'Gateway returns PENDING directly from metadata while Coordinator is down')
    print('STAGE 07 CRASH STATE: VERIFIED; SAFE TO RESTART COORDINATOR', flush=True)


def recovered(request_id: str):
    record, txid = baseline(request_id)
    deadline = time.monotonic() + 75
    code, result = status_for_request(META, request_id)
    while (code != 200 or result['status'] != 'COMMITTED') and time.monotonic() < deadline:
        time.sleep(2)
        code, result = status_for_request(META, request_id)
    require(code == 200 and result['state'] == 'COMPLETED' and
            result['decision'] == 'COMMIT' and result['status'] == 'COMMITTED',
            'Coordinator recovered original durable COMMIT decision')
    require(all(p['ack'] and p['vote'] == 'YES' for p in result['participants']),
            'both shard COMMIT acknowledgements are durably recorded')
    for shard, dsn in [('A', SHARD_A), ('B', SHARD_B)]:
        require(pg_evidence(dsn, txid, shard) == (1, 0, 1),
                f'Shard {shard} committed once, with no residual prepared GID')
    require(first_balance(SHARD_A, 'A0500') == record['before_source'] - AMOUNT and
            first_balance(SHARD_B, 'A1500') == record['before_destination'] + AMOUNT,
            'exactly-once debit and credit, total money conserved')
    payload = {'from_account': 'A0500', 'to_account': 'A1500',
               'amount': '1.00', 'request_id': request_id}
    with httpx.Client(timeout=30, trust_env=False) as client:
        again = client.post(GATEWAY + '/transfers', json=payload)
        require(again.status_code == 200 and again.json()['tx_id'] == str(txid),
                'retry SAME request_id returns original transaction (idempotent)')
        status = client.get(GATEWAY + '/transactions/' + str(txid))
        require(status.status_code == 200 and status.json()['status'] == 'COMMITTED',
                'transaction lookup by tx_id works after recovery')
    require(first_balance(SHARD_A, 'A0500') == record['before_source'] - AMOUNT and
            first_balance(SHARD_B, 'A1500') == record['before_destination'] + AMOUNT,
            'duplicate request did not move money a second time')
    with psycopg.connect(META) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM transaction_recovery_events WHERE tx_id=%s "
            "AND event_type='STAGE07_CRASH_AFTER_ONE_ACK'", (txid,),
        ).fetchone()[0]
        require(count == 1, 'injection happened exactly once and is audit-logged')
    print('STAGE 07 CRASH RECOVERY: ALL CHECKS PASSED', flush=True)


def cleanup(request_id: str):
    record, txid = baseline(request_id)
    code, result = status_for_request(META, request_id)
    assert code == 200 and result['status'] == 'COMMITTED', result
    a, b = first_balance(SHARD_A, 'A0500'), first_balance(SHARD_B, 'A1500')
    if a == record['before_source'] and b == record['before_destination']:
        print('STAGE 07 CLEANUP: balances already restored', flush=True)
        return
    require(a == record['before_source'] - AMOUNT and
            b == record['before_destination'] + AMOUNT,
            'balances match unchanged test state before reversing')
    reverse_id = 'stage07-reverse-' + str(txid)
    with httpx.Client(timeout=30, trust_env=False) as client:
        r = client.post(GATEWAY + '/transfers', json={
            'from_account': 'A1500', 'to_account': 'A0500',
            'amount': '1.00', 'request_id': reverse_id,
        })
        require(r.status_code == 200 and r.json()['status'] == 'COMMITTED',
                'new reverse 2PC transfer restores account balances')
    require(first_balance(SHARD_A, 'A0500') == record['before_source'] and
            first_balance(SHARD_B, 'A1500') == record['before_destination'],
            'balances back at pre-test values')
    print('STAGE 07 CLEANUP: ALL CHECKS PASSED', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=['arm', 'crashed', 'recovered', 'cleanup'], required=True)
    parser.add_argument('--request-id', required=True)
    args = parser.parse_args()
    globals()[args.scenario](args.request_id)


if __name__ == '__main__':
    main()
