"""Real-DB/HTTP Stage 06 acceptance; use with running Docker Compose.

Balances are restored on *successful* completion by a real reverse transfer.
If interrupted, do NOT reset balances manually: inspect request status first.
"""
from __future__ import annotations

import os
from uuid import uuid4
from decimal import Decimal

import httpx
import psycopg

BASE = 'http://gateway:8000'


def verify():
    with httpx.Client(timeout=40, trust_env=False) as client:
        a = client.get(BASE + '/accounts/A0500/balance')
        b = client.get(BASE + '/accounts/A1500/balance')
        assert a.status_code == b.status_code == 200, (a.text, b.text)
        a0, b0 = Decimal(a.json()['balance']), Decimal(b.json()['balance'])
        assert a0 >= Decimal('2.30'), 'need A0500 balance >= 2.30 for the test'
        for env in ('SHARD_A_DATABASE_URL', 'SHARD_B_DATABASE_URL'):
            with psycopg.connect(os.environ[env]) as conn:
                assert int(conn.execute('SHOW max_prepared_transactions').fetchone()[0]) > 0
        with psycopg.connect(os.environ['METADATA_DATABASE_URL']) as conn:
            assert conn.execute("SELECT 1 FROM schema_migrations WHERE version='003_stage06_2pc.sql'").fetchone()
        print('PASS metadata migration and PostgreSQL prepared-transaction configuration', flush=True)

        request_id = 'stage06-' + str(uuid4())
        payload = {'from_account': 'A0500', 'to_account': 'A1500',
                   'amount': '2.30', 'request_id': request_id}
        r = client.post(BASE + '/transfers', json=payload)
        assert r.status_code == 200, (r.status_code, r.text)
        data = r.json()
        assert data['status'] == 'COMMITTED' and data['decision'] == 'COMMIT', data
        assert data['state'] == 'COMPLETED', data
        assert len(data['participants']) == 2 and all(p['ack'] and p['vote']=='YES' for p in data['participants']), data
        txid = data['tx_id']
        assert Decimal(client.get(BASE + '/accounts/A0500/balance').json()['balance']) == a0 - Decimal('2.30')
        assert Decimal(client.get(BASE + '/accounts/A1500/balance').json()['balance']) == b0 + Decimal('2.30')
        print('PASS cross-shard COMMIT only after two acknowledged participants', flush=True)

        payload['amount'] = '2.3'
        again = client.post(BASE + '/transfers', json=payload)
        assert again.status_code == 200 and again.json()['tx_id'] == txid, again.text
        mismatch = client.post(BASE + '/transfers', json={**payload, 'amount': '3.00'})
        assert mismatch.status_code == 409, mismatch.text
        status = client.get(BASE + '/transactions/' + txid)
        by_request = client.get(BASE + '/transfers/requests/' + request_id)
        assert status.status_code == by_request.status_code == 200
        assert status.json()['tx_id'] == by_request.json()['tx_id'] == txid
        assert Decimal(client.get(BASE + '/accounts/A0500/balance').json()['balance']) == a0 - Decimal('2.30')
        print('PASS request-id replay, payload conflict, and durable status lookup', flush=True)

        # Proven insufficient funds, so B must vote NO and the A debit must
        # never become committed. No effects are visible on either shard.
        reject_id = 'stage06-reject-' + str(uuid4())
        # A is processed first (CREDIT) and votes YES; B then votes NO
        # because the requested debit exceeds B's current balance.
        impossible_debit = b0 + Decimal('3.30')
        assert a0 - Decimal('2.30') + impossible_debit <= Decimal('999999999999999999.99')
        reject = client.post(BASE + '/transfers', json={
            'from_account': 'A1500', 'to_account': 'A0500',
            'amount': f'{impossible_debit:.2f}', 'request_id': reject_id,
        })
        assert reject.status_code == 409, (reject.status_code, reject.text)
        result = reject.json()
        assert result['status'] == 'ABORTED' and result['decision'] == 'ABORT', result
        assert all(p['ack'] for p in result['participants']), result
        assert {p['shard']:p['vote'] for p in result['participants']} == {'A':'YES', 'B':'NO'}, result
        assert Decimal(client.get(BASE + '/accounts/A0500/balance').json()['balance']) == a0 - Decimal('2.30')
        assert Decimal(client.get(BASE + '/accounts/A1500/balance').json()['balance']) == b0 + Decimal('2.30')
        print('PASS definitive participant NO leads to durable ABORT and zero effects', flush=True)

        # Retrying phase-2 COMMIT on already committed participants is harmless.
        token = os.environ['INTERNAL_2PC_TOKEN']
        for shard in ('A', 'B'):
            r = client.post(f'http://shard-{shard.lower()}-1:8000/internal/2pc/decision',
                json={'tx_id': txid, 'shard_id': shard, 'decision': 'COMMIT'},
                headers={'X-Internal-Token': token})
            assert r.status_code == 200 and r.json()['outcome'] == 'COMMITTED' and r.json()['replayed'] is True, r.text
        print('PASS repeated COMMIT safely acknowledged without duplicate effects', flush=True)

        # Compensating reverse uses a DIFFERENT cross-shard transaction.
        reverse_id = 'stage06-reverse-' + str(uuid4())
        reverse = client.post(BASE + '/transfers', json={
            'from_account': 'A1500', 'to_account': 'A0500',
            'amount': '2.30', 'request_id': reverse_id,
        })
        assert reverse.status_code == 200 and reverse.json()['status'] == 'COMMITTED', reverse.text
        assert Decimal(client.get(BASE + '/accounts/A0500/balance').json()['balance']) == a0
        assert Decimal(client.get(BASE + '/accounts/A1500/balance').json()['balance']) == b0
        for shard in ('A','B'):
            with psycopg.connect(os.environ[f'SHARD_{shard}_DATABASE_URL']) as conn:
                left = conn.execute(
                    "SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid LIKE 'bank2pc:%' AND database=current_database()"
                ).fetchone()[0]
                assert left == 0, f'shard {shard}: {left} unresolved prepared transactions'
        print('PASS reverse transfer restores balances; no lingering prepared GIDs', flush=True)
        print('STAGE 06 TWO-PHASE COMMIT CHECK: ALL CHECKS PASSED', flush=True)


if __name__ == '__main__':
    verify()
