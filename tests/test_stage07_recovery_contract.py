"""Pure-contract unit tests: no running databases or Docker required."""
from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from shared import durable_status, stage07_faults


class DummyCursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.row


class DummyConnection:
    def __init__(self, tx, parts):
        self.tx, self.parts = tx, parts
        self.executed = []
        self.committed = False

    def execute(self, query, args=None):
        self.executed.append((query, args))
        if 'FROM transactions WHERE' in query:
            return DummyCursor(self.tx)
        if 'FROM transaction_participants' in query:
            return DummyCursor(self.parts)
        return DummyCursor(None)

    @contextmanager
    def transaction(self):
        yield

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.committed = True


def test_status_pending_even_when_commit_decided_but_partially_acked(monkeypatch):
    txid = uuid4()
    tx = {'state': 'COMMIT_DECIDED', 'decision': 'COMMIT', 'client_request_id': 'req',
          'from_account': 'A0500', 'to_account': 'A1500', 'amount': '1.00'}
    parts = [{'shard_id': 'A', 'vote': 'YES', 'completion_ack': True},
             {'shard_id': 'B', 'vote': 'YES', 'completion_ack': False}]
    c = DummyConnection(tx, parts)
    monkeypatch.setattr(durable_status.psycopg, 'connect', lambda *a, **k: c)
    code, payload = durable_status.status_for_tx('dsn', txid)
    assert code == 202
    assert payload['status'] == 'PENDING'
    assert payload['decision'] == 'COMMIT'
    assert [p['ack'] for p in payload['participants']] == [True, False]


def test_status_committed_only_after_all_acks(monkeypatch):
    txid = uuid4()
    tx = {'state': 'COMPLETED', 'decision': 'COMMIT', 'client_request_id': 'req',
          'from_account': 'A0500', 'to_account': 'A1500', 'amount': '1.00'}
    parts = [{'shard_id': 'A', 'vote': 'YES', 'completion_ack': True},
             {'shard_id': 'B', 'vote': 'YES', 'completion_ack': True}]
    monkeypatch.setattr(durable_status.psycopg, 'connect', lambda *a, **k: DummyConnection(tx, parts))
    code, payload = durable_status.status_for_tx('dsn', txid)
    assert code == 200 and payload['status'] == 'COMMITTED'
    parts[-1]['completion_ack'] = False
    code, payload = durable_status.status_for_tx('dsn', txid)
    assert code == 202 and payload['status'] == 'PENDING'


def test_fault_injector_off_by_default(monkeypatch):
    monkeypatch.delenv('STAGE07_ENABLE_FAULTS', raising=False)
    monkeypatch.setattr(stage07_faults.psycopg, 'connect', lambda *a, **k: (_ for _ in ()).throw(AssertionError('unexpected database read')))
    stage07_faults.after_first_commit_ack('dsn', uuid4())


def test_fault_injector_durable_one_shot(monkeypatch):
    monkeypatch.setenv('STAGE07_ENABLE_FAULTS', '1')
    class CrashInjected(Exception):
        pass

    conn = DummyConnection(None, None)
    calls = []
    def execute(query, args=None):
        calls.append(query)
        if 'UPDATE stage07_fault_injections' in query:
            return DummyCursor(('stage07-crash-id',))
        return DummyCursor(None)
    conn.execute = execute
    monkeypatch.setattr(stage07_faults.psycopg, 'connect', lambda *a, **k: conn)
    def crash(code):
        assert conn.committed is True, 'fault must be durable before process exit'
        assert code == 74
        raise CrashInjected
    monkeypatch.setattr(stage07_faults.os, '_exit', crash)
    with pytest.raises(CrashInjected):
        stage07_faults.after_first_commit_ack('dsn', uuid4())
    assert any('STAGE07_CRASH_AFTER_ONE_ACK' in q for q in calls)
