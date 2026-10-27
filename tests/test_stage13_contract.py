"""No-database Stage 13 safety contracts."""
import pytest

from shared.banking_contract import fingerprint
from shared.migration_idempotency import touches_migrated_range
from shared.migration_idempotency import archive_guard
from services.gateway import main as gateway


def test_moving_boundary():
    assert not touches_migrated_range(['A1500'])
    assert touches_migrated_range(['A1501'])
    assert touches_migrated_range(['A2000', 'A0500'])
    assert not touches_migrated_range(['A0000', 'not-an-account'])


def test_legacy_fingerprint_is_original_same_shard():
    old=fingerprint('SAME_SHARD_TRANSFER','A1500','1.00','A2000')
    new=fingerprint('CROSS_SHARD_TRANSFER','A1500','1.00','A2000')
    assert old != new


def test_cross_lookup_respects_fingerprint(monkeypatch):
    # A cross-shard request must be routed by durable request ID even if the
    # Shard Map changed such that the current operation appears local.
    from uuid import uuid4
    txid=uuid4()
    monkeypatch.setenv('METADATA_DATABASE_URL','postgres://mock')
    class Conn:
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def execute(self,*args):
            assert 'FROM transactions' in args[0]
            return self
        def fetchone(self):
            return (txid,fingerprint('CROSS_SHARD_TRANSFER','A0500','2.00','A2000'))
    monkeypatch.setattr(gateway.psycopg,'connect',lambda *args,**kwargs:Conn())
    monkeypatch.setattr(gateway,'status_for_tx',lambda *args: (200,{'tx_id':str(txid),'status':'COMMITTED'}))
    from shared.banking_contract import TransferRequest
    body=TransferRequest(from_account='A0500',to_account='A2000',amount='2.00',request_id='old-id')
    old=gateway._previous_cross(body)
    assert old.status_code==200
    modified=TransferRequest(from_account='A0500',to_account='A2000',amount='3.00',request_id='old-id')
    bad=gateway._previous_cross(modified)
    assert bad.status_code==409


def test_archive_fail_closed_until_complete(monkeypatch):
    # Simulate metadata job published but the one-time B ledger import missing.
    class DB:
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def transaction(self): return self
        def execute(self,query,params=None):
            self.query=query
            return self
        def fetchone(self):
            if 'FROM migration_jobs' in self.query:
                return {'job_id':'fake-job-id','phase':'VERIFYING'}
            if 'FROM migrated_request_backfill' in self.query:
                return None
            raise AssertionError('archive query must not run before COMPLETE')
    import shared.migration_idempotency as impl
    monkeypatch.setattr(impl.psycopg,'connect',lambda *args,**kwargs:DB())
    code,body=archive_guard('fake-dsn','req-1',kind='DEPOSIT',source='A2000',amount='1.00')
    assert code==503 and body['status']=='PENDING'


def test_archived_id_replay_and_conflict(monkeypatch):
    fp=fingerprint('WITHDRAW','A2000','1.00')
    class DB:
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def transaction(self): return self
        def execute(self,query,params=None):
            self.query=query
            return self
        def fetchone(self):
            if 'FROM migration_jobs' in self.query:
                return {'job_id':'old-migration','phase':'VERIFYING'}
            if 'FROM migrated_request_backfill' in self.query:
                return {'ok':1}
            if 'FROM migrated_legacy_requests' in self.query:
                return {'request_fingerprint':fp,'status':'COMMITTED',
                        'response':{'http_status':200,'body':{'balance':'10.00'}}}
            raise AssertionError(self.query)
    import shared.migration_idempotency as impl
    monkeypatch.setattr(impl.psycopg,'connect',lambda *args,**kwargs:DB())
    assert archive_guard('fake','rid',kind='WITHDRAW',source='A2000',amount='1.00')==(200,{'balance':'10.00'})
    conflict=archive_guard('fake','rid',kind='WITHDRAW',source='A2000',amount='2.00')
    assert conflict[0]==409
