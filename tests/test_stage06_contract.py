"""Stage 06 deterministic checks without a running database."""
from uuid import UUID, uuid4
from pydantic import ValidationError
import pytest
from shared.banking_contract import fingerprint
from shared.two_pc import PrepareMessage, DecisionMessage, gid_for


def test_gid_reproducible_and_shard_isolation():
    tx = uuid4()
    assert gid_for(tx, 'A') == gid_for(str(tx), 'A')
    assert gid_for(tx, 'A') != gid_for(tx, 'B')
    assert gid_for(tx, 'A').startswith('bank2pc:')
    assert UUID(gid_for(tx, 'A').split(':')[1]) == tx


def test_invalid_gid_shard():
    with pytest.raises(ValueError):
        gid_for(uuid4(), 'unknown')


def test_prepare_and_decision_contract():
    tx = uuid4()
    p = PrepareMessage(tx_id=tx, shard_id='A', role='DEBIT',
                       account_id='A0500', amount='1.00', epoch=1)
    assert p.shard_id == 'A'
    assert DecisionMessage(tx_id=tx, shard_id='A', decision='COMMIT').decision == 'COMMIT'
    with pytest.raises(ValidationError):
        PrepareMessage(tx_id=tx, shard_id='BAD', role='DEBIT', account_id='A0500', amount='1.00', epoch=1)
    with pytest.raises(ValidationError):
        DecisionMessage(tx_id=tx, shard_id='A', decision='MAYBE')


def test_global_request_fingerprint_is_canonical():
    a = fingerprint('CROSS_SHARD_TRANSFER', 'A0500', '2.3', 'A1500')
    assert a == fingerprint('CROSS_SHARD_TRANSFER', 'A0500', '2.30', 'A1500')
    assert a != fingerprint('CROSS_SHARD_TRANSFER', 'A1500', '2.30', 'A0500')
    assert a != fingerprint('SAME_SHARD_TRANSFER', 'A0500', '2.30', 'A1500')
