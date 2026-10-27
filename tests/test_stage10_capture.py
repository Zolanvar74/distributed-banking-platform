"""No PostgreSQL required: immutable source snapshot/hash contract tests."""
from decimal import Decimal

import pytest

from shared.migration_capture import snapshot_digest, create_source_snapshot, RANGE_START, RANGE_END


def test_snapshot_digest_stable_for_order_and_decimal_encoding():
    one = [dict(account_id="A2000",balance=Decimal("100.0"),version=2),
           dict(account_id="A1501",balance=Decimal("30.00"),version=0)]
    two = [dict(account_id="A1501",balance=Decimal("30.0"),version=0),
           dict(account_id="A2000",balance=Decimal("100.00"),version=2)]
    assert snapshot_digest(one) == snapshot_digest(two)


def test_snapshot_digest_changes_if_balance_or_version_changes():
    a = [dict(account_id="A2000",balance=Decimal("100"),version=0)]
    b = [dict(account_id="A2000",balance=Decimal("101"),version=0)]
    c = [dict(account_id="A2000",balance=Decimal("100"),version=1)]
    assert len({snapshot_digest(x) for x in (a,b,c)}) == 3


def test_range_pinned_to_assignment():
    assert (RANGE_START,RANGE_END)==(1501,2000)


def test_snapshot_rejects_invalid_bounds_without_opening_database():
    with pytest.raises(ValueError):
        create_source_snapshot("unused", "unused", 0, 2000)
