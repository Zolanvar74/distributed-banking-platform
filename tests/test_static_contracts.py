"""Fast tests, no running Docker environment required."""
from shared.routing import ACCOUNT_PATTERN


def test_account_format():
    assert ACCOUNT_PATTERN.fullmatch("A0001")
    assert ACCOUNT_PATTERN.fullmatch("A2000")
    assert not ACCOUNT_PATTERN.fullmatch("A12345")
    assert not ACCOUNT_PATTERN.fullmatch("B0001")
