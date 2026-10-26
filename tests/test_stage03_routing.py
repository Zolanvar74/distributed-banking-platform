"""Fast local unit checks (DB/integration checks are verify_stage03.py)."""
import pytest

from shared.routing import account_number


@pytest.mark.parametrize("account_id, expected", [
    ("A0000", 0), ("A0001", 1), ("A1000", 1000),
    ("A1001", 1001), ("A1500", 1500), ("A2000", 2000),
    ("A9999", 9999),
    ("B0500", None), ("A00001", None), ("a0500", None),
    ("A１２３４", None), ("A12", None), ("", None),
    ("A+500", None), ("A0500 ", None), (None, None),
])
def test_canonical_account_numbers(account_id, expected):
    assert account_number(account_id) == expected
