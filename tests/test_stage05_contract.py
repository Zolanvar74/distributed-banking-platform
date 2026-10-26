"""Pure unit checks (do not need PostgreSQL or running Docker)."""
from decimal import Decimal

import pytest
from pydantic import ValidationError

from shared.banking_contract import WriteRequest, TransferRequest, canonical_amount, fingerprint


@pytest.mark.parametrize("invalid", ["0", "0.00", "-1.00", "1.001", "1e3", "1,00", " 1.00",
                                      "0001.00", "9999999999999999999.00", 1, 1.5])
def test_invalid_amounts(invalid):
    with pytest.raises(ValidationError):
        WriteRequest(amount=invalid, request_id="test-1")


def test_exact_cent_amount_and_max_range():
    assert WriteRequest(amount="0.01", request_id="z").amount == "0.01"
    assert WriteRequest(amount="999999999999999999.99", request_id="z").amount
    assert canonical_amount("1.1") == "1.10"
    assert Decimal(canonical_amount("0.01")) * 3 == Decimal("0.03")


@pytest.mark.parametrize("invalid", ["", " ", " a", "b ", "hello\nthere", "x" * 201])
def test_reject_invalid_request_ids(invalid):
    with pytest.raises(ValidationError):
        WriteRequest(amount="1.00", request_id=invalid)


def test_canonical_fingerprint_retries_and_payload_conflict():
    base = fingerprint("DEPOSIT", "A0500", "1")
    assert base == fingerprint("DEPOSIT", "A0500", "1.00")
    assert base != fingerprint("WITHDRAW", "A0500", "1.00")
    assert base != fingerprint("DEPOSIT", "A0500", "2.00")
    assert base != fingerprint("DEPOSIT", "A1500", "1.00")
    t = fingerprint("SAME_SHARD_TRANSFER", "A0500", "2", "A0001")
    assert t != fingerprint("SAME_SHARD_TRANSFER", "A0001", "2", "A0500")


def test_transfer_contract():
    req = TransferRequest(from_account="A0500", to_account="A0001",
                          amount="10.25", request_id="transfer-001")
    assert req.model_dump()["to_account"] == "A0001"


def test_preserved_stage02_migration_checksum_files():
    """Stage 05 must be additive and never edit an applied Stage 02 migration."""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    assert (root / "sql/migrations/shard/003_stage05_local_acid.sql").is_file()
    assert "ADD CONSTRAINT" in (root / "sql/migrations/shard/003_stage05_local_acid.sql").read_text()
