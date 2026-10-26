"""Stage 05 public/internal write contract and deterministic idempotency keys.

Money amounts are always decimal *strings*. Floats are intentionally invalid.
The fingerprint does not depend on which replica serves a request.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator

MAX_BALANCE = Decimal("999999999999999999.99")  # NUMERIC(20,2)
AMOUNT_PATTERN = r"^(?:0|[1-9][0-9]{0,17})(?:\.[0-9]{1,2})?$"


class WriteRequest(BaseModel):
    amount: str = Field(pattern=AMOUNT_PATTERN)
    request_id: str = Field(min_length=1, max_length=200)

    @field_validator("amount")
    @classmethod
    def positive_exact_amount(cls, v: str) -> str:
        amount = Decimal(v)
        if amount <= 0 or amount > MAX_BALANCE:
            raise ValueError("amount must be between 0.01 and 999999999999999999.99")
        return v

    @field_validator("request_id")
    @classmethod
    def nonblank_request_id(cls, v: str) -> str:
        if v != v.strip() or any(ord(c) < 32 for c in v):
            raise ValueError("request_id must have no outer whitespace or control characters")
        return v


class TransferRequest(WriteRequest):
    from_account: str
    to_account: str


def canonical_amount(value: str) -> str:
    return f"{Decimal(value):.2f}"


def fingerprint(kind: Literal["DEPOSIT", "WITHDRAW", "SAME_SHARD_TRANSFER"],
                from_account: str, amount: str, to_account: str | None = None) -> str:
    canonical = {"kind": kind, "from_account": from_account,
                 "to_account": to_account, "amount": canonical_amount(amount)}
    return hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
