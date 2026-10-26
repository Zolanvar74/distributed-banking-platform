"""Canonical account identifier parsing, independent of DB and web framework.

Ownership is intentionally NOT hardcoded here: Metadata's active shard_map
is the authority, so a future live migration can change ownership at runtime.
"""
from __future__ import annotations

import re

ACCOUNT_PATTERN = re.compile(r"A([0-9]{4})\Z", re.ASCII)


def account_number(account_id: str) -> int | None:
    """Return numeric portion of a canonical account ID; None if malformed.

    A0000 is syntactically valid but is deliberately not owned in our initial
    shard map, and will be rejected at the ownership lookup stage.
    """
    if not isinstance(account_id, str):
        return None
    match = ACCOUNT_PATTERN.fullmatch(account_id)
    return int(match.group(1)) if match is not None else None
