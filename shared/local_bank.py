"""Transactional banking core shared by all replicas of one owning shard.

No Python process-local mutexes: concurrency is enforced in PostgreSQL using
row locks, stable lock ordering, a persistent idempotency row, and one commit.
Same-shard transfers generate a 2-entry durable change group.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from shared.banking_contract import MAX_BALANCE, canonical_amount, fingerprint
from shared.routing import account_number


@dataclass(frozen=True)
class BankResult:
    http_status: int
    body: dict
    replayed: bool = False


class RouteRejected(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code, self.detail = status_code, detail
        super().__init__(detail)


def require_local_fences(conn, account_ids: list[str], shard: str, epoch: int) -> None:
    """Lock ownership rows through commit; cutover needs conflicting EXCLUSIVE lock.

    Ordering by range start avoids cross-operation inversions if a shard holds
    multiple subranges during future re-sharding. A stale owner cannot write.
    """
    nums = []
    for account in account_ids:
        n = account_number(account)
        if n is None:
            raise RouteRejected(404, "invalid account identifier")
        nums.append(n)
    fences = conn.execute(
        "SELECT range_start, range_end, epoch, phase, writable FROM ownership_fences "
        "WHERE range_start <= %s AND range_end >= %s "
        "OR range_start <= %s AND range_end >= %s "
        "ORDER BY range_start FOR SHARE",
        (nums[0], nums[0], nums[-1], nums[-1]),
    ).fetchall()
    for number in nums:
        matches = [f for f in fences if f["range_start"] <= number <= f["range_end"]]
        if len(matches) != 1:
            raise RouteRejected(409, f"account range not owned on shard {shard}")
        fence = matches[0]
        if (fence["epoch"] != epoch or not fence["writable"]
                or fence["phase"] not in ("OWNED", "MIGRATING")):
            raise RouteRejected(409, "stale routing epoch or write-fenced account")


def _stored_result(row: dict, requested_fingerprint: str) -> BankResult:
    if row["request_fingerprint"] != requested_fingerprint:
        return BankResult(409, {"detail": "request_id already used for a different operation"}, True)
    if row["status"] == "PENDING" or row["response"] is None:
        return BankResult(503, {"detail": "operation pending", "outcome": "UNKNOWN"}, True)
    response = row["response"]
    return BankResult(int(response["http_status"]), dict(response["body"]), True)


def execute_local(dsn: str, shard: str, epoch: int, kind: str, account_id: str,
                  amount_text: str, request_id: str, to_account: str | None = None) -> BankResult:
    """Run one atomic operation; return only after the SQL commit is finished.

    The unique request ID INSERT serializes concurrent duplicates across replicas.
    Both validation rejects and successes are committed as idempotent outcomes.
    Database/network failures are NOT converted into a false ABORT verdict.
    """
    accounts = [account_id] + ([to_account] if to_account is not None else [])
    if len(set(accounts)) != len(accounts):
        raise RouteRejected(422, "source and target accounts must differ")
    if any(account_number(a) is None for a in accounts):
        raise RouteRejected(404, "invalid account identifier")
    amount = Decimal(canonical_amount(amount_text))
    digest = fingerprint(kind, account_id, amount_text, to_account)
    tx_id = uuid4()
    outcome: BankResult | None = None

    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=4) as conn:
        with conn.transaction():
            conn.execute("SET LOCAL lock_timeout = '8s'")
            conn.execute("SET LOCAL statement_timeout = '20s'")
            require_local_fences(conn, accounts, shard, epoch)
            inserted = conn.execute(
                "INSERT INTO local_request_dedup "
                "(client_request_id, request_fingerprint, tx_id, status) "
                "VALUES (%s, %s, %s, 'PENDING') ON CONFLICT DO NOTHING "
                "RETURNING client_request_id",
                (request_id, digest, tx_id),
            ).fetchone()
            if inserted is None:
                old = conn.execute(
                    "SELECT request_fingerprint, status, response FROM local_request_dedup "
                    "WHERE client_request_id = %s FOR UPDATE", (request_id,)
                ).fetchone()
                if old is None:
                    raise RuntimeError("conflicting request vanished")
                outcome = _stored_result(old, digest)
            else:
                # All account row locks use account-id order to avoid deadlocks.
                balances = {}
                for a in sorted(accounts):
                    row = conn.execute(
                        "SELECT balance, version FROM accounts WHERE account_id = %s FOR UPDATE", (a,)
                    ).fetchone()
                    if row is None:
                        outcome = BankResult(404, {"detail": f"account {a} not found"})
                        break
                    balances[a] = row

                if outcome is None:
                    before = balances[account_id]["balance"]
                    if kind in ("WITHDRAW", "SAME_SHARD_TRANSFER") and before < amount:
                        outcome = BankResult(409, {"detail": "insufficient funds"})
                    elif kind == "DEPOSIT" and before + amount > MAX_BALANCE:
                        outcome = BankResult(409, {"detail": "balance limit exceeded"})
                    elif kind == "SAME_SHARD_TRANSFER" and balances[to_account]["balance"] + amount > MAX_BALANCE:
                        outcome = BankResult(409, {"detail": "recipient balance limit exceeded"})
                    else:
                        after = before + amount if kind == "DEPOSIT" else before - amount
                        changes = {account_id: (before, after)}
                        if kind == "SAME_SHARD_TRANSFER":
                            dest_before = balances[to_account]["balance"]
                            changes[to_account] = (dest_before, dest_before + amount)

                        group = uuid4()
                        conn.execute(
                            "INSERT INTO account_change_groups "
                            "(change_group_id, tx_id, expected_entries, operation_kind) "
                            "VALUES (%s, %s, %s, %s)",
                            (group, tx_id, len(changes), kind),
                        )
                        for a in sorted(changes):
                            old, new = changes[a]
                            next_version = balances[a]["version"] + 1
                            conn.execute(
                                "UPDATE accounts SET balance = %s, version = %s WHERE account_id = %s",
                                (new, next_version, a),
                            )
                            conn.execute(
                                "INSERT INTO account_changes "
                                "(account_id, version, change_group_id, before_balance, after_balance) "
                                "VALUES (%s, %s, %s, %s, %s)",
                                (a, next_version, group, old, new),
                            )
                        body = {"status": "COMMITTED", "request_id": request_id,
                                "tx_id": str(tx_id), "kind": kind,
                                "account_id": account_id, "amount": canonical_amount(amount_text),
                                "balance": f"{after:.2f}"}
                        if to_account is not None:
                            body["to_account"] = to_account
                            body["to_balance"] = f"{changes[to_account][1]:.2f}"
                        outcome = BankResult(200, body)

                assert outcome is not None
                response = {"http_status": outcome.http_status, "body": outcome.body}
                conn.execute(
                    "UPDATE local_request_dedup "
                    "SET status=%s, response=%s::jsonb, updated_at=clock_timestamp() "
                    "WHERE client_request_id=%s",
                    ("COMMITTED" if outcome.http_status == 200 else "ABORTED",
                     Jsonb(response), request_id),
                )
    assert outcome is not None
    return outcome


def lookup_local(dsn: str, request_id: str) -> BankResult:
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=4) as conn:
        row = conn.execute(
            "SELECT tx_id, status, response FROM local_request_dedup WHERE client_request_id=%s",
            (request_id,),
        ).fetchone()
    if row is None:
        return BankResult(404, {"detail": "request_id not found on this shard"})
    return BankResult(200, {"request_id": request_id, "tx_id": str(row["tx_id"]),
                            "status": row["status"],
                            "result": dict(row["response"]["body"]) if row["response"] else None})
