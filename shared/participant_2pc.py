"""PostgreSQL prepared-transaction participant shared by application replicas.

Only the selected shard DB is modified. Prepared effects are *invisible* until
COMMIT PREPARED. The participant_commit_ledger is written in the prepared SQL
transaction, so after a crash a visible ledger entry proves commit.
"""
from __future__ import annotations

from decimal import Decimal
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from shared.banking_contract import MAX_BALANCE, canonical_amount
from shared.local_bank import RouteRejected, require_local_fences
from shared.two_pc import PrepareMessage, gid_for


def _prepared(conn: psycopg.Connection, gid: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM pg_prepared_xacts WHERE gid=%s AND database=current_database()",
        (gid,),
    ).fetchone()
    return row is not None


def _committed(conn: psycopg.Connection, tx_id: UUID, shard: str, gid: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM participant_commit_ledger "
        "WHERE tx_id=%s AND shard_id=%s AND prepare_gid=%s", (tx_id, shard, gid),
    ).fetchone() is not None


def prepare(dsn: str, actual_shard: str, message: PrepareMessage) -> dict:
    if message.shard_id != actual_shard:
        return {"vote": "NO", "reason": "incorrect participant shard"}
    gid = gid_for(message.tx_id, actual_shard)
    amount = Decimal(canonical_amount(message.amount))
    if amount <= 0 or amount > MAX_BALANCE:
        return {"vote": "NO", "reason": "invalid amount"}
    # autocommit=True is required: PREPARE TRANSACTION terminates the explicit
    # BEGIN and must not be followed by psycopg's own COMMIT.
    with psycopg.connect(dsn, autocommit=True, connect_timeout=4, row_factory=dict_row) as conn:
        if _prepared(conn, gid):
            return {"vote": "YES", "gid": gid, "replayed": True}
        if _committed(conn, message.tx_id, actual_shard, gid):
            return {"vote": "YES", "gid": gid, "replayed": True, "already_committed": True}
        conn.execute("BEGIN")
        try:
            conn.execute("SET LOCAL lock_timeout = '6s'")
            conn.execute("SET LOCAL statement_timeout = '18s'")
            require_local_fences(conn, [message.account_id], actual_shard, message.epoch)
            row = conn.execute(
                "SELECT balance, version FROM accounts WHERE account_id=%s FOR UPDATE",
                (message.account_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                return {"vote": "NO", "reason": "account not found"}
            before = row["balance"]
            if message.role == "DEBIT" and before < amount:
                conn.execute("ROLLBACK")
                return {"vote": "NO", "reason": "insufficient funds"}
            if message.role == "CREDIT" and before + amount > MAX_BALANCE:
                conn.execute("ROLLBACK")
                return {"vote": "NO", "reason": "recipient balance limit exceeded"}
            after = before - amount if message.role == "DEBIT" else before + amount
            next_version = row["version"] + 1
            conn.execute(
                "INSERT INTO account_change_groups "
                "(change_group_id, tx_id, expected_entries, operation_kind) "
                "VALUES (%s, %s, 1, 'CROSS_SHARD_TRANSFER')",
                (message.tx_id, message.tx_id),
            )
            conn.execute(
                "UPDATE accounts SET balance=%s, version=%s WHERE account_id=%s",
                (after, next_version, message.account_id),
            )
            conn.execute(
                "INSERT INTO account_changes "
                "(account_id,version,change_group_id,before_balance,after_balance) "
                "VALUES (%s,%s,%s,%s,%s)",
                (message.account_id, next_version, message.tx_id, before, after),
            )
            conn.execute(
                "INSERT INTO participant_commit_ledger (tx_id,shard_id,prepare_gid) "
                "VALUES (%s,%s,%s)", (message.tx_id, actual_shard, gid),
            )
            # Atomic durability boundary; the transaction remains prepared and
            # keeps its row locks even if every application replica dies.
            conn.execute(sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(gid)))
            return {"vote": "YES", "gid": gid, "replayed": False}
        except Exception:
            # ROLLBACK has no effect after a successful PREPARE, which ended the
            # SQL transaction. In uncertain cases the next query checks the GID.
            try:
                conn.execute("ROLLBACK")
            except psycopg.Error:
                pass
            raise


def finish(dsn: str, actual_shard: str, tx_id: UUID, decision: str) -> dict:
    gid = gid_for(tx_id, actual_shard)
    if decision not in ("COMMIT", "ABORT"):
        raise ValueError("invalid decision")
    with psycopg.connect(dsn, autocommit=True, connect_timeout=4) as conn:
        if _committed(conn, tx_id, actual_shard, gid):
            if decision == "ABORT":
                return {"outcome": "CONFLICT", "reason": "already committed"}
            return {"outcome": "COMMITTED", "gid": gid, "replayed": True}
        if not _prepared(conn, gid):
            if decision == "ABORT":
                return {"outcome": "ABORTED", "gid": gid, "replayed": True}
            return {"outcome": "UNKNOWN", "gid": gid,
                    "reason": "no prepared transaction and no committed ledger"}
        command = "COMMIT PREPARED" if decision == "COMMIT" else "ROLLBACK PREPARED"
        conn.execute(sql.SQL(command + " {}").format(sql.Literal(gid)))
        if decision == "COMMIT":
            if not _committed(conn, tx_id, actual_shard, gid):
                return {"outcome": "UNKNOWN", "reason": "commit evidence missing"}
            return {"outcome": "COMMITTED", "gid": gid, "replayed": False}
        return {"outcome": "ABORTED", "gid": gid, "replayed": False}


def participant_state(dsn: str, actual_shard: str, tx_id: UUID) -> str:
    gid = gid_for(tx_id, actual_shard)
    with psycopg.connect(dsn, autocommit=True, connect_timeout=4) as conn:
        if _committed(conn, tx_id, actual_shard, gid):
            return "COMMITTED"
        if _prepared(conn, gid):
            return "PREPARED"
        return "ABSENT"  # ABSENT is not proof of ABORT if PREPARE is in-flight.
