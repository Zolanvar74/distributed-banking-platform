"""Read-only, durable transaction status directly from metadata PostgreSQL.

The Gateway must expose PENDING even if the Coordinator is dead. Never infer
COMMITTED merely from COMMIT_DECIDED: all phase-2 ACKs are required.
"""
from __future__ import annotations

from uuid import UUID

import psycopg
from psycopg.rows import dict_row


def status_for_tx(dsn: str, txid: UUID) -> tuple[int, dict]:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            tx = conn.execute(
                "SELECT * FROM transactions WHERE tx_id=%s", (txid,),
            ).fetchone()
            if tx is None:
                return 404, {"detail": "transaction not found"}
            parts = conn.execute(
                "SELECT * FROM transaction_participants WHERE tx_id=%s ORDER BY shard_id", (txid,),
            ).fetchall()
    completed = (tx["state"] == "COMPLETED" and len(parts) == 2
                 and all(p["completion_ack"] for p in parts))
    if completed and tx["decision"] == "COMMIT":
        status, code = "COMMITTED", 200
    elif completed and tx["decision"] == "ABORT":
        status, code = "ABORTED", 409
    else:
        status, code = "PENDING", 202
    return code, {
        "tx_id": str(txid), "request_id": tx["client_request_id"],
        "from_account": tx["from_account"], "to_account": tx["to_account"],
        "amount": str(tx["amount"]), "status": status,
        "state": tx["state"], "decision": tx["decision"],
        "participants": [
            {"shard": p["shard_id"], "vote": p["vote"], "ack": p["completion_ack"]}
            for p in parts
        ],
    }


def status_for_request(dsn: str, request_id: str) -> tuple[int, dict]:
    with psycopg.connect(dsn) as conn:
        found = conn.execute(
            "SELECT tx_id FROM transactions WHERE client_request_id=%s", (request_id,),
        ).fetchone()
    if found is None:
        return 404, {"detail": "cross-shard request not found"}
    return status_for_tx(dsn, found[0])
