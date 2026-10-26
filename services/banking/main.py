"""Stage-04 read-only banking participant with local ownership verification."""
import os
import logging
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Header, HTTPException
from psycopg.rows import dict_row

from shared.metadata import register_replica
from shared.routing import account_number

SHARD_ID = os.environ["SHARD_ID"]
REPLICA_ID = os.environ["REPLICA_ID"]
REPLICA_HOST = os.environ["REPLICA_HOST"]
SHARD_DATABASE_URL = os.environ["SHARD_DATABASE_URL"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    register_replica(SHARD_ID, REPLICA_ID, REPLICA_HOST)
    print(f"Registered banking replica {REPLICA_ID} in shard {SHARD_ID}", flush=True)
    yield


app = FastAPI(title=f"Banking Replica {REPLICA_ID} / Stage 04", lifespan=lifespan)
log = logging.getLogger("banking.traffic")


@app.get("/health")
def health():
    try:
        with psycopg.connect(SHARD_DATABASE_URL, connect_timeout=2) as conn:
            conn.execute("SELECT 1")
        return {"status": "ok", "shard": SHARD_ID, "replica": REPLICA_ID}
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="shard database unavailable") from exc


@app.get("/internal/identity")
def identity():
    return {"shard": SHARD_ID, "replica": REPLICA_ID}


@app.get("/internal/accounts/{account_id}/balance")
def get_balance(
    account_id: str,
    expected_shard: str | None = Header(default=None, alias="X-Expected-Shard"),
    routing_epoch: int | None = Header(default=None, alias="X-Routing-Epoch"),
):
    """Refuse wrong/stale routes *at the shard*, not just in the Gateway.

    Both the local fence and the account row are read from one repeatable-read
    snapshot. This is a READ guard, not yet the write-locking/fencing required
    for the future mutation API and live cutover.
    """
    if expected_shard is None or routing_epoch is None:
        raise HTTPException(status_code=428, detail="routing claims required")
    if expected_shard != SHARD_ID:
        raise HTTPException(status_code=409, detail="wrong shard for routed request")
    number = account_number(account_id)
    if number is None:
        raise HTTPException(status_code=404, detail="invalid account identifier")
    try:
        with psycopg.connect(SHARD_DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            fence = conn.execute(
                "SELECT epoch, phase, writable FROM ownership_fences "
                "WHERE %s BETWEEN range_start AND range_end",
                (number,),
            ).fetchone()
            if fence is None:
                raise HTTPException(status_code=409, detail="account range not owned here")
            if fence["epoch"] != routing_epoch or not fence["writable"]:
                raise HTTPException(status_code=409, detail="stale epoch or fenced account range")
            row = conn.execute(
                "SELECT balance FROM accounts WHERE account_id=%s", (account_id,)
            ).fetchone()
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="shard database unavailable") from exc
    if row is None:
        raise HTTPException(status_code=404, detail="account not found on owning shard")
    log.info("balance_read account=%s shard=%s replica=%s", account_id, SHARD_ID, REPLICA_ID)
    return {
        "account_id": account_id,
        "balance": str(row["balance"]),
        "shard": SHARD_ID,
        "served_by": REPLICA_ID,
    }
