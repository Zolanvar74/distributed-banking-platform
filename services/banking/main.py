import os
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row

from shared.metadata import register_replica

SHARD_ID = os.environ["SHARD_ID"]
REPLICA_ID = os.environ["REPLICA_ID"]
REPLICA_HOST = os.environ["REPLICA_HOST"]
SHARD_DATABASE_URL = os.environ["SHARD_DATABASE_URL"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    register_replica(SHARD_ID, REPLICA_ID, REPLICA_HOST)
    print(f"Registered banking replica {REPLICA_ID} in shard {SHARD_ID}", flush=True)
    yield


app = FastAPI(title=f"Banking Replica {REPLICA_ID} / Stage 02", lifespan=lifespan)


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
def get_balance(account_id: str):
    # This stage is READ ONLY. Mutations, locking and 2PC come later.
    with psycopg.connect(SHARD_DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT balance FROM accounts WHERE account_id = %s", (account_id,))
            row = cursor.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="account not found on owning shard")
    return {
        "account_id": account_id,
        "balance": str(row["balance"]),
        "shard": SHARD_ID,
        "served_by": REPLICA_ID,
    }
