"""Stage-05 banking participant, SQL-atomic mutations and ownership fencing."""
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
REPLICA_GENERATION = int(os.environ.get("REPLICA_GENERATION", "1"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    register_replica(SHARD_ID, REPLICA_ID, REPLICA_HOST)
    print(f"Registered banking replica {REPLICA_ID} in shard {SHARD_ID}", flush=True)
    yield


app = FastAPI(title=f"Banking Replica {REPLICA_ID} / Stage 05", lifespan=lifespan)
log = logging.getLogger("banking.traffic")


@app.get("/health")
def health():
    try:
        with psycopg.connect(SHARD_DATABASE_URL, connect_timeout=2) as conn:
            conn.execute("SELECT 1")
        return {"status": "ok", "shard": SHARD_ID, "replica": REPLICA_ID,
                "generation": REPLICA_GENERATION}
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

# Stage 05: all monetary mutations are SQL transactions in the shared shard DB.
from fastapi.responses import JSONResponse
from shared.banking_contract import WriteRequest, TransferRequest
from shared.local_bank import RouteRejected, execute_local, lookup_local


def _claims(expected_shard: str | None, routing_epoch: int | None) -> int:
    if expected_shard is None or routing_epoch is None:
        raise HTTPException(status_code=428, detail="routing claims required")
    if expected_shard != SHARD_ID:
        raise HTTPException(status_code=409, detail="wrong shard for routed request")
    return routing_epoch


def _mutation(kind: str, account_id: str, body: WriteRequest,
              expected_shard: str | None, routing_epoch: int | None,
              to_account: str | None = None):
    epoch = _claims(expected_shard, routing_epoch)
    try:
        result = execute_local(SHARD_DATABASE_URL, SHARD_ID, epoch, kind,
                               account_id, body.amount, body.request_id, to_account)
    except RouteRejected as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    except psycopg.Error as exc:
        log.exception("banking mutation database error request_id=%s", body.request_id)
        return JSONResponse(status_code=503, content={"detail": "database error; outcome uncertain",
                                                      "request_id": body.request_id,
                                                      "outcome": "UNKNOWN"})
    response = dict(result.body)
    response.update({"shard": SHARD_ID, "served_by": REPLICA_ID,
                     "idempotent_replay": result.replayed})
    log.info("mutation kind=%s request_id=%s shard=%s replica=%s replay=%s status=%s",
             kind, body.request_id, SHARD_ID, REPLICA_ID, result.replayed, result.http_status)
    return JSONResponse(status_code=result.http_status, content=response)


@app.post("/internal/accounts/{account_id}/deposit")
def deposit(account_id: str, body: WriteRequest,
            expected_shard: str | None = Header(default=None, alias="X-Expected-Shard"),
            routing_epoch: int | None = Header(default=None, alias="X-Routing-Epoch")):
    return _mutation("DEPOSIT", account_id, body, expected_shard, routing_epoch)


@app.post("/internal/accounts/{account_id}/withdraw")
def withdraw(account_id: str, body: WriteRequest,
             expected_shard: str | None = Header(default=None, alias="X-Expected-Shard"),
             routing_epoch: int | None = Header(default=None, alias="X-Routing-Epoch")):
    return _mutation("WITHDRAW", account_id, body, expected_shard, routing_epoch)


@app.post("/internal/transfers/local")
def same_shard_transfer(body: TransferRequest,
                        expected_shard: str | None = Header(default=None, alias="X-Expected-Shard"),
                        routing_epoch: int | None = Header(default=None, alias="X-Routing-Epoch")):
    return _mutation("SAME_SHARD_TRANSFER", body.from_account, body,
                     expected_shard, routing_epoch, body.to_account)


@app.get("/internal/accounts/{account_id}/operations/{request_id}")
def operation_status(account_id: str, request_id: str,
                     expected_shard: str | None = Header(default=None, alias="X-Expected-Shard"),
                     routing_epoch: int | None = Header(default=None, alias="X-Routing-Epoch")):
    epoch = _claims(expected_shard, routing_epoch)
    # Check ownership before exposing local result. Not a cross-shard global status.
    try:
        with psycopg.connect(SHARD_DATABASE_URL, row_factory=dict_row) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            number = account_number(account_id)
            if number is None:
                raise HTTPException(status_code=404, detail="invalid account")
            fence = conn.execute(
                "SELECT epoch, writable FROM ownership_fences WHERE %s BETWEEN range_start AND range_end",
                (number,),
            ).fetchone()
            if fence is None or fence["epoch"] != epoch or not fence["writable"]:
                raise HTTPException(status_code=409, detail="wrong owner or stale epoch")
        result = lookup_local(SHARD_DATABASE_URL, request_id)
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return JSONResponse(status_code=result.http_status, content=result.body)

# Stage 06 internal 2PC protocol. All actions are executed against the shard's
# shared PostgreSQL, not a process-local in-memory transaction.
from fastapi import Depends
from shared.two_pc import PrepareMessage, DecisionMessage
from shared.participant_2pc import prepare as prepared_vote, finish as prepared_finish, participant_state


def _internal_token(x_internal_token: str | None = Header(default=None, alias="X-Internal-Token")):
    import hmac
    configured = os.environ.get("INTERNAL_2PC_TOKEN", "")
    if not configured or x_internal_token is None or not hmac.compare_digest(x_internal_token, configured):
        raise HTTPException(status_code=403, detail="internal 2PC authentication failed")


@app.post("/internal/2pc/prepare", dependencies=[Depends(_internal_token)])
def prepare_two_pc(body: PrepareMessage):
    try:
        return prepared_vote(SHARD_DATABASE_URL, SHARD_ID, body)
    except RouteRejected as exc:
        return {"vote": "NO", "reason": exc.detail}
    except psycopg.Error:
        log.exception("2pc PREPARE uncertain tx=%s shard=%s", body.tx_id, SHARD_ID)
        return JSONResponse(status_code=503, content={"vote": "UNKNOWN", "reason": "database busy or unavailable"})


@app.post("/internal/2pc/decision", dependencies=[Depends(_internal_token)])
def decide_two_pc(body: DecisionMessage):
    if body.shard_id != SHARD_ID:
        raise HTTPException(status_code=409, detail="incorrect shard")
    try:
        answer = prepared_finish(SHARD_DATABASE_URL, SHARD_ID, body.tx_id, body.decision)
        return JSONResponse(status_code=200 if answer["outcome"] in ("COMMITTED", "ABORTED") else 409
                            if answer["outcome"] == "CONFLICT" else 503, content=answer)
    except psycopg.Error:
        log.exception("2pc DECISION uncertain tx=%s shard=%s", body.tx_id, SHARD_ID)
        return JSONResponse(status_code=503, content={"outcome": "UNKNOWN"})


@app.get("/internal/2pc/{tx_id}/status", dependencies=[Depends(_internal_token)])
def two_pc_status(tx_id: str):
    from uuid import UUID
    try:
        tx = UUID(tx_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid transaction ID") from exc
    try:
        return {"tx_id": str(tx), "shard": SHARD_ID,
                "participant_state": participant_state(SHARD_DATABASE_URL, SHARD_ID, tx)}
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="participant status unknown") from exc
