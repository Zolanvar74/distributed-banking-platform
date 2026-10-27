"""Stage 06 durable 2PC coordinator.

PostgreSQL metadata is the decision log. A session advisory lock serializes all
attempts to drive the *same* transaction, including the startup recovery loop.
COMMIT is never reported until both participants acknowledge the decided COMMIT.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import httpx
import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from shared.banking_contract import TransferRequest, canonical_amount, fingerprint
from shared.metadata import list_replicas
from shared.two_pc import gid_for
from shared.durable_status import status_for_tx, status_for_request
from shared.stage07_faults import after_first_commit_ack

META = os.environ["METADATA_DATABASE_URL"]
TOKEN = os.environ["INTERNAL_2PC_TOKEN"]
log = logging.getLogger("banking.coordinator")


def _internal_token(x_internal_token: str | None = Header(default=None, alias="X-Internal-Token")):
    import hmac
    if not x_internal_token or not hmac.compare_digest(x_internal_token, TOKEN):
        raise HTTPException(status_code=403, detail="internal API authorization failed")


def _create_or_find(body: TransferRequest) -> UUID:
    if body.from_account == body.to_account:
        raise HTTPException(status_code=422, detail="source and destination must differ")
    digest = fingerprint("CROSS_SHARD_TRANSFER", body.from_account, body.amount, body.to_account)
    with psycopg.connect(META, row_factory=dict_row) as conn:
        with conn.transaction():
            # Hold a share lock on the map revision while recording both
            # participant owners. Future cutover must UPDATE routing_epoch.
            conn.execute("SELECT epoch FROM routing_epoch WHERE singleton=1 FOR SHARE")
            existing = conn.execute(
                "SELECT tx_id, request_fingerprint FROM transactions "
                "WHERE client_request_id=%s", (body.request_id,),
            ).fetchone()
            if existing:
                if existing["request_fingerprint"] != digest:
                    raise HTTPException(status_code=409, detail="request_id reused with different transfer")
                return existing["tx_id"]
            accounts = [body.from_account, body.to_account]
            # Stage 12 fencing gate shares routing_epoch's lock with map
            # activation: no NEW 2PC reservation may enter the moving range
            # after FENCING begins. Existing request IDs were handled above.
            from shared.routing import account_number
            for account in accounts:
                number = account_number(account)
                if number is None:
                    raise HTTPException(status_code=404, detail="invalid account")
                migrating = conn.execute(
                    "SELECT 1 FROM migration_jobs "
                    "WHERE %s BETWEEN range_start AND range_end "
                    "AND phase IN ('FENCING','FINAL_REPLAY','MAP_ACTIVATED')",
                    (number,),
                ).fetchone()
                if migrating:
                    raise HTTPException(status_code=503,
                        detail="ownership cutover in progress; retry same request_id")
            # Both ownership lookups are made in one database transaction.
            owners = []
            for account in accounts:
                from shared.routing import account_number
                number = account_number(account)
                if number is None:
                    raise HTTPException(status_code=404, detail="invalid account")
                owner = conn.execute(
                    "SELECT shard_id, epoch FROM shard_map "
                    "WHERE %s BETWEEN range_start AND range_end AND active=true", (number,),
                ).fetchone()
                if not owner:
                    raise HTTPException(status_code=404, detail="account not mapped")
                owners.append(owner)
            if owners[0]["shard_id"] == owners[1]["shard_id"]:
                raise HTTPException(status_code=409, detail="same-shard transfer belongs on local path")
            txid = uuid4()
            inserted = conn.execute(
                """INSERT INTO transactions (tx_id,client_request_id,request_fingerprint,
                    from_account,to_account,amount,state)
                    VALUES (%s,%s,%s,%s,%s,%s,'INITIATED')
                    ON CONFLICT (client_request_id) DO NOTHING RETURNING tx_id""",
                (txid, body.request_id, digest, body.from_account, body.to_account,
                 canonical_amount(body.amount)),
            ).fetchone()
            if not inserted:
                # A concurrent first request won the unique constraint.
                existing = conn.execute(
                    "SELECT tx_id, request_fingerprint FROM transactions "
                    "WHERE client_request_id=%s", (body.request_id,),
                ).fetchone()
                if not existing or existing["request_fingerprint"] != digest:
                    raise HTTPException(status_code=409, detail="request_id conflict")
                return existing["tx_id"]
            for account, owner, role in zip(accounts, owners, ("DEBIT", "CREDIT")):
                conn.execute(
                    """INSERT INTO transaction_participants
                       (tx_id,shard_id,prepare_gid,role,account_id,routing_epoch)
                       VALUES (%s,%s,%s,%s,%s,%s)""",
                    (txid, owner["shard_id"], gid_for(txid, owner["shard_id"]),
                     role, account, owner["epoch"]),
                )
            return txid


def _get_transaction(txid: UUID):
    with psycopg.connect(META, row_factory=dict_row) as conn:
        transaction = conn.execute("SELECT * FROM transactions WHERE tx_id=%s", (txid,)).fetchone()
        participants = list(conn.execute(
            "SELECT * FROM transaction_participants WHERE tx_id=%s ORDER BY shard_id", (txid,),
        ).fetchall())
        return transaction, participants


def _status_payload(txid: UUID) -> tuple[int, dict]:
    return status_for_tx(META, txid)


def _post_participant(shard: str, path: str, payload: dict) -> dict | None:
    """Single attempt; unknown delivery is retried by the recovery worker.

    Only proven-healthy replicas of the participant's shard may be used.
    Never infer vote NO from a timeout/transport error.
    """
    candidates = list_replicas(shard)
    with httpx.Client(timeout=httpx.Timeout(7.0, connect=1.5), trust_env=False) as client:
        for replica in candidates:
            host = f"http://{replica['hostname']}:{replica['port']}"
            try:
                probe = client.get(host + "/health", timeout=1.5)
                if probe.status_code != 200 or probe.json().get("shard") != shard or (
                    probe.json().get("replica") != replica["replica_id"]
                    or probe.json().get("generation") != replica.get("generation")
                ):
                    continue
            except (httpx.RequestError, ValueError):
                continue
            try:
                r = client.post(host + path, json=payload,
                                headers={"X-Internal-Token": TOKEN})
                data = r.json()
                if r.status_code == 200 and isinstance(data, dict):
                    return data
                log.warning("2PC RPC non-success shard=%s status=%s data=%s",
                            shard, r.status_code, str(data)[:200])
                return None
            except (httpx.RequestError, ValueError):
                return None
    return None


def _set_phase(txid: UUID, state: str) -> None:
    with psycopg.connect(META) as conn:
        conn.execute("UPDATE transactions SET state=%s, updated_at=clock_timestamp() "
                     "WHERE tx_id=%s AND decision IS NULL", (state, txid))


def _vote(txid: UUID, shard: str, vote: str, error: str | None = None) -> None:
    with psycopg.connect(META) as conn:
        conn.execute("UPDATE transaction_participants SET vote=%s,last_error=%s,"
                     "updated_at=clock_timestamp() WHERE tx_id=%s AND shard_id=%s",
                     (vote, error, txid, shard))


def _decide(txid: UUID, decision: str) -> None:
    state = "COMMIT_DECIDED" if decision == "COMMIT" else "ABORT_DECIDED"
    # Deciding and persisting the decision MUST commit before any phase-2 RPC.
    with psycopg.connect(META) as conn:
        with conn.transaction():
            conn.execute("UPDATE transactions SET decision=%s, state=%s, "
                         "decision_at=clock_timestamp(), updated_at=clock_timestamp() "
                         "WHERE tx_id=%s AND decision IS NULL", (decision, state, txid))
            conn.execute("INSERT INTO transaction_recovery_events "
                         "(tx_id,event_type,details) VALUES (%s,%s,%s)",
                         (txid, "DECISION_DURABLE", Jsonb({"decision": decision})))


def _ack(txid: UUID, shard: str) -> None:
    with psycopg.connect(META) as conn:
        conn.execute("UPDATE transaction_participants SET completion_ack=true, "
                     "ack_at=clock_timestamp(),updated_at=clock_timestamp() "
                     "WHERE tx_id=%s AND shard_id=%s", (txid, shard))


def _try_complete(txid: UUID) -> None:
    with psycopg.connect(META) as conn:
        result = conn.execute(
            "UPDATE transactions SET state='COMPLETED',updated_at=clock_timestamp() "
            "WHERE tx_id=%s AND state <> 'COMPLETED' AND decision IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM transaction_participants "
            "WHERE tx_id=%s AND completion_ack=false) RETURNING tx_id", (txid, txid)
        ).fetchone()
        if result:
            conn.execute(
                "INSERT INTO transaction_recovery_events(tx_id,event_type,details) "
                "VALUES (%s,'PHASE_2_COMPLETED','{}'::jsonb)", (txid,)
            )


def _deliver(txid: UUID, tx: dict, parts: list[dict]) -> None:
    decision = tx["decision"]
    for p in parts:
        if p["completion_ack"]:
            continue
        if decision == "ABORT" and p["vote"] != "YES":
            # After a definitive NO we never dispatched any later PREPARE.
            _ack(txid, p["shard_id"])
            continue
        data = _post_participant(p["shard_id"], "/internal/2pc/decision", {
            "tx_id": str(txid), "shard_id": p["shard_id"], "decision": decision,
        })
        if data and data.get("outcome") == ("COMMITTED" if decision == "COMMIT" else "ABORTED"):
            _ack(txid, p["shard_id"])
            # The one-shot crash occurs *only* after COMMIT decision is durable,
            # first participant is committed and its ACK is durable, while the
            # second participant still has a prepared transaction.
            if decision == "COMMIT":
                after_first_commit_ack(META, txid)
    _try_complete(txid)


def _drive(txid: UUID) -> None:
    # Advisory lock held by this physical metadata session for the *entire*
    # attempt; on abrupt Coordinator death PostgreSQL automatically releases it.
    with psycopg.connect(META, autocommit=True) as lock_connection:
        lock_connection.execute("SELECT pg_advisory_lock(hashtext(%s))", (str(txid),))
        tx, parts = _get_transaction(txid)
        if tx is None or tx["state"] == "COMPLETED":
            return
        if tx["decision"] is None:
            _set_phase(txid, "PREPARING")
            for p in parts:
                if p["vote"] == "YES" or p["vote"] == "NO":
                    continue
                data = _post_participant(p["shard_id"], "/internal/2pc/prepare", {
                    "tx_id": str(txid), "shard_id": p["shard_id"],
                    "role": p["role"], "account_id": p["account_id"],
                    "amount": str(tx["amount"]), "epoch": p["routing_epoch"],
                })
                if not data or data.get("vote") not in ("YES", "NO"):
                    # Crucial: uncertainty is not a definitive NO. This may
                    # already be prepared on a shard after its response was lost.
                    return
                _vote(txid, p["shard_id"], data["vote"], data.get("reason"))
                if data["vote"] == "NO":
                    break
            tx, parts = _get_transaction(txid)
            votes = {p["vote"] for p in parts}
            if "NO" in votes:
                _decide(txid, "ABORT")
            elif votes == {"YES"}:
                _set_phase(txid, "PREPARED")
                _decide(txid, "COMMIT")
            else:
                return
            tx, parts = _get_transaction(txid)
        _deliver(txid, tx, parts)


async def _recovery_loop():
    while True:
        try:
            with psycopg.connect(META) as conn:
                pending = [r[0] for r in conn.execute(
                    "SELECT tx_id FROM transactions WHERE state <> 'COMPLETED' "
                    "ORDER BY created_at LIMIT 50").fetchall()]
            for txid in pending:
                try:
                    await asyncio.to_thread(_drive, txid)
                except Exception:
                    log.exception("recovery deferred tx=%s", txid)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("recovery metadata unavailable")
        await asyncio.sleep(4)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_recovery_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Durable Transaction Coordinator / Stage 07", lifespan=lifespan)


@app.get("/health")
def health():
    with psycopg.connect(META, connect_timeout=2) as conn:
        conn.execute("SELECT 1")
    return {"status": "ok", "service": "coordinator", "stage": "07",
            "fault_injection_enabled": os.environ.get("STAGE07_ENABLE_FAULTS") == "1"}


@app.post("/internal/transfers/cross", dependencies=[Depends(_internal_token)])
async def cross_transfer(body: TransferRequest):
    # Defense-in-depth for direct internal calls bypassing the Gateway. A
    # historical B->B transfer must not be reinterpreted as a fresh B->C 2PC.
    from shared.migration_idempotency import archive_guard, touches_migrated_range
    if touches_migrated_range([body.from_account, body.to_account]):
        try:
            archived = await asyncio.to_thread(
                archive_guard, META, body.request_id,
                kind="SAME_SHARD_TRANSFER", source=body.from_account,
                amount=body.amount, destination=body.to_account)
        except psycopg.Error as exc:
            raise HTTPException(status_code=503, detail="legacy request index unavailable") from exc
        if archived is not None:
            code, payload = archived
            return JSONResponse(status_code=code, content=payload)
    txid = await asyncio.to_thread(_create_or_find, body)
    try:
        await asyncio.to_thread(_drive, txid)
    except Exception:
        log.exception("2PC processing deferred tx=%s; recover by durable log", txid)
    code, result = await asyncio.to_thread(_status_payload, txid)
    return JSONResponse(status_code=code, content=result)


@app.get("/internal/transactions/{tx_id}", dependencies=[Depends(_internal_token)])
def transaction_status(tx_id: UUID):
    code, result = _status_payload(tx_id)
    return JSONResponse(status_code=code, content=result)


@app.get("/internal/requests/{request_id}", dependencies=[Depends(_internal_token)])
def request_status(request_id: str):
    code, result = status_for_request(META, request_id)
    return JSONResponse(status_code=code, content=result)
