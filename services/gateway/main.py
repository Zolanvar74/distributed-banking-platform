"""Stage 06 shard-aware routing with safe local write semantics.

The registry is discovery-only. The Gateway probes *each* candidate to confirm
it is alive and matches the expected shard and replica identity. On read-only
GET, a failed selected replica can be retried on an alternate healthy replica
within the same shard. Money-changing APIs choose one replica only; unknown outcomes are exposed.
"""
from __future__ import annotations

import asyncio
import logging

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from services.gateway.balancer import RoundRobin, identity_ok
from shared.metadata import Owner, active_shard_map, get_owner, list_replicas

log = logging.getLogger("banking.gateway")
app = FastAPI(title="Shard-Aware Gateway / Stage 06")
_balancer = RoundRobin()


@app.get("/health")
def health():
    return {"status": "ok", "service": "gateway", "stage": "06"}


async def probe_healthy(shard_id: str) -> tuple[list[dict], list[dict]]:
    """List healthy replicas, never selecting an unrelated shard.

    Each candidate has a bounded per-request timeout. Snapshot the registry
    for this lookup; replacements will be discovered by subsequent requests.
    """
    candidates = list_replicas(shard_id)
    async with httpx.AsyncClient(timeout=httpx.Timeout(1.5), trust_env=False) as client:
        async def probe(replica: dict) -> bool:
            url = f"http://{replica['hostname']}:{replica['port']}/health"
            try:
                response = await client.get(url)
                if response.status_code != 200:
                    return False
                return identity_ok(response.json(), shard_id, replica["replica_id"])
            except (httpx.RequestError, ValueError, TypeError):
                return False

        statuses = await asyncio.gather(*(probe(replica) for replica in candidates))
    healthy = [replica for replica, ok in zip(candidates, statuses) if ok]
    unhealthy = [replica for replica, ok in zip(candidates, statuses) if not ok]
    return healthy, unhealthy


async def ordered_healthy(shard_id: str) -> list[dict]:
    healthy, _ = await probe_healthy(shard_id)
    if not healthy:
        raise HTTPException(status_code=503, detail=f"no healthy replicas for shard {shard_id}")
    return _balancer.ordered(shard_id, healthy)


def require_owner(account_id: str) -> Owner:
    owner = get_owner(account_id)
    if owner is None:
        raise HTTPException(status_code=404, detail="account is outside the current shard map")
    return owner


@app.get("/debug/shard-map")
def shard_map_snapshot():
    return active_shard_map()


@app.get("/debug/replicas/{shard_id}")
async def replica_snapshot(shard_id: str):
    """Demo-only health and discovery overview; no management actions."""
    if shard_id not in {"A", "B", "C"}:
        raise HTTPException(status_code=404, detail="unknown shard")
    healthy, unhealthy = await probe_healthy(shard_id)
    return {"shard": shard_id,
            "healthy": [item["replica_id"] for item in healthy],
            "unhealthy": [item["replica_id"] for item in unhealthy],
            "healthy_count": len(healthy),
            "candidate_count": len(healthy) + len(unhealthy)}


@app.get("/debug/route/{account_id}")
async def debug_route(account_id: str):
    owner = require_owner(account_id)
    replica = (await ordered_healthy(owner.shard_id))[0]
    return {"account_id": account_id, "owner_shard": owner.shard_id,
            "shard_epoch": owner.epoch, "selected_replica": replica["replica_id"]}


@app.get("/accounts/{account_id}/balance")
async def get_balance(account_id: str):
    owner = require_owner(account_id)
    # Choose ONLY verified healthy replicas of the correct owner shard.
    candidates = await ordered_healthy(owner.shard_id)
    headers = {"X-Expected-Shard": owner.shard_id,
               "X-Routing-Epoch": str(owner.epoch)}
    failure_count = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(3.0), trust_env=False) as client:
        for replica in candidates:
            url = (f"http://{replica['hostname']}:{replica['port']}"
                   f"/internal/accounts/{account_id}/balance")
            try:
                response = await client.get(url, headers=headers)
            except httpx.RequestError:
                failure_count += 1
                log.warning("read replica unreachable: shard=%s replica=%s",
                            owner.shard_id, replica["replica_id"])
                continue
            if response.status_code in (502, 503, 504):
                failure_count += 1
                log.warning("read replica error: shard=%s replica=%s status=%s",
                            owner.shard_id, replica["replica_id"], response.status_code)
                continue
            try:
                payload = response.json()
            except ValueError:
                failure_count += 1
                log.warning("read replica returned non-JSON: replica=%s", replica["replica_id"])
                continue
            if response.status_code == 200 and (
                not isinstance(payload, dict)
                or payload.get("account_id") != account_id
                or payload.get("shard") != owner.shard_id
                or payload.get("served_by") != replica["replica_id"]
            ):
                failure_count += 1
                log.error("read replica identity mismatch: shard=%s replica=%s",
                          owner.shard_id, replica["replica_id"])
                continue
            if not isinstance(payload, dict):
                raise HTTPException(status_code=502, detail="invalid replica response")
            log.info("read routed: account=%s shard=%s replica=%s retries=%s",
                     account_id, owner.shard_id, replica["replica_id"], failure_count)
            # Application errors (404/409/428) are authoritative; do not
            # try another shard, and do not conceal stale ownership epochs.
            return JSONResponse(status_code=response.status_code, content=payload)
    raise HTTPException(status_code=503,
                        detail=f"all healthy candidates failed for shard {owner.shard_id}")

# Stage 05 write operations: a single selected replica, NO automatic POST retry.
# If the connection is lost after dispatch, return UNKNOWN; retry with the same
# request_id (safe within a shard) or query operation status before retrying.
from shared.banking_contract import WriteRequest, TransferRequest


async def forward_write(shard_id: str, epoch: int, path: str,
                        payload: dict, request_id: str):
    replica = (await ordered_healthy(shard_id))[0]
    headers = {"X-Expected-Shard": shard_id, "X-Routing-Epoch": str(epoch)}
    url = f"http://{replica['hostname']}:{replica['port']}{path}"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(16.0, connect=2.0), trust_env=False) as client:
            response = await client.post(url, json=payload, headers=headers)
    except httpx.RequestError as exc:
        log.warning("write outcome unknown request_id=%s replica=%s reason=%s",
                    request_id, replica["replica_id"], type(exc).__name__)
        return JSONResponse(status_code=503, content={
            "detail": "write outcome unknown; inspect status or retry SAME request_id",
            "request_id": request_id, "outcome": "UNKNOWN"})
    try:
        data = response.json()
    except ValueError:
        return JSONResponse(status_code=502, content={
            "detail": "non-JSON response; operation outcome unknown",
            "request_id": request_id, "outcome": "UNKNOWN"})
    if not isinstance(data, dict):
        return JSONResponse(status_code=502, content={
            "detail": "malformed replica response", "request_id": request_id,
            "outcome": "UNKNOWN"})
    if response.status_code == 200 and (
        data.get("shard") != shard_id or data.get("served_by") != replica["replica_id"]
        or data.get("request_id") != request_id
    ):
        log.error("write response identity mismatch request_id=%s", request_id)
        return JSONResponse(status_code=502, content={
            "detail": "replica identity mismatch; operation outcome unknown",
            "request_id": request_id, "outcome": "UNKNOWN"})
    return JSONResponse(status_code=response.status_code, content=data)


@app.post("/accounts/{account_id}/deposit")
async def deposit(account_id: str, body: WriteRequest):
    owner = require_owner(account_id)
    return await forward_write(owner.shard_id, owner.epoch,
                               f"/internal/accounts/{account_id}/deposit",
                               body.model_dump(), body.request_id)


@app.post("/accounts/{account_id}/withdraw")
async def withdraw(account_id: str, body: WriteRequest):
    owner = require_owner(account_id)
    return await forward_write(owner.shard_id, owner.epoch,
                               f"/internal/accounts/{account_id}/withdraw",
                               body.model_dump(), body.request_id)


@app.post("/transfers")
async def transfer(body: TransferRequest):
    if body.from_account == body.to_account:
        raise HTTPException(status_code=422, detail="source and target must differ")
    source, destination = require_owner(body.from_account), require_owner(body.to_account)
    if source.shard_id != destination.shard_id:
        return await forward_coordinator("/internal/transfers/cross", body.model_dump(),
                                         request_id=body.request_id, method="POST")
    if source.epoch != destination.epoch:
        raise HTTPException(status_code=409, detail="local transfer crosses different ownership epochs")
    return await forward_write(source.shard_id, source.epoch,
                               "/internal/transfers/local", body.model_dump(), body.request_id)


@app.get("/accounts/{account_id}/operations/{request_id}")
async def local_status(account_id: str, request_id: str):
    """Shard-local result lookup only; cross-shard/global recovery comes later."""
    owner = require_owner(account_id)
    replicas = await ordered_healthy(owner.shard_id)
    headers = {"X-Expected-Shard": owner.shard_id, "X-Routing-Epoch": str(owner.epoch)}
    async with httpx.AsyncClient(timeout=httpx.Timeout(5.0), trust_env=False) as client:
        for replica in replicas:
            try:
                response = await client.get(
                    f"http://{replica['hostname']}:{replica['port']}"
                    f"/internal/accounts/{account_id}/operations/{request_id}",
                    headers=headers)
                return JSONResponse(status_code=response.status_code, content=response.json())
            except (httpx.RequestError, ValueError):
                continue
    raise HTTPException(status_code=503, detail="status currently unavailable")


# Stage 06: Gateway invokes the durable coordinator for cross-shard transfers.
# Unknown coordinator outcomes are returned as UNKNOWN, never "transfer failed".
import os


async def forward_coordinator(path: str, payload: dict | None = None,
                              request_id: str | None = None, method: str = "GET"):
    base = os.environ["COORDINATOR_URL"].rstrip("/")
    token = os.environ["INTERNAL_2PC_TOKEN"]
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(24.0, connect=2.0), trust_env=False) as client:
            if method == "POST":
                response = await client.post(base + path, json=payload,
                                             headers={"X-Internal-Token": token})
            else:
                response = await client.get(base + path,
                                            headers={"X-Internal-Token": token})
            return JSONResponse(status_code=response.status_code, content=response.json())
    except (httpx.RequestError, ValueError):
        return JSONResponse(status_code=503, content={
            "detail": "coordinator unavailable or reply uncertain; query request status",
            "status": "UNKNOWN", "request_id": request_id,
        })


@app.get("/transactions/{tx_id}")
async def global_transaction_status(tx_id: str):
    from uuid import UUID
    try:
        txid = str(UUID(tx_id))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid tx_id") from exc
    return await forward_coordinator(f"/internal/transactions/{txid}")


@app.get("/transfers/requests/{request_id}")
async def cross_request_status(request_id: str):
    from urllib.parse import quote
    return await forward_coordinator(f"/internal/requests/{quote(request_id, safe='')}",
                                     request_id=request_id)


# Stage 06: Gateway invokes durable coordinator for cross-shard transfers.
import os


async def forward_coordinator(path: str, payload: dict | None = None,
                              request_id: str | None = None, method: str = "GET"):
    base = os.environ["COORDINATOR_URL"].rstrip("/")
    token = os.environ["INTERNAL_2PC_TOKEN"]
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(24.0, connect=2.0), trust_env=False) as client:
            if method == "POST":
                response = await client.post(base + path, json=payload,
                                             headers={"X-Internal-Token": token})
            else:
                response = await client.get(base + path,
                                            headers={"X-Internal-Token": token})
            return JSONResponse(status_code=response.status_code, content=response.json())
    except (httpx.RequestError, ValueError):
        return JSONResponse(status_code=503, content={
            "detail": "coordinator unavailable or reply uncertain; query request status",
            "status": "UNKNOWN", "request_id": request_id,
        })


@app.get("/transactions/{tx_id}")
async def global_transaction_status(tx_id: str):
    from uuid import UUID
    try:
        txid = str(UUID(tx_id))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid tx_id") from exc
    return await forward_coordinator(f"/internal/transactions/{txid}")


@app.get("/transfers/requests/{request_id}")
async def cross_request_status(request_id: str):
    from urllib.parse import quote
    return await forward_coordinator(f"/internal/requests/{quote(request_id, safe='')}",
                                     request_id=request_id)
