"""Stage 04 shard-aware read routing with healthy replica failover.

The registry is discovery-only. The Gateway probes *each* candidate to confirm
it is alive and matches the expected shard and replica identity. On read-only
GET, a failed selected replica can be retried on an alternate healthy replica
within the same shard. All money-changing APIs arrive in later stages and must
not copy this automatic retry behavior without idempotency keys.
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
app = FastAPI(title="Shard-Aware Gateway / Stage 04")
_balancer = RoundRobin()


@app.get("/health")
def health():
    return {"status": "ok", "service": "gateway", "stage": "04"}


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
