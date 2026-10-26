import itertools

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from shared.metadata import Owner, get_owner, list_replicas

app = FastAPI(title="Shard-Aware Gateway / Stage 02")
_counters: dict[str, itertools.count] = {}


@app.get("/health")
def health():
    return {"status": "ok", "service": "gateway", "stage": "02"}


async def choose_replica(shard_id: str) -> dict:
    candidates = list_replicas(shard_id)
    healthy = []
    async with httpx.AsyncClient(timeout=1.5) as client:
        for replica in candidates:
            url = f"http://{replica['hostname']}:{replica['port']}/health"
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    healthy.append(replica)
            except httpx.RequestError:
                continue
    if not healthy:
        raise HTTPException(status_code=503, detail=f"no healthy replicas for shard {shard_id}")
    # Process-local RR is sufficient for our single gateway at this stage.
    counter = _counters.setdefault(shard_id, itertools.count())
    return healthy[next(counter) % len(healthy)]


def require_owner(account_id: str) -> Owner:
    owner = get_owner(account_id)
    if owner is None:
        raise HTTPException(status_code=404, detail="account is outside the current shard map")
    return owner


@app.get("/debug/route/{account_id}")
async def debug_route(account_id: str):
    owner = require_owner(account_id)
    replica = await choose_replica(owner.shard_id)
    return {
        "account_id": account_id,
        "owner_shard": owner.shard_id,
        "shard_epoch": owner.epoch,
        "selected_replica": replica["replica_id"],
    }


@app.get("/accounts/{account_id}/balance")
async def get_balance(account_id: str):
    owner = require_owner(account_id)
    replica = await choose_replica(owner.shard_id)
    url = (f"http://{replica['hostname']}:{replica['port']}"
           f"/internal/accounts/{account_id}/balance")
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            response = await client.get(url)
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail="selected replica became unavailable; retry request") from exc
    return JSONResponse(status_code=response.status_code, content=response.json())
