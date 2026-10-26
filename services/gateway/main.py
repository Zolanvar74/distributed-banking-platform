"""Stage-03 shard-aware read routing with strict replica identity checks."""
import itertools

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from shared.metadata import Owner, active_shard_map, get_owner, list_replicas

app = FastAPI(title="Shard-Aware Gateway / Stage 03")
_counters: dict[str, itertools.count] = {}


@app.get("/health")
def health():
    return {"status": "ok", "service": "gateway", "stage": "03"}


async def choose_replica(shard_id: str) -> dict:
    candidates = list_replicas(shard_id)
    healthy = []
    async with httpx.AsyncClient(timeout=1.5, trust_env=False) as client:
        for replica in candidates:
            url = f"http://{replica['hostname']}:{replica['port']}/health"
            try:
                response = await client.get(url)
                if response.status_code != 200:
                    continue
                identity = response.json()
                # A stale/mistaken Registry row must not send traffic to a
                # healthy container of a different shard or replica identity.
                if (identity.get("status") == "ok"
                        and identity.get("shard") == shard_id
                        and identity.get("replica") == replica["replica_id"]):
                    healthy.append(replica)
            except (httpx.RequestError, ValueError):
                continue
    if not healthy:
        raise HTTPException(status_code=503, detail=f"no healthy replicas for shard {shard_id}")
    counter = _counters.setdefault(shard_id, itertools.count())
    return healthy[next(counter) % len(healthy)]


def require_owner(account_id: str) -> Owner:
    owner = get_owner(account_id)
    if owner is None:
        raise HTTPException(status_code=404, detail="account is outside the current shard map")
    return owner


@app.get("/debug/shard-map")
def shard_map_snapshot():
    """Local-demo inspection endpoint, not a privileged mutation API."""
    return active_shard_map()


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
        async with httpx.AsyncClient(timeout=3, trust_env=False) as client:
            response = await client.get(
                url,
                headers={"X-Expected-Shard": owner.shard_id,
                         "X-Routing-Epoch": str(owner.epoch)},
            )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail="selected replica became unavailable; retry request") from exc
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="replica returned invalid JSON") from exc
    if response.status_code == 200 and (
        not isinstance(payload, dict)
        or payload.get("account_id") != account_id
        or payload.get("shard") != owner.shard_id
        or payload.get("served_by") != replica["replica_id"]
    ):
        raise HTTPException(status_code=502, detail="replica returned inconsistent routing identity")
    return JSONResponse(status_code=response.status_code, content=payload)
