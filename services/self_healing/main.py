"""Read-only controller status API plus self-healing background reconciler."""
from __future__ import annotations

import logging
import threading
import os
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, HTTPException, Header
from psycopg.rows import dict_row

from services.self_healing.controller import run_controller, controller_cycle_healthy

log = logging.getLogger("banking.self_healing")


_worker: threading.Thread | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker
    # Dedicated daemon thread exits when this controller container terminates.
    _worker = threading.Thread(target=run_controller, name="healing-reconciler", daemon=True)
    _worker.start()
    yield


app = FastAPI(title="Custom Self-Healing Controller / Stage 09", lifespan=lifespan)


@app.get("/health")
def health():
    if _worker is None or not _worker.is_alive() or not controller_cycle_healthy():
        raise HTTPException(status_code=503, detail="controller reconciliation loop not healthy")
    return {"status": "ok", "service": "self-healing", "stage": "09",
            "faults_enabled": os.environ.get("STAGE09_ENABLE_FAULTS") == "1"}


@app.get("/status")
def status():
    try:
        with psycopg.connect(os.environ["METADATA_DATABASE_URL"], row_factory=dict_row) as db:
            desired = list(db.execute("SELECT * FROM desired_replica_count ORDER BY shard_id").fetchall())
            replicas = list(db.execute("""SELECT replica_id,shard_id,slot,hostname,
                      generation,lifecycle,container_id,last_heartbeat
                      FROM replica_registry ORDER BY shard_id,slot""").fetchall())
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="metadata unavailable") from exc
    return {"desired": desired, "replicas": replicas}


# Internal, authenticated Docker evidence: only this controller has docker.sock.
# The API is intentionally NOT published on the host.
@app.get("/internal/slots/{shard}/{number}/containers")
def slot_containers(shard: str, number: int,
                    x_internal_token: str | None = Header(
                        default=None, alias="X-Internal-Token")):
    import hmac
    from shared.healing_contract import Slot
    from services.self_healing.controller import containers_for
    try:
        slot = Slot(shard, number)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid slot") from exc
    expected = os.environ.get("INTERNAL_2PC_TOKEN", "")
    if not expected or x_internal_token is None or not hmac.compare_digest(expected, x_internal_token):
        raise HTTPException(status_code=403, detail="internal authentication required")
    import docker
    try:
        client = docker.DockerClient(base_url="unix://var/run/docker.sock", timeout=8)
        result = []
        for c in containers_for(client, slot):
            c.reload()
            result.append({"name": c.name, "id": c.id, "status": c.status,
                           "generation": int(c.labels["banking.generation"]),
                           "managed": c.labels.get("banking.managed") == "true"})
        return {"replica_id": slot.replica_id, "containers": result}
    except Exception as exc:
        log.exception("could not inspect slot containers")
        raise HTTPException(status_code=503, detail="docker inspection unavailable") from exc
