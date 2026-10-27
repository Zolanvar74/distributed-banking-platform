"""Stage 11 target shadow copy + online replay; Shard B remains owner.

Endpoints bind only to Docker's private network, and mutations require the
internal token. A background task durably advances per-account *observation*
checkpoints. This does NOT mean changes were replayed to Shard C.
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from uuid import UUID, uuid4

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException
from psycopg.rows import dict_row

from shared.migration_target import copy_snapshot, replay_once
from shared.stage14_faults import crash_if_armed
from shared.migration_cutover import cutover_once
from shared.migration_capture import (
    RANGE_END, RANGE_START, CaptureError, create_source_snapshot,
    persist_observations, read_and_validate_source,
)

META = os.environ["METADATA_DATABASE_URL"]
SOURCE = os.environ["SHARD_B_DATABASE_URL"]
TARGET = os.environ["SHARD_C_DATABASE_URL"]
TOKEN = os.environ["INTERNAL_2PC_TOKEN"]
POLL_SECONDS = max(2, int(os.environ.get("MIGRATION_SCAN_SECONDS", "3")))
log = logging.getLogger("migration.capture")
last_success = 0.0
last_error = ""


def require_token(x_internal_token: str | None = Header(default=None, alias="X-Internal-Token")):
    if not TOKEN or x_internal_token is None or not hmac.compare_digest(x_internal_token, TOKEN):
        raise HTTPException(status_code=403, detail="internal token required")


def find_or_plan_job() -> dict:
    with psycopg.connect(META, row_factory=dict_row, connect_timeout=8) as db:
        with db.transaction():
            # Prevent competing plans during concurrent API calls.
            db.execute("SELECT pg_advisory_xact_lock(hashtext('stage10-plan-b-to-c'))")
            exists = db.execute(
                "SELECT * FROM migration_jobs WHERE range_start=%s AND range_end=%s "
                "AND phase<>'COMPLETED' ORDER BY created_at DESC LIMIT 1",
                (RANGE_START, RANGE_END),
            ).fetchone()
            if exists:
                if exists["source_shard"] != "B" or exists["target_shard"] != "C":
                    raise CaptureError("overlapping migration already exists")
                return _job_json(exists)
            owner = db.execute(
                "SELECT shard_id,epoch FROM shard_map WHERE active=true "
                "AND range_start<=%s AND range_end>=%s", (RANGE_START, RANGE_END),
            ).fetchone()
            if not owner or owner["shard_id"] != "B":
                raise CaptureError("B is not the sole active owner of A1501-A2000")
            job_id = uuid4()
            job = db.execute(
                "INSERT INTO migration_jobs(job_id,source_shard,target_shard,"
                "range_start,range_end,epoch_before,phase) "
                "VALUES (%s,'B','C',%s,%s,%s,'PLANNED') RETURNING *",
                (job_id, RANGE_START, RANGE_END, owner["epoch"]),
            ).fetchone()
            db.execute(
                "INSERT INTO migration_recovery_events(job_id,phase,details) "
                "VALUES (%s,'PLANNED',%s::jsonb)",
                (job_id,'{"stage":10,"target_owner_active":false}'),
            )
            return _job_json(job)


def _job_json(row: dict) -> dict:
    return {"job_id": str(row["job_id"]), "source_shard": row["source_shard"],
            "target_shard": row["target_shard"], "range_start": row["range_start"],
            "range_end": row["range_end"], "phase": row["phase"],
            "epoch_before": row["epoch_before"], "epoch_after": row["epoch_after"], "snapshot_id": row["snapshot_id"]}


def get_job(job_id: UUID) -> dict:
    with psycopg.connect(META, row_factory=dict_row, connect_timeout=8) as db:
        job = db.execute("SELECT * FROM migration_jobs WHERE job_id=%s", (job_id,)).fetchone()
    if job is None:
        raise CaptureError("migration job not found")
    if (job["source_shard"],job["target_shard"],job["range_start"],job["range_end"]) != (
            "B","C",RANGE_START,RANGE_END):
        raise CaptureError("this stage supports only B -> C, A1501-A2000")
    return job


def capture_snapshot(job_id: UUID) -> dict:
    job = get_job(job_id)
    if job["phase"] not in ("PLANNED", "SNAPSHOTTING", "TARGET_READY", "CATCHING_UP"):
        raise CaptureError("job phase incompatible with snapshot verification")
    # Snapshot first in SOURCE and persist, then advance metadata. A crash
    # between the two is healed by retrying the same job_id on the next call.
    evidence = create_source_snapshot(SOURCE, job_id, RANGE_START, RANGE_END)
    crash_if_armed(job_id, "SNAPSHOT_PERSISTED")
    with psycopg.connect(META, row_factory=dict_row, connect_timeout=8) as db:
        with db.transaction():
            current = db.execute(
                "SELECT phase,snapshot_id FROM migration_jobs WHERE job_id=%s FOR UPDATE",
                (job_id,),
            ).fetchone()
            if current["phase"] not in ("PLANNED", "SNAPSHOTTING", "TARGET_READY", "CATCHING_UP"):
                raise CaptureError("ownership phase changed during snapshot")
            if current["snapshot_id"] not in (None, str(job_id)):
                raise CaptureError("snapshot identity changed")
            if current["phase"] in ("PLANNED", "SNAPSHOTTING"):
                db.execute(
                    "UPDATE migration_jobs SET snapshot_id=%s,phase='SNAPSHOTTING',"
                    "updated_at=clock_timestamp() WHERE job_id=%s", (str(job_id), job_id),
                )
            if current["snapshot_id"] is None:
                db.execute(
                    "INSERT INTO migration_recovery_events(job_id,phase,details) "
                    "VALUES (%s,'SNAPSHOTTING',%s::jsonb)",
                    (job_id, '{"snapshot_persisted":true,"target_replayed":false}'),
                )
    return {**evidence, "phase": current["phase"] if current["phase"] in ("TARGET_READY", "CATCHING_UP") else "SNAPSHOTTING", "ownership_unchanged": True,
            "target_replayed": False}


def observe_once(job_id: UUID) -> dict:
    get_job(job_id)
    observed, stats = read_and_validate_source(SOURCE, job_id)
    return persist_observations(META, job_id, observed, stats)


def observe_active_jobs() -> None:
    with psycopg.connect(META, row_factory=dict_row, connect_timeout=8) as db:
        rows = db.execute(
            "SELECT job_id FROM migration_jobs WHERE source_shard='B' AND target_shard='C' "
            "AND snapshot_id IS NOT NULL AND phase IN ('SNAPSHOTTING','TARGET_READY','CATCHING_UP')"
        ).fetchall()
    for row in rows:
        observe_once(row["job_id"])




def bootstrap_target(job_id: UUID) -> dict:
    job=get_job(job_id)
    if job["snapshot_id"] != str(job_id) or job["phase"] not in (
            "SNAPSHOTTING","TARGET_READY","CATCHING_UP"):
        raise CaptureError("source snapshot required before target copy")
    evidence=copy_snapshot(SOURCE,TARGET,job_id)
    with psycopg.connect(META,row_factory=dict_row,connect_timeout=8) as db:
        with db.transaction():
            current=db.execute("SELECT phase,snapshot_id FROM migration_jobs "
                               "WHERE job_id=%s FOR UPDATE",(job_id,)).fetchone()
            if current is None or current["snapshot_id"]!=str(job_id) or current["phase"] not in (
                    "SNAPSHOTTING","TARGET_READY","CATCHING_UP"):
                raise CaptureError("target copy cannot advance unexpected job state")
            if current["phase"]=="SNAPSHOTTING":
                db.execute("UPDATE migration_jobs SET phase='TARGET_READY',"
                           "updated_at=clock_timestamp() WHERE job_id=%s",(job_id,))
                db.execute("INSERT INTO migration_recovery_events(job_id,phase,details) "
                           "VALUES (%s,'TARGET_READY','{\"stage\":11,\"owner\":\"B\"}'::jsonb)",
                           (job_id,))
    return {**evidence,"ownership_unchanged":True}


def catch_up_target(job_id: UUID) -> dict:
    job=get_job(job_id)
    if job["phase"] not in ("TARGET_READY","CATCHING_UP","FINAL_REPLAY"):
        raise CaptureError("target snapshot must be fully copied before replay")
    return replay_once(SOURCE,TARGET,META,job_id)


def replay_active_jobs() -> None:
    with psycopg.connect(META,row_factory=dict_row,connect_timeout=8) as db:
        rows=db.execute("SELECT job_id FROM migration_jobs WHERE source_shard='B' "
                        "AND target_shard='C' AND phase='CATCHING_UP'").fetchall()
    for row in rows:
        catch_up_target(row["job_id"])


def resume_pending_cutovers() -> None:
    # The durable phase machine automatically converges after an abrupt
    # manager restart. If prepared xacts or unknown outcomes remain, it
    # remains safely paused and retries (never clears a COMMIT decision).
    with psycopg.connect(META,row_factory=dict_row,connect_timeout=8) as db:
        rows=db.execute(
            "SELECT job_id FROM migration_jobs WHERE phase IN "
            "('FENCING','FINAL_REPLAY','MAP_ACTIVATED') AND source_shard='B' "
            "AND target_shard='C' ORDER BY created_at"
        ).fetchall()
    for row in rows:
        try:
            cutover_once(META,SOURCE,TARGET,row['job_id'])
        except (psycopg.Error,CaptureError) as exc:
            log.warning("cutover recovery deferred job=%s reason=%s",row['job_id'],str(exc)[:250])


async def observer_loop() -> None:
    global last_success, last_error
    while True:
        try:
            await asyncio.to_thread(observe_active_jobs)
            await asyncio.to_thread(replay_active_jobs)
            await asyncio.to_thread(resume_pending_cutovers)
            last_success = time.monotonic()
            last_error = ""
        except Exception as exc:
            last_error = type(exc).__name__ + ": " + str(exc)[:240]
            log.exception("stage10 source observation failed; will retry")
        await asyncio.sleep(POLL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(observer_loop())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="Migration Manager / Stage 12", lifespan=lifespan)


@app.get("/health")
def health():
    from fastapi.responses import JSONResponse
    if last_success == 0 or time.monotonic() - last_success > max(POLL_SECONDS * 6, 30):
        return JSONResponse(status_code=503, content={"status":"degraded", "last_error":last_error})
    return {"status": "ok", "service": "migration-manager",
            "source_capture_poll_seconds": POLL_SECONDS, "shadow_target_enabled": True,
            "stage12_cutover_enabled": True}


@app.post("/internal/migrations/stage10/plan", dependencies=[Depends(require_token)])
def plan():
    try:
        return find_or_plan_job()
    except (psycopg.Error, CaptureError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/internal/migrations/{job_id}/snapshot", dependencies=[Depends(require_token)])
def snapshot(job_id: UUID):
    try:
        return capture_snapshot(job_id)
    except (psycopg.Error, CaptureError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/internal/migrations/{job_id}/observe", dependencies=[Depends(require_token)])
def observe(job_id: UUID):
    try:
        return observe_once(job_id)
    except (psycopg.Error, CaptureError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/internal/migrations/{job_id}", dependencies=[Depends(require_token)])
def status(job_id: UUID):
    try:
        job = get_job(job_id)
        with psycopg.connect(META, row_factory=dict_row, connect_timeout=8) as db:
            counts = db.execute(
                "SELECT COUNT(*)::integer AS accounts, "
                "COALESCE(SUM(observed_version-snapshot_version),0)::bigint AS observed_deltas "
                "FROM migration_source_observations WHERE job_id=%s", (job_id,),
            ).fetchone()
            progress=db.execute(
                "SELECT COUNT(*)::integer AS accounts, "
                "COALESCE(SUM(applied_version-snapshot_version),0)::bigint AS replayed_deltas "
                "FROM migration_target_progress WHERE job_id=%s",(job_id,)
            ).fetchone()
        return {**_job_json(job),"source_capture":dict(counts),
                "target_progress":dict(progress),
                "target_replayed":progress["accounts"]>0,
                "ownership_unchanged":job["phase"] not in ("MAP_ACTIVATED","VERIFYING","COMPLETED")}
    except (psycopg.Error, CaptureError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/internal/migrations/{job_id}/stage11/copy", dependencies=[Depends(require_token)])
def stage11_copy(job_id: UUID):
    try:
        return bootstrap_target(job_id)
    except (psycopg.Error,CaptureError) as exc:
        raise HTTPException(status_code=409,detail=str(exc)) from exc


@app.post("/internal/migrations/{job_id}/stage11/replay", dependencies=[Depends(require_token)])
def stage11_replay(job_id: UUID):
    try:
        return catch_up_target(job_id)
    except (psycopg.Error,CaptureError) as exc:
        raise HTTPException(status_code=409,detail=str(exc)) from exc


@app.post("/internal/migrations/{job_id}/stage12/cutover", dependencies=[Depends(require_token)])
def stage12_cutover(job_id: UUID):
    # Cutover is intentionally not an ordinary user-facing operation. It is a
    # one-way administrative operation with durable fail-closed recovery.
    try:
        return cutover_once(META,SOURCE,TARGET,job_id)
    except (psycopg.Error,CaptureError) as exc:
        log.warning("stage12 cutover deferred job=%s reason=%s",job_id,str(exc)[:250])
        raise HTTPException(status_code=409,detail=f"cutover pending/safely blocked: {exc}") from exc
