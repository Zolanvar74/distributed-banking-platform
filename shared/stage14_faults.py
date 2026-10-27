"""Stage 14 *one-shot* process-kill injection; disabled by default.

Only use in a disposable, separately named Docker Compose test project.
The status transition commits *before* os._exit(), so restart won't repeat it.
This module never changes banking balances, ownership, or transaction decisions.
"""
from __future__ import annotations

import logging
import os
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

POINTS = frozenset({
    "SNAPSHOT_PERSISTED", "DELTA_GROUP_APPLIED",
    "FENCING_GATE_CLOSED", "MAP_ACTIVATED",
})


def crash_if_armed(job_id: UUID, point: str) -> None:
    if point not in POINTS:
        raise ValueError("unrecognized migration fault point")
    if os.environ.get("STAGE14_ENABLE_FAULTS") != "1":
        return
    dsn = os.environ.get("METADATA_DATABASE_URL")
    if not dsn:
        raise RuntimeError("cannot inject crash without durable metadata")
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=8) as db:
        with db.transaction():
            hit = db.execute("""UPDATE stage14_fault_experiments SET state='FIRED',
                    fired_at=clock_timestamp() WHERE experiment_id=(
                        SELECT experiment_id FROM stage14_fault_experiments
                        WHERE job_id=%s AND point=%s AND state='ARMED'
                        ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED)
                    RETURNING experiment_id""", (job_id, point)).fetchone()
    if hit:
        logging.getLogger("banking.stage14.fault").critical(
            "STAGE14 CONTROLLED CRASH point=%s job=%s experiment=%s",
            point, job_id, hit["experiment_id"])
        logging.shutdown()
        os._exit(94)  # true process termination, not exception handling
