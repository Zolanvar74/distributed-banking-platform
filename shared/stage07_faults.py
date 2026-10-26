"""Stage 07 local-only, durable, one-shot crash trigger.

Only compiled into this demo build and disabled by default. The fault row is
committed *before* os._exit; a recovered process sees TRIPPED and cannot crash
again on this transaction. NEVER enable this outside an isolated local lab.
"""
from __future__ import annotations

import os
import logging
from uuid import UUID

import psycopg

log = logging.getLogger("banking.coordinator.faults")


def after_first_commit_ack(dsn: str, txid: UUID) -> None:
    if os.environ.get("STAGE07_ENABLE_FAULTS") != "1":
        return
    with psycopg.connect(dsn) as conn:
        # Avoid checks based solely on in-memory counters; verify durable ACKs.
        fired = conn.execute(
            """UPDATE stage07_fault_injections AS fault
               SET status='TRIPPED', fired_at=clock_timestamp(), tx_id=tx.tx_id
               FROM transactions AS tx
               WHERE fault.request_id=tx.client_request_id
                 AND tx.tx_id=%s AND fault.status='ARMED'
                 AND tx.decision='COMMIT' AND tx.state='COMMIT_DECIDED'
                 AND (SELECT count(*) FROM transaction_participants AS p
                      WHERE p.tx_id=tx.tx_id AND p.completion_ack)=1
                 AND (SELECT count(*) FROM transaction_participants AS p
                      WHERE p.tx_id=tx.tx_id)=2
               RETURNING fault.request_id""",
            (txid,),
        ).fetchone()
        if fired:
            conn.execute(
                "INSERT INTO transaction_recovery_events(tx_id,event_type,details) "
                "VALUES(%s,'STAGE07_CRASH_AFTER_ONE_ACK','{}'::jsonb)", (txid,),
            )
    if fired:
        log.critical("Stage 07 INJECTED PROCESS CRASH tx=%s after 1 ACK", txid)
        os._exit(74)  # simulate SIGKILL-style abrupt loss; DO NOT raise/recover.
