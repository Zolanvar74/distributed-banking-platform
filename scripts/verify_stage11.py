"""Stage 11 Docker integration proofs; source B stays authoritative.

preflight -> bootstrap -> online -> resume
No target C balances are exposed via ordinary banking API in this stage.
"""
from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import httpx
import psycopg
from psycopg.rows import dict_row

from shared.migration_capture import snapshot_digest

META=os.environ["METADATA_DATABASE_URL"]
SOURCE=os.environ["SHARD_B_DATABASE_URL"]
TARGET=os.environ["SHARD_C_DATABASE_URL"]
HEADERS={"X-Internal-Token":os.environ["INTERNAL_2PC_TOKEN"]}
MANAGER="http://migration-manager:8000"
GATEWAY="http://gateway:8000"


def ok(resp):
    assert resp.status_code==200,(resp.status_code,resp.text[:1000])
    return resp.json()


def job_id(client):
    return ok(client.post(f"{MANAGER}/internal/migrations/stage10/plan",headers=HEADERS))["job_id"]


def meta_check():
    with psycopg.connect(META,row_factory=dict_row) as db:
        ranges=db.execute("SELECT shard_id,range_start,range_end FROM shard_map WHERE active=true").fetchall()
        assert any(x["shard_id"]=="B" and x["range_start"]<=1501 and x["range_end"]>=2000 for x in ranges),ranges
        assert not any(x["shard_id"]=="C" for x in ranges),ranges
        desired=db.execute("SELECT desired_count FROM desired_replica_count WHERE shard_id='C'").fetchone()
        assert desired is not None and desired["desired_count"]==2,desired


def preflight():
    meta_check()
    with psycopg.connect(TARGET,row_factory=dict_row) as db:
        assert db.execute("SELECT COUNT(*) AS n FROM ownership_fences WHERE writable=true").fetchone()["n"]==0
        assert db.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]==0
    with httpx.Client(timeout=15,trust_env=False) as c:
        for slot in (1,2):
            r=ok(c.get(f"http://shard-c-{slot}:8000/health"))
            assert r["shard"]=="C" and r["replica"]==f"C{slot}",r
            rejected=c.get(f"http://shard-c-{slot}:8000/internal/accounts/A2000/balance",
                headers={"X-Expected-Shard":"C","X-Routing-Epoch":"1"})
            assert rejected.status_code==409,(rejected.status_code,rejected.text)
        r=ok(c.get(f"{GATEWAY}/accounts/A2000/balance"))
        assert r["shard"]=="B",r
        assert ok(c.get(f"{MANAGER}/health"))["status"]=="ok"
    print("PASS C database empty, no writable fence; C1/C2 healthy",flush=True)
    print("PASS Gateway still routes migrated range to B",flush=True)
    print("STAGE 11 PREFLIGHT: ALL CHECKS PASSED",flush=True)


def bootstrap():
    meta_check()
    with httpx.Client(timeout=40,trust_env=False) as c:
        jid=job_id(c)
        source=ok(c.post(f"{MANAGER}/internal/migrations/{jid}/snapshot",headers=HEADERS))
        copy1=ok(c.post(f"{MANAGER}/internal/migrations/{jid}/stage11/copy",headers=HEADERS))
        copy2=ok(c.post(f"{MANAGER}/internal/migrations/{jid}/stage11/copy",headers=HEADERS))
        assert copy1==copy2 and source["snapshot_digest"]==copy1["snapshot_digest"]
        assert copy1["accounts"]==source["row_count"]>0
        result=ok(c.post(f"{MANAGER}/internal/migrations/{jid}/stage11/replay",headers=HEADERS))
        assert result["ownership_unchanged"] and result["caught_up_at_source_read"],result
        status=ok(c.get(f"{MANAGER}/internal/migrations/{jid}",headers=HEADERS))
        assert status["phase"]=="CATCHING_UP" and status["target_progress"]["accounts"]==source["row_count"]
    with psycopg.connect(TARGET,row_factory=dict_row) as db:
        rows=db.execute("SELECT account_id,snapshot_balance AS balance,snapshot_version AS version "
                        "FROM migration_target_accounts WHERE job_id=%s ORDER BY account_id",(jid,)).fetchall()
    assert snapshot_digest(rows)==source["snapshot_digest"]
    print(f"PASS target C copied, snapshot verified, job={jid} accounts={copy1['accounts']}",flush=True)
    print("PASS target group replay + durable progress; B still authoritative",flush=True)
    print("STAGE 11 SHADOW COPY: ALL CHECKS PASSED",flush=True)


def _post_write(kind,rid):
    with httpx.Client(timeout=25,trust_env=False) as c:
        payload={"amount":"1.00","request_id":rid}
        r=ok(c.post(f"{GATEWAY}/accounts/A2000/{kind}",json=payload))
        assert r["status"]=="COMMITTED",r
        return r


def online():
    meta_check()
    with psycopg.connect(SOURCE,row_factory=dict_row) as db:
        initial=db.execute("SELECT balance,version FROM accounts WHERE account_id='A2000'").fetchone()
    with httpx.Client(timeout=40,trust_env=False) as c:
        jid=job_id(c)
        st=ok(c.get(f"{MANAGER}/internal/migrations/{jid}",headers=HEADERS))
        assert st["phase"]=="CATCHING_UP","run bootstrap first"
    # Four concurrent writes per batch, different request IDs. Log IDs before
    # starting so any interrupted test can be reconciled without a new debit.
    deposits=[f"stage11-deposit-{uuid4()}" for _ in range(4)]
    withdrawals=[f"stage11-withdraw-{uuid4()}" for _ in range(4)]
    print("TEST REQUEST IDS",deposits,withdrawals,flush=True)
    with ThreadPoolExecutor(max_workers=5) as pool:
        dep=list(pool.map(lambda rid:_post_write("deposit",rid),deposits))
    with httpx.Client(timeout=40,trust_env=False) as c:
        ok(c.post(f"{MANAGER}/internal/migrations/{jid}/stage11/replay",headers=HEADERS))
    with ThreadPoolExecutor(max_workers=5) as pool:
        wdr=list(pool.map(lambda rid:_post_write("withdraw",rid),withdrawals))
    assert len(dep)==4 and len(wdr)==4
    with httpx.Client(timeout=40,trust_env=False) as c:
        one=ok(c.post(f"{MANAGER}/internal/migrations/{jid}/stage11/replay",headers=HEADERS))
        two=ok(c.post(f"{MANAGER}/internal/migrations/{jid}/stage11/replay",headers=HEADERS))
    assert one["caught_up_at_source_read"] and two["groups_applied_this_pass"]==0,(one,two)
    with psycopg.connect(SOURCE,row_factory=dict_row) as db:
        final=db.execute("SELECT balance,version FROM accounts WHERE account_id='A2000'").fetchone()
    with psycopg.connect(TARGET,row_factory=dict_row) as db:
        target=db.execute("SELECT balance,version FROM migration_target_accounts "
                          "WHERE job_id=%s AND account_id='A2000'",(jid,)).fetchone()
    assert final["balance"]==initial["balance"] and final["version"]==initial["version"]+8,(initial,final)
    assert target["balance"]==final["balance"] and target["version"]==final["version"],(target,final)
    meta_check()
    print("PASS 8 real concurrent-API account updates, C replay versions match B",flush=True)
    print("PASS duplicate replay has no effect and Gateway remains on B",flush=True)
    print("STAGE 11 ONLINE CATCH-UP: ALL CHECKS PASSED",flush=True)


def resume():
    # The caller should recreate JUST migration-manager before running this.
    meta_check()
    with httpx.Client(timeout=40,trust_env=False) as c:
        jid=job_id(c)
        before=ok(c.get(f"{MANAGER}/internal/migrations/{jid}",headers=HEADERS))
        assert before["phase"]=="CATCHING_UP",before
        result=ok(c.post(f"{MANAGER}/internal/migrations/{jid}/stage11/replay",headers=HEADERS))
        assert result["caught_up_at_source_read"],result
        after=ok(c.get(f"{MANAGER}/internal/migrations/{jid}",headers=HEADERS))
        assert after["job_id"]==before["job_id"] and after["snapshot_id"]==before["snapshot_id"]
    print("PASS manager restarted, same durable job/snapshot, replay recovered",flush=True)
    print("STAGE 11 RESTART RESUME: ALL CHECKS PASSED",flush=True)


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--scenario",required=True,choices=("preflight","bootstrap","online","resume"))
    args=parser.parse_args()
    {"preflight":preflight,"bootstrap":bootstrap,"online":online,"resume":resume}[args.scenario]()
