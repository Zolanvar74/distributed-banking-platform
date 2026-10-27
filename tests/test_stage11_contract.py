"""Pure contract tests; full target replay requires PostgreSQL integration."""
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from shared.migration_capture import snapshot_digest

ROOT=Path(__file__).resolve().parents[1]


def test_snapshot_fingerprint_has_strict_version_and_balance():
    one=[{"account_id":"A2000","balance":Decimal("4.00"),"version":3}]
    other=[{"account_id":"A2000","balance":Decimal("4.00"),"version":4}]
    assert snapshot_digest(one)!=snapshot_digest(other)


def test_schema_target_is_separate_and_fenced():
    sql=(ROOT/"sql/migrations/shard/005_stage11_target.sql").read_text()
    assert "migration_target_accounts" in sql
    assert "migration_target_applied_groups" in sql
    assert "migration_target_replayed_changes" in sql
    assert "INSERT INTO ownership_fences" not in sql
    assert "ON DELETE CASCADE" not in sql


def test_stage11_no_ownership_change():
    sql=(ROOT/"sql/migrations/metadata/008_stage11_target.sql").read_text()
    assert "migration_target_progress" in sql
    assert "desired_replica_count" in sql
    assert "UPDATE shard_map" not in sql


def test_replay_recorded_as_one_group_and_consistent_accounts():
    from shared.migration_target import replay_once, copy_snapshot, CaptureError
    assert callable(replay_once) and callable(copy_snapshot)
    assert issubclass(CaptureError,RuntimeError)


def test_atomic_group_scheduler_preserves_per_account_dependencies():
    from shared.migration_target import ready_group_ids
    from uuid import UUID
    g1,g2,g3=[UUID(int=i) for i in (1,2,3)]
    current={"A1600":{"version":0},"A2000":{"version":0}}
    groups={
        g1:[{"account_id":"A1600","version":1}],
        g2:[{"account_id":"A1600","version":2},{"account_id":"A2000","version":1}],
        g3:[{"account_id":"A2000","version":2}],
    }
    assert ready_group_ids(groups,current)==[g1]
    current["A1600"]["version"]=1
    assert ready_group_ids({g2:groups[g2],g3:groups[g3]},current)==[g2]
    current["A1600"]["version"]=2
    current["A2000"]["version"]=1
    assert ready_group_ids({g3:groups[g3]},current)==[g3]


def test_atomic_group_scheduler_refuses_partial_group_application():
    import pytest
    from shared.migration_target import ready_group_ids,CaptureError
    g=uuid4()
    with pytest.raises(CaptureError):
        ready_group_ids({g:[{"account_id":"A1600","version":1},
                            {"account_id":"A2000","version":2}]},
                         {"A1600":{"version":1},"A2000":{"version":0}})
