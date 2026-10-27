"""Non-Docker stage-14 checks: fault injection strictly opt-in and well-scoped."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from shared.stage14_faults import POINTS, crash_if_armed


def test_exact_four_fault_points():
    assert POINTS == {
        'SNAPSHOT_PERSISTED', 'DELTA_GROUP_APPLIED',
        'FENCING_GATE_CLOSED', 'MAP_ACTIVATED',
    }


def test_disabled_faults_never_open_database():
    with patch.dict('os.environ', {'STAGE14_ENABLE_FAULTS':'0'},clear=False):
        with patch('shared.stage14_faults.psycopg.connect',side_effect=AssertionError('must not connect')):
            crash_if_armed('not-needed','SNAPSHOT_PERSISTED')


def test_unknown_point_rejected_even_when_disabled():
    with pytest.raises(ValueError):
        crash_if_armed('unused','BAD_POINT')


def test_missing_metadata_dsn_fails_closed_when_faults_enabled():
    with patch.dict('os.environ',{'STAGE14_ENABLE_FAULTS':'1'},clear=True):
        with pytest.raises(RuntimeError,match='metadata'):
            crash_if_armed('unused','MAP_ACTIVATED')


def test_compose_and_manager_reference_faults():
    from pathlib import Path
    root=Path(__file__).parents[1]
    compose=(root/'compose.yaml').read_text()
    manager=(root/'services/migration_manager/main.py').read_text()
    target=(root/'shared/migration_target.py').read_text()
    cutover=(root/'shared/migration_cutover.py').read_text()
    assert 'STAGE14_ENABLE_FAULTS: ${STAGE14_ENABLE_FAULTS:-0}' in compose
    assert 'crash_if_armed(job_id, "SNAPSHOT_PERSISTED")' in manager
    assert 'crash_if_armed(job_id, "DELTA_GROUP_APPLIED")' in target
    assert 'crash_if_armed(job_id, "FENCING_GATE_CLOSED")' in cutover
    assert 'crash_if_armed(job_id, "MAP_ACTIVATED")' in cutover
