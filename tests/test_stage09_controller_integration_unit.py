"""Mocked controller crash tests; run inside the project Docker image."""
from __future__ import annotations

import pytest
pytest.importorskip("psycopg")

from shared.healing_contract import Slot
from services.self_healing import controller

class _Result:
    def __init__(self, row): self.row = row
    def fetchone(self): return self.row

class FakeDB:
    def __init__(self): self.queries=[]; self.armed=True
    def execute(self, query, params=None):
        self.queries.append((query,params))
        if not self.armed: return _Result(None)
        self.armed=False
        return _Result({"experiment_id": "00000000-0000-0000-0000-000000000001"})

class SimulatedExit(Exception): pass


def test_controlled_crash_fires_once(monkeypatch):
    db=FakeDB(); slot=Slot("B",2)
    monkeypatch.setattr(controller, "FAULTS_ENABLED", True)
    monkeypatch.setattr(controller.logging, "shutdown", lambda: None)
    def die(code):
        assert code == 93
        raise SimulatedExit
    monkeypatch.setattr(controller.os, "_exit", die)
    with pytest.raises(SimulatedExit):
        controller.controlled_crash(db,slot,2,"AFTER_RESERVE")
    controller.controlled_crash(db,slot,2,"AFTER_RESERVE")  # already marked FIRED
    assert len(db.queries)==2


def test_fault_disabled_has_no_db_side_effect(monkeypatch):
    db=FakeDB()
    monkeypatch.setattr(controller, "FAULTS_ENABLED", False)
    controller.controlled_crash(db, Slot("A",1),2,"AFTER_CREATE")
    assert not db.queries


def test_invalid_point_never_crashes(monkeypatch):
    monkeypatch.setattr(controller,"FAULTS_ENABLED",True)
    with pytest.raises(ValueError):
        controller.controlled_crash(FakeDB(),Slot("B",2),2,"UNKNOWN")
