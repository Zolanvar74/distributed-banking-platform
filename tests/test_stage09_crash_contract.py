"""No-Docker contract tests for Stage 09 crash safety/evidence."""
from __future__ import annotations

import pytest

from shared.healing_contract import Slot, check_container_evidence, replacement_name, should_replace


def evidence(gen=3, old_status="exited", fresh_status="running"):
    return [
        {"generation":gen-1,"id":"older","status":old_status,"name":"old"},
        {"generation":gen,"id":"new","status":fresh_status,"name":"new"},
    ]


def test_single_current_running_with_retired_old_allowed():
    check_container_evidence(evidence(), 3, "new")


def test_stale_running_is_rejected():
    with pytest.raises(AssertionError, match="running stale"):
        check_container_evidence(evidence(old_status="running"), 3, "new")


def test_duplicate_generation_rejected():
    e=evidence()
    e.append({"generation":3,"id":"other","status":"exited"})
    with pytest.raises(AssertionError, match="expected one current"):
        check_container_evidence(e,3,"new")


def test_current_stopped_is_not_accepted_as_healed():
    with pytest.raises(AssertionError):
        check_container_evidence(evidence(fresh_status="exited"),3,"new")


def test_registry_id_must_match_docker_id():
    with pytest.raises(AssertionError, match="registry container_id"):
        check_container_evidence(evidence(),3,"different")


def test_missing_current_generation_rejected():
    with pytest.raises(AssertionError):
        check_container_evidence(evidence(),5,"new")


def test_new_generation_is_stable_under_replay():
    slot=Slot("B",2)
    assert replacement_name(slot,3)==replacement_name(slot,3)
    assert should_replace(3,"REPLACING",False,False,2,2,False)=="CREATE_CURRENT"


def test_different_generations_have_distinct_names():
    assert replacement_name(Slot("B",2),2)!=replacement_name(Slot("B",2),3)


def test_start_grace_waits_for_existing_created_container():
    assert should_replace(3,"REPLACING",True,False,2,2,True)=="WAIT"
