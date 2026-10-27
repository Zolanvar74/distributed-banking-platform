"""Unit tests exercise names, incarnation safety, crash decisions without Docker."""
from dataclasses import dataclass
import pytest
from shared.healing_contract import Slot, labels_for, labels_match, replacement_name, should_replace


@dataclass
class FakeContainer:
    labels: dict


def test_names_unique_and_deterministic_for_recovery():
    s = Slot("A", 1)
    assert replacement_name(s, 2) == replacement_name(Slot("A", 1), 2)
    assert replacement_name(s, 2) != replacement_name(s, 3)
    assert replacement_name(s, 2) != replacement_name(Slot("B", 1), 2)


def test_unsafe_slot_rejected():
    with pytest.raises(ValueError): Slot("X", 1)
    with pytest.raises(ValueError): Slot("A", 0)
    with pytest.raises(ValueError): replacement_name(Slot("A", 1), 1)


def test_label_based_adoption_never_touches_other_project():
    s = Slot("A", 1)
    a = FakeContainer(labels_for(s, 2, True))
    assert labels_match(a, s, 2)
    assert not labels_match(a, Slot("B", 1))
    a.labels["banking.project"] = "other-project"
    assert not labels_match(a, s)


def test_crash_after_reservation_reuses_existing_generation():
    assert should_replace(2, "REPLACING", False, False, 2, 2, False) == "CREATE_CURRENT"


def test_failed_replica_creates_new_generation_after_threshold():
    assert should_replace(1, "HEALTHY", True, False, 2, 2, False) == "CREATE_NEXT"
    assert should_replace(1, "HEALTHY", True, False, 1, 2, False) == "WAIT"


def test_start_grace_prevents_container_churn():
    assert should_replace(2, "REPLACING", True, False, 100, 2, True) == "WAIT"


def test_healthy_current_replica_never_replaced():
    assert should_replace(2, "REGISTERED", True, True, 2, 2, False) == "KEEP"
