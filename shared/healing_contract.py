"""Durable naming and safety rules for the custom Docker self-healing controller."""
from __future__ import annotations

from dataclasses import dataclass
import os

PROJECT = os.environ.get("HEALING_PROJECT", "distributed-banking")
MAX_SLOT = 12


@dataclass(frozen=True)
class Slot:
    shard: str
    number: int

    def __post_init__(self):
        if self.shard not in {"A", "B", "C"} or not 1 <= self.number <= MAX_SLOT:
            raise ValueError("invalid shard / slot")

    @property
    def replica_id(self) -> str:
        return f"{self.shard}{self.number}"


def replacement_name(slot: Slot, generation: int) -> str:
    if generation < 2:
        raise ValueError("replacement generation must be >= 2")
    return f"{PROJECT}-healed-{slot.shard.lower()}{slot.number}-g{generation}"


def labels_for(slot: Slot, generation: int, managed: bool) -> dict[str, str]:
    if generation < 1:
        raise ValueError("generation must be positive")
    return {"banking.project": PROJECT, "banking.role": "replica",
            "banking.shard": slot.shard, "banking.slot": str(slot.number),
            "banking.generation": str(generation),
            "banking.managed": "true" if managed else "false"}


def container_generation(container) -> int | None:
    labels = container.labels
    try:
        gen = int(labels.get("banking.generation", ""))
        slot = int(labels.get("banking.slot", ""))
        if labels.get("banking.project") != PROJECT or labels.get("banking.role") != "replica":
            return None
        Slot(labels["banking.shard"], slot)
        if gen < 1:
            return None
        return gen
    except (ValueError, TypeError, KeyError):
        return None


def labels_match(container, slot: Slot, generation: int | None = None) -> bool:
    gen = container_generation(container)
    return gen is not None and (generation is None or gen == generation) and (
        container.labels.get("banking.shard") == slot.shard
        and container.labels.get("banking.slot") == str(slot.number)
    )


def should_replace(current_generation: int, lifecycle: str, candidate_present: bool,
                   alive: bool, failures: int, threshold: int,
                   within_start_grace: bool) -> str:
    """Return KEEP, WAIT, CREATE_CURRENT or CREATE_NEXT.

    If metadata advanced before a crash, CREATE_CURRENT uses that *same* generation.
    This is the key to not duplicating replacements after controller recovery.
    """
    if candidate_present and alive:
        return "KEEP"
    if candidate_present and within_start_grace and lifecycle == "REPLACING":
        return "WAIT"
    if failures < threshold:
        return "WAIT"
    if not candidate_present and lifecycle == "REPLACING" and current_generation >= 2:
        return "CREATE_CURRENT"
    return "CREATE_NEXT"


def check_container_evidence(containers: list[dict], generation: int,
                             container_id: str) -> None:
    """Validate one active incarnation and no running stale copies.

    Historical exited generations are allowed as auditable evidence. Both
    duplicate current-generation Docker containers and stale RUNNING copies
    are forbidden even if the Gateway would not route to them.
    """
    current = [c for c in containers if c.get("generation") == generation]
    if len(current) != 1:
        raise AssertionError(f"expected one current container; got {current}")
    if current[0].get("id") != container_id:
        raise AssertionError("registry container_id does not match Docker")
    running = [c for c in containers if c.get("status") == "running"]
    if len(running) != 1 or running[0].get("id") != container_id:
        raise AssertionError(f"running stale or duplicate replicas: {running}")
