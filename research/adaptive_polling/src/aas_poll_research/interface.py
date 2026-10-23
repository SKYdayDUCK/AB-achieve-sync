"""PollPolicy contract mirrored from PATCH-01 §2.2 (research copy, no production dependency).

The production package lives at <repo>/src (aas.b.scheduling.policy).
We deliberately do not import it here so the research code never couples to production state;
tests/test_simulator.py::TestProductionCompat checks structural compatibility against it (read-only).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional, Protocol, Tuple

RESEARCH_STATUS = "research_candidate/not_for_production"


class PolicyConfigError(ValueError):
    """Invalid policy configuration."""


class PolicyStateError(ValueError):
    """Invalid / incompatible exported state."""


@dataclass(frozen=True)
class DiscoveryObservation:
    """Exactly the production fields: one complete, *successful* READY query."""
    observed_ready_events: int
    newly_enqueued_batches: int
    queried_pages: int
    completed_at_monotonic: float


@dataclass(frozen=True)
class ResearchDiscoveryObservation(DiscoveryObservation):
    """Production fields + optional research fields allowed by PATCH-01 §2.2 note.

    snapshot_at : A-clock instant at which the READY listing is complete (ready_at <= snapshot_at).
    ready_at    : A-clock READY instants of the batches newly discovered by this query.
    """
    snapshot_at: Optional[float] = None
    ready_at: Tuple[float, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class PollContext:
    now_monotonic: float
    has_pending_transfers: bool
    shutdown_requested: bool


class PollPolicy(Protocol):
    policy_name: str

    def validate_config(self) -> None: ...
    def observe(self, event: DiscoveryObservation) -> None: ...
    def next_delay_seconds(self, context: PollContext) -> float: ...
    def export_state(self) -> dict: ...
    def import_state(self, state: dict) -> None: ...


def finite(x: float) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


def clamp(x: float, lo: float, hi: float) -> float:
    if not math.isfinite(x):
        return hi
    return lo if x < lo else hi if x > hi else x
