"""Production-safe policy registry. A future adaptive model cannot alter transfer state."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol
from aas.common.core import ProtocolError


@dataclass(frozen=True)
class DiscoveryObservation:
    observed_ready_events: int
    newly_enqueued_batches: int
    queried_pages: int
    completed_at_monotonic: float


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


class FixedIntervalPoll:
    policy_name = "fixed_interval"

    def __init__(self, interval_seconds: float):
        self.interval_seconds = float(interval_seconds)
        self.validate_config()

    def validate_config(self):
        if not 0 < self.interval_seconds < 365 * 86400:
            raise ProtocolError("invalid fixed polling interval")

    def observe(self, event: DiscoveryObservation):
        pass

    def next_delay_seconds(self, context: PollContext) -> float:
        return self.interval_seconds

    def export_state(self) -> dict:
        return {"schema_version": 1}

    def import_state(self, state: dict) -> None:
        if state != {"schema_version": 1}:
            raise ProtocolError("invalid fixed policy state")


PRODUCTION_POLICIES = {"fixed_interval": FixedIntervalPoll}


def make_policy(name: str, interval_seconds: float) -> PollPolicy:
    if name not in PRODUCTION_POLICIES:
        raise ProtocolError("adaptive_research/long_poll not production-registered")
    return PRODUCTION_POLICIES[name](interval_seconds)


class RetryPolicy:
    """Only network/recoverable failure backoff; separate from READY sampling."""
    def __init__(self, start_seconds: float, max_seconds: float, jitter: bool = True):
        self.start, self.maximum, self.jitter = start_seconds, max_seconds, jitter
        self.failures = 0

    def reset(self):
        self.failures = 0

    def next_delay(self) -> float:
        import random
        base = min(self.maximum, self.start * 2 ** min(self.failures, 24))
        self.failures += 1
        return random.uniform(0.5 * base, base) if self.jitter else base
