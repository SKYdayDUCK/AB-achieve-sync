"""Policy-independent network-failure traces (pre-generated, shared by all policies for pairing)."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

HOUR = 3600.0


@dataclass
class FaultTrace:
    seed: int
    outages: np.ndarray        # shape (k, 2): [start, end) seconds, sorted, non-overlapping
    transient: np.ndarray      # bool per 1-second slot: an attempt started in that second fails
    span_s: float

    def in_outage(self, t: float) -> bool:
        if self.outages.size == 0:
            return False
        i = int(np.searchsorted(self.outages[:, 0], t, side="right")) - 1
        return i >= 0 and t < self.outages[i, 1]

    def attempt_fails(self, t: float) -> bool:
        if self.in_outage(t):
            return True
        k = int(t)
        return 0 <= k < self.transient.size and bool(self.transient[k])

    def next_outage_start(self, t: float) -> float:
        """First outage start strictly after t (inf if none)."""
        if self.outages.size == 0:
            return float("inf")
        i = int(np.searchsorted(self.outages[:, 0], t, side="right"))
        return float(self.outages[i, 0]) if i < len(self.outages) else float("inf")

    def outage_seconds(self, t0: float, t1: float) -> float:
        tot = 0.0
        for s, e in self.outages:
            tot += max(0.0, min(e, t1) - max(s, t0))
        return tot


def no_faults(span_s: float) -> FaultTrace:
    return FaultTrace(-1, np.empty((0, 2)), np.zeros(0, bool), span_s)


def make_faults(seed: int, span_s: float, outage_rate_per_h: float = 1.0 / 12.0,
                outage_median_s: float = 300.0, outage_sigma: float = 1.0,
                transient_p: float = 0.01) -> FaultTrace:
    rng = np.random.default_rng([int(seed), 0xFA17])
    n = rng.poisson(outage_rate_per_h * span_s / HOUR)
    starts = np.sort(rng.uniform(0, span_s, n))
    durs = outage_median_s * np.exp(outage_sigma * rng.standard_normal(n))
    merged = []
    for s, d in zip(starts, durs):
        e = s + d
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    outages = np.array(merged, float).reshape(-1, 2)
    transient = rng.uniform(0, 1, int(span_s) + 1) < transient_p
    return FaultTrace(int(seed), outages, transient, span_s)
