"""Small O(1)/O(K) online estimators used by the research PollPolicies.

All estimators are updated ONLY from successful READY observations (exact A-side ready_at and
snapshot boundary). Network failures never reach them (see simulator / PATCH-01 §9).
"""
from __future__ import annotations

import math
from typing import Iterable, List, Sequence

LN2 = math.log(2.0)


class DecayedGammaRate:
    """Gamma–Poisson pseudo-count / pseudo-exposure rate estimator with exponential forgetting.

    Prior Gamma(a0, b0) with a0 = r0 * tau0 (events), b0 = tau0 (hours).
    Data part decays with half-life ``half_life_h``; the prior part never decays, so the
    estimate is always defined (no 0/0) and shrinks to r0 when evidence is old.

    For an exposure interval of length dt (hours) ending at the snapshot, with events at ages
    x_i (hours before the snapshot):
        a <- a0 + rho*(a - a0) + sum_i 2^(-x_i/h)
        b <- b0 + rho*(b - b0) + (1 - rho)/k,   rho = 2^(-dt/h), k = ln2/h
    i.e. every instant of exposure and every event is weighted by its own age (exact
    exponentially-weighted Poisson likelihood). With h -> inf it is the plain conjugate update.
    An *empty* successful observation (n=0) increases exposure b and leaves events a unchanged
    apart from forgetting.
    """

    def __init__(self, prior_rate_per_hour: float, prior_exposure_h: float, half_life_h: float):
        self.r0 = float(prior_rate_per_hour)
        self.tau0 = float(prior_exposure_h)
        self.half_life_h = float(half_life_h)
        self.a0 = self.r0 * self.tau0
        self.b0 = self.tau0
        self.a = self.a0
        self.b = self.b0

    def update(self, dt_s: float, event_ages_s: Iterable[float]) -> None:
        if dt_s < 0:
            raise ValueError("negative exposure")
        dt_h = dt_s / 3600.0
        k = LN2 / self.half_life_h
        rho = math.exp(-k * dt_h)
        ev = 0.0
        for age in event_ages_s:
            ev += math.exp(-k * max(0.0, age) / 3600.0)
        expo = (1.0 - rho) / k if dt_h > 0 else 0.0
        self.a = self.a0 + rho * (self.a - self.a0) + ev
        self.b = self.b0 + rho * (self.b - self.b0) + expo

    def rate_per_hour(self) -> float:
        return self.a / self.b

    def reset_to(self, other: "DecayedGammaRate") -> None:
        """Move this estimator's data part to match another estimator's posterior mean.

        Used after a detected change point: keep our own prior, but replace data so that the
        posterior mean equals ``other``'s with ``other``'s (shorter) effective exposure."""
        self.a = other.a - other.a0 + self.a0
        self.b = other.b - other.b0 + self.b0

    def to_list(self) -> List[float]:
        return [self.a, self.b]

    def load_list(self, xs: Sequence[float]) -> None:
        a, b = float(xs[0]), float(xs[1])
        if not (math.isfinite(a) and math.isfinite(b) and a > 0 and b > 0):
            raise ValueError("invalid gamma state")
        self.a, self.b = a, b


class GapEWMA:
    """EWMA of inter-arrival *gaps* (renewal mean/variance) — NOT EWMA(1/gap).

    rate = 1/E[X] is a consistent renewal-rate estimator; EWMA(1/X) is biased upward
    (Jensen: E[1/X] >= 1/E[X], infinite for exponential gaps) and is forbidden by v1.2 §8.1.
    """

    def __init__(self, prior_mean_s: float, alpha: float):
        self.alpha = float(alpha)
        self.mean = float(prior_mean_s)
        self.var = float(prior_mean_s) ** 2  # prior CV = 1 (Poisson-like): window mode disabled
        self.n = 0

    def update(self, gap_s: float) -> None:
        d = gap_s - self.mean
        inc = self.alpha * d
        self.mean += inc
        self.var = (1.0 - self.alpha) * (self.var + d * inc)
        self.n += 1

    @property
    def std(self) -> float:
        return math.sqrt(max(self.var, 0.0))

    @property
    def cv(self) -> float:
        return self.std / self.mean if self.mean > 0 else float("inf")


class PoissonCUSUM:
    """Two-sided CUSUM for a multiplicative rate change by factor k (default 2).

    Each successful interval contributes (n events, mu expected under the *pre-update* model):
        G+ <- max(0, G+ + n ln k - (k-1) mu)
        G- <- max(0, G- + n ln(1/k) - (1/k - 1) mu)
    """

    def __init__(self, k: float = 2.0, threshold: float = 5.0):
        self.k = float(k)
        self.h = float(threshold)
        self.g_up = 0.0
        self.g_down = 0.0
        self.alarms = 0

    def step(self, n: int, mu: float) -> bool:
        lk = math.log(self.k)
        self.g_up = max(0.0, self.g_up + n * lk - (self.k - 1.0) * mu)
        self.g_down = max(0.0, self.g_down - n * lk - (1.0 / self.k - 1.0) * mu)
        if self.g_up > self.h or self.g_down > self.h:
            self.g_up = self.g_down = 0.0
            self.alarms += 1
            return True
        return False
