"""Reproducible synthetic READY-arrival traces (A side), independent of any B policy."""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

HOUR = 3600.0
DAY = 86400.0


@dataclass
class Trace:
    workload: str
    seed: int
    horizon_s: float
    arrivals: np.ndarray          # sorted READY instants (A clock, seconds)
    service_s: np.ndarray         # per-package transfer service time (seconds)
    params: dict = field(default_factory=dict)

    @property
    def n(self) -> int:
        return int(self.arrivals.size)


def _rng(workload: str, seed: int) -> np.random.Generator:
    # stable, workload-specific stream (no Python hash randomisation)
    key = sum((i + 1) * ord(ch) for i, ch in enumerate(workload))
    return np.random.default_rng([int(seed), key, 0xA5])


def _homog_poisson(rng, rate_per_s: float, t0: float, t1: float) -> np.ndarray:
    if rate_per_s <= 0 or t1 <= t0:
        return np.empty(0)
    n = rng.poisson(rate_per_s * (t1 - t0))
    return np.sort(rng.uniform(t0, t1, n))


def _thinned(rng, rate_fn, rate_max: float, t0: float, t1: float) -> np.ndarray:
    cand = _homog_poisson(rng, rate_max, t0, t1)
    if cand.size == 0:
        return cand
    keep = rng.uniform(0, 1, cand.size) < rate_fn(cand) / rate_max
    return cand[keep]


def gen_poisson(rng, H, rate_h=6.0):
    return _homog_poisson(rng, rate_h / HOUR, 0.0, H), {"rate_h": rate_h}


def gen_periodic(rng, H, period_s=600.0, jitter_s=30.0):
    phase = rng.uniform(0, period_s)
    k = np.arange(int(H / period_s) + 2)
    t = phase + k * period_s + rng.normal(0.0, jitter_s, k.size)
    t = np.sort(t[(t > 0) & (t <= H)])
    return t, {"period_s": period_s, "jitter_s": jitter_s}


def gen_onoff(rng, H, on_rate_h=30.0, off_rate_h=0.5, mean_on_h=1.0, mean_off_h=3.0):
    out, t = [], 0.0
    state_on = rng.uniform() < mean_on_h / (mean_on_h + mean_off_h)
    while t < H:
        dur = rng.exponential((mean_on_h if state_on else mean_off_h) * HOUR)
        t1 = min(H, t + dur)
        out.append(_homog_poisson(rng, (on_rate_h if state_on else off_rate_h) / HOUR, t, t1))
        t, state_on = t1, not state_on
    return np.sort(np.concatenate(out)), {"on_rate_h": on_rate_h, "off_rate_h": off_rate_h,
                                          "mean_on_h": mean_on_h, "mean_off_h": mean_off_h}


def gen_drift(rng, H, seg_h=12.0, levels=(1.0, 3.0, 10.0, 30.0)):
    out, rates, t = [], [], 0.0
    while t < H:
        r = float(rng.choice(levels))
        rates.append(r)
        t1 = min(H, t + seg_h * HOUR)
        out.append(_homog_poisson(rng, r / HOUR, t, t1))
        t = t1
    return np.sort(np.concatenate(out)), {"seg_h": seg_h, "rates_h": rates}


def gen_diurnal(rng, H, mean_rate_h=6.0, amp=1.5):
    phase = rng.uniform(0, DAY)
    i0 = float(np.i0(amp))

    def rate(t):
        return mean_rate_h / HOUR * np.exp(amp * np.cos(2 * np.pi * (t - phase) / DAY)) / i0
    rmax = mean_rate_h / HOUR * np.exp(amp) / i0
    return _thinned(rng, rate, rmax, 0.0, H), {"mean_rate_h": mean_rate_h, "amp": amp, "phase_s": phase}


def gen_idle_burst(rng, H, bg_rate_h=0.2, burst_rate_h=60.0, burst_h=0.5, gap_lo_h=12.0, gap_hi_h=30.0):
    out = [_homog_poisson(rng, bg_rate_h / HOUR, 0.0, H)]
    starts = []
    t = rng.uniform(gap_lo_h, gap_hi_h) * HOUR * 0.5
    while t < H:
        starts.append(t)
        out.append(_homog_poisson(rng, burst_rate_h / HOUR, t, min(H, t + burst_h * HOUR)))
        t += rng.uniform(gap_lo_h, gap_hi_h) * HOUR
    return np.sort(np.concatenate(out)), {"bg_rate_h": bg_rate_h, "burst_rate_h": burst_rate_h,
                                          "burst_starts_s": [float(s) for s in starts]}


def gen_packer(rng, H, file_rate_h=120.0, amp=1.0, max_files=50, max_wait_s=1200.0,
               pack_lo_s=20.0, pack_hi_s=120.0):
    """Generator-side batching: files arrive (diurnal Poisson); a batch is sealed when it holds
    max_files files or max_wait_s after its first file; READY after a packing delay."""
    phase = rng.uniform(0, DAY)
    i0 = float(np.i0(amp))

    def rate(t):
        return file_rate_h / HOUR * np.exp(amp * np.cos(2 * np.pi * (t - phase) / DAY)) / i0
    files = _thinned(rng, rate, file_rate_h / HOUR * np.exp(amp) / i0, 0.0, H)
    ready, i, n = [], 0, files.size
    while i < n:
        first = files[i]
        j = i
        while j < n and j - i < max_files and files[j] <= first + max_wait_s:
            j += 1
        seal = files[j - 1] if j - i >= max_files else first + max_wait_s
        ready.append(seal + rng.uniform(pack_lo_s, pack_hi_s))
        i = j
    t = np.sort(np.array(ready))
    t = t[t <= H]
    return t, {"file_rate_h": file_rate_h, "amp": amp, "max_files": max_files, "max_wait_s": max_wait_s}


GENERATORS = {
    "poisson": gen_poisson,
    "periodic": gen_periodic,
    "onoff": gen_onoff,
    "drift": gen_drift,
    "diurnal": gen_diurnal,
    "idle_burst": gen_idle_burst,
    "packer": gen_packer,
}
WORKLOADS = list(GENERATORS)


def make_trace(workload: str, seed: int, horizon_s: float = 4 * DAY,
               service_median_s: float = 60.0, service_sigma: float = 0.8, **params) -> Trace:
    rng = _rng(workload, seed)
    arr, info = GENERATORS[workload](rng, horizon_s, **params)
    arr = np.asarray(arr, float)
    arr = arr[(arr > 0) & (arr <= horizon_s)]
    arr.sort()
    srng = np.random.default_rng([int(seed), 0x5E, arr.size])
    service = service_median_s * np.exp(service_sigma * srng.standard_normal(arr.size))
    return Trace(workload, seed, horizon_s, arr, service, info)
