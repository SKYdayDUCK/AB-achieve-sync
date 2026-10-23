"""Offline discrete-event simulator for B-side READY discovery (paired, reproducible).

Mirrors adaptive-archive-sync/src/aas/b/core.py::run():
  * scan immediately at start; single-flight scans;
  * success -> policy.observe(obs) -> next_due = completed + policy.next_delay_seconds(ctx);
  * one bounded immediate re-check when a scan found new READY (PATCH-01 §2.3);
  * failure -> RetryPolicy backoff (10s doubling to 300s, jitter off for reproducibility);
    the policy never sees failures.
Deviation (intentional, documented): the re-check's observation is ALSO passed to observe(),
otherwise an adaptive policy would miss the exposure/events of the re-check.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Callable, List, Optional

import numpy as np

from .faults import FaultTrace
from .interface import PollContext, ResearchDiscoveryObservation
from .workloads import Trace

COST_RATIOS = (3.0, 10.0, 30.0, 100.0, 300.0)


@dataclass
class SimConfig:
    scan_duration_s: float = 2.0
    immediate_recheck: bool = True
    retry_start_s: float = 10.0
    retry_max_s: float = 300.0
    fail_latency_s: float = 10.0
    workers: int = 2
    max_tail_s: float = 3 * 86400.0
    long_poll_timeout_s: float = 300.0


class SimulationError(RuntimeError):
    pass


def _retry_delay(cfg: SimConfig, failures: int) -> float:
    return min(cfg.retry_max_s, cfg.retry_start_s * 2 ** min(failures, 24))


def _assign_transfers(i0, i1, done, service, worker_free, transfer_done):
    for i in range(i0, i1):
        w = int(np.argmin(worker_free))
        start = max(done, worker_free[w])
        worker_free[w] = start + service[i]
        transfer_done[i] = worker_free[w]


def simulate_poll(policy, trace: Trace, faults: FaultTrace, cfg: SimConfig = SimConfig(), *,
                  checkpoint_at: Optional[float] = None,
                  policy_factory: Optional[Callable[[], object]] = None,
                  record_log: bool = False, spy: Optional[Callable] = None) -> dict:
    arr, service, H = trace.arrivals, trace.service_s, trace.horizon_s
    N = arr.size
    disc = np.full(N, np.nan)
    transfer_done = np.full(N, np.nan)
    worker_free = np.zeros(cfg.workers)
    ptr = 0
    t = 0.0
    failures_in_row = 0
    c = dict(attempts=0, success_nonempty=0, success_empty=0, failed=0, rechecks=0, observe_calls=0)
    recheck_next = False
    checkpointed = False
    max_backlog = 0
    log: List[tuple] = []
    while True:
        if ptr >= N and t > H:
            break
        if t > H + cfg.max_tail_s:
            raise SimulationError("packages left undiscovered after tail budget")
        if checkpoint_at is not None and not checkpointed and t >= checkpoint_at:
            state = json.loads(json.dumps(policy.export_state()))
            policy = policy_factory()
            policy.import_state(state)
            checkpointed = True
        c["attempts"] += 1
        is_recheck = recheck_next
        recheck_next = False
        if is_recheck:
            c["rechecks"] += 1
        if faults.attempt_fails(t):
            c["failed"] += 1
            done = t + cfg.fail_latency_s
            d = _retry_delay(cfg, failures_in_row)
            failures_in_row += 1
            if record_log:
                log.append((t, 0, 0, d))
            t = done + d
            continue
        failures_in_row = 0
        s = t
        done = t + cfg.scan_duration_s
        hi = int(np.searchsorted(arr, s, side="right"))
        n = hi - ptr
        if n:
            disc[ptr:hi] = done
            _assign_transfers(ptr, hi, done, service, worker_free, transfer_done)
            c["success_nonempty"] += 1
            max_backlog = max(max_backlog, n)
        else:
            c["success_empty"] += 1
        obs = ResearchDiscoveryObservation(
            observed_ready_events=n, newly_enqueued_batches=n, queried_pages=1,
            completed_at_monotonic=done, snapshot_at=s, ready_at=tuple(arr[ptr:hi].tolist()))
        ptr = hi
        if spy is not None:
            spy("observe", t, obs)
        policy.observe(obs)
        c["observe_calls"] += 1
        if n and cfg.immediate_recheck and not is_recheck:
            recheck_next = True
            if record_log:
                log.append((t, 1, n, 0.0))
            t = done
            continue
        ctx = PollContext(done, bool(worker_free.max() > done), False)
        d = float(policy.next_delay_seconds(ctx))
        if not (math.isfinite(d) and d > 0):
            raise SimulationError(f"policy returned invalid delay {d!r}")
        if record_log:
            log.append((t, 1, n, d))
        t = done + d
    res = _metrics(trace, disc, transfer_done, c, max_backlog)
    res["end_time_s"] = t
    if record_log:
        res["log"] = log
    return res


def simulate_long_poll(trace: Trace, faults: FaultTrace, cfg: SimConfig = SimConfig(),
                       timeout_s: Optional[float] = None) -> dict:
    """Reference: one blocking READY request that returns on new READY or timeout, re-issued at once.
    Same accounting as polling: every request (returned, timed out or failed) is one query."""
    timeout_s = cfg.long_poll_timeout_s if timeout_s is None else timeout_s
    arr, service, H = trace.arrivals, trace.service_s, trace.horizon_s
    N = arr.size
    disc = np.full(N, np.nan)
    transfer_done = np.full(N, np.nan)
    worker_free = np.zeros(cfg.workers)
    ptr, t, fails = 0, 0.0, 0
    c = dict(attempts=0, success_nonempty=0, success_empty=0, failed=0, rechecks=0, observe_calls=0)
    max_backlog = 0
    conn_s = 0.0
    lat = cfg.scan_duration_s
    while True:
        if ptr >= N and t > H:
            break
        if t > H + cfg.max_tail_s:
            raise SimulationError("long poll tail budget exceeded")
        c["attempts"] += 1
        if faults.attempt_fails(t):
            c["failed"] += 1
            t = t + cfg.fail_latency_s + _retry_delay(cfg, fails)
            fails += 1
            continue
        nxt = arr[ptr] if ptr < N else math.inf
        ev = max(t, nxt)
        ostart = faults.next_outage_start(t)
        if ostart < min(ev, t + timeout_s):          # held connection dropped by an outage
            c["failed"] += 1
            conn_s += ostart - t
            t = ostart + cfg.fail_latency_s + _retry_delay(cfg, fails)
            fails += 1
            continue
        fails = 0
        if ev <= t + timeout_s:
            done = ev + lat
            hi = int(np.searchsorted(arr, ev, side="right"))
            n = hi - ptr
            disc[ptr:hi] = done
            _assign_transfers(ptr, hi, done, service, worker_free, transfer_done)
            max_backlog = max(max_backlog, n)
            ptr = hi
            c["success_nonempty"] += 1
        else:
            done = t + timeout_s + lat
            c["success_empty"] += 1
        conn_s += done - t
        t = done
    res = _metrics(trace, disc, transfer_done, c, max_backlog)
    res["connection_seconds_per_hour"] = conn_s / (H / 3600.0)
    res["end_time_s"] = t
    return res


def _metrics(trace: Trace, disc, transfer_done, c: dict, max_backlog: int) -> dict:
    H_h = trace.horizon_s / 3600.0
    arr = trace.arrivals
    if np.isnan(disc).any():
        raise SimulationError("undiscovered packages at end of simulation")
    d = disc - arr
    if (d < 0).any():
        raise SimulationError("discovery before arrival (leak)")
    e2e = transfer_done - arr
    out = dict(c)
    out.update(
        n_arrivals=int(arr.size),
        polls_per_hour=c["attempts"] / H_h,
        delay_mean=float(d.mean()) if d.size else 0.0,
        delay_p50=float(np.percentile(d, 50)) if d.size else 0.0,
        delay_p95=float(np.percentile(d, 95)) if d.size else 0.0,
        delay_p99=float(np.percentile(d, 99)) if d.size else 0.0,
        delay_max=float(d.max()) if d.size else 0.0,
        delay_sum=float(d.sum()),
        backlog_peak=int(max_backlog),
        e2e_p95=float(np.percentile(e2e, 95)) if d.size else 0.0,
        transfer_queue_peak=_queue_peak(disc, transfer_done),
    )
    for r in COST_RATIOS:
        out[f"J_{int(r)}"] = (r * c["attempts"] + out["delay_sum"]) / H_h
    return out


def _queue_peak(start, end) -> int:
    if start.size == 0:
        return 0
    ev = np.concatenate([np.stack([start, np.ones_like(start)], 1), np.stack([end, -np.ones_like(end)], 1)])
    order = np.lexsort((ev[:, 1], ev[:, 0]))  # departures (-1) before arrivals at equal times
    return int(np.cumsum(ev[order, 1]).max())
