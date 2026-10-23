"""Per-call overhead of observe() / next_delay_seconds() and exported-state size (survey §7.3)."""
from __future__ import annotations

import gc
import json
import os
import time

import numpy as np
import pandas as pd

from aas_poll_research import (KNOBS, PollContext, ResearchDiscoveryObservation, knob_kwargs,
                               make_faults, make_research_policy, make_trace, simulate_poll)

POLICIES = ["fixed_interval", "gamma_poisson", "gamma_poisson_sqrt", "renewal_window", "hazard_renewal"]


class Timed:
    def __init__(self, inner):
        self.inner = inner
        self.policy_name = inner.policy_name
        self.t_obs, self.t_next, self.state_bytes = [], [], []

    def validate_config(self):
        self.inner.validate_config()

    def observe(self, ev):
        t0 = time.perf_counter_ns()
        self.inner.observe(ev)
        self.t_obs.append(time.perf_counter_ns() - t0)
        if len(self.t_obs) % 50 == 0:
            self.state_bytes.append(len(json.dumps(self.inner.export_state())))

    def next_delay_seconds(self, ctx):
        t0 = time.perf_counter_ns()
        d = self.inner.next_delay_seconds(ctx)
        self.t_next.append(time.perf_counter_ns() - t0)
        return d

    def export_state(self):
        return self.inner.export_state()

    def import_state(self, s):
        self.inner.import_state(s)


def run(out, quick=False):
    rows = []
    days = 1.0 if quick else 4.0
    for name in POLICIES:
        tob, tnx, sb = [], [], []
        for w in ("onoff", "periodic", "drift"):
            tr = make_trace(w, 1000, horizon_s=days * 86400)
            p = Timed(make_research_policy(name, **knob_kwargs(name, KNOBS[name][2])))
            gc.collect()
            simulate_poll(p, tr, make_faults(1000, (days + 4) * 86400))
            tob += p.t_obs
            tnx += p.t_next
            sb += p.state_bytes or [len(json.dumps(p.export_state()))]
        # worst case: one observation carrying 300 READY (a day of backlog after an outage)
        worst = []
        for rep in range(20):
            q = make_research_policy(name, **knob_kwargs(name, KNOBS[name][2]))
            q.observe(ResearchDiscoveryObservation(0, 0, 1, 2.0, snapshot_at=0.0, ready_at=()))
            ready = tuple(np.sort(np.random.default_rng(rep).uniform(1, 86400, 300)).tolist())
            ev = ResearchDiscoveryObservation(300, 300, 3, 86402.0, snapshot_at=86400.0, ready_at=ready)
            t0 = time.perf_counter_ns()
            q.observe(ev)
            q.next_delay_seconds(PollContext(86402.0, True, False))
            worst.append(time.perf_counter_ns() - t0)
        tob, tnx = np.array(tob) / 1e3, np.array(tnx) / 1e3
        rows.append({"policy": name, "n_observe": tob.size,
                     "observe_p50_us": np.percentile(tob, 50), "observe_p99_us": np.percentile(tob, 99),
                     "observe_max_us": tob.max(),
                     "next_delay_p50_us": np.percentile(tnx, 50), "next_delay_p99_us": np.percentile(tnx, 99),
                     "next_delay_max_us": tnx.max(),
                     "batch300_observe_plus_decide_p99_us": np.percentile(np.array(worst) / 1e3, 99),
                     "state_bytes_max": max(sb)})
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(out, "overhead.csv"), index=False, float_format="%.1f")
    print(df.to_string(index=False))
    return df
