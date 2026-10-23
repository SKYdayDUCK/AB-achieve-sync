"""One-shot reproduction: simulate the full workload x policy x seed matrix, then analyse.

    cd research/adaptive_polling
    python -B experiments\\run_all.py            # full run (train 10 + test 20 seeds)
    python -B experiments\\run_all.py --quick    # smoke (2+3 seeds, 1 day)

Outputs go to research_adaptive/results/.
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "experiments"))

import pandas as pd  # noqa: E402

from aas_poll_research import (KNOBS, WORKLOADS, SimConfig, knob_kwargs, make_faults,  # noqa: E402
                               make_research_policy, make_trace, no_faults, simulate_long_poll,
                               simulate_poll)

DAY = 86400.0
MAIN_FAMILIES = ["fixed_interval", "fixed_fine", "gamma_poisson", "gamma_poisson_sqrt", "renewal_window", "hazard_renewal"]
# (family, variant, extra kwargs) -- ablations of the improved candidate
ABLATIONS = [
    ("hazard_renewal", "ratio_objective", {"objective": "ratio"}),
    ("hazard_renewal", "no_censoring_conditioning", {"use_conditioning": False}),
]
R0_SENS = [("hazard_renewal", 1.5), ("hazard_renewal", 24.0),
           ("gamma_poisson_sqrt", 1.5), ("gamma_poisson_sqrt", 24.0)]


def run_job(job):
    workload, seed, split, horizon_s = job
    tr = make_trace(workload, seed, horizon_s=horizon_s)
    cfg = SimConfig()
    faults_on = make_faults(seed, horizon_s + cfg.max_tail_s + DAY)
    faults_off = no_faults(0)
    rows = []

    def add(res, **key):
        res = {k: v for k, v in res.items() if k != "log"}
        rows.append({"workload": workload, "seed": seed, "split": split, **key, **res})

    for fname, faults in (("on", faults_on), ("off", faults_off)):
        for fam in MAIN_FAMILIES:
            for knob in KNOBS[fam]:
                kw = knob_kwargs(fam, knob)
                t0 = time.perf_counter()
                r = simulate_poll(make_research_policy(fam, **kw), tr, faults, cfg)
                r["wall_s"] = time.perf_counter() - t0
                add(r, faults=fname, family=fam, variant="base", r0=6.0, knob=knob)
        r = simulate_long_poll(tr, faults, cfg)
        add(r, faults=fname, family="long_poll", variant="base", r0=6.0, knob=cfg.long_poll_timeout_s)
    for fam, variant, extra in ABLATIONS:
        for knob in KNOBS[fam]:
            r = simulate_poll(make_research_policy(fam, **knob_kwargs(fam, knob, **extra)), tr, faults_on, cfg)
            add(r, faults="on", family=fam, variant=variant, r0=6.0, knob=knob)
    for fam, r0 in R0_SENS:
        for knob in KNOBS[fam]:
            kw = knob_kwargs(fam, knob, prior_rate_per_hour=r0)
            r = simulate_poll(make_research_policy(fam, **kw), tr, faults_on, cfg)
            add(r, faults="on", family=fam, variant=f"r0={r0:g}", r0=r0, knob=knob)
    return rows


def simulate_all(train_seeds, test_seeds, horizon_s, procs, out_csv):
    jobs = [(w, s, "train", horizon_s) for w in WORKLOADS for s in train_seeds]
    jobs += [(w, s, "test", horizon_s) for w in WORKLOADS for s in test_seeds]
    t0 = time.time()
    rows = []
    with mp.Pool(procs) as pool:
        for i, part in enumerate(pool.imap_unordered(run_job, jobs, chunksize=1)):
            rows.extend(part)
            if (i + 1) % 20 == 0 or i + 1 == len(jobs):
                print(f"  {i + 1}/{len(jobs)} jobs done ({time.time() - t0:.0f}s)", flush=True)
    df = pd.DataFrame(rows).sort_values(["workload", "split", "seed", "faults", "family", "variant", "knob"])
    df.to_csv(out_csv, index=False)
    print(f"wrote {out_csv}: {len(df)} runs in {time.time() - t0:.0f}s")
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--procs", type=int, default=max(1, min(8, (os.cpu_count() or 2) - 2)))
    ap.add_argument("--skip-sim", action="store_true", help="reuse results/runs.csv")
    ap.add_argument("--n-train", type=int, default=10)
    ap.add_argument("--n-test", type=int, default=20)
    ap.add_argument("--horizon-days", type=float, default=4.0)
    args = ap.parse_args()
    out = os.path.join(ROOT, "results")
    if args.quick:
        out = os.path.join(out, "quick")
        args.n_train, args.n_test, args.horizon_days = 2, 3, 1.0
    os.makedirs(os.path.join(out, "figures"), exist_ok=True)
    runs_csv = os.path.join(out, "runs.csv")
    train = list(range(0, args.n_train))
    test = list(range(1000, 1000 + args.n_test))
    if args.skip_sim and os.path.exists(runs_csv):
        df = pd.read_csv(runs_csv)
    else:
        print(f"simulating: {len(WORKLOADS)} workloads x {len(train)}+{len(test)} seeds, "
              f"horizon {args.horizon_days} d, {args.procs} processes")
        df = simulate_all(train, test, args.horizon_days * DAY, args.procs, runs_csv)
    import bench_overhead  # noqa: E402
    bench_overhead.run(out, quick=args.quick)
    import analyze  # noqa: E402
    analyze.run(df, out)
    import plots  # noqa: E402
    plots.run(df, out)
    print("done ->", out)


if __name__ == "__main__":
    main()
