"""Live race: several PollPolicy candidates poll the SAME real A over real SSH, read-only (list-ready only,
never ACK), while an uploader feeds a scheduled arrival pattern. Every policy sees the same READY events.

Latency D (A clock) = snapshot_at of the first successful scan that listed the batch - ready_at. B-side return
time (~RTT/2, identical for all policies) is not included.   Cost per scan = 1 SSH call (no `hello`).
usage: live_race.py OUT.json [--dry]   (env AAS_PW needed for the uploader)
"""
from __future__ import annotations
import io, json, os, sys, threading, time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE.parent / "src"), str(HERE.parents[2] / "src")]   # research src, production src (read-only import)
from aas_poll_research.interface import PollContext, ResearchDiscoveryObservation  # noqa: E402
from aas_poll_research.policies import FixedIntervalPolicy, make_research_policy  # noqa: E402

# phase 1: every 10 s (0-170) | phase 2: every 20 s (180-400) | phase 3: every 15 s (420-585)
SCHEDULE = [*range(0, 180, 10), *range(180, 420, 20), *range(420, 600, 15)]
PHASES = [("P1 10s", 0, 180), ("P2 20s", 180, 420), ("P3 15s", 420, 600)]
HARD_STOP = 680.0

# Time-scaled research configs (the shipped defaults assume hour-scale traffic: min_poll 30 s, half-lives of hours).
SCALE = dict(min_poll_s=2.0, max_poll_s=60.0, prior_rate_per_hour=180.0, prior_exposure_h=0.003,
             fast_half_life_h=1 / 60, slow_half_life_h=1 / 12)
HAZ = dict(SCALE, grid_step_s=1.0, bw_min_s=1.0, gap_half_life_h=1 / 12, g_half_life_h=1 / 12, g_prior_s=60.0)
CONTENDERS = {
    "fixed_5s": lambda: FixedIntervalPolicy(5.0), "fixed_10s": lambda: FixedIntervalPolicy(10.0),
    "fixed_15s": lambda: FixedIntervalPolicy(15.0), "fixed_20s": lambda: FixedIntervalPolicy(20.0),
    "fixed_30s": lambda: FixedIntervalPolicy(30.0),
    "gamma_poisson(a) pf=.5": lambda: make_research_policy("gamma_poisson", poll_factor=0.5, **SCALE),
    "gp_sqrt(a') c=3": lambda: make_research_policy("gamma_poisson_sqrt", cost_ratio=3.0, **SCALE),
    "gp_sqrt(a') c=10": lambda: make_research_policy("gamma_poisson_sqrt", cost_ratio=10.0, **SCALE),
    "renewal(b) k=.3": lambda: make_research_policy("renewal_window", kappa=0.3, **SCALE),
    "hazard(c) c=3": lambda: make_research_policy("hazard_renewal", cost_ratio=3.0, **HAZ),
    "hazard(c) c=10": lambda: make_research_policy("hazard_renewal", cost_ratio=10.0, **HAZ),
}


def epoch(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()


class Poller(threading.Thread):
    def __init__(self, name, factory, transport, t0, barrier, start_cursor=0):
        super().__init__(daemon=True)
        self.pname, self.policy, self.transport, self.t0, self.barrier = name, factory(), transport, t0, barrier
        self.scans, self.found = [], {}  # found: event_id -> (ready_at, snapshot_at)
        self.cursor, self.fail_streak = start_cursor, 0

    def run(self):
        self.barrier.wait()
        while time.time() - self.t0[0] < HARD_STOP and len(self.found) < len(SCHEDULE):
            ts = time.time()
            rec = {"ts": ts - self.t0[0], "ok": False, "delay": None, "new": 0, "ready_at": [], "snap": None}
            try:
                page = self.transport.list_ready(self.cursor, 100, None)
                snap = epoch(page["server_time"])
                items = sorted(page["items"], key=lambda i: i["ready_event_id"])
                ready = tuple(epoch(i["ready_at"]) for i in items)
                for i, r in zip(items, ready):
                    self.found.setdefault(i["ready_event_id"], (r, snap))
                self.cursor = page["next_event_id"]
                te = time.time()
                self.policy.observe(ResearchDiscoveryObservation(len(items), len(items), 1, time.monotonic(), snap, ready))
                delay = float(self.policy.next_delay_seconds(PollContext(time.monotonic(), False, False)))
                self.fail_streak = 0
                rec.update(ok=True, te=te - self.t0[0], snap=snap, new=len(items), ready_at=list(ready), delay=delay)
            except Exception as exc:  # network trouble: never fed to the policy (PATCH-01 §9)
                self.fail_streak += 1
                delay = min(30.0, 3.0 * 2 ** min(self.fail_streak, 4))
                rec.update(te=time.time() - self.t0[0], err=repr(exc)[:160], delay=delay)
            self.scans.append(rec)
            time.sleep(max(0.3, delay))


class Uploader(threading.Thread):
    def __init__(self, t0, barrier, dry=False):
        super().__init__(daemon=True)
        self.t0, self.barrier, self.dry, self.uploads = t0, barrier, dry, []

    def run(self):
        from PIL import Image
        if not self.dry:
            from remote_harness import client
            sftp = client().open_sftp()
        self.barrier.wait()
        for k, at in enumerate(SCHEDULE):
            time.sleep(max(0.0, self.t0[0] + at - time.time()))
            b = io.BytesIO()
            Image.new("RGB", (64, 64), ((k * 37) % 256, (k * 91) % 256, 120)).save(b, "PNG")
            name = f"race_{k:03d}.png"
            if not self.dry:
                g = os.environ["AAS_REMOTE_BASE"] + "/generated"
                with sftp.file(f"{g}/{name}.tmp", "wb") as f:
                    f.write(b.getvalue())
                sftp.rename(f"{g}/{name}.tmp", f"{g}/{name}")
            self.uploads.append({"k": k, "planned": at, "actual": time.time() - self.t0[0], "wall": time.time()})


def main():
    out, dry = sys.argv[1], "--dry" in sys.argv
    from aas.common.config import BConfig
    from aas.transport.ssh import SSHTransport
    transport = SSHTransport(BConfig.load(os.environ["AAS_B_CONFIG"]))
    start = transport.hello()["last_ready_event_id"]  # ignore READY events from earlier runs
    t0 = [0.0]
    barrier = threading.Barrier(len(CONTENDERS) + 2, action=lambda: t0.__setitem__(0, time.time()))
    pollers = [Poller(n, f, transport, t0, barrier, start) for n, f in CONTENDERS.items()]
    up = Uploader(t0, barrier, dry)
    for t in [*pollers, up]:
        t.start()
    barrier.wait()
    for t in [*pollers, up]:
        t.join()
    json.dump({"t0_wall": t0[0], "schedule": SCHEDULE, "phases": PHASES, "uploads": up.uploads,
               "pollers": {p.pname: {"scans": p.scans, "found": {str(k): v for k, v in p.found.items()},
                                     "config": getattr(p.policy, "config", lambda: {})()} for p in pollers}},
              open(out, "w"), indent=1)
    print("done", out, {p.pname: (len(p.scans), len(p.found)) for p in pollers})


if __name__ == "__main__":
    main()
