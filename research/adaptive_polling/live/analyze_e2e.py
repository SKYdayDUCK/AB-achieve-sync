import hashlib, json, os, sys
from datetime import datetime
from pathlib import Path
import numpy as np

d = json.load(open(sys.argv[1])); OUT = Path(os.environ["AAS_RUNTIME"]) / "b" / "output" / "gpu-a-01"
ep = lambda s: datetime.fromisoformat(s).timestamp()
print("== backlog drain:", d["backlog_drain"]["ok"], d["backlog_drain"]["seconds"], "s for 43 READY batches")
# ---- integrity of every uploaded image
found = {}
for f in OUT.glob("*/*"):
    found.setdefault(f.name, []).append(f)
bad, missing, dup, ok = [], [], [], 0
for name, rec in d["uploaded"].items():
    fs = found.get(name, [])
    if not fs: missing.append(name); continue
    if len(fs) > 1: dup.append(name)
    if hashlib.sha256(fs[0].read_bytes()).hexdigest() != rec["sha"]: bad.append(name)
    else: ok += 1
print(f"== integrity: uploaded={len(d['uploaded'])} sha-identical={ok} missing={missing} duplicated={dup} corrupted={bad}")
# ---- slow writers: never packed before the producer closed the file
ev_ready = {e["batch_id"]: ep(e["ready_at"]) for e in d["a"]["events"]}
batch_of = {f.name: f.parent.name for fs in found.values() for f in fs}
slow = [(n, r) for n, r in d["uploaded"].items() if r["slow"]]
early = []
for n, r in slow:
    gap = ev_ready[batch_of[n]] - r["closed"]
    early.append(gap)
print(f"== open-file guard: {len(slow)} slow writers (12 s hold); batch READY minus file close, s: " + ", ".join(f"{g:+.1f}" for g in early)
      + "  (a premature claim would show about -6..-9)")
# ---- state
print("== final A status:", {x["state"]: x["count"] for x in d["final_a_status"]["batches_by_state"]}, "input:", d["final_a_status"]["input"])
print("== B transfers:", {s: sum(1 for t in d["b_transfers"] if t["state"] == s) for s in {t["state"] for t in d["b_transfers"]}})
# ---- discovery latency per phase
first_seen = {}
for o in d["b_obs"]:
    if o["kind"] != "success" or "events=" not in (o["detail"] or ""): continue
    srv, evs = o["detail"].split(";events=")
    for e in filter(None, evs.split(",")):
        first_seen.setdefault((o["id"] // 1000000, int(e)), (o["id"], ep(srv.split("=", 1)[1])))
names = list(d["phases"]); bounds = [d["phases"][n]["start_event"] for n in names] + [10 ** 9]
ready_by_ev = {e["event_id"]: ep(e["ready_at"]) for e in d["a"]["events"]}
snap = {}
for (_, ev), (oid, srv) in first_seen.items():
    if ev not in snap or oid < snap[ev][0]: snap[ev] = (oid, srv)
print("== discovery latency D (A clock: first listing that contained the batch - batch ready_at)")
for i, n in enumerate(names):
    evs = [e for e in ready_by_ev if bounds[i] < e <= bounds[i + 1]]
    D = np.array([snap[e][1] - ready_by_ev[e] for e in evs if e in snap])
    obs = [o for o in d["b_obs"] if o["kind"] == "success"]
    print(f"{n:9s} batches={len(evs):3d} found={len(D):3d}  mean={D.mean():6.2f}s  p50={np.percentile(D,50):6.2f}  p95={np.percentile(D,95):6.2f}  max={D.max():6.2f}")
