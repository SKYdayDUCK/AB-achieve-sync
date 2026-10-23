"""Live check of the n+1 (hold back newest) + idle-flush rule against the real A.

Producer = one sequential writer (like a single generator process): it creates each image directly under its
final name, writes the COMPLETE valid PNG, then keeps the file descriptor open for HOLD seconds (this is the
case neither the mtime heuristic nor Pillow's verify can catch). B runs in long_poll mode and delivers.

Properties checked
  P1 every delivered image is byte-identical to what was written
  P2 no batch became READY before ALL its files were closed by the producer (nothing packed while still open)
  P3 after the producer stops, the tail (incl. the newest image) is flushed by the idle rule and delivered
  P4 A ends clean: all DELETED, nothing left in input/staging/building/ready
needs env AAS_PW.   usage: e2e_holdback.py OUT.json
"""
import hashlib, io, json, os, random, sqlite3, subprocess, sys, time
from datetime import datetime
from pathlib import Path
import numpy as np
from PIL import Image

from remote_harness import client, run

BASE = os.environ["AAS_REMOTE_BASE"]; GEN = f"{BASE}/generated"   # e.g. /data/aas-trial
RT = Path(os.environ["AAS_RUNTIME"])                                 # B runtime dir: venv/, tools/, b/b.yaml, logs/
N, HOLD, GAP = 8, 10.0, 2.0
HOLD_BACK, MAX_WAIT = int(sys.argv[2]), int(sys.argv[3])  # control run: 0 ; treatment run: 1
AAS_A = f"{BASE}/aas-venv/bin/aas-a --config {BASE}/aas-keys/a.yaml"
c = client(); sftp = c.open_sftp(); rng = random.Random(7)
ep = lambda s: datetime.fromisoformat(s).timestamp()


def status():
    return json.loads(run(c, f"{AAS_A} status", timeout=60)[1])


def dump():
    code = ("import sqlite3,json;c=sqlite3.connect('%s/aas/state/aas_a.sqlite3');c.row_factory=sqlite3.Row;"
            "print(json.dumps({'events':[dict(r) for r in c.execute('select event_id,batch_id,ready_at from ready_events order by event_id')]}))" % BASE)
    return json.loads(run(c, f"{BASE}/aas-venv/bin/python -c \"{code}\"", timeout=60)[1])


def png(kb):
    side = int((kb * 1024 / 0.75) ** 0.5)
    arr = np.random.default_rng(rng.randrange(1 << 30)).integers(0, 256, (side, side, 3), dtype=np.uint8)
    b = io.BytesIO(); Image.fromarray(arr).save(b, "PNG", compress_level=1); return b.getvalue()


# ---- configure A for this run (config is read at daemon start) and restart it
D = f"{BASE}/upload/adaptive-archive-sync/deploy/aas-a-daemon.sh"
run(c, f"bash {D} {BASE} stop")
run(c, f"sed -i 's/max_wait_seconds: [0-9]*/max_wait_seconds: {MAX_WAIT}/; s/hold_back_newest: [0-9]*/hold_back_newest: {HOLD_BACK}/' {BASE}/aas-keys/a.yaml")
print(run(c, f"AAS_INTERVAL=2 bash {D} {BASE} start; grep -E 'max_wait|watch' {BASE}/aas-keys/a.yaml")[1], flush=True)

# ---- start B (long_poll wake-up) and let it settle
env = dict(os.environ, PATH=str(RT / "tools") + os.pathsep + os.environ["PATH"], PYTHONDONTWRITEBYTECODE="1")
proc = subprocess.Popen([str(RT / "venv" / "Scripts" / "aas-b.exe"), "--config", str(RT / "b" / "b_longpoll.yaml"), "run"],
                        stdout=open(RT / "logs" / "e2e_holdback_b.log", "w"), stderr=subprocess.STDOUT, env=env)
time.sleep(8)
t0 = time.time(); written = {}
print("producer start", flush=True)
for i in range(N):
    name = f"r{HOLD_BACK}w{MAX_WAIT}_{i:04d}.png"; data = png(rng.choice([30, 60, 120]))
    f = sftp.file(f"{GEN}/{name}", "wb"); f.write(data); f.flush()
    start = time.time(); time.sleep(HOLD); f.close()
    written[name] = {"sha": hashlib.sha256(data).hexdigest(), "start": start, "closed": time.time()}
    time.sleep(GAP)
last_close = max(r["closed"] for r in written.values())
print(f"producer stopped at +{last_close - t0:.0f}s; waiting for idle flush", flush=True)

# ---- watch the tail get flushed
timeline = []; deadline = time.time() + 300; done = False
while time.time() < deadline:
    s = status(); st = {x["state"]: x["count"] for x in s["batches_by_state"]}
    timeline.append({"t": round(time.time() - last_close, 1), "watch": s["watch"], "waiting": s["input"]["images_waiting"]})
    if s["input"]["images_waiting"] == 0 and sum(v for k, v in st.items() if k not in ("DELETED", "EMPTY")) == 0 and time.time() - last_close > 5:
        done = True; break
    time.sleep(5)
proc.kill(); proc.wait()
res = {"written": written, "last_close": last_close, "timeline": timeline, "drained": done, "events": dump()["events"], "final": status()}
json.dump(res, open(sys.argv[1], "w"), indent=1, default=str)

# ---- evaluation
OUT = RT / "b" / "output" / "gpu-a-01"
loc = {}
for f in OUT.glob("*/*"):
    loc.setdefault(f.name, []).append(f)
ready = {e["batch_id"]: ep(e["ready_at"]) for e in res["events"]}
ok = 0; early = []; per_batch = {}
for n, r in written.items():
    fs = [f for f in loc.get(n, []) if hashlib.sha256(f.read_bytes()).hexdigest() == r["sha"]]
    if fs: ok += 1
    else: print("MISSING/CORRUPT", n); continue
    b = fs[0].parent.name; per_batch.setdefault(b, []).append(n)
for b, names in per_batch.items():
    latest_close = max(written[n]["closed"] for n in names)
    margin = ready[b] - latest_close
    if margin < -0.5: early.append((b, round(margin, 1)))
    print(f"batch {b[-12:]} files={len(names)} ready - last file closed = {margin:+6.1f}s   [{names[0]} .. {names[-1]}]")
print(f"P1 integrity: {ok}/{N} byte-identical")
print(f"P2 packed-while-open violations (hold_back={HOLD_BACK}, max_wait={MAX_WAIT}): {early or 'none'}")
tail_batch = max(per_batch, key=lambda b: ready[b])
print(f"P3 tail: last file closed, last batch READY +{ready[tail_batch] - last_close:.1f}s later; drained={done}; watch at stop: {timeline[0]['watch'] if timeline else None}")
print("P4 final A:", {x['state']: x['count'] for x in res['final']['batches_by_state']}, res["final"]["input"])
