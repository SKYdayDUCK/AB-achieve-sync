"""Full end-to-end run against the real A: random (unpredictable) arrivals + awkward files, B in two discovery
modes (long_poll wake-up vs plain fixed 30 s), verify integrity of every delivered image and A's cleanup.
needs env AAS_PW. usage: e2e_full.py OUT.json [phase_seconds]"""
import hashlib, io, json, os, random, sqlite3, subprocess, sys, threading, time
from pathlib import Path
import yaml
from remote_harness import client, run
from PIL import Image
import numpy as np

BASE = os.environ["AAS_REMOTE_BASE"]; GEN = f"{BASE}/generated"   # e.g. /data/aas-trial
RT = Path(os.environ["AAS_RUNTIME"])                                 # B runtime dir: venv/, tools/, b/b.yaml, logs/
PHASE_S = float(sys.argv[2]) if len(sys.argv) > 2 else 240.0
AAS_A = f"{BASE}/aas-venv/bin/aas-a --config {BASE}/aas-keys/a.yaml"
PYA = f"{BASE}/aas-venv/bin/python"
c = client(); sftp = c.open_sftp()
rng = random.Random(2026)


def a_status():
    rc, out, err = run(c, f"{AAS_A} status", timeout=60); return json.loads(out)


def a_dump():
    code = ("import sqlite3,json;c=sqlite3.connect('%s/aas/state/aas_a.sqlite3');c.row_factory=sqlite3.Row;"
            "print(json.dumps({'batches':[dict(r) for r in c.execute('select batch_id,seq,state,ready_event_id,ready_at,file_count from batches order by seq')],"
            "'events':[dict(r) for r in c.execute('select event_id,batch_id,ready_at from ready_events order by event_id')]}))" % BASE)
    return json.loads(run(c, f"{PYA} -c \"{code}\"", timeout=60)[1])


def b_cfg(name, wake):
    cfg = yaml.safe_load(open(RT / "b" / "b.yaml", encoding="utf-8"))
    cfg["scheduler"]["fixed_interval"]["interval_seconds"] = 120 if wake else 30
    if wake: cfg["scheduler"]["wake"] = {"mode": "long_poll", "timeout_seconds": 55}
    path = RT / "b" / f"b_{name}.yaml"; yaml.safe_dump(cfg, open(path, "w", encoding="utf-8")); return path


def start_b(cfg_path, logname):
    env = dict(os.environ, PATH=str(RT / "tools") + os.pathsep + os.environ["PATH"], PYTHONDONTWRITEBYTECODE="1")
    return subprocess.Popen([str(RT / "venv" / "Scripts" / "aas-b.exe"), "--config", str(cfg_path), "run"],
                            stdout=open(RT / "logs" / logname, "w"), stderr=subprocess.STDOUT, env=env)


def b_db():
    return sqlite3.connect(f"file:{(RT/'b'/'state'/'aas_b.sqlite3').as_posix()}?mode=ro", uri=True)


def png(size_kb):
    side = int((size_kb * 1024 / 0.75) ** 0.5)
    a = np.random.default_rng(rng.randrange(1 << 30)).integers(0, 256, (side, side, 3), dtype=np.uint8)
    b = io.BytesIO(); Image.fromarray(a).save(b, "PNG", compress_level=1); return b.getvalue()


uploaded = {}  # name -> dict(sha, wall, slow, closed)


errors = []


def put(name, data, slow_hold=0.0):
    """One SFTP channel per upload thread: a shared SFTPClient is not safe across threads."""
    try:
        sf = c.open_sftp()
        sha = hashlib.sha256(data).hexdigest(); rec = {"sha": sha, "bytes": len(data), "slow": slow_hold > 0}
        if slow_hold:  # complete valid file, but the producer keeps the descriptor open
            f = sf.file(f"{GEN}/{name}", "wb"); f.write(data); f.flush(); rec["written"] = time.time()
            time.sleep(slow_hold); f.close(); rec["closed"] = time.time()
        else:
            with sf.file(f"{GEN}/{name}.tmp", "wb") as f: f.write(data)
            sf.rename(f"{GEN}/{name}.tmp", f"{GEN}/{name}"); rec["written"] = time.time()
        uploaded[name] = rec
        sf.close()
    except Exception as exc:  # never swallow harness failures silently
        errors.append((name, repr(exc)))


def awkward_files():
    sftp.mkdir(f"{GEN}/subdir_x") if "subdir_x" not in sftp.listdir(GEN) else None
    for rel, data in (("subdir_x/inner.png", png(5)), ("bad:name.png", png(5)), ("empty.png", b""), ("notes.txt", b"hello"), ("stale.png.tmp", b"partial")):
        with sftp.file(f"{GEN}/{rel}", "wb") as f: f.write(data)


threads = []


def arrivals(prefix, seconds, slow_every=8):
    t0 = time.time(); i = 0; threads.clear()
    while time.time() - t0 < seconds:
        time.sleep(rng.expovariate(1 / 6.0))
        if time.time() - t0 >= seconds: break
        th = threading.Thread(target=put, args=(f"{prefix}_{i:04d}.png", png(rng.choice([20, 60, 120, 250])), 12.0 if i % slow_every == 3 else 0.0), daemon=True)
        th.start(); threads.append(th)
        i += 1
    for th in threads: th.join(timeout=60)
    return i


def drained(extra_ok_waiting=1, timeout=240):  # 1 = the deliberate 0-byte empty.png that must stay untouched
    end = time.time() + timeout
    while time.time() < end:
        s = a_status(); st = {x["state"]: x["count"] for x in s["batches_by_state"]}
        busy = sum(v for k, v in st.items() if k not in ("DELETED", "EMPTY"))
        if busy == 0 and s["input"]["images_waiting"] <= extra_ok_waiting: return True, s
        time.sleep(5)
    return False, a_status()


result = {"phases": {}}
b_cfg("longpoll", True); b_cfg("fixed30", False)
# ---- backlog drain (43 READY batches left from the polling race) + awkward files present from now on
awkward_files()
t = time.time(); p = start_b(RT / "b" / "b_longpoll.yaml", "e2e_backlog.log")
ok, s = drained(timeout=900)
result["backlog_drain"] = {"ok": ok, "seconds": round(time.time() - t, 1), "status": s}
print("backlog drained:", result["backlog_drain"]["ok"], result["backlog_drain"]["seconds"], "s", flush=True)
p.kill(); p.wait()
for name, wake in (("longpoll", True), ("fixed30", False)):
    start_ev = a_dump()["events"][-1]["event_id"]
    p = start_b(RT / "b" / f"b_{name}.yaml", f"e2e_{name}.log"); time.sleep(8)
    t = time.time(); n = arrivals(name, PHASE_S); print(name, "uploaded", n, flush=True)
    ok, s = drained(timeout=300)
    p.kill(); p.wait()
    result["phases"][name] = {"start_event": start_ev, "images": n, "drained": ok, "total_seconds": round(time.time() - t, 1), "a_status": s}
    print(name, "drained", ok, flush=True)
result["a"] = a_dump(); result["uploaded"] = uploaded; result["harness_errors"] = errors
print("harness errors:", errors, flush=True)
con = b_db(); con.row_factory = sqlite3.Row
result["b_obs"] = [dict(r) for r in con.execute("select id,at,kind,observed,new_batches,detail from observations order by id")]
result["b_transfers"] = [dict(r) for r in con.execute("select seq,batch_id,state,attempts from transfers order by seq")]
result["final_a_status"] = a_status()
# cleanup of the awkward files we created
for rel in ("subdir_x/inner.png", "bad:name.png", "empty.png", "notes.txt", "stale.png.tmp"):
    try: sftp.remove(f"{GEN}/{rel}")
    except OSError: pass
try: sftp.rmdir(f"{GEN}/subdir_x")
except OSError: pass
json.dump(result, open(sys.argv[1], "w"), indent=1, default=str)
print("saved", sys.argv[1])
