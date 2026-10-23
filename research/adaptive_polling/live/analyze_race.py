"""Analyse race JSON -> markdown report + figures.  usage: analyze_race.py race1.json"""
import json, sys
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

src = Path(sys.argv[1]); d = json.load(open(src))
S = d["schedule"]; PH = d["phases"]; t0 = d["t0_wall"]
CS = [3, 10, 30]  # c = seconds of latency one query is worth


def phase_of(k):
    for i, (_, lo, hi) in enumerate(PH):
        if lo <= S[k] < hi:
            return i


rows = {}
for name, p in d["pollers"].items():
    ev = sorted(p["found"].items(), key=lambda kv: int(kv[0]))
    D = [snap - ready for _, (ready, snap) in ev]  # A clock latency
    scans = p["scans"]
    ok = [s for s in scans if s["ok"]]
    r = {"n_found": len(ev), "D": D, "scans": len(scans), "fail": len(scans) - len(ok), "ok": ok, "all": scans}
    r["phase"] = []
    for i, (lab, lo, hi) in enumerate(PH):
        dd = [D[k] for k in range(len(D)) if phase_of(k) == i]
        ns = sum(1 for s in scans if lo <= s["ts"] < hi)
        r["phase"].append({"n": len(dd), "mean": float(np.mean(dd)) if dd else float("nan"), "p95": float(np.percentile(dd, 95)) if dd else float("nan"),
                           "scans": ns, "T": float(np.mean([s["delay"] for s in ok if lo <= s["ts"] < hi])) if any(lo <= s["ts"] < hi for s in ok) else float("nan")})
    r["sumD"] = float(np.sum(D)); r["mean"] = float(np.mean(D)); r["p95"] = float(np.percentile(D, 95)); r["max"] = float(np.max(D))
    for c in CS:
        r[f"J{c}"] = (c * r["scans"] + r["sumD"]) / 60.0  # per-minute-of-test units: (c*queries + total latency s)/60
    rows[name] = r

out = []
out.append(f"# Live race report ({src.name})\n")
out.append("A-clock latency D = snapshot of first successful scan listing the batch - ready_at (B return leg not included, same for all). Cost J_c = (c * scans + sum D) / 60 with c in seconds-of-latency per query (lower is better).\n")
out.append("## Overall\n\n| policy | batches found | scans | failed | mean D s | P95 D s | max D s | J(c=3) | J(c=10) | J(c=30) |\n|---|---|---|---|---|---|---|---|---|---|")
for n, r in sorted(rows.items(), key=lambda kv: kv[1]["J10"]):
    out.append(f"| {n} | {r['n_found']}/{len(S)} | {r['scans']} | {r['fail']} | {r['mean']:.2f} | {r['p95']:.2f} | {r['max']:.2f} | {r['J3']:.1f} | {r['J10']:.1f} | {r['J30']:.1f} |")
for c in CS:
    best = min(rows, key=lambda n: rows[n][f"J{c}"])
    out.append(f"\n- best by J(c={c}): **{best}**")
for i, (lab, lo, hi) in enumerate(PH):
    out.append(f"\n## {lab}  (t={lo}-{hi}s)\n\n| policy | batches | scans | mean poll interval T s | mean D s | P95 D s |\n|---|---|---|---|---|---|")
    for n, r in sorted(rows.items(), key=lambda kv: kv[1]["phase"][i]["mean"]):
        q = r["phase"][i]
        out.append(f"| {n} | {q['n']} | {q['scans']} | {q['T']:.1f} | {q['mean']:.2f} | {q['p95']:.2f} |")
# learning speed: latency of the first 3 batches after a phase switch vs the last 3 of that phase
out.append("\n## Learning after a rate change (mean D of first 3 vs last 3 batches of the phase)\n\n| policy | P2 first3 | P2 last3 | P3 first3 | P3 last3 |\n|---|---|---|---|---|")
idx = {i: [k for k in range(len(S)) if phase_of(k) == i] for i in range(3)}
for n, r in rows.items():
    D = r["D"]; f = lambda ks: float(np.mean([D[k] for k in ks if k < len(D)]))
    out.append(f"| {n} | {f(idx[1][:3]):.2f} | {f(idx[1][-3:]):.2f} | {f(idx[2][:3]):.2f} | {f(idx[2][-3:]):.2f} |")
Path(src.with_suffix(".report.md")).write_text("\n".join(out), encoding="utf-8")
print("\n".join(out))

# --- figures
arr = np.array(sorted(u["actual"] for u in d["uploads"]))
fig, axes = plt.subplots(4, 3, figsize=(17, 12), sharex=True); axes = axes.ravel()
for ax, (n, r) in zip(axes, rows.items()):
    ts = [s["te"] for s in r["all"] if s.get("te") is not None]
    ax.vlines(arr, 0, 1, color="#999", lw=0.8, alpha=.6)
    ax.vlines(ts, 1.1, 1.9, color="#2a6fdb", lw=0.9)
    ax.axvline(180, color="k", ls=":"); ax.axvline(420, color="k", ls=":")
    ax.set_title(f"{n}  scans={r['scans']} meanD={r['mean']:.1f}s", fontsize=9); ax.set_yticks([])
axes[-1].axis("off")
fig.supxlabel("seconds since start (grey = batch arrival, blue = poll completion; dotted = rate change)")
fig.tight_layout(); fig.savefig(src.with_suffix(".timeline.png"), dpi=110)
fig2, ax = plt.subplots(figsize=(8, 5))
for n, r in rows.items():
    ax.scatter(r["scans"], r["mean"], s=40); ax.annotate(n, (r["scans"], r["mean"]), fontsize=7)
ax.set_xlabel("total scans (SSH queries)"); ax.set_ylabel("mean discovery latency D (s)"); ax.grid(alpha=.3)
fig2.tight_layout(); fig2.savefig(src.with_suffix(".pareto.png"), dpi=110)
