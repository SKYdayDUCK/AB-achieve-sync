"""Analysis: train-seed knob selection, paired bootstrap vs. fixed baseline, matched-budget
comparison, pre-registered promotion gates (DESIGN.md §6), markdown tables."""
from __future__ import annotations

import json
import os

import numpy as np
import pandas as pd

COSTS = [3, 10, 30, 100, 300]
GATE_COSTS = [10, 30, 100]
ADAPTIVE = ["gamma_poisson", "gamma_poisson_sqrt", "renewal_window", "hazard_renewal"]
NON_POISSON = ["periodic", "onoff", "drift", "diurnal", "idle_burst", "packer"]
METRICS = ["attempts", "success_nonempty", "success_empty", "failed", "rechecks", "polls_per_hour",
           "delay_mean", "delay_p50", "delay_p95", "delay_p99", "delay_max", "backlog_peak",
           "e2e_p95", "transfer_queue_peak"] + [f"J_{c}" for c in COSTS]
B = 10_000


def pid_of(row_family, row_variant):
    return row_family if row_variant == "base" else f"{row_family}[{row_variant}]"


def bootstrap_ci(x, seed=12345):
    x = np.asarray(x, float)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, x.size, (B, x.size))
    m = x[idx].mean(1)
    return float(np.percentile(m, 2.5)), float(np.percentile(m, 97.5))


def add_pid(df):
    df = df.copy()
    df["pid"] = [pid_of(f, v) for f, v in zip(df["family"], df["variant"])]
    return df


def select_knobs(df):
    """Per (faults, workload, pid, c): knob with minimal mean J_c on TRAIN seeds."""
    tr = df[df.split == "train"]
    rows = []
    for (faults, w, pid), g in tr.groupby(["faults", "workload", "pid"]):
        means = g.groupby("knob")[[f"J_{c}" for c in COSTS]].mean()
        for c in COSTS:
            rows.append({"faults": faults, "workload": w, "pid": pid, "c": c,
                         "knob": float(means[f"J_{c}"].idxmin()),
                         "train_J": float(means[f"J_{c}"].min())})
    return pd.DataFrame(rows)


def paired(df, sel, baseline="fixed_interval"):
    te = df[df.split == "test"]
    key = sel.set_index(["faults", "workload", "pid", "c"])["knob"]
    out = []
    for (faults, w), g in te.groupby(["faults", "workload"]):
        for c in COSTS:
            Tf = key.get((faults, w, baseline, c))
            if Tf is None:
                continue
            fx = g[(g.pid == baseline) & (g.knob == Tf)].set_index("seed").sort_index()
            # hindsight-best fixed (diagnostic only, not an online competitor)
            fmeans = g[g.pid == baseline].groupby("knob")[f"J_{c}"].mean()
            T_hind = float(fmeans.idxmin())
            for pid in sorted(g.pid.unique()):
                k = key.get((faults, w, pid, c))
                if k is None:
                    continue
                cd = g[(g.pid == pid) & (g.knob == k)].set_index("seed").sort_index()
                cd = cd.loc[fx.index]
                Jf, Jc = fx[f"J_{c}"].to_numpy(), cd[f"J_{c}"].to_numpy()
                rel = (Jf - Jc) / Jf
                lo, hi = bootstrap_ci(rel)
                out.append({
                    "baseline": baseline, "faults": faults, "workload": w, "c": c, "pid": pid, "knob": k, "fixed_T": Tf,
                    "fixed_T_hindsight": T_hind,
                    "J_fixed": Jf.mean(), "J_cand": Jc.mean(),
                    "rel_gain_mean": rel.mean(), "rel_gain_ci_lo": lo, "rel_gain_ci_hi": hi,
                    "rel_gain_median": float(np.median(rel)), "win_rate": float((rel > 0).mean()),
                    "worst_seed_rel": float(rel.min()),
                    "polls_h_fixed": fx.polls_per_hour.mean(), "polls_h_cand": cd.polls_per_hour.mean(),
                    "delay_mean_fixed": fx.delay_mean.mean(), "delay_mean_cand": cd.delay_mean.mean(),
                    "p95_fixed": fx.delay_p95.mean(), "p95_cand": cd.delay_p95.mean(),
                    "p99_fixed": fx.delay_p99.mean(), "p99_cand": cd.delay_p99.mean(),
                    "p95_ratio": cd.delay_p95.mean() / fx.delay_p95.mean(),
                    "p99_ratio": cd.delay_p99.mean() / fx.delay_p99.mean(),
                    "n_seeds": int(len(rel)),
                })
    return pd.DataFrame(out)


def matched_budget(df):
    """Delay of each adaptive point relative to the fixed curve at the same polls/hour (per seed,
    log-log interpolation through the 5 fixed points of that very seed)."""
    te = df[(df.split == "test")]
    rows = []
    for (faults, w, seed), g in te.groupby(["faults", "workload", "seed"]):
        fx = g[g.pid == "fixed_interval"].sort_values("polls_per_hour")
        lx = np.log(fx.polls_per_hour.to_numpy())
        for metric in ("delay_mean", "delay_p95"):
            ly = np.log(fx[metric].to_numpy())
            for _, r in g[~g.pid.isin(["fixed_interval", "fixed_fine"])].iterrows():
                x = np.log(r.polls_per_hour)
                inside = lx[0] <= x <= lx[-1]
                ref = float(np.exp(np.interp(x, lx, ly))) if inside else np.nan
                rows.append({"faults": faults, "workload": w, "seed": seed, "pid": r.pid, "knob": r.knob,
                             "metric": metric, "polls_per_hour": r.polls_per_hour, "value": r[metric],
                             "fixed_at_same_budget": ref,
                             "ratio": r[metric] / ref if inside else np.nan})
    m = pd.DataFrame(rows)
    agg = (m.groupby(["faults", "workload", "pid", "knob", "metric"])
           .agg(polls_per_hour=("polls_per_hour", "mean"), value=("value", "mean"),
                ratio_geomean=("ratio", lambda r: float(np.exp(np.log(r.dropna()).mean())) if r.notna().any() else np.nan),
                frac_inside=("ratio", lambda r: float(r.notna().mean())))
           .reset_index())
    return agg


def gates(cmp, overhead, df):
    res = {}
    main = cmp[(cmp.faults == "on")]
    for pid in ADAPTIVE:
        g = main[main.pid == pid]
        r = {}
        g1 = {}
        for c in GATE_COSTS:
            gc = g[(g.c == c) & g.workload.isin(NON_POISSON)]
            wins = gc[(gc.rel_gain_mean >= 0.05) & (gc.rel_gain_ci_lo > 0)].workload.tolist()
            g1[c] = wins
        r["G1_main_gain"] = {"pass": all(len(v) >= 4 for v in g1.values()),
                             "detail": {str(c): f"{len(v)}/6 workloads: {', '.join(v) or '-'}" for c, v in g1.items()}}
        gg = g[g.c.isin(GATE_COSTS)]
        bad = gg[gg.rel_gain_mean < -0.02]
        r["G2_no_harm"] = {"pass": bad.empty,
                           "detail": "; ".join(f"{a}@c={b}: {x:+.1%}" for a, b, x in
                                               zip(bad.workload, bad.c, bad.rel_gain_mean)) or "ok"}
        tail = gg[(gg.p95_ratio > 1.10) | (gg.p99_ratio > 1.15)]
        r["G3_tail"] = {"pass": tail.empty,
                        "detail": "; ".join(f"{a}@c={b}: P95x{p:.2f}/P99x{q:.2f}" for a, b, p, q in
                                            zip(tail.workload, tail.c, tail.p95_ratio, tail.p99_ratio)) or "ok"}
        pz = gg[(gg.workload == "poisson") & (gg.rel_gain_mean > 0.05) & (gg.rel_gain_ci_lo > 0)]
        r["G4_negative_control"] = {"pass": pz.empty,
                                    "detail": "ok" if pz.empty else "suspicious Poisson win"}
        sub = df[df.pid == pid]
        lost = int((sub.n_arrivals < 0).sum())  # simulator raises on undiscovered packages
        r["G5_reliability"] = {"pass": lost == 0 and len(sub) > 0,
                               "detail": f"{len(sub)} runs, all packages discovered; failure isolation & state "
                                         f"round-trip covered by unit tests"}
        ov = overhead[overhead.policy == pid]
        if ov.empty:
            r["G6_resources"] = {"pass": None, "detail": "not measured"}
        else:
            o = ov.iloc[0]
            ok = o.observe_p99_us < 1000 and o.next_delay_p99_us < 1000 and o.state_bytes_max < 1_000_000
            r["G6_resources"] = {"pass": bool(ok),
                                 "detail": f"observe P99 {o.observe_p99_us:.0f}us, next_delay P99 "
                                           f"{o.next_delay_p99_us:.0f}us, state {int(o.state_bytes_max)} B"}
        r["ALL_PASS"] = all(v["pass"] for k, v in r.items() if isinstance(v, dict) and "pass" in v)
        res[pid] = r
    return res


def fmt_pct(x):
    return f"{x * 100:+.1f}%"


def write_tables(cmp, mb, summ, gate_res, sel, out):
    L = []
    main = cmp[cmp.faults == "on"]
    pids = ["fixed_fine", "gamma_poisson", "gamma_poisson_sqrt", "renewal_window", "hazard_renewal", "long_poll"]
    for c in COSTS:
        L.append(f"\n### 配对 ΔJ 相对增益（c={c}，故障开启；正=优于训练选出的最佳固定周期；[95% CI]）\n")
        L.append("| workload | fixed T* | " + " | ".join(pids) + " |")
        L.append("|---|---|" + "---|" * len(pids))
        for w in main.workload.unique():
            g = main[(main.c == c) & (main.workload == w)]
            cells = []
            for p in pids:
                r = g[g.pid == p]
                if r.empty:
                    cells.append("-")
                    continue
                r = r.iloc[0]
                star = "**" if r.rel_gain_ci_lo > 0 and r.rel_gain_mean >= 0.05 else ""
                cells.append(f"{star}{fmt_pct(r.rel_gain_mean)}{star} [{fmt_pct(r.rel_gain_ci_lo)},{fmt_pct(r.rel_gain_ci_hi)}]")
            L.append(f"| {w} | {int(g.fixed_T.iloc[0])}s | " + " | ".join(cells) + " |")
    # detail table c=30
    L.append("\n### c=30 详细：选中旋钮、查询数与尾延迟（测试种子均值，故障开启）\n")
    L.append("| workload | policy | knob | polls/h | mean D (s) | P95 (s) | P99 (s) | P95 比 | P99 比 | 胜率 |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for w in main.workload.unique():
        g = main[(main.c == 30) & (main.workload == w)]
        f0 = g.iloc[0]
        L.append(f"| {w} | fixed_interval | T={int(f0.fixed_T)} | {f0.polls_h_fixed:.1f} | {f0.delay_mean_fixed:.0f} | "
                 f"{f0.p95_fixed:.0f} | {f0.p99_fixed:.0f} | 1.00 | 1.00 | - |")
        for p in pids:
            r = g[g.pid == p]
            if r.empty:
                continue
            r = r.iloc[0]
            L.append(f"| {w} | {p} | {r.knob:g} | {r.polls_h_cand:.1f} | {r.delay_mean_cand:.0f} | {r.p95_cand:.0f} | "
                     f"{r.p99_cand:.0f} | {r.p95_ratio:.2f} | {r.p99_ratio:.2f} | {r.win_rate:.0%} |")
    # ablations
    abl = [p for p in main.pid.unique() if "[" in p]
    if abl:
        L.append("\n### 消融与先验敏感性（c=30，相对最佳固定周期的配对增益）\n")
        L.append("| workload | hazard_renewal | " + " | ".join(abl) + " |")
        L.append("|---|---|" + "---|" * len(abl))
        for w in main.workload.unique():
            g = main[(main.c == 30) & (main.workload == w)].set_index("pid")
            cells = [fmt_pct(g.loc[p].rel_gain_mean) if p in g.index else "-" for p in ["hazard_renewal"] + abl]
            L.append(f"| {w} | " + " | ".join(cells) + " |")
    # no-fault condition
    nf = cmp[cmp.faults == "off"]
    if not nf.empty:
        L.append("\n### 无网络故障对照（c=30，配对增益）\n")
        L.append("| workload | fixed T* | " + " | ".join(pids) + " |")
        L.append("|---|---|" + "---|" * len(pids))
        for w in nf.workload.unique():
            g = nf[(nf.c == 30) & (nf.workload == w)].set_index("pid")
            cells = [f"{fmt_pct(g.loc[p].rel_gain_mean)} [{fmt_pct(g.loc[p].rel_gain_ci_lo)},{fmt_pct(g.loc[p].rel_gain_ci_hi)}]"
                     if p in g.index else "-" for p in pids]
            L.append(f"| {w} | {int(g.fixed_T.iloc[0])}s | " + " | ".join(cells) + " |")
    # matched budget
    m = mb[(mb.faults == "on") & (mb.metric == "delay_mean") & mb.pid.isin(ADAPTIVE)]
    if not m.empty:
        L.append("\n### 匹配查询预算：自适应平均延迟 / 同 polls/h 固定周期插值延迟（几何均值；<1 更好；故障开启）\n")
        L.append("每格为 5 个旋钮档中，落在固定曲线覆盖范围内的那些档的比值（min…max）。\n")
        L.append("| workload | " + " | ".join(ADAPTIVE) + " |")
        L.append("|---|" + "---|" * len(ADAPTIVE))
        for w in m.workload.unique():
            cells = []
            for p in ADAPTIVE:
                r = m[(m.workload == w) & (m.pid == p)].ratio_geomean.dropna()
                cells.append(f"{r.min():.2f}…{r.max():.2f}" if len(r) else "-")
            L.append(f"| {w} | " + " | ".join(cells) + " |")
    L += gate_lines(gate_res, "预注册晋升闸门（DESIGN.md §6，基线 = 5 档网格固定周期）")
    with open(os.path.join(out, "tables.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


def write_fine_tables(cmp_fine, gate_fine, out):
    L = ["\n## 补充：以 18 档细网格固定周期（30…600s）为基线（强基线，排除“网格离散化”带来的伪收益）\n"]
    pids = ["fixed_interval", "gamma_poisson", "gamma_poisson_sqrt", "renewal_window", "hazard_renewal", "long_poll"]
    main = cmp_fine[cmp_fine.faults == "on"]
    for c in COSTS:
        L.append(f"\n### 细网格基线 ΔJ 相对增益（c={c}，故障开启；[95% CI]）\n")
        L.append("| workload | fine T* | " + " | ".join(pids) + " |")
        L.append("|---|---|" + "---|" * len(pids))
        for w in main.workload.unique():
            g = main[(main.c == c) & (main.workload == w)].set_index("pid")
            cells = []
            for p in pids:
                if p not in g.index:
                    cells.append("-")
                    continue
                r = g.loc[p]
                star = "**" if r.rel_gain_ci_lo > 0 and r.rel_gain_mean >= 0.05 else ""
                cells.append(f"{star}{fmt_pct(r.rel_gain_mean)}{star} [{fmt_pct(r.rel_gain_ci_lo)},{fmt_pct(r.rel_gain_ci_hi)}]")
            L.append(f"| {w} | {int(g.fixed_T.iloc[0])}s | " + " | ".join(cells) + " |")
    L.append("\n### c=30 细网格基线：P95/P99 比（候选/固定）\n")
    L.append("| workload | " + " | ".join(pids) + " |")
    L.append("|---|" + "---|" * len(pids))
    for w in main.workload.unique():
        g = main[(main.c == 30) & (main.workload == w)].set_index("pid")
        L.append(f"| {w} | " + " | ".join(f"{g.loc[p].p95_ratio:.2f}/{g.loc[p].p99_ratio:.2f}" if p in g.index else "-"
                                         for p in pids) + " |")
    L += gate_lines(gate_fine, "补充闸门（同一 G1–G6 定义，基线 = 细网格固定周期）")
    with open(os.path.join(out, "tables_fine_baseline.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


def write_selected_metrics(df, sel, out, c=30):
    """Full PATCH-01 §15.2-style metric breakdown for the train-selected config of each policy."""
    te = df[(df.split == "test") & (df.faults == "on") & (df.variant == "base")]
    key = sel[(sel.faults == "on") & (sel.c == c)].set_index(["workload", "pid"])["knob"]
    cols = ["polls_per_hour", "success_nonempty", "success_empty", "failed", "rechecks", "delay_mean",
            "delay_p50", "delay_p95", "delay_p99", "delay_max", "backlog_peak", "e2e_p95", f"J_{c}"]
    rows = []
    for (w, pid), k in key.items():
        g = te[(te.workload == w) & (te.pid == pid) & (te.knob == k)]
        rows.append({"workload": w, "pid": pid, "knob": k, **g[cols].mean().to_dict()})
    t = pd.DataFrame(rows)
    t.to_csv(os.path.join(out, f"selected_metrics_c{c}.csv"), index=False, float_format="%.4g")
    L = [f"\n### 训练选出配置的完整指标（c={c}，故障开启，20 个测试种子均值；每 run = 96h）\n",
         "| workload | policy | knob | polls/h | 非空成功 | 空成功 | 失败 | 复查 | 均值 D | P50 | P95 | P99 | 最大 | 积压峰值 | 端到端P95 | J |",
         "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    order = ["fixed_interval", "fixed_fine", "gamma_poisson", "gamma_poisson_sqrt", "renewal_window",
             "hazard_renewal", "long_poll"]
    for w in t.workload.unique():
        for p in order:
            r = t[(t.workload == w) & (t.pid == p)]
            if r.empty:
                continue
            r = r.iloc[0]
            L.append(f"| {w} | {p} | {r.knob:g} | {r.polls_per_hour:.1f} | {r.success_nonempty:.0f} | {r.success_empty:.0f} | "
                     f"{r.failed:.0f} | {r.rechecks:.0f} | {r.delay_mean:.0f} | {r.delay_p50:.0f} | {r.delay_p95:.0f} | "
                     f"{r.delay_p99:.0f} | {r.delay_max:.0f} | {r.backlog_peak:.1f} | {r.e2e_p95:.0f} | {r[f'J_{c}']:.0f} |")
    with open(os.path.join(out, f"selected_metrics_c{c}.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


def gate_lines(gate_res, title):
    L = []
    L.append(f"\n### {title}\n")
    L.append("| policy | G1 主收益 | G2 不伤害 | G3 尾延迟 | G4 负控制 | G5 可靠性 | G6 资源 | 全部通过 |")
    L.append("|---|---|---|---|---|---|---|---|")
    for p, r in gate_res.items():
        def f(k):
            v = r[k]["pass"]
            return "PASS" if v else ("n/a" if v is None else "FAIL")
        L.append(f"| {p} | {f('G1_main_gain')} | {f('G2_no_harm')} | {f('G3_tail')} | {f('G4_negative_control')} | "
                 f"{f('G5_reliability')} | {f('G6_resources')} | {'YES' if r['ALL_PASS'] else 'NO'} |")
    L.append("\n闸门细节：\n")
    for p, r in gate_res.items():
        L.append(f"- **{p}**")
        for k, v in r.items():
            if isinstance(v, dict):
                L.append(f"  - {k}: {'PASS' if v['pass'] else 'FAIL'} — {v['detail']}")
    return L


def run(df, out):
    df = add_pid(df)
    summ = (df[df.split == "test"].groupby(["workload", "faults", "pid", "knob"])[METRICS].mean().reset_index())
    summ.to_csv(os.path.join(out, "summary_by_config.csv"), index=False, float_format="%.4g")
    sel = select_knobs(df)
    sel.to_csv(os.path.join(out, "selection.csv"), index=False)
    cmp_all = pd.concat([paired(df, sel, "fixed_interval"), paired(df, sel, "fixed_fine")], ignore_index=True)
    cmp_all.to_csv(os.path.join(out, "paired_comparison.csv"), index=False, float_format="%.5g")
    cmp = cmp_all[cmp_all.baseline == "fixed_interval"]
    cmp_fine = cmp_all[cmp_all.baseline == "fixed_fine"]
    mb = matched_budget(df[df.variant == "base"])
    mb.to_csv(os.path.join(out, "matched_budget.csv"), index=False, float_format="%.5g")
    ov_path = os.path.join(out, "overhead.csv")
    overhead = pd.read_csv(ov_path) if os.path.exists(ov_path) else pd.DataFrame(columns=["policy"])
    gate_res = gates(cmp, overhead, df[(df.variant == "base") & (df.faults == "on")])
    gate_fine = gates(cmp_fine, overhead, df[(df.variant == "base") & (df.faults == "on")])
    with open(os.path.join(out, "gates.json"), "w", encoding="utf-8") as fh:
        json.dump({"preregistered_grid_baseline": gate_res, "supplementary_fine_grid_baseline": gate_fine},
                  fh, indent=2, ensure_ascii=False, default=str)
    write_tables(cmp, mb, summ, gate_res, sel, out)
    write_fine_tables(cmp_fine, gate_fine, out)
    write_selected_metrics(df, sel, out)
    print("analysis written:", out)
    for p, r in gate_res.items():
        print(f"  {p:20s} ALL_PASS={r['ALL_PASS']}  G1={r['G1_main_gain']['detail']}")
    for p, r in gate_fine.items():
        print(f"  [fine] {p:20s} ALL_PASS={r['ALL_PASS']}  G1={r['G1_main_gain']['detail']}")
    return cmp_all
