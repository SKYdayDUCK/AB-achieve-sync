"""Figures (PNG): Pareto fronts, paired gains with CIs, gain heatmaps, poll timelines."""
from __future__ import annotations

import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

from aas_poll_research import (FixedIntervalPolicy, KNOBS, knob_kwargs, make_faults,  # noqa: E402
                               make_research_policy, make_trace, simulate_poll)

INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
STYLE = {  # fixed baseline = neutral ink; adaptive families = categorical slots in fixed order + marker
    "fixed_interval": dict(color=INK2, marker="s", lw=2.4, label="fixed_interval (基线, 5 档)"),
    "fixed_fine": dict(color="#a8a7a2", marker=".", lw=1.0, label="fixed_interval 细网格 (18 档)"),
    "gamma_poisson": dict(color="#2a78d6", marker="o", lw=1.6, label="(a) gamma_poisson"),
    "gamma_poisson_sqrt": dict(color="#eb6834", marker="v", lw=1.6, label="(a') gamma_poisson_sqrt"),
    "renewal_window": dict(color="#1baf7a", marker="D", lw=1.6, label="(b) renewal_window"),
    "hazard_renewal": dict(color="#6250d6", marker="^", lw=1.6, label="(c) hazard_renewal"),
    "long_poll": dict(color="#e34948", marker="*", lw=0, label="long_poll (参考上界)"),
}
ORDER = list(STYLE)
plt.rcParams.update({
    "font.sans-serif": ["Microsoft YaHei", "SimHei", "DejaVu Sans"], "axes.unicode_minus": False,
    "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "figure.facecolor": SURF,
    "axes.facecolor": SURF, "axes.spines.top": False, "axes.spines.right": False, "font.size": 9,
})


def _pareto(summ, metric, ylabel, path, faults="on"):
    ws = list(summ.workload.unique())
    fig, axes = plt.subplots(2, 4, figsize=(15, 7.2))
    for ax, w in zip(axes.flat, ws):
        g = summ[(summ.workload == w) & (summ.faults == faults)]
        for pid in ORDER:
            h = g[g.pid == pid].sort_values("polls_per_hour")
            if h.empty:
                continue
            st = STYLE[pid]
            ax.plot(h.polls_per_hour, h[metric], color=st["color"], marker=st["marker"], lw=st["lw"],
                    ms=7 if pid != "long_poll" else 13, mec=SURF, mew=1.0, label=st["label"], zorder=3)
            if pid == "fixed_interval":
                for _, r in h.iterrows():
                    ax.annotate(f"T={int(r.knob)}", (r.polls_per_hour, r[metric]), textcoords="offset points",
                                xytext=(4, 4), fontsize=7, color=INK2)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_title(w, color=INK, fontsize=10, loc="left")
        ax.set_xlabel("SSH 查询 / 小时（含失败与复查）")
        ax.set_ylabel(ylabel)
    leg_ax = axes.flat[-1]
    leg_ax.axis("off")
    h, l = axes.flat[0].get_legend_handles_labels()
    leg_ax.legend(h, l, loc="center", frameon=False, fontsize=9)
    fig.suptitle(f"延迟–查询数 Pareto（测试种子均值，网络故障{'开启' if faults == 'on' else '关闭'}；左下更好）",
                 color=INK, fontsize=12, x=0.01, ha="left")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def _gain_dots(cmp, c, path):
    m = cmp[(cmp.faults == "on") & (cmp.c == c)]
    ws = list(m.workload.unique())
    base = m.baseline.iloc[0]
    pids = ["gamma_poisson", "gamma_poisson_sqrt", "renewal_window", "hazard_renewal"]
    fig, ax = plt.subplots(figsize=(10, 5.2))
    off = np.linspace(-0.27, 0.27, len(pids))
    for k, pid in enumerate(pids):
        g = m[m.pid == pid].set_index("workload").loc[ws]
        y = np.arange(len(ws)) + off[k]
        st = STYLE[pid]
        ax.errorbar(g.rel_gain_mean * 100, y,
                    xerr=[(g.rel_gain_mean - g.rel_gain_ci_lo) * 100, (g.rel_gain_ci_hi - g.rel_gain_mean) * 100],
                    fmt=st["marker"], color=st["color"], ms=7, mec=SURF, elinewidth=1.6, capsize=0,
                    label=st["label"])
    ax.axvline(0, color=INK2, lw=1.2)
    ax.axvline(5, color=INK2, lw=1, ls="--")
    ax.text(5.3, len(ws) - 0.45, "G1 门槛 +5%", color=INK2, fontsize=8)
    ax.set_yticks(range(len(ws)))
    ax.set_yticklabels(ws)
    ax.invert_yaxis()
    ax.set_xlabel("相对“训练选出的最佳固定周期”的配对代价改善 ΔJ/J (%)，95% bootstrap CI（右侧=自适应更好）")
    bname = "5 档网格固定周期" if base == "fixed_interval" else "18 档细网格固定周期"
    ax.set_title(f"配对代价差异 vs 训练选出的{bname}（c = c_q/c_d = {c} 包·秒/次查询，20 个测试种子）",
                 loc="left", color=INK)
    ax.legend(frameon=False, loc="lower right", fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def _heatmaps(cmp, path):
    m = cmp[cmp.faults == "on"]
    pids = ["gamma_poisson", "gamma_poisson_sqrt", "renewal_window", "hazard_renewal", "long_poll"]
    ws = list(m.workload.unique())
    cs = sorted(m.c.unique())
    cmap = LinearSegmentedColormap.from_list("div", ["#d95926", "#ecebe7", "#2a78d6"])
    fig, axes = plt.subplots(1, len(pids), figsize=(17, 4.2), sharey=True)
    lim = 0.5
    for ax, pid in zip(axes, pids):
        mat = np.array([[m[(m.pid == pid) & (m.workload == w) & (m.c == c)].rel_gain_mean.iloc[0]
                         for c in cs] for w in ws])
        im = ax.imshow(np.clip(mat, -lim, lim), cmap=cmap, vmin=-lim, vmax=lim, aspect="auto")
        for i in range(len(ws)):
            for j in range(len(cs)):
                ax.text(j, i, f"{mat[i, j] * 100:+.0f}", ha="center", va="center", fontsize=8, color=INK)
        ax.set_xticks(range(len(cs)))
        ax.set_xticklabels([str(int(c)) for c in cs])
        ax.set_xlabel("c = c_q/c_d")
        ax.set_title(STYLE[pid]["label"], fontsize=9, loc="left", color=INK)
        ax.grid(False)
    axes[0].set_yticks(range(len(ws)))
    axes[0].set_yticklabels(ws)
    cb = fig.colorbar(im, ax=axes, fraction=0.015, pad=0.01)
    cb.set_label("ΔJ/J vs 最佳固定（色阶截断于 ±0.5）")
    fig.suptitle("各代价比下的平均配对增益（数字为 %；蓝=自适应更好，橙=更差）", x=0.01, ha="left", color=INK)
    fig.savefig(path, dpi=130, bbox_inches="tight")
    plt.close(fig)


def _timeline(path):
    fig, axes = plt.subplots(2, 1, figsize=(13, 5.2))
    for ax, (w, fam_knobs) in zip(axes, [("periodic", [("fixed_interval", 120.0), ("hazard_renewal", 30.0)]),
                                         ("onoff", [("fixed_interval", 120.0), ("hazard_renewal", 30.0)])]):
        tr = make_trace(w, 1000)
        f = make_faults(1000, tr.horizon_s + 4 * 86400)
        t0, t1 = 86400.0, 86400.0 + 8 * 3600
        a = tr.arrivals[(tr.arrivals >= t0) & (tr.arrivals <= t1)]
        ax.vlines((a - t0) / 3600, 2.6, 3.4, color=INK, lw=1)
        ax.text(0, 3.55, "READY 到达", fontsize=8, color=INK)
        for k, (fam, knob) in enumerate(fam_knobs):
            p = (FixedIntervalPolicy(knob) if fam == "fixed_interval"
                 else make_research_policy(fam, **knob_kwargs(fam, knob)))
            r = simulate_poll(p, tr, f, record_log=True)
            lg = np.array([x for x in r["log"] if t0 <= x[0] <= t1])
            y = 1.6 - k * 1.2
            ok = lg[:, 1] == 1
            ax.vlines((lg[ok, 0] - t0) / 3600, y - 0.35, y + 0.35, color=STYLE[fam]["color"], lw=0.8)
            ax.plot((lg[~ok, 0] - t0) / 3600, np.full((~ok).sum(), y), "x", color="#e34948", ms=5)
            n_poll = len(lg)
            ax.text(0, y + 0.45, f"{fam} ({knob:g}) 查询 {n_poll} 次 / 8h", fontsize=8, color=STYLE[fam]["color"])
        ax.set_ylim(-0.2, 3.9)
        ax.set_yticks([])
        ax.set_xlim(0, 8)
        ax.set_title(f"{w}：第 2 天 8 小时内的查询时刻（竖线=成功查询，× = 失败查询）", loc="left", color=INK, fontsize=10)
        ax.set_xlabel("小时")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def run(df, out):
    figdir = os.path.join(out, "figures")
    os.makedirs(figdir, exist_ok=True)
    summ = pd.read_csv(os.path.join(out, "summary_by_config.csv"))
    cmp_all = pd.read_csv(os.path.join(out, "paired_comparison.csv"))
    cmp = cmp_all[cmp_all.baseline == "fixed_interval"]
    cmp_fine = cmp_all[cmp_all.baseline == "fixed_fine"]
    _pareto(summ, "delay_mean", "平均发现延迟 (s)", os.path.join(figdir, "pareto_mean_delay.png"))
    _pareto(summ, "delay_p95", "P95 发现延迟 (s)", os.path.join(figdir, "pareto_p95_delay.png"))
    _pareto(summ, "delay_mean", "平均发现延迟 (s)", os.path.join(figdir, "pareto_mean_delay_nofault.png"), faults="off")
    for c in (10, 30, 100):
        _gain_dots(cmp, c, os.path.join(figdir, f"paired_gain_c{c}.png"))
    _gain_dots(cmp_fine, 30, os.path.join(figdir, "paired_gain_c30_fine_baseline.png"))
    _heatmaps(cmp, os.path.join(figdir, "gain_heatmap.png"))
    _heatmaps(cmp_fine, os.path.join(figdir, "gain_heatmap_fine_baseline.png"))
    _timeline(os.path.join(figdir, "timeline_examples.png"))
    print("figures written:", figdir)
