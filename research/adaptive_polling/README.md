# research_adaptive —— AAS B 端自适应轮询离线研究

状态：**research_candidate / not_for_production**。生产默认仍为 `fixed_interval`（PATCH-01 §2/§16）。
本目录与生产代码（仓库根目录的 `src/`）完全隔离：只读引用其
`src/aas/b/scheduling/policy.py` 做接口兼容测试（导入时禁止写 `__pycache__`），不修改任何文件。

## 目录

```
research_adaptive/
  DESIGN.md                 问题形式化、候选策略、实验协议、预注册晋升标准
  REPORT.md                 实验报告（中文；子 agent 环境禁止直接写报告文件，内容随最终回复交付，由主 agent 落盘）
  src/aas_poll_research/
    interface.py            PollPolicy 契约（镜像 PATCH-01 §2.2）+ ResearchDiscoveryObservation
    estimators.py           带遗忘 Gamma–Poisson、间隔 EWMA、Poisson CUSUM
    policies.py             fixed_interval / gamma_poisson / gamma_poisson_sqrt / renewal_window / hazard_renewal
    workloads.py            7 个可复现到达生成器（poisson, periodic, onoff, drift, diurnal, idle_burst, packer）
    faults.py               与策略无关的预生成故障轨迹（断网区间 + 瞬时失败场）
    simulator.py            配对离散事件仿真（含立即复查、RetryPolicy、传输队列、long_poll 参考）
  tests/                    unittest：接口契约、失败隔离、暴露量、状态往返、钳制、可复现、T/2 理论值、生产兼容
  experiments/
    run_all.py              一键：仿真矩阵 -> 开销基准 -> 分析/闸门 -> 图
    analyze.py, bench_overhead.py, plots.py
  results/                  runs.csv（逐 run）、summary_by_config.csv、selection.csv、paired_comparison.csv、
                            matched_budget.csv、overhead.csv、gates.json、tables.md、figures/*.png
```

## 一键复现

环境：Python 3.10，numpy / scipy / pandas / matplotlib（无需 pytest，测试用标准库 unittest）。

```powershell
cd research/adaptive_polling
python -B -m unittest discover -s tests -v          # 单元测试（约 3 秒）
python -B experiments\run_all.py                    # 完整实验：7 workload × 30 种子（10 训练 + 20 测试），约 10–15 分钟（8 进程）
python -B experiments\run_all.py --quick            # 冒烟版（1 天、2+3 种子），输出到 results\quick\
python -B experiments\run_all.py --skip-sim         # 复用 results\runs.csv，只重做分析与作图
```

`--procs N` 可调并行度（默认 8；本机内存较紧，30 进程会 OOM）。
所有随机性都由 `(workload, seed)` 决定，同一命令两次运行结果逐位一致。


## 实机实验脚本（live/）

需要一台按主仓库 README 部署好的 A，并设置环境变量：`AAS_HOST` `AAS_PORT` `AAS_PW`（仅用于上传测试文件的密码登录）、`AAS_REMOTE_BASE`（服务器上的 AAS 根目录）、`AAS_B_CONFIG`（B 的配置文件）、`AAS_RUNTIME`（B 的运行目录）。依赖：`pip install paramiko numpy scipy pillow matplotlib pyyaml`。

- `live_race.py` / `analyze_race.py` / `replay_phases.py`：多个轮询策略同时对同一台真实 A 做只读轮询的竞赛与随机相位重放。
- `e2e_full.py` / `analyze_e2e.py`：随机到达 + 干扰文件的完整端到端，对比 long-poll 唤醒与固定轮询。
- `e2e_holdback.py`：n+1（留住最新一张）与空闲收尾的对照实验。
- `results/`：本次实验的原始输出与报告。
