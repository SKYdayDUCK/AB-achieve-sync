# Adaptive Archive Sync (AAS)

[![tests](https://github.com/SKYdayDUCK/adaptive-archive-sync/actions/workflows/tests.yml/badge.svg)](https://github.com/SKYdayDUCK/adaptive-archive-sync/actions/workflows/tests.yml) [![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**把云服务器上不断生成的图片，可靠地同步到本地电脑，同步成功后才清理服务器上的文件。**

*English: AAS reliably moves continuously generated images from a (GPU) cloud server "A" to a local PC "B" over SSH. A batches new images into verified, optionally age-encrypted tar bundles; B polls A, downloads over SSH, verifies, delivers, and only then acknowledges so that A may delete the bundle. Every deletion on A is gated by a persisted proof (verification certificate before the source images are deleted, a matching ACK before the bundle is deleted). B never connects to anything inbound; no public port is needed on the PC.*

> 状态：工程 MVP。在真实云服务器 + Windows 本机上完成了端到端试运行（见 [测试与验证](#测试与验证)），**尚未做 72 小时长稳、GiB 级大包、macOS 验证**，不要把它当作无人值守的唯一数据副本。

---

## 目录

1. [项目简介](#项目简介)（使用场景、工作流程、可靠性取舍）
2. [配置教程](#配置教程)（部署 A、部署 B、配置详解、运行与验证、常见问题）
3. [测试与验证](#测试与验证)
4. [自适应轮询研究（仍开放）](#自适应轮询研究仍开放)
5. [后续开发路线](#后续开发路线)
6. [文档索引与已知限制](#文档索引与已知限制)

---

# 项目简介

## 使用场景

- **服务器 A**（Linux，例如租用的 GPU 云主机）：图片生成程序（ComfyUI、Stable Diffusion 等）不断往一个目录里写图。服务器可能随时关机，磁盘不大，也不想长期保存。
- **本地 B**（Windows 或 Linux 的个人电脑）：在家里/办公室，没有公网 IP，**只能由 B 主动连接 A**。

AAS 的做法：

1. **A 端部署**：监控图片目录，把新图片分批打成 tar 压缩包（可用 age 加密），**完整回读验包通过后**才删除原图，把压缩包放入“就绪”目录。
2. **B 端部署**：定时轮询 A 的就绪列表（可选 long-poll 唤醒，出包即刻发现），通过 **SSH** 分块下载压缩包（支持断点续传），校验哈希、解密、解包、逐张核对，放进本地输出目录。
3. B 写下本地交付收据后向 A 发 **ACK**，A 收到匹配的 ACK 才删除压缩包。
4. 交付之后图片完全归你所有：随便移动、修改、删除，AAS 不再管理。

```
 云服务器 A                                          本地电脑 B
 ┌──────────────────────────────────────┐            ┌─────────────────────────────────────┐
 │ 生成器 ──► input/                    │            │  轮询 / long-poll 唤醒              │
 │            │ 目录监控(n+1, 空闲收尾)  │            │       │ list-ready                  │
 │            ▼                          │            │       ▼                             │
 │          staging/ ─► tar(+age) ─► 回读验包 ─► ready/ ◄──── SSH 分块下载(可续传)           │
 │            │ 验包+证书落盘后才删原图   │            │       ▼                             │
 │            ▼                          │            │  校验 SHA ► 解密 ► 解包 ► 逐图核对   │
 │          READY ◄──────── ACK ─────────┼────────────┼── 写交付收据 ► 发 ACK               │
 │            │ 收到匹配 ACK 才删压缩包   │            │       ▼                             │
 │            ▼ DELETED                  │            │   output/<producer>/<batch>/ (归你)  │
 └──────────────────────────────────────┘            └─────────────────────────────────────┘
        B 主动发起 SSH（受限密钥，仅能执行 aasctl 白名单命令）
```

## 可靠性设计要点

| 要点 | 做法 |
|---|---|
| 不会提前删原图 | A 对**最终可交付的压缩包**做完整回读（解密、tar 解析、逐图 SHA-256），写入持久验证证书后才删除原图；崩溃后只凭证书和密文指纹续删 |
| 不会提前删压缩包 | 只有收到与 A 账本逐字段匹配的 ACK 才清理；磁盘再紧也不会删未 ACK 的包 |
| 不会漏包 | READY 事件号单调；每页发现与游标在同一个 SQLite 事务里提交；定期全量对账 |
| 崩溃可恢复 | A/B 双端都是持久状态机；下载分块续传；ACK 幂等 |
| 用户可自由处置输出 | 交付收据独立于输出目录；用户删/改图片不会触发重下或回滚 |
| 半截文件保护 | A 不打包目录里**最新的一张**（n+1 规则）；一段时间没有新图（默认 4×出图间隔）才把尾巴收尾打包；跳过仍被其他进程打开的文件 |
| 受限 SSH | `restrict,command="aasctl-shell ..."`，普通密钥与应急密钥权限互相隔离，无法执行任意命令、无法路径穿越 |

**明确不保证**：A 整盘/实例损毁后的恢复（只有一份副本）、介质级安全擦除、应急通道的严格不丢不重、多生产者/多消费者。详见 [docs/SECURITY.md](docs/SECURITY.md)。

---

# 配置教程

## 环境要求

| | A（服务器） | B（本机） |
|---|---|---|
| 系统 | Linux（需要 `renameat2`，glibc ≥ 2.28） | Windows 10+/Linux |
| Python | ≥ 3.11 | ≥ 3.11 |
| 依赖 | PyYAML、Pillow（`pip install .` 自动装） | 同左 |
| 其他 | `age`、`age-keygen`（启用加密时） | `ssh` 客户端、`age`（Windows 版可从 age 官方 releases 下载） |

## 第 1 步：准备密钥（在 B 上做）

```powershell
# 两套 SSH 密钥：普通通道、应急通道，彼此权限隔离
ssh-keygen -t ed25519 -N "" -f $HOME\.ssh\id_ed25519_aas           -C aas-normal
ssh-keygen -t ed25519 -N "" -f $HOME\.ssh\id_ed25519_aas_emergency -C aas-emergency

# 两把 age 密钥：主密钥 + 离线备份密钥。私钥只留在 B，A 只需要 .pub
age-keygen -o $HOME\.config\aas\b_primary.key
age-keygen -y $HOME\.config\aas\b_primary.key > $HOME\.config\aas\b_primary.pub
age-keygen -o $HOME\.config\aas\b_backup.key
age-keygen -y $HOME\.config\aas\b_backup.key  > $HOME\.config\aas\b_backup.pub   # 无 BOM 的 ASCII/UTF-8
```

> 备份私钥请离线保存。丢了主私钥又没有备份，已加密的包无法解开。

## 第 2 步：部署 A（服务器）

把本仓库拷到服务器，然后：

```bash
bash deploy/a_setup.sh <BASE> <INPUT> normal.pub emergency.pub b_primary.pub b_backup.pub
bash deploy/aas-a-daemon.sh <BASE> start      # 容器常无 systemd：nohup + pidfile
bash deploy/aas-a-daemon.sh <BASE> status
```

- `<BASE>`：AAS 根目录，放**数据盘**；`<INPUT>`：生成器输出目录，**必须与 `<BASE>` 在同一文件系统**（A 靠 rename 领取文件）。
- 脚本会：建 venv 并安装、装 `age`、写 `<BASE>/aas-keys/a.yaml`、在 `~/.ssh/authorized_keys` 追加两行带 `restrict,command=...` 的受限公钥（以 `# aas-managed` 标记，不动你原有的密钥）。
- 守护进程轮询间隔：环境变量 `AAS_INTERVAL`（秒，默认 15）。实例重启后需要重新 `start`。
- 想手动配置而不用脚本：见 [docs/OPERATIONS.md](docs/OPERATIONS.md)。

### A 配置详解（`a.yaml`，完整示例见 [config/a.example.yaml](config/a.example.yaml)）

```yaml
node:   {producer_id: gpu-a-01, consumer_id: desktop-b-01}   # 身份绑定，ACK 逐字段核对
paths:  {root_dir: /data/aas, input_dir: /data/generated}    # 必须同一文件系统
batch:
  max_files: 100            # 满 100 张成一批
  max_bytes: 1073741824     # 或满 1 GiB
  max_batch_bytes: 4294967296   # 单批硬上限
  max_wait_seconds: 600     # 或最老的已完成文件等了 600 秒就成批
  min_age_seconds: 3        # 修改时间距今不足 3 秒的文件不碰
crypto: {strategy: age_x25519, recipients: [/data/aas-keys/b_primary.pub, /data/aas-keys/b_backup.pub]}   # 或 strategy: none
watch:                      # 目录监控：识别“写完的图片”
  skip_open_files: true     # Linux：跳过仍被其他进程打开的文件
  hold_back_newest: 1       # n+1 规则：永远不打包最新的 1 张
  idle_flush_factor: 4      # 超过 4 × 近期出图间隔没有新图 => 视为生成结束，收尾打包尾巴
  idle_flush_min_seconds: 10
  idle_flush_max_seconds: 300   # 同时是学到 3 个间隔之前的冷启动等待
capacity: {warn_free_bytes: 5368709120, critical_free_bytes: 2147483648}   # 只告警/拒绝新打包，不删未 ACK 的包
emergency: {enabled: true, idle_seconds: 60, max_session_seconds: 1800}
```

**关于 `watch`（半截文件）：** 生成器写一张图需要时间，目录里会短暂出现写到一半的 `xxx.png`。AAS 的判断依据：
1. 最可靠：生成器先写 `xxx.tmp`，写完再改名成 `xxx.png`（`.tmp` 和 `.part` 会被忽略）。
2. 否则靠 `hold_back_newest`：顺序出图时，目录里出现了更新的文件，旧文件必然已写完；最新的那张留着不动。
3. 生成器停止后，超过 `idle_flush_factor` × 近期出图间隔（取最近文件修改时间间隔的中位数，限制在 10–300 秒）没有新图或新的 `.tmp`，才把剩下的（包括最新那张）打包。
4. **前提是顺序出图。** 多进程并行写同一目录时，把 `hold_back_newest` 调成并行数，或改用 `.tmp` 改名。
5. 不满足条件的文件（非法文件名、子目录、0 字节、非图片后缀）**不会被打包也不会被删除**，`aas-a status` 的 `input` 字段会列出它们。

## 第 3 步：部署 B（本机）

```powershell
python -m venv venv ; .\venv\Scripts\pip install .
copy config\b.example.yaml b.yaml          # 然后按下文修改
.\venv\Scripts\aas-b --config b.yaml init
.\venv\Scripts\aas-b --config b.yaml crypto-probe   # 验证 SSH 通、age 私钥能解 A 的挑战
.\venv\Scripts\aas-b --config b.yaml once            # 先单次试跑
.\venv\Scripts\aas-b --config b.yaml run             # 常驻
```

首次连接前，用普通 `ssh` 核对并接受服务器主机密钥（写入 `known_hosts`）。AAS 自身一律 `StrictHostKeyChecking=yes`，不会自动接受未知主机。

### B 配置详解（`b.yaml`，完整示例见 [config/b.example.yaml](config/b.example.yaml)）

```yaml
node:   {consumer_id: desktop-b-01, producer_id: gpu-a-01}
paths:  {root_dir: D:/AAS}          # 下面的 incoming/restoring/output/state 都在其下
remote:
  host: gpu.example.com
  port: 22
  user: root
  host_key_checking: strict
  identity_file: C:/Users/you/.ssh/id_ed25519_aas
  emergency_identity_file: C:/Users/you/.ssh/id_ed25519_aas_emergency
  known_hosts_file: C:/Users/you/.ssh/known_hosts
  address_family: inet              # 见常见问题 Q1；默认 any
crypto: {strategy: age_x25519, identity_keys: [C:/Users/you/.config/aas/b_primary.key]}
scheduler:
  strategy: fixed_interval          # 当前唯一的生产策略
  fixed_interval:
    interval_seconds: 300           # 固定轮询周期；有 wake 时它只是安全网，可调大
  wake:                             # 可选：long-poll 唤醒（A 出包即通知 B）
    mode: long_poll                 # none | long_poll
    timeout_seconds: 55             # 5–110
  reconcile_interval_minutes: 30    # 全量对账
  remote_query_timeout_seconds: 30
  network_retry: {initial_seconds: 10, max_seconds: 300, jitter: true}
transfer: {max_parallel_downloads: 2, chunk_bytes: 8388608}   # chunk 上限 16 MiB
capacity: {min_free_bytes: 1073741824}
```

**怎么选轮询方式：**

| 场景 | 配置 | 发现延迟 |
|---|---|---|
| 出包时间可预测、对延迟不敏感 | `fixed_interval` 300 s | 平均约 150 s |
| **出包时间不可预知（推荐）** | `fixed_interval` 120 s + `wake: long_poll` | 实测平均约 3 s |

`wake: long_poll` 只是“提示”：B 挂着一个只读 `wait-ready` 请求，A 一出包就返回，B 随后做一次**完整的常规扫描**；游标/事务/对账语义不变，固定轮询仍是安全网。

## 第 4 步：运行与验证

```bash
# 向 A 的 input 目录放一张图（建议生成器 .tmp -> rename）。A 日志：<BASE>/aas/aas-a.log
aas-a --config a.yaml status          # A：各状态批次数、容量、input 目录摘要、watch 状态
aas-b --config b.yaml status          # B：传输队列
```

成功的标志：`output/<producer_id>/<batch_id>/` 里出现图片；B 队列状态 `ACK_CONFIRMED`；A 的批次变为 `DELETED`，`ready/`、`staging/`、`building/` 清空。

## 运维命令与故障处理

| 现象 | 命令 / 处理 |
|---|---|
| A `VERIFY_RECOVERY_BLOCKED`（压缩包与证书不一致） | 恢复**完全相同字节**后 `aas-a retry-blocked --batch-id ID`；不会强行删图 |
| B `MANUAL_HOLD` | 修复根因（密钥、哈希损坏、目录占用）后 `aas-b retry-held --batch-id ID --reason "..."` |
| B `HANDOFF_UNCERTAIN`（交付窗口内用户动了目录） | `aas-b resolve-handoff --batch-id ID --action confirm-delivered|hold --reason "..."` |
| 手动重新公告某个 READY | `aas-a reannounce-ready --batch-id ID` |
| 紧急情况下直接拉原图（不打包、不保证不丢不重） | `aas-b emergency`（使用独立的应急密钥） |
| 服务器容量不足 | 提高容量、尽快运行 B，或手动启动应急通道；A 不会删未 ACK 的包 |

完整故障表见 [docs/OPERATIONS.md](docs/OPERATIONS.md)。

## 常见问题

**Q1. 为什么每次 SSH 有时要十几秒？**
本机 DNS 做 IPv6（AAAA）解析时可能卡 6–13 秒（实测约一半的调用中招，其余 1.3 秒）。在 B 配置里设 `remote.address_family: inet` 后，12 次实测全部 1.2–1.4 秒。

**Q2. 容器没有 systemd 怎么办？** 用 `deploy/aas-a-daemon.sh`（nohup + pidfile）；实例重启后重新 `start`。

**Q3. 图片放进去了却没被打包？** 看 `aas-a status` 的 `input` 与 `watch` 字段：可能是文件名不合法（含 `:*?"<>|`、Windows 保留名）、后缀不是图片、位于子目录（不递归）、0 字节，或它是“最新一张”正被留住等待收尾。

**Q4. 云实例每次开机端口都变？** 改 B 配置里的 `remote.host/port`，并确认 `known_hosts` 里有 `[host]:port` 的条目。

**Q5. Windows 长路径。** 输出路径 = `output\<producer>\<batch_id>\<文件名>`，文件名很长时可能超 260 字符，建议开启系统 LongPathsEnabled。

---

# 测试与验证

| 项目 | 结果 |
|---|---|
| 自动化测试（Windows，B 目标平台） | 46 通过，2 跳过（age 未装 / `/proc` 仅 Linux）；放入真实 `age.exe` 后 age 用例通过 |
| 自动化测试（Linux 云服务器，真实 age） | 48 通过，0 跳过 |
| 端到端试运行（真实云服务器 + Windows 本机，真实 SSH 受限密钥 + age 加密） | 多轮共数百个包/图：全部字节一致，B 全部 `ACK_CONFIRMED`、A 全部 `DELETED`，`ready/staging/building` 清空 |
| 积压拉取 | 43 个积压包 161 秒内全部取完 |
| 受限 SSH | 普通密钥执行应急命令/任意命令/路径穿越均被拒；应急密钥只能执行应急命令；PTY 被禁用 |
| 网络故障 | 记为 `network_failure`，不当作“空轮询”，指数退避，队列不被污染 |
| 空轮询 | 记为成功的零到达观测，A 无副作用 |
| long-poll 唤醒 vs 固定 30 秒 | 发现延迟平均 3.0 s vs 17.1 s |
| 半截文件保护（对照实验，写完后仍持有句柄 10 秒的生成器） | 关闭 hold-back：8/8 个包在文件关闭前就绪；开启：0/8，最新一张被留住，生成器停止后约 43 秒尾巴被收尾 |

脚本：`scripts/local_smoke.py`（进程内链路）、`scripts/local_ssh_shim_smoke.py`（真实 SSHTransport + 真实 `aasctl-shell` 子进程，仅替换 sshd）。

**试运行中发现并已修复的问题**（详见 [docs/CODE_REVIEW_LOCAL_2026-10-22.md](docs/CODE_REVIEW_LOCAL_2026-10-22.md)）：Windows 上对只读句柄 `fsync` 导致 B 永远无法交付；ACK 的 30 秒超时过短；损坏的下载文件无法重下；应急通道 O(N²) 扫描；ACK 并发重复领取；本机 DNS 的 AAAA 拖慢。

**尚未验证：** 72 小时长稳、GiB 级大包、手动应急通道的真实运行、macOS、多进程并行写同一目录的生成器、A 在 SFTP 等沙箱进程持有文件时的“跳过已打开文件”（该保护在容器里读不到 `sftp-server` 的描述符，对 SFTP 上传无效，普通进程写图应有效）。

---

# 自适应轮询研究（仍开放）

动机：出包时间不可预知，固定周期要在“查询次数”和“发现延迟”之间折中。研究目标是让 B 学习到达节奏并动态调整轮询时机。

**目前的结论：没有自适应策略达到预先写定的“优于固定轮询”标准，生产默认仍是 `fixed_interval`。**

- **仿真**（7 种工作负载、24,780 次仿真，由子 agent 完成，实验数字我未独立重算）：hazard 更新过程模型在近似周期出包（+45%）和长空窗后突发（+26%）上收益显著，但在平稳泊松到达上比调好的固定周期差 3%–10%，且大多数场景 P95 延迟变长 1.2–1.8 倍。
- **实机竞赛**（42 个真实包，11 个策略同台）：延迟优先时 hazard 略优于最佳固定间隔；均衡时基本打平；强调省查询时固定 20–30 秒更优。到达模式是严格分段周期的，对 hazard 最有利，结论是上限。
- **对设计方案的修正**：原式 `T = poll_factor·3600/λ` 应改为与 1/√λ 成正比；仅把固定周期选对就可稳定改善 2%–9%。
- **实际采用的方案：不预测，让 A 通知**——`wake: long_poll`，实测发现延迟 3 s 对 17 s。

代码与数据：[research/adaptive_polling/](research/adaptive_polling/)（仿真器、6 个策略实现、32 个单元测试、实机竞赛/重放/端到端脚本及结果）。

仍然开放的问题：真实日志的影子模式回放、hazard 的门控与 P95 约束、新种子重新预注册、生产观测对象补充 `ready_at`、long-poll 的长期稳定性。详见 **[docs/ADAPTIVE_POLLING_RESEARCH.md](docs/ADAPTIVE_POLLING_RESEARCH.md)**。

---

# 后续开发路线

**B 端**
- [ ] B 端可关闭压缩包处理功能，仅对某个目录内的文件做**传输监控**（直接按文件传输）。
- [ ] 可配置的文件类型：压缩监控与传输监控分别配置。
- [ ] 可配置的监控目录：压缩监控与传输监控分别配置。

**A 端**
- [ ] A 端可关闭压缩/解压缩（打包）功能。
- [ ] 更多轮询参数：可改变 B 端的行为；提供“单次传输”或“后续传输”的配置文件。

**产品化**
- [ ] UI 界面。
- [ ] 兼容 Linux、macOS（目前 A 仅 Linux；B 为 Windows / Linux。macOS 需要替换 `renameat2` 的无覆盖重命名实现，并处理 `/proc` 不存在）。

**来自代码评审的待办**
- [ ] `scan_once` 立即复查那次扫描的观测对策略不可见；生产观测对象缺少 `ready_at`（为任何自适应策略接入的前提）。
- [ ] 被忽略的文件（非法名、超大）目前只在 `status` 中可见，缺少主动告警。
- [ ] 每个 8 MiB 块一次 SSH 连接（Windows OpenSSH 无 ControlMaster），高延迟链路吞吐受限。
- [ ] 72 小时长稳、GiB 级大包、应急通道的真实环境验收。

---

# 文档索引与已知限制

| 文档 | 内容 |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 架构与状态机 |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | SSH 协议与命令白名单 |
| [docs/SECURITY.md](docs/SECURITY.md) | 安全边界与已承认的限制 |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | 部署与故障操作手册 |
| [docs/TRIAL_RUNBOOK.md](docs/TRIAL_RUNBOOK.md) | 云容器场景的试运行手册 |
| [docs/TEST_PLAN.md](docs/TEST_PLAN.md) | 测试与验收矩阵 |
| [docs/CODE_REVIEW_LOCAL_2026-10-22.md](docs/CODE_REVIEW_LOCAL_2026-10-22.md) | 本地 code review 与修复记录 |
| [docs/ADAPTIVE_POLLING_RESEARCH.md](docs/ADAPTIVE_POLLING_RESEARCH.md) | 自适应轮询研究 |
| [research/adaptive_polling/](research/adaptive_polling/) | 自适应轮询研究代码、实验与数据 |
| [docs/reference/](docs/reference/) | 原始设计方案 v1.2 与架构补丁 PATCH-01 |

> `reports/` 目录是原始实现包自带的报告，写于本地复测之前，其中“33 passed”等结论仅反映当时 Linux 容器内的结果，以上文 [测试与验证](#测试与验证) 为准。

**许可证：** [MIT](LICENSE)。欢迎提 issue / PR。注意：这是 Alpha 阶段的工程 MVP，请先阅读 [测试与验证](#测试与验证) 中“尚未验证”的部分再用于重要数据。
