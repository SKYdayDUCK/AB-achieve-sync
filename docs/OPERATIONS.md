# 部署与故障操作手册（Linux A / Windows 或 Linux B）

## 0. 上线前环境核验

操作以 Python ≥3.11、`aas-a`/`aas-b`/`aasctl-shell` 已通过 `python -m pip install .` 安装为前提。使用独立运行账户；确保生产图片目录和 A staging 同卷，A building 与 ready 同卷，B restoring 与 output 同卷。A 进程对 `/workspace/aas` 及生成器 `input` 目录有相应访问权。

建议先运行 `python scripts/local_smoke.py`；这个仅验证本机不加密核心业务链路，**不测试真实 SSH 或 age**。配置基于 `config/a.example.yaml` 和 `config/b.example.yaml`，先修改所有示例路径、主机名、认证信息。不要把私钥放到版本控制或可公开访问的目录。

## 1. 配置 age 接收密钥（生产）

在 **B** 机器本地生成两对 age X25519 私钥和公钥（推荐主密钥+离线恢复密钥）：

```bash
age-keygen -o b_primary.key
age-keygen -y b_primary.key > b_primary.pub
age-keygen -o b_backup.key
age-keygen -y b_backup.key > b_backup.pub
```

仅把 `*.pub` 公钥安全送往 A `/etc/aas/keys`。将这些文件路径写入 A 配置 `crypto.recipients`。把 B 实际要用于正常解密的私钥路径写入 `crypto.identity_keys`；备份私钥建议离线保存，只有实际恢复时再提供给 B。两端 `crypto.strategy: age_x25519` 必须匹配。

验证 B 能调用系统 `age`（`age --version`）。Windows PowerShell 的输出文件编码要确认公钥文件为无 BOM 的普通 ASCII/UTF-8 文本。

## 2. 创建隔离 SSH 登录凭证

在 B 生成两套**不同** Ed25519 SSH 密钥：

```bash
ssh-keygen -t ed25519 -f id_ed25519_aas -C aas-normal
ssh-keygen -t ed25519 -f id_ed25519_aas_emergency -C aas-emergency
```

在 A 为专用 `aas` 用户的 `~/.ssh/authorized_keys` 写两行（替换真实二进制和公钥）：

```text
restrict,command="/path/to/aasctl-shell --config /etc/aas/a.yaml --role consumer" ssh-ed25519 AAAA_REPLACE_WITH_NORMAL_KEY aas-normal
restrict,command="/path/to/aasctl-shell --config /etc/aas/a.yaml --role emergency" ssh-ed25519 AAAA_REPLACE_WITH_EMERGENCY_KEY aas-emergency
```

这两行提供不同的命令权限：consumer 可执行 `hello/list-ready/info/get/ack/ledger/status/crypto-probe`，却不能执行 `emergency-stream`；emergency 只能执行 `emergency-stream`。`restrict` 禁止 PTY、端口转发等额外功能。禁止在同一个账户额外加入不受限制的可登录密钥。

在 B 的 `known_hosts` 预先写入经**独立可信渠道核验指纹**的 A 主机公钥。不要关闭 SSH host key 验证。B `remote.identity_file` 指向普通 key，`remote.emergency_identity_file` 指向应急 key。

用 B 的普通 key 验证：

```bash
ssh -T -i /path/to/id_ed25519_aas aas@gpu.example.com 'aasctl hello'
```

此命令应输出 JSON，例如 `protocol_version=1`、`producer_id` 等。认证失败、服务器主机密钥变化，应停止排查，不要接受未知 host key。`aas-b crypto-probe` 在真实 SSH 和 age 配置就绪后执行。

## 3. Linux A 初始化与常驻运行

```bash
aas-a --config /etc/aas/a.yaml init
aas-a --config /etc/aas/a.yaml status
aas-a --config /etc/aas/a.yaml once
aas-a --config /etc/aas/a.yaml run --interval 15
```

单次命令可在计划任务里运行；常驻进程建议由 systemd 管理，且**同一 A root 只允许一个 A 主进程**（`producer.lock` 自动阻止并行入口）。A 检测 input 完成图片，满 100 张 / 1 GiB / 最老等待 600s 任一阈值时构建包；运行 `--interval 15` 是检查输入的间隔，不是 B 的 READY 轮询间隔。

正常队列是 `input → staging → building → ready → ACK → DELETED`。严禁脚本定期删 `ready/` 或 `staging/`。A 默认到达容量阈值时拒绝继续不安全打包，**不自动暂停生成器或自动删除尚未 ACK 的包**。

## 4. B 初始化与运行

```bash
aas-b --config /path/to/b.yaml init
aas-b --config /path/to/b.yaml crypto-probe
aas-b --config /path/to/b.yaml once
aas-b --config /path/to/b.yaml run
aas-b --config /path/to/b.yaml status
```

B 默认启动**立即首次发现**，成功轮询之后每 `300s` 查询；传输由受限 worker 处理；full READY + ledger 每 30 分钟对账。Windows 上可用 Task Scheduler/NSSM 等服务管理方式启动，但应确保一次只运行一个 `aas-b run` 实例。

**收到用户图片后**：AAS 只在 `state/receipts/` 保存证明；用户可自由管理 `output/<producer_id>/<batch_id>/`，无需向 AAS 报告移动/编辑/删除。

## 5. 故障修复

| 故障 | 操作 |
|---|---|
| A `PACKAGING` / `PUBLISHING` / `SOURCE_CLEANING` 中断 | 重启 `aas-a run` 或执行 `aas-a --config ... recover`；验证原始密文哈希和证书后续执行 |
| A `VERIFY_RECOVERY_BLOCKED` | 检查 DB、manifest、bundle 原始指纹，必要时从可信备份恢复**完全相同字节**；然后 `aas-a --config ... retry-blocked --batch-id ID`；未恢复不可强行删图 |
| A 需人工重新公告尚未 ACK 的 READY | `aas-a --config ... reannounce-ready --batch-id ID`，保持旧 seq 和不可变归档 SHA，仅产生新的 READY 事件 ID |
| A `ACKED/DELETING` 中断 | `aas-a --config ... recover`，依持久 ACK 墓碑续删，禁止手工目录递归删除 |
| B 网络失败 / 下载中断 | 正常 worker 断点重试；登录凭证失效请修复 SSH host key/用户授权而非关闭安全选项 |
| B `ACK_PENDING` | 内部 DELIVERED 已持久化，可重试 ACK；不重新验证用户当前输出 |
| B `MANUAL_HOLD` | 修复根因（密钥、哈希损坏、被占用目录、配置不匹配），然后 `aas-b --config ... retry-held --batch-id ID --reason "reason details"` |
| B `HANDOFF_UNCERTAIN` | 检查 PREPARED、用户交付事实；可 `aas-b ... resolve-handoff --batch-id ID --action hold --reason "..."`，或在人工已经独立确认交付后使用 `--action confirm-delivered`。后者明确记为 `MANUAL_ATTESTATION`，不伪称自动校验 |
| B SQLite 丢失，但独立收据存在 | 重新启动 B，自动读 `state/receipts` 重建已交付索引；用户输出即使全被删也不回退 DELIVERED |
| 容量不足 | 降低生产批量、提升容量、尽快运行 B，或由操作员**显式**启动独立应急通道；不删未 ACK 的归档 |

**最危险的是不知交付成功与否时，手工清空 B 的 receipts 或 A 的 DB。** 先保留状态目录与证据副本，再恢复。

## 6. 手动应急通道

```bash
aas-b --config /path/to/b.yaml emergency
```

B 使用专属应急 SSH key。A 对 `input/` 中尚未被正常批次领取的完整图片逐个发送原始字节，B 在 `output/emergency/<token>_<name>` 落盘并 SHA 通过后答复单图 ACK，A 才 unlink 该应急领取项。`staging/` 和 `ready/` 一律不参与。意外断连允许文件后续恢复回 input 并重复传送；不能把这个简化机制当成正常包系统的同等可靠保证。

## 7. 生产上线门槛

必须在真实目标环境重新执行 `docs/TEST_PLAN.md`，至少验证：`age` 最终密文回读与 B 私钥解密、两套 SSH key 权限隔离、批次断点续传、用户输出移动后的收据恢复、Windows 同卷 no-clobber rename（如使用 Windows）、A/B 异常进程终止与 72 小时长稳运行。没有这些验收，只能称为通过本机自动化测试的工程实现。
